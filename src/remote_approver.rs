use std::collections::{hash_map::Entry, HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use async_trait::async_trait;
use peakcode_core::{ApprovalDecision, ApprovalRequest, Approver};
use tokio::sync::{mpsc, oneshot};

use crate::ipc::{IpcApprovalDecision, WorkerEvent};
use crate::redaction::Redactor;

/// Routes core approval requests to the daemon and correlates typed replies.
#[derive(Clone)]
pub struct RemoteApprover {
    state: Arc<Mutex<ApprovalState>>,
    next_token: Arc<AtomicU64>,
    events: mpsc::Sender<WorkerEvent>,
    redactor: Arc<Redactor>,
}

#[derive(Default)]
struct ApprovalState {
    pending: HashMap<String, PendingApproval>,
    allowed_tools: HashSet<String>,
}

struct PendingApproval {
    token: u64,
    tool: String,
    response: oneshot::Sender<ApprovalDecision>,
}

struct PendingRegistration {
    call_id: String,
    token: u64,
    state: Arc<Mutex<ApprovalState>>,
}

impl Drop for PendingRegistration {
    fn drop(&mut self) {
        let mut state = lock_state(&self.state);
        if state
            .pending
            .get(&self.call_id)
            .is_some_and(|approval| approval.token == self.token)
        {
            state.pending.remove(&self.call_id);
        }
    }
}

impl RemoteApprover {
    pub fn new(events: mpsc::Sender<WorkerEvent>) -> Self {
        Self::with_redactor(events, Arc::new(Redactor::empty()))
    }

    pub(crate) fn with_redactor(
        events: mpsc::Sender<WorkerEvent>,
        redactor: Arc<Redactor>,
    ) -> Self {
        Self {
            state: Arc::new(Mutex::new(ApprovalState::default())),
            next_token: Arc::new(AtomicU64::new(0)),
            events,
            redactor,
        }
    }

    /// Resolve exactly one pending request. Unknown, duplicate, and canceled IDs return false.
    pub async fn resolve(&self, call_id: &str, decision: IpcApprovalDecision) -> bool {
        let mut state = lock_state(&self.state);
        let Some(approval) = state.pending.remove(call_id) else {
            return false;
        };
        let tool = approval.tool.clone();
        let sent = approval
            .response
            .send(map_decision(decision.clone()))
            .is_ok();
        if sent && decision == IpcApprovalDecision::AllowAll {
            state.allowed_tools.insert(tool);
        }
        sent
    }

    /// Drop every pending waiter so interrupted approval requests fail closed.
    pub async fn cancel_pending(&self) {
        lock_state(&self.state).pending.clear();
    }
}

#[async_trait]
impl Approver for RemoteApprover {
    async fn approve(&self, request: ApprovalRequest) -> ApprovalDecision {
        let (response_tx, response_rx) = oneshot::channel();
        let token = self.next_token.fetch_add(1, Ordering::Relaxed);
        let call_id = self.redactor.redact_identifier(&request.call_id);
        {
            let mut state = lock_state(&self.state);
            if state.allowed_tools.contains(&request.tool) {
                return ApprovalDecision::AllowAll;
            }
            match state.pending.entry(call_id.clone()) {
                Entry::Vacant(entry) => {
                    entry.insert(PendingApproval {
                        token,
                        tool: request.tool.clone(),
                        response: response_tx,
                    });
                }
                Entry::Occupied(_) => return ApprovalDecision::Deny,
            }
        }
        let _registration = PendingRegistration {
            call_id: call_id.clone(),
            token,
            state: Arc::clone(&self.state),
        };

        let event = WorkerEvent::NeedsApproval {
            call_id,
            tool: request.tool,
            arguments_json: request.arguments.to_string(),
        };
        if self.events.send(event).await.is_err() {
            return ApprovalDecision::Deny;
        }

        response_rx.await.unwrap_or(ApprovalDecision::Deny)
    }
}

fn lock_state(state: &Mutex<ApprovalState>) -> MutexGuard<'_, ApprovalState> {
    state
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn map_decision(decision: IpcApprovalDecision) -> ApprovalDecision {
    match decision {
        IpcApprovalDecision::Allow => ApprovalDecision::Allow,
        IpcApprovalDecision::Deny => ApprovalDecision::Deny,
        IpcApprovalDecision::AllowAll => ApprovalDecision::AllowAll,
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use peakcode_core::{ApprovalDecision, ApprovalRequest, Approver};
    use serde_json::json;
    use tokio::sync::mpsc;
    use tokio::time::timeout;

    use super::RemoteApprover;
    use crate::ipc::{IpcApprovalDecision, WorkerEvent};

    fn request_for(tool: &str, call_id: &str) -> ApprovalRequest {
        ApprovalRequest {
            tool: tool.to_owned(),
            arguments: json!({"command": "pwd"}),
            call_id: call_id.to_owned(),
        }
    }

    fn request(call_id: &str) -> ApprovalRequest {
        request_for("bash", call_id)
    }

    #[tokio::test]
    async fn approve_emits_request_then_maps_typed_resolution() {
        let (events_tx, mut events_rx) = mpsc::channel(4);
        let approver = RemoteApprover::new(events_tx);
        let pending = tokio::spawn({
            let approver = approver.clone();
            async move { approver.approve(request("call-1")).await }
        });

        assert_eq!(
            events_rx.recv().await,
            Some(WorkerEvent::NeedsApproval {
                call_id: "call-1".to_owned(),
                tool: "bash".to_owned(),
                arguments_json: r#"{"command":"pwd"}"#.to_owned(),
            })
        );
        assert!(
            approver
                .resolve("call-1", IpcApprovalDecision::AllowAll)
                .await
        );
        assert_eq!(pending.await.unwrap(), ApprovalDecision::AllowAll);
    }

    #[tokio::test]
    async fn resolve_rejects_unknown_and_duplicate_ids() {
        let (events_tx, mut events_rx) = mpsc::channel(4);
        let approver = RemoteApprover::new(events_tx);
        let pending = tokio::spawn({
            let approver = approver.clone();
            async move { approver.approve(request("known")).await }
        });
        events_rx.recv().await.unwrap();

        assert!(
            !approver
                .resolve("unknown", IpcApprovalDecision::Allow)
                .await
        );
        assert!(approver.resolve("known", IpcApprovalDecision::Deny).await);
        assert!(!approver.resolve("known", IpcApprovalDecision::Allow).await);
        assert_eq!(pending.await.unwrap(), ApprovalDecision::Deny);
    }

    #[tokio::test]
    async fn concurrent_requests_do_not_cross_wire_decisions() {
        let (events_tx, mut events_rx) = mpsc::channel(4);
        let approver = RemoteApprover::new(events_tx);
        let first = tokio::spawn({
            let approver = approver.clone();
            async move { approver.approve(request("first")).await }
        });
        let second = tokio::spawn({
            let approver = approver.clone();
            async move { approver.approve(request("second")).await }
        });

        let mut ids = vec![
            match events_rx.recv().await.unwrap() {
                WorkerEvent::NeedsApproval { call_id, .. } => call_id,
                event => panic!("unexpected event: {event:?}"),
            },
            match events_rx.recv().await.unwrap() {
                WorkerEvent::NeedsApproval { call_id, .. } => call_id,
                event => panic!("unexpected event: {event:?}"),
            },
        ];
        ids.sort();
        assert_eq!(ids, ["first", "second"]);

        assert!(approver.resolve("second", IpcApprovalDecision::Deny).await);
        assert!(approver.resolve("first", IpcApprovalDecision::Allow).await);
        assert_eq!(first.await.unwrap(), ApprovalDecision::Allow);
        assert_eq!(second.await.unwrap(), ApprovalDecision::Deny);
    }

    #[tokio::test]
    async fn duplicate_pending_call_id_is_denied_without_replacing_waiter() {
        let (events_tx, mut events_rx) = mpsc::channel(4);
        let approver = RemoteApprover::new(events_tx);
        let first = tokio::spawn({
            let approver = approver.clone();
            async move { approver.approve(request("same")).await }
        });
        events_rx.recv().await.unwrap();

        assert_eq!(
            approver.approve(request("same")).await,
            ApprovalDecision::Deny
        );
        assert!(timeout(Duration::from_millis(20), events_rx.recv())
            .await
            .is_err());
        assert!(approver.resolve("same", IpcApprovalDecision::Allow).await);
        assert_eq!(first.await.unwrap(), ApprovalDecision::Allow);
    }

    #[tokio::test]
    async fn approve_denies_when_event_delivery_fails() {
        let (events_tx, events_rx) = mpsc::channel(1);
        drop(events_rx);
        let approver = RemoteApprover::new(events_tx);

        assert_eq!(
            approver.approve(request("call-1")).await,
            ApprovalDecision::Deny
        );
        assert!(!approver.resolve("call-1", IpcApprovalDecision::Allow).await);
    }

    #[tokio::test]
    async fn approve_denies_when_pending_wait_is_closed() {
        let (events_tx, mut events_rx) = mpsc::channel(1);
        let approver = RemoteApprover::new(events_tx);
        let pending = tokio::spawn({
            let approver = approver.clone();
            async move { approver.approve(request("call-1")).await }
        });
        events_rx.recv().await.unwrap();

        approver.cancel_pending().await;
        assert_eq!(pending.await.unwrap(), ApprovalDecision::Deny);
        assert!(!approver.resolve("call-1", IpcApprovalDecision::Allow).await);
    }

    #[tokio::test]
    async fn aborted_approval_removes_only_its_waiter_and_call_id_can_be_reused() {
        let (events_tx, mut events_rx) = mpsc::channel(4);
        let approver = RemoteApprover::new(events_tx);
        let first = tokio::spawn({
            let approver = approver.clone();
            async move { approver.approve(request("reused")).await }
        });
        events_rx.recv().await.unwrap();

        first.abort();
        assert!(first.await.unwrap_err().is_cancelled());

        let second = tokio::spawn({
            let approver = approver.clone();
            async move { approver.approve(request("reused")).await }
        });
        let event = timeout(Duration::from_millis(100), events_rx.recv())
            .await
            .expect("reused call_id did not emit a new approval event")
            .unwrap();
        assert!(matches!(event, WorkerEvent::NeedsApproval { call_id, .. } if call_id == "reused"));
        assert!(approver.resolve("reused", IpcApprovalDecision::Allow).await);
        assert_eq!(second.await.unwrap(), ApprovalDecision::Allow);
    }

    #[tokio::test]
    async fn allow_all_persists_for_same_tool_but_not_other_tools() {
        let (events_tx, mut events_rx) = mpsc::channel(4);
        let approver = RemoteApprover::new(events_tx);
        let first = tokio::spawn({
            let approver = approver.clone();
            async move { approver.approve(request_for("bash", "first")).await }
        });
        events_rx.recv().await.unwrap();
        assert!(
            approver
                .resolve("first", IpcApprovalDecision::AllowAll)
                .await
        );
        assert_eq!(first.await.unwrap(), ApprovalDecision::AllowAll);

        assert_eq!(
            timeout(
                Duration::from_millis(100),
                approver.approve(request_for("bash", "second")),
            )
            .await
            .expect("same-tool AllowAll did not persist"),
            ApprovalDecision::AllowAll
        );
        assert!(timeout(Duration::from_millis(20), events_rx.recv())
            .await
            .is_err());

        let other = tokio::spawn({
            let approver = approver.clone();
            async move { approver.approve(request_for("edit", "third")).await }
        });
        assert!(
            matches!(events_rx.recv().await, Some(WorkerEvent::NeedsApproval { call_id, tool, .. }) if call_id == "third" && tool == "edit")
        );
        assert!(approver.resolve("third", IpcApprovalDecision::Deny).await);
        assert_eq!(other.await.unwrap(), ApprovalDecision::Deny);
    }
}
