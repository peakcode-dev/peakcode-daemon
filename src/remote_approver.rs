use std::collections::{hash_map::Entry, HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use async_trait::async_trait;
use peakcode_core::{ApprovalDecision, ApprovalRequest, Approver};
use tokio::sync::{mpsc, oneshot};

use crate::call_id_mapper::CallIdMapper;
use crate::ipc::IpcApprovalDecision;

/// Routes core approval requests to the daemon and correlates typed replies.
#[derive(Clone)]
pub struct RemoteApprover {
    state: Arc<Mutex<ApprovalState>>,
    next_token: Arc<AtomicU64>,
    approvals: mpsc::Sender<ApprovalNotice>,
    call_ids: Arc<CallIdMapper>,
}

pub(crate) struct ApprovalNotice {
    pub(crate) call_id: String,
    pub(crate) tool: String,
    pub(crate) arguments_json: String,
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
    #[cfg(test)]
    pub(crate) fn new(approvals: mpsc::Sender<ApprovalNotice>) -> Self {
        Self::with_call_ids(approvals, Arc::new(CallIdMapper::default()))
    }

    pub(crate) fn with_call_ids(
        approvals: mpsc::Sender<ApprovalNotice>,
        call_ids: Arc<CallIdMapper>,
    ) -> Self {
        Self {
            state: Arc::new(Mutex::new(ApprovalState::default())),
            next_token: Arc::new(AtomicU64::new(0)),
            approvals,
            call_ids,
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
        {
            let state = lock_state(&self.state);
            if state.allowed_tools.contains(&request.tool) {
                return ApprovalDecision::AllowAll;
            }
        }
        let call_id = self.call_ids.start_approval(&request.call_id);
        {
            let mut state = lock_state(&self.state);
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

        let notice = ApprovalNotice {
            call_id,
            tool: request.tool,
            arguments_json: request.arguments.to_string(),
        };
        if self.approvals.send(notice).await.is_err() {
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
    use std::collections::HashMap;
    use std::time::Duration;

    use peakcode_core::{ApprovalDecision, ApprovalRequest, Approver};
    use serde_json::json;
    use tokio::sync::mpsc;
    use tokio::time::timeout;

    use super::{ApprovalNotice, RemoteApprover};
    use crate::ipc::IpcApprovalDecision;

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

        let ApprovalNotice {
            call_id: handle,
            tool,
            arguments_json,
        } = events_rx.recv().await.unwrap();
        assert_eq!(tool, "bash");
        assert_eq!(arguments_json, r#"{"command":"pwd"}"#);
        assert_ne!(handle, "call-1");
        assert!(
            approver
                .resolve(&handle, IpcApprovalDecision::AllowAll)
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
        let handle = events_rx.recv().await.unwrap().call_id;

        assert!(
            !approver
                .resolve("unknown", IpcApprovalDecision::Allow)
                .await
        );
        assert!(approver.resolve(&handle, IpcApprovalDecision::Deny).await);
        assert!(!approver.resolve(&handle, IpcApprovalDecision::Allow).await);
        assert_eq!(pending.await.unwrap(), ApprovalDecision::Deny);
    }

    #[tokio::test]
    async fn concurrent_requests_do_not_cross_wire_decisions() {
        let (events_tx, mut events_rx) = mpsc::channel(4);
        let approver = RemoteApprover::new(events_tx);
        let first = tokio::spawn({
            let approver = approver.clone();
            async move { approver.approve(request_for("first-tool", "first")).await }
        });
        let second = tokio::spawn({
            let approver = approver.clone();
            async move { approver.approve(request_for("second-tool", "second")).await }
        });

        let mut handles = HashMap::new();
        for _ in 0..2 {
            let notice = events_rx.recv().await.unwrap();
            handles.insert(notice.tool, notice.call_id);
        }

        assert!(
            approver
                .resolve(&handles["second-tool"], IpcApprovalDecision::Deny)
                .await
        );
        assert!(
            approver
                .resolve(&handles["first-tool"], IpcApprovalDecision::Allow)
                .await
        );
        assert_eq!(first.await.unwrap(), ApprovalDecision::Allow);
        assert_eq!(second.await.unwrap(), ApprovalDecision::Deny);
    }

    #[tokio::test]
    async fn repeated_provider_call_ids_receive_distinct_pending_handles() {
        let (events_tx, mut events_rx) = mpsc::channel(4);
        let approver = RemoteApprover::new(events_tx);
        let first = tokio::spawn({
            let approver = approver.clone();
            async move { approver.approve(request("same")).await }
        });
        let first_handle = events_rx.recv().await.unwrap().call_id;
        let second = tokio::spawn({
            let approver = approver.clone();
            async move { approver.approve(request("same")).await }
        });
        let second_handle = events_rx.recv().await.unwrap().call_id;

        assert_ne!(first_handle, second_handle);
        assert!(
            approver
                .resolve(&second_handle, IpcApprovalDecision::Deny)
                .await
        );
        assert!(
            approver
                .resolve(&first_handle, IpcApprovalDecision::Allow)
                .await
        );
        assert_eq!(first.await.unwrap(), ApprovalDecision::Allow);
        assert_eq!(second.await.unwrap(), ApprovalDecision::Deny);
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
        let handle = event.call_id;
        assert!(approver.resolve(&handle, IpcApprovalDecision::Allow).await);
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
        let first_handle = events_rx.recv().await.unwrap().call_id;
        assert!(
            approver
                .resolve(&first_handle, IpcApprovalDecision::AllowAll)
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
        let other_notice = events_rx.recv().await.unwrap();
        assert_eq!(other_notice.tool, "edit");
        let other_handle = other_notice.call_id;
        assert!(
            approver
                .resolve(&other_handle, IpcApprovalDecision::Deny)
                .await
        );
        assert_eq!(other.await.unwrap(), ApprovalDecision::Deny);
    }
}
