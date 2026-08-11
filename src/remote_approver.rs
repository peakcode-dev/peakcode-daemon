use std::collections::{hash_map::Entry, HashMap};
use std::sync::Arc;

use async_trait::async_trait;
use peakcode_core::{ApprovalDecision, ApprovalRequest, Approver};
use tokio::sync::{mpsc, oneshot, Mutex};

use crate::ipc::{IpcApprovalDecision, WorkerEvent};

/// Routes core approval requests to the daemon and correlates typed replies.
#[derive(Clone)]
pub struct RemoteApprover {
    pending: Arc<Mutex<HashMap<String, oneshot::Sender<ApprovalDecision>>>>,
    events: mpsc::Sender<WorkerEvent>,
}

impl RemoteApprover {
    pub fn new(events: mpsc::Sender<WorkerEvent>) -> Self {
        Self {
            pending: Arc::new(Mutex::new(HashMap::new())),
            events,
        }
    }

    /// Resolve exactly one pending request. Unknown, duplicate, and canceled IDs return false.
    pub async fn resolve(&self, call_id: &str, decision: IpcApprovalDecision) -> bool {
        let sender = self.pending.lock().await.remove(call_id);
        sender.is_some_and(|sender| sender.send(map_decision(decision)).is_ok())
    }

    /// Drop every pending waiter so interrupted approval requests fail closed.
    pub async fn cancel_pending(&self) {
        self.pending.lock().await.clear();
    }
}

#[async_trait]
impl Approver for RemoteApprover {
    async fn approve(&self, request: ApprovalRequest) -> ApprovalDecision {
        let (response_tx, response_rx) = oneshot::channel();
        {
            let mut pending = self.pending.lock().await;
            match pending.entry(request.call_id.clone()) {
                Entry::Vacant(entry) => {
                    entry.insert(response_tx);
                }
                Entry::Occupied(_) => return ApprovalDecision::Deny,
            }
        }

        let event = WorkerEvent::NeedsApproval {
            call_id: request.call_id.clone(),
            tool: request.tool,
            arguments_json: request.arguments.to_string(),
        };
        if self.events.send(event).await.is_err() {
            self.pending.lock().await.remove(&request.call_id);
            return ApprovalDecision::Deny;
        }

        response_rx.await.unwrap_or(ApprovalDecision::Deny)
    }
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

    fn request(call_id: &str) -> ApprovalRequest {
        ApprovalRequest {
            tool: "bash".to_owned(),
            arguments: json!({"command": "pwd"}),
            call_id: call_id.to_owned(),
        }
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
}
