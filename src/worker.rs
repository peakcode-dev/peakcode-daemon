use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail};
use peakcode_core::tool::{BashTool, EditTool, GlobTool, GrepTool, ReadTool, WriteTool};
use peakcode_core::{
    Agent, AgentEvent, Approver, Config, Message, OpenAiProvider, Provider, ProviderConfig,
    ToolContext, ToolRegistry,
};
use tokio::io::BufReader;
use tokio::net::UnixStream;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::timeout;

use crate::call_id_mapper::CallIdMapper;
use crate::ipc::{read_frame, write_frame, DaemonCommand, WorkerEvent};
use crate::redaction::Redactor;
use crate::remote_approver::{ApprovalNotice, RemoteApprover};

const WORKER_EVENT_CHANNEL_CAPACITY: usize = 64;
const WORKER_COMMAND_CHANNEL_CAPACITY: usize = 64;
const WORKER_APPROVAL_CHANNEL_CAPACITY: usize = 64;
const WORKER_SHUTDOWN_TIMEOUT: Duration = Duration::from_millis(250);
/// Maximum user inputs retained behind one active agent turn.
pub const MAX_QUEUED_INPUTS: usize = 32;

struct ActiveTurn {
    events: mpsc::Receiver<AgentEvent>,
    handle: JoinHandle<anyhow::Result<Vec<Message>>>,
}

/// Connect to the daemon socket and run one long-lived worker session.
pub async fn run(_session_id: String, ipc_path: String) -> anyhow::Result<()> {
    let stream = UnixStream::connect(ipc_path).await?;
    let config = Config::discover()?;
    if config.provider != "openai" {
        bail!("unsupported provider: {}", config.provider);
    }
    let provider_config = Arc::new(config.provider_config()?);
    let provider: Arc<dyn Provider> = Arc::new(OpenAiProvider::default());
    let context = Arc::new(ToolContext::new(std::env::current_dir()?));
    let tools = default_tools();
    let messages = config
        .system_prompt
        .map(Message::system)
        .into_iter()
        .collect();

    run_connection(stream, provider, provider_config, tools, context, messages).await
}

fn default_tools() -> Arc<ToolRegistry> {
    let mut tools = ToolRegistry::new();
    tools.register(Box::new(ReadTool));
    tools.register(Box::new(GlobTool));
    tools.register(Box::new(GrepTool));
    tools.register(Box::new(WriteTool));
    tools.register(Box::new(EditTool));
    tools.register(Box::new(BashTool));
    Arc::new(tools)
}

async fn run_connection(
    stream: UnixStream,
    provider: Arc<dyn Provider>,
    provider_config: Arc<ProviderConfig>,
    tools: Arc<ToolRegistry>,
    context: Arc<ToolContext>,
    mut messages: Vec<Message>,
) -> anyhow::Result<()> {
    let (read_half, mut write_half) = stream.into_split();
    let redactor = Arc::new(Redactor::from_env(&provider_config.api_key));
    let call_ids = Arc::new(CallIdMapper::default());
    let mut socket_reader = BufReader::new(read_half);
    let (command_tx, mut command_rx) = mpsc::channel(WORKER_COMMAND_CHANNEL_CAPACITY);
    let command_reader = tokio::spawn(async move {
        loop {
            match read_frame::<_, DaemonCommand>(&mut socket_reader).await {
                Ok(Some(command)) => {
                    if command_tx.send(Ok(command)).await.is_err() {
                        break;
                    }
                }
                Ok(None) => break,
                Err(error) => {
                    let _ = command_tx.send(Err(error)).await;
                    break;
                }
            }
        }
    });
    let (event_tx, mut event_rx) = mpsc::channel(WORKER_EVENT_CHANNEL_CAPACITY);
    let (approval_tx, mut approval_rx) = mpsc::channel(WORKER_APPROVAL_CHANNEL_CAPACITY);
    let writer_redactor = Arc::clone(&redactor);
    let writer = tokio::spawn(async move {
        while let Some(event) = event_rx.recv().await {
            write_frame(&mut write_half, &writer_redactor.redact_event(event)).await?;
        }
        Ok::<(), std::io::Error>(())
    });
    let approver = Arc::new(RemoteApprover::with_call_ids(
        approval_tx,
        Arc::clone(&call_ids),
        &tools,
    ));
    let mut active: Option<ActiveTurn> = None;
    let mut queued_inputs = VecDeque::new();
    let mut pending_event = None;
    let mut approval_barrier = None;

    let result = 'worker: loop {
        if active.is_none() {
            if pending_event.is_some() {
                tokio::select! {
                    command = command_rx.recv() => {
                        match command {
                            Some(Ok(DaemonCommand::Input { text })) => {
                                if queued_inputs.len() == MAX_QUEUED_INPUTS {
                                    let error = anyhow!(
                                        "worker input queue limit exceeded (maximum {MAX_QUEUED_INPUTS})"
                                    );
                                    send_ordered_terminal_event(
                                        &event_tx,
                                        pending_event.take(),
                                        WorkerEvent::Crash {
                                            message: error.to_string(),
                                        },
                                    )
                                    .await;
                                    break 'worker Err(error);
                                }
                                queued_inputs.push_back(text);
                            }
                            Some(Ok(DaemonCommand::Approve { call_id, decision })) => {
                                if !approver.resolve(&call_id, decision).await {
                                    tracing::warn!("approval did not match a pending request");
                                }
                            }
                            Some(Ok(DaemonCommand::Cancel)) => {}
                            Some(Ok(DaemonCommand::Stop)) => {
                                send_ordered_terminal_event(
                                    &event_tx,
                                    pending_event.take(),
                                    done_event(messages.len()),
                                )
                                .await;
                                break 'worker Ok(());
                            }
                            Some(Err(error)) => break 'worker Err(error.into()),
                            None => break 'worker Ok(()),
                        }
                    }
                    permit = event_tx.reserve() => {
                        match permit {
                            Ok(permit) => permit.send(
                                pending_event.take().expect("pending event was checked")
                            ),
                            Err(_) => break 'worker Err(anyhow!("worker event writer closed")),
                        }
                    }
                }
                continue;
            }

            if let Some(text) = queued_inputs.pop_front() {
                active = Some(
                    start_turn(
                        Arc::clone(&provider),
                        Arc::clone(&provider_config),
                        Arc::clone(&tools),
                        approver.clone(),
                        Arc::clone(&context),
                        &messages,
                        text,
                    )
                    .await,
                );
                continue;
            }

            match command_rx.recv().await {
                Some(Ok(command)) => match command {
                    DaemonCommand::Input { text } => queued_inputs.push_back(text),
                    DaemonCommand::Approve { call_id, decision } => {
                        if !approver.resolve(&call_id, decision).await {
                            tracing::warn!("approval did not match a pending request");
                        }
                    }
                    DaemonCommand::Cancel => {}
                    DaemonCommand::Stop => {
                        send_terminal_event(&event_tx, done_event(messages.len())).await;
                        break 'worker Ok(());
                    }
                },
                Some(Err(error)) => break 'worker Err(error.into()),
                None => break 'worker Ok(()),
            }
            continue;
        }

        if pending_event.is_none() && approval_barrier.is_some() {
            let turn = active.as_mut().expect("active turn was checked");
            match turn.events.try_recv() {
                Ok(event) => {
                    pending_event = Some(map_agent_event(&call_ids, event));
                    continue;
                }
                Err(mpsc::error::TryRecvError::Empty) => {
                    pending_event = Some(map_approval_notice(
                        approval_barrier
                            .take()
                            .expect("approval barrier was checked"),
                    ));
                    continue;
                }
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    approval_barrier = None;
                }
            }
        }

        let turn = active.as_mut().expect("active turn was checked");
        tokio::select! {
            command = command_rx.recv() => {
                match command {
                    Some(Ok(DaemonCommand::Input { text })) => {
                        if queued_inputs.len() == MAX_QUEUED_INPUTS {
                            let error = anyhow!(
                                "worker input queue limit exceeded (maximum {MAX_QUEUED_INPUTS})"
                            );
                            send_ordered_terminal_event(
                                &event_tx,
                                pending_event.take(),
                                WorkerEvent::Crash {
                                    message: error.to_string(),
                                },
                            )
                            .await;
                            break 'worker Err(error);
                        }
                        queued_inputs.push_back(text);
                    }
                    Some(Ok(DaemonCommand::Approve { call_id, decision })) => {
                        if !approver.resolve(&call_id, decision).await {
                            tracing::warn!("approval did not match a pending request");
                        }
                    }
                    Some(Ok(DaemonCommand::Cancel)) => {
                        let cancellation = if let Some(turn) = active.take() {
                            cancel_active_turn(turn, &approver).await
                        } else {
                            approver.cancel_pending().await;
                            Ok(())
                        };
                        call_ids.clear();
                        approval_barrier = None;
                        drain_approval_notices(&mut approval_rx);
                        if matches!(pending_event, Some(WorkerEvent::NeedsApproval { .. })) {
                            pending_event = None;
                        }
                        if let Err(error) = cancellation {
                            send_ordered_terminal_event(
                                &event_tx,
                                pending_event.take(),
                                WorkerEvent::Crash {
                                    message: error.to_string(),
                                },
                            )
                            .await;
                            break 'worker Err(error);
                        }
                    }
                    Some(Ok(DaemonCommand::Stop)) => {
                        if let Some(turn) = active.take() {
                            abort_turn(turn).await;
                        }
                        approver.cancel_pending().await;
                        call_ids.clear();
                        drain_approval_notices(&mut approval_rx);
                        if matches!(pending_event, Some(WorkerEvent::NeedsApproval { .. })) {
                            pending_event = None;
                        }
                        send_ordered_terminal_event(
                            &event_tx,
                            pending_event.take(),
                            done_event(messages.len()),
                        )
                        .await;
                        break 'worker Ok(());
                    }
                    Some(Err(error)) => break 'worker Err(error.into()),
                    None => break 'worker Ok(()),
                }
            }
            permit = event_tx.reserve(), if pending_event.is_some() => {
                match permit {
                    Ok(permit) => permit.send(pending_event.take().expect("pending event was checked")),
                    Err(_) => break 'worker Err(anyhow!("worker event writer closed")),
                }
            }
            notice = approval_rx.recv(), if approval_barrier.is_none() => {
                match notice {
                    Some(notice) => approval_barrier = Some(notice),
                    None => break 'worker Err(anyhow!("worker approval channel closed")),
                }
            }
            event = turn.events.recv(), if pending_event.is_none() && approval_barrier.is_none() => {
                match event {
                    Some(event) => pending_event = Some(map_agent_event(&call_ids, event)),
                    None => {
                        let turn = active.take().expect("active turn was checked");
                        match turn.handle.await {
                            Ok(Ok(final_messages)) => messages = final_messages,
                            Ok(Err(error)) => {
                                let message = error.to_string();
                                send_terminal_event(&event_tx, WorkerEvent::Crash { message }).await;
                                break 'worker Err(anyhow!("agent turn failed"));
                            }
                            Err(error) => break 'worker Err(error.into()),
                        }
                        call_ids.clear();
                        drain_approval_notices(&mut approval_rx);
                    }
                }
            }
        }
    };

    if let Some(turn) = active {
        abort_turn(turn).await;
    }
    approver.cancel_pending().await;
    call_ids.clear();
    drain_approval_notices(&mut approval_rx);
    command_reader.abort();
    let command_reader_result = timeout(WORKER_SHUTDOWN_TIMEOUT, command_reader).await;
    drop(approver);
    drop(event_tx);
    let mut writer = writer;
    let writer_result = match timeout(WORKER_SHUTDOWN_TIMEOUT, &mut writer).await {
        Ok(result) => Some(result),
        Err(_) => {
            writer.abort();
            let _ = timeout(WORKER_SHUTDOWN_TIMEOUT, &mut writer).await;
            None
        }
    };

    result?;
    if let Ok(Err(error)) = command_reader_result {
        if !error.is_cancelled() {
            return Err(anyhow!("worker command reader task failed: {error}"));
        }
    }
    if let Some(result) = writer_result {
        result.map_err(|error| anyhow!("worker event writer task failed: {error}"))??;
    }
    Ok(())
}

fn done_event(final_message_count: usize) -> WorkerEvent {
    WorkerEvent::Done {
        final_message_count,
    }
}

async fn send_terminal_event(events: &mpsc::Sender<WorkerEvent>, event: WorkerEvent) {
    let _ = timeout(WORKER_SHUTDOWN_TIMEOUT, events.send(event)).await;
}

async fn send_ordered_terminal_event(
    events: &mpsc::Sender<WorkerEvent>,
    pending_event: Option<WorkerEvent>,
    terminal_event: WorkerEvent,
) {
    let _ = timeout(WORKER_SHUTDOWN_TIMEOUT, async {
        if let Some(event) = pending_event {
            events.send(event).await?;
        }
        events.send(terminal_event).await
    })
    .await;
}

async fn abort_turn(mut turn: ActiveTurn) {
    turn.handle.abort();
    let _ = timeout(WORKER_SHUTDOWN_TIMEOUT, &mut turn.handle).await;
}

async fn cancel_active_turn(turn: ActiveTurn, approver: &RemoteApprover) -> anyhow::Result<()> {
    turn.handle.abort();
    let mut waiter = tokio::spawn(async move {
        let _ = turn.handle.await;
    });
    let completed = timeout(WORKER_SHUTDOWN_TIMEOUT, &mut waiter).await.is_ok();
    if !completed {
        waiter.abort();
    }
    approver.cancel_pending().await;
    if completed {
        Ok(())
    } else {
        Err(anyhow!("agent cancellation cleanup deadline exceeded"))
    }
}

async fn start_turn(
    provider: Arc<dyn Provider>,
    provider_config: Arc<ProviderConfig>,
    tools: Arc<ToolRegistry>,
    approver: Arc<RemoteApprover>,
    context: Arc<ToolContext>,
    messages: &[Message],
    input: String,
) -> ActiveTurn {
    let mut turn_messages = messages.to_vec();
    turn_messages.push(Message::user(input));
    let approver: Arc<dyn Approver> = approver;
    let (events, handle) = Agent::run(
        provider,
        provider_config,
        tools,
        approver,
        context,
        turn_messages,
    )
    .await;
    ActiveTurn { events, handle }
}

fn map_agent_event(call_ids: &CallIdMapper, event: AgentEvent) -> WorkerEvent {
    match event {
        AgentEvent::TextDelta(text) => WorkerEvent::TextDelta { text },
        AgentEvent::AssistantMessage(message) => WorkerEvent::AssistantMessage {
            text: message.text_content(),
        },
        AgentEvent::ToolStart {
            call_id,
            name,
            arguments,
        } => WorkerEvent::ToolStart {
            call_id: call_ids.start_event(&call_id),
            name,
            arguments_json: arguments.to_string(),
        },
        AgentEvent::ToolResult {
            call_id,
            name,
            output,
        } => WorkerEvent::ToolResult {
            call_id: call_ids.finish(&call_id),
            name,
            content: output.content,
            is_error: output.is_error,
        },
        AgentEvent::TurnFinished => WorkerEvent::TurnFinished,
    }
}

fn map_approval_notice(notice: ApprovalNotice) -> WorkerEvent {
    WorkerEvent::NeedsApproval {
        call_id: notice.call_id,
        tool: notice.tool,
        arguments_json: notice.arguments_json,
    }
}

fn drain_approval_notices(approvals: &mut mpsc::Receiver<ApprovalNotice>) {
    while approvals.try_recv().is_ok() {}
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::task::{Context, Poll};
    use std::time::Duration;

    use async_trait::async_trait;
    use futures::Stream;
    use peakcode_core::{
        Approval, ApprovalRequest, Approver, FinishReason, Message, Provider, ProviderConfig,
        ProviderError, ProviderEvent, ProviderStream, Role, Tool, ToolContext, ToolOutput,
        ToolRegistry,
    };
    use serde_json::json;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::UnixStream;
    use tokio::sync::mpsc;
    use tokio::time::timeout;
    use tokio_stream::wrappers::UnboundedReceiverStream;

    use super::{cancel_active_turn, run_connection, ActiveTurn};
    use crate::ipc::{read_frame, write_frame, DaemonCommand, IpcApprovalDecision, WorkerEvent};
    use crate::remote_approver::RemoteApprover;

    struct StopProvider;

    #[async_trait]
    impl Provider for StopProvider {
        fn name(&self) -> &str {
            "stop"
        }

        async fn stream(
            &self,
            messages: &[Message],
            _tools: &[peakcode_core::ToolSchema],
            _config: &ProviderConfig,
        ) -> Result<ProviderStream, ProviderError> {
            let text = messages
                .iter()
                .rev()
                .find(|message| message.role == Role::User)
                .map(Message::text_content)
                .unwrap_or_default();
            Ok(stream(vec![
                ProviderEvent::TextDelta(text),
                ProviderEvent::Finish {
                    reason: FinishReason::Stop,
                },
            ]))
        }
    }

    struct ApprovalProvider {
        calls: AtomicUsize,
    }

    #[async_trait]
    impl Provider for ApprovalProvider {
        fn name(&self) -> &str {
            "approval"
        }

        async fn stream(
            &self,
            _messages: &[Message],
            _tools: &[peakcode_core::ToolSchema],
            _config: &ProviderConfig,
        ) -> Result<ProviderStream, ProviderError> {
            if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                Ok(stream(vec![
                    ProviderEvent::ToolCallStart {
                        id: "tool-1".to_owned(),
                        name: "mutate".to_owned(),
                    },
                    ProviderEvent::ToolCallArgsDelta {
                        id: "tool-1".to_owned(),
                        delta: "{}".to_owned(),
                    },
                    ProviderEvent::Finish {
                        reason: FinishReason::ToolUse,
                    },
                ]))
            } else {
                Ok(stream(vec![ProviderEvent::Finish {
                    reason: FinishReason::Stop,
                }]))
            }
        }
    }

    struct CancelProvider {
        calls: AtomicUsize,
    }

    #[async_trait]
    impl Provider for CancelProvider {
        fn name(&self) -> &str {
            "cancel"
        }

        async fn stream(
            &self,
            _messages: &[Message],
            _tools: &[peakcode_core::ToolSchema],
            _config: &ProviderConfig,
        ) -> Result<ProviderStream, ProviderError> {
            let (tx, rx) = mpsc::unbounded_channel();
            if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                tokio::spawn(async move {
                    let _ = tx.send(Ok(ProviderEvent::TextDelta("started".to_owned())));
                    tokio::time::sleep(Duration::from_secs(30)).await;
                });
            } else {
                let _ = tx.send(Ok(ProviderEvent::TextDelta("second".to_owned())));
                let _ = tx.send(Ok(ProviderEvent::Finish {
                    reason: FinishReason::Stop,
                }));
            }
            Ok(Box::pin(UnboundedReceiverStream::new(rx)))
        }
    }

    struct FragmentProvider {
        calls: AtomicUsize,
    }

    #[async_trait]
    impl Provider for FragmentProvider {
        fn name(&self) -> &str {
            "fragment"
        }

        async fn stream(
            &self,
            _messages: &[Message],
            _tools: &[peakcode_core::ToolSchema],
            _config: &ProviderConfig,
        ) -> Result<ProviderStream, ProviderError> {
            let (tx, rx) = mpsc::unbounded_channel();
            if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                tokio::spawn(async move {
                    let _ = tx.send(Ok(ProviderEvent::TextDelta("before".to_owned())));
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    let _ = tx.send(Ok(ProviderEvent::TextDelta("during".to_owned())));
                    tokio::time::sleep(Duration::from_secs(30)).await;
                });
            } else {
                let _ = tx.send(Ok(ProviderEvent::Finish {
                    reason: FinishReason::Stop,
                }));
            }
            Ok(Box::pin(UnboundedReceiverStream::new(rx)))
        }
    }

    struct HeldEventProvider {
        calls: AtomicUsize,
    }

    struct BackpressureTool {
        executed: Arc<AtomicUsize>,
        held: Arc<tokio::sync::Notify>,
    }

    struct NonApprovalProvider {
        calls: AtomicUsize,
    }

    struct BackpressuredApprovalProvider {
        calls: AtomicUsize,
    }

    struct NonApprovalTool;

    #[async_trait]
    impl Provider for NonApprovalProvider {
        fn name(&self) -> &str {
            "nonapproval"
        }

        async fn stream(
            &self,
            _messages: &[Message],
            _tools: &[peakcode_core::ToolSchema],
            _config: &ProviderConfig,
        ) -> Result<ProviderStream, ProviderError> {
            if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                Ok(stream(vec![
                    ProviderEvent::ToolCallStart {
                        id: "raw-provider-id".to_owned(),
                        name: "inspect".to_owned(),
                    },
                    ProviderEvent::ToolCallArgsDelta {
                        id: "raw-provider-id".to_owned(),
                        delta: "{}".to_owned(),
                    },
                    ProviderEvent::Finish {
                        reason: FinishReason::ToolUse,
                    },
                ]))
            } else {
                Ok(stream(vec![ProviderEvent::Finish {
                    reason: FinishReason::Stop,
                }]))
            }
        }
    }

    #[async_trait]
    impl Provider for BackpressuredApprovalProvider {
        fn name(&self) -> &str {
            "backpressured-approval"
        }

        async fn stream(
            &self,
            _messages: &[Message],
            _tools: &[peakcode_core::ToolSchema],
            _config: &ProviderConfig,
        ) -> Result<ProviderStream, ProviderError> {
            if self.calls.fetch_add(1, Ordering::SeqCst) != 0 {
                return Ok(stream(vec![ProviderEvent::Finish {
                    reason: FinishReason::Stop,
                }]));
            }

            let mut events = Vec::with_capacity(257);
            for index in 0..128 {
                let call_id = format!("pressure-{index}");
                events.push(ProviderEvent::ToolCallStart {
                    id: call_id.clone(),
                    name: if index == 127 {
                        "mutate".to_owned()
                    } else {
                        "backpressure".to_owned()
                    },
                });
                events.push(ProviderEvent::ToolCallArgsDelta {
                    id: call_id,
                    delta: json!({"index": index}).to_string(),
                });
            }
            events.push(ProviderEvent::Finish {
                reason: FinishReason::ToolUse,
            });
            Ok(stream(events))
        }
    }

    #[async_trait]
    impl Tool for NonApprovalTool {
        fn name(&self) -> &str {
            "inspect"
        }

        fn description(&self) -> &str {
            "does not require approval"
        }

        fn parameters(&self) -> serde_json::Value {
            json!({"type": "object"})
        }

        fn requires_approval(&self) -> Approval {
            Approval::None
        }

        async fn execute(
            &self,
            _params: serde_json::Value,
            _ctx: &ToolContext,
        ) -> anyhow::Result<ToolOutput> {
            Ok(ToolOutput::ok("inspected"))
        }
    }

    #[async_trait]
    impl Provider for HeldEventProvider {
        fn name(&self) -> &str {
            "held-event"
        }

        async fn stream(
            &self,
            _messages: &[Message],
            _tools: &[peakcode_core::ToolSchema],
            _config: &ProviderConfig,
        ) -> Result<ProviderStream, ProviderError> {
            if self.calls.fetch_add(1, Ordering::SeqCst) != 0 {
                return Ok(stream(vec![
                    ProviderEvent::TextDelta("next-turn".to_owned()),
                    ProviderEvent::Finish {
                        reason: FinishReason::Stop,
                    },
                ]));
            }

            let mut events = Vec::with_capacity(257);
            for index in 0..128 {
                let call_id = format!("held-{index}");
                events.push(ProviderEvent::ToolCallStart {
                    id: call_id.clone(),
                    name: "backpressure".to_owned(),
                });
                events.push(ProviderEvent::ToolCallArgsDelta {
                    id: call_id,
                    delta: json!({"index": index}).to_string(),
                });
            }
            events.push(ProviderEvent::Finish {
                reason: FinishReason::ToolUse,
            });
            Ok(stream(events))
        }
    }

    #[async_trait]
    impl Tool for BackpressureTool {
        fn name(&self) -> &str {
            "backpressure"
        }

        fn description(&self) -> &str {
            "fills the worker event writer"
        }

        fn parameters(&self) -> serde_json::Value {
            json!({"type": "object"})
        }

        fn requires_approval(&self) -> Approval {
            Approval::None
        }

        async fn execute(
            &self,
            params: serde_json::Value,
            _ctx: &ToolContext,
        ) -> anyhow::Result<ToolOutput> {
            let index = params["index"].as_u64().unwrap();
            let count = self.executed.fetch_add(1, Ordering::SeqCst) + 1;
            if count == 34 {
                self.held.notify_one();
            }
            Ok(ToolOutput::ok(format!("{index}:{}", "x".repeat(16 * 1024))))
        }
    }

    struct SecretErrorProvider {
        secret: String,
    }

    #[async_trait]
    impl Provider for SecretErrorProvider {
        fn name(&self) -> &str {
            "secret-error"
        }

        async fn stream(
            &self,
            _messages: &[Message],
            _tools: &[peakcode_core::ToolSchema],
            _config: &ProviderConfig,
        ) -> Result<ProviderStream, ProviderError> {
            Err(ProviderError::Network(format!(
                "provider leaked {}",
                self.secret
            )))
        }
    }

    struct SecretToolProvider {
        calls: AtomicUsize,
        secret: String,
    }

    #[async_trait]
    impl Provider for SecretToolProvider {
        fn name(&self) -> &str {
            "secret-tool"
        }

        async fn stream(
            &self,
            _messages: &[Message],
            _tools: &[peakcode_core::ToolSchema],
            _config: &ProviderConfig,
        ) -> Result<ProviderStream, ProviderError> {
            if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                let call_id = format!("call-{}", self.secret);
                Ok(stream(vec![
                    ProviderEvent::ToolCallStart {
                        id: call_id.clone(),
                        name: "secret-output".to_owned(),
                    },
                    ProviderEvent::ToolCallArgsDelta {
                        id: call_id,
                        delta: json!({"token": self.secret}).to_string(),
                    },
                    ProviderEvent::Finish {
                        reason: FinishReason::ToolUse,
                    },
                ]))
            } else {
                Ok(stream(vec![ProviderEvent::Finish {
                    reason: FinishReason::Stop,
                }]))
            }
        }
    }

    struct SecretOutputTool {
        secret: String,
    }

    #[async_trait]
    impl Tool for SecretOutputTool {
        fn name(&self) -> &str {
            "secret-output"
        }

        fn description(&self) -> &str {
            "returns test output"
        }

        fn parameters(&self) -> serde_json::Value {
            json!({"type": "object"})
        }

        fn requires_approval(&self) -> Approval {
            Approval::Required
        }

        async fn execute(
            &self,
            _params: serde_json::Value,
            _ctx: &ToolContext,
        ) -> anyhow::Result<ToolOutput> {
            Ok(ToolOutput::ok(format!("tool leaked {}", self.secret)))
        }
    }

    struct CrossTurnApprovalProvider {
        calls: AtomicUsize,
    }

    #[async_trait]
    impl Provider for CrossTurnApprovalProvider {
        fn name(&self) -> &str {
            "cross-turn-approval"
        }

        async fn stream(
            &self,
            _messages: &[Message],
            _tools: &[peakcode_core::ToolSchema],
            _config: &ProviderConfig,
        ) -> Result<ProviderStream, ProviderError> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            if call % 2 == 1 {
                return Ok(stream(vec![ProviderEvent::Finish {
                    reason: FinishReason::Stop,
                }]));
            }
            let turn = call / 2;
            let tool = if turn < 2 { "mutate" } else { "other" };
            let call_id = format!("turn-{turn}");
            Ok(stream(vec![
                ProviderEvent::ToolCallStart {
                    id: call_id.clone(),
                    name: tool.to_owned(),
                },
                ProviderEvent::ToolCallArgsDelta {
                    id: call_id,
                    delta: "{}".to_owned(),
                },
                ProviderEvent::Finish {
                    reason: FinishReason::ToolUse,
                },
            ]))
        }
    }

    struct OtherTool;

    #[async_trait]
    impl Tool for OtherTool {
        fn name(&self) -> &str {
            "other"
        }

        fn description(&self) -> &str {
            "other mutation"
        }

        fn parameters(&self) -> serde_json::Value {
            json!({"type": "object"})
        }

        fn requires_approval(&self) -> Approval {
            Approval::Required
        }

        async fn execute(
            &self,
            _params: serde_json::Value,
            _ctx: &ToolContext,
        ) -> anyhow::Result<ToolOutput> {
            Ok(ToolOutput::ok("other"))
        }
    }

    struct SlowCancelStream {
        emitted: bool,
        canceled: Arc<AtomicBool>,
        cancel_delay: Duration,
    }

    impl Stream for SlowCancelStream {
        type Item = Result<ProviderEvent, ProviderError>;

        fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            if self.emitted {
                Poll::Pending
            } else {
                self.emitted = true;
                Poll::Ready(Some(Ok(ProviderEvent::TextDelta("partial".to_owned()))))
            }
        }
    }

    impl Drop for SlowCancelStream {
        fn drop(&mut self) {
            std::thread::sleep(self.cancel_delay);
            self.canceled.store(true, Ordering::SeqCst);
        }
    }

    struct CancellationOrderProvider {
        calls: AtomicUsize,
        canceled: Arc<AtomicBool>,
        cancel_delay: Duration,
    }

    struct ReusedProviderCallIdProvider;

    #[async_trait]
    impl Provider for ReusedProviderCallIdProvider {
        fn name(&self) -> &str {
            "reused-provider-call-id"
        }

        async fn stream(
            &self,
            messages: &[Message],
            _tools: &[peakcode_core::ToolSchema],
            _config: &ProviderConfig,
        ) -> Result<ProviderStream, ProviderError> {
            if messages
                .last()
                .is_some_and(|message| message.role == Role::Tool)
            {
                return Ok(stream(vec![ProviderEvent::Finish {
                    reason: FinishReason::Stop,
                }]));
            }
            Ok(stream(vec![
                ProviderEvent::ToolCallStart {
                    id: "reused-provider-id".to_owned(),
                    name: "mutate".to_owned(),
                },
                ProviderEvent::ToolCallArgsDelta {
                    id: "reused-provider-id".to_owned(),
                    delta: "{}".to_owned(),
                },
                ProviderEvent::Finish {
                    reason: FinishReason::ToolUse,
                },
            ]))
        }
    }

    #[async_trait]
    impl Provider for CancellationOrderProvider {
        fn name(&self) -> &str {
            "cancellation-order"
        }

        async fn stream(
            &self,
            messages: &[Message],
            _tools: &[peakcode_core::ToolSchema],
            _config: &ProviderConfig,
        ) -> Result<ProviderStream, ProviderError> {
            match self.calls.fetch_add(1, Ordering::SeqCst) {
                0 => Ok(stream(vec![
                    ProviderEvent::TextDelta("completed".to_owned()),
                    ProviderEvent::Finish {
                        reason: FinishReason::Stop,
                    },
                ])),
                1 => Ok(Box::pin(SlowCancelStream {
                    emitted: false,
                    canceled: Arc::clone(&self.canceled),
                    cancel_delay: self.cancel_delay,
                })),
                _ => {
                    let has_completed = messages
                        .iter()
                        .any(|message| message.text_content() == "completed");
                    let has_canceled_input = messages
                        .iter()
                        .any(|message| message.text_content() == "cancel-me");
                    let cancellation_finished = self.canceled.load(Ordering::SeqCst);
                    let text = if has_completed && !has_canceled_input && cancellation_finished {
                        "history-preserved"
                    } else {
                        "cancellation-or-history-invalid"
                    };
                    Ok(stream(vec![
                        ProviderEvent::TextDelta(text.to_owned()),
                        ProviderEvent::Finish {
                            reason: FinishReason::Stop,
                        },
                    ]))
                }
            }
        }
    }

    struct MutatingTool;

    #[async_trait]
    impl Tool for MutatingTool {
        fn name(&self) -> &str {
            "mutate"
        }

        fn description(&self) -> &str {
            "mutates"
        }

        fn parameters(&self) -> serde_json::Value {
            json!({"type": "object"})
        }

        fn requires_approval(&self) -> Approval {
            Approval::Required
        }

        async fn execute(
            &self,
            _params: serde_json::Value,
            _ctx: &ToolContext,
        ) -> anyhow::Result<ToolOutput> {
            Ok(ToolOutput::ok("changed"))
        }
    }

    fn stream(events: Vec<ProviderEvent>) -> ProviderStream {
        Box::pin(tokio_stream::iter(events.into_iter().map(Ok)))
    }

    fn config() -> Arc<ProviderConfig> {
        Arc::new(ProviderConfig {
            api_key: "test".to_owned(),
            model: "test".to_owned(),
            base_url: None,
            max_tokens: None,
        })
    }

    fn tools(tool: Option<Box<dyn Tool>>) -> Arc<ToolRegistry> {
        let mut registry = ToolRegistry::new();
        if let Some(tool) = tool {
            registry.register(tool);
        }
        Arc::new(registry)
    }

    async fn spawn_worker(
        provider: Arc<dyn Provider>,
        tools: Arc<ToolRegistry>,
    ) -> (
        tokio::net::unix::OwnedReadHalf,
        tokio::net::unix::OwnedWriteHalf,
        tokio::task::JoinHandle<anyhow::Result<()>>,
    ) {
        let (daemon, worker) = UnixStream::pair().unwrap();
        let task = tokio::spawn(run_connection(
            worker,
            provider,
            config(),
            tools,
            Arc::new(ToolContext::new(".")),
            Vec::new(),
        ));
        let (read, write) = daemon.into_split();
        (read, write, task)
    }

    async fn spawn_worker_with_api_key(
        provider: Arc<dyn Provider>,
        tools: Arc<ToolRegistry>,
        api_key: &str,
    ) -> (
        tokio::net::unix::OwnedReadHalf,
        tokio::net::unix::OwnedWriteHalf,
        tokio::task::JoinHandle<anyhow::Result<()>>,
    ) {
        let (daemon, worker) = UnixStream::pair().unwrap();
        let provider_config = Arc::new(ProviderConfig {
            api_key: api_key.to_owned(),
            model: "test".to_owned(),
            base_url: None,
            max_tokens: None,
        });
        let task = tokio::spawn(run_connection(
            worker,
            provider,
            provider_config,
            tools,
            Arc::new(ToolContext::new(".")),
            Vec::new(),
        ));
        let (read, write) = daemon.into_split();
        (read, write, task)
    }

    async fn next_event(reader: &mut BufReader<tokio::net::unix::OwnedReadHalf>) -> WorkerEvent {
        timeout(Duration::from_secs(1), read_frame::<_, WorkerEvent>(reader))
            .await
            .expect("worker event timed out")
            .unwrap()
            .expect("worker closed unexpectedly")
    }

    async fn next_serialized_event(
        reader: &mut BufReader<tokio::net::unix::OwnedReadHalf>,
    ) -> (String, WorkerEvent) {
        let mut line = String::new();
        timeout(Duration::from_secs(1), reader.read_line(&mut line))
            .await
            .expect("worker event timed out")
            .unwrap();
        let event = serde_json::from_str(line.trim_end()).unwrap();
        (line, event)
    }

    async fn spawn_worker_with_held_event() -> (
        tokio::net::unix::OwnedReadHalf,
        tokio::net::unix::OwnedWriteHalf,
        tokio::task::JoinHandle<anyhow::Result<()>>,
    ) {
        let held = Arc::new(tokio::sync::Notify::new());
        let executed = Arc::new(AtomicUsize::new(0));
        let provider = Arc::new(HeldEventProvider {
            calls: AtomicUsize::new(0),
        });
        let tool = BackpressureTool {
            executed: Arc::clone(&executed),
            held: Arc::clone(&held),
        };
        let (read, mut write, task) = spawn_worker(provider, tools(Some(Box::new(tool)))).await;
        write_frame(
            &mut write,
            &DaemonCommand::Input {
                text: "fill-output".to_owned(),
            },
        )
        .await
        .unwrap();
        timeout(Duration::from_secs(1), held.notified())
            .await
            .expect("coordinator did not reach a held ordinary event");
        assert!(executed.load(Ordering::SeqCst) >= 34);
        (read, write, task)
    }

    fn is_held_tool_start(event: &WorkerEvent) -> bool {
        matches!(
            event,
            WorkerEvent::ToolStart { arguments_json, .. }
                if serde_json::from_str::<serde_json::Value>(arguments_json).unwrap()["index"] == 33
        )
    }

    #[tokio::test]
    async fn cancel_preserves_held_event_before_accepting_next_turn() {
        let (read, mut write, task) = spawn_worker_with_held_event().await;

        write_frame(&mut write, &DaemonCommand::Cancel)
            .await
            .unwrap();
        write_frame(
            &mut write,
            &DaemonCommand::Input {
                text: "after-cancel".to_owned(),
            },
        )
        .await
        .unwrap();

        let mut reader = BufReader::new(read);
        let mut saw_held = false;
        loop {
            let event = next_event(&mut reader).await;
            if is_held_tool_start(&event) {
                saw_held = true;
            }
            if matches!(event, WorkerEvent::TextDelta { ref text } if text == "next-turn") {
                assert!(saw_held, "next turn overtook the coordinator-held event");
                break;
            }
        }

        loop {
            if matches!(next_event(&mut reader).await, WorkerEvent::TurnFinished) {
                break;
            }
        }
        write_frame(&mut write, &DaemonCommand::Stop).await.unwrap();
        next_event(&mut reader).await;
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn stop_orders_held_event_before_done_when_peer_drains() {
        let (read, mut write, task) = spawn_worker_with_held_event().await;
        let reader = tokio::spawn(async move {
            let mut reader = BufReader::new(read);
            let mut saw_held = false;
            loop {
                let event = next_event(&mut reader).await;
                if is_held_tool_start(&event) {
                    saw_held = true;
                }
                if matches!(event, WorkerEvent::Done { .. }) {
                    assert!(saw_held, "done overtook the coordinator-held event");
                    break;
                }
            }
        });

        write_frame(&mut write, &DaemonCommand::Stop).await.unwrap();
        reader.await.unwrap();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn overflow_orders_held_event_before_crash_when_peer_drains() {
        let (read, mut write, mut task) = spawn_worker_with_held_event().await;
        let (draining_tx, draining_rx) = tokio::sync::oneshot::channel();
        let reader = tokio::spawn(async move {
            let mut reader = BufReader::new(read);
            let mut saw_held = false;
            let mut draining_tx = Some(draining_tx);
            loop {
                let event = next_event(&mut reader).await;
                if let Some(draining_tx) = draining_tx.take() {
                    let _ = draining_tx.send(());
                }
                if is_held_tool_start(&event) {
                    saw_held = true;
                }
                if matches!(event, WorkerEvent::Crash { .. }) {
                    assert!(saw_held, "crash overtook the coordinator-held event");
                    break;
                }
            }
        });
        draining_rx.await.unwrap();

        for index in 0..=super::MAX_QUEUED_INPUTS {
            write_frame(
                &mut write,
                &DaemonCommand::Input {
                    text: format!("queued-{index}"),
                },
            )
            .await
            .unwrap();
        }
        reader.await.unwrap();
        let result = timeout(Duration::from_secs(1), &mut task)
            .await
            .expect("worker did not terminate after overflow")
            .unwrap();
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn input_streams_events_across_turns_until_stop() {
        let (read, mut write, task) = spawn_worker(Arc::new(StopProvider), tools(None)).await;
        let mut reader = BufReader::new(read);

        write_frame(
            &mut write,
            &DaemonCommand::Input {
                text: "first".to_owned(),
            },
        )
        .await
        .unwrap();
        assert!(
            matches!(next_event(&mut reader).await, WorkerEvent::TextDelta { text } if text == "first")
        );
        assert!(
            matches!(next_event(&mut reader).await, WorkerEvent::AssistantMessage { text } if text == "first")
        );
        assert_eq!(next_event(&mut reader).await, WorkerEvent::TurnFinished);

        write_frame(
            &mut write,
            &DaemonCommand::Input {
                text: "second".to_owned(),
            },
        )
        .await
        .unwrap();
        assert!(
            matches!(next_event(&mut reader).await, WorkerEvent::TextDelta { text } if text == "second")
        );
        assert!(
            matches!(next_event(&mut reader).await, WorkerEvent::AssistantMessage { text } if text == "second")
        );
        assert_eq!(next_event(&mut reader).await, WorkerEvent::TurnFinished);

        write_frame(&mut write, &DaemonCommand::Stop).await.unwrap();
        assert!(
            matches!(next_event(&mut reader).await, WorkerEvent::Done { final_message_count } if final_message_count == 4)
        );
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn approval_command_resumes_exact_pending_tool_call() {
        let provider = Arc::new(ApprovalProvider {
            calls: AtomicUsize::new(0),
        });
        let (read, mut write, task) =
            spawn_worker(provider, tools(Some(Box::new(MutatingTool)))).await;
        let mut reader = BufReader::new(read);

        write_frame(
            &mut write,
            &DaemonCommand::Input {
                text: "change".to_owned(),
            },
        )
        .await
        .unwrap();

        let approval_handle = loop {
            if let WorkerEvent::NeedsApproval {
                call_id,
                tool,
                arguments_json,
            } = next_event(&mut reader).await
            {
                assert_eq!(tool, "mutate");
                assert_eq!(arguments_json, "{}");
                break call_id;
            }
        };
        write_frame(
            &mut write,
            &DaemonCommand::Approve {
                call_id: approval_handle.clone(),
                decision: IpcApprovalDecision::Allow,
            },
        )
        .await
        .unwrap();

        let mut saw_start = false;
        let mut saw_result = false;
        loop {
            match next_event(&mut reader).await {
                WorkerEvent::ToolStart { call_id, .. } if call_id == approval_handle => {
                    saw_start = true;
                }
                WorkerEvent::ToolResult {
                    call_id,
                    content,
                    is_error,
                    ..
                } if call_id == approval_handle && content == "changed" && !is_error => {
                    saw_result = true;
                }
                WorkerEvent::TurnFinished => break,
                _ => {}
            }
        }
        assert!(saw_start);
        assert!(saw_result);

        write_frame(&mut write, &DaemonCommand::Stop).await.unwrap();
        next_event(&mut reader).await;
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn nonapproval_tool_events_share_one_opaque_handle() {
        let provider = Arc::new(NonApprovalProvider {
            calls: AtomicUsize::new(0),
        });
        let (read, mut write, task) =
            spawn_worker(provider, tools(Some(Box::new(NonApprovalTool)))).await;
        let mut reader = BufReader::new(read);
        write_frame(
            &mut write,
            &DaemonCommand::Input {
                text: "inspect".to_owned(),
            },
        )
        .await
        .unwrap();

        let mut start_handle = None;
        let mut result_handle = None;
        loop {
            match next_event(&mut reader).await {
                WorkerEvent::NeedsApproval { .. } => {
                    panic!("nonapproval tool emitted an approval request")
                }
                WorkerEvent::ToolStart { call_id, .. } => start_handle = Some(call_id),
                WorkerEvent::ToolResult { call_id, .. } => result_handle = Some(call_id),
                WorkerEvent::TurnFinished => break,
                _ => {}
            }
        }
        let start_handle = start_handle.expect("missing tool_start");
        assert_ne!(start_handle, "raw-provider-id");
        assert_eq!(result_handle.as_deref(), Some(start_handle.as_str()));

        write_frame(&mut write, &DaemonCommand::Stop).await.unwrap();
        next_event(&mut reader).await;
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn cancel_aborts_only_active_turn_and_worker_accepts_later_input() {
        let provider = Arc::new(CancelProvider {
            calls: AtomicUsize::new(0),
        });
        let (read, mut write, task) = spawn_worker(provider, tools(None)).await;
        let mut reader = BufReader::new(read);

        write_frame(
            &mut write,
            &DaemonCommand::Input {
                text: "slow".to_owned(),
            },
        )
        .await
        .unwrap();
        assert!(
            matches!(next_event(&mut reader).await, WorkerEvent::TextDelta { text } if text == "started")
        );
        write_frame(&mut write, &DaemonCommand::Cancel)
            .await
            .unwrap();
        write_frame(
            &mut write,
            &DaemonCommand::Input {
                text: "again".to_owned(),
            },
        )
        .await
        .unwrap();

        loop {
            if matches!(next_event(&mut reader).await, WorkerEvent::TurnFinished) {
                break;
            }
        }
        write_frame(&mut write, &DaemonCommand::Stop).await.unwrap();
        next_event(&mut reader).await;
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn fragmented_command_survives_concurrent_outbound_event() {
        let provider = Arc::new(FragmentProvider {
            calls: AtomicUsize::new(0),
        });
        let (read, mut write, task) = spawn_worker(provider, tools(None)).await;
        let mut reader = BufReader::new(read);

        write_frame(
            &mut write,
            &DaemonCommand::Input {
                text: "slow".to_owned(),
            },
        )
        .await
        .unwrap();
        assert!(
            matches!(next_event(&mut reader).await, WorkerEvent::TextDelta { text } if text == "before")
        );

        write.write_all(br#"{"kind":"can"#).await.unwrap();
        assert!(
            matches!(next_event(&mut reader).await, WorkerEvent::TextDelta { text } if text == "during")
        );
        write.write_all(b"cel\"}\n").await.unwrap();
        write_frame(
            &mut write,
            &DaemonCommand::Input {
                text: "again".to_owned(),
            },
        )
        .await
        .unwrap();

        loop {
            if matches!(next_event(&mut reader).await, WorkerEvent::TurnFinished) {
                break;
            }
        }
        write_frame(&mut write, &DaemonCommand::Stop).await.unwrap();
        next_event(&mut reader).await;
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn stop_exits_within_deadline_when_daemon_does_not_drain_events() {
        let (_undrained_read, mut write, mut task) = spawn_worker_with_held_event().await;
        write_frame(&mut write, &DaemonCommand::Stop).await.unwrap();

        if timeout(Duration::from_secs(1), &mut task).await.is_err() {
            task.abort();
            panic!("worker did not stop while outbound socket was backpressured");
        }
    }

    #[tokio::test]
    async fn provider_error_api_key_is_redacted_from_serialized_crash() {
        let secret = "sk-provider-secret-value";
        let provider = Arc::new(SecretErrorProvider {
            secret: secret.to_owned(),
        });
        let (read, mut write, task) =
            spawn_worker_with_api_key(provider, tools(None), secret).await;
        let mut reader = BufReader::new(read);

        write_frame(
            &mut write,
            &DaemonCommand::Input {
                text: "fail".to_owned(),
            },
        )
        .await
        .unwrap();

        loop {
            let (line, event) = next_serialized_event(&mut reader).await;
            assert!(!line.contains(secret), "serialized frame leaked API key");
            if matches!(event, WorkerEvent::Crash { .. }) {
                assert!(line.contains("[REDACTED]"));
                break;
            }
        }
        let error = task.await.unwrap().unwrap_err().to_string();
        assert!(!error.contains(secret), "worker error leaked API key");
    }

    #[tokio::test]
    async fn tool_arguments_and_output_are_redacted_before_serialization() {
        let secret = "sk-tool-secret-value";
        let provider = Arc::new(SecretToolProvider {
            calls: AtomicUsize::new(0),
            secret: secret.to_owned(),
        });
        let tool = SecretOutputTool {
            secret: secret.to_owned(),
        };
        let (read, mut write, task) =
            spawn_worker_with_api_key(provider, tools(Some(Box::new(tool))), secret).await;
        let mut reader = BufReader::new(read);

        write_frame(
            &mut write,
            &DaemonCommand::Input {
                text: "use secret".to_owned(),
            },
        )
        .await
        .unwrap();

        let mut approved = false;
        let mut saw_tool_start = false;
        let mut saw_tool_result = false;
        loop {
            let (line, event) = next_serialized_event(&mut reader).await;
            assert!(!line.contains(secret), "serialized frame leaked API key");
            match event {
                WorkerEvent::NeedsApproval { call_id, .. } => {
                    assert!(line.contains("[REDACTED]"));
                    write_frame(
                        &mut write,
                        &DaemonCommand::Approve {
                            call_id,
                            decision: IpcApprovalDecision::Allow,
                        },
                    )
                    .await
                    .unwrap();
                    approved = true;
                }
                WorkerEvent::ToolStart { .. } => {
                    assert!(line.contains("[REDACTED]"));
                    saw_tool_start = true;
                }
                WorkerEvent::ToolResult { .. } => {
                    assert!(line.contains("[REDACTED]"));
                    saw_tool_result = true;
                }
                WorkerEvent::TurnFinished => break,
                _ => {}
            }
        }
        assert!(approved && saw_tool_start && saw_tool_result);

        write_frame(&mut write, &DaemonCommand::Stop).await.unwrap();
        next_serialized_event(&mut reader).await;
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn allow_all_persists_across_worker_turns_for_only_approved_tool() {
        let provider = Arc::new(CrossTurnApprovalProvider {
            calls: AtomicUsize::new(0),
        });
        let mut registry = ToolRegistry::new();
        registry.register(Box::new(MutatingTool));
        registry.register(Box::new(OtherTool));
        let (read, mut write, task) = spawn_worker(provider, Arc::new(registry)).await;
        let mut reader = BufReader::new(read);

        write_frame(
            &mut write,
            &DaemonCommand::Input {
                text: "first".to_owned(),
            },
        )
        .await
        .unwrap();
        loop {
            match next_event(&mut reader).await {
                WorkerEvent::NeedsApproval { call_id, tool, .. } if tool == "mutate" => {
                    write_frame(
                        &mut write,
                        &DaemonCommand::Approve {
                            call_id,
                            decision: IpcApprovalDecision::AllowAll,
                        },
                    )
                    .await
                    .unwrap();
                }
                WorkerEvent::TurnFinished => break,
                _ => {}
            }
        }

        write_frame(
            &mut write,
            &DaemonCommand::Input {
                text: "second".to_owned(),
            },
        )
        .await
        .unwrap();
        loop {
            match next_event(&mut reader).await {
                WorkerEvent::NeedsApproval { tool, .. } => {
                    panic!("same tool requested approval after AllowAll: {tool}")
                }
                WorkerEvent::TurnFinished => break,
                _ => {}
            }
        }

        write_frame(
            &mut write,
            &DaemonCommand::Input {
                text: "third".to_owned(),
            },
        )
        .await
        .unwrap();
        let mut saw_other_approval = false;
        loop {
            match next_event(&mut reader).await {
                WorkerEvent::NeedsApproval { call_id, tool, .. } if tool == "other" => {
                    saw_other_approval = true;
                    write_frame(
                        &mut write,
                        &DaemonCommand::Approve {
                            call_id,
                            decision: IpcApprovalDecision::Deny,
                        },
                    )
                    .await
                    .unwrap();
                }
                WorkerEvent::TurnFinished => break,
                _ => {}
            }
        }
        assert!(saw_other_approval);

        write_frame(&mut write, &DaemonCommand::Stop).await.unwrap();
        next_event(&mut reader).await;
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn queued_input_overflow_reports_error_and_terminates_worker() {
        let provider = Arc::new(CancelProvider {
            calls: AtomicUsize::new(0),
        });
        let (read, mut write, mut task) = spawn_worker(provider, tools(None)).await;
        let mut reader = BufReader::new(read);

        write_frame(
            &mut write,
            &DaemonCommand::Input {
                text: "blocked".to_owned(),
            },
        )
        .await
        .unwrap();
        assert!(
            matches!(next_event(&mut reader).await, WorkerEvent::TextDelta { text } if text == "started")
        );

        for index in 0..=super::MAX_QUEUED_INPUTS {
            write_frame(
                &mut write,
                &DaemonCommand::Input {
                    text: format!("queued-{index}"),
                },
            )
            .await
            .unwrap();
        }

        let event = next_event(&mut reader).await;
        assert!(
            matches!(event, WorkerEvent::Crash { message } if message.contains("input queue limit"))
        );
        let result = timeout(Duration::from_secs(1), &mut task)
            .await
            .expect("worker did not terminate after input queue overflow")
            .unwrap();
        assert!(result.is_err());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn cancel_waits_for_completion_and_preserves_last_completed_history() {
        let canceled = Arc::new(AtomicBool::new(false));
        let provider = Arc::new(CancellationOrderProvider {
            calls: AtomicUsize::new(0),
            canceled: Arc::clone(&canceled),
            cancel_delay: Duration::from_millis(10),
        });
        let (read, mut write, task) = spawn_worker(provider, tools(None)).await;
        let mut reader = BufReader::new(read);

        write_frame(
            &mut write,
            &DaemonCommand::Input {
                text: "first".to_owned(),
            },
        )
        .await
        .unwrap();
        loop {
            if matches!(next_event(&mut reader).await, WorkerEvent::TurnFinished) {
                break;
            }
        }

        write_frame(
            &mut write,
            &DaemonCommand::Input {
                text: "cancel-me".to_owned(),
            },
        )
        .await
        .unwrap();
        assert!(
            matches!(next_event(&mut reader).await, WorkerEvent::TextDelta { text } if text == "partial")
        );
        write_frame(&mut write, &DaemonCommand::Cancel)
            .await
            .unwrap();
        write_frame(
            &mut write,
            &DaemonCommand::Input {
                text: "after-cancel".to_owned(),
            },
        )
        .await
        .unwrap();

        let next = next_event(&mut reader).await;
        assert!(matches!(next, WorkerEvent::TextDelta { text } if text == "history-preserved"));
        assert!(canceled.load(Ordering::SeqCst));

        loop {
            if matches!(next_event(&mut reader).await, WorkerEvent::TurnFinished) {
                break;
            }
        }
        write_frame(&mut write, &DaemonCommand::Stop).await.unwrap();
        next_event(&mut reader).await;
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn cancel_completes_active_task_before_clearing_approval_waiters() {
        struct CancellationGuard(Arc<AtomicBool>);

        impl Drop for CancellationGuard {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }

        let active_canceled = Arc::new(AtomicBool::new(false));
        let started = Arc::new(tokio::sync::Notify::new());
        let handle = tokio::spawn({
            let active_canceled = Arc::clone(&active_canceled);
            let started = Arc::clone(&started);
            async move {
                let _guard = CancellationGuard(active_canceled);
                started.notify_one();
                std::future::pending::<()>().await;
                Ok(Vec::new())
            }
        });
        started.notified().await;
        let (_agent_events_tx, agent_events) = mpsc::channel(1);
        let turn = ActiveTurn {
            events: agent_events,
            handle,
        };

        let (approval_events_tx, mut approval_events_rx) = mpsc::channel(1);
        let approver = Arc::new(RemoteApprover::new(approval_events_tx));
        let (observed_tx, observed_rx) = tokio::sync::oneshot::channel();
        let approval = tokio::spawn({
            let approver = Arc::clone(&approver);
            let active_canceled = Arc::clone(&active_canceled);
            async move {
                let _ = approver
                    .approve(ApprovalRequest {
                        tool: "bash".to_owned(),
                        arguments: json!({}),
                        call_id: "pending".to_owned(),
                    })
                    .await;
                let _ = observed_tx.send(active_canceled.load(Ordering::SeqCst));
            }
        });
        approval_events_rx.recv().await.unwrap();

        cancel_active_turn(turn, &approver).await.unwrap();

        assert!(observed_rx.await.unwrap());
        approval.await.unwrap();
    }

    #[tokio::test]
    async fn stale_approval_handle_cannot_authorize_reused_provider_call_id() {
        let (read, mut write, task) = spawn_worker(
            Arc::new(ReusedProviderCallIdProvider),
            tools(Some(Box::new(MutatingTool))),
        )
        .await;
        let mut reader = BufReader::new(read);

        write_frame(
            &mut write,
            &DaemonCommand::Input {
                text: "generation-a".to_owned(),
            },
        )
        .await
        .unwrap();
        let handle_a = loop {
            if let WorkerEvent::NeedsApproval { call_id, .. } = next_event(&mut reader).await {
                break call_id;
            }
        };
        write_frame(&mut write, &DaemonCommand::Cancel)
            .await
            .unwrap();

        write_frame(
            &mut write,
            &DaemonCommand::Input {
                text: "generation-b".to_owned(),
            },
        )
        .await
        .unwrap();
        let handle_b = loop {
            if let WorkerEvent::NeedsApproval { call_id, .. } = next_event(&mut reader).await {
                break call_id;
            }
        };
        assert_ne!(handle_a, handle_b);

        write_frame(
            &mut write,
            &DaemonCommand::Approve {
                call_id: handle_a,
                decision: IpcApprovalDecision::Allow,
            },
        )
        .await
        .unwrap();
        assert!(timeout(Duration::from_millis(50), async {
            loop {
                if matches!(
                    next_event(&mut reader).await,
                    WorkerEvent::ToolStart { .. } | WorkerEvent::ToolResult { .. }
                ) {
                    break;
                }
            }
        })
        .await
        .is_err());

        write_frame(
            &mut write,
            &DaemonCommand::Approve {
                call_id: handle_b.clone(),
                decision: IpcApprovalDecision::Allow,
            },
        )
        .await
        .unwrap();
        let mut saw_start = false;
        let mut saw_result = false;
        loop {
            match next_event(&mut reader).await {
                WorkerEvent::ToolStart { call_id, .. } => {
                    assert_eq!(call_id, handle_b);
                    saw_start = true;
                }
                WorkerEvent::ToolResult { call_id, .. } => {
                    assert_eq!(call_id, handle_b);
                    saw_result = true;
                }
                WorkerEvent::TurnFinished => break,
                _ => {}
            }
        }
        assert!(saw_start && saw_result);

        write_frame(&mut write, &DaemonCommand::Stop).await.unwrap();
        next_event(&mut reader).await;
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn assistant_message_precedes_approval_and_tool_start_follows_decision() {
        let provider = Arc::new(ApprovalProvider {
            calls: AtomicUsize::new(0),
        });
        let (read, mut write, task) =
            spawn_worker(provider, tools(Some(Box::new(MutatingTool)))).await;
        let mut reader = BufReader::new(read);

        write_frame(
            &mut write,
            &DaemonCommand::Input {
                text: "ordered".to_owned(),
            },
        )
        .await
        .unwrap();

        let mut kinds = Vec::new();
        let approval_handle = loop {
            match next_event(&mut reader).await {
                WorkerEvent::AssistantMessage { .. } => kinds.push("assistant"),
                WorkerEvent::NeedsApproval { call_id, .. } => {
                    kinds.push("approval");
                    break call_id;
                }
                WorkerEvent::ToolStart { .. } => panic!("tool started before approval decision"),
                _ => {}
            }
        };
        assert_eq!(kinds, ["assistant", "approval"]);

        write_frame(
            &mut write,
            &DaemonCommand::Approve {
                call_id: approval_handle.clone(),
                decision: IpcApprovalDecision::Allow,
            },
        )
        .await
        .unwrap();
        loop {
            if let WorkerEvent::ToolStart { call_id, .. } = next_event(&mut reader).await {
                assert_eq!(call_id, approval_handle);
                break;
            }
        }

        loop {
            if matches!(next_event(&mut reader).await, WorkerEvent::TurnFinished) {
                break;
            }
        }
        write_frame(&mut write, &DaemonCommand::Stop).await.unwrap();
        next_event(&mut reader).await;
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn approval_barrier_preserves_order_under_writer_backpressure() {
        let held = Arc::new(tokio::sync::Notify::new());
        let provider = Arc::new(BackpressuredApprovalProvider {
            calls: AtomicUsize::new(0),
        });
        let mut registry = ToolRegistry::new();
        registry.register(Box::new(BackpressureTool {
            executed: Arc::new(AtomicUsize::new(0)),
            held: Arc::clone(&held),
        }));
        registry.register(Box::new(MutatingTool));
        let (read, mut write, task) = spawn_worker(provider, Arc::new(registry)).await;
        write_frame(
            &mut write,
            &DaemonCommand::Input {
                text: "pressure approval".to_owned(),
            },
        )
        .await
        .unwrap();
        timeout(Duration::from_secs(1), held.notified())
            .await
            .expect("writer did not become backpressured");

        let mut reader = BufReader::new(read);
        let mut prior_results = 0;
        let approval_handle = loop {
            match next_event(&mut reader).await {
                WorkerEvent::ToolResult { name, .. } if name == "backpressure" => {
                    prior_results += 1;
                }
                WorkerEvent::NeedsApproval { call_id, tool, .. } => {
                    assert_eq!(tool, "mutate");
                    assert_eq!(prior_results, 127);
                    break call_id;
                }
                WorkerEvent::ToolStart { name, .. } if name == "mutate" => {
                    panic!("approval tool started before its approval request")
                }
                _ => {}
            }
        };

        write_frame(
            &mut write,
            &DaemonCommand::Approve {
                call_id: approval_handle.clone(),
                decision: IpcApprovalDecision::Allow,
            },
        )
        .await
        .unwrap();
        loop {
            if let WorkerEvent::ToolStart { call_id, name, .. } = next_event(&mut reader).await {
                if name == "mutate" {
                    assert_eq!(call_id, approval_handle);
                    break;
                }
            }
        }
        loop {
            if matches!(next_event(&mut reader).await, WorkerEvent::TurnFinished) {
                break;
            }
        }
        write_frame(&mut write, &DaemonCommand::Stop).await.unwrap();
        next_event(&mut reader).await;
        task.await.unwrap().unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn cancel_timeout_reports_crash_and_terminates_worker() {
        let canceled = Arc::new(AtomicBool::new(false));
        let provider = Arc::new(CancellationOrderProvider {
            calls: AtomicUsize::new(0),
            canceled,
            cancel_delay: Duration::from_millis(500),
        });
        let (read, mut write, mut task) = spawn_worker(provider, tools(None)).await;
        let mut reader = BufReader::new(read);

        write_frame(
            &mut write,
            &DaemonCommand::Input {
                text: "completed-turn".to_owned(),
            },
        )
        .await
        .unwrap();
        loop {
            if matches!(next_event(&mut reader).await, WorkerEvent::TurnFinished) {
                break;
            }
        }

        write_frame(
            &mut write,
            &DaemonCommand::Input {
                text: "blocking-cancel".to_owned(),
            },
        )
        .await
        .unwrap();
        assert!(
            matches!(next_event(&mut reader).await, WorkerEvent::TextDelta { text } if text == "partial")
        );
        write_frame(&mut write, &DaemonCommand::Cancel)
            .await
            .unwrap();

        let event = timeout(Duration::from_millis(450), next_event(&mut reader))
            .await
            .expect("cancel timeout did not produce a bounded terminal event");
        assert!(
            matches!(event, WorkerEvent::Crash { message } if message.contains("cancellation cleanup deadline"))
        );
        let result = timeout(Duration::from_millis(450), &mut task)
            .await
            .expect("worker did not terminate after cancellation cleanup deadline")
            .unwrap();
        assert!(result.is_err());
    }
}
