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

use crate::ipc::{read_frame, write_frame, DaemonCommand, WorkerEvent};
use crate::redaction::Redactor;
use crate::remote_approver::RemoteApprover;

const WORKER_EVENT_CHANNEL_CAPACITY: usize = 64;
const WORKER_COMMAND_CHANNEL_CAPACITY: usize = 64;
const WORKER_SHUTDOWN_TIMEOUT: Duration = Duration::from_millis(250);
/// Maximum user inputs retained behind one active agent turn.
pub const MAX_QUEUED_INPUTS: usize = 32;

struct ActiveTurn {
    events: mpsc::UnboundedReceiver<AgentEvent>,
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
    let writer_redactor = Arc::clone(&redactor);
    let writer = tokio::spawn(async move {
        while let Some(event) = event_rx.recv().await {
            write_frame(&mut write_half, &writer_redactor.redact_event(event)).await?;
        }
        Ok::<(), std::io::Error>(())
    });
    let approver = Arc::new(RemoteApprover::with_redactor(event_tx.clone(), redactor));
    let mut active: Option<ActiveTurn> = None;
    let mut queued_inputs = VecDeque::new();
    let mut pending_event = None;

    let result = 'worker: loop {
        if active.is_none() {
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
                        send_done(&event_tx, messages.len()).await;
                        break 'worker Ok(());
                    }
                },
                Some(Err(error)) => break 'worker Err(error.into()),
                None => break 'worker Ok(()),
            }
            continue;
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
                            send_terminal_event(
                                &event_tx,
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
                        if let Some(turn) = active.take() {
                            cancel_active_turn(turn, &approver).await;
                        } else {
                            approver.cancel_pending().await;
                        }
                        pending_event = None;
                    }
                    Some(Ok(DaemonCommand::Stop)) => {
                        if let Some(turn) = active.take() {
                            abort_turn(turn).await;
                        }
                        approver.cancel_pending().await;
                        send_done(&event_tx, messages.len()).await;
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
            event = turn.events.recv(), if pending_event.is_none() => {
                match event {
                    Some(event) => pending_event = Some(map_agent_event(event)),
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
                    }
                }
            }
        }
    };

    if let Some(turn) = active {
        abort_turn(turn).await;
    }
    approver.cancel_pending().await;
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

async fn send_done(events: &mpsc::Sender<WorkerEvent>, final_message_count: usize) {
    send_terminal_event(
        events,
        WorkerEvent::Done {
            final_message_count,
        },
    )
    .await;
}

async fn send_terminal_event(events: &mpsc::Sender<WorkerEvent>, event: WorkerEvent) {
    let _ = timeout(WORKER_SHUTDOWN_TIMEOUT, events.send(event)).await;
}

async fn abort_turn(mut turn: ActiveTurn) {
    turn.handle.abort();
    let _ = timeout(WORKER_SHUTDOWN_TIMEOUT, &mut turn.handle).await;
}

async fn cancel_active_turn(turn: ActiveTurn, approver: &RemoteApprover) {
    turn.handle.abort();
    let _ = turn.handle.await;
    approver.cancel_pending().await;
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

fn map_agent_event(event: AgentEvent) -> WorkerEvent {
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
            call_id,
            name,
            arguments_json: arguments.to_string(),
        },
        AgentEvent::ToolResult {
            call_id,
            name,
            output,
        } => WorkerEvent::ToolResult {
            call_id,
            name,
            content: output.content,
            is_error: output.is_error,
        },
        AgentEvent::TurnFinished => WorkerEvent::TurnFinished,
    }
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

    struct FloodProvider {
        flooded: Arc<tokio::sync::Notify>,
    }

    #[async_trait]
    impl Provider for FloodProvider {
        fn name(&self) -> &str {
            "flood"
        }

        async fn stream(
            &self,
            _messages: &[Message],
            _tools: &[peakcode_core::ToolSchema],
            _config: &ProviderConfig,
        ) -> Result<ProviderStream, ProviderError> {
            let (tx, rx) = mpsc::unbounded_channel();
            let flooded = Arc::clone(&self.flooded);
            tokio::spawn(async move {
                let payload = "x".repeat(256 * 1024);
                for _ in 0..96 {
                    let _ = tx.send(Ok(ProviderEvent::TextDelta(payload.clone())));
                }
                flooded.notify_one();
                tokio::time::sleep(Duration::from_secs(30)).await;
            });
            Ok(Box::pin(UnboundedReceiverStream::new(rx)))
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
            std::thread::sleep(Duration::from_millis(500));
            self.canceled.store(true, Ordering::SeqCst);
        }
    }

    struct CancellationOrderProvider {
        calls: AtomicUsize,
        canceled: Arc<AtomicBool>,
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

        loop {
            if matches!(next_event(&mut reader).await, WorkerEvent::NeedsApproval { call_id, tool, arguments_json } if call_id == "tool-1" && tool == "mutate" && arguments_json == "{}")
            {
                break;
            }
        }
        write_frame(
            &mut write,
            &DaemonCommand::Approve {
                call_id: "tool-1".to_owned(),
                decision: IpcApprovalDecision::Allow,
            },
        )
        .await
        .unwrap();

        let mut saw_start = false;
        let mut saw_result = false;
        loop {
            match next_event(&mut reader).await {
                WorkerEvent::ToolStart { call_id, .. } if call_id == "tool-1" => {
                    saw_start = true;
                }
                WorkerEvent::ToolResult {
                    call_id,
                    content,
                    is_error,
                    ..
                } if call_id == "tool-1" && content == "changed" && !is_error => {
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
        let flooded = Arc::new(tokio::sync::Notify::new());
        let provider = Arc::new(FloodProvider {
            flooded: Arc::clone(&flooded),
        });
        let (_undrained_read, mut write, mut task) = spawn_worker(provider, tools(None)).await;

        write_frame(
            &mut write,
            &DaemonCommand::Input {
                text: "flood".to_owned(),
            },
        )
        .await
        .unwrap();
        timeout(Duration::from_secs(1), flooded.notified())
            .await
            .expect("provider did not flood events");
        tokio::time::sleep(Duration::from_millis(100)).await;
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
        let (_agent_events_tx, agent_events) = mpsc::unbounded_channel();
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

        cancel_active_turn(turn, &approver).await;

        assert!(observed_rx.await.unwrap());
        approval.await.unwrap();
    }
}
