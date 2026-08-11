use std::collections::VecDeque;
use std::sync::Arc;

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

use crate::ipc::{read_frame, write_frame, DaemonCommand, WorkerEvent};
use crate::remote_approver::RemoteApprover;

const WORKER_EVENT_CHANNEL_CAPACITY: usize = 64;
const WORKER_COMMAND_CHANNEL_CAPACITY: usize = 64;

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
    let writer = tokio::spawn(async move {
        while let Some(event) = event_rx.recv().await {
            write_frame(&mut write_half, &event).await?;
        }
        Ok::<(), std::io::Error>(())
    });
    let approver = Arc::new(RemoteApprover::new(event_tx.clone()));
    let mut active: Option<ActiveTurn> = None;
    let mut queued_inputs = VecDeque::new();

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
                            tracing::warn!(call_id, "approval did not match a pending request");
                        }
                    }
                    DaemonCommand::Cancel => {}
                    DaemonCommand::Stop => {
                        if event_tx
                            .send(WorkerEvent::Done {
                                final_message_count: messages.len(),
                            })
                            .await
                            .is_err()
                        {
                            break 'worker Err(anyhow!("worker event writer closed"));
                        }
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
                    Some(Ok(DaemonCommand::Input { text })) => queued_inputs.push_back(text),
                    Some(Ok(DaemonCommand::Approve { call_id, decision })) => {
                        if !approver.resolve(&call_id, decision).await {
                            tracing::warn!(call_id, "approval did not match a pending request");
                        }
                    }
                    Some(Ok(DaemonCommand::Cancel)) => {
                        approver.cancel_pending().await;
                        if let Some(turn) = active.take() {
                            turn.handle.abort();
                            let _ = turn.handle.await;
                        }
                    }
                    Some(Ok(DaemonCommand::Stop)) => {
                        approver.cancel_pending().await;
                        if let Some(turn) = active.take() {
                            turn.handle.abort();
                            let _ = turn.handle.await;
                        }
                        if event_tx.send(WorkerEvent::Done { final_message_count: messages.len() }).await.is_err() {
                            break 'worker Err(anyhow!("worker event writer closed"));
                        }
                        break 'worker Ok(());
                    }
                    Some(Err(error)) => break 'worker Err(error.into()),
                    None => break 'worker Ok(()),
                }
            }
            event = turn.events.recv() => {
                match event {
                    Some(event) => {
                        if event_tx.send(map_agent_event(event)).await.is_err() {
                            break 'worker Err(anyhow!("worker event writer closed"));
                        }
                    }
                    None => {
                        let turn = active.take().expect("active turn was checked");
                        match turn.handle.await {
                            Ok(Ok(final_messages)) => messages = final_messages,
                            Ok(Err(error)) => {
                                let message = error.to_string();
                                let _ = event_tx.send(WorkerEvent::Crash { message }).await;
                                break 'worker Err(error);
                            }
                            Err(error) => break 'worker Err(error.into()),
                        }
                    }
                }
            }
        }
    };

    approver.cancel_pending().await;
    if let Some(turn) = active {
        turn.handle.abort();
        let _ = turn.handle.await;
    }
    command_reader.abort();
    let command_reader_result = command_reader.await;
    drop(approver);
    drop(event_tx);
    let writer_result = writer
        .await
        .map_err(|error| anyhow!("worker event writer task failed: {error}"))?;

    result?;
    if let Err(error) = command_reader_result {
        if !error.is_cancelled() {
            return Err(anyhow!("worker command reader task failed: {error}"));
        }
    }
    writer_result?;
    Ok(())
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
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use async_trait::async_trait;
    use peakcode_core::{
        Approval, FinishReason, Message, Provider, ProviderConfig, ProviderError, ProviderEvent,
        ProviderStream, Role, Tool, ToolContext, ToolOutput, ToolRegistry,
    };
    use serde_json::json;
    use tokio::io::{AsyncWriteExt, BufReader};
    use tokio::net::UnixStream;
    use tokio::sync::mpsc;
    use tokio::time::timeout;
    use tokio_stream::wrappers::UnboundedReceiverStream;

    use super::run_connection;
    use crate::ipc::{read_frame, write_frame, DaemonCommand, IpcApprovalDecision, WorkerEvent};

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

    async fn next_event(reader: &mut BufReader<tokio::net::unix::OwnedReadHalf>) -> WorkerEvent {
        timeout(Duration::from_secs(1), read_frame::<_, WorkerEvent>(reader))
            .await
            .expect("worker event timed out")
            .unwrap()
            .expect("worker closed unexpectedly")
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
}
