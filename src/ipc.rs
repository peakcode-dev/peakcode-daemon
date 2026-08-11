use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::io;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt};

pub const MAX_IPC_FRAME_BYTES: usize = 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IpcApprovalDecision {
    Allow,
    Deny,
    AllowAll,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WorkerEvent {
    TextDelta {
        text: String,
    },
    AssistantMessage {
        text: String,
    },
    ToolStart {
        call_id: String,
        name: String,
        arguments_json: String,
    },
    ToolResult {
        call_id: String,
        name: String,
        content: String,
        is_error: bool,
    },
    NeedsApproval {
        call_id: String,
        tool: String,
        arguments_json: String,
    },
    TurnFinished,
    Done {
        final_message_count: usize,
    },
    Crash {
        message: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DaemonCommand {
    /// Forward a user turn to the worker so gRPC `SendInput` can reach it.
    Input {
        text: String,
    },
    Approve {
        call_id: String,
        decision: IpcApprovalDecision,
    },
    Cancel,
    Stop,
}

pub async fn write_frame<W: AsyncWrite + Unpin>(
    w: &mut W,
    frame: &impl Serialize,
) -> io::Result<()> {
    let mut line =
        serde_json::to_vec(frame).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    if line.len() > MAX_IPC_FRAME_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "IPC frame exceeds maximum size",
        ));
    }
    line.push(b'\n');
    w.write_all(&line).await?;
    w.flush().await
}

pub async fn read_frame<R: AsyncBufRead + Unpin, T: DeserializeOwned>(
    r: &mut R,
) -> io::Result<Option<T>> {
    let mut buf = String::new();
    let n = r.read_line(&mut buf).await?;
    if n == 0 {
        return Ok(None);
    }
    if buf.len() > MAX_IPC_FRAME_BYTES + 1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "IPC frame exceeds maximum size",
        ));
    }
    let frame = serde_json::from_str(buf.trim_end())
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    Ok(Some(frame))
}
