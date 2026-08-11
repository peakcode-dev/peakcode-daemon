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

struct CappedBuffer {
    bytes: Vec<u8>,
    limit: usize,
    peak_len: usize,
    peak_capacity: usize,
}

impl CappedBuffer {
    fn new(limit: usize) -> Self {
        let bytes = Vec::with_capacity(limit);
        let peak_capacity = bytes.capacity();
        Self {
            bytes,
            limit,
            peak_len: 0,
            peak_capacity,
        }
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.bytes.len()
    }

    #[cfg(test)]
    fn peak_len(&self) -> usize {
        self.peak_len
    }

    #[cfg(test)]
    fn peak_capacity(&self) -> usize {
        self.peak_capacity
    }

    fn into_inner(self) -> Vec<u8> {
        self.bytes
    }
}

impl io::Write for CappedBuffer {
    fn write(&mut self, input: &[u8]) -> io::Result<usize> {
        if input.len() > self.limit - self.bytes.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "IPC frame exceeds maximum size",
            ));
        }
        self.bytes.extend_from_slice(input);
        self.peak_len = self.peak_len.max(self.bytes.len());
        self.peak_capacity = self.peak_capacity.max(self.bytes.capacity());
        Ok(input.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub async fn write_frame<W: AsyncWrite + Unpin>(
    w: &mut W,
    frame: &impl Serialize,
) -> io::Result<()> {
    let mut line = CappedBuffer::new(MAX_IPC_FRAME_BYTES);
    serde_json::to_writer(&mut line, frame)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    let line = line.into_inner();
    w.write_all(&line).await?;
    w.write_all(b"\n").await?;
    w.flush().await
}

pub async fn read_frame<R: AsyncBufRead + Unpin, T: DeserializeOwned>(
    r: &mut R,
) -> io::Result<Option<T>> {
    let mut payload = Vec::new();
    loop {
        let available = r.fill_buf().await?;
        if available.is_empty() {
            if payload.is_empty() {
                return Ok(None);
            }
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "IPC frame missing newline delimiter",
            ));
        }

        if let Some(pos) = available.iter().position(|byte| *byte == b'\n') {
            if pos > MAX_IPC_FRAME_BYTES - payload.len() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "IPC frame exceeds maximum size",
                ));
            }
            payload.extend_from_slice(&available[..pos]);
            r.consume(pos + 1);
            break;
        }

        let available_len = available.len();
        if available_len > MAX_IPC_FRAME_BYTES - payload.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "IPC frame exceeds maximum size",
            ));
        }
        payload.extend_from_slice(available);
        r.consume(available_len);
    }

    let frame = serde_json::from_slice(&payload)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    Ok(Some(frame))
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use tokio::io::AsyncWrite;

    use super::{write_frame, CappedBuffer, MAX_IPC_FRAME_BYTES};

    #[derive(Default)]
    struct ObservedWriter {
        largest_write: usize,
        total_written: usize,
    }

    impl AsyncWrite for ObservedWriter {
        fn poll_write(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            input: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            self.largest_write = self.largest_write.max(input.len());
            self.total_written += input.len();
            Poll::Ready(Ok(input.len()))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[test]
    fn oversized_serialization_never_grows_buffer_past_frame_limit() {
        let value = "x".repeat(MAX_IPC_FRAME_BYTES * 4);
        let mut buffer = CappedBuffer::new(MAX_IPC_FRAME_BYTES);

        let result = serde_json::to_writer(&mut buffer, &value);

        assert!(result.is_err());
        assert!(buffer.len() <= MAX_IPC_FRAME_BYTES);
        assert!(buffer.peak_len() <= MAX_IPC_FRAME_BYTES);
        assert!(buffer.peak_capacity() <= MAX_IPC_FRAME_BYTES);
        assert!(buffer
            .write_all(&vec![b'x'; MAX_IPC_FRAME_BYTES + 1])
            .is_err());
        assert!(buffer.len() <= MAX_IPC_FRAME_BYTES);
    }

    #[tokio::test]
    async fn exact_max_frame_writes_line_feed_without_growing_payload() {
        let value = "x".repeat(MAX_IPC_FRAME_BYTES - 2);
        let mut writer = ObservedWriter::default();

        write_frame(&mut writer, &value).await.unwrap();

        assert_eq!(writer.total_written, MAX_IPC_FRAME_BYTES + 1);
        assert!(writer.largest_write <= MAX_IPC_FRAME_BYTES);
    }
}
