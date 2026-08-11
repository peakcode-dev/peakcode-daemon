use std::io;

use peakcode_daemon::ipc::{
    read_frame, write_frame, DaemonCommand, IpcApprovalDecision, WorkerEvent, MAX_IPC_FRAME_BYTES,
};
use tokio::io::{duplex, AsyncWriteExt, BufReader};

#[tokio::test]
async fn worker_text_delta_roundtrips() {
    let expected = WorkerEvent::TextDelta {
        text: "hello".to_owned(),
    };
    let (mut writer, reader) = duplex(4096);

    write_frame(&mut writer, &expected).await.unwrap();
    let actual = read_frame(&mut BufReader::new(reader)).await.unwrap();

    assert_eq!(actual, Some(expected));
}

#[tokio::test]
async fn approve_allow_all_roundtrips() {
    let expected = DaemonCommand::Approve {
        call_id: "call-1".to_owned(),
        decision: IpcApprovalDecision::AllowAll,
    };
    let (mut writer, reader) = duplex(4096);

    write_frame(&mut writer, &expected).await.unwrap();
    let actual = read_frame(&mut BufReader::new(reader)).await.unwrap();

    assert_eq!(actual, Some(expected));
}

#[tokio::test]
async fn input_roundtrips() {
    let expected = DaemonCommand::Input {
        text: "continue".to_owned(),
    };
    let (mut writer, reader) = duplex(4096);

    write_frame(&mut writer, &expected).await.unwrap();
    let actual = read_frame(&mut BufReader::new(reader)).await.unwrap();

    assert_eq!(actual, Some(expected));
}

#[tokio::test]
async fn eof_returns_none() {
    let (writer, reader) = duplex(64);
    drop(writer);

    let actual = read_frame::<_, WorkerEvent>(&mut BufReader::new(reader))
        .await
        .unwrap();

    assert_eq!(actual, None);
}

#[tokio::test]
async fn malformed_json_returns_invalid_data() {
    let (mut writer, reader) = duplex(64);
    writer.write_all(b"{not json}\n").await.unwrap();

    let error = read_frame::<_, WorkerEvent>(&mut BufReader::new(reader))
        .await
        .unwrap_err();

    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
}

#[tokio::test]
async fn oversized_write_returns_invalid_data() {
    let event = WorkerEvent::TextDelta {
        text: "x".repeat(MAX_IPC_FRAME_BYTES + 1),
    };
    let (mut writer, _reader) = duplex(64);

    let error = write_frame(&mut writer, &event).await.unwrap_err();

    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
}
