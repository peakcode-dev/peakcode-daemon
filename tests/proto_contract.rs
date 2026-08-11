use peakcode_daemon::proto::v1::{self, client_command, daemon_event, SessionStatus};

#[test]
fn generated_contract_exposes_session_and_stream_shapes() {
    let _session = v1::Session {
        id: "session-1".into(),
        created_at_unix_ms: 0,
        status: SessionStatus::Running as i32,
        model: "model".into(),
        workdir: "/tmp".into(),
    };

    let _command = v1::ClientCommand {
        session_id: "session-1".into(),
        body: Some(client_command::Body::Detach(v1::Detach {})),
    };

    let _event = v1::DaemonEvent {
        session_id: "session-1".into(),
        seq: 1,
        body: Some(daemon_event::Body::TextDelta(v1::TextDelta {
            text: "hi".into(),
        })),
    };
}
