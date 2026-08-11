use std::ffi::OsString;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "peakcode-daemon")]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    Daemon,
    Daemonize,
    Worker {
        #[arg(long)]
        session: String,
        #[arg(long)]
        ipc: String,
    },
    Start,
    Stop,
    Status,
}

#[derive(Debug, PartialEq, Eq)]
enum Mode {
    Daemon,
    Daemonize,
    Worker {
        session_id: String,
        ipc_path: String,
    },
    Start,
    Stop,
    Status,
}

fn parse_mode_from<I, T>(itr: I) -> Result<Mode, clap::Error>
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    let cli = Cli::try_parse_from(itr)?;

    Ok(match cli.command {
        None | Some(Command::Daemon) => Mode::Daemon,
        Some(Command::Daemonize) => Mode::Daemonize,
        Some(Command::Worker { session, ipc }) => Mode::Worker {
            session_id: session,
            ipc_path: ipc,
        },
        Some(Command::Start) => Mode::Start,
        Some(Command::Stop) => Mode::Stop,
        Some(Command::Status) => Mode::Status,
    })
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mode = parse_mode_from(std::env::args_os()).unwrap_or_else(|error| error.exit());

    match mode {
        Mode::Daemon => println!("peakcode-daemon: daemon mode"),
        Mode::Daemonize => println!("peakcode-daemon: daemonize mode"),
        Mode::Worker {
            session_id,
            ipc_path,
        } => println!("peakcode-daemon: worker mode (session: {session_id}, ipc: {ipc_path})"),
        Mode::Start => println!("peakcode-daemon: start mode"),
        Mode::Stop => println!("peakcode-daemon: stop mode"),
        Mode::Status => println!("peakcode-daemon: status mode"),
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_subcommand_maps_to_daemon() {
        assert_eq!(parse_mode_from(["peakcode-daemon"]).unwrap(), Mode::Daemon);
    }

    #[test]
    fn explicit_daemon_maps_to_daemon() {
        assert_eq!(
            parse_mode_from(["peakcode-daemon", "daemon"]).unwrap(),
            Mode::Daemon
        );
    }

    #[test]
    fn worker_maps_session_and_ipc_arguments() {
        assert_eq!(
            parse_mode_from([
                "peakcode-daemon",
                "worker",
                "--session",
                "s1",
                "--ipc",
                "/tmp/worker.sock",
            ])
            .unwrap(),
            Mode::Worker {
                session_id: "s1".to_owned(),
                ipc_path: "/tmp/worker.sock".to_owned(),
            }
        );
    }

    #[test]
    fn lifecycle_commands_map_to_their_modes() {
        let cases = [
            ("daemonize", Mode::Daemonize),
            ("start", Mode::Start),
            ("stop", Mode::Stop),
            ("status", Mode::Status),
        ];

        for (command, expected) in cases {
            assert_eq!(
                parse_mode_from(["peakcode-daemon", command]).unwrap(),
                expected
            );
        }
    }

    #[test]
    fn worker_missing_required_argument_returns_clap_error() {
        assert!(parse_mode_from(["peakcode-daemon", "worker", "--session", "s1"]).is_err());
    }
}
