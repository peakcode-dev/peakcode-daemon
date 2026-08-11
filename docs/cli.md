# Command-line interface

peakcode-daemon exposes six process and lifecycle modes:

| Command | Mode |
| --- | --- |
| `peakcode-daemon daemon` | Run the daemon supervisor in the foreground. |
| `peakcode-daemon daemonize` | Launch the daemon as a detached process. |
| `peakcode-daemon worker --session <ID> --ipc <PATH>` | Run a worker for one session using the specified IPC socket. |
| `peakcode-daemon start` | Start the daemon through the lifecycle interface. |
| `peakcode-daemon stop` | Stop the daemon through the lifecycle interface. |
| `peakcode-daemon status` | Report daemon status through the lifecycle interface. |

Running `peakcode-daemon` without a subcommand selects `daemon` mode.

For `worker`, `--session <ID>` supplies the daemon session identifier and `--ipc <PATH>` selects its
private worker socket. Worker mode connects to that UDS, loads peakcode-core configuration, runs
OpenAI-backed agent turns, and streams typed internal IPC events until it receives `stop` or the
daemon connection closes. `cancel` aborts only the active turn, so the worker can accept later input.

Worker mode is implemented. Daemon supervision, daemonization, and lifecycle modes remain
command-surface stubs in pre-alpha and currently identify the selected mode before exiting
successfully.
