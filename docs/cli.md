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
daemon connection closes. `cancel` aborts only the active turn, waits for peakcode-core cancellation
cleanup, preserves completed history, and leaves the worker available for later input. On Unix,
canceling an active bash tool also kills its process group so descendants cannot outlive the turn.

Worker output is bounded and centrally redacted before serialization. If the daemon stops draining
events, `stop` uses 250 ms deadlines for final event delivery and writer cleanup, then aborts the
blocked writer. At most 32 inputs can wait behind an active turn; a 33rd queued input reports a
sanitized terminal error instead of being dropped or retained without bound.

Worker mode is implemented. Daemon supervision, daemonization, and lifecycle modes remain
command-surface stubs in pre-alpha and currently identify the selected mode before exiting
successfully.
