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
canceling an active bash tool also kills the shell process group so the leader and descendants that
remain in that group cannot outlive the turn. Deliberate `setsid` or `setpgid` escapes require Linux
cgroup containment in supervisor Task 6 and are outside worker Task 4.

Cancellation cleanup has a 250 ms deadline. If it expires, the worker reports a sanitized terminal
error best-effort and ends the session instead of accepting another input.

Worker output is bounded and centrally redacted before serialization. If the daemon stops draining
events, `stop` uses 250 ms deadlines for final event delivery and writer cleanup, then aborts the
blocked writer. An ordinary event already held by the coordinator is kept across cancel and is
enqueued before `done` or a queue-overflow error. A draining peer observes that order. Only an
undrained or broken peer may lose unsent frames when the deadline expires and bounded termination
takes priority. Pending approval barriers and notices may be discarded by cancel or stop because
their authorization waiters are invalidated. The reader-to-coordinator transport channel holds at
most 64 commands, and the coordinator separately retains at most 32 inputs behind an active turn. A
33rd input received by the coordinator reports a sanitized terminal error instead of being dropped
or retained without bound.

Provider tool-call IDs never cross IPC. The worker assigns fresh opaque UUID-derived handles for
approval and tool-event correlation, and orders each assistant message before its corresponding
approval request even under writer backpressure. Session-wide `allow_all` applies only to subsequent
calls to the same registered tool. Unknown model-generated tool names can be allowed for one
invocation but cannot create persistent approval state.

Worker mode is implemented. Daemon supervision, daemonization, and lifecycle modes remain
command-surface stubs in pre-alpha and currently identify the selected mode before exiting
successfully.
