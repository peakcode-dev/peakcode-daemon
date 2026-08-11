# Command-line interface

peakcode-daemon exposes six process and lifecycle modes:

| Command | Mode |
| --- | --- |
| `peakcode-daemon daemon` | Run the daemon supervisor in the foreground. |
| `peakcode-daemon daemonize` | Launch the daemon as a detached process. |
| `peakcode-daemon worker --session <UUID> --ipc <PATH>` | Run a worker for one session using the specified IPC socket. |
| `peakcode-daemon start` | Start the daemon through the lifecycle interface. |
| `peakcode-daemon stop` | Stop the daemon through the lifecycle interface. |
| `peakcode-daemon status` | Report daemon status through the lifecycle interface. |

Running `peakcode-daemon` without a subcommand selects `daemon` mode.

All modes are command-surface stubs in pre-alpha. They currently identify the selected mode and
exit successfully. Daemon supervision, worker execution, daemonization, and lifecycle behavior
will land in later tasks.
