# peakcode-daemon

Long-running supervisor and gRPC gateway for [peakcode](https://github.com/peakcode-dev/peakcode),
the AI coding agent.

peakcode-daemon hosts [peakcode-core](https://github.com/peakcode-dev/peakcode-core) and keeps
agent sessions alive independently of any CLI or other frontend process. Clients communicate
with the daemon over gRPC through a Unix domain socket (UDS).

## Status

Early development (pre-alpha). Not yet released.

## Process roles

- **Daemon supervisor** - owns the local gRPC endpoint and supervises one worker subprocess per
  agent session.
- **Worker** - hosts one peakcode-core agent session so session failures remain isolated from the
  supervisor and other sessions.

This repository currently contains only the project scaffold and typed wire contract. Daemon,
registry, worker, and service implementations will be added in later development.

## Build

```bash
cargo build
```

The build uses vendored `protoc`; no system Protocol Buffers compiler is required.

## Usage

The binary exposes six modes:

```text
peakcode-daemon daemon
peakcode-daemon daemonize
peakcode-daemon worker --session <UUID> --ipc <PATH>
peakcode-daemon start
peakcode-daemon stop
peakcode-daemon status
```

Running `peakcode-daemon` without a subcommand is equivalent to `peakcode-daemon daemon` and
runs in the foreground. These modes are command-surface stubs in pre-alpha; daemon, worker,
daemonization, and lifecycle behavior will land in later tasks.

## Test

```bash
cargo test
```

## Security

See [`SECURITY.md`](SECURITY.md) for vulnerability reporting. peakcode-daemon is a component of
peakcode, a product of [peakssh](https://peakssh.dev).

## Contributing

See [`CONTRIBUTING.md`](CONTRIBUTING.md).

## License

MIT. See [`LICENSE`](LICENSE).
