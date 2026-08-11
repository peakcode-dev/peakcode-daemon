# Contributing to peakcode

Thank you for your interest in contributing to peakcode! peakcode is an AI coding agent
written in Rust and a product of the [peakssh](https://peakssh.dev) ecosystem. Every
contribution, big or small, is welcome and appreciated.

## Getting Started

1. **Fork** this repository and clone it locally.
2. Create a new branch from `main`:
   ```bash
   git checkout -b feat/your-feature-name
   ```
3. Make your changes, then push your branch and open a Pull Request.

Read this repository's `README.md` for local setup and build instructions before diving in.

## How to Contribute

- **Bug fixes** - Found something broken? Open an issue first, then submit a fix.
- **New features** - Discuss larger ideas via an issue before writing code.
- **Docs and typos** - Always welcome, no issue needed.
- **Refactoring** - Keep it focused and well-explained in the PR description.

## Commit Message Convention

We follow the [Conventional Commits](https://www.conventionalcommits.org/) standard.
This keeps the history clean and makes changelogs easy to generate.

All commits **must** be signed off with `git commit -s` (adds a `Signed-off-by:` trailer).
This is a Developer Certificate of Origin attestation that you have the right to submit
the change under the project's license.

Use this format:

```
<type>(<scope>): <short description>
```

Common types are `feat`, `fix`, `docs`, `style`, `refactor`, `test`, `chore`, and `ci`.
Scope is optional but encouraged. For example:

```
feat(session): supervise worker process
fix(attach): preserve event sequence
docs(readme): update local setup instructions
chore(deps): bump tokio
```

## Pull Request Guidelines

- Keep each pull request focused on one concern.
- Explain what changed and why.
- Link related issues with `Closes #123` or `Relates to #456`.
- Keep changes reasonably small and responsive to review feedback.
- Run `cargo fmt -- --check`, `cargo test`, and
  `cargo clippy --all-targets -- -D warnings` before submitting.
- Use a Conventional Commit title for the pull request.

## Tech Stack Overview

| Layer | Technology |
|---|---|
| Language | Rust |
| Async runtime | tokio |
| RPC | tonic gRPC over UDS |
| Agent engine | peakcode-core |

Use a recent Rust toolchain installed through `rustup`.

## Reporting Issues

When opening a bug report, include a clear title, reproduction steps, expected and actual
behavior, and environment details. For feature requests, describe the use case and its value
to peakcode.

Thank you for helping make peakcode better!
