# AGENTS.md - peakcode-daemon

This file provides guidance to AI coding agents working in this repository.

peakcode-daemon is the long-running supervisor and gRPC gateway for peakcode agent
sessions. It hosts peakcode-core and supervises one worker subprocess per session.
Frontends such as peakcode-cli are thin clients and must not own agent lifetimes.

## Naming convention

The project name is **`peakcode`**, always lowercase. The crate and repository slug are
**`peakcode-daemon`**, also always lowercase. This applies to documentation, code comments,
commit messages, pull request descriptions, changelogs, and prose everywhere.

## ALWAYS `git pull` before starting work

Before editing, building, or reasoning about this repository, run `git pull --ff-only`
(or `git fetch` and check). Local checkouts can drift behind `origin/main`; editing stale
code wastes work because changes conflict on rebase.

## Architecture decisions: build for the standard, not the corner shop

When making any architecture decision, design it as if this system is meant to compete with
the best-in-class tools and become a new standard, not to out-do a toy. Reach for scalable,
production-grade solutions over toy ones. Aim high by default.

## UI-agnostic and process invariants

- The daemon is a headless service. Do not add TUI, terminal rendering, or frontend concerns.
- Frontends communicate through typed gRPC contracts and must be able to attach and detach
  without changing the lifetime of an agent session.
- The daemon supervisor owns the local UDS endpoint and session registry.
- Each agent session runs in its own worker subprocess. A worker failure must not terminate
  the supervisor or another session.
- peakcode-core owns provider, tool, agent-loop, session, and configuration domain logic.
  Do not duplicate that logic in the daemon.
- Services and process lifecycle code must be testable without a terminal or interactive UI.
- Preserve explicit event ordering and session identity across process and gRPC boundaries.

## Documentation Conventions

When modifying a module, check if documentation for it exists under `/docs/`. If a matching
document exists, update it to reflect the changes. If no document exists, do not create one.

Any change that touches a public contract, including protobuf services, messages, enums,
process protocols, lifecycle guarantees, socket behavior, or anything consumers build
against, **must** be accompanied by a documentation update in `/docs/`. If no document exists
yet, create one. Treat these docs like a public API: every addition, removal, or behavioral
change must be reflected before the change is considered complete.

## Documentation authoring (`/docs`)

The peakssh docs platform imports each repository's `/docs` folder into the central docs
backend (docs.peakssh.dev). Author docs so the importer maps them correctly:

- **Scope:** only the repository's `/docs` folder is imported. Anything outside `/docs`,
  including the repository's top-level `README.md`, is ignored. A `README.md` or `index.md`
  inside `/docs` is imported as an ordinary document; those names get no special treatment.
- **Folders = categories, `.md` files = documents.** A subfolder becomes a subcategory; each
  `.md` file becomes a document under its folder's category.
- **Only `.md` files and subfolders** are considered inside `/docs`. Any other file type is
  ignored by the importer.
- **Ordering:** prefix a file or folder with `NN-` (for example, `01-intro.md` or `02-setup/`)
  to set its navigation position. The numeric prefix is stripped from the slug
  (`01-intro.md` -> slug `intro`). Without a prefix, items are ordered alphabetically.
- **Slugs** derive from the file or folder name after removing any `NN-` prefix and the `.md`
  extension. Names are lowercased and non-alphanumeric characters become hyphens. Keep names
  slug-friendly: `^[a-z0-9]+(?:-[a-z0-9]+)*$`.
- **Document title** resolves in order: YAML frontmatter `title:` -> first `# H1` heading ->
  file name. A leading frontmatter block (`--- ... ---`) is used only for the title and is
  stripped from stored content.
- **Category title** is humanized from the folder name (`provider-integration` ->
  `Provider Integration`).
- One document is imported per file. The maximum content size is 512 KB; larger files are
  skipped.
- A category that **tracks** a repository is managed by that repository. Its documents are
  overwritten on each sync. Edit docs in the repository, not in the admin panel.

Configure import and tracking in the admin panel at **/docs/documents -> "Import documents"**.

## Git Conventions

- **Pull first:** run `git pull --ff-only` before starting work.
- **Commits:** always use `git commit -s` to add the Developer Certificate of Origin
  `Signed-off-by` trailer.
- **Commit messages:** follow [Conventional Commits](https://www.conventionalcommits.org/):
  `<type>(<scope>): <description>` (for example, `feat(session): supervise worker process`).

## Style

- Never use the em dash (Unicode U+2014) in any text, including code comments,
  documentation, commit messages, and prose. Use a normal hyphen, comma, or rephrase.
