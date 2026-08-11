# peakcode daemon gRPC API

The public protobuf package is `peakcode.daemon.v1`. Its source of truth is
[`proto/peakcode.proto`](../proto/peakcode.proto).

The daemon currently exposes gRPC over a local Unix domain socket (UDS). The UDS is the local
trust boundary and must be protected with filesystem ownership and permissions. A future network
transport will carry the same gRPC contract over TLS. Network transport must not be exposed
without authentication and transport encryption.

## SessionService

`SessionService` manages daemon-owned agent sessions.

| RPC | Request | Response | Purpose |
| --- | --- | --- | --- |
| `CreateSession` | `CreateSessionRequest` | `Session` | Creates a session and returns its initial state. |
| `ListSessions` | `google.protobuf.Empty` | `SessionList` | Lists sessions known to the daemon. |
| `GetSession` | `SessionId` | `Session` | Returns the current state of one session. |
| `StopSession` | `StopSessionRequest` | `google.protobuf.Empty` | Permanently stops the identified session. |

### Session requests and responses

`SessionId` contains:

| Field | Number | Type | Meaning |
| --- | ---: | --- | --- |
| `id` | 1 | `string` | Daemon-assigned session identifier. |

`CreateSessionRequest` contains:

| Field | Number | Type | Meaning |
| --- | ---: | --- | --- |
| `model` | 1 | `string` | Model requested for the agent session. |
| `system_prompt` | 2 | `string` | System instructions supplied when the session is created. |
| `workdir` | 3 | `string` | Working directory for the session. |

`SessionList` contains one field, `sessions` (field 1), a repeated list of `Session` values.

`StopSessionRequest` contains `id` (field 1, `string`), the identifier of the session to stop.
Stopping a session is distinct from cancelling a turn or detaching a client.

### Session

`Session` is the daemon's current view of a session:

| Field | Number | Type | Meaning |
| --- | ---: | --- | --- |
| `id` | 1 | `string` | Stable daemon-assigned session identifier. |
| `created_at_unix_ms` | 2 | `int64` | Creation time in milliseconds since the Unix epoch. |
| `status` | 3 | `SessionStatus` | Current lifecycle state. |
| `model` | 4 | `string` | Model selected for the session. |
| `workdir` | 5 | `string` | Working directory owned by the session worker. |

`SessionStatus` values and semantics are:

| Value | Number | Semantics |
| --- | ---: | --- |
| `SESSION_STATUS_UNSPECIFIED` | 0 | Protobuf default or an unavailable state. It is not an operational lifecycle state. |
| `SESSION_STATUS_IDLE` | 1 | The session is alive and has no active agent turn. |
| `SESSION_STATUS_RUNNING` | 2 | The session is alive and processing an agent turn. |
| `SESSION_STATUS_STOPPED` | 3 | The session was intentionally stopped and cannot process more turns. |
| `SESSION_STATUS_CRASHED` | 4 | The session worker ended unexpectedly and cannot process more turns. |

`STOPPED` and `CRASHED` are terminal states. A client can detach while a session is `IDLE` or
`RUNNING` without changing its status.

## AttachService

`AttachService.Attach` is a bidirectional streaming RPC:

```proto
rpc Attach(stream ClientCommand) returns (stream DaemonEvent);
```

The client writes a stream of `ClientCommand` messages to the daemon. Independently, the daemon
writes a stream of ordered `DaemonEvent` messages to the client. A slow or idle direction must
not require the opposite direction to send a message.

### ClientCommand

Every `ClientCommand` is an envelope with:

| Field | Number | Type | Meaning |
| --- | ---: | --- | --- |
| `session_id` | 1 | `string` | Session targeted by this command. |
| `body` | 2-5 | `oneof` | Exactly one command variant. |

The `body` variants are:

| Variant | Number | Payload fields | Purpose |
| --- | ---: | --- | --- |
| `send_input` | 2 | `SendInput.text` (field 1, `string`) | Starts or continues agent work with user input. |
| `approve_tool` | 3 | `ApproveTool.call_id` (field 1, `string`), `ApproveTool.decision` (field 2, `ApprovalDecision`) | Resolves a pending tool approval request. |
| `cancel` | 4 | `Cancel` has no fields | Cancels the active turn without stopping the session. |
| `detach` | 5 | `Detach` has no fields | Removes this client from the session without stopping or cancelling it. |

`ApprovalDecision` values and semantics are:

| Value | Number | Semantics |
| --- | ---: | --- |
| `APPROVAL_DECISION_UNSPECIFIED` | 0 | Protobuf default. It is not a valid resolution for a pending approval. |
| `APPROVAL_DECISION_ALLOW` | 1 | Allows the tool call identified by `call_id`. |
| `APPROVAL_DECISION_DENY` | 2 | Denies the tool call identified by `call_id`. |
| `APPROVAL_DECISION_ALLOW_ALL` | 3 | Allows the identified call and subsequent approval-requiring calls for the session. |

### DaemonEvent

Every `DaemonEvent` is an envelope with:

| Field | Number | Type | Meaning |
| --- | ---: | --- | --- |
| `session_id` | 1 | `string` | Session that produced the event. |
| `seq` | 2 | `uint64` | Per-session event sequence used for ordering, deduplication, and replay. |
| `body` | 3-10 | `oneof` | Exactly one event variant. |

The `body` variants and their exact payloads are:

| Variant | Number | Payload fields | Meaning |
| --- | ---: | --- | --- |
| `text_delta` | 3 | `TextDelta.text` (field 1, `string`) | Incremental assistant text. |
| `assistant_message` | 4 | `AssistantMessage.text` (field 1, `string`) | Complete assistant message. |
| `tool_start` | 5 | `ToolStart.call_id` (field 1, `string`), `ToolStart.name` (field 2, `string`), `ToolStart.arguments_json` (field 3, `string`) | Announces a tool invocation and its JSON-encoded arguments. |
| `tool_result` | 6 | `ToolResult.call_id` (field 1, `string`), `ToolResult.name` (field 2, `string`), `ToolResult.content` (field 3, `string`), `ToolResult.is_error` (field 4, `bool`) | Reports tool output and whether that output represents an error. |
| `needs_approval` | 7 | `NeedsApproval.call_id` (field 1, `string`), `NeedsApproval.tool` (field 2, `string`), `NeedsApproval.arguments_json` (field 3, `string`) | Pauses a tool call until a client supplies an approval decision. |
| `turn_finished` | 8 | `TurnFinished` has no fields | Marks completion of the active turn. |
| `session_ended` | 9 | `SessionEnded.reason` (field 1, `string`) | Reports that the session ended and gives the reason. |
| `error` | 10 | `ErrorEvent.message` (field 1, `string`) | Reports an error associated with the session stream. |

Sequence numbers are monotonic within a session. Clients use `(session_id, seq)` to preserve
event order, discard duplicate deliveries, and detect gaps. When an event is replayed, it keeps
its original `seq`; all clients observing the same session event observe the same sequence
identity. The current contract does not expose a replay cursor in `ClientCommand`, so clients
must not assume that opening `Attach` alone requests history or that sequence numbering starts at
a particular value.

## Multi-client behavior

Multiple clients may attach to the same session. Attaching a new client does not transfer
ownership from an existing client and does not restart the session. Session events sent to
multiple attached clients retain the same `session_id` and `seq`.

A `Detach` command drops only the sending client's attachment to the command's `session_id`.
Closing an `Attach` stream likewise removes that stream's attachments. Neither operation stops
the worker, cancels its active turn, or detaches other clients. Use `Cancel` to cancel active work
and `SessionService.StopSession` to end the session.

## Tool approval flow

1. The daemon emits `NeedsApproval` with the session ID, tool `call_id`, tool name, and
   JSON-encoded arguments.
2. The tool call remains pending while an attached client decides.
3. A client sends `ApproveTool` for the same `session_id` and `call_id` with a non-unspecified
   `ApprovalDecision`.
4. The daemon allows or denies the pending call according to the decision. An unknown, stale, or
   already-resolved `call_id` is rejected.

Clients should correlate approvals by both `session_id` and `call_id`. They must not infer an
approval from attachment, input, or the absence of an error.

## Errors and security

- Commands with an empty session ID, a missing or unknown command body, invalid enum values,
  malformed payloads, or references to unavailable sessions or calls are rejected rather than
  executed.
- Producers must not place credentials, authentication tokens, private keys, or other secrets in
  `DaemonEvent` payloads. This includes free-form text, errors, tool arguments, and tool results.
- Clients must treat event text, tool names, JSON arguments, tool results, end reasons, and error
  messages as untrusted data.
- UDS deployments must restrict access to the socket. Future network deployments must use TLS
  and authenticate clients before accepting session commands or returning events.

## Compatibility

Protobuf field numbers and enum numeric values are stable public API. They must not be changed,
and tags or numeric values removed from the contract must never be reused. Removed fields and
enum values should be reserved in the protobuf definition.

Compatible evolution is additive: new fields, messages, enum values, and `oneof` variants use new
numbers. Consumers must tolerate unknown protobuf fields and values, while servers reject command
envelopes that do not contain a command variant they understand. Any additive or behavioral
contract change requires a corresponding update to this document in the same change.
