# peakcode daemon internal IPC

The daemon supervisor and each worker subprocess communicate over a private Unix domain socket
(UDS). This socket is internal process transport and is separate from the external gRPC UDS used
by frontends. The supervisor owns the worker socket and must protect it with filesystem ownership
and permissions.

## Framing

The protocol uses newline-delimited JSON (NDJSON):

- Each frame is one UTF-8 JSON object followed by a line-feed byte (`\n`).
- Each line contains exactly one frame. JSON objects cannot span lines.
- The serialized JSON object can be at most 1 MiB (`1,048,576` bytes), excluding its terminating
  line feed.
- Receivers enforce the size limit incrementally before copying each input chunk, so an
  unterminated frame cannot cause unbounded allocation.
- The line-feed byte is mandatory and is the only byte removed before JSON deserialization. A
  non-empty fragment followed by end of file is invalid rather than a completed frame.
- The daemon and worker flush each frame after writing it.
- End of file with no pending frame bytes means the peer closed its side of the connection.
- Each worker has one dedicated command-reader task, so concurrent outbound events cannot cancel an
  in-progress read and discard part of a fragmented command frame.

Every frame has a string `kind` discriminator encoded in `snake_case`. Frames sent by workers are
`WorkerEvent` values. Frames sent by the daemon are `DaemonCommand` values.

## Worker events

| `kind` | Fields | Meaning |
| --- | --- | --- |
| `text_delta` | `text: string` | Emits an incremental portion of assistant text. |
| `assistant_message` | `text: string` | Emits a complete assistant message. |
| `tool_start` | `call_id: string`, `name: string`, `arguments_json: string` | Announces a tool invocation and its JSON-encoded arguments. |
| `tool_result` | `call_id: string`, `name: string`, `content: string`, `is_error: boolean` | Reports tool output and whether the output represents an error. |
| `needs_approval` | `call_id: string`, `tool: string`, `arguments_json: string` | Pauses a tool call until the daemon sends an approval decision. |
| `turn_finished` | None | Marks completion of the active user turn while keeping the worker alive. |
| `done` | `final_message_count: non-negative integer` | Reports normal worker completion and the final number of messages. |
| `crash` | `message: string` | Reports that the worker cannot continue because of an unexpected failure. |

`arguments_json` is a JSON value encoded inside a JSON string. Consumers must parse it separately
when they need structured tool arguments.

## Daemon commands

| `kind` | Fields | Meaning |
| --- | --- | --- |
| `input` | `text: string` | Forwards a user turn received through gRPC `SendInput` to the worker. |
| `approve` | `call_id: string`, `decision: IpcApprovalDecision` | Resolves the matching pending tool approval request. |
| `cancel` | None | Cancels the active turn without terminating the worker session. |
| `stop` | None | Permanently stops the worker session. |

`IpcApprovalDecision` is encoded as one of these strings:

| Value | Meaning |
| --- | --- |
| `allow` | Allows the tool call identified by `call_id`. |
| `deny` | Denies the tool call identified by `call_id`. |
| `allow_all` | Allows the identified call and subsequent approval-requiring calls for the session. |

## Input flow

1. A frontend sends gRPC `SendInput` for a daemon-owned session.
2. The daemon validates the session and writes an `input` command to that session's worker socket.
3. The worker processes the turn and emits ordered worker events.
4. The worker emits `turn_finished` when the turn ends and remains available for later input.

Each worker runs at most one agent turn at a time. Additional `input` commands received during an
active turn are queued in socket order and start after the current turn finishes.

Input is scoped by the socket connection. Frames do not carry a session ID because each supervised
worker connection belongs to exactly one daemon session.

## Approval flow

1. The worker's approval adapter registers a waiter keyed by `call_id`, then immediately emits
   `needs_approval` with the same ID, tool name, and JSON-encoded arguments.
2. The worker keeps that call pending and does not execute it before a decision arrives.
3. A frontend resolves the request through gRPC, and the daemon writes `approve` with the same
   `call_id` and a typed decision.
4. The worker applies the decision only to the matching pending call. Unknown, stale, or
   already-resolved call IDs must be rejected safely.

The approval adapter emits `needs_approval` itself because peakcode-core requests approval before
it emits `tool_start`. Waiting for `tool_start` to announce the approval would deadlock the tool
call. If the event cannot be delivered or the response waiter closes, the adapter denies the tool
call. Duplicate pending IDs are also denied rather than replacing an existing waiter.

All worker events, including approval requests and forwarded peakcode-core events, pass through one
bounded channel and one writer task. Only that task writes NDJSON bytes to the socket, preventing
concurrent producers from interleaving frames.

## Cancel and stop

`cancel` interrupts only the active turn. The worker remains alive, preserves the session, and can
accept a later `input` command. If no turn is active, cancellation is a safe no-op.

`stop` ends the session permanently. The worker must stop accepting input, perform bounded cleanup,
emit `done` with the final in-memory message count, and exit. A frontend detach is neither `cancel`
nor `stop`; disconnecting a frontend must not change the worker lifetime.

## Compatibility and safety

- Protocol evolution is additive. New `WorkerEvent` and `DaemonCommand` variants may be added, but
  existing kinds, fields, decision values, and semantics must not be renamed, removed, or changed.
- An unknown, malformed, invalid UTF-8, oversized, or unterminated frame is rejected with
  `InvalidData`. This error is terminal for the stream: the receiver must close the affected worker
  connection without attempting to read another frame. The protocol failure must not terminate the
  daemon supervisor or unrelated workers.
- Producers must never place API keys, authentication tokens, private keys, credentials, or other
  secrets in frames. This applies to free-form text, errors, tool arguments, and tool results.
- Receivers must treat all free-form strings and nested `arguments_json` values as untrusted data.
- The internal UDS must not be exposed as the frontend API. Frontends use the separately managed,
  typed gRPC UDS.
