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
- Worker events pass through one bounded channel and one writer task. The coordinator retains at
  most one additional mapped event while waiting for channel capacity, so it can continue handling
  `cancel` and `stop` under socket backpressure without reordering ordinary events.
- `stop` allows up to 250 ms to enqueue `done`. Final reader, active-turn, and writer cleanup each
  use a 250 ms deadline. If the peer does not drain output, `done` is best-effort and the blocked
  writer task is aborted after its deadline so the worker can exit.

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
active turn are queued in socket order and start after the current turn finishes. A worker retains
at most 32 queued inputs. The 33rd input emits a sanitized `crash` stating that the input queue limit
was exceeded, terminates that worker session, and follows bounded writer cleanup. Inputs are never
silently dropped.

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

Each pending registration has a unique generation token and cancellation-safe drop guard. Aborting
an approval future removes only its own generation, so a stale cleanup cannot delete or resolve a
later request that reuses the same `call_id`.

An `allow_all` decision is retained by the worker approval adapter for the rest of that worker
session. Later turns can run the same tool without another `needs_approval`; unrelated tools still
require their own decision.

All worker events, including approval requests and forwarded peakcode-core events, pass through one
bounded channel and one writer task. Only that task writes NDJSON bytes to the socket, preventing
concurrent producers from interleaving frames.

## Cancel and stop

`cancel` aborts the active peakcode-core agent task and waits for cancellation to complete before
clearing any remaining approval waiters. The worker retains history from completed turns, discards
the canceled turn's partial history, and remains available for later input. If no turn is active,
cancellation is a safe no-op. On Unix, peakcode-core starts each bash tool in its own process group;
aborting the agent drops the tool guard and sends `SIGKILL` to that group so descendants do not
survive cancellation.

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
- Immediately before serialization, the single writer redacts every string-bearing worker event.
  Redaction inputs include the active provider API key and values of environment variables whose
  names indicate passwords, tokens, secrets, API keys, private keys, credentials, authentication,
  database URLs, Redis URLs, or DSNs. Common bearer/token forms and PEM private-key blocks are also
  redacted. The replacement is the deterministic string `[REDACTED]`.
- Secret-bearing tool-call IDs receive stable, unique per-worker aliases when approval waiters are
  registered. The same alias is used by approval and tool events, preserving correlation without
  putting the original ID on the wire or collapsing distinct IDs. Worker errors returned to the
  process boundary are generic after a detailed, redacted `crash` event.
- Receivers must treat all free-form strings and nested `arguments_json` values as untrusted data.
- The internal UDS must not be exposed as the frontend API. Frontends use the separately managed,
  typed gRPC UDS.
