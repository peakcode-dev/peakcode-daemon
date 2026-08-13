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
- peakcode-core and its OpenAI provider each bound their event channels at 64 entries. The worker's
  core-event receiver is therefore bounded end to end; no unbounded event queue sits behind the
  worker writer.
- During cancel, normal stop, or queue-overflow termination, an ordinary event already held by the
  coordinator remains ahead of later turn or terminal events. While idle with such an event, the
  coordinator selects between command handling and writer capacity, so `stop` remains responsive.
- Normal stop and queue-overflow termination use one 250 ms enqueue deadline for the held ordinary
  event, if any, followed by `done` or `crash`. Final reader, active-turn, and writer cleanup each use
  a 250 ms deadline. A draining peer therefore observes the ordinary event before the terminal
  event. If the peer is undrained or broken when a deadline expires, bounded worker termination
  takes priority and any frame not fully written by then may be lost.
- Outbound JSON is serialized directly into a capped buffer. The buffer rejects a write before it
  would grow beyond 1 MiB; the line-feed byte is written separately only after successful
  serialization, so an exact-max payload does not grow the payload allocation.

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
| `allow_all` | Allows the identified call and subsequent calls to the same registered tool for this worker session. |

## Input flow

1. A frontend sends gRPC `SendInput` for a daemon-owned session.
2. The daemon validates the session and writes an `input` command to that session's worker socket.
3. The worker processes the turn and emits ordered worker events.
4. The worker emits `turn_finished` when the turn ends and remains available for later input.

Each worker runs at most one agent turn at a time. Additional `input` commands received during an
active turn are queued in socket order and start after the current turn finishes. The command-reader
transport channel can hold up to 64 commands before applying socket backpressure. Separately, the
coordinator retains at most 32 inputs behind the active turn. Overflow is detected when the
coordinator receives a 33rd retained input; it emits a sanitized `crash`, terminates that worker
session, and follows bounded writer cleanup. Inputs are never silently dropped. The 32-entry limit
does not include commands still waiting in the 64-entry transport channel.

Input is scoped by the socket connection. Frames do not carry a session ID because each supervised
worker connection belongs to exactly one daemon session.

## Approval flow

1. The worker's approval adapter creates a fresh UUID-derived opaque authorization handle, registers
   a waiter keyed by that handle, and sends a bounded internal approval notice to the coordinator.
2. The coordinator establishes an approval barrier. It drains every core event already queued at
   that point, in order and through the normal bounded event path, before emitting `needs_approval`.
   Because core is blocked awaiting approval, an empty core receiver is the causal boundary.
3. The worker keeps that call pending and does not execute it before a decision arrives.
4. A frontend resolves the request through gRPC, and the daemon writes `approve` with the opaque
   handle as `call_id` and a typed decision.
5. The worker applies the decision only to the matching pending handle. Unknown, stale, or
   already-resolved call IDs must be rejected safely.

The approval adapter sends its coordinator notice from `approve` because peakcode-core requests
approval before it emits `tool_start`. Waiting for `tool_start` to initiate approval would deadlock
the tool call. The coordinator, not the adapter, emits the outward event after satisfying the causal
barrier. If notice delivery fails or the response waiter closes, the adapter denies the tool call.

Each pending registration has a unique generation token and cancellation-safe drop guard. Aborting
an approval future removes only its own generation, so a stale cleanup cannot delete or resolve a
later request that reuses the same `call_id`.

Provider call IDs are never authorization identities and never cross the socket. Every approval
invocation receives a fresh opaque handle, even when the provider reuses its ID. A delayed decision
for an earlier handle cannot authorize a later invocation. `tool_start` and `tool_result` use the
same handle as that invocation's `needs_approval`. Calls that do not require approval receive a fresh
opaque handle on `tool_start`. The mapping is removed on `tool_result` and cleared on cancel or turn
termination, bounding its lifetime.

At worker construction, the approval adapter derives an immutable finite set of registered tool
names from the tool registry schemas. An `allow_all` decision is retained for the rest of the worker
session only when its tool is in that set. Later turns can run that registered tool without another
`needs_approval`; unrelated registered tools still require their own decision. For an unknown tool,
`allow_all` acts as ordinary `allow` for only that invocation and creates no session state. The
persistent approved set is therefore always a subset of the finite worker tool registry.

All worker events, including approval requests and forwarded peakcode-core events, pass through one
bounded channel and one writer task. Only that task writes NDJSON bytes to the socket, preventing
concurrent producers from interleaving frames.

## Cancel and stop

`cancel` aborts the active peakcode-core agent task and allows up to 250 ms for cancellation cleanup
before clearing any remaining approval waiters and call-ID mappings. On timely completion, the
worker retains history from completed turns, discards the canceled turn's partial history, and
remains available for later input. If cleanup exceeds the deadline, the worker emits a sanitized
terminal `crash` best-effort and ends the session rather than accepting another turn. If no turn is
active, cancellation is a safe no-op.

An ordinary core event already received by the coordinator is not part of the canceled turn's
mutable history: the worker retains and flushes that event through the single writer before starting
another turn. A pending approval barrier, a held `needs_approval`, and queued internal approval
notices may instead be discarded on `cancel` or `stop`, because cancellation invalidates their
authorization handles and waiters. This exception does not permit discarding an ordinary held event.

On supported Unix platforms, peakcode-core starts each bash tool in its own process group; aborting
the agent drops the managed child and sends `SIGKILL` to the shell leader and descendants that remain
in that group. A child that deliberately escapes with `setsid` or `setpgid` is outside this Task 4
guarantee. Linux cgroup containment for deliberate escapes belongs to supervisor Task 6 and is not
implemented by the worker.

`stop` ends the session permanently. The worker must stop accepting input, perform bounded cleanup,
enqueue an ordinary coordinator-held event before `done`, and exit. The ordering guarantee applies
while the peer drains output; an undrained or broken peer may lose unsent frames when the fixed
deadline expires. A frontend detach is neither `cancel` nor `stop`; disconnecting a frontend must
not change the worker lifetime.

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
  database URLs, Redis URLs, or DSNs. Every non-empty matching value is redacted, including
  one-character values. Common bearer/token forms and PEM private-key blocks are also redacted. The
  replacement is normally the deterministic string `[REDACTED]`. If a configured secret is itself
  contained in that marker, redaction uses an empty replacement so generated marker text cannot
  reintroduce the secret.
- Call identity is handled separately from redaction: all outward tool-call IDs are opaque
  UUID-derived handles. Redaction has no call-ID alias map. Worker errors returned to the process
  boundary are generic after a detailed, redacted `crash` event.
- Receivers must treat all free-form strings and nested `arguments_json` values as untrusted data.
- The internal UDS must not be exposed as the frontend API. Frontends use the separately managed,
  typed gRPC UDS.
