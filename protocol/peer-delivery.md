# Peer delivery v1: contract before implementation

This is a separate, addressed delivery service. Existing `message` room sequence,
state, checkpoint, catchup, blobs and lossy latest/live semantics do not change.
Applications own authorization, event meaning, conflict resolution, snapshots,
application acknowledgements and persistence. No automatic broadcast or host
election is introduced.

## Public API and success

JS: `await room.sendTo(peer, topic, value, {signal?, timeoutMs?})` returns a
`DeliveryReceipt`. `room.on("peer_message", handler)` receives a complete
`{peer,publisherEpoch,targetEpoch,topic,messageId,seq,value}` once. Go provides
`SendTo(ctx, peer, topic, value)` and the existing Messages channel; C# provides
`SendToAsync(peer, topic, value, ct)` and the existing Events stream. All use the
same wire contract and portable MessagePack Value subset already used by the
SDKs, including byte arrays. MessagePack extension objects are not added.

A receipt means that the receiver SDK has verified length/hash, accepted the
message in sender/topic order and admitted it to its bounded application delivery
path. It does not mean that a listener succeeded, a canvas applied the operation,
or a server persisted it. Applications requiring those guarantees send a separate
application response containing their own stable event ID.

IDs are generated once per logical send and retained across fragments, retries
and route changes. Callers must not retry an ambiguous operation as a new ID
without their own application event deduplication. A send to several peers is
several independent sends; use per-recipient receipts/errors (e.g.
`Promise.allSettled`). There is no all-recipient atomic commit.

## Ordering, membership and failure

An ordering lane is `(room epoch, sender publisherEpoch, target publisherEpoch,
topic)`. Sequence numbers start at one and increment once when a send becomes
the active head of that lane. A lane has only one unacknowledged logical message.
Other lanes proceed independently; there is no global or cross-topic order.

The sender pins the recipient generation at API invocation. A missing peer,
unsupported capability or changed recipient generation fails explicitly. Queued
and in-flight sends are never retargeted to a replacement peer with the same name.
Incoming direct and relayed frames must match current membership generations.

The receiver retains a bounded lane high-water mark for its connection lifetime.
It never notifies the application twice for an accepted sequence, including when
the final receipt is lost and the sender retransmits through another route.
Completed messages are not retained as unbounded payload caches. A duplicate
current sequence must agree with its message ID and immutable metadata; older
sequences can be acknowledged again but cannot be delivered again.

Timeout/cancellation while waiting in a lane removes that queued send before a
sequence is assigned. Once a sequence has been assigned at active-head start,
a terminal failure conservatively poisons that
sender lane and rejects its later queued sends with `LaneFailed`; it must not skip
the missing sequence. Other topics/peers remain usable. Rejoin creates fresh
generations and fresh lanes; application resynchronization decides what to resend.
Receiver-side incomplete transfer expiry/cancellation similarly terminates its
lane. An abort is best effort and cannot retract a previously accepted message.
An error must distinguish `not-sent` from `unknown` delivery outcome. A connection
loss, missing receipt or cancellation after sending can have an unknown outcome.
No exactly-once execution guarantee survives reconnection or process restart.
An active send can therefore fail with `not-sent` and still terminate its lane
if local transport admission failed after sequence assignment; queued sends that
never became active do not consume a sequence or terminate the lane.

## Capability and routing

Updated clients advertise `peerDelivery` version 1 and their limits in join.
The core advertises support and includes each peer's delivery capability in
joined/membership information. Unsupported or legacy peers are detected before
sending. Negotiated message/chunk/window limits are the minimum of sender,
receiver and core limits. Configured receive/queue budgets are local admission
limits; they are not promises of available memory.

JS uses an additional ordered, reliable `kokosync-peer-delivery` DataChannel on
the existing selected peer RTCPeerConnection. Native clients without a direct
adapter use the same API/wire contract through the core. If no usable direct
channel exists, send through the core immediately; do not wait for ICE setup.
If the direct route fails or stops progressing, retry the same logical ID through
the core. A reliable DataChannel alone does not replace end-to-end receipts.

Core relay forwarding pins both member generations, assigns sender identity and
room epoch, validates frame type/shape/limits, and returns an explicit relay error
on stale/missing/incompatible destinations. It does not assign a room sequence,
write history, interpret the value or acknowledge receipt on behalf of the peer.

## Transfer and budgets

Every logical Value is encoded once, hashed with SHA-256 and transferred using an
open/credit/chunk/complete protocol. Both routes carry the same transfer identity
and offsets. Receiver credit is cumulative and bounded. Duplicate chunks are
harmless; out-of-order chunks trigger recovery from the contiguous accepted
offset, never partial application delivery. Admission and length/hash validation
finish before the receipt and one application notification.

Defaults: message 4 MiB; chunk 12 KiB; window 64 KiB; outgoing encoded data 8 MiB;
incomplete incoming data 8 MiB; four concurrent transfers; 128 queued sends;
64 active/retained ordering lanes per connection; total send deadline 30 seconds;
receiver assembly deadline 30 seconds; at most five retransmission rounds with
bounded retry delays. Limits are configurable and validated against hard ceilings
(message 64 MiB, chunk 16 KiB, window 256 KiB, queue/receive 256 MiB, lanes 1024,
concurrent 32, queued sends 4096, deadline 300 seconds, retries 20; zero
retries is supported). Lane retention must never evict a
live-generation high-water mark and then accept old messages as new; reject new
lanes when full. Resource reservations release on all terminal paths.

Frames carry at most one chunk and fit the existing physical frame maximum.
They are not nested inside the existing logical fragmentation protocol. Core and
SDK scheduling reserve progress for receipts/control and small messages, with
bulk chunks using bounded windows and bounded transport buffering. A logical
message larger than 16 KiB is bulk traffic. Small messages in *other topics* can
overtake bulk; the same topic's order is never violated. latest/live keep their
separate lossy path. These controls do not promise latency on a saturated network.

## Diagnostics and required validation

Expose snapshots keyed by peer generation, route (direct/core relay), and kind
(`peer_message`, `peer_bulk`, `latest`, `live`) with frame/message counts, encoded
bytes, retries, unacknowledged messages, queued message/byte counts and last route
change reason. Unmeasured RTT is null, never zero; define measured RTT as protocol
receipt/heartbeat round trip, not application processing/persistence latency.
Counters measure actual transport submissions/receptions, not guaranteed delivery.

Validate five concurrent peers with direct and relay paths, cross-language
interop, large transfers with small operations and latest updates progressing,
lost acknowledgements, duplicate/reordered fragments, route loss/fallback,
cancellation/timeouts, capacity limits, departures and same-name rejoin. Verify
one application notification, per-lane order and rejection of stale generations.
Keep an executable integration harness and document commands and actual results.

## Concrete wire schema (frozen v1)

Join advertises `peerDelivery:{version:1,messageBytes:4194304,chunkBytes:12288,
windowBytes:65536,queueBytes:8388608,receiveBytes:8388608,concurrent:4,
queued:128,lanes:64,timeoutMs:30000,assemblyMs:30000,retries:5}`.
Core returns its supported map in `joined.capabilities.peerDelivery` and includes
the peer's selected map as `peerDelivery` in joined roster/member events; absence
means unsupported. Core clamps advertised fields to its own limits. Senders take
the minimum local/core/recipient message, chunk, window, timeout and retry limits.
Queue/receive/count limits remain local admission budgets. The core configuration
has a `peerDelivery` section using this same map (version is fixed at 1).

All peer-delivery wire frames are standalone MessagePack maps and bypass generic
`frag_*` fragmentation. Common fields are:
`{v:1,type,to,targetEpoch,epoch,peer,publisherEpoch,topic,messageId,seq:uint64}`.
Each transmitting endpoint is `peer/publisherEpoch`; `to/targetEpoch` identifies
the destination generation. Responses reverse these fields, while retaining
topic/messageId/seq. The core first rejects mismatched source/room epochs, then
assigns source and room epoch from authenticated membership;
direct receivers bind them to the authenticated current peer connection.
Peer names are at most 128 UTF-8 bytes, topic at most 256, messageId at most 128.

* `peer_open`: adds `size:uint64,sha256:lowercaseHex64`. The payload is exactly
  one encoded MessagePack Value, not an envelope or array of separately encoded
  chunks. Size must be positive and within negotiated messageBytes.
* `peer_credit`: adds `offset:uint64,limit:uint64,retry?:bool`. Offset is the
  contiguous received prefix; limit is the absolute exclusive allowed send
  offset, at most `min(size,offset+windowBytes)`. It is cumulative, not additive.
  A retry flag asks the sender to resume from offset. Ordinary stale credits
  never rewind transmission. Receivers answer duplicate opens/chunks with their
  current credit; future/out-of-order chunks get credit with retry=true.
* `peer_chunk`: adds `offset:uint64,value:bytes,bulk:bool`. A chunk is nonempty
  and at most negotiated chunkBytes. Bulk is true for logical size >16384.
* `peer_complete`: no extra fields. If incomplete, receiver answers retry credit.
  Otherwise validate size/hash, decode Value, validate portable Value, admit one
  `peer_message` into the bounded application path, save high-water metadata,
  then send peer_done. No partial application delivery.
* `peer_done`: no extra fields. This is the end-to-end DeliveryReceipt.
* `peer_cancel`: adds `code:string` and optional `message:string`. Terminates an
  incomplete transfer and poisons that receive lane; cannot retract accepted data.
* `peer_relay_error`: generated only by core, adds `code,message`, and identifies
  the intended recipient as peer/publisherEpoch so the sender can match its
  active transfer. This is an error, never a delivery receipt.

The sender starts each attempt with peer_open, including retries after a missing
receipt; the same ID/seq/size/hash is retained. Initial retry delay is 500ms, then
1000ms, then 2000ms capped, bounded by retries and the original total deadline.
Progress resets the next retry timer to 500ms but does not extend the deadline.
A retry prefers core relay, even if the first attempt used a direct channel.
Receivers expire incomplete assemblies against an absolute assemblyMs deadline.
Duplicate current completed opens must match saved id/size/hash; older sequences
receive peer_done without redispatch. An unknown chunk/completion receives
peer_cancel (`UnknownTransfer`) so the sender cannot wait indefinitely.

Receipts return `{peer,publisherEpoch,topic,messageId,seq,route,rttMs}`; route is
the successful receipt path (`direct` or `relay`). RTT is open-to-receipt elapsed
time only for an unambiguous first attempt on the same route. Any retransmission
or receipt on a different route yields null (Karn-style ambiguity handling).
It is not application processing time or total completion time across retries.
Errors expose `code` and `outcome:"not-sent"|"unknown"` consistently.
Diagnostics API is JS `deliveryStats()`, Go `DeliveryStats()`, C# `DeliveryStats`,
returning entries keyed by peer/publisherEpoch/route/kind with
`sentFrames,receivedFrames,sentBytes,receivedBytes,sentMessages,receivedMessages,
retries,unackedMessages,queuedMessages,queuedBytes,reason,rttMs`.
Existing route snapshots remain available for latest/live; deliveryStats also
includes their per-peer route counters. queuedMessages/queuedBytes count all
retained outgoing values (active plus waiting); unackedMessages is the active
subset. Actual encoded frame bytes are counted; unmeasured RTT is null.

### Admission and lifetime details

Bulk values exceed 16384 encoded bytes. Each direction reserves one concurrency
slot for small values: bulk may use at most max(1, concurrent-1) slots. With
concurrent=1, traffic serializes. Bulk queued sends similarly use at most
max(1, queued-1) entries, and bulk queue/assembly reservations leave 16384 bytes
for small messages. Native completed application queues also reserve one entry
and 16384 bytes against bulk. Admission can fail explicitly when these bounds
are reached; priority does not imply unlimited small-message capacity.

The relay stores bounded active transfer metadata, not payload assemblies. It
derives chunk priority from the validated open size, ignoring a caller's bulk
claim. Duplicate opens cannot extend the original metadata deadline. Transport
queues and the process transfer budget still bound encoded relay payloads.

An accepted application event survives sender departure and retains its original
publisherEpoch/targetEpoch. Only pre-admission frames and assemblies are filtered
against live membership. Applications must not relabel old events as coming from
a newly joined peer with the same name. Closing the recipient ends its queue.
Retained lane highwaters hold metadata only; payload buffers are released on
terminal outcomes. Statistics are removed with departed generations and on close.
Byte limits account encoded/reserved bytes, not precise decoded heap/RSS.

To raise the logical Value limit, also raise the local/core/recipient queue and
receive budgets. Bulk admission needs an additional 16 KiB reserve, and native
application queues account the completed event envelope including metadata.
A high logical ceiling alone does not override these independent budgets.

### Public error categories

`SendTo`/`sendTo`/`SendToAsync` expose the following common `code` values.
Language-specific exception types differ, but `code` and `outcome` have the same
meaning. Configuration or invocation errors detected before transmission use
`not-sent`. After submission may have occurred, the outcome is conservatively
`unknown`. Once a sequence is assigned, its lane remains poisoned on terminal
failure even when the first frame was definitely rejected locally and the
outcome is `not-sent`. Later queued sends fail `LaneFailed`/`not-sent`.

| Code | Meaning |
| --- | --- |
| `UnsupportedCapability` | Local/core/recipient peer delivery is disabled or unsupported. |
| `PeerUnavailable` | Recipient is absent, left, or its pinned generation changed. |
| `InvalidArgument` | Invalid target/topic/options, including empty topic or sending to self. |
| `InvalidValue` | Value is outside the portable MessagePack subset. |
| `PayloadTooLarge` | Encoded Value exceeds the negotiated logical message limit. |
| `Backpressure` | Queue, receive, application admission, concurrency or retained-lane capacity exhausted. |
| `Timeout` | Caller deadline, total delivery deadline, receiver expiry or retry limit elapsed. |
| `Cancelled` | Explicit caller cancellation. |
| `Closed` | The local client or transport is closed. |
| `TransportFailed` | A transport submission failed for another reason. |
| `LaneFailed` | A previous sequence failed and this lane requires rejoin. |
| `ProtocolError` | Invalid transfer metadata, bytes, hash, credit or receipt. |

Wire diagnostics may be more specific. SDKs normalize `StaleGeneration` to
`PeerUnavailable`, `DeadlineExceeded` to `Timeout`, `TransportClosed` to
`Closed`, and malformed transfer errors (`InvalidMessage`, `DuplicateMismatch`,
`UnknownTransfer`, hash/length failures) to `ProtocolError`. Legacy `Unsupported`,
`PeerMissing`/`PeerLeft`/`PeerNotFound`/`TargetChanged`/`GenerationChanged`, and `Capacity` aliases map
to `UnsupportedCapability`, `PeerUnavailable`, and `Backpressure`; `TransportError`
maps to `TransportFailed`. Unrecognized
remote rejection codes are `ProtocolError`; the error message retains detail.
