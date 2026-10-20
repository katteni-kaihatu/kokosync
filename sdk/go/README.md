# Go QUIC SDK

From the repository root, with the relay running:

```powershell
.\.tools\go\bin\go.exe run ./examples/go --insecure-development --smoke
```

Import `github.com/katteni-kaihatu/kokosync/sdk/go` as `kokosync`.
`Connect(ctx, Options{Address, Room, Peer, InsecureDevelopment:true})` joins a raw messaging room.
Set `State:true` only to enable the optional state snapshot/cache helper.
`Publish`, `Event`, `Latest`, `Live`, `Set`, `Delete`, `Get`, `Checkpoint`, `CatchUp`, `Stats`,
`SendBlob(io.Reader,size)` and `ReceiveBlob(blobOpen,io.Writer)` cover the prototype API.
`Request`/`Send` expose the full envelope contract in `protocol/prototype.md`.

Read `Messages()` continuously; a slow consumer is disconnected when its reliable queue fills.
Messages are immutable to callers. `Get` includes tombstones (`deleted:true`) and revision.
Set success follows cache update; callbacks/consumer handling need not have finished.
The stream preserves raw-message order. QUIC DATAGRAM latest/live are separate and may be lost.
Counters are `uint64`. Application integer values must remain within ±(2^53−1).

Reliable messages, events, state, joined snapshots and checkpoint/catchup replies
fragment automatically. Configure `Options.Fragmentation` with `protocol.Limits`;
zero fields use defaults (4 MiB logical envelope, 12 KiB chunk, 8 MiB receive/send
budgets, 64 KiB credit window, four concurrent transfers, 30 second deadline).
The server selects the smaller advertised limits, visible in `Info().capabilities`.
Leave at least 1024 bytes of logical-envelope headroom when choosing a Value size.
Initial state is exposed only after full length/SHA256 validation and decoding.

Each message lane remains ordered through transfer completion. Ping/control,
state and event lanes can proceed between bulk chunks; a later message cannot
overtake an earlier large message. Latest/live stay small, unfragmented datagrams.
`Send` accepts an envelope into the bounded outgoing queue; it does not mean the
recipient has received it. Request context cancellation closes the session and
releases all transfers; an already sent operation may have committed.
Byte budgets count encoded/reserved bytes, not exact decoded-object memory/RSS.
See [fragmentation protocol](../../protocol/fragmentation.md) for server budgets.

CatchUp returns an atomic application checkpoint at R, messages R+1..`seq`, and current cutoff `seq`.
Applications that consume both CatchUp and Messages must apply the returned replay first, then ignore
queued raw messages at or below that cutoff. Checkpoint content is application-owned; the relay
cannot verify that it correctly represents R. Reconnect by calling Connect again; automatic
reconnect and durable resume are not implemented. Core requests are not
automatically retried; addressed `SendTo` uses the bounded retries described below.

Blob completion validates SHA256 after incremental sink writes. Incoming offers remain unaccepted
until ReceiveBlob is called. Context cancellation sends blob_cancel. Transfers cannot resume.
Live is a <=872-byte encoded request carrying small bytes and a relay queue deadline; it does not
negotiate codecs, decode video or implement keyframe dependencies.

`Routes()` returns immutable per-peer route snapshots with publisher generation,
route/reason, optional RTT and direct/relay counters. The Go QUIC adapter currently
reports `relay`, `local-adapter-unavailable`, and nil RTT; it does not open peer
connections. `Latest` and `Live` use the same API when other room members have
browser direct routes. Membership changes produce `type:"route"` notifications
through `Messages()` (`removed:true` on departure); initial members are available
from `Routes()` after `Connect`. Counters describe accepted local submissions and
valid received packets before duplicate suppression, not delivery guarantees.
Low-level `Send` supports protocol `to` or `exclude` selectors when needed.
See [optional direct-route protocol](../../protocol/p2p.md).


Addressed reliable delivery uses `receipt, err := client.SendTo(ctx, peer, topic,
value)` and `type:"peer_message"` on `Messages()`. Receipts mean validated SDK
queue admission; they do not acknowledge application processing. Values are
fragmented automatically (4 MiB encoded default); the context bounds queueing
and delivery. `Options.PeerDelivery` overrides `protocol.DefaultPeerLimits()`,
and `DisablePeerDelivery` opts out. `DeliveryStats()` reports peer/route/kind
counters; Go uses relay with reason `local-adapter-unavailable`. Receipt `RTTMs`
is nil after retries; it is not total completion time. Queued counters include
active retained sends; `UnackedMessages` identifies the active subset.

Cancellation before a sequence is assigned is `not-sent`. Active failure has
`unknown` outcome and poisons that peer-generation/topic lane until rejoin;
other topics proceed. Read `*DeliveryError.Code` and `.Outcome`; codes use the
shared [error table](../../protocol/peer-delivery.md#public-error-categories).
Context deadline expiry is `Timeout`; explicit cancellation is `Cancelled`. Receiver events
retain immutable epochs and remain available after sender departure once
accepted. See [peer delivery protocol](../../protocol/peer-delivery.md) for
limits, priority reservations, deduplication and reconnection semantics.
