# Executable prototype protocol (v1)

This document describes implemented scope; the v0 documents in `docs/` remain the larger design target.
Core is arbitrary ordered messaging. State and checkpoint helpers are opt-in. Memory durability only.
Optional browser peer routes for latest/live, authoritative membership, directed
relay selectors and native route diagnostics are specified in [p2p.md](p2p.md).

All messages are MessagePack string-key maps. `v:1`, `type:string`, optional `id:string` correlate requests.
Protocol counters (`seq`, `revision`, `expected`, `offset`, `size`, `credit`) encode as uint64, JS bigint.
Application `value` is null/bool/finite safe numeric/string/bytes/arrays/string maps, maximum nesting 32.
Transport frame max 65536 bytes. Negotiated reliable logical envelopes default to 4 MiB, fragmented into 12288-byte chunks. Leave 1024 bytes of envelope headroom for application values. Blob chunks max 12288 bytes.
See [fragmentation.md](fragmentation.md) for capability negotiation, byte budgets, SHA256 validation and cancellation.
QUIC uses one bidirectional stream, uint32 big-endian length prefixed frames, ALPN `kokosync/1`.
WebRTC uses a reliable ordered DataChannel named `kokosync` and `kokosync-realtime` (unordered, maxRetransmits=0) for latest/live.
QUIC uses DATAGRAM for latest/live. LiteNetLib uses Unreliable for latest/live. Encoded realtime frames max 1000 bytes.
Client realtime requests are limited to 872 encoded bytes, reserving 128 bytes for relay metadata.
WebSocket uses binary frames at `/ws` and has explicit degraded reliable-only semantics.

Local development endpoints: HTTP `127.0.0.1:8080`; QUIC `127.0.0.1:8081`; LiteNetLib UDP `127.0.0.1:8082`.
HTTP POST `/offer` takes standard JSON RTCSessionDescriptionInit and returns gathered SDP answer.
GET `/health` health; GET `/` browser example. LiteNetLib sidecar connects per client to `/bridge`
using header `X-Kokosync-Bridge: <secret>` (server flag/environment config; default `local-dev-only`).
The bridge requires loopback. Sidecar uses official LiteNetLib ReliableOrdered except latest/live; one frame/message.
LiteNetLib clients must explicitly opt into insecure localhost development. QUIC uses generated dev TLS certificate;
SDK localhost-only dev TLS opt-in. No production authentication claim.

First request `{v:1,type:"join",id,room,peer,state:false,fragmentation?:<limits map>}`. New SDKs advertise their receive/send limits; legacy clients omit the map and retain 65536-byte frames.
`peer` is a display identifier, not authenticated identity, must be unique within room.
Reply `{type:"joined",id,room,peer,epoch,seq,revision,durability:"memory",capabilities:{transport,reliableOrdered:true,datagram:<adapter-dependent>,boundedLatest:false,insecureDevelopment:true,maxFrame:65536,maxRealtimeFrame:1000,blobResume:false}}`.
`joined` also carries a random `publisherEpoch` for this connection. Relayed latest/live frames carry it too;
receivers deduplicate by peer + publisherEpoch + kind + key/stream, so reconnecting with the same peer name can restart sequence numbers.
If `state:true`, reply also contains `entries` array `{key,value,revision,deleted}` at exactly the returned revision.
The join snapshot and delivery subscription are atomic. The joined logical envelope is a barrier ahead of later state/message data, even when fragmented. Initial latest/live is dropped.
Error `{type:"error",id,code,message}`; generic success `{type:"ack",id,seq?,revision?}`.

Requests after join:

* `message {id,topic,value}` -> room-wide (including sender) `{type:"message",id,peer,topic,value,seq}` and ack. Each accepted raw message increments room `seq`. No replay unless checkpoint helper is requested.
* `event {id,topic,value,ttlMs?}` -> current members receive `{type:"event",id,peer,topic,value}`; ack. No history. TTL limited to queued residence (not a wall-clock end-to-end promise).
* `latest {id?,key,value,seq}` -> current members `{type:"latest",peer,key,value,seq}`; stale per-peer/key sequences discarded. No bounded latency guarantee; WebSocket is degraded.
* `live {id?,stream,value:bytes,seq,ttlMs,codec?}` -> current members same metadata + peer, queued-expiry dropping. Single small frame, encoded bytes only; no codec negotiation, fragmentation, decoder or dependency support.
* `set {id,key,value,expected?}` or `delete {id,key,expected?}` -> state subscribers `{type:"state",key,value?,deleted,revision}` then ack. Expected is per-key revision; new key = 0. Tombstone retained. `Conflict` on mismatch. Idempotency by room+peer+id while retained; same ID/different request -> `DuplicateMismatch`.
* `snapshot {id}` -> `{type:"snapshot",id,revision,entries}`. Requires state opt-in.
* `checkpoint {id,seq,value}` -> saves application checkpoint at raw operation seq R if retained log can cover R+1..current; ack. Caller is responsible for deriving correct snapshot at R.
* `catchup {id}` -> `{type:"catchup",id,seq:<current>,checkpoint:{seq,value},messages:[ordered raw messages after checkpoint seq]}`. Captured atomically. Without checkpoint defaults `{seq:0,value:null}` if complete history retained. `ResyncRequired` if history gap. Requests and subsequent messages use same ordered outbound queue; application ignores duplicates <= catchup seq. Log bounded to 256 raw messages and 4 MiB by default; checkpoint encoded bytes <= 2 MiB by default.
* `ping {id}` -> `{type:"pong",id}`.
* `stats {id}` -> `{type:"stats",id,queuedBytes,sentBytes,receivedBytes,dropped,transport,rttMs:null}`. Numbers here are diagnostic safe numbers.

Blob flow is peer-to-peer via relay, not stored:
`blob_open {id,transfer,to,size}` -> receiver `{type:"blob_open",transfer,peer,size}` + sender ack.
Receiver `blob_credit {transfer,credit}` -> sender same + peer. Credit is incremental and capped at 65536 outstanding.
Sender `blob_chunk {transfer,offset,value:bytes}` -> receiver same + peer; offset contiguous; charge credit.
Receiver grants further credit only after sink write. Sender `blob_end {transfer,sha256:<lowercase hex>}` -> receiver same.
Receiver validates byte count/hash and sink completion then `blob_done {transfer}` -> sender same; relay removes transfer.
Either side `blob_cancel {transfer}` -> opposite peer same and removes transfer.
Maximum declared transfer 64 MiB, 4 active transfers/room; no resume. Hash verification is receiver SDK responsibility.
Sender must await open ack before chunks, receiver must grant credit first.

Rooms max 64, peers max 32/room, state keys max 256/room, op dedup max 512/room. Reliable outbound queues default to 8 MiB / 256 logical envelopes. State/checkpoint/history/dedup and aggregate server storage/transfer bytes have configurable limits detailed in fragmentation.md.
Slow reliable receivers are disconnected (never silently skip ordered/state messages); latest/live may drop.
State snapshots/catchup are transparently fragmented but the complete encoded response must fit negotiated logicalBytes, otherwise PayloadTooLarge is returned. Room memory lives until server restart.
Message, state and event lanes remain FIFO individually. Control/ping and other lanes progress between chunks; a later ordered message never overtakes an earlier message. Ping is not an application-delivery barrier. Event TTL expires in the server queue before transfer starts; an already started reliable transfer completes atomically.
