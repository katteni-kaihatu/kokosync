# Fragmented reliable envelopes (prototype v1)

An optional `fragmentation` map in the small, unfragmented `join` request enables
transparent fragmentation. Fields (positive integers) and defaults:
`logicalBytes=4194304`, `chunkBytes=12288`, `reassemblyBytes=8388608`,
`inflightBytes=65536`, `concurrent=4`, `timeoutMs=30000`, `queueBytes=8388608`.
The server returns the selected map as `joined.capabilities.fragmentation`.
Each selected field is the smaller local and advertised limit. Chunk bytes are
at most 16384. Clients initialize their receiver before sending join, allowing
the joined envelope itself to be fragmented before capabilities arrive.
Absent capability retains legacy 65536-byte envelopes.

Encode a complete reliable envelope once. If its byte length exceeds the selected
chunkBytes, send these ordinary MessagePack maps (`v:1` on every map):

* `frag_open {transfer:string,size:uint,sha256:string}` (lowercase hex SHA256)
* Receiver replies `frag_credit {transfer,credit:uint}` with at most inflightBytes.
* `frag_chunk {transfer,offset:uint,value:bytes}` with contiguous zero-based
  offsets and at most chunkBytes; deduct credit before sending.
* Receiver copies each valid chunk into its reserved bounded buffer and replies
  with `frag_credit` for the consumed bytes.
* `frag_end {transfer}` after exactly size bytes.
* Receiver validates length and SHA256, decodes the original bytes once, dispatches
  exactly one logical envelope, and replies `frag_done {transfer}`.
* `frag_cancel {transfer,reason:string}` releases either direction. Invalid input,
  expiry, local cancellation and disconnect release buffers. A lost reliable
  envelope is fatal to the session: close rather than silently skip ordered data.

No nested fragmentation or fragmented latest/live is allowed. Fragment IDs are
unique per connection and direction. Credits for a completed transfer may arrive
late and are ignored. Active inbound reservations count full declared size against
reassemblyBytes, with at most concurrent reservations. Outbound queued and active
encoded envelopes count against queueBytes. All transfers expire timeoutMs after
open; progress does not extend this deadline.

Existing bounded `blob_*` envelopes up to 65536 bytes bypass this fragmentation
layer; their streaming API already implements chunking and credit.

Senders keep FIFO queues per semantic lane: `message`, `catchup`, `checkpoint`, and acknowledgments
with `seq` use the message lane; `set`, `delete`, `state`, `snapshot`, and acknowledgments
with `revision` use the state lane; `event` uses the event lane. Other operations
are control. A large envelope occupies its lane through frag_done. Control and
other lanes proceed between chunks; physical transports still share congestion.
The initial joined envelope is a barrier ahead of all subsequent non-control
lanes. Its complete snapshot is captured with revision and membership registration
under the core lock; subsequent updates queue behind it. Nothing partial is exposed.
Later message sequence numbers never overtake an earlier large message.

The logical limit includes envelope metadata. Applications should leave metadata
headroom in values; `latest`/`live` remain small and unfragmented. This is an in-memory
bounded Value mechanism; the separate directed blob API remains the streaming API.

## Server storage and aggregate budgets

`kokosync -limits config/limits.example.json` or `KOKOSYNC_LIMITS` selects a JSON
configuration. Omitted fields retain defaults. The top-level `fragmentation` map
overrides the fields above. Other top-level byte budgets are `stateBytes=2097152`,
`checkpointBytes=2097152`, `historyBytes=4194304`, `dedupBytes=4194304`,
`storageBytes=67108864`, and `transferBytes=67108864`. The first four apply to each
room. `storageBytes` bounds retained encoded room data across the core; `transferBytes`
bounds combined queued outbound and reserved inbound encoded buffers across all
peers. Queues additionally permit at most 256 logical envelopes per connection.
These are encoded/reserved byte budgets, not an exact bound on process RSS or
decoded object overhead. State has at most 256 keys, history 256 messages, dedup
512 operations, each room 32 peers and the server 64 rooms.

History and dedup evict oldest records to meet their byte/count caps. A checkpoint
too old for retained history yields `ResyncRequired`. State/checkpoint and aggregate
storage overflow reject new operations. Broadcast recipient logical/queue checks
precede state or message mutation; an unexpected delivery failure disconnects the
affected peer. A catchup response or total initial snapshot still must fit the
negotiated logical limit, independently from retained history capacity.

During the initial joined barrier the server drops ephemeral latest/live delivery.
All state and message data waits behind the complete joined envelope. A request
deadline or active cancellation closes the SDK connection and releases transfers;
an already dispatched operation may have committed, so its outcome is unknown.
