# JavaScript / TypeScript prototype SDK

Install dependencies with `npm ci` at the repository root. The SDK is plain ESM with TypeScript declarations alongside it. Node 22+ provides the native WebSocket and Web Crypto APIs; development is checked on Node 26. Browser ESM uses the import map in `examples/browser/index.html`.

```js
import { KokosyncClient } from "./sdk/js/index.js";

const client = await KokosyncClient.connect({
  url: "http://127.0.0.1:8080",
  room: "example",
  peer: "alice", // Unique within the room; a display name, not authentication.
  transport: "websocket", // Required explicit fallback in Node.
});

client.on("message", ({ topic, value, seq }) => console.log(topic, value, seq));
await client.message("move", { x: 0.25, y: 0.5 });
await client.event("wave", { emoji: "hello" }, 1000);
client.latest("cursor", { x: 0.25, y: 0.5 }, 1n);
client.live("custom-bytes", new Uint8Array([1, 2, 3]), 1n, 250, "raw");
console.log(await client.stats());
client.close();
```

Browser transport defaults to `webrtc`. It gathers a local SDP offer and posts `/offer`, using `kokosync` for reliable ordered operations and `kokosync-realtime` for unordered, unreliable latest/live updates. There is no automatic downgrade. Select `transport: "websocket"` explicitly to use `/ws`. Inspect `client.capabilities` to distinguish datagram support; neither adapter promises a bounded latest-delivery window. Client realtime frames must fit in 872 encoded bytes, reserving 128 of the 1000-byte wire budget for relay metadata. Receive-side stale suppression includes the server's publisher generation, so reconnecting under the same peer name can restart its sequence counter.

Application-visible protocol counters are `bigint`, including seq, revision, expected and blob offset/size/credit. Application values reject bigint, undefined, dates, class instances, cycles, unsafe/nonfinite numbers, and excess nesting. Use plain maps, arrays, strings, booleans, numbers, null, and Uint8Array. `JSON.stringify` needs a replacer for protocol counters.

## Large messages and snapshots

Reliable operations transparently negotiate fragmentation. The default limit is **4 MiB for the complete encoded envelope**, including topic, keys and other metadata. Leave headroom in application values. `message`, `event`, `set`, checkpoints, catchup and complete state snapshots use the same APIs for small and large values:

```js
await client.message("image-data", { bytes: new Uint8Array(1024 * 1024) });
await client.set("large-document", { text: "x".repeat(1024 * 1024) });
```

Bytes are hashed before splitting and verified before decoding or dispatch. A joining client's state remains unexposed until the entire snapshot is received. Messages and checkpoints share a FIFO lane; state and events have independent lanes, so an active large message does not prevent those operations or pings from progressing. A ping is therefore not a barrier for prior message delivery. `follow` still applies every raw message in sequence and never receives partial values.

Use `fragmentation` in `connect` to configure limits; the server selects limits no larger than those advertised. `client.capabilities.fragmentation` reports the selection.

```js
const client = await KokosyncClient.connect({
  room: "example", transport: "websocket",
  fragmentation: {
    logicalBytes: 4 * 1024 * 1024, chunkBytes: 12288,
    reassemblyBytes: 8 * 1024 * 1024, inflightBytes: 65536,
    concurrent: 4, timeoutMs: 30000, queueBytes: 8 * 1024 * 1024,
  },
});
```

These are the defaults. Outbound encoded data counts toward `queueBytes` until acknowledged, with at most 256 queued envelopes. Inbound transfers reserve their complete size within `reassemblyBytes`; credits bound transmission in flight. These budgets count encoded and reserved bytes, not total process memory or decoded JavaScript object overhead. Transfers expire `timeoutMs` after opening, regardless of progress. Corruption, a cancelled or expired reliable transfer, and transport failure close the session to avoid silently losing ordered data. Closing releases queued data and reassembly buffers. Request `timeout` remains separate from transfer `timeoutMs`; request timeout also closes the session.

Pass `{ signal, id? }` instead of the optional request-ID string to `message` or `set` to support local cancellation. The low-level `request` method also takes `{ signal }` as its fourth argument. Cancelling an active request closes the connection and rejects outstanding requests, preserving the rule that reliable operations cannot be silently skipped:

```js
const abort = new AbortController();
const sending = client.message("large", bytes, { signal: abort.signal });
// abort.abort() cancels the operation and closes this client.
await sending;
```

Set `fragmentation: false` for the legacy protocol. If a server omits the capability, the SDK retains the legacy 16 KiB application-value and 64 KiB envelope limits. `latest` and `live` always stay small and unfragmented. Existing directed blob transfers retain their own streaming/credit protocol; they are the better fit for files that should not be held as whole in-memory values.

Requests return promises and reject `KokosyncError` on server errors. A timeout does not prove a write failed. Pass a stable optional request ID to `message`, `set`, or `delete` when retrying the exact same operation within the server's bounded deduplication retention. `on(type, listener)` returns an unsubscribe function. Listen to `error` and `close` in long-running applications.

## Optional direct latest/live routes

Browsers attempt direct WebRTC routes for at most two compatible peers by default. The ordinary `latest` and `live` methods use a healthy direct route, with relay fallback for every other recipient. Messages, state, snapshots and checkpoints always use the core. Node without a WebRTC adapter, Go and C# remain on relay.

```js
const client = await KokosyncClient.connect({
  room: "example",
  routing: {
    enabled: true, maxPeers: 2, // 0..8
    peers: ["bob"], // optional: limit candidates to these names
    iceServers: [], // optional RTCIceServer entries, including your own STUN/TURN
  },
});
client.on("route", route => console.log(route.peer, route.route, route.reason, route.rttMs));
console.table(client.routes());
client.setRouting({ enabled: false }); // immediately return to relay
client.setRouting({ enabled: true, peers: ["bob"] });
```

The advertised WebRTC capability allows up to eight peers even when local routing is disabled, so changing policy does not require a rejoin. Actual connections respect the local `maxPeers` and optional `peers` list on both ends. Selection follows configured policy and availability, not a comparison for the lowest latency. There is no required full mesh. By default, no external ICE servers are configured, so NAT traversal is not guaranteed. A `direct` route means peer WebRTC bypasses the Kokosync core; it may still use a TURN relay if one is supplied in `iceServers`.

Connection setup times out after 10 seconds. Direct heartbeat RTT is measured every two seconds; a missing response for six seconds, transport failure, or excess buffered data immediately falls back to relay. Failed routes retry after a 15-second cooldown. Signals are bound to current room membership and publisher generation. Disabling, removing a peer, replacing its generation or closing the client cleans up obsolete connections and callbacks.

Each send submits one relay copy excluding successfully used direct recipients. If selector metadata would exceed the small-frame limit, the relay copy is broadcast; the shared publisher-generation/sequence deduplicator drops duplicate application delivery. Direct receivers validate size, identity, generation, Value, operation and uint64 sequence, and cannot inject reliable operations.

Healthy direct submissions happen before the relay write. If relay buffering is full, the call reports relay backpressure even when a direct recipient was successfully sent its copy. Both paths reject delayed data from departed publisher generations when authoritative membership is available.

`routes()` entries include `sentDirect`, `receivedDirect`, `sentRelay` and `receivedRelay`; `stats().routing` returns session totals. TX counts describe recipient copies submitted to a transport, and RX counts describe valid transport receptions **before** duplicate suppression. They are diagnostics, not delivery guarantees. `reason` explains relay use, including disabled policy, unselected peers, missing adapters, congestion, connection failure and heartbeat timeout.

## Addressed reliable values

`sendTo` sends one portable Value to one current peer, including large byte arrays. It uses the reliable, ordered `kokosync-peer-delivery` channel when that channel is already available on the selected peer connection. Otherwise it immediately uses the core relay. It does not wait for a direct connection, broadcast to the room, assign a room message sequence, or update state/checkpoints.

```js
client.on("peer_message", ({ peer, topic, messageId, seq, value }) => {
  console.log("Complete verified value", peer, topic, messageId, seq, value);
});
const receipt = await client.sendTo("bob", "document", {
  bytes: new Uint8Array(1024 * 1024),
}, { timeoutMs: 30000 });
console.log(receipt.messageId, receipt.seq, receipt.route, receipt.rttMs);
```

A receipt means the receiver SDK validated the complete byte count, SHA-256 and Value, accepted its order, and admitted one `peer_message` notification. It does not mean an application listener finished, saved data, or applied an operation. Send an application response carrying your own stable event ID when those guarantees are required.

Order is independent for each recipient generation and topic. Only one message in each lane waits for a receipt; other topics and recipients can progress. Retries preserve the same message ID and sequence, use cumulative byte credits, and prefer the relay. A lost receipt does not redispatch an accepted message. Retry delays start at 500 ms, then 1000 ms and at most 2000 ms; progress resets the next delay to 500 ms without extending the original deadline.

An active direct transfer waits through temporary local channel backpressure, pausing its retry timer until buffer space returns while retaining the original overall deadline. A lost direct channel triggers relay fallback. A new send uses relay immediately when its direct channel is already congested.

```js
const abort = new AbortController();
try {
  await client.sendTo("bob", "document", value, { signal: abort.signal });
} catch (error) {
  console.log(error.code, error.outcome); // "not-sent" or "unknown"
}
console.table(client.deliveryStats());
```

Cancelling or timing out a queued send removes it before sequence assignment. A terminal failure after activation poisons that lane; later queued sends reject with `LaneFailed`. An active cancellation cannot retract data that the recipient already accepted. An `unknown` outcome must not be retried as a new application operation without application-level deduplication. Rejoin creates new publisher generations and fresh lanes; the application decides how to resynchronize. Recipients are pinned when `sendTo` is called, so replacing a peer with the same name never retargets an existing send.

Public `DeliveryError.code` values are `UnsupportedCapability`, `PeerUnavailable`, `InvalidArgument`, `InvalidValue`, `PayloadTooLarge`, `Backpressure`, `Timeout`, `Cancelled`, `Closed`, `TransportFailed`, `LaneFailed`, and `ProtocolError`. Missing or departed recipient generations use `PeerUnavailable`; malformed arguments use `InvalidArgument`; nonportable local values use `InvalidValue`. Wire aliases normalize to the same categories: `DeadlineExceeded` becomes `Timeout`, `TransportClosed` becomes `Closed`, and `StaleGeneration` becomes `PeerUnavailable`. Invalid wire payloads, hash or duplicate conflicts, unknown transfers, and unrecognized remote codes become `ProtocolError`. These categories preserve the separate `not-sent` or `unknown` outcome.

`connect({ peerDelivery: { ... } })` configures the following defaults. Set `peerDelivery: false` to disable the service.

```js
{
  version: 1, messageBytes: 4194304, chunkBytes: 12288, windowBytes: 65536,
  queueBytes: 8388608, receiveBytes: 8388608, concurrent: 4, queued: 128,
  lanes: 64, timeoutMs: 30000, assemblyMs: 30000, retries: 5,
}
```

Message, chunk, window, timeout and retry limits use the minimum of sender, receiver and core capabilities. Queue and receive budgets remain local admission limits. Payload bytes are encoded once and reserved until completion or failure. At most 128 sends, including active sends, are queued; transmit and receive lanes share the 64-lane cap. Live-generation high-water marks are retained rather than evicted. A recipient departure releases its lanes and reservations.

Values larger than 16 KiB use the bulk class. Bulk admission reserves one transfer slot, one queued-send slot and 16 KiB of each byte budget for small values in other topics; configuring concurrency to one serializes transfers. Frame scheduling prioritizes controls and small values. These bounds cover encoded payloads and reservations, not decoded object memory or network latency.

`deliveryStats()` reports entries by peer generation, route and kind (`peer_message`, `peer_bulk`, `latest`, `live`): frame/message counts, encoded bytes, retries, unacknowledged and queued work, route reason and RTT. Reliable `sentMessages` counts received receipts and `receivedMessages` counts successful local admissions; frame counts include retransmissions. Reliable delivery RTT measures the open-to-receipt round trip only when no retransmission occurred and the receipt returned on the same route as the open. Retransmitted transfers, receipts returning on another route, and other unknown measurements report `null`; counters describe transport submissions/receptions, not application execution.

Byte counters include the actual encoded physical envelopes. Relay sends attribute the submitted envelope size to each intended peer; the core adds publisher metadata before forwarding, so received sizes can differ. `queuedBytes` and `queuedMessages` count all retained sends, including waiting and active payloads; `unackedMessages` is the active subset. The configured queue byte limit covers the same retained payloads.

## Optional state cache

Pass `state: true` to join with the state snapshot and subscribe to subsequent state revisions.

```js
await client.set("title", "Hello", 0n); // expected=0 for a new key
const entry = client.state.get("title");
await client.set("title", "Updated", entry.revision);
console.log(client.get("title")); // undefined for a deleted or absent key
await client.delete("title", client.state.get("title").revision);
await client.snapshot(); // refreshes the cache; tombstones remain in state
```

State operations and raw message sequence numbers are independent.

## Optional application checkpoints

`follow` subscribes before requesting catchup, restores the application checkpoint, applies contiguous operations, and discards duplicate live delivery. It advances through **every** raw message, including topics ignored by your reducer. Reducers and capture must be synchronous.

While catchup is pending, `follow` buffers at most 256 live messages and no more encoded bytes than the smaller of `queueBytes` and `reassemblyBytes` (8 MiB in legacy mode). Overflow clears the buffer, closes the client, and rejects initialization with `ResyncRequired`; reconnect and synchronize again. Initialization failure, disconnection, and follower stop release buffered messages.

```js
let total = 0;
const follower = await client.follow({
  restore: value => { total = value?.total ?? 0; },
  apply: message => { if (message.topic === "add") total += message.value; },
  capture: () => ({ total }),
});
await client.message("add", 2);
await follower.checkpoint();
console.log(follower.seq, total);
follower.stop();
```

Keep checkpoints and the resulting catchup envelope within the negotiated logical limit (legacy checkpoint values remain under 16 KiB). The server retains a bounded raw-message history, so checkpoint regularly. The helper rejects on history gaps or unavailable catchup; it does not fabricate missing state. It freezes further processing after a detected gap or reducer error and reports through `error`; recreate a follower after resolving the problem. `client.seq` alone is insufficient for constructing a checkpoint because it does not establish what your reducer has applied.

## Streaming file transfer

```js
client.on("blob_open", async offer => {
  // Supply a storage sink suitable for your environment.
  await client.acceptBlob(offer, {
    async write(bytes) { await destination.write(bytes); },
    async close() { await destination.close(); },
    async abort(error) { await destination.abort(error); },
  });
});

const abort = new AbortController();
await client.sendBlob("bob", fileOrAsyncByteIterable, {
  size: 1048576n, // optional for Blob and Uint8Array
  signal: abort.signal,
});
```

Credits are granted only after each sink write. Both directions compute SHA-256 incrementally. A receiver sends completion only after byte count/hash validation and sink close. Use a temporary or transactional sink when unverified bytes must not be visible. `cancelBlob(transfer)` cancels either side; an AbortSignal cancels sending. The receiver must explicitly accept or cancel each offer. Transfers are bounded to 64 MiB and four concurrent transfers per room, with no reconnect/resume. The SDK does not buffer whole transfers; the browser demo deliberately collects files in memory and limits them to 5 MiB.

## Run and check

With the repository server running from the repository root, visit `http://127.0.0.1:8080/`. Open a second window, join the same room with another name, and draw. The canvas uses only raw operations plus application checkpoints, retaining the latest 120 marks. Blue/coral/green/gold choose ink; cursors use latest updates. A new window catches up from the last checkpoint. The file-transfer panel accepts the other window's name.

The browser's “Send a large message” panel sends 256 KiB, 1 MiB or 2 MiB through the ordinary message API and verifies byte count and SHA-256 in every receiving browser.

Run `node --test sdk/js/fragmentation.test.js` for light, in-memory protocol tests. Run `npm run smoke:js -- http://127.0.0.1:8080` for a real-server integration check: uint64 fidelity, ordered messages, checkpoint catchup, optional state/CAS, dedup, events, latest/live, a 100 KB streamed file with SHA-256, cancellation, and 1 MiB messages/state/joined snapshots/catchup. It uses a new room and explicitly selects WebSocket.

Run `node --test sdk/js/peer-delivery.test.js sdk/js/routing.test.js` for addressed delivery fault tests and deterministic direct-route tests. `node sdk/js/peer-delivery-smoke.js http://127.0.0.1:8080` exercises real relay delivery of a 1 MiB Value, independent small messages, receipts, replies, cancellation and same-name rejoin. The relay smoke was verified against the local core on port 8180.

This local prototype has memory-only durability and no authentication. Peer WebRTC supports STUN/TURN configuration through `routing.iceServers`, with none configured by default. Connectivity beyond the local development network is not established by this demo.
