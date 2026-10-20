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

All protocol counters are `bigint`, including seq, revision, expected, offset, size, and credit. Application values reject bigint, undefined, dates, class instances, cycles, unsafe/nonfinite numbers, excess nesting, and values over 16 KiB encoded. Use plain maps, arrays, strings, booleans, numbers, null, and Uint8Array. `JSON.stringify` needs a replacer for protocol counters.

Requests return promises and reject `KokosyncError` on server errors. A timeout does not prove a write failed. Pass a stable optional request ID to `message`, `set`, or `delete` when retrying the exact same operation within the server's bounded deduplication retention. `on(type, listener)` returns an unsubscribe function. Listen to `error` and `close` in long-running applications.

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

Keep checkpoint values under 16 KiB. The server only retains 256 raw messages, so checkpoint regularly. The helper rejects on history gaps or unavailable catchup; it does not fabricate missing state. It freezes further processing after a detected gap or reducer error and reports through `error`; recreate a follower after resolving the problem. `client.seq` alone is insufficient for constructing a checkpoint because it does not establish what your reducer has applied.

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

Run `npm run smoke:js -- http://127.0.0.1:8080` for a small real-server integration check: uint64 fidelity, ordered messages, checkpoint catchup, optional state/CAS, dedup, events, latest/live, a 100 KB streamed file with SHA-256, and cancellation. It uses a new room and explicitly selects WebSocket.

This local prototype has memory-only durability and no authentication. WebRTC has no TURN/STUN configuration, so connectivity beyond the local development network is not established by this demo.
