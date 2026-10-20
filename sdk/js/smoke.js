// Start the Go server first. Run: npm run smoke:js -- http://127.0.0.1:8080
import assert from "node:assert/strict";
import { KokosyncClient, validateValue } from "./index.js";

const url = process.argv[2] ?? "http://127.0.0.1:8080";
const room = `js-smoke-${crypto.randomUUID().slice(0, 8)}`;
const clients = [];
function next(client, type, predicate = () => true) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => { off(); reject(new Error(`Missing ${type}`)); }, 5000);
    const off = client.on(type, value => { if (predicate(value)) { clearTimeout(timer); off(); resolve(value); } });
  });
}
async function connect(peer) {
  const client = await KokosyncClient.connect({ url, room, peer, state: true, transport: "websocket", timeout: 5000 });
  clients.push(client); return client;
}
try {
  for (const invalid of [1n, NaN, Infinity, undefined, new Date()]) assert.throws(() => validateValue(invalid));
  const a = await connect("alice"), b = await connect("bob");
  assert.equal(typeof a.joined.seq, "bigint");
  let sum = 0;
  const follower = await b.follow({
    restore: value => { sum = value?.sum ?? 0; },
    apply: frame => { if (frame.topic === "add") sum += frame.value; },
    capture: () => ({ sum }),
  });
  await a.message("add", 3);
  await a.message("unrelated", { bytes: new Uint8Array([1, 2, 3]) });
  await a.message("add", 4);
  // Ordered ping on b puts prior room delivery ahead of pong on b's stream.
  await b.ping();
  assert.equal(sum, 7); assert.equal(follower.seq, 3n);
  await follower.checkpoint();
  const c = await connect("carol");
  let restored = 0;
  const recovered = await c.follow({
    restore: value => { restored = value?.sum ?? 0; },
    apply: frame => { if (frame.topic === "add") restored += frame.value; },
    capture: () => ({ sum: restored }),
  });
  await a.message("add", 5); await c.ping();
  assert.equal(restored, 12); assert.equal(recovered.seq, 4n);
  const write = await a.set("title", "hello", 0n, "write-once");
  const duplicate = await a.set("title", "hello", 0n, "write-once");
  assert.equal(write.revision, duplicate.revision);
  await assert.rejects(a.set("title", "wrong", 0n), error => error.code === "Conflict");
  await b.snapshot(); assert.equal(b.get("title"), "hello");
  const ephemeral = next(b, "event", frame => frame.topic === "wave");
  await a.event("wave", { hello: true });
  assert.deepEqual((await ephemeral).value, { hello: true });
  const latest = next(b, "latest");
  a.latest("cursor", { x: 0.5 }, 9007199254740993n);
  assert.equal((await latest).seq, 9007199254740993n);
  const live = next(b, "live");
  a.live("raw", new Uint8Array([4, 5, 6]), 1n);
  assert.deepEqual((await live).value, new Uint8Array([4, 5, 6]));
  let received;
  let receivedDone;
  b.on("blob_open", offer => {
    if (offer.transfer === "cancel-smoke") { b.cancelBlob(offer.transfer); return; }
    const chunks = [];
    receivedDone = b.acceptBlob(offer, {
      write: async chunk => { await new Promise(resolve => setTimeout(resolve, 1)); chunks.push(chunk); },
      close: () => { received = Buffer.concat(chunks); },
    });
  });
  const payload = Uint8Array.from({ length: 100000 }, (_, i) => i % 251);
  await a.sendBlob("bob", payload);
  await receivedDone;
  assert.deepEqual(received, Buffer.from(payload));
  await assert.rejects(a.sendBlob("bob", payload, { transfer: "cancel-smoke" }), /cancel/i);
  console.log("PASS: uint64, ordered messages, checkpoint catchup, CAS, dedup, event, latest, live, streaming SHA-256 blob, cancellation.");
} finally {
  for (const client of clients) client.close();
}
