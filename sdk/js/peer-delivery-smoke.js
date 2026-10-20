// Start the Go core, then run: node sdk/js/peer-delivery-smoke.js http://127.0.0.1:8080
import assert from "node:assert/strict";
import { KokosyncClient } from "./index.js";
import { sha256 } from "@noble/hashes/sha2.js";

const url = process.argv[2] ?? "http://127.0.0.1:8080";
const room = `delivery-${crypto.randomUUID().slice(0, 8)}`;
const clients = [];
async function connect(peer) {
  const client = await KokosyncClient.connect({ url, room, peer, transport: "websocket", timeout: 5000 });
  clients.push(client); return client;
}
async function until(check) {
  const deadline = Date.now() + 5000;
  while (!check()) {
    if (Date.now() > deadline) throw new Error("Delivery smoke timed out");
    await new Promise(resolve => setTimeout(resolve, 5));
  }
}
try {
  const a = await connect("alice"), b = await connect("bob");
  await until(() => a.routes().some(route => route.peer === "bob"));
  const got = [];
  b.on("peer_message", frame => got.push(frame));
  const bytes = Uint8Array.from({ length: 1048576 }, (_, i) => i % 251);
  const [bulk, next, small] = await Promise.all([
    a.sendTo("bob", "document", { bytes }),
    a.sendTo("bob", "document", "after"),
    a.sendTo("bob", "presence", { online: true }),
  ]);
  assert.equal(bulk.seq, 1n); assert.equal(next.seq, 2n); assert.equal(small.seq, 1n);
  assert.equal(bulk.route, "relay");
  const document = got.filter(frame => frame.topic === "document");
  assert.deepEqual(sha256(document[0].value.bytes), sha256(bytes));
  assert.equal(document[1].value, "after");
  assert.equal(a.seq, 0n); assert.equal(b.seq, 0n); // Addressed delivery does not enter room history.
  const reverse = [];
  a.on("peer_message", frame => reverse.push(frame));
  await b.sendTo("alice", "reply", { accepted: bulk.messageId });
  assert.equal(reverse[0].value.accepted, bulk.messageId);
  const abort = new AbortController();
  const cancelled = a.sendTo("bob", "cancel", null, { signal: abort.signal });
  abort.abort();
  await assert.rejects(cancelled, error => error.code === "Cancelled" && error.outcome === "not-sent");
  const oldEpoch = b.joined.publisherEpoch;
  b.close(); await until(() => !a.routes().some(route => route.peer === "bob"));
  const replacement = await connect("bob");
  await until(() => a.routes().some(route => route.publisherEpoch === replacement.joined.publisherEpoch));
  assert.notEqual(replacement.joined.publisherEpoch, oldEpoch);
  const received = [];
  replacement.on("peer_message", frame => received.push(frame));
  const fresh = await a.sendTo("bob", "document", "fresh generation");
  assert.equal(fresh.seq, 1n); assert.equal(received[0].value, "fresh generation");
  assert.ok(a.deliveryStats().some(row => row.kind === "peer_message" && row.sentMessages > 0));
  console.log("PASS: addressed 1 MiB Value/hash, independent small topic, per-topic order, receipts, reply, queued cancel, same-name rejoin, diagnostics, no room history.");
} finally { for (const client of clients) client.close(); }
