import assert from "node:assert/strict";
import test from "node:test";
import { encode, decode } from "@msgpack/msgpack";
import { sha256 } from "@noble/hashes/sha2.js";
import { Fragmentation, fragmentationLimits } from "./fragmentation.js";
import { KokosyncClient } from "./index.js";

const codec = { useBigInt64: true };
const hex = bytes => Buffer.from(sha256(bytes)).toString("hex");
const pause = ms => new Promise(resolve => setTimeout(resolve, ms));
async function until(check) {
  for (let i = 0; i < 1000; i++) { if (check()) return; await pause(2); }
  throw new Error("Test delivery timed out");
}
function pair(overrides = {}) {
  const received = [], failures = [], wire = [];
  const limits = fragmentationLimits(overrides);
  let a, b;
  const connect = (name, target) => bytes => {
    const frame = decode(bytes, codec); wire.push({ name, frame });
    queueMicrotask(() => { if (!target().closed) target().receive(frame); });
  };
  a = new Fragmentation({ limits, write: connect("a", () => b), buffered: () => 0,
    deliver: frame => received.push({ at: "a", frame }), fail: error => failures.push(error) });
  b = new Fragmentation({ limits, write: connect("b", () => a), buffered: () => 0,
    deliver: frame => received.push({ at: "b", frame }), fail: error => failures.push(error) });
  return { a, b, received, failures, wire, close() { a.close(); b.close(); } };
}
function send(engine, fields) {
  const frame = { v: 1, ...fields };
  engine.enqueue(frame, encode(frame, codec));
}

test("large exact-byte transfer retains uint64, lane order and other-lane progress", async () => {
  const p = pair({ inflightBytes: 32768 });
  try {
    const value = Uint8Array.from({ length: 1048576 }, (_, i) => i % 251);
    send(p.a, { type: "message", seq: 9007199254740993n, value });
    send(p.a, { type: "message", seq: 9007199254740994n, value: "after" });
    send(p.a, { type: "checkpoint", seq: 9007199254740994n, value: "checkpoint" });
    send(p.a, { type: "set", value: "state" });
    send(p.a, { type: "event", value: "event" });
    send(p.a, { type: "ping" });
    await until(() => p.received.length === 6 && p.a.queuedBytes === 0);
    assert.deepEqual(p.failures, []);
    const frames = p.received.map(x => x.frame);
    const first = frames.findIndex(x => x.type === "message");
    for (const type of ["set", "event", "ping"]) assert.ok(frames.findIndex(x => x.type === type) < first);
    assert.deepEqual(frames.slice(first).map(x => x.type), ["message", "message", "checkpoint"]);
    assert.equal(frames[first].seq, 9007199254740993n);
    assert.deepEqual(frames[first].value, value);
    assert.equal(hex(frames[first].value), hex(value));
    const open = p.wire.find(x => x.name === "a" && x.frame.type === "frag_open").frame;
    const encoded = encode({ v: 1, type: "message", seq: 9007199254740993n, value }, codec);
    assert.equal(open.sha256, hex(encoded));
    assert.equal(p.b.reservedBytes, 0);
  } finally { p.close(); }
});

test("corrupt hash closes session without partial dispatch and releases reservations", () => {
  const p = pair();
  try {
    const bytes = encode({ v: 1, type: "message", value: "invisible" }, codec);
    p.b.receive({ v: 1, type: "frag_open", transfer: "bad", size: bytes.length, sha256: "0".repeat(64) });
    p.b.receive({ v: 1, type: "frag_chunk", transfer: "bad", offset: 0n, value: bytes });
    p.b.receive({ v: 1, type: "frag_end", transfer: "bad" });
    assert.equal(p.b.closed, true);
    assert.equal(p.b.reservedBytes, 0);
    assert.equal(p.received.length, 0);
    assert.match(p.failures[0].message, /SHA-256/);
  } finally { p.close(); }
});

test("bounds reject full queues, overlapping offsets and excess reservations", () => {
  const p = pair({ queueBytes: 100, reassemblyBytes: 100, concurrent: 1 });
  try {
    assert.throws(() => send(p.a, { type: "message", value: "x".repeat(100) }), /queue/);
    assert.equal(p.a.queuedBytes, 0);
    p.b.receive({ v: 1, type: "frag_open", transfer: "one", size: 70, sha256: "0".repeat(64) });
    p.b.receive({ v: 1, type: "frag_open", transfer: "two", size: 70, sha256: "0".repeat(64) });
    assert.equal(p.b.closed, true); assert.equal(p.b.reservedBytes, 0);
  } finally { p.close(); }
  const q = pair();
  try {
    q.b.receive({ v: 1, type: "frag_open", transfer: "one", size: 70, sha256: "0".repeat(64) });
    q.b.receive({ v: 1, type: "frag_chunk", transfer: "one", offset: 1, value: new Uint8Array(3) });
    assert.equal(q.b.closed, true); assert.equal(q.received.length, 0);
  } finally { q.close(); }
});

test("credit starvation expires, cancellation and disconnect clear all buffers", async () => {
  const p = pair({ timeoutMs: 20 });
  try {
    p.b.write = () => {};
    send(p.a, { type: "message", value: new Uint8Array(50000) });
    await until(() => p.a.closed);
    assert.equal(p.a.queuedBytes, 0);
    assert.match(p.failures[0].message, /timed out/);
  } finally { p.close(); }
  const q = pair();
  send(q.a, { type: "message", value: new Uint8Array(50000) });
  q.a.receive({ v: 1, type: "frag_cancel", transfer: "one", reason: "cancelled" });
  assert.equal(q.a.closed, true); assert.equal(q.a.queuedBytes, 0);
  q.close(); assert.equal(q.b.reservedBytes, 0);
});

test("joined can fragment before capability selection and exposes one complete snapshot", async () => {
  let server, client, resolved = false;
  const limits = fragmentationLimits();
  const channel = { bufferedAmount: 0, send(bytes) {
    queueMicrotask(() => server.receive(decode(bytes, codec)));
  } };
  client = new KokosyncClient({ reliable: channel, realtime: channel, name: "test", close() {} },
    { timeout: 3000, fragmentation: limits });
  server = new Fragmentation({ limits, buffered: () => 0,
    write: bytes => queueMicrotask(() => { if (!client.closed) channel.onmessage({ data: bytes }); }),
    fail: error => { throw error; },
    deliver: frame => {
      if (frame.type === "join") send(server, {
        type: "joined", id: frame.id, seq: 9007199254740993n, revision: 1n,
        capabilities: { fragmentation: { ...limits, chunkBytes: 4096 } },
        entries: [{ key: "large", value: new Uint8Array(200000), revision: 1n }],
      });
    },
  });
  try {
    const joined = client.request("join", { room: "test", fragmentation: limits }, "joined").then(frame => { resolved = true; return frame; });
    await pause(5);
    assert.equal(resolved, false); assert.equal(client.state.size, 0);
    const result = await joined;
    assert.equal(client.get("large").length, 200000);
    assert.equal(result.seq, 9007199254740993n);
    assert.equal(client.fragments.limits.chunkBytes, 4096);
    assert.throws(() => client.latest("too-big", new Uint8Array(1000), 1n), /too large/);
  } finally { client.close(); server.close(); }
});

test("blob frames stay inline; nested fragments and fragmented live are rejected", async () => {
  const p = pair();
  try {
    send(p.a, { type: "blob_chunk", value: new Uint8Array(12288) });
    await until(() => p.received.length === 1);
    assert.equal(p.wire[0].frame.type, "blob_chunk");
  } finally { p.close(); }
  for (const type of ["frag_open", "live", "latest", "peer_chunk"]) {
    const q = pair();
    try {
      const bytes = encode({ v: 1, type }, codec);
      q.b.receive({ v: 1, type: "frag_open", transfer: "nested", size: bytes.length, sha256: hex(bytes) });
      q.b.receive({ v: 1, type: "frag_chunk", transfer: "nested", offset: 0, value: bytes });
      q.b.receive({ v: 1, type: "frag_end", transfer: "nested" });
      assert.equal(q.b.closed, true); assert.equal(q.received.length, 0);
    } finally { q.close(); }
  }
});

test("request cancellation and timeout close the session and release both directions", async () => {
  function client(timeout = 1000) {
    const channel = { bufferedAmount: 0, send() {} };
    return new KokosyncClient({ reliable: channel, realtime: channel, name: "test", close() {} }, { timeout });
  }
  const a = client();
  try {
    const abort = new AbortController();
    const pending = a.message("large", new Uint8Array(100000), { signal: abort.signal });
    const rejected = assert.rejects(pending, /cancel/i);
    a.fragments.receive({ v: 1, type: "frag_open", transfer: "incoming", size: 100000, sha256: "0".repeat(64) });
    abort.abort(new Error("Request cancelled by test"));
    await rejected;
    assert.equal(a.closed, true);
    assert.equal(a.fragments.queuedBytes, 0);
    assert.equal(a.fragments.reservedBytes, 0);
  } finally { a.close(); }
  const b = client(15);
  try {
    const pending = b.snapshot();
    b.fragments.receive({ v: 1, type: "frag_open", transfer: "snapshot", size: 100000, sha256: "0".repeat(64) });
    await assert.rejects(pending, error => error.code === "Timeout");
    assert.equal(b.closed, true); assert.equal(b.fragments.reservedBytes, 0);
  } finally { b.close(); }
  const c = client();
  try {
    await assert.rejects(c.message("small", null, { signal: AbortSignal.abort() }));
    assert.equal(c.closed, false); assert.equal(c.fragments.queuedBytes, 0);
  } finally { c.close(); }
});

test("follow caps pending catchup by bytes and count, then closes for resynchronization", async () => {
  for (const scenario of [
    { limits: { queueBytes: 1000, reassemblyBytes: 2000 }, payload: new Uint8Array(600), count: 2 },
    { limits: { queueBytes: 2000, reassemblyBytes: 1000 }, payload: new Uint8Array(600), count: 2 },
    { limits: {}, payload: null, count: 257 },
  ]) {
    const channel = { bufferedAmount: 0, send() {} };
    const client = new KokosyncClient({ reliable: channel, realtime: channel, name: "test", close() {} },
      { timeout: 1000, fragmentation: scenario.limits });
    let applied = 0;
    try {
      const ready = client.follow({ restore() { applied++; }, apply() { applied++; }, capture() {} });
      const rejected = assert.rejects(ready, error => error.code === "ResyncRequired");
      for (let seq = 1; seq <= scenario.count; seq++)
        client.receive({ v: 1, type: "message", seq: BigInt(seq), value: scenario.payload });
      await rejected;
      assert.equal(applied, 0);
      assert.equal(client.closed, true);
      assert.equal(client.pending.size, 0);
      assert.equal(client.listeners.get("message").size, 0);
      assert.equal(client.fragments.queuedBytes, 0);
    } finally { client.close(); }
  }
});

test("latest and live arriving before joined readiness are dropped", () => {
  const channel = { bufferedAmount: 0, send() {} };
  const client = new KokosyncClient({ reliable: channel, realtime: channel, name: "test", close() {} }, { timeout: 1000 });
  const received = [];
  client.on("latest", frame => received.push(frame));
  client.on("live", frame => received.push(frame));
  const latest = { v: 1, type: "latest", peer: "other", key: "cursor", seq: 1n, value: null };
  const live = { v: 1, type: "live", peer: "other", stream: "audio", seq: 1n, value: new Uint8Array(2) };
  try {
    client.receive(latest); client.receive(live);
    assert.equal(received.length, 0);
    client.receive({ v: 1, type: "joined", seq: 0n, revision: 0n, capabilities: {} });
    client.receive(latest); client.receive(live);
    assert.equal(received.length, 2);
  } finally { client.close(); }
});
