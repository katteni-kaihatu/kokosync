import assert from "node:assert/strict";
import test from "node:test";
import { encode, decode } from "@msgpack/msgpack";
import { PeerDelivery, deliveryLimits } from "./peer-delivery.js";
import { validateValue } from "./index.js";

const codec = { useBigInt64: true };
const pause = ms => new Promise(resolve => setTimeout(resolve, ms));
async function until(check) {
  for (let i = 0; i < 1000; i++) { if (check()) return; await pause(2); }
  throw new Error("Peer delivery test timed out");
}
function pair(overrides = {}) {
  const limits = deliveryLimits(overrides), received = [], wire = [];
  const members = new Map(["a", "b"].map(peer => [peer, { peer, publisherEpoch: `epoch-${peer}`, peerDelivery: limits }]));
  const engines = new Map(), network = { hook: null, direct: true };
  for (const peer of ["a", "b"]) {
    const engine = new PeerDelivery({ peer, limits, member: name => members.get(name), validate: validateValue,
      deliver: frame => received.push({ at: peer, frame }),
      transmit(frame, bytes, preference) {
        if (preference === "direct" && !network.direct) return null;
        const route = preference === "relay" || !network.direct ? "relay" : "direct";
        const decoded = decode(bytes, codec); wire.push({ route, frame: decoded });
        const deliver = value => queueMicrotask(() => engines.get(frame.to).incoming(value ?? decoded, route, bytes.length));
        if (!network.hook?.(decoded, route, deliver)) deliver();
        return route;
      },
    });
    engine.joined({ epoch: "room", publisherEpoch: `epoch-${peer}`, capabilities: { peerDelivery: limits } });
    engines.set(peer, engine);
  }
  return { a: engines.get("a"), b: engines.get("b"), limits, members, network, received, wire,
    close() { for (const engine of engines.values()) engine.close(); } };
}

test("addressed large Value uses receipt, stable ID and ordered per-topic delivery", async () => {
  const p = pair();
  try {
    const bytes = Uint8Array.from({ length: 200000 }, (_, i) => i % 251);
    const [first, second] = await Promise.all([p.a.sendTo("b", "data", { bytes }), p.a.sendTo("b", "data", "next")]);
    assert.equal(first.seq, 1n); assert.equal(second.seq, 2n); assert.equal(first.route, "direct");
    assert.equal(typeof first.rttMs, "number");
    assert.deepEqual(p.received[0].frame.value.bytes, bytes);
    assert.equal(p.received[1].frame.value, "next");
    assert.equal(p.a.queueBytes, 0); assert.equal(p.b.receiveBytes, 0);
    assert.ok(p.wire.filter(x => x.frame.type === "peer_chunk").every(x => x.frame.value.length <= 12288));
    p.members.delete("a"); p.b.departed("a", "epoch-a");
    assert.equal(p.received[0].frame.publisherEpoch, "epoch-a");
    assert.deepEqual(p.received[0].frame.value.bytes, bytes);
  } finally { p.close(); }
});

test("lost final receipt retries same ID through relay without application duplication", async () => {
  const p = pair(); let dropped = false;
  p.network.hook = frame => {
    if (frame.type === "peer_done" && !dropped) { dropped = true; return true; }
  };
  try {
    const pending = p.a.sendTo("b", "once", { important: true });
    await until(() => p.received.length === 1 && dropped);
    const active = [...p.a.tx.values()][0].queue[0];
    active.nextRetry = 0; p.a.tick();
    const receipt = await pending;
    assert.equal(receipt.route, "relay"); assert.equal(p.received.length, 1);
    assert.equal(receipt.rttMs, null);
    assert.equal(p.a.stats().find(row => row.route === "relay").rttMs, null);
    const opens = p.wire.filter(x => x.frame.type === "peer_open");
    assert.equal(opens.length, 2); assert.equal(opens[0].frame.messageId, opens[1].frame.messageId);
    assert.equal(opens[1].route, "relay");
    assert.equal(p.a.stats().reduce((sum, row) => sum + row.retries, 0), 1);
  } finally { p.close(); }
});

test("receipt returning on a different route has unknown RTT without a retry", async () => {
  const p = pair();
  p.network.hook = frame => {
    if (frame.type === "peer_done") {
      queueMicrotask(() => p.a.incoming(frame, "relay", encode(frame, codec).length));
      return true;
    }
  };
  try {
    const receipt = await p.a.sendTo("b", "different-route", "complete");
    assert.equal(receipt.route, "relay"); assert.equal(receipt.rttMs, null);
    assert.equal(p.a.stats().reduce((sum, row) => sum + row.retries, 0), 0);
    assert.equal(p.a.stats().find(row => row.route === "relay").rttMs, null);
  } finally { p.close(); }
});

test("duplicate and reordered chunks recover from contiguous offset and validate exact bytes", async () => {
  const p = pair(); let held, reordered = false;
  p.network.hook = (frame, route, deliver) => {
    if (frame.type !== "peer_chunk" || frame.peer !== "a") return;
    if (!held && !reordered) { held = frame; return true; }
    if (!reordered) { reordered = true; deliver(); deliver(held); deliver(held); return true; }
  };
  try {
    const value = Uint8Array.from({ length: 70000 }, (_, i) => i % 251);
    await p.a.sendTo("b", "reorder", value);
    assert.equal(p.received.length, 1); assert.deepEqual(p.received[0].frame.value, value);
    assert.ok(p.wire.some(x => x.frame.type === "peer_credit" && x.frame.retry));
  } finally { p.close(); }
});

test("queued cancellation is not-sent; active timeout poisons only its lane", async () => {
  const p = pair();
  p.network.hook = frame => frame.type === "peer_chunk" && frame.topic === "blocked";
  try {
    const first = p.a.sendTo("b", "blocked", "first");
    const firstRejected = assert.rejects(first, error => error.outcome === "unknown" && error.code === "Timeout");
    const abort = new AbortController();
    const second = p.a.sendTo("b", "blocked", "second", { signal: abort.signal });
    const secondRejected = assert.rejects(second, error => error.outcome === "not-sent" && error.code === "Cancelled");
    await until(() => [...p.a.tx.values()][0].queue[0].sent);
    const totals = p.a.stats().reduce((sum, row) => ({
      queued: sum.queued + row.queuedMessages, bytes: sum.bytes + row.queuedBytes,
      active: sum.active + row.unackedMessages,
    }), { queued: 0, bytes: 0, active: 0 });
    assert.deepEqual(totals, { queued: 2, bytes: p.a.queueBytes, active: 1 });
    abort.abort(); await secondRejected;
    await until(() => [...p.a.tx.values()][0].queue[0].sent);
    [...p.a.tx.values()][0].queue[0].deadline = 0; p.a.tick();
    await firstRejected;
    await assert.rejects(p.a.sendTo("b", "blocked", "third"), error => error.code === "LaneFailed");
    const other = await p.a.sendTo("b", "other", "works");
    assert.equal(other.seq, 1n); assert.equal(p.a.queueBytes, 0);
  } finally { p.close(); }
});

test("bulk admission reserves a slot and bytes for small messages on another topic", async () => {
  const p = pair(); const pending = [];
  p.network.hook = frame => frame.type === "peer_chunk" && frame.bulk;
  try {
    for (let i = 0; i < 4; i++) {
      const promise = p.a.sendTo("b", `bulk-${i}`, new Uint8Array(100000)); promise.catch(() => {}); pending.push(promise);
    }
    await until(() => [...p.b.rx.values()].filter(lane => lane.active).length === 3);
    const receipt = await p.a.sendTo("b", "small", "responsive");
    assert.equal(receipt.seq, 1n);
    assert.equal(p.received[0].frame.topic, "small");
    assert.equal(p.a.activeCounts().bulk, 3);
  } finally { p.close(); await Promise.allSettled(pending); }
});

test("retained lane capacity never evicts accepted high-water and departure rejects pinned sends", async () => {
  const p = pair({ lanes: 2 });
  try {
    await p.a.sendTo("b", "one", 1); await p.a.sendTo("b", "two", 2);
    await assert.rejects(p.a.sendTo("b", "three", 3), error => error.code === "Backpressure" && error.outcome === "not-sent");
    const oldOpen = p.wire.find(x => x.frame.type === "peer_open").frame;
    p.b.incoming(oldOpen, "relay", encode(oldOpen, codec).length);
    await pause(5); assert.equal(p.received.length, 2);
    p.network.hook = frame => frame.type === "peer_chunk";
    const pending = p.a.sendTo("b", "one", "pending");
    const rejected = assert.rejects(pending, error => error.code === "PeerUnavailable" && error.outcome === "unknown");
    await until(() => [...p.a.tx.values()][0].queue[0]?.sent);
    p.members.set("b", { peer: "b", publisherEpoch: "replacement", peerDelivery: p.limits });
    p.a.departed("b", "epoch-b"); await rejected;
    assert.equal(p.a.tx.size, 0); assert.equal(p.a.queueBytes, 0);
  } finally { p.close(); }
});

test("direct route failure retries open via relay; stale membership cannot inject", async () => {
  const p = pair(); let lost = false;
  p.network.hook = frame => {
    if (frame.type === "peer_credit" && !lost) { lost = true; p.network.direct = false; }
  };
  try {
    const receipt = await p.a.sendTo("b", "fallback", new Uint8Array(20000));
    assert.equal(receipt.route, "relay");
    assert.equal(p.received.length, 1);
    const open = p.wire.find(x => x.frame.type === "peer_open").frame;
    p.b.incoming({ ...open, publisherEpoch: "old-generation", messageId: "stale", seq: 2n }, "direct", 200);
    assert.equal(p.b.rx.size, 1); assert.equal(p.received.length, 1);
  } finally { p.close(); }
});

test("hash failure and assembly expiry release bytes and poison only affected lanes", async () => {
  const p = pair(); let corrupted = false;
  p.network.hook = (frame, route, deliver) => {
    if (frame.type === "peer_chunk" && frame.topic === "corrupt" && !corrupted) {
      corrupted = true; const value = frame.value.slice(); value[0] ^= 1;
      deliver({ ...frame, value }); return true;
    }
    if (frame.type === "peer_chunk" && frame.topic === "expire") return true;
  };
  try {
    await assert.rejects(p.a.sendTo("b", "corrupt", new Uint8Array(1000)),
      error => error.code === "ProtocolError" && error.outcome === "unknown");
    assert.equal(p.received.length, 0); assert.equal(p.b.receiveBytes, 0);
    const pending = p.a.sendTo("b", "expire", new Uint8Array(20000));
    const rejected = assert.rejects(pending, error => error.code === "Timeout" && error.outcome === "unknown");
    await until(() => p.b.receiveBytes > 0);
    const lane = [...p.b.rx.values()].find(lane => lane.topic === "expire");
    lane.active.deadline = 0; p.b.tick(); await rejected;
    assert.equal(p.b.receiveBytes, 0); assert.equal(p.a.queueBytes, 0);
    await p.a.sendTo("b", "healthy", "still usable");
    assert.equal(p.received[0].frame.topic, "healthy");
  } finally { p.close(); }
});

test("unknown cancellation blocks a delayed open and stale retry credit never rewinds acknowledgements", async () => {
  const p = pair();
  try {
    const cancelled = { v: 1, type: "peer_cancel", epoch: "room", peer: "a", publisherEpoch: "epoch-a",
      to: "b", targetEpoch: "epoch-b", topic: "cancel-before-open", messageId: "cancelled", seq: 1n, code: "Cancelled" };
    p.b.incoming(cancelled, "relay", encode(cancelled, codec).length);
    await assert.rejects(p.a.sendTo("b", "cancel-before-open", "late"), error => error.code === "LaneFailed");
    p.network.hook = frame => frame.type === "peer_chunk" || frame.type === "peer_credit";
    const pending = p.a.sendTo("b", "credit", new Uint8Array(100000)); pending.catch(() => {});
    await until(() => [...p.a.tx.values()].find(lane => lane.topic === "credit")?.queue[0]?.sent);
    const item = [...p.a.tx.values()].find(lane => lane.topic === "credit").queue[0];
    item.maxSent = 30000; item.ackOffset = 20000; item.offset = 30000; item.awaitCredit = false;
    const credit = { v: 1, type: "peer_credit", epoch: "room", peer: "b", publisherEpoch: "epoch-b",
      to: "a", targetEpoch: "epoch-a", topic: item.topic, messageId: item.messageId, seq: item.seq,
      offset: 10000n, limit: 40000n, retry: true };
    p.a.incoming(credit, "relay", encode(credit, codec).length);
    assert.equal(item.ackOffset, 20000); assert.equal(item.offset, 30000); assert.equal(item.retries, 0);
    p.close(); await Promise.allSettled([pending]);
  } finally { p.close(); }
});

test("failed application-path admission never commits high-water or emits a receipt", async () => {
  const p = pair();
  p.b.deliver = () => { throw new Error("Application admission is full"); };
  try {
    await assert.rejects(p.a.sendTo("b", "admission", { value: 1 }), error => error.code === "ProtocolError");
    const lane = [...p.b.rx.values()][0];
    assert.equal(lane.high, 0n); assert.equal(lane.failed, true); assert.equal(p.b.receiveBytes, 0);
    const open = p.wire.find(item => item.frame.type === "peer_open").frame;
    p.b.incoming(open, "relay", encode(open, codec).length);
    await pause(5);
    assert.equal(p.wire.filter(item => item.frame.type === "peer_done").length, 0);
  } finally { p.close(); }
});

test("public delivery failures use shared categories and preserve not-sent outcomes", async () => {
  const p = pair({ messageBytes: 8 });
  const rejects = (promise, code) => assert.rejects(promise, error => error.name === "DeliveryError" &&
    error.code === code && error.outcome === "not-sent");
  try {
    await rejects(p.a.sendTo("", "topic", 1), "InvalidArgument");
    await rejects(p.a.sendTo("b", "", 1), "InvalidArgument");
    for (const options of [null, 7, [], { signal: {} }, { timeoutMs: 0 }])
      await rejects(p.a.sendTo("b", "topic", 1, options), "InvalidArgument");
    await rejects(p.a.sendTo("absent", "topic", 1), "PeerUnavailable");
    await rejects(p.a.sendTo("b", "topic", () => {}), "InvalidValue");
    await rejects(p.a.sendTo("b", "topic", "longer than eight bytes"), "PayloadTooLarge");
    p.members.get("b").peerDelivery = undefined;
    await rejects(p.a.sendTo("b", "topic", 1), "UnsupportedCapability");
    p.a.close();
    await rejects(p.a.sendTo("b", "topic", 1), "Closed");
    assert.throws(() => deliveryLimits({ concurrent: 0 }), error => error.code === "InvalidArgument");
  } finally { p.close(); }
});

test("wire rejection aliases normalize to shared public codes without losing unknown outcomes", async () => {
  const aliases = {
    Unsupported: "UnsupportedCapability", PeerMissing: "PeerUnavailable", PeerLeft: "PeerUnavailable",
    PeerNotFound: "PeerUnavailable", TargetChanged: "PeerUnavailable", GenerationChanged: "PeerUnavailable",
    Capacity: "Backpressure", TransportError: "TransportFailed",
    StaleGeneration: "PeerUnavailable", DeadlineExceeded: "Timeout", TransportClosed: "Closed",
    InvalidMessage: "ProtocolError", InvalidPayload: "ProtocolError", HashMismatch: "ProtocolError",
    DuplicateMismatch: "ProtocolError", UnknownTransfer: "ProtocolError", InvalidCredit: "ProtocolError",
    InvalidReceipt: "ProtocolError", UnknownRemoteCode: "ProtocolError", toString: "ProtocolError",
    Backpressure: "Backpressure", Cancelled: "Cancelled", TransportFailed: "TransportFailed", LaneFailed: "LaneFailed",
  };
  for (const [code, expected] of Object.entries(aliases)) {
    const p = pair();
    p.network.hook = (frame, route, deliver) => {
      if (frame.type === "peer_credit") { deliver({ ...frame, type: "peer_cancel", code }); return true; }
    };
    try {
      await assert.rejects(p.a.sendTo("b", "rejected", 1), error => error.code === expected && error.outcome === "unknown");
      assert.equal(p.a.stats().find(row => row.sentFrames > 0).reason, expected);
    } finally { p.close(); }
  }
});
