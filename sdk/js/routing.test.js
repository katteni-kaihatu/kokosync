import assert from "node:assert/strict";
import test from "node:test";
import { encode, decode } from "@msgpack/msgpack";
import { KokosyncClient } from "./index.js";
import { routingOptions } from "./routing.js";
import { peerDeliveryDefaults, peerDeliveryTypes } from "./peer-delivery.js";

const codec = { useBigInt64: true };
const pause = ms => new Promise(resolve => setTimeout(resolve, ms));
async function until(check) {
  for (let i = 0; i < 400; i++) { if (check()) return; await pause(2); }
  throw new Error("Route test timed out");
}
class FakeChannel {
  constructor(label = "kokosync-direct", options = { ordered: false, maxRetransmits: 0 }) {
    this.label = label; this.ordered = options.ordered; this.maxRetransmits = options.maxRetransmits ?? null;
    this.readyState = "connecting"; this.bufferedAmount = 0; this.sent = [];
  }
  send(bytes) {
    if (this.readyState !== "open") throw new Error("Channel closed");
    this.sent.push(decode(bytes, codec));
    queueMicrotask(() => {
      if (this.other?.readyState === "open") this.other.onmessage?.({ data: bytes });
    });
  }
  close() { this.readyState = "closed"; }
}
class FakePC {
  constructor(network) {
    this.network = network; this.id = String(network.pcs.length); this.connectionState = "new";
    this.channels = [];
    network.pcs.push(this);
  }
  createDataChannel(label, options) {
    const channel = new FakeChannel(label, options); this.channels.push(channel);
    if (label === "kokosync-direct") this.channel = channel;
    return channel;
  }
  async createOffer() { return { type: "offer", sdp: this.id }; }
  async createAnswer() { return { type: "answer", sdp: this.id }; }
  async setLocalDescription(description) { this.localDescription = description; }
  async setRemoteDescription(description) {
    this.remoteDescription = description;
    this.other = this.network.pcs[Number(description.sdp)];
    if (description.type === "answer") {
      this.connectionState = this.other.connectionState = "connected";
      for (const local of this.channels) {
        const remote = new FakeChannel(local.label, { ordered: local.ordered, maxRetransmits: local.maxRetransmits });
        this.other.channels.push(remote);
        if (local.label === "kokosync-direct") this.other.channel = remote;
        this.other.ondatachannel?.({ channel: remote });
        local.other = remote; remote.other = local;
        local.readyState = remote.readyState = "open";
        queueMicrotask(() => { local.onopen?.(); remote.onopen?.(); });
      }
    }
  }
  async addIceCandidate() {}
  close() { this.connectionState = "closed"; for (const channel of this.channels) channel.close(); }
}
function room(names = ["alice", "bob"], policies = {}) {
  const network = { pcs: [], clients: new Map(), submissions: [] };
  for (const peer of names) {
    const channel = { bufferedAmount: 0, send(bytes) {
      const frame = decode(bytes, codec); network.submissions.push({ peer, frame });
      if (frame.type === "route_signal") {
        queueMicrotask(() => network.clients.get(frame.to)?.receive({
          ...frame, peer, publisherEpoch: `generation-${peer}`, epoch: "room",
        }));
      } else if (peerDeliveryTypes.has(frame.type)) {
        queueMicrotask(() => network.clients.get(frame.to)?.receive({
          ...frame, peer, publisherEpoch: `generation-${peer}`, epoch: "room",
        }));
      } else if (["latest", "live"].includes(frame.type)) {
        for (const [name, client] of network.clients) {
          if (frame.to && !frame.to.includes(name) || frame.exclude?.includes(name)) continue;
          const delivered = { ...frame, peer, publisherEpoch: `generation-${peer}` };
          delete delivered.to; delete delivered.exclude;
          queueMicrotask(() => client.receive(delivered));
        }
      }
    } };
    const client = new KokosyncClient({ reliable: channel, realtime: channel, name: "test", close() {} },
      { peer, timeout: 1000, routing: policies[peer] });
    client.routeManager.makePeerConnection = config => {
      const pc = new FakePC(network); pc.config = config; return pc;
    };
    network.clients.set(peer, client);
  }
  for (const [peer, client] of network.clients) client.receive({
    v: 1, type: "joined", seq: 0n, revision: 0n, epoch: "room", publisherEpoch: `generation-${peer}`,
    capabilities: { routeSignaling: true, peerDelivery: peerDeliveryDefaults },
    peers: names.filter(name => name !== peer).map(name => ({
      peer: name, publisherEpoch: `generation-${name}`, directProtocol: "webrtc", maxDirectPeers: 8, peerDelivery: peerDeliveryDefaults,
    })),
  });
  return { ...network, close() { for (const client of network.clients.values()) client.close(); } };
}

test("direct latest/live keeps application API, identity, uint64, relay exclusions and diagnostics", async () => {
  const r = room();
  const a = r.clients.get("alice"), b = r.clients.get("bob");
  try {
    await until(() => a.routes()[0].route === "direct" && b.routes()[0].route === "direct");
    assert.equal(r.pcs.length, 2); // One deterministic offerer, one answerer.
    const got = [];
    b.on("latest", frame => got.push(frame)); b.on("live", frame => got.push(frame));
    a.latest("cursor", { x: 1 }, 9007199254740993n);
    a.live("audio", new Uint8Array([1, 2]), 9007199254740993n);
    await until(() => got.length === 2);
    assert.equal(got[0].seq, 9007199254740993n);
    assert.equal(got[0].peer, "alice");
    assert.equal(a.routes()[0].sentDirect, 2);
    assert.equal(a.routes()[0].sentRelay, 0);
    assert.equal(b.routes()[0].receivedDirect, 2);
    assert.deepEqual(r.submissions.find(item => item.frame.type === "latest").frame.exclude, ["bob"]);
    await until(() => a.routes()[0].rttMs !== null);
  } finally { r.close(); }
  assert.ok(r.pcs.every(pc => pc.connectionState === "closed"));
});

test("congestion and policy changes fall back to relay and can reconnect", async () => {
  const r = room();
  const a = r.clients.get("alice"), b = r.clients.get("bob");
  try {
    await until(() => a.routes()[0].route === "direct");
    const got = []; b.on("latest", frame => got.push(frame));
    a.routeManager.members.get("bob").connection.channel.bufferedAmount = 65537;
    a.latest("cursor", 1, 1n);
    await until(() => got.length === 1);
    assert.equal(a.routes()[0].reason, "congested");
    assert.equal(a.routes()[0].sentRelay, 1);
    a.setRouting({ enabled: false });
    assert.equal(a.routes()[0].reason, "disabled");
    a.latest("cursor", 2, 2n);
    await until(() => got.length === 2);
    await pause(10); // Let the peer consume the close notification before reenabling.
    b.setRouting({ enabled: true }); a.setRouting({ enabled: true });
    await until(() => a.routes()[0].route === "direct");
    await until(() => a.routes()[0].rttMs !== null);
    a.setRouting({ peers: [] });
    assert.equal(a.routes()[0].reason, "not-selected");
    assert.equal(a.routes()[0].rttMs, null);
    assert.equal(a.routeManager.members.get("bob").connection, null);
  } finally { r.close(); }
});

test("latest/live diagnostics count encoded physical bytes on direct and relay routes", async () => {
  const r = room();
  const a = r.clients.get("alice"), b = r.clients.get("bob");
  const got = []; b.on("latest", frame => got.push(frame)); b.on("live", frame => got.push(frame));
  try {
    await until(() => a.routes()[0].route === "direct" && b.routes()[0].route === "direct");
    const channel = a.routeManager.members.get("bob").connection.channel;
    a.latest("cursor", { x: 1 }, 1n); a.live("audio", new Uint8Array([1, 2]), 1n);
    await until(() => got.length === 2);
    for (const kind of ["latest", "live"]) {
      const length = encode(channel.sent.find(frame => frame.type === kind), codec).length;
      assert.equal(a.deliveryStats().find(row => row.kind === kind && row.route === "direct").sentBytes, length);
      assert.equal(b.deliveryStats().find(row => row.kind === kind && row.route === "direct").receivedBytes, length);
    }
    a.setRouting({ enabled: false });
    a.latest("cursor", { x: 2 }, 2n); a.live("audio", new Uint8Array([3, 4]), 2n);
    await until(() => got.length === 4);
    for (const kind of ["latest", "live"]) {
      const sent = r.submissions.findLast(item => item.peer === "alice" && item.frame.type === kind).frame;
      const received = got.findLast(frame => frame.type === kind);
      assert.equal(a.deliveryStats().find(row => row.kind === kind && row.route === "relay").sentBytes, encode(sent, codec).length);
      assert.equal(b.deliveryStats().find(row => row.kind === kind && row.route === "relay").receivedBytes, encode(received, codec).length);
    }
    // A uint8 wire encoding of v=1 is one byte longer than the canonical positive fixint.
    const frame = { v: 1, type: "latest", peer: "alice", publisherEpoch: "generation-alice", key: "cursor", value: 3, seq: 3n };
    const canonical = encode(frame, codec), physical = new Uint8Array(canonical.length + 1);
    assert.deepEqual([...canonical.slice(1, 4)], [161, 118, 1]);
    physical.set(canonical.subarray(0, 3)); physical.set([204, 1], 3); physical.set(canonical.subarray(4), 5);
    const before = b.deliveryStats().find(row => row.kind === "latest" && row.route === "relay").receivedBytes;
    b.wire.realtime.onmessage({ data: physical });
    assert.equal(b.deliveryStats().find(row => row.kind === "latest" && row.route === "relay").receivedBytes, before + physical.length);
  } finally { r.close(); }
});

test("reliable direct transfer waits through local buffer pressure without retrying via relay", async () => {
  const r = room();
  const a = r.clients.get("alice"), b = r.clients.get("bob");
  try {
    await until(() => a.routes()[0].route === "direct" && b.routes()[0].route === "direct");
    const channel = a.routeManager.members.get("bob").connection.deliveryChannel;
    const send = channel.send.bind(channel);
    let filled = false;
    channel.send = bytes => {
      send(bytes);
      if (!filled && decode(bytes, codec).type === "peer_chunk") {
        filled = true; channel.bufferedAmount = 65537;
      }
    };
    const value = new Uint8Array(1024 * 1024).fill(71), received = [];
    b.on("peer_message", frame => received.push(frame));
    const pending = a.sendTo("bob", "buffer-pressure", value);
    await until(() => [...a.peerDeliveries.tx.values()][0]?.queue[0]?.blocked);
    const item = [...a.peerDeliveries.tx.values()][0].queue[0];
    item.nextRetry = 0;
    a.peerDeliveries.tick();
    assert.equal(item.retries, 0);
    assert.equal(r.submissions.some(item => item.frame.type === "peer_open"), false);
    channel.bufferedAmount = 0;
    a.peerDeliveries.tick();
    const receipt = await pending;
    assert.equal(receipt.route, "direct"); assert.equal(item.retries, 0);
    assert.deepEqual(received[0].value, value);
    assert.equal(r.submissions.some(item => item.frame.type === "peer_open"), false);
  } finally { r.close(); }
});

test("native relay peers with 65 to 128 UTF-8 byte names remain addressable", async () => {
  const names = ["n".repeat(65), "猫".repeat(42) + "ab"];
  assert.equal(new TextEncoder().encode(names[1]).length, 128);
  assert.deepEqual(routingOptions({ peers: names }).peers, names);
  assert.throws(() => routingOptions({ peers: [names[1] + "x"] }), TypeError);
  const r = room(["alice", ...names], Object.fromEntries(["alice", ...names].map(peer => [peer, { enabled: false }])));
  const a = r.clients.get("alice");
  try {
    for (const peer of names) {
      a.routeManager.members.get(peer).directProtocol = "none";
      a.routeManager.members.get(peer).maxDirectPeers = 0;
      const got = []; r.clients.get(peer).on("peer_message", frame => got.push(frame));
      const receipt = await a.sendTo(peer, "native", "hello");
      assert.equal(receipt.route, "relay"); assert.equal(receipt.peer, peer);
      assert.equal(got.length, 1); assert.equal(got[0].value, "hello");
    }
  } finally { r.close(); }
});

test("disabling a measured direct route clears its RTT", async () => {
  const r = room();
  const a = r.clients.get("alice");
  try {
    await until(() => a.routes()[0].route === "direct" && a.routes()[0].rttMs !== null);
    a.setRouting({ enabled: false });
    assert.equal(a.routes()[0].route, "relay");
    assert.equal(a.routes()[0].reason, "disabled");
    assert.equal(a.routes()[0].rttMs, null);
  } finally { r.close(); }
});

test("selection is bounded and native peers explain adapter fallback", async () => {
  assert.throws(() => routingOptions({ maxPeers: 9 }), /maxPeers/);
  assert.throws(() => routingOptions({ peers: ["a", "a"] }), /unique/);
  const r = room(["alice", "bob", "carol"], {
    alice: { maxPeers: 1, peers: ["bob"] }, bob: { maxPeers: 1, peers: ["alice"] }, carol: { enabled: false },
  });
  const a = r.clients.get("alice");
  try {
    await until(() => a.routes().find(route => route.peer === "bob").route === "direct");
    assert.equal(a.routes().find(route => route.peer === "carol").reason, "not-selected");
    a.routeManager.memberJoined({ epoch: "room", peer: "native", publisherEpoch: "native-generation", directProtocol: "none", maxDirectPeers: 0 });
    assert.equal(a.routes().find(route => route.peer === "native").reason, "peer-adapter-unavailable");
    assert.equal([...a.routeManager.members.values()].filter(record => record.connection).length, 1);
    assert.deepEqual(a.routeManager.advertisement(), { directProtocol: "webrtc", maxDirectPeers: 8 });
  } finally { r.close(); }
});

test("invalid direct operations cannot inject reliable data; obsolete callbacks and epochs are ignored", async () => {
  const r = room();
  const a = r.clients.get("alice"), b = r.clients.get("bob");
  try {
    await until(() => b.routes()[0].route === "direct");
    let injected = 0; b.on("message", () => injected++);
    const old = b.routeManager.members.get("alice").connection;
    old.channel.onmessage({ data: encode({ v: 1, type: "message", value: "attack" }, codec) });
    assert.equal(injected, 0); assert.equal(b.routes()[0].route, "relay");
    const record = a.routeManager.members.get("bob");
    const staleCallback = record.connection?.channel.onmessage;
    a.routeManager.memberLeft({ epoch: "room", peer: "bob", publisherEpoch: "generation-bob" });
    a.routeManager.memberJoined({ epoch: "room", peer: "bob", publisherEpoch: "new-bob", directProtocol: "none", maxDirectPeers: 0 });
    staleCallback?.({ data: encode({ v: 1, type: "latest", peer: "bob", publisherEpoch: "generation-bob", epoch: "room", key: "x", seq: 1n, value: 1 }, codec) });
    a.routeManager.memberLeft({ epoch: "room", peer: "bob", publisherEpoch: "generation-bob" });
    a.routeManager.memberJoined({ epoch: "room", peer: "bob", publisherEpoch: "generation-bob", directProtocol: "webrtc", maxDirectPeers: 8 });
    assert.equal(a.routes()[0].publisherEpoch, "new-bob");
    assert.equal(a.routes()[0].receivedDirect, 0);
  } finally { r.close(); }
});

test("heartbeat expiry and setup deadline release connections with a bounded retry cooldown", async () => {
  const r = room();
  const a = r.clients.get("alice");
  try {
    await until(() => a.routes()[0].route === "direct");
    const manager = a.routeManager, connection = manager.members.get("bob").connection;
    let now = connection.lastPong;
    manager.now = () => now;
    now += 6001; manager.tick();
    assert.equal(a.routes()[0].reason, "heartbeat-timeout");
    const count = r.pcs.length;
    now += 1000; manager.tick();
    assert.equal(r.pcs.length, count);
    now += 15000; manager.tick();
    assert.ok(r.pcs.length > count);
    manager.signalToCore = () => {};
    now += 10001; manager.tick();
    assert.equal(a.routes()[0].reason, "connection-failed");
  } finally { r.close(); }
});

test("relay broadcast fallback deduplicates direct copies when exclusions do not fit", async () => {
  const other = "b".repeat(64);
  const r = room(["a", other]);
  const a = r.clients.get("a"), b = r.clients.get(other);
  try {
    await until(() => a.routes()[0].route === "direct");
    const got = []; b.on("latest", frame => got.push(frame));
    const value = "v".repeat(790);
    a.latest("cursor", value, 1n);
    await until(() => b.routes()[0].receivedRelay === 1);
    assert.equal(got.length, 1);
    assert.equal(b.routes()[0].receivedDirect, 1);
    assert.equal(a.routes()[0].sentDirect, 1);
    assert.equal(a.routes()[0].sentRelay, 1);
    assert.equal(r.submissions.find(item => item.frame.type === "latest").frame.exclude, undefined);
  } finally { r.close(); }
});

test("healthy direct submission proceeds when relay buffering is full and reports relay failure", async () => {
  const r = room();
  const a = r.clients.get("alice"), b = r.clients.get("bob");
  try {
    await until(() => a.routes()[0].route === "direct");
    let received = 0; b.on("latest", () => received++);
    a.wire.realtime.bufferedAmount = 1048577;
    assert.throws(() => a.latest("cursor", 1, 1n), /Relay transport backpressure/);
    await until(() => received === 1);
    assert.equal(a.routes()[0].sentDirect, 1);
    assert.equal(a.routes()[0].sentRelay, 0);
    assert.equal(a.routes()[0].route, "direct");
  } finally { r.close(); }
});

test("non-offerer waiting for an unselected route times out without allocating a connection", () => {
  const r = room(["alice", "bob"], { alice: { enabled: false } });
  const b = r.clients.get("bob"), manager = b.routeManager;
  try {
    const started = manager.members.get("alice").waitingSince;
    manager.now = () => started + 10001;
    manager.tick();
    assert.equal(b.routes()[0].reason, "connection-failed");
    assert.equal(manager.members.get("alice").connection, undefined);
    assert.equal(r.pcs.length, 0);
    manager.now = () => started + 11000;
    manager.tick();
    assert.equal(b.routes()[0].reason, "connection-failed");
  } finally { r.close(); }
});

test("delayed signals from an earlier negotiation cannot close a replacement connection", async () => {
  const r = room();
  const a = r.clients.get("alice"), b = r.clients.get("bob");
  try {
    await until(() => a.routes()[0].route === "direct");
    const oldId = a.routeManager.members.get("bob").connection.id;
    a.setRouting({ enabled: false });
    await pause(10);
    b.setRouting({ enabled: true }); a.setRouting({ enabled: true });
    await until(() => a.routes()[0].route === "direct");
    const currentId = b.routeManager.members.get("alice").connection.id;
    assert.notEqual(currentId, oldId);
    for (const type of ["close", "answer", "candidate", "offer"]) b.routeManager.receiveSignal({
      peer: "alice", publisherEpoch: "generation-alice", epoch: "room", to: "bob", targetEpoch: "generation-bob",
      signal: { type, connection: oldId, sdp: "old", candidate: { candidate: "old" } },
    });
    assert.equal(b.routeManager.members.get("alice").connection.id, currentId);
    assert.equal(b.routes()[0].route, "direct");
  } finally { r.close(); }
});

test("both paths reject departed generations before dedup while accepting the current publisher", () => {
  const r = room(["alice", "bob"], { alice: { enabled: false }, bob: { enabled: false } });
  const a = r.clients.get("alice"), got = [];
  a.on("latest", frame => got.push(frame));
  try {
    a.receive({ type: "peer_left", epoch: "room", peer: "bob", publisherEpoch: "generation-bob" });
    a.receive({ type: "peer_joined", epoch: "room", peer: "bob", publisherEpoch: "bob-new",
      directProtocol: "none", maxDirectPeers: 0 });
    const base = { v: 1, type: "latest", peer: "bob", key: "cursor", value: 1 };
    a.receive({ ...base, publisherEpoch: "bob-new", seq: 1n });
    a.receive({ ...base, publisherEpoch: "generation-bob", seq: 99n });
    a.receive({ ...base, publisherEpoch: "generation-bob", seq: 100n }, "direct");
    assert.equal(got.length, 1);
    assert.equal(got[0].seq, 1n);
    assert.equal(a.routes()[0].receivedRelay, 1);
    a.receive({ ...base, peer: "alice", publisherEpoch: "generation-alice", seq: 1n });
    a.receive({ ...base, peer: "alice", publisherEpoch: "alice-old", seq: 99n });
    assert.equal(got.length, 2);
  } finally { r.close(); }
});

test("direct live frames normalize fields and deduplicate by stream regardless of extra key", async () => {
  const r = room();
  const b = r.clients.get("bob"), got = [];
  b.on("live", frame => got.push(frame));
  try {
    await until(() => b.routes()[0].route === "direct");
    const channel = b.routeManager.members.get("alice").connection.channel;
    const frame = { v: 1, type: "live", epoch: "room", peer: "alice", publisherEpoch: "generation-alice",
      stream: "audio", ttlMs: 250, seq: 1n, value: new Uint8Array([1]), id: "inject-request-id" };
    channel.onmessage({ data: encode({ ...frame, key: "one" }, codec) });
    channel.onmessage({ data: encode({ ...frame, key: "two" }, codec) });
    assert.equal(got.length, 1);
    assert.equal(got[0].key, undefined);
    assert.equal(got[0].id, undefined);
    assert.equal(b.routes()[0].receivedDirect, 2);
  } finally { r.close(); }
});

test("sendTo uses a separate reliable direct channel and falls back without changing latest", async () => {
  const r = room();
  const a = r.clients.get("alice"), b = r.clients.get("bob"), delivered = [], cursors = [];
  b.on("peer_message", frame => delivered.push(frame));
  b.on("latest", frame => cursors.push(frame));
  try {
    await until(() => a.routes()[0].route === "direct");
    const connection = a.routeManager.members.get("bob").connection;
    assert.equal(connection.deliveryChannel.label, "kokosync-peer-delivery");
    assert.equal(connection.deliveryChannel.ordered, true);
    assert.equal(connection.deliveryChannel.maxRetransmits, null);
    const pending = a.sendTo("bob", "document", new Uint8Array(50000));
    a.latest("cursor", { x: 1 }, 1n);
    const direct = await pending;
    assert.equal(direct.route, "direct"); assert.equal(delivered.length, 1); assert.equal(cursors.length, 1);
    a.setRouting({ enabled: false });
    const relay = await a.sendTo("bob", "document", "next");
    assert.equal(relay.route, "relay"); assert.equal(relay.seq, 2n); assert.equal(delivered.length, 2);
    assert.ok(a.deliveryStats().some(row => row.kind === "peer_bulk" && row.route === "direct" && row.sentMessages === 1));
    assert.ok(a.deliveryStats().some(row => row.kind === "latest" && row.route === "direct" && row.sentFrames === 1));
  } finally { r.close(); }
});
