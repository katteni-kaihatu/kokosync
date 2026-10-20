import { encode, decode } from "@msgpack/msgpack";
import { peerDeliveryTypes } from "./peer-delivery.js";

const codec = { useBigInt64: true, maxDepth: 40 };
const U64_MAX = (1n << 64n) - 1n;
const textSize = value => new TextEncoder().encode(value).length;
const realtime = type => type === "latest" || type === "live";
const positive = value => (typeof value === "bigint" && value > 0n && value <= U64_MAX) ||
  (Number.isSafeInteger(value) && value > 0);

export function routingOptions(options = {}, previous = {}) {
  const result = { enabled: true, maxPeers: 2, iceServers: [], ...previous, ...options };
  if (typeof result.enabled !== "boolean" || !Number.isInteger(result.maxPeers) || result.maxPeers < 0 || result.maxPeers > 8)
    throw new TypeError("routing requires enabled:boolean and maxPeers between 0 and 8");
  if (result.peers !== undefined && (!Array.isArray(result.peers) || result.peers.length > 32 ||
      result.peers.some(peer => typeof peer !== "string" || !peer.length || textSize(peer) > 128) ||
      new Set(result.peers).size !== result.peers.length)) throw new TypeError("routing.peers must contain up to 32 unique peer names");
  if (!Array.isArray(result.iceServers) || result.iceServers.length > 16) throw new TypeError("routing.iceServers must be an array of at most 16 servers");
  return { ...result, ...(result.peers ? { peers: [...result.peers] } : {}), iceServers: [...result.iceServers] };
}

/** Optional lossy routes. Reliable traffic and authoritative membership stay on the core. */
export class DirectRoutes {
  constructor({ peer, options, signal, deliver, changed, validateValue, deliveryReceive, makePeerConnection,
    now = () => Date.now(), timing = {} }) {
    this.peer = peer;
    this.options = routingOptions(options);
    this.signalToCore = signal;
    this.deliver = deliver;
    this.changed = changed;
    this.validateValue = validateValue;
    this.deliveryReceive = deliveryReceive;
    this.makePeerConnection = makePeerConnection ?? (globalThis.RTCPeerConnection
      ? config => new RTCPeerConnection(config) : null);
    this.now = now;
    this.timing = { setup: 10000, heartbeat: 2000, stale: 6000, retry: 15000, ...timing };
    this.members = new Map();
    this.retired = new Set();
    this.totals = { sentDirect: 0, receivedDirect: 0, sentRelay: 0, receivedRelay: 0 };
    this.closed = false;
    this.signalWindow = now();
    this.signalCount = 0;
  }
  advertisement() {
    // Capability ceiling stays stable so runtime policy can be enabled without rejoining.
    return { directProtocol: this.makePeerConnection ? "webrtc" : "none", maxDirectPeers: this.makePeerConnection ? 8 : 0 };
  }
  snapshot(record) {
    const { peer, publisherEpoch, route, reason, rttMs, sentDirect, receivedDirect, sentRelay, receivedRelay } = record;
    return { peer, publisherEpoch, route, reason, rttMs, sentDirect, receivedDirect, sentRelay, receivedRelay };
  }
  routes() { return [...this.members.values()].map(record => this.snapshot(record)); }
  notify(record) { this.changed(this.snapshot(record)); }
  status(record, route, reason) {
    if (record.route === route && record.reason === reason) return;
    record.route = route; record.reason = reason; this.notify(record);
  }
  joined(frame) {
    if (this.closed) return;
    this.epoch = frame.epoch;
    this.publisherEpoch = frame.publisherEpoch;
    this.signaling = frame.capabilities?.routeSignaling === true;
    for (const record of this.members.values()) this.dispose(record);
    this.members.clear(); this.retired.clear();
    for (const member of frame.peers ?? []) this.memberJoined({ ...member, epoch: this.epoch }, false);
    if (!this.timer && this.signaling) {
      this.timer = setInterval(() => this.tick(), 1000);
      this.timer.unref?.();
    }
    this.reconcile();
  }
  memberJoined(frame, reconcile = true) {
    if (this.closed || frame.epoch !== this.epoch || frame.peer === this.peer ||
        typeof frame.peer !== "string" || !frame.peer.length || textSize(frame.peer) > 128 ||
        typeof frame.publisherEpoch !== "string" || !frame.publisherEpoch.length ||
        this.retired.has(JSON.stringify([frame.peer, frame.publisherEpoch]))) return;
    const previous = this.members.get(frame.peer);
    if (previous?.publisherEpoch === frame.publisherEpoch) return;
    // Authoritative membership orders departure before a replacement generation.
    if (previous || this.members.size >= 32) return;
    const record = { peer: frame.peer, publisherEpoch: frame.publisherEpoch,
      directProtocol: frame.directProtocol, maxDirectPeers: Number(frame.maxDirectPeers),
      peerDelivery: frame.peerDelivery, kinds: {},
      route: "relay", reason: "not-selected", rttMs: null,
      sentDirect: 0, receivedDirect: 0, sentRelay: 0, receivedRelay: 0, retryAt: 0, retiredConnections: new Set() };
    this.members.set(record.peer, record);
    this.notify(record);
    if (reconcile) this.reconcile();
  }
  memberLeft(frame) {
    const record = this.members.get(frame.peer);
    if (frame.epoch !== this.epoch || !record || frame.publisherEpoch !== record.publisherEpoch) return;
    this.retire(record); this.dispose(record); this.members.delete(record.peer);
    this.status(record, "relay", "disconnected"); this.reconcile();
  }
  retire(record) {
    this.retired.add(JSON.stringify([record.peer, record.publisherEpoch]));
    if (this.retired.size > 256) this.retired.delete(this.retired.values().next().value);
  }
  setOptions(options) {
    this.options = routingOptions(options, this.options);
    for (const record of this.members.values()) { record.retryAt = 0; record.waitingSince = undefined; }
    this.reconcile();
  }
  selected() {
    if (!this.options.enabled || !this.makePeerConnection || !this.signaling) return new Set();
    return new Set([...this.members.values()]
      .filter(record => record.directProtocol === "webrtc" && record.maxDirectPeers > 0 &&
        (this.options.peers === undefined || this.options.peers.includes(record.peer)))
      .sort((a, b) => a.peer < b.peer ? -1 : a.peer > b.peer ? 1 : 0)
      .slice(0, this.options.maxPeers).map(record => record.peer));
  }
  reconcile() {
    if (this.closed) return;
    const chosen = this.selected();
    for (const record of this.members.values()) {
      let reason;
      if (!this.makePeerConnection) reason = "local-adapter-unavailable";
      else if (!this.options.enabled) reason = "disabled";
      else if (!this.signaling || record.directProtocol !== "webrtc" || !record.maxDirectPeers) reason = "peer-adapter-unavailable";
      else if (!chosen.has(record.peer)) reason = "not-selected";
      if (reason) {
        if (record.connection) this.dispose(record, true);
        this.status(record, "relay", reason);
      } else if (!record.connection && this.now() >= record.retryAt) {
        this.status(record, "relay", "connecting");
        if (this.peer < record.peer) this.start(record);
        else {
          record.waitingSince ??= this.now();
          if (this.now() - record.waitingSince >= this.timing.setup) this.failed(record, "connection-failed");
        }
      }
    }
  }
  current(record, connection) {
    return !this.closed && this.members.get(record.peer) === record && record.connection === connection;
  }
  signal(record, signal) {
    if (this.now() - this.signalWindow >= 60000) { this.signalWindow = this.now(); this.signalCount = 0; }
    if (this.closed || this.signalCount >= 60) throw new Error("Direct signaling rate limit reached");
    signal = { ...signal, connection: signal.connection ?? record.connection?.id };
    const fields = { to: record.peer, targetEpoch: record.publisherEpoch, signal };
    if (encode({ v: 1, type: "route_signal", ...fields }, codec).length > 16384) throw new Error("Direct signal is too large");
    this.signalCount++;
    this.signalToCore(fields);
  }
  create(record, id = crypto.randomUUID()) {
    let pc;
    try { pc = this.makePeerConnection({ iceServers: this.options.iceServers }); }
    catch { this.failed(record, "connection-failed"); return null; }
    const connection = { id, pc, started: this.now(), lastPing: 0, lastPong: this.now(), pendingPing: null,
      chain: Promise.resolve(), queued: 0, localSignaled: false, localCandidates: [], remoteCandidates: [] };
    record.connection = connection;
    this.status(record, "relay", "connecting");
    pc.onicecandidate = event => {
      if (!this.current(record, connection) || !event.candidate) return;
      const candidate = event.candidate.toJSON?.() ?? event.candidate;
      try {
        if (connection.localSignaled) this.signal(record, { type: "candidate", candidate });
        else if (connection.localCandidates.length < 32) connection.localCandidates.push(candidate);
        else throw new Error("Too many ICE candidates");
      } catch { this.failed(record, "connection-failed", connection); }
    };
    pc.ondatachannel = event => {
      if (this.current(record, connection) && event.channel.label === "kokosync-peer-delivery" && !connection.deliveryChannel) {
        try { this.attachDeliveryChannel(record, connection, event.channel); } catch { event.channel.close(); }
        return;
      }
      if (!this.current(record, connection) || connection.channel || event.channel.label !== "kokosync-direct") {
        event.channel.close(); return;
      }
      try { this.attachChannel(record, connection, event.channel); }
      catch { event.channel.close(); this.failed(record, "connection-failed", connection); }
    };
    pc.onconnectionstatechange = () => {
      if (!this.current(record, connection)) return;
      if (["failed", "closed", "disconnected"].includes(pc.connectionState))
        this.failed(record, pc.connectionState === "failed" ? "connection-failed" : "disconnected", connection);
    };
    return connection;
  }
  start(record) {
    const connection = this.create(record);
    if (!connection) return;
    try {
      this.attachChannel(record, connection, connection.pc.createDataChannel("kokosync-direct", { ordered: false, maxRetransmits: 0 }));
      this.attachDeliveryChannel(record, connection, connection.pc.createDataChannel("kokosync-peer-delivery", { ordered: true }));
    }
    catch { this.failed(record, "connection-failed", connection); return; }
    void this.negotiate(record, connection, "offer").catch(() => this.failed(record, "connection-failed", connection));
  }
  async negotiate(record, connection, type) {
    const description = await (type === "offer" ? connection.pc.createOffer() : connection.pc.createAnswer());
    if (!this.current(record, connection)) return;
    await connection.pc.setLocalDescription(description);
    if (!this.current(record, connection)) return;
    this.signal(record, { type, sdp: connection.pc.localDescription.sdp });
    connection.localSignaled = true;
    for (const candidate of connection.localCandidates) this.signal(record, { type: "candidate", candidate });
    connection.localCandidates = [];
  }
  receiveSignal(frame) {
    const record = this.members.get(frame.peer);
    if (this.closed || !record || frame.epoch !== this.epoch || frame.publisherEpoch !== record.publisherEpoch ||
        frame.targetEpoch !== this.publisherEpoch || frame.to !== this.peer || !this.selected().has(record.peer)) return;
    const signal = frame.signal;
    if (!signal || !["offer", "answer", "candidate", "close"].includes(signal.type) ||
        typeof signal.connection !== "string" || !signal.connection.length || signal.connection.length > 64 ||
        record.retiredConnections.has(signal.connection)) return;
    if (signal.type === "close") {
      if (record.connection?.id === signal.connection) this.failed(record, "disconnected", record.connection, false);
      return;
    }
    if (signal.type === "offer" && !(record.peer < this.peer)) return; // Only the deterministic offerer may offer.
    let connection = record.connection;
    if (connection && connection.id !== signal.connection) return;
    if (!connection) {
      if (signal.type !== "offer" || this.now() < record.retryAt) return;
      connection = this.create(record, signal.connection);
    }
    if (!connection) return;
    if (++connection.queued > 32) { this.failed(record, "connection-failed", connection); return; }
    connection.chain = connection.chain.then(async () => {
      if (!this.current(record, connection)) return;
      if (signal.type === "offer" || signal.type === "answer") {
        if (typeof signal.sdp !== "string" || signal.sdp.length > 16384) throw new Error("Invalid SDP");
        if (signal.type === "offer" && connection.pc.remoteDescription) throw new Error("Unexpected offer");
        if (signal.type === "answer" && !(this.peer < record.peer)) return;
        await connection.pc.setRemoteDescription({ type: signal.type, sdp: signal.sdp });
        if (!this.current(record, connection)) return;
        for (const candidate of connection.remoteCandidates) {
          await connection.pc.addIceCandidate(candidate);
          if (!this.current(record, connection)) return;
        }
        connection.remoteCandidates = [];
        if (signal.type === "offer") await this.negotiate(record, connection, "answer");
      } else {
        if (!signal.candidate || typeof signal.candidate.candidate !== "string" || signal.candidate.candidate.length > 4096)
          throw new Error("Invalid ICE candidate");
        if (connection.pc.remoteDescription) await connection.pc.addIceCandidate(signal.candidate);
        else if (connection.remoteCandidates.length < 32) connection.remoteCandidates.push(signal.candidate);
        else throw new Error("Too many ICE candidates");
      }
    }).catch(() => this.failed(record, "connection-failed", connection))
      .finally(() => { connection.queued--; });
  }
  attachChannel(record, connection, channel) {
    if (channel.ordered || channel.maxRetransmits !== 0) throw new Error("Direct channel must be unordered and unreliable");
    connection.channel = channel; channel.binaryType = "arraybuffer";
    channel.onopen = () => {
      if (!this.current(record, connection)) return;
      connection.lastPong = this.now();
      this.status(record, "direct", "connected");
      this.heartbeat(record, connection);
    };
    channel.onclose = () => this.failed(record, "disconnected", connection);
    channel.onerror = () => this.failed(record, "connection-failed", connection);
    channel.onmessage = event => {
      if (!this.current(record, connection)) return;
      try {
        const bytes = new Uint8Array(event.data);
        if (!bytes.length || bytes.length > 1000) throw new Error("Invalid direct frame size");
        const frame = decode(bytes, { useBigInt64: true, maxStrLength: 1000, maxBinLength: 1000, maxArrayLength: 1000, maxMapLength: 1000 });
        if (frame.v !== 1) throw new Error("Invalid direct frame version");
        if (frame.type === "route_ping" || frame.type === "route_pong") {
          if (typeof frame.nonce !== "string" || !frame.nonce.length || frame.nonce.length > 64) throw new Error("Invalid heartbeat");
          if (frame.type === "route_ping") {
            if (!connection.lastReply || this.now() - connection.lastReply >= 250) {
              this.channelSend(connection, encode({ v: 1, type: "route_pong", nonce: frame.nonce }, codec));
              connection.lastReply = this.now();
            }
          } else if (frame.nonce === connection.pendingPing) {
            record.rttMs = Math.max(0, this.now() - connection.lastPing);
            connection.lastPong = this.now(); connection.pendingPing = null; this.notify(record);
          }
          return;
        }
        if (!realtime(frame.type) || frame.peer !== record.peer || frame.publisherEpoch !== record.publisherEpoch ||
            frame.epoch !== this.epoch || frame.to !== undefined || frame.exclude !== undefined ||
            typeof frame.seq !== "bigint" || frame.seq < 0n || frame.seq > U64_MAX)
          throw new Error("Invalid direct publisher or operation");
        const key = frame.type === "latest" ? frame.key : frame.stream;
        if (typeof key !== "string" || !key.length || textSize(key) > 128) throw new Error("Invalid direct key");
        if (frame.type === "live" && (!(frame.value instanceof Uint8Array) || !positive(frame.ttlMs) ||
            frame.codec != null && typeof frame.codec !== "string")) throw new Error("Invalid direct live value");
        this.validateValue(frame.value, 1000);
        record.receivedDirect++; this.totals.receivedDirect++; this.countKind(record, frame.type, "receivedDirect", bytes.length); this.notify(record);
        // Bind identity to this current signaled connection, then use the common deduplicator.
        this.deliver({ v: 1, type: frame.type, epoch: this.epoch, peer: record.peer,
          publisherEpoch: record.publisherEpoch, seq: frame.seq, value: frame.value,
          ...(frame.type === "latest" ? { key: frame.key } : {
            stream: frame.stream, ttlMs: frame.ttlMs, ...(frame.codec != null ? { codec: frame.codec } : {}),
          }),
        }, "direct");
      } catch { this.failed(record, "connection-failed", connection); }
    };
  }
  channelSend(connection, bytes) {
    if (connection.channel?.readyState !== "open" || connection.channel.bufferedAmount > 65536)
      throw new Error("Direct channel is unavailable or congested");
    connection.channel.send(bytes);
  }
  attachDeliveryChannel(record, connection, channel) {
    if (!channel.ordered || channel.maxRetransmits != null || channel.maxPacketLifeTime != null)
      throw new Error("Peer delivery channel must be ordered and reliable");
    connection.deliveryChannel = channel; channel.binaryType = "arraybuffer";
    const stop = () => {
      if (connection.deliveryChannel === channel) connection.deliveryChannel = null;
      channel.onmessage = channel.onclose = channel.onerror = null;
      try { channel.close(); } catch {}
    };
    channel.onclose = stop; channel.onerror = stop;
    channel.onmessage = event => {
      if (!this.current(record, connection) || connection.deliveryChannel !== channel) return;
      try {
        const bytes = new Uint8Array(event.data);
        if (!bytes.length || bytes.length > 65536) throw new Error("Invalid peer delivery frame size");
        const frame = decode(bytes, { useBigInt64: true, maxStrLength: 65536, maxBinLength: 65536,
          maxArrayLength: 65536, maxMapLength: 65536 });
        if (frame.v !== 1 || !peerDeliveryTypes.has(frame.type) || frame.type === "peer_relay_error" ||
            frame.peer !== record.peer || frame.publisherEpoch !== record.publisherEpoch || frame.epoch !== this.epoch ||
            frame.to !== this.peer || frame.targetEpoch !== this.publisherEpoch) throw new Error("Invalid peer delivery identity");
        this.deliveryReceive?.(frame, bytes.length);
      } catch { stop(); }
    };
  }
  sendDelivery(frame, bytes) {
    const record = this.members.get(frame.to), connection = record?.connection, channel = connection?.deliveryChannel;
    if (!record || record.publisherEpoch !== frame.targetEpoch || !connection || !this.current(record, connection) ||
        channel?.readyState !== "open") return false;
    // null means temporary backpressure; false means the direct route is unavailable.
    if (channel.bufferedAmount > 65536) return null;
    try { channel.send(bytes); return true; } catch { return false; }
  }
  countKind(record, kind, field, byteLength) {
    record.kinds[kind] ??= { sentDirect: 0, receivedDirect: 0, sentRelay: 0, receivedRelay: 0,
      sentDirectBytes: 0, receivedDirectBytes: 0, sentRelayBytes: 0, receivedRelayBytes: 0 };
    record.kinds[kind][field]++;
    record.kinds[kind][`${field}Bytes`] += byteLength;
  }
  deliveryStats() {
    const entries = [];
    for (const record of this.members.values()) for (const [kind, counts] of Object.entries(record.kinds))
      for (const route of ["direct", "relay"]) {
        const suffix = route === "direct" ? "Direct" : "Relay";
        entries.push({ peer: record.peer, publisherEpoch: record.publisherEpoch, route, kind,
          sentFrames: counts[`sent${suffix}`], receivedFrames: counts[`received${suffix}`],
          sentMessages: counts[`sent${suffix}`], receivedMessages: counts[`received${suffix}`],
          sentBytes: counts[`sent${suffix}Bytes`], receivedBytes: counts[`received${suffix}Bytes`],
          retries: 0, unackedMessages: 0, queuedMessages: 0, queuedBytes: 0,
          reason: record.reason, rttMs: route === "direct" ? record.rttMs : null });
      }
    return entries;
  }
  heartbeat(record, connection) {
    if (connection.pendingPing) return;
    try {
      connection.pendingPing = crypto.randomUUID(); connection.lastPing = this.now();
      this.channelSend(connection, encode({ v: 1, type: "route_ping", nonce: connection.pendingPing }, codec));
    } catch { this.failed(record, "congested", connection); }
  }
  tick() {
    if (this.closed) return;
    for (const record of this.members.values()) {
      const connection = record.connection;
      if (!connection) continue;
      if (record.route !== "direct") {
        if (this.now() - connection.started >= this.timing.setup) this.failed(record, "connection-failed", connection);
      } else if (this.now() - connection.lastPong >= this.timing.stale) this.failed(record, "heartbeat-timeout", connection);
      else if (this.now() - connection.lastPing >= this.timing.heartbeat) this.heartbeat(record, connection);
    }
    this.reconcile();
  }
  send(frame, relay) {
    const key = frame.type === "latest" ? frame.key : frame.stream;
    if (!realtime(frame.type) || typeof frame.seq !== "bigint" || frame.seq < 0n || frame.seq > U64_MAX ||
        typeof key !== "string" || !key.length || textSize(key) > 128 ||
        frame.type === "live" && (!(frame.value instanceof Uint8Array) || !positive(frame.ttlMs)))
      throw new TypeError("Invalid latest/live envelope");
    this.validateValue(frame.value, 1000);
    const successes = [];
    const targets = [...this.members.values()].filter(record =>
      (!frame.to || frame.to.includes(record.peer)) && (!frame.exclude || !frame.exclude.includes(record.peer)));
    const direct = { ...frame, epoch: this.epoch, peer: this.peer, publisherEpoch: this.publisherEpoch };
    delete direct.to; delete direct.exclude;
    const bytes = encode(direct, codec);
    if (bytes.length <= 1000) {
      for (const record of targets) {
        if (record.route !== "direct" || !record.connection) continue;
        try {
          this.channelSend(record.connection, bytes);
          record.sentDirect++; this.totals.sentDirect++; this.countKind(record, frame.type, "sentDirect", bytes.length); successes.push(record.peer); this.notify(record);
        } catch { this.failed(record, "congested", record.connection); }
      }
    }
    let relayed = frame;
    if (successes.length) {
      const candidate = frame.to
        ? { ...frame, to: frame.to.filter(peer => !successes.includes(peer)) }
        : { ...frame, exclude: [...new Set([...(frame.exclude ?? []), ...successes])] };
      if ((!candidate.exclude || candidate.exclude.length <= 32) && encode(candidate, codec).length <= 872) relayed = candidate;
    }
    const relayBytes = encode(relayed, codec);
    relay(relayBytes);
    for (const record of targets) {
      if ((!relayed.to || relayed.to.includes(record.peer)) && (!relayed.exclude || !relayed.exclude.includes(record.peer))) {
        record.sentRelay++; this.totals.sentRelay++; this.countKind(record, frame.type, "sentRelay", relayBytes.length); this.notify(record);
      }
    }
  }
  relayReceived(frame, byteLength = encode(frame, codec).length) {
    const record = this.members.get(frame.peer);
    if (!record || frame.publisherEpoch !== record.publisherEpoch) return;
    record.receivedRelay++; this.totals.receivedRelay++; this.countKind(record, frame.type, "receivedRelay", byteLength); this.notify(record);
  }
  acceptsPublisher(frame) {
    if (!this.signaling) return true; // Preserve compatibility with legacy relays.
    if (frame.epoch !== undefined && frame.epoch !== this.epoch) return false;
    if (frame.peer === this.peer) return frame.publisherEpoch === this.publisherEpoch;
    return this.members.get(frame.peer)?.publisherEpoch === frame.publisherEpoch &&
      this.members.has(frame.peer);
  }
  failed(record, reason, connection = record.connection, notifyPeer = true) {
    if (this.closed || this.members.get(record.peer) !== record || (connection && record.connection !== connection)) return;
    this.dispose(record, notifyPeer);
    record.retryAt = this.now() + this.timing.retry;
    record.waitingSince = undefined;
    record.rttMs = null;
    this.status(record, "relay", reason);
  }
  dispose(record, notifyPeer = false) {
    record.rttMs = null;
    const connection = record.connection;
    if (!connection) return;
    record.connection = null;
    record.retiredConnections.add(connection.id);
    if (record.retiredConnections.size > 8) record.retiredConnections.delete(record.retiredConnections.values().next().value);
    if (notifyPeer && !this.closed) { try { this.signal(record, { type: "close", connection: connection.id }); } catch {} }
    const { pc, channel } = connection;
    pc.onicecandidate = pc.ondatachannel = pc.onconnectionstatechange = null;
    if (channel) {
      channel.onopen = channel.onclose = channel.onerror = channel.onmessage = null;
      try { channel.close(); } catch {}
    }
    if (connection.deliveryChannel) {
      const delivery = connection.deliveryChannel;
      delivery.onmessage = delivery.onclose = delivery.onerror = null;
      try { delivery.close(); } catch {}
      connection.deliveryChannel = null;
    }
    try { pc.close(); } catch {}
    connection.localCandidates = []; connection.remoteCandidates = [];
  }
  close() {
    this.closed = true; clearInterval(this.timer); this.timer = null;
    for (const record of this.members.values()) this.dispose(record);
    this.members.clear(); this.retired.clear();
  }
}
