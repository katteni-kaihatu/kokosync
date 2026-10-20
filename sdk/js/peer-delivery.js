import { encode, decode } from "@msgpack/msgpack";
import { sha256 } from "@noble/hashes/sha2.js";

export const peerDeliveryTypes = new Set(["peer_open", "peer_credit", "peer_chunk", "peer_complete", "peer_done", "peer_cancel", "peer_relay_error"]);
export const peerDeliveryDefaults = Object.freeze({ version: 1, messageBytes: 4194304, chunkBytes: 12288,
  windowBytes: 65536, queueBytes: 8388608, receiveBytes: 8388608, concurrent: 4, queued: 128,
  lanes: 64, timeoutMs: 30000, assemblyMs: 30000, retries: 5 });
const ceilings = { messageBytes: 67108864, chunkBytes: 16384, windowBytes: 262144,
  queueBytes: 268435456, receiveBytes: 268435456, concurrent: 32, queued: 4096, lanes: 1024,
  timeoutMs: 300000, assemblyMs: 300000, retries: 20 };
const codec = { useBigInt64: true, maxDepth: 40 };
const bytesOf = value => new TextEncoder().encode(value).length;
const hash = bytes => Array.from(sha256(bytes), x => x.toString(16).padStart(2, "0")).join("");
const key = (peer, epoch, topic) => JSON.stringify([peer, epoch, topic]);
const bulk = size => size > 16384;
const publicCodes = new Set(["UnsupportedCapability", "PeerUnavailable", "InvalidArgument", "InvalidValue",
  "PayloadTooLarge", "Backpressure", "Timeout", "Cancelled", "Closed", "TransportFailed", "LaneFailed", "ProtocolError"]);
const codeAliases = { Unsupported: "UnsupportedCapability", PeerMissing: "PeerUnavailable", PeerLeft: "PeerUnavailable",
  PeerNotFound: "PeerUnavailable", TargetChanged: "PeerUnavailable", GenerationChanged: "PeerUnavailable",
  StaleGeneration: "PeerUnavailable", Capacity: "Backpressure", DeadlineExceeded: "Timeout",
  TransportClosed: "Closed", TransportError: "TransportFailed" };
const publicCode = code => publicCodes.has(code) ? code
  : Object.hasOwn(codeAliases, code) ? codeAliases[code] : "ProtocolError";
function integer(value) {
  if (typeof value === "bigint" && value >= 0n && value <= BigInt(Number.MAX_SAFE_INTEGER)) return Number(value);
  return Number.isSafeInteger(value) && value >= 0 ? value : -1;
}
export class DeliveryError extends Error {
  constructor(code, message, outcome = "not-sent") { super(message); this.name = "DeliveryError"; this.code = publicCode(code); this.outcome = outcome; }
}
export function deliveryLimits(options = {}) {
  if (options === false) return null;
  if (!options || typeof options !== "object" || Array.isArray(options))
    throw new DeliveryError("InvalidArgument", "peerDelivery options must be an object or false");
  const result = { ...peerDeliveryDefaults, ...options };
  if (result.version !== 1) throw new DeliveryError("InvalidArgument", "peerDelivery.version must be 1");
  for (const [field, maximum] of Object.entries(ceilings)) {
    const value = integer(result[field]);
    if (value < (field === "retries" ? 0 : 1) || value > maximum) throw new DeliveryError("InvalidArgument", `Invalid peerDelivery.${field}`);
    result[field] = value;
  }
  return result;
}

/** Addressed reliable values; completely independent of room message/state sequence. */
export class PeerDelivery {
  constructor({ peer, limits, member, transmit, deliver, validate, now = () => Date.now() }) {
    this.peer = peer; this.limits = deliveryLimits(limits); this.member = member; this.transmit = transmit;
    this.deliver = deliver; this.validate = validate; this.now = now;
    this.tx = new Map(); this.rx = new Map(); this.counters = new Map(); this.controls = new Map();
    this.queueBytes = 0; this.bulkQueueBytes = 0; this.queued = 0; this.bulkQueued = 0; this.receiveBytes = 0; this.bulkReceiveBytes = 0;
    this.closed = false;
    this.timer = setInterval(() => this.tick(), 50); this.timer.unref?.();
  }
  joined(frame) { this.epoch = frame.epoch; this.publisherEpoch = frame.publisherEpoch; this.core = frame.capabilities?.peerDelivery; }
  negotiated(member) {
    if (!this.limits || !this.core || !member?.peerDelivery || integer(member.peerDelivery.version) !== 1)
      throw new DeliveryError("UnsupportedCapability", "Peer delivery is not supported by this connection or recipient");
    const limits = { ...this.limits };
    for (const field of ["messageBytes", "chunkBytes", "windowBytes", "timeoutMs", "retries"]) {
      const remote = integer(member.peerDelivery[field]), core = integer(this.core[field]);
      if (remote < 0 || core < 0) throw new DeliveryError("UnsupportedCapability", "Invalid peer delivery capability");
      limits[field] = Math.min(limits[field], remote, core);
    }
    return limits;
  }
  frame(item, type, extra = {}) {
    return { v: 1, type, to: item.peer, targetEpoch: item.publisherEpoch, epoch: this.epoch,
      peer: this.peer, publisherEpoch: this.publisherEpoch, topic: item.topic,
      messageId: item.messageId, seq: item.seq, ...extra };
  }
  counter(item, route, kind = bulk(item.size ?? 0) ? "peer_bulk" : "peer_message") {
    const id = JSON.stringify([item.peer, item.publisherEpoch, route, kind]);
    if (!this.counters.has(id)) this.counters.set(id, { peer: item.peer, publisherEpoch: item.publisherEpoch,
      route, kind, sentFrames: 0, receivedFrames: 0, sentBytes: 0, receivedBytes: 0,
      sentMessages: 0, receivedMessages: 0, retries: 0, unackedMessages: 0, queuedMessages: 0,
      queuedBytes: 0, reason: "available", rttMs: null });
    return this.counters.get(id);
  }
  stats() {
    const result = new Map([...this.counters].map(([id, value]) => [id, { ...value, unackedMessages: 0, queuedMessages: 0, queuedBytes: 0 }]));
    for (const lane of this.tx.values()) for (const item of lane.queue) {
      const base = this.counter(item, item.route ?? "relay");
      const id = JSON.stringify([base.peer, base.publisherEpoch, base.route, base.kind]);
      if (!result.has(id)) result.set(id, { ...base });
      const entry = result.get(id);
      // Queued totals include all retained sends; unackedMessages is the active subset.
      entry.queuedMessages++; entry.queuedBytes += item.size;
      if (item.seq !== undefined) entry.unackedMessages++;
    }
    return [...result.values()];
  }
  sendTo(peer, topic, value, options = {}) {
    try {
      if (this.closed) throw new DeliveryError("Closed", "Client is closed");
      if (!options || typeof options !== "object" || Array.isArray(options))
        throw new DeliveryError("InvalidArgument", "Delivery options must be an object");
      const { signal, timeoutMs } = options;
      if (signal !== undefined && (!signal || typeof signal !== "object" || typeof signal.aborted !== "boolean" ||
          typeof signal.addEventListener !== "function" || typeof signal.removeEventListener !== "function"))
        throw new DeliveryError("InvalidArgument", "signal must be an AbortSignal");
      if (signal?.aborted) throw new DeliveryError("Cancelled", "Delivery cancelled before sending");
      if (typeof peer !== "string" || !peer.length || bytesOf(peer) > 128 || typeof topic !== "string" || !topic.length || bytesOf(topic) > 256)
        throw new DeliveryError("InvalidArgument", "A recipient and topic are required");
      const member = this.member(peer);
      if (!member) throw new DeliveryError("PeerUnavailable", "Recipient is not in the room");
      const limits = this.negotiated(member);
      if (timeoutMs !== undefined && (!Number.isInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > 300000))
        throw new DeliveryError("InvalidArgument", "timeoutMs must be between 1 and 300000");
      this.validate(value, Infinity);
      const payload = encode(value, codec);
      if (payload.length > limits.messageBytes) throw new DeliveryError("PayloadTooLarge", "Encoded Value exceeds recipient message limit");
      const isBulk = bulk(payload.length);
      if (this.queued >= this.limits.queued || this.queueBytes + payload.length > this.limits.queueBytes ||
          isBulk && (this.bulkQueued >= Math.max(1, this.limits.queued - 1) || this.bulkQueueBytes + payload.length > this.limits.queueBytes - 16384))
        throw new DeliveryError("Backpressure", "Peer delivery send queue is full");
      const laneKey = key(peer, member.publisherEpoch, topic);
      let lane = this.tx.get(laneKey);
      if (!lane) {
        if (this.tx.size + this.rx.size >= this.limits.lanes) throw new DeliveryError("Backpressure", "Peer delivery lane limit reached");
        lane = { peer, publisherEpoch: member.publisherEpoch, topic, sequence: 0n, queue: [], failed: false };
        this.tx.set(laneKey, lane);
      }
      if (lane.failed) throw new DeliveryError("LaneFailed", "This delivery lane requires rejoin and application resynchronization");
      return new Promise((resolve, reject) => {
        const item = { peer, publisherEpoch: member.publisherEpoch, topic, payload, size: payload.length,
          sha256: hash(payload), messageId: crypto.randomUUID(), limits, lane, resolve, reject,
          deadline: this.now() + Math.min(timeoutMs ?? limits.timeoutMs, limits.timeoutMs), retries: 0,
          offset: 0, ackOffset: 0, limit: 0, sent: false, signal };
        item.abort = () => this.failItem(item, "Cancelled", "Delivery cancelled", true);
        signal?.addEventListener("abort", item.abort, { once: true });
        lane.queue.push(item); this.queued++; this.queueBytes += item.size;
        if (isBulk) { this.bulkQueued++; this.bulkQueueBytes += item.size; }
        this.schedule();
      });
    } catch (error) { return Promise.reject(error instanceof DeliveryError ? error : new DeliveryError("InvalidValue", error.message)); }
  }
  schedule() {
    if (!this.closed && this.scheduled === undefined) this.scheduled = setTimeout(() => { this.scheduled = undefined; this.pump(); }, 0);
  }
  activeCounts() {
    const active = [...this.tx.values()].map(lane => lane.queue[0]).filter(item => item?.seq !== undefined);
    return { total: active.length, bulk: active.filter(item => bulk(item.size)).length };
  }
  activate() {
    const count = this.activeCounts();
    const heads = [...this.tx.values()].filter(lane => !lane.failed && lane.queue.length && lane.queue[0].seq === undefined)
      .map(lane => lane.queue[0]).sort((a, b) => Number(bulk(a.size)) - Number(bulk(b.size)));
    for (const item of heads) {
      if (count.total >= this.limits.concurrent) break;
      if (bulk(item.size) && count.bulk >= Math.max(1, this.limits.concurrent - 1)) continue;
      if (item.lane.sequence >= (1n << 64n) - 1n) {
        item.lane.failed = true;
        for (const queued of [...item.lane.queue]) this.failItem(queued, "LaneFailed", "Peer delivery sequence exhausted", false);
        continue;
      }
      item.seq = ++item.lane.sequence; item.needsOpen = true; count.total++; if (bulk(item.size)) count.bulk++;
    }
  }
  submit(item, frame, preference) {
    const bytes = encode(frame, codec);
    if (bytes.length > 65536) throw new Error("Peer delivery physical frame is too large");
    const route = this.transmit(frame, bytes, preference);
    // Preserve temporary local congestion separately from an unavailable route.
    if (route === "blocked") return undefined;
    if (!route) return null;
    const stats = this.counter(item, route); stats.sentFrames++; stats.sentBytes += bytes.length;
    stats.reason = item.retries ? "retry-via-relay" : route === "direct" ? "direct-available" : "relay";
    return route;
  }
  pump() {
    if (this.closed || !this.limits) return;
    let work = false;
    for (const [id, control] of [...this.controls].slice(0, 8)) {
      try {
        if (!this.submit(control.item, control.frame, control.preference)) continue;
        this.controls.delete(id); work = true;
      } catch { this.controls.delete(id); }
    }
    this.activate();
    const active = [...this.tx.values()].map(lane => lane.queue[0]).filter(item => item?.seq !== undefined)
      .sort((a, b) => Number(bulk(a.size)) - Number(bulk(b.size)));
    for (const item of active) {
      try {
        if (item.needsOpen) {
          const route = this.submit(item, this.frame(item, "peer_open", { size: BigInt(item.size), sha256: item.sha256 }), item.retries ? "relay" : "auto");
          if (!route) continue;
          item.sent = true; item.route = route; item.lastOpenAt = this.now(); item.needsOpen = false; item.blocked = false;
          item.awaitCredit = true; item.nextRetry = this.now() + Math.min(2000, 500 * 2 ** item.retries);
          continue;
        }
        if (item.awaitCredit) continue;
        if (item.offset < item.size && item.offset < item.limit) {
          const length = Math.min(item.limits.chunkBytes, item.size - item.offset, item.limit - item.offset);
          const route = this.submit(item, this.frame(item, "peer_chunk", { offset: BigInt(item.offset),
            value: item.payload.subarray(item.offset, item.offset + length), bulk: bulk(item.size) }), item.route);
          if (item.blocked && route) item.nextRetry = this.now() + 500;
          item.blocked = route === undefined;
          if (!route) { if (!item.blocked && item.route === "direct") this.retry(item); continue; }
          if (item.offset < (item.maxSent ?? 0)) item.retransmitted = true;
          item.offset += length; item.maxSent = Math.max(item.maxSent ?? 0, item.offset); work = true;
        } else if (item.offset === item.size && !item.completeSent) {
          const route = this.submit(item, this.frame(item, "peer_complete"), item.route);
          if (item.blocked && route) item.nextRetry = this.now() + 500;
          item.blocked = route === undefined;
          if (!route) {
            if (!item.blocked && item.route === "direct") this.retry(item); continue;
          }
          if (item.completeEver) item.retransmitted = true;
          item.completeSent = item.completeEver = true;
        }
      } catch (error) { this.failItem(item, "TransportFailed", error.message, true); }
    }
    if (work) this.schedule();
  }
  retry(item) {
    if (item.retries >= item.limits.retries) { this.failItem(item, "Timeout", "Peer delivery retries exhausted", true); return; }
    item.retries++; this.counter(item, "relay").retries++;
    item.needsOpen = true; item.awaitCredit = true; item.completeSent = false; item.blocked = false;
    item.offset = item.ackOffset; item.limit = item.ackOffset; item.nextRetry = undefined;
    this.schedule();
  }
  tick() {
    if (this.closed) return;
    for (const lane of this.tx.values()) for (const item of [...lane.queue]) {
      if (this.now() >= item.deadline) this.failItem(item, "Timeout", "Peer delivery deadline expired", true);
      else if (lane.queue[0] === item && !item.blocked && item.nextRetry !== undefined && this.now() >= item.nextRetry) this.retry(item);
    }
    for (const lane of this.rx.values()) if (lane.active && this.now() >= lane.active.deadline)
      this.failReceive(lane, "Timeout", "Peer assembly deadline expired");
    this.schedule();
  }
  control(item, type, extra = {}, source = "relay") {
    if (this.closed) return;
    const frame = this.frame(item, type, extra);
    const id = JSON.stringify([item.peer, item.publisherEpoch, item.messageId, type]);
    if (this.controls.size >= 1024 && !this.controls.has(id)) return;
    this.controls.set(id, { item: { ...item, payload: undefined, buffer: undefined, lane: undefined }, frame,
      preference: source === "direct" ? "auto" : "relay" });
    this.schedule();
  }
  credit(item, retry = false) {
    item.grantLimit = Math.min(item.size, item.offset + item.limits.windowBytes);
    this.control(item, "peer_credit", { offset: BigInt(item.offset),
      limit: BigInt(item.grantLimit), ...(retry ? { retry: true } : {}) }, item.source);
  }
  incoming(frame, source, length) {
    if (this.closed || !this.limits || !peerDeliveryTypes.has(frame.type) || frame.type === "peer_relay_error" && source !== "relay") return;
    const member = this.member(frame.peer);
    if (frame.v !== 1 || frame.epoch !== this.epoch || frame.to !== this.peer || frame.targetEpoch !== this.publisherEpoch ||
        !member || member.publisherEpoch !== frame.publisherEpoch || typeof frame.topic !== "string" || !frame.topic.length ||
        bytesOf(frame.topic) > 256 || typeof frame.messageId !== "string" || !frame.messageId.length || bytesOf(frame.messageId) > 128 ||
        typeof frame.seq !== "bigint" || frame.seq < 1n || frame.seq > (1n << 64n) - 1n) return;
    let limits;
    try { limits = this.negotiated(member); } catch { return; }
    const laneKey = key(frame.peer, frame.publisherEpoch, frame.topic);
    const outgoing = this.tx.get(laneKey)?.queue[0];
    const response = ["peer_credit", "peer_done", "peer_relay_error"].includes(frame.type);
    if (response || frame.type === "peer_cancel" && outgoing?.messageId === frame.messageId && outgoing.seq === frame.seq) {
      if (!outgoing || outgoing.messageId !== frame.messageId || outgoing.seq !== frame.seq) return;
      const stats = this.counter(outgoing, source); stats.receivedFrames++; stats.receivedBytes += length;
      if (frame.type === "peer_credit") {
        const offset = integer(frame.offset), limit = integer(frame.limit);
        if (offset < 0 || offset > (outgoing.maxSent ?? 0) || limit < offset || limit > Math.min(outgoing.size, offset + limits.windowBytes) ||
            frame.retry !== undefined && typeof frame.retry !== "boolean") {
          this.failItem(outgoing, "InvalidCredit", "Invalid cumulative peer credit", true); return;
        }
        const old = outgoing.ackOffset;
        outgoing.ackOffset = Math.max(old, offset);
        if (outgoing.awaitCredit || frame.retry === true && offset >= old) {
          outgoing.offset = Math.max(outgoing.ackOffset, offset); outgoing.limit = Math.max(outgoing.ackOffset, limit);
          outgoing.completeSent = false; outgoing.awaitCredit = false;
        } else outgoing.limit = Math.max(outgoing.limit, limit);
        if (offset > old) outgoing.nextRetry = this.now() + 500;
        this.schedule();
      } else if (frame.type === "peer_done") {
        if (!outgoing.completeEver) { this.failItem(outgoing, "InvalidReceipt", "Receipt arrived before complete submission", true); return; }
        const rttMs = outgoing.retries || outgoing.retransmitted || source !== outgoing.route
          ? null : Math.max(0, this.now() - outgoing.lastOpenAt);
        stats.sentMessages++; stats.rttMs = rttMs;
        const receipt = { peer: outgoing.peer, publisherEpoch: outgoing.publisherEpoch, topic: outgoing.topic,
          messageId: outgoing.messageId, seq: outgoing.seq, route: source, rttMs };
        this.remove(outgoing); outgoing.resolve(receipt); this.schedule();
      } else this.failItem(outgoing, typeof frame.code === "string" ? frame.code : "InvalidFrame",
        typeof frame.message === "string" ? frame.message : "Peer delivery rejected", false);
      return;
    }
    const item = { peer: frame.peer, publisherEpoch: frame.publisherEpoch, topic: frame.topic, messageId: frame.messageId, seq: frame.seq,
      size: frame.type === "peer_open" ? integer(frame.size) : 0, limits, source };
    let lane = this.rx.get(laneKey);
    if (!lane) {
      if (this.tx.size + this.rx.size >= this.limits.lanes) { this.control(item, "peer_cancel", { code: "Backpressure" }, source); return; }
      lane = { peer: item.peer, publisherEpoch: item.publisherEpoch, topic: item.topic, high: 0n, failed: false, active: null };
      this.rx.set(laneKey, lane);
    }
    const counterItem = lane.active ?? { ...item, size: frame.seq === lane.high ? lane.last?.size ?? item.size : item.size };
    const stats = this.counter(counterItem, source); stats.receivedFrames++; stats.receivedBytes += length;
    if (frame.seq <= lane.high) {
      if (frame.seq === lane.high && (frame.messageId !== lane.last.messageId || frame.type === "peer_open" &&
          (item.size !== lane.last.size || frame.sha256 !== lane.last.sha256))) {
        this.failReceive(lane, "Conflict", "Conflicting completed metadata", false);
        this.control(item, "peer_cancel", { code: "Conflict" }, source); return;
      }
      this.control({ ...item, size: lane.last.size }, "peer_done", {}, source);
      return;
    }
    if (lane.failed) { this.control(item, "peer_cancel", { code: "LaneFailed" }, source); return; }
    if (frame.type === "peer_cancel") {
      this.failReceive(lane, typeof frame.code === "string" ? frame.code : "Cancelled",
        typeof frame.message === "string" ? frame.message : "Peer cancelled", false); lane.failed = true; return;
    }
    if (frame.type === "peer_open") {
      if (frame.seq !== lane.high + 1n || item.size < 1 || item.size > limits.messageBytes || !/^[a-f0-9]{64}$/.test(frame.sha256 ?? "")) {
        this.failReceive(lane, "InvalidOpen", "Invalid peer open", false);
        this.control(item, "peer_cancel", { code: "InvalidOpen" }, source); return;
      }
      if (lane.active) {
        if (lane.active.messageId !== item.messageId || lane.active.size !== item.size || lane.active.sha256 !== frame.sha256) {
          this.failReceive(lane, "Conflict", "Conflicting peer open"); return;
        }
        lane.active.source = source; this.credit(lane.active); return;
      }
      const active = [...this.rx.values()].filter(value => value.active);
      const isBulk = bulk(item.size);
      if (active.length >= this.limits.concurrent || this.receiveBytes + item.size > this.limits.receiveBytes ||
          isBulk && (active.filter(value => bulk(value.active.size)).length >= Math.max(1, this.limits.concurrent - 1) ||
            this.bulkReceiveBytes + item.size > this.limits.receiveBytes - 16384)) {
        lane.failed = true; this.control(item, "peer_cancel", { code: "Backpressure" }, source); return;
      }
      lane.active = { ...item, buffer: new Uint8Array(item.size), offset: 0, sha256: frame.sha256,
        deadline: this.now() + this.limits.assemblyMs };
      this.receiveBytes += item.size; if (isBulk) this.bulkReceiveBytes += item.size;
      this.credit(lane.active); return;
    }
    const assembly = lane.active;
    if (!assembly || assembly.messageId !== item.messageId || assembly.seq !== item.seq) {
      this.failReceive(lane, "UnknownTransfer", "Unknown peer transfer", false);
      this.control(item, "peer_cancel", { code: "UnknownTransfer" }, source); return;
    }
    assembly.source = source;
    if (frame.type === "peer_chunk") {
      const offset = integer(frame.offset), value = frame.value;
      if (!(value instanceof Uint8Array) || !value.length || value.length > limits.chunkBytes || offset < 0 ||
          offset + value.length > assembly.size || frame.bulk !== bulk(assembly.size)) {
        this.failReceive(lane, "InvalidChunk", "Invalid peer chunk"); return;
      }
      if (offset !== assembly.offset) {
        if (offset < assembly.offset && offset + value.length <= assembly.offset) {
          for (let i = 0; i < value.length; i++) if (assembly.buffer[offset + i] !== value[i]) {
            this.failReceive(lane, "Conflict", "Duplicate chunk bytes differ"); return;
          }
          this.credit(assembly);
        } else this.credit(assembly, true);
        return;
      }
      if (offset + value.length > assembly.grantLimit) {
        this.failReceive(lane, "InvalidChunk", "Peer chunk exceeded window"); return;
      }
      assembly.buffer.set(value, offset); assembly.offset += value.length; this.credit(assembly); return;
    }
    if (frame.type === "peer_complete") {
      if (assembly.offset !== assembly.size) { this.credit(assembly, true); return; }
      try {
        if (hash(assembly.buffer) !== assembly.sha256) throw new Error("Peer Value SHA-256 mismatch");
        const value = decode(assembly.buffer, { useBigInt64: true, maxStrLength: limits.messageBytes,
          maxBinLength: limits.messageBytes, maxArrayLength: limits.messageBytes, maxMapLength: limits.messageBytes });
        this.validate(value, Infinity);
        this.deliver({ v: 1, type: "peer_message", peer: assembly.peer, publisherEpoch: assembly.publisherEpoch,
          targetEpoch: this.publisherEpoch, topic: assembly.topic, messageId: assembly.messageId, seq: assembly.seq, value });
        lane.high = assembly.seq; lane.last = { messageId: assembly.messageId, size: assembly.size, sha256: assembly.sha256 };
        this.releaseAssembly(lane); stats.receivedMessages++;
        this.control(assembly, "peer_done", {}, source);
      } catch (error) { this.failReceive(lane, "ProtocolError", error.message); }
    }
  }
  remove(item) {
    const index = item.lane.queue.indexOf(item);
    if (index < 0) return;
    item.lane.queue.splice(index, 1); item.signal?.removeEventListener("abort", item.abort);
    this.queued--; this.queueBytes -= item.size;
    if (bulk(item.size)) { this.bulkQueued--; this.bulkQueueBytes -= item.size; }
    item.payload = null;
    if (!item.lane.queue.length && item.lane.sequence === 0n && !item.lane.failed)
      this.tx.delete(key(item.peer, item.publisherEpoch, item.topic));
  }
  failItem(item, code, message, notify) {
    if (!item.lane.queue.includes(item)) return;
    code = publicCode(code);
    const assigned = item.seq !== undefined;
    if (notify && item.sent) this.control(item, "peer_cancel", { code, message }, "relay");
    const stats = this.counter(item, item.route ?? "relay"); stats.reason = code;
    this.remove(item); item.reject(new DeliveryError(code, message, item.sent ? "unknown" : "not-sent"));
    if (assigned) {
      item.lane.failed = true;
      for (const queued of [...item.lane.queue]) {
        this.remove(queued); queued.reject(new DeliveryError("LaneFailed", "Earlier delivery in this lane failed"));
      }
    }
    this.schedule();
  }
  releaseAssembly(lane) {
    if (!lane.active) return;
    this.receiveBytes -= lane.active.size;
    if (bulk(lane.active.size)) this.bulkReceiveBytes -= lane.active.size;
    lane.active.buffer = null; lane.active = null;
  }
  failReceive(lane, code, message, notify = true) {
    if (notify && lane.active) this.control(lane.active, "peer_cancel", { code, message }, lane.active.source);
    this.releaseAssembly(lane); lane.failed = true;
  }
  departed(peer, publisherEpoch) {
    for (const [id, lane] of this.tx) if (lane.peer === peer && lane.publisherEpoch === publisherEpoch) {
      for (const item of [...lane.queue]) this.failItem(item, "PeerUnavailable", "Recipient generation left", false);
      this.tx.delete(id);
    }
    for (const [id, lane] of this.rx) if (lane.peer === peer && lane.publisherEpoch === publisherEpoch) {
      this.releaseAssembly(lane); this.rx.delete(id);
    }
    for (const [id, control] of this.controls) if (control.item.peer === peer && control.item.publisherEpoch === publisherEpoch) this.controls.delete(id);
    for (const [id, stats] of this.counters) if (stats.peer === peer && stats.publisherEpoch === publisherEpoch) this.counters.delete(id);
  }
  close() {
    if (this.closed) return;
    this.closed = true; clearInterval(this.timer); clearTimeout(this.scheduled);
    for (const lane of this.tx.values()) for (const item of [...lane.queue]) this.failItem(item, "Closed", "Client closed", false);
    for (const lane of this.rx.values()) this.releaseAssembly(lane);
    this.tx.clear(); this.rx.clear(); this.controls.clear();
  }
}
