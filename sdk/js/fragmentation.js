import { encode, decode } from "@msgpack/msgpack";
import { sha256 } from "@noble/hashes/sha2.js";

export const fragmentationDefaults = Object.freeze({
  logicalBytes: 4194304, chunkBytes: 12288, reassemblyBytes: 8388608,
  inflightBytes: 65536, concurrent: 4, timeoutMs: 30000, queueBytes: 8388608,
});
const codec = { useBigInt64: true, maxDepth: 40 };
const peerFrames = new Set(["peer_open", "peer_credit", "peer_chunk", "peer_complete", "peer_done", "peer_cancel", "peer_relay_error"]);
const digest = bytes => Array.from(sha256(bytes), b => b.toString(16).padStart(2, "0")).join("");
function uint(value, name) {
  if (typeof value === "bigint" && value >= 0n && value <= BigInt(Number.MAX_SAFE_INTEGER)) value = Number(value);
  if (!Number.isSafeInteger(value) || value < 0) throw new Error(`Invalid fragment ${name}`);
  return value;
}
export function fragmentationLimits(options = {}) {
  if (!options || typeof options !== "object" || Array.isArray(options)) throw new TypeError("fragmentation must be an options object or false");
  const limits = {};
  for (const [key, fallback] of Object.entries(fragmentationDefaults)) {
    limits[key] = uint(options[key] ?? fallback, key);
    if (!limits[key]) throw new RangeError(`fragmentation.${key} must be positive`);
  }
  limits.chunkBytes = Math.min(16384, limits.chunkBytes);
  return limits;
}
function lane(frame) {
  if (["peer_open", "peer_credit", "peer_complete", "peer_done", "peer_cancel", "peer_relay_error"].includes(frame.type)) return "peer_control";
  if (frame.type === "peer_chunk") return frame.bulk ? "peer_bulk" : "peer_small";
  if (["route_signal", "peer_joined", "peer_left"].includes(frame.type)) return "routing";
  if (["message", "catchup", "checkpoint"].includes(frame.type) || frame.type === "ack" && frame.seq !== undefined) return "message";
  if (["set", "delete", "state", "snapshot"].includes(frame.type) || frame.type === "ack" && frame.revision !== undefined) return "state";
  return frame.type === "event" ? "event" : "control";
}

/** One scheduler owns reliable writes; fragment handshakes bypass occupied lanes. */
export class Fragmentation {
  constructor({ limits, write, buffered, deliver, fail }) {
    this.limits = limits;
    this.write = write;
    this.buffered = buffered;
    this.deliver = deliver;
    this.fail = fail;
    this.queues = new Map(["control", "routing", "peer_control", "peer_small", "message", "state", "event", "peer_bulk"].map(key => [key, []]));
    this.outgoing = new Map();
    this.incoming = new Map();
    this.queuedBytes = 0;
    this.reservedBytes = 0;
    this.closed = false;
  }
  select(advertised) {
    if (!advertised) { this.limits = null; return; }
    for (const key of Object.keys(fragmentationDefaults)) {
      if (advertised[key] === undefined) throw new Error(`Missing selected fragment limit ${key}`);
    }
    const selected = fragmentationLimits(advertised);
    if (!this.limits) throw new Error("Unrequested fragmentation capability");
    for (const key of Object.keys(selected)) {
      if (selected[key] > this.limits[key]) throw new Error(`Server exceeded advertised ${key}`);
    }
    this.limits = selected;
  }
  control(type, fields) {
    this.write(encode({ v: 1, type, ...fields }, codec));
  }
  enqueue(frame, bytes) {
    if (this.closed) throw new Error("Client is closed");
    const limits = this.limits;
    if (peerFrames.has(frame.type) && bytes.length > 65536) throw new RangeError("Peer delivery frame exceeds physical limit");
    if (bytes.length > (limits?.logicalBytes ?? 65536)) throw new RangeError("Encoded reliable envelope exceeds logicalBytes");
    if (this.queuedBytes + bytes.length > (limits?.queueBytes ?? 8388608)) throw new Error("Reliable send queue is full");
    if ([...this.queues.values()].reduce((count, queue) => count + queue.length, 0) >= 256)
      throw new Error("Reliable send queue contains 256 envelopes");
    const entry = { bytes, id: frame.id, offset: 0, credit: 0,
      inline: frame.type.startsWith("blob_") && bytes.length <= 65536 || frame.type === "route_signal" && bytes.length <= 16384 ||
        ["peer_open", "peer_credit", "peer_chunk", "peer_complete", "peer_done", "peer_cancel", "peer_relay_error"].includes(frame.type) && bytes.length <= 65536 };
    this.queues.get(lane(frame)).push(entry);
    this.queuedBytes += bytes.length;
    this.schedule();
  }
  schedule(delay = 0) {
    if (!this.closed && this.tick === undefined)
      this.tick = setTimeout(() => { this.tick = undefined; this.pump(); }, delay);
  }
  pump() {
    if (this.closed) return;
    try {
      if (this.buffered() > 262144) { this.schedule(10); return; }
      let work = false;
      for (const queue of this.queues.values()) {
        const entry = queue[0];
        if (!entry) continue;
        const limits = this.limits;
        if (!limits || entry.inline || entry.bytes.length <= limits.chunkBytes) {
          this.write(entry.bytes); this.finish(queue); work = true; continue;
        }
        if (!entry.transfer) {
          if (this.outgoing.size >= limits.concurrent) continue;
          entry.transfer = crypto.randomUUID();
          entry.timer = setTimeout(() => this.fatal(new Error("Fragment transfer timed out"), entry.transfer), limits.timeoutMs);
          this.outgoing.set(entry.transfer, { entry, queue });
          this.control("frag_open", { transfer: entry.transfer, size: entry.bytes.length, sha256: digest(entry.bytes) });
          continue;
        }
        if (entry.ended || !entry.credit) continue;
        const length = Math.min(limits.chunkBytes, entry.credit, entry.bytes.length - entry.offset);
        const offset = entry.offset;
        entry.credit -= length; entry.offset += length;
        this.control("frag_chunk", { transfer: entry.transfer, offset, value: entry.bytes.subarray(offset, offset + length) });
        if (entry.offset === entry.bytes.length) {
          entry.ended = true;
          this.control("frag_end", { transfer: entry.transfer });
        } else if (entry.credit) work = true;
      }
      if (work) this.schedule();
    } catch (error) { this.fatal(error); }
  }
  finish(queue) {
    const entry = queue.shift();
    this.queuedBytes -= entry.bytes.length;
    clearTimeout(entry.timer);
    if (entry.transfer) this.outgoing.delete(entry.transfer);
  }
  receive(frame, byteLength) {
    if (!frame || typeof frame.type !== "string") throw new Error("Invalid envelope");
    if (!frame.type.startsWith("frag_")) {
      if (this.limits && !["latest", "live"].includes(frame.type) && byteLength > this.limits.logicalBytes)
        throw new Error("Reliable envelope exceeds logicalBytes");
      this.deliver(frame, byteLength); return;
    }
    const limits = this.limits;
    const id = frame.transfer;
    if (!limits || frame.v !== 1 || typeof id !== "string" || !id.length || id.length > 128)
      throw new Error("Invalid fragmentation envelope");
    try {
      const outbound = this.outgoing.get(id);
      if (frame.type === "frag_cancel") throw new Error(`Fragment cancelled: ${frame.reason ?? "peer cancelled"}`);
      if (frame.type === "frag_credit") {
        const credit = uint(frame.credit, "credit");
        if (!outbound) return; // Completion may race with replenishment credits.
        const entry = outbound.entry;
        if (!credit || entry.credit + credit > limits.inflightBytes) throw new Error("Fragment credit exceeds limit");
        entry.credit += credit; this.schedule(); return;
      }
      if (frame.type === "frag_done") {
        if (!outbound) return;
        if (!outbound.entry.ended) throw new Error("Premature fragment completion");
        this.finish(outbound.queue); this.schedule(); return;
      }
      if (frame.type === "frag_open") {
        const size = uint(frame.size, "size");
        if (!size || size > limits.logicalBytes || this.incoming.has(id) ||
            this.incoming.size >= limits.concurrent || this.reservedBytes + size > limits.reassemblyBytes ||
            typeof frame.sha256 !== "string" || !/^[a-f0-9]{64}$/.test(frame.sha256))
          throw new Error("Fragment reservation rejected");
        const entry = { bytes: new Uint8Array(size), offset: 0, hash: frame.sha256, credit: Math.min(size, limits.inflightBytes) };
        entry.timer = setTimeout(() => this.fatal(new Error("Fragment receive timed out"), id), limits.timeoutMs);
        this.incoming.set(id, entry); this.reservedBytes += size;
        this.control("frag_credit", { transfer: id, credit: entry.credit }); return;
      }
      const entry = this.incoming.get(id);
      if (!entry) throw new Error("Unknown fragment transfer");
      if (frame.type === "frag_chunk") {
        const offset = uint(frame.offset, "offset");
        const bytes = frame.value;
        if (!(bytes instanceof Uint8Array) || !bytes.length || bytes.length > limits.chunkBytes ||
            bytes.length > entry.credit || offset !== entry.offset || offset + bytes.length > entry.bytes.length)
          throw new Error("Invalid fragment chunk or offset");
        entry.credit -= bytes.length;
        entry.bytes.set(bytes, offset); entry.offset += bytes.length;
        entry.credit += bytes.length;
        this.control("frag_credit", { transfer: id, credit: bytes.length }); return;
      }
      if (frame.type !== "frag_end") throw new Error("Unknown fragmentation operation");
      if (entry.offset !== entry.bytes.length || digest(entry.bytes) !== entry.hash) throw new Error("Fragment byte count or SHA-256 mismatch");
      const logical = decode(entry.bytes, { useBigInt64: true, maxStrLength: limits.logicalBytes,
        maxBinLength: limits.logicalBytes, maxArrayLength: limits.logicalBytes, maxMapLength: limits.logicalBytes });
      if (!logical || logical.v !== 1 || typeof logical.type !== "string" || logical.type.startsWith("frag_") || ["latest", "live"].includes(logical.type) || peerFrames.has(logical.type))
        throw new Error("Forbidden fragmented envelope");
      clearTimeout(entry.timer); this.incoming.delete(id); this.reservedBytes -= entry.bytes.length;
      this.deliver(logical);
      this.control("frag_done", { transfer: id });
    } catch (error) { this.fatal(error, id); }
  }
  fatal(error, transfer) {
    if (this.closed) return;
    if (transfer) { try { this.control("frag_cancel", { transfer, reason: error.message }); } catch {} }
    this.close();
    this.fail(error);
  }
  close() {
    this.closed = true;
    clearTimeout(this.tick); this.tick = undefined;
    for (const { entry } of this.outgoing.values()) clearTimeout(entry.timer);
    for (const entry of this.incoming.values()) clearTimeout(entry.timer);
    this.outgoing.clear(); this.incoming.clear();
    for (const queue of this.queues.values()) queue.length = 0;
    this.queuedBytes = this.reservedBytes = 0;
  }
}
