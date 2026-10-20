import { encode, decode } from "@msgpack/msgpack";
import { sha256 } from "@noble/hashes/sha2.js";
import { Fragmentation, fragmentationLimits } from "./fragmentation.js";
import { DirectRoutes, routingOptions } from "./routing.js";
import { PeerDelivery, deliveryLimits, peerDeliveryTypes, DeliveryError } from "./peer-delivery.js";
export { DeliveryError };

const U64_MAX = (1n << 64n) - 1n;
const codec = { useBigInt64: true, maxDepth: 40 };
const hex = bytes => Array.from(bytes, b => b.toString(16).padStart(2, "0")).join("");
const uid = () => globalThis.crypto.randomUUID();
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

export class KokosyncError extends Error {
  constructor(code, message) { super(message); this.name = "KokosyncError"; this.code = code; }
}

export function counter(value, name = "counter") {
  if (typeof value !== "bigint" || value < 0n || value > U64_MAX)
    throw new TypeError(`${name} must be a uint64 bigint`);
  return value;
}

export function validateValue(value, maxBytes = 4194304) {
  const ancestors = new Set();
  function visit(v, depth) {
    if (depth > 32) throw new TypeError("Application value exceeds nesting depth 32");
    if (v === null || typeof v === "boolean" || typeof v === "string") return;
    if (typeof v === "number") {
      if (!Number.isFinite(v) || Math.abs(v) > Number.MAX_SAFE_INTEGER)
        throw new TypeError("Application numbers must be finite and within the safe numeric range");
      return;
    }
    if (v instanceof Uint8Array) return;
    if (typeof v !== "object" || ancestors.has(v)) throw new TypeError("Unsupported or cyclic application value");
    if (!Array.isArray(v) && Object.getPrototypeOf(v) !== Object.prototype && Object.getPrototypeOf(v) !== null)
      throw new TypeError("Application maps must be plain string-key objects");
    if (Object.getOwnPropertySymbols(v).length) throw new TypeError("Symbol keys are unsupported");
    ancestors.add(v);
    for (const child of Array.isArray(v) ? v : Object.values(v)) visit(child, depth + 1);
    ancestors.delete(v);
  }
  visit(value, 0);
  if (Number.isFinite(maxBytes) && encode(value, codec).length > maxBytes) throw new RangeError(`Application value exceeds ${maxBytes} encoded bytes`);
  return value;
}

async function openWebSocket(base, timeout) {
  const url = new URL("/ws", base);
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
  const socket = new WebSocket(url);
  socket.binaryType = "arraybuffer";
  await new Promise((resolve, reject) => {
    const timer = setTimeout(() => { socket.close(); reject(new Error("WebSocket connection timed out")); }, timeout);
    socket.onopen = () => { clearTimeout(timer); resolve(); };
    socket.onerror = () => { clearTimeout(timer); reject(new Error("WebSocket connection failed")); };
  });
  return { reliable: socket, realtime: socket, close: () => socket.close(), name: "websocket" };
}

async function openWebRTC(base, timeout) {
  if (!globalThis.RTCPeerConnection) throw new Error("WebRTC is unavailable; explicitly select transport: 'websocket'");
  const pc = new RTCPeerConnection({ iceServers: [] });
  const reliable = pc.createDataChannel("kokosync", { ordered: true });
  const realtime = pc.createDataChannel("kokosync-realtime", { ordered: false, maxRetransmits: 0 });
  reliable.binaryType = realtime.binaryType = "arraybuffer";
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(new Error("WebRTC connection timed out")), timeout);
  const aborted = () => controller.signal.throwIfAborted();
  try {
    await pc.setLocalDescription(await pc.createOffer());
    while (pc.iceGatheringState !== "complete") { aborted(); await sleep(20); }
    const response = await fetch(new URL("/offer", base), {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify(pc.localDescription), signal: controller.signal,
    });
    if (!response.ok) throw new Error(`Offer rejected: HTTP ${response.status}`);
    await pc.setRemoteDescription(await response.json());
    while (reliable.readyState !== "open" || realtime.readyState !== "open") {
      aborted();
      if (pc.connectionState === "failed" || pc.connectionState === "closed") throw new Error("WebRTC connection failed");
      await sleep(20);
    }
    return { reliable, realtime, close: () => pc.close(), name: "webrtc", pc };
  } catch (error) { pc.close(); throw error; }
  finally { clearTimeout(timer); }
}

/** Ordered arbitrary messaging; state caching and application checkpoints are opt-in. */
export class KokosyncClient {
  static async connect({ url = globalThis.location?.origin ?? "http://127.0.0.1:8080",
    room, peer = uid(), state = false, transport = "webrtc", timeout = 15000, fragmentation = {}, routing = {}, peerDelivery = {} } = {}) {
    if (!room) throw new TypeError("room is required");
    if (!["webrtc", "websocket"].includes(transport)) throw new TypeError("Choose webrtc or websocket explicitly");
    const limits = fragmentation === false ? null : fragmentationLimits(fragmentation);
    routing = routingOptions(routing);
    peerDelivery = deliveryLimits(peerDelivery);
    const wire = await (transport === "webrtc" ? openWebRTC(url, timeout) : openWebSocket(url, timeout));
    const client = new KokosyncClient(wire, { room, peer, state, timeout, fragmentation: limits, routing, peerDelivery });
    try {
      client.joined = await client.request("join", { room, peer, state, routing: client.routeManager.advertisement(),
        ...(limits ? { fragmentation: limits } : {}), ...(peerDelivery ? { peerDelivery } : {}) }, "joined");
      return client;
    } catch (error) { client.close(); throw error; }
  }

  constructor(wire, options) {
    this.transport = wire.name;
    this.options = options;
    this.wire = wire;
    this.pending = new Map();
    this.listeners = new Map();
    this.state = new Map();
    this.transfers = new Map();
    this.revision = 0n;
    this.seq = 0n;
    this.closed = false;
    this.joinedComplete = false;
    this.latestSequences = new Map();
    this.liveSequences = new Map();
    this.routeManager = new DirectRoutes({ peer: options.peer, options: options.routing,
      signal: fields => this.send("route_signal", fields),
      deliver: (frame, source) => this.receive(frame, source), changed: entry => this.emit("route", entry), validateValue,
      deliveryReceive: (frame, length) => this.peerDeliveries.incoming(frame, "direct", length) });
    this.peerDeliveries = new PeerDelivery({ peer: options.peer, limits: options.peerDelivery === null ? false : options.peerDelivery,
      member: peer => this.routeManager.members.get(peer), validate: validateValue,
      deliver: frame => this.emit("peer_message", frame),
      transmit: (frame, bytes, preference) => {
        if (this.closed) throw new Error("Client closed");
        if (preference !== "relay") {
          const direct = this.routeManager.sendDelivery(frame, bytes);
          if (direct) return "direct";
          if (direct === null && preference === "direct") return "blocked";
        }
        if (preference === "direct" || wire.reliable.bufferedAmount > 262144) return null;
        wire.reliable.send(bytes); return "relay";
      },
    });
    this.fragments = new Fragmentation({
      limits: options.fragmentation === false || options.fragmentation === null ? null : fragmentationLimits(options.fragmentation),
      write: bytes => {
        if (wire.reliable.bufferedAmount > 1048576) throw new Error("Transport backpressure limit exceeded");
        wire.reliable.send(bytes);
      }, buffered: () => wire.reliable.bufferedAmount ?? 0,
      deliver: (frame, byteLength) => this.receive(frame, "relay", byteLength), fail: error => { this.emit("error", error); this.close(error); },
    });
    const receive = event => {
      try {
        const bytes = new Uint8Array(event.data);
        if (bytes.length > 65536) throw new RangeError("Frame exceeds 65536 bytes");
        const frame = decode(bytes, { useBigInt64: true, maxStrLength: 65536, maxBinLength: 65536,
          maxArrayLength: 65536, maxMapLength: 65536 });
        if (peerDeliveryTypes.has(frame.type)) this.peerDeliveries.incoming(frame, "relay", bytes.length);
        else this.fragments.receive(frame, bytes.length);
      } catch (error) { this.emit("error", error); this.close(); }
    };
    wire.reliable.onmessage = receive;
    wire.realtime.onmessage = receive;
    wire.reliable.onclose = () => this.close();
    wire.reliable.onerror = () => this.emit("error", new Error("Transport error"));
    if (wire.pc) wire.pc.onconnectionstatechange = () => {
      if (["failed", "closed"].includes(wire.pc.connectionState)) this.close();
    };
  }

  on(type, listener) {
    let listeners = this.listeners.get(type);
    if (!listeners) this.listeners.set(type, listeners = new Set());
    listeners.add(listener);
    return () => listeners.delete(listener);
  }
  emit(type, message) {
    for (const listener of this.listeners.get(type) ?? []) {
      try {
        const result = listener(message);
        if (result?.catch) result.catch(error => { if (type !== "error") this.emit("error", error); });
      } catch (error) { if (type !== "error") this.emit("error", error); }
    }
  }
  send(type, fields = {}) {
    if (this.closed) throw new Error("Client is closed");
    const frame = { v: 1, type, ...fields };
    const data = encode(frame, codec);
    const realtime = type === "latest" || type === "live";
    // Reserve 128 bytes for the relay's peer/publisher generation metadata.
    if (data.length > (realtime ? 872 : this.fragments.limits?.logicalBytes ?? 65536)) throw new RangeError(`Encoded ${type} frame is too large`);
    if (type === "route_signal" && data.length > 16384) throw new RangeError("Route signal exceeds 16384 bytes");
    if (peerDeliveryTypes.has(type) && data.length > 65536) throw new RangeError("Peer delivery frame exceeds 65536 bytes");
    if (!realtime && type !== "join") { this.fragments.enqueue({ type, ...fields }, data); return; }
    if (type === "join" && data.length > 65536) throw new RangeError("Join must fit in one frame");
    const channel = realtime ? this.wire.realtime : this.wire.reliable;
    const relay = bytes => {
      if (channel.bufferedAmount > 1048576) throw new Error("Relay transport backpressure: wait before sending more");
      channel.send(bytes);
    };
    if (realtime) this.routeManager.send(frame, relay);
    else relay(data);
  }
  request(type, fields = {}, response = "ack", { signal } = {}) {
    if (signal?.aborted) return Promise.reject(signal.reason ?? new Error("Request cancelled"));
    const id = fields.id ?? uid();
    if (this.pending.has(id)) return Promise.reject(new Error("Request ID is already pending"));
    return new Promise((resolve, reject) => {
      const abort = () => this.fragments.fatal(signal.reason instanceof Error ? signal.reason : new Error("Request cancelled"));
      const timer = setTimeout(() => {
        this.pending.delete(id);
        signal?.removeEventListener("abort", abort);
        const error = new KokosyncError("Timeout", `${type} timed out; its outcome may be unknown`);
        reject(error);
        this.fragments.fatal(error);
      }, this.options.timeout);
      this.pending.set(id, { response, resolve, reject, timer, signal, abort });
      signal?.addEventListener("abort", abort, { once: true });
      try { this.send(type, { ...fields, id }); }
      catch (error) {
        clearTimeout(timer); signal?.removeEventListener("abort", abort);
        this.pending.delete(id); reject(error);
      }
    });
  }
  receive(frame, source = "relay", byteLength) {
    if (!this.joinedComplete && (frame.type === "latest" || frame.type === "live")) return;
    if (frame.type === "joined") {
      this.fragments.select(frame.capabilities?.fragmentation);
      this.seq = frame.seq;
      this.revision = frame.revision;
      this.capabilities = frame.capabilities;
      for (const entry of frame.entries ?? []) this.state.set(entry.key, entry);
      this.joinedComplete = true;
      this.routeManager.joined(frame);
      this.peerDeliveries.joined(frame);
    }
    if (frame.type === "peer_joined") this.routeManager.memberJoined(frame);
    if (frame.type === "peer_left") {
      const member = this.routeManager.members.get(frame.peer);
      if (member?.publisherEpoch === frame.publisherEpoch && frame.epoch === this.routeManager.epoch)
        this.peerDeliveries.departed(frame.peer, frame.publisherEpoch);
      this.routeManager.memberLeft(frame);
    }
    if (frame.type === "route_signal") this.routeManager.receiveSignal(frame);
    if (peerDeliveryTypes.has(frame.type)) {
      this.peerDeliveries.incoming(frame, source, encode(frame, codec).length);
      return;
    }
    if (frame.type === "message") this.seq = frame.seq > this.seq ? frame.seq : this.seq;
    if (frame.type === "state") {
      this.revision = frame.revision;
      this.state.set(frame.key, frame);
    }
    if (frame.type === "snapshot") {
      this.state.clear();
      for (const entry of frame.entries) this.state.set(entry.key, entry);
      this.revision = frame.revision;
    }
    if (frame.type === "latest" || frame.type === "live") {
      if (!this.routeManager.acceptsPublisher(frame)) return;
      if (source === "relay") this.routeManager.relayReceived(frame, byteLength);
      const key = JSON.stringify([frame.peer, frame.publisherEpoch, frame.type === "latest" ? frame.key : frame.stream]);
      const seen = frame.type === "latest" ? this.latestSequences : this.liveSequences;
      if (seen.has(key) && seen.get(key) >= frame.seq) return;
      seen.set(key, frame.seq);
      if (seen.size > 1024) seen.delete(seen.keys().next().value);
    }
    const pending = this.pending.get(frame.id);
    if (pending && (frame.type === pending.response || frame.type === "error")) {
      clearTimeout(pending.timer);
      pending.signal?.removeEventListener("abort", pending.abort);
      this.pending.delete(frame.id);
      if (frame.type === "error") pending.reject(new KokosyncError(frame.code, frame.message));
      else pending.resolve(frame);
    }
    if (frame.type.startsWith("blob_")) this.receiveBlob(frame);
    this.emit(frame.type, frame);
  }
  value(value) { return validateValue(value, this.fragments.limits?.logicalBytes ?? 16384); }
  message(topic, value, idOrOptions) {
    const options = typeof idOrOptions === "string" ? { id: idOrOptions } : idOrOptions ?? {};
    return this.request("message", { topic, value: this.value(value), ...(options.id ? { id: options.id } : {}) }, "ack", options);
  }
  event(topic, value, ttlMs = 1000) { return this.request("event", { topic, value: this.value(value), ttlMs }); }
  latest(key, value, seq) { this.send("latest", { key, value: validateValue(value, 16384), seq: counter(seq, "seq") }); }
  live(stream, value, seq, ttlMs = 250, codecName) {
    if (!(value instanceof Uint8Array)) throw new TypeError("live value must be Uint8Array");
    this.send("live", { stream, value, seq: counter(seq, "seq"), ttlMs, ...(codecName ? { codec: codecName } : {}) });
  }
  set(key, value, expected, idOrOptions) {
    const options = typeof idOrOptions === "string" ? { id: idOrOptions } : idOrOptions ?? {};
    return this.request("set", { key, value: this.value(value),
      ...(expected === undefined ? {} : { expected: counter(expected, "expected") }), ...(options.id ? { id: options.id } : {}) }, "ack", options);
  }
  delete(key, expected, id) {
    return this.request("delete", { key, ...(expected === undefined ? {} : { expected: counter(expected, "expected") }),
      ...(id ? { id } : {}) });
  }
  snapshot() { return this.request("snapshot", {}, "snapshot"); }
  checkpoint(seq, value) { return this.request("checkpoint", { seq: counter(seq, "seq"), value: this.value(value) }); }
  catchup() { return this.request("catchup", {}, "catchup"); }
  ping() { return this.request("ping", {}, "pong"); }
  async stats() { return { ...await this.request("stats", {}, "stats"), routing: { ...this.routeManager.totals } }; }
  routes() { return this.routeManager.routes(); }
  setRouting(options) { this.routeManager.setOptions(options); this.options.routing = this.routeManager.options; }
  sendTo(peer, topic, value, options) { return this.peerDeliveries.sendTo(peer, topic, value, options); }
  deliveryStats() { return [...this.peerDeliveries.stats(), ...this.routeManager.deliveryStats()]; }
  get(key) { const entry = this.state.get(key); return entry?.deleted ? undefined : entry?.value; }

  /**
   * Subscribe before capturing catchup. Reducers must be synchronous and process every
   * raw message (even ignored topics), so checkpoints always represent a contiguous seq.
   */
  async follow({ restore, apply, capture }) {
    let active = false, stopped = false, failed = null, seq = 0n, buffered = [], bufferedBytes = 0;
    const bufferLimit = Math.min(this.fragments.limits?.queueBytes ?? 8388608,
      this.fragments.limits?.reassemblyBytes ?? 8388608);
    const clearBuffer = () => { buffered = []; bufferedBytes = 0; };
    const run = frame => {
      if (frame.seq <= seq) return;
      if (frame.seq !== seq + 1n) throw new KokosyncError("SequenceGap", `Expected ${seq + 1n}, received ${frame.seq}`);
      if (apply(frame)?.then) throw new TypeError("follow.apply must be synchronous");
      seq = frame.seq;
    };
    const off = this.on("message", frame => {
      if (stopped || failed) return;
      if (!active) {
        try {
          const size = encode(frame, codec).length;
          if (buffered.length >= 256 || bufferedBytes + size > bufferLimit)
            throw new KokosyncError("ResyncRequired", "Catchup message buffer exceeded its limit; reconnect and resynchronize");
          buffered.push(frame); bufferedBytes += size;
        } catch (error) {
          failed = error; clearBuffer(); off();
          this.emit("error", error); this.close(error);
        }
        return;
      }
      try { run(frame); } catch (error) { failed = error; this.emit("error", error); }
    });
    try {
      const result = await this.catchup();
      if (failed) throw failed;
      if (restore(result.checkpoint.value)?.then) throw new TypeError("follow.restore must be synchronous");
      seq = result.checkpoint.seq;
      for (const frame of result.messages) run(frame);
      if (seq !== result.seq) throw new KokosyncError("SequenceGap", "Catchup did not cover its declared sequence");
      for (const frame of buffered) run(frame);
      clearBuffer();
      active = true;
      const client = this;
      return {
        get seq() { return seq; },
        checkpoint() {
          if (stopped || failed) return Promise.reject(failed ?? new Error("Follower stopped"));
          const value = capture();
          return client.checkpoint(seq, value);
        },
        stop() { stopped = true; clearBuffer(); off(); },
      };
    } catch (error) { clearBuffer(); off(); throw error; }
  }

  /** Accept an incoming blob_open with an async write/close/abort sink. */
  acceptBlob(offer, sink) {
    if (this.transfers.has(offer.transfer)) throw new Error("Transfer already accepted");
    const transfer = { receiving: true, sink, size: counter(offer.size, "size"), offset: 0n,
      hash: sha256.create(), chain: Promise.resolve() };
    transfer.done = new Promise((resolve, reject) => { transfer.resolve = resolve; transfer.reject = reject; });
    // The caller still receives rejection; mark handled until it attaches its own handler.
    transfer.done.catch(() => {});
    this.transfers.set(offer.transfer, transfer);
    try { this.send("blob_credit", { transfer: offer.transfer, credit: 32768n }); }
    catch (error) { this.failBlob(offer.transfer, error); }
    return transfer.done;
  }
  receiveBlob(frame) {
    const transfer = this.transfers.get(frame.transfer);
    if (!transfer) return;
    if (frame.type === "blob_cancel") { this.failBlob(frame.transfer, new Error("Blob transfer cancelled by peer")); return; }
    if (frame.type === "blob_credit" && !transfer.receiving) {
      transfer.credit += frame.credit; transfer.wake?.(); return;
    }
    if (frame.type === "blob_done" && !transfer.receiving) {
      this.transfers.delete(frame.transfer); transfer.resolve({ transfer: frame.transfer }); return;
    }
    if (!transfer.receiving) return;
    transfer.chain = transfer.chain.then(async () => {
      if (!this.transfers.has(frame.transfer)) return;
      if (frame.type === "blob_chunk") {
        if (frame.offset !== transfer.offset || transfer.offset + BigInt(frame.value.length) > transfer.size)
          throw new Error("Invalid blob offset or size");
        await transfer.sink.write(frame.value);
        if (!this.transfers.has(frame.transfer)) return;
        transfer.hash.update(frame.value);
        transfer.offset += BigInt(frame.value.length);
        this.send("blob_credit", { transfer: frame.transfer, credit: BigInt(frame.value.length) });
      } else if (frame.type === "blob_end") {
        if (transfer.offset !== transfer.size || hex(transfer.hash.digest()) !== frame.sha256)
          throw new Error("Blob byte count or SHA-256 mismatch");
        await transfer.sink.close?.();
        if (!this.transfers.has(frame.transfer)) return;
        this.send("blob_done", { transfer: frame.transfer });
        this.transfers.delete(frame.transfer);
        transfer.resolve({ transfer: frame.transfer, size: transfer.size, sha256: frame.sha256 });
      }
    }).catch(error => this.cancelBlob(frame.transfer, error));
  }
  failBlob(id, error) {
    const transfer = this.transfers.get(id);
    if (!transfer) return;
    this.transfers.delete(id);
    transfer.error = error;
    transfer.wake?.();
    transfer.reject(error);
    if (transfer.receiving) Promise.resolve().then(() => transfer.sink.abort?.(error)).catch(() => {});
  }
  cancelBlob(id, reason = new Error("Blob cancelled")) {
    try { if (!this.closed) this.send("blob_cancel", { transfer: id }); } catch {}
    this.failBlob(id, reason);
  }
  /** source is Uint8Array, Blob, ReadableStream, or AsyncIterable<Uint8Array>. */
  async sendBlob(to, source, { size, transfer = uid(), signal } = {}) {
    size ??= source instanceof Uint8Array ? BigInt(source.length) : typeof source.size === "number" ? BigInt(source.size) : undefined;
    counter(size, "size");
    if (size > 67108864n) throw new RangeError("Blob exceeds 64 MiB");
    const state = { receiving: false, credit: 0n };
    const done = new Promise((resolve, reject) => { state.resolve = resolve; state.reject = reject; });
    done.catch(() => {});
    if (this.transfers.has(transfer)) throw new Error("Duplicate transfer ID");
    this.transfers.set(transfer, state);
    const abort = () => this.cancelBlob(transfer, signal.reason ?? new Error("Blob cancelled"));
    signal?.addEventListener("abort", abort, { once: true });
    try {
      signal?.throwIfAborted();
      await this.request("blob_open", { transfer, to, size });
      const hash = sha256.create();
      let offset = 0n;
      const iterable = source instanceof Uint8Array ? [source] : typeof source.stream === "function" ? source.stream() : source;
      for await (const input of iterable) {
        if (!(input instanceof Uint8Array)) throw new TypeError("Blob source must yield Uint8Array");
        for (let cursor = 0; cursor < input.length;) {
          while (state.credit === 0n && !state.error) {
            await new Promise((resolve, reject) => {
              const timer = setTimeout(() => { state.wake = null; reject(new Error("Blob credit timed out")); }, this.options.timeout);
              state.wake = () => { clearTimeout(timer); state.wake = null; resolve(); };
            });
          }
          if (state.error) throw state.error;
          const length = Math.min(12288, input.length - cursor, Number(state.credit));
          const chunk = input.subarray(cursor, cursor + length);
          if (offset + BigInt(length) > size) throw new Error("Blob source exceeds declared size");
          while (this.wire.reliable.bufferedAmount > 262144) {
            if (this.closed || state.error) throw state.error ?? new Error("Client closed");
            await sleep(10);
          }
          this.send("blob_chunk", { transfer, offset, value: chunk });
          hash.update(chunk); offset += BigInt(length); state.credit -= BigInt(length); cursor += length;
        }
      }
      if (offset !== size) throw new Error("Blob source shorter than declared size");
      this.send("blob_end", { transfer, sha256: hex(hash.digest()) });
      let timer;
      try {
        return await Promise.race([done, new Promise((_, reject) => {
          timer = setTimeout(() => reject(new Error("Blob completion timed out")), this.options.timeout);
        })]);
      } finally { clearTimeout(timer); }
    } catch (error) { this.cancelBlob(transfer, error); throw error; }
    finally { signal?.removeEventListener("abort", abort); }
  }
  close(error = new Error("Client closed")) {
    if (this.closed) return;
    this.closed = true;
    this.fragments.close();
    this.routeManager.close();
    this.peerDeliveries.close();
    this.wire.close();
    for (const pending of this.pending.values()) {
      clearTimeout(pending.timer); pending.signal?.removeEventListener("abort", pending.abort); pending.reject(error);
    }
    this.pending.clear();
    for (const id of this.transfers.keys()) this.failBlob(id, error);
    this.emit("close", {});
  }
}
