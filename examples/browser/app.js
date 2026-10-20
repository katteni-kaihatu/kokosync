import { KokosyncClient } from "/sdk/index.js";
import { sha256 } from "@noble/hashes/sha2.js";

const $ = id => document.getElementById(id);
const canvas = $("canvas"), ctx = canvas.getContext("2d");
const params = new URLSearchParams(location.search);
$("room").value = params.get("room") || "little-canvas";
$("peer").value = `guest-${crypto.randomUUID().slice(0, 4)}`;
let client, follower, points = [], color = "#2558b5", cursorSeq = 0n;
let drawing = false, lastPoint = 0, lastCursor = 0, statsTimer, routeTimer, checkpointBusy = false, probeSeq = 0n;
let deliveryController, deliveredCount = 0;
const peerMessages = [];
const cursors = new Map(), downloads = new Set();
const stringify = value => JSON.stringify(value, (_, v) => typeof v === "bigint" ? v.toString() : v, 2);
const notice = (text, error = false) => { $("notice").textContent = text; $("notice").classList.toggle("error", error); };
const report = error => notice(error.message ?? String(error), true);
const hash = bytes => Array.from(sha256(bytes), b => b.toString(16).padStart(2, "0")).join("");

function renderRoutes() {
  const body = $("routes");
  body.replaceChildren();
  const routes = client && !client.closed ? client.routes() : [];
  const currentTarget = $("delivery-to").value;
  const desiredTargets = ["*", ...routes.map(route => route.peer)];
  if (Array.from($("delivery-to").options, option => option.value).join("\0") !== desiredTargets.join("\0")) {
    $("delivery-to").replaceChildren(...desiredTargets.map(value => {
      const option = document.createElement("option"); option.value = value;
      option.textContent = value === "*" ? "All other peers" : value; return option;
    }));
    $("delivery-to").value = desiredTargets.includes(currentTarget) ? currentTarget : "*";
  }
  if (client && !client.closed && client.deliveryStats) $("peer-statistics").textContent = stringify(client.deliveryStats());
  if (!routes.length) {
    const row = body.insertRow(), cell = row.insertCell();
    cell.colSpan = 5; cell.textContent = client && !client.closed ? "Waiting for another person to join this room." : "Join a room to see peer routes.";
    return;
  }
  for (const route of routes) {
    const row = body.insertRow();
    const fields = [route.peer, `${route.route} / ${route.reason}`, route.rttMs == null ? "Not measured" : `${Math.round(route.rttMs)} ms`,
      `${route.sentDirect} / ${route.receivedDirect}`, `${route.sentRelay} / ${route.receivedRelay}`];
    for (const field of fields) row.insertCell().textContent = field;
  }
}

function render() {
  const ratio = devicePixelRatio || 1;
  const { width, height } = canvas.getBoundingClientRect();
  if (canvas.width !== Math.round(width * ratio) || canvas.height !== Math.round(height * ratio)) {
    canvas.width = Math.round(width * ratio); canvas.height = Math.round(height * ratio);
  }
  ctx.setTransform(ratio, 0, 0, ratio, 0, 0); ctx.clearRect(0, 0, width, height);
  for (const point of points) {
    ctx.fillStyle = point.color; ctx.beginPath();
    ctx.arc(point.x * width, point.y * height, 5, 0, Math.PI * 2); ctx.fill();
  }
  for (const [peer, cursor] of cursors) {
    if (performance.now() - cursor.at > 1800) { cursors.delete(peer); continue; }
    const x = cursor.x * width, y = cursor.y * height;
    ctx.strokeStyle = cursor.color; ctx.lineWidth = 1.5;
    ctx.beginPath(); ctx.arc(x, y, 11, 0, Math.PI * 2); ctx.stroke();
    ctx.fillStyle = cursor.color; ctx.font = "11px Segoe UI"; ctx.fillText(peer, x + 15, y + 4);
  }
  $("empty").hidden = points.length > 0;
  $("marks").textContent = `${points.length} mark${points.length === 1 ? "" : "s"}`;
  $("sequence").textContent = follower?.seq.toString() ?? "0";
}
new ResizeObserver(render).observe(canvas);
setInterval(render, 300);

function validPoint(p) {
  return p && Number.isFinite(p.x) && Number.isFinite(p.y) && p.x >= 0 && p.x <= 1 && p.y >= 0 && p.y <= 1 &&
    typeof p.color === "string" && /^#[0-9a-f]{6}$/i.test(p.color);
}
async function saveCheckpoint() {
  if (!follower || checkpointBusy) return;
  checkpointBusy = true;
  try { await follower.checkpoint(); notice(`Checkpoint saved. New arrivals can continue from operation ${follower.seq}.`); }
  catch (error) { report(error); }
  finally { checkpointBusy = false; }
}
function disconnected() {
  clearInterval(statsTimer);
  clearInterval(routeTimer);
  deliveryController?.abort(); deliveryController = null;
  follower?.stop(); follower = null;
  $("status").textContent = "Disconnected";
  $("status").parentElement.classList.remove("connected");
  $("join").disabled = false; $("join").textContent = "Join canvas ↗";
  for (const id of ["clear", "checkpoint", "send-file", "send-large", "send-probe", "send-peer", "cancel-peer"]) $(id).disabled = true;
  renderRoutes();
}
$("join-form").addEventListener("submit", async event => {
  event.preventDefault();
  client?.close(); disconnected();
  $("join").disabled = true; $("status").textContent = "Connecting…";
  notice("Opening the connection and catching up with this room…");
  try {
    client = await KokosyncClient.connect({ room: $("room").value.trim(), peer: $("peer").value.trim(), transport: $("transport").value,
      routing: { enabled: !$("relay-only").checked, maxPeers: 2 } });
    client.on("error", report); client.on("close", disconnected);
    client.on("route", renderRoutes);
    client.on("peer_message", frame => {
      const value = frame.value;
      const bytes = value?.bytes;
      const verified = bytes instanceof Uint8Array ? bytes.length === value.size && hash(bytes) === value.sha256 : true;
      deliveredCount++;
      peerMessages.unshift({ from: frame.peer, topic: frame.topic, seq: frame.seq, messageId: frame.messageId,
        value: bytes instanceof Uint8Array ? { bytes: bytes.length, sha256: value.sha256, verified } : value });
      peerMessages.length = Math.min(peerMessages.length, 8);
      $("delivery-count").textContent = `${deliveredCount} RECEIVED`;
      $("peer-messages").textContent = stringify(peerMessages);
      if (!verified) report(new Error("Peer payload hash mismatch"));
    });
    client.on("latest", frame => {
      if (frame.key === "demo.probe") $("latest-probe").textContent = `Latest: ${frame.peer} / sequence ${frame.seq} / ${frame.value?.text ?? ""}`;
      if (frame.key === "cursor" && frame.peer !== client.options.peer && validPoint(frame.value))
        cursors.set(frame.peer, { ...frame.value, at: performance.now() });
      render();
    });
    client.on("live", frame => {
      if (frame.stream === "demo.probe") $("live-probe").textContent = `Live: ${frame.peer} / sequence ${frame.seq} / ${new TextDecoder().decode(frame.value)}`;
    });
    client.on("blob_open", async offer => {
      if (offer.size > 5n * 1024n * 1024n) { client.cancelBlob(offer.transfer); notice("Incoming file rejected: demo limit is 5 MiB.", true); return; }
      const chunks = [];
      try {
        await client.acceptBlob(offer, { write: bytes => chunks.push(bytes), abort: () => { chunks.length = 0; } });
        const url = URL.createObjectURL(new Blob(chunks)); downloads.add(url);
        const link = document.createElement("a"); link.href = url; link.download = `kokosync-${offer.transfer}.bin`;
        link.textContent = `Download verified file from ${offer.peer} (${offer.size} bytes)`;
        $("downloads").append(link); notice(`File from ${offer.peer} verified successfully.`);
      } catch (error) { report(error); }
    });
    follower = await client.follow({
      restore: value => { points = Array.isArray(value?.points) ? value.points.filter(validPoint).slice(-120) : []; },
      apply: frame => {
        if (frame.topic === "canvas.point" && validPoint(frame.value)) points = [...points, frame.value].slice(-120);
        if (frame.topic === "canvas.clear") points = [];
        let preview = frame.value;
        if (frame.topic === "demo.large") {
          const value = frame.value;
          const valid = value?.bytes instanceof Uint8Array && value.bytes.length === value.size && hash(value.bytes) === value.sha256;
          $("large-result").textContent = valid
            ? `Verified ${value.size.toLocaleString()} bytes from ${frame.peer} · operation ${frame.seq} · SHA-256 ${value.sha256}`
            : `Large message from ${frame.peer} failed verification.`;
          if (!valid) throw new Error("Large message failed byte count or SHA-256 verification");
          preview = { size: value.size, sha256: value.sha256, verified: true };
        }
        $("operation").textContent = stringify({ topic: frame.topic, peer: frame.peer, seq: frame.seq, value: preview });
        // The microtask runs after the follower advances its contiguous sequence.
        queueMicrotask(() => { render(); if (frame.seq % 40n === 0n) void saveCheckpoint(); });
      },
      capture: () => ({ points }),
    });
    $("status").textContent = `In ${client.joined.room}`;
    $("status").parentElement.classList.add("connected");
    $("join").disabled = false; $("join").textContent = "Rejoin canvas ↗";
    for (const id of ["clear", "checkpoint", "send-file", "send-probe"]) $(id).disabled = false;
    $("send-large").disabled = !client.capabilities.fragmentation;
    $("send-peer").disabled = typeof client.sendTo !== "function" || !client.capabilities.peerDelivery;
    $("delivery-result").textContent = $("send-peer").disabled ? "This server does not support reliable peer delivery." : "Ready. Select peers and send an operation or a snapshot.";
    $("large-result").textContent = client.capabilities.fragmentation
      ? `Ready · maximum envelope ${(Number(client.capabilities.fragmentation.logicalBytes) / 1048576).toFixed(1)} MiB.`
      : "This server does not support large messages.";
    const datagram = client.capabilities.datagram === true;
    $("capabilities").textContent = `${client.transport === "webrtc" ? "WebRTC" : "WebSocket"} relay · optional peer routes`;
    $("transport-detail").textContent = `${datagram ? "The relay carries cursors on an unordered, unreliable channel." : "The relay carries cursors on the reliable stream."} Peer routes below may carry updates directly. Bounded latency is not guaranteed.`;
    const url = new URL(location.href); url.searchParams.set("room", $("room").value); $("second-window").href = url;
    $("empty").querySelector("small").textContent = "Click or drag anywhere on the canvas.";
    notice("Connected. Open a second canvas with a different name to draw together.");
    render();
    renderRoutes();
    routeTimer = setInterval(renderRoutes, 1000);
    statsTimer = setInterval(async () => {
      try { const stats = await client.stats(); $("traffic").textContent = `${Math.round(stats.sentBytes / 1024)} KB SENT · ${stats.dropped} DROPPED`; }
      catch {}
    }, 3000);
  } catch (error) { client?.close(); disconnected(); report(error); }
});
$("delivery-size").addEventListener("change", () => {
  $("delivery-topic").value = Number($("delivery-size").value) ? "demo.peer.snapshot" : "demo.peer.operation";
});
$("cancel-peer").addEventListener("click", () => deliveryController?.abort());
$("peer-delivery-form").addEventListener("submit", async event => {
  event.preventDefault();
  if (!client || client.closed || deliveryController) return;
  const sendingClient = client;
  const target = $("delivery-to").value;
  const targets = target === "*" ? client.routes().map(route => route.peer) : [target];
  if (!targets.length) { $("delivery-result").textContent = "No other peers in this room."; return; }
  const size = Number($("delivery-size").value), topic = $("delivery-topic").value.trim();
  const bytes = size ? Uint8Array.from({ length: size }, (_, i) => i % 251) : null;
  const value = bytes ? { bytes, size, sha256: hash(bytes) } : { eventId: crypto.randomUUID(), operation: "demo", text: "Hello from " + client.options.peer };
  const controller = deliveryController = new AbortController();
  $("send-peer").disabled = true; $("cancel-peer").disabled = false;
  $("delivery-result").textContent = `Sending ${size ? size.toLocaleString() + " bytes" : "operation"} to ${targets.length} peer(s)…`;
  try {
    const results = await Promise.allSettled(targets.map(peer => sendingClient.sendTo(peer, topic, value, { signal: controller.signal })));
    if (client === sendingClient) $("delivery-result").textContent = results.map((result, i) => result.status === "fulfilled"
      ? `${targets[i]}: received by SDK (${result.value.messageId ?? result.value.id})`
      : `${targets[i]}: ${result.reason.code ?? "Failed"} / ${result.reason.outcome ?? "unknown"} / ${result.reason.message}`).join(" · ");
  } finally {
    if (deliveryController === controller) {
      deliveryController = null;
      $("send-peer").disabled = !client || client.closed; $("cancel-peer").disabled = true;
      renderRoutes();
    }
  }
});
$("relay-only").addEventListener("change", () => {
  if (client && !client.closed) { client.setRouting({ enabled: !$("relay-only").checked }); renderRoutes(); }
});
$("send-probe").addEventListener("click", () => {
  if (!client || client.closed) return;
  try {
    const seq = ++probeSeq, text = `probe-${seq}`;
    client.latest("demo.probe", { text }, seq);
    client.live("demo.probe", new TextEncoder().encode(text), seq, 1000, "text/plain");
    renderRoutes();
    notice(`Latest and live probe ${seq} sent. The other window shows receipt and route counters.`);
  } catch (error) { report(error); }
});
function position(event) {
  const rect = canvas.getBoundingClientRect();
  return { x: Math.max(0, Math.min(1, (event.clientX - rect.left) / rect.width)), y: Math.max(0, Math.min(1, (event.clientY - rect.top) / rect.height)), color };
}
function mark(point) { if (client && follower) client.message("canvas.point", point).catch(report); }
canvas.addEventListener("pointerdown", event => { drawing = true; canvas.setPointerCapture(event.pointerId); mark(position(event)); lastPoint = performance.now(); });
canvas.addEventListener("pointerup", () => { drawing = false; });
canvas.addEventListener("pointercancel", () => { drawing = false; });
canvas.addEventListener("pointermove", event => {
  if (!client || !follower) return;
  const now = performance.now(), point = position(event);
  if (drawing && now - lastPoint > 45) { mark(point); lastPoint = now; }
  if (now - lastCursor > 65) {
    try { client.latest("cursor", point, ++cursorSeq); } catch (error) { report(error); }
    lastCursor = now;
  }
});
canvas.addEventListener("keydown", event => { if (event.key === "Enter") { event.preventDefault(); mark({ x: .5, y: .5, color }); } });
document.querySelectorAll(".swatch").forEach(button => button.addEventListener("click", () => {
  color = button.dataset.color;
  document.querySelectorAll(".swatch").forEach(swatch => { const selected = swatch === button; swatch.classList.toggle("selected", selected); swatch.setAttribute("aria-pressed", String(selected)); });
}));
$("clear").addEventListener("click", () => client?.message("canvas.clear", null).catch(report));
$("checkpoint").addEventListener("click", saveCheckpoint);
$("file-form").addEventListener("submit", async event => {
  event.preventDefault();
  const file = $("file").files[0];
  if (!client || !file) return;
  if (file.size > 5 * 1024 * 1024) { notice("Choose a file up to 5 MiB for this demo.", true); return; }
  $("send-file").disabled = true; notice("Sending file. Waiting for receiver verification…");
  try { await client.sendBlob($("recipient").value.trim(), file); notice("File delivered and verified by the receiver."); }
  catch (error) { report(error); }
  finally { $("send-file").disabled = !follower; }
});
$("large-form").addEventListener("submit", async event => {
  event.preventDefault();
  if (!client || !follower) return;
  const size = Number($("large-size").value);
  const bytes = Uint8Array.from({ length: size }, (_, i) => i % 251);
  const value = { bytes, size, sha256: hash(bytes) };
  $("send-large").disabled = true;
  $("large-result").textContent = `Sending ${size.toLocaleString()} bytes…`;
  const started = performance.now();
  try {
    await client.message("demo.large", value);
    notice(`Large message accepted in ${Math.round(performance.now() - started)} ms. Receiver verification appears in the large-message panel.`);
  } catch (error) { report(error); $("large-result").textContent = `Send failed: ${error.message}`; }
  finally { $("send-large").disabled = !follower || !client?.capabilities.fragmentation; }
});
window.addEventListener("beforeunload", () => { client?.close(); for (const url of downloads) URL.revokeObjectURL(url); });
render();
