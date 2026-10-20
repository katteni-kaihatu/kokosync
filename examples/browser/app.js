import { KokosyncClient } from "/sdk/index.js";

const $ = id => document.getElementById(id);
const canvas = $("canvas"), ctx = canvas.getContext("2d");
const params = new URLSearchParams(location.search);
$("room").value = params.get("room") || "little-canvas";
$("peer").value = `guest-${crypto.randomUUID().slice(0, 4)}`;
let client, follower, points = [], color = "#2558b5", cursorSeq = 0n;
let drawing = false, lastPoint = 0, lastCursor = 0, statsTimer, checkpointBusy = false;
const cursors = new Map(), downloads = new Set();
const stringify = value => JSON.stringify(value, (_, v) => typeof v === "bigint" ? v.toString() : v, 2);
const notice = (text, error = false) => { $("notice").textContent = text; $("notice").classList.toggle("error", error); };
const report = error => notice(error.message ?? String(error), true);

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
  follower?.stop(); follower = null;
  $("status").textContent = "Disconnected";
  $("status").parentElement.classList.remove("connected");
  $("join").disabled = false; $("join").textContent = "Join canvas ↗";
  for (const id of ["clear", "checkpoint", "send-file"]) $(id).disabled = true;
}
$("join-form").addEventListener("submit", async event => {
  event.preventDefault();
  client?.close(); disconnected();
  $("join").disabled = true; $("status").textContent = "Connecting…";
  notice("Opening the connection and catching up with this room…");
  try {
    client = await KokosyncClient.connect({ room: $("room").value.trim(), peer: $("peer").value.trim(), transport: $("transport").value });
    client.on("error", report); client.on("close", disconnected);
    client.on("latest", frame => {
      if (frame.key === "cursor" && frame.peer !== client.options.peer && validPoint(frame.value))
        cursors.set(frame.peer, { ...frame.value, at: performance.now() });
      render();
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
        $("operation").textContent = stringify({ topic: frame.topic, peer: frame.peer, seq: frame.seq, value: frame.value });
        // The microtask runs after the follower advances its contiguous sequence.
        queueMicrotask(() => { render(); if (frame.seq % 40n === 0n) void saveCheckpoint(); });
      },
      capture: () => ({ points }),
    });
    $("status").textContent = `In ${client.joined.room}`;
    $("status").parentElement.classList.add("connected");
    $("join").disabled = false; $("join").textContent = "Rejoin canvas ↗";
    for (const id of ["clear", "checkpoint", "send-file"]) $(id).disabled = false;
    const datagram = client.capabilities.datagram === true;
    $("capabilities").textContent = `${client.transport === "webrtc" ? "WebRTC" : "WebSocket"} · ${datagram ? "ordered marks + lossy cursors" : "reliable-only fallback"}`;
    $("transport-detail").textContent = `${datagram ? "Cursors use an unordered, unreliable channel." : "Cursors share the reliable stream in this fallback."} Bounded latency is not guaranteed.`;
    const url = new URL(location.href); url.searchParams.set("room", $("room").value); $("second-window").href = url;
    $("empty").querySelector("small").textContent = "Click or drag anywhere on the canvas.";
    notice("Connected. Open a second canvas with a different name to draw together.");
    render();
    statsTimer = setInterval(async () => {
      try { const stats = await client.stats(); $("traffic").textContent = `${Math.round(stats.sentBytes / 1024)} KB SENT · ${stats.dropped} DROPPED`; }
      catch {}
    }, 3000);
  } catch (error) { client?.close(); disconnected(); report(error); }
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
window.addEventListener("beforeunload", () => { client?.close(); for (const url of downloads) URL.revokeObjectURL(url); });
render();
