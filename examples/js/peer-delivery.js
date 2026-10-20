// Node relay participant. Browsers can use the same sendTo contract over P2P.
import { KokosyncClient } from "../../sdk/js/index.js";
import { sha256 } from "@noble/hashes/sha2.js";

const [url = "http://127.0.0.1:8080", room = "peer-delivery", peer = "node-peer", target] = process.argv.slice(2);
const hex = bytes => Array.from(sha256(bytes), byte => byte.toString(16).padStart(2, "0")).join("");
const stringify = value => JSON.stringify(value, (_, item) => typeof item === "bigint" ? item.toString() : item);
const client = await KokosyncClient.connect({ url, room, peer, transport: "websocket" });
client.on("error", error => console.error(error.code ?? "Error", error.message));
client.on("peer_message", frame => {
  const bytes = frame.value?.bytes;
  const summary = bytes instanceof Uint8Array
    ? { size: bytes.length, sha256: hex(bytes), verified: hex(bytes) === frame.value.sha256 }
    : frame.value;
  console.log(stringify({ peer: frame.peer, topic: frame.topic, messageId: frame.messageId, seq: frame.seq, value: summary }));
});
console.log(`Joined ${room} as ${peer}. Listening for 30 seconds.`);
try {
  if (target) {
    for (let operation = 1; operation <= 3; operation++)
      console.log(stringify(await client.sendTo(target, "demo.peer.operation", { source: peer, operation })));
    const bytes = Uint8Array.from({ length: 1024 * 1024 }, (_, index) => index % 251);
    console.log(stringify(await client.sendTo(target, "demo.peer.snapshot", { bytes, size: bytes.length, sha256: hex(bytes) })));
  }
  await new Promise(resolve => {
    const timer = setTimeout(resolve, 30000);
    process.once("SIGINT", () => { clearTimeout(timer); resolve(); });
    client.on("close", () => { clearTimeout(timer); resolve(); });
  });
  console.log(stringify(client.deliveryStats()));
} finally {
  client.close();
}
