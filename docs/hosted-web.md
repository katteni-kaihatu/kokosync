# Hosted browser prototype

The default remains local development. An explicit hosted mode exposes **browser
WebRTC only**, behind an authenticated HTTPS reverse proxy:

```sh
./kokosync --root /opt/kokosync \
  --http 127.0.0.1:8080 --quic 127.0.0.1:8081 \
  --public-ip 45.77.130.146 --rtc-udp-min 40000 --rtc-udp-max 40100
```

Run under an unprivileged service account. Allow public HTTPS and UDP 40000–40100
in the host/provider firewall. Pion advertises the configured public IPv4 address
on its ICE host candidates and binds only within that UDP range. No STUN/TURN
service is configured. Networks blocking UDP may use the demo's explicitly
selected WebSocket fallback. QUIC 8081 and LiteNetLib 8082 remain loopback-only;
this mode does not add remote native SDK authentication or encryption.

The TLS reverse proxy must:

- Require access authentication (for example HTTP Basic over HTTPS) for the
  entire site, including `/offer`, `/ws`, assets, and `/health`.
- Preserve the public `Host`, set `X-Forwarded-Proto` to `https`, and connect to
  `127.0.0.1:8080`. Forward WebSocket Upgrade/Connection headers for `/ws`.
- Deny `/bridge` from the public edge. It is an internal sidecar endpoint.
  For nginx, use `location = /bridge { return 403; }`; ensure an existing broader
  proxy rule cannot override it. Do not forward client-supplied bridge secrets.
- Redirect public HTTP to HTTPS. Store access credentials and TLS keys outside
  the repository. Keep the internal bridge secret private when overriding it.

The application checks the immediate HTTP peer is loopback and requires the
browser Origin to match the request Host and effective scheme. It honors
`X-Forwarded-Proto: https` only in hosted mode after the loopback check. As defense
in depth it rejects proxied `/bridge` requests carrying this header.

`GET /health` reports `mode: "hosted-web"` and
`nativeTransports: "loopback-only"`. WebRTC traffic is protected by DTLS; the edge
access password controls signaling access. This is still a memory-only shared
prototype: there are no per-user room permissions, durable storage, automatic
recovery, production membership tickets, or full media codec support.
Restarting the service clears rooms, operations, and checkpoints.

Both the relay and LiteNetLib sidecar read `KOKOSYNC_BRIDGE_SECRET`. Set the same
nonempty secret in their protected systemd `EnvironmentFile`. The relay's
`--bridge-secret` option overrides the environment when explicitly supplied;
without either, the existing local-development default remains unchanged.
