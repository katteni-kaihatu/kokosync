# Optional direct latest/live routes

Reliable messages, state, joins and checkpoints always use the core. Only the
existing small `latest` / `live` envelopes may use a direct route. The application
API and publisher sequence numbers stay the same. Browser WebRTC is the first
direct adapter; Go QUIC and C# LiteNetLib/WebSocket advertise no direct adapter.

## Membership and signaling

`join` may include `routing:{directProtocol:"webrtc"|"none",maxDirectPeers:uint}`.
The maximum is 0..8. Browser capability advertisement uses 8 independently of
local policy (normally at most 2 active connections), allowing runtime disable
and re-enable without a rejoin. Missing routing means `none`, zero.
The core's `joined` includes `peers:[{peer,publisherEpoch,directProtocol,
maxDirectPeers}]` for other members and `capabilities.routeSignaling:true`.
The existing `joined.epoch` is the room epoch and `joined.publisherEpoch` is the
local connection generation. Member changes are authoritative core envelopes:

* `peer_joined {epoch,peer,publisherEpoch,directProtocol,maxDirectPeers}`
* `peer_left {epoch,peer,publisherEpoch}`

They are reliable and arrive after the recipient's complete joined barrier.
Stale generation updates and direct callbacks must not replace current members.

`route_signal {id?,to,targetEpoch,signal}` routes an offer, answer, ICE candidate
or close notification to one current room member. `signal.type` is
`offer|answer|candidate|close`; the whole encoded request is at most 16384 bytes.
The core assigns `peer`, `publisherEpoch`, and `epoch` on the forwarded envelope,
retains `to`, `targetEpoch`, `signal`, and never trusts sender-supplied identity.
Every signal includes `connection`, a unique offerer-generated attempt ID of at
most 64 bytes. Answers, candidates and close notifications echo that ID; SDKs
ignore messages for replaced attempts even within the same membership generation.
Offers/answers carry `sdp`; candidates carry an ICE candidate JSON object in
`candidate` (candidate string at most 4096 bytes, mid and username fragment at
most 256 bytes, optional line index 0..65535).
Both peers must advertise WebRTC. An unknown target or stale target generation
is rejected. A peer may relay at most 60 signals per minute. An optional request
id receives the ordinary ack/error. Membership/signaling use a separate reliable
lane which remains behind initial join but independent of bulk message transfers.

This binds signaling to membership of this prototype's room; it does not add
application identity authentication to the prototype.

## Directed relay and duplicate suppression

Existing latest/live requests may include either `to:[peer,...]` or
`exclude:[peer,...]`, never both. Arrays contain unique valid room member IDs,
at most 32. Unknown `to` recipients are rejected. Well-formed departed/unknown
`exclude` names are ignored so a membership race cannot suppress healthy peers'
relay copy. Absent selectors preserve broadcast including the sender. The relay
assigns the publisher identity, strips the selector, and applies normal size,
sequence and live TTL checks. Invalid selectors reject the request before
advancing its sequence.

The browser sends one direct copy to each healthy chosen peer, then one relay
copy excluding successful direct recipients. Failed/direct-ineligible recipients
remain on the relay. When exclusion metadata cannot fit the small request limit,
broadcast through relay is allowed: the common publisherEpoch+kind+key+seq
deduplicator drops redundant copies. Direct receivers validate size (1000-byte
wire maximum), type, active room/member generation, uint64 sequence and Value
before calling the same receive path. Peer identity is taken from the active
signaled connection. Direct frames cannot inject state/message/control events.

## SDK route policy and diagnostics

Browser options use `routing:{enabled?:boolean,maxPeers?:number,peers?:string[],
iceServers?:RTCIceServer[]}`; defaults are enabled, at most two peers and browser
WebRTC configuration with no external ICE servers. An explicit peers list limits
candidates; otherwise choose from current compatible members. Connection setup,
heartbeats, retry cooldown and transport buffering are bounded. A connection
uses an unordered data channel with zero retransmissions. Deterministic offerer
selection prevents simultaneous-offer glare.
Direct-only `{v:1,type:"route_ping"|"route_pong",nonce:string}` heartbeats never
reach application callbacks. Nonces are matched to the active connection.

`routes()` returns entries `{peer,publisherEpoch,route:"direct"|"relay",
reason,rttMs:number|null,sentDirect,receivedDirect,sentRelay,receivedRelay}`.
`on("route",entry)` reports changes in JS; Go exposes `Routes()` and route
notifications through Messages; C# exposes Routes and route events via Events.
Native peers report relay with `local-adapter-unavailable`; browser peers report
`peer-adapter-unavailable`, `disabled`, `not-selected`, `connecting`,
`connection-failed`, `disconnected`, `congested`, or `heartbeat-timeout` as
applicable. `rttMs` is direct heartbeat RTT, null where unmeasured. Counts describe
transport submission/reception, not guaranteed unreliable delivery.

JS `setRouting({enabled?,maxPeers?,peers?})` changes local route policy. Disabling,
removing a member, reconnecting, closing or changing member epoch closes obsolete
connections and cancels callbacks/timers. Direct failure returns that peer to
relay without changing latest/live application calls. There is no required full
mesh, no direct native adapter, and no guaranteed NAT traversal without suitable
ICE infrastructure.

Here `direct` means a peer WebRTC connection that bypasses the KokoSync core.
If the application configures TURN in `iceServers`, ICE may select that relay;
the route label does not guarantee a direct UDP network path. Selection is based
on availability and local policy, not a measured direct-versus-core RTT comparison
or a guarantee of the fastest route.
