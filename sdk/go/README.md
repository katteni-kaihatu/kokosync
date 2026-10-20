# Go QUIC SDK

From the repository root, with the relay running:

```powershell
.\.tools\go\bin\go.exe run ./examples/go --insecure-development --smoke
```

Import `github.com/katteni-kaihatu/kokosync/sdk/go` as `kokosync`.
`Connect(ctx, Options{Address, Room, Peer, InsecureDevelopment:true})` joins a raw messaging room.
Set `State:true` only to enable the optional state snapshot/cache helper.
`Publish`, `Event`, `Latest`, `Live`, `Set`, `Delete`, `Get`, `Checkpoint`, `CatchUp`, `Stats`,
`SendBlob(io.Reader,size)` and `ReceiveBlob(blobOpen,io.Writer)` cover the prototype API.
`Request`/`Send` expose the full envelope contract in `protocol/prototype.md`.

Read `Messages()` continuously; a slow consumer is disconnected when its reliable queue fills.
Messages are immutable to callers. `Get` includes tombstones (`deleted:true`) and revision.
Set success follows cache update; callbacks/consumer handling need not have finished.
The stream preserves raw-message order. QUIC DATAGRAM latest/live are separate and may be lost.
Counters are `uint64`. Application integer values must remain within ±(2^53−1).

CatchUp returns an atomic application checkpoint at R, messages R+1..`seq`, and current cutoff `seq`.
Applications that consume both CatchUp and Messages must apply the returned replay first, then ignore
queued raw messages at or below that cutoff. Checkpoint content is application-owned; the relay
cannot verify that it correctly represents R. Reconnect by calling Connect again; automatic
reconnect, durable resume and retries are not implemented.

Blob completion validates SHA256 after incremental sink writes. Incoming offers remain unaccepted
until ReceiveBlob is called. Context cancellation sends blob_cancel. Transfers cannot resume.
Live is a <=872-byte encoded request carrying small bytes and a relay queue deadline; it does not
negotiate codecs, decode video or implement keyframe dependencies.
