# SDK設計: JS・C#・Goで同じ同期契約を使う

状態: v0草案 / 2026-10-21。以下のAPIは設計例であり、配布済みパッケージではない。
実装済みSDKの入口は[README](../README.md)、今回の実装範囲は[実装状況](implementation-status.md)を参照。本文のAPI例と実行版の名前には差がある。

## 1. 接続方式を選ばなくても、できることと接続状態が分かる

SDKはendpoint情報、実行環境、必要な能力からadapterを選ぶ。アプリは通常`join`、`state`、`latest`等だけを使う。開発やベンチマークでは通信方式を固定できるようにする。

「透過的」はAPIの意味が揃うことを指す。codec、OS、ブラウザの制限、通信障害まで消すことは約束しない。必要な機能がなければ失敗し、縮退を認めた場合は実際の能力を返す。

`require`に指定するSDK機能と、下位Transportの能力は分ける。公開機能は`state`、`latest`、`events`、`blobs`、`liveFrames`とし、内部ではblobsにbulkチャネルを割り当てる。

| SDK機能 | 必要な経路能力 |
| --- | --- |
| state / events | 認証済みのreliableOrdered。stateはsnapshot/resume対応も必要 |
| latest | datagramと送信待ちを制限できるboundedLatest |
| blobs | reliable、分割、credit、逐次I/O |
| liveFrames | datagram、期限・再分割、対象宛先とのcodec合意 |

WebSocket等でlatestを模擬する場合は、アプリが`allowDegradedLatest`を選ぶ。低遅延の保証範囲が変わることを通知し、必須機能の不足を黙って代替しない。

| 共通機能 | JS / TypeScript | C# | Go |
| --- | --- | --- | --- |
| join・確定操作 | Promise | Task / ValueTask | context + 戻り値・error |
| 状態変更の購読 | AsyncIterable、必要ならcallback wrapper | IAsyncEnumerable | 購読handle + channel |
| 最新値の投入 | 同期的なtryPublish | TryPublish | TryPublish |
| 大容量の送受信 | ReadableStream / AsyncIterable | Stream / IAsyncEnumerable | io.Reader / io.Writer |
| キャンセル | AbortSignal | CancellationToken | context.Context |
| 解放 | close / dispose | IAsyncDisposable | Close |

同じ操作のエラーコード、ACKの意味、再接続時の処理は言語で変えない。各SDKは同じwire fixtureと障害シナリオを実行する。ホスト言語のクラスをそのまま通信契約にせず、MessagePackの共通Valueへ写す。

## 2. 小さな共有オブジェクトから使い始める

```ts
// TypeScript: 仮API。joinの完了時にはstateの初期同期が終わっている。
const client = createClient({ endpoint, getToken });
const room = await client.join("lounge", {
  require: ["state", "latest", "blobs", "liveFrames"],
  signal,
});

await room.state.set("note:1", {
  text: "こんにちは",
  position: [1, 0, 2],
}); // 永続モードなら永続化の確定後に完了

for await (const change of room.state.watch({ prefix: "note:", signal })) {
  // 先頭は現在のキャッシュのreset、その後はset/delete。
  render(change);
}
```

`watch`開始時のresetと、以後の変更に間ができないよう、SDKの同じ状態処理loopで購読を登録する。再snapshotでは新しいresetを発行する。アプリはresetを受けて表示を再構築できる。

購読中のkeyならawait set後のgetは、その操作の確定版かそれより新しい値を返す。書込権限だけがあり購読していないkeyでは、setの戻り値から確定値と版を読む。

`state.get`はローカルcacheから値・version・freshnessを返す。返した値は読み取り専用として扱い、変更は`set`等を通す。動的な購読範囲の拡縮は新しい購読世代で同期し、権限が失われた範囲をcacheから削除する。

```csharp
// C#: 仮API。Dictionaryは文字列キーのMessagePack mapへ写す。
await using var room = await client.JoinAsync("lounge", options, cancellationToken);
await room.State.SetAsync("note:1", new Dictionary<string, object?> {
    ["text"] = "こんにちは",
    ["position"] = new[] { 1.0, 0.0, 2.0 }
}, cancellationToken);

await foreach (var change in room.State.WatchAsync("note:", cancellationToken)) {
    Render(change);
}
```

```go
// Go: 仮API。購読channelは内部上限を持ち、Closeで解放する。
room, err := client.Join(ctx, "lounge", options)
if err != nil { return err }
defer room.Close()

_, err = room.State.Set(ctx, "note:1", map[string]any{
    "text": "こんにちは", "position": []float64{1, 0, 2},
})
if err != nil { return err }

sub := room.State.Watch(ctx, "note:")
defer sub.Close()
for change := range sub.Changes() { render(change) }
return sub.Err()
```

書込中のキャンセルは、サーバーで確定済みの操作を巻き戻さない。送信前ならCancelled、送信後で結果不明ならopId付きOutcomeUnknownを返す。SDKの操作履歴で後から結果を問い合わせられるようにする。

## 3. データの性質に応じてAPIを選ぶ

### state: 正しい現在値を共有する

`get`、`set`、`delete`、`compareAndSet`、`watch`を提供する。`set`の戻り値にはopIdと確定revisionを含める。自動的な深いmergeやCRDTはv0に含めない。

```ts
const current = room.state.get("board");
await room.state.compareAndSet("board", current.version, nextBoard);
```

楽観的な見た目はアプリが持つか、SDKの明示的なpending overlayを使う。確定cacheに未確定の値を混ぜない。値の取り消しや競合がUIへ分かることを優先する。

### latest: 最新値を送り、古い未送信値を置き換える

```ts
const outcome = room.latest.tryPublish("pose", { position, rotation });
// queued / replaced / notReady / tooLarge / backpressure
```

キーは送信者ごとに独立する。購読値にはpeerId、publisherEpoch、sequence、到着時刻、経路を添える。`queued`はローカルに受け付けた意味で、相手への到着確認ではない。切断中に最新値を無期限に保存しない。

### events: 今参加している相手へ出来事を通知する

`publish(topic, value, { ttlMs })`と`watch(topic)`を提供する。publishの完了は中継側の受付までで、全受信者の処理完了を意味しない。対象peerを指定でき、権限と購読を通った相手へだけ送る。受信側で同じeventIdを重複実行しない。

### blobs: 大容量を逐次処理し、完了を検証する

```ts
const transfer = room.blobs.send(source, {
  to: peerId,
  size,
  signal,
});
transfer.onProgress(({ sentBytes, acknowledgedBytes, queuedBytes }) => {});
await transfer.done;
```

sourceから読む速度は相手のcreditに合わせる。`done`は相手のsinkとhash検証の完了まで待つ。seek可能なsourceか再読込callbackがあれば、同じ内容の途中から再開できる。非seekableなsourceで再読込callbackもspoolもない場合は、切断時に失敗する。sourceの能力はtransfer.info.resumableで確認する。

送信元終了後もダウンロード可能にする場合は、永続blob storeを明示的に指定する。任意のファイルをstateの履歴に埋め込まない。

### live: 期限付きのencoded frameを運ぶ

```ts
const live = await room.live.open({
  codec: negotiatedCodec,
  timebase: { numerator: 1, denominator: 90000 },
  maxLatencyMs: 150,
});
live.onFeedback(({ suggestedBitrateBps, requestKeyframe }) => {
  updateEncoder(suggestedBitrateBps, requestKeyframe);
});
live.tryWriteFrame(encodedBytes, { pts, keyframe, dependsOn });
```

`tryWriteFrame`はqueued、dropped、tooLarge等を返す。最大frameや残り予算を超えたら待ち行列へ無制限に積まない。上の150msはアプリの期限設定例であり、ネットワーク越しの保証値ではない。

codec交渉には各端末が利用可能なcodec、profile、設定を出す。利用できる共通codecがなければ`UnsupportedCapability`。動画の撮影・encode・decode・表示は、別のmedia helperかアプリが担当する。

## 4. 通信の状態は層と経路を付けて返す

`client.stats()` / `room.stats()`は取得時点のsnapshot、`watchStats({ intervalMs })`は定期購読とする。通常は1秒間隔、診断時は100ms以上の間隔を初期案とし、観測負荷も測る。

```ts
const stats = room.stats();
for (const route of stats.routes) {
  inspect({
    path: route.path,                  // direct / relay
    transport: route.transport,        // webrtc / litenetlib / quic / websocket
    rttMs: route.rtt?.value ?? null,
    queueBytes: route.sendQueueBytes,
    oldestQueueAgeMs: route.oldestQueueAgeMs,
    goodputBps: route.applicationGoodputBps,
    loss: route.transportLoss?.value ?? null,
  });
}
```

数値は`{ value, unit, source, scope, sampledAt, intervalMs }`として定義する。非対応・未取得は`null`と理由で返し、0にしない。累積counterと区間rateを区別する。

| 観測項目 | 定義・注意 |
| --- | --- |
| connection / sync state | 物理接続と初期同期を別に返す。lastError、reconnectCount、lastReadyAtを含む |
| transport RTT | SDKが使う一つの接続区間。WebRTCでは候補pair等、取得元を明示 |
| peer echo RTT | SDKのping/echoで測ったpeer往復。中継なら両区間と転送処理を含む |
| state commit latency | 操作送信から確定応答まで。永続化待ちも含む |
| sync progress | snapshot受信bytes、適用件数、未適用commit数、現在cursor |
| transport bytes / app goodput | 暗号・再送込みの通信量と、アプリへ渡った有効bytesを別計上 |
| send / receive queue | SDK・gateway・adapterそれぞれのbytesと最古の滞留時間 |
| loss / retransmit | 下位層で取得できる場合だけ提供。アプリsequenceの欠番は別counter |
| latest replacement | 送信前の置換数。ネットワークのpacket lossとは異なる |
| live quality | incomplete / expired / dependency drop、keyframe待ち、jitter、現在bitrate |
| bulk progress | 読取・送信・受信ACK・sink完了bytes、credit待ち時間 |
| path / capabilities | 直接・TURN・自前relayの別、adapter、利用可能機能、縮退理由 |

P2Pから中継へ変わったときはrouteIdを替え、統計を混ぜない。受信までの一方向時間は時計ずれがあるため、推定値と誤差を表示する。transportのRTTをそのまま相手アプリまでの遅延として返さない。

ブラウザではWebRTCの統計APIから取れる範囲に制約がある。nativeのsocket統計と全項目が揃うと仮定せず、[WebRTC Stats仕様](https://www.w3.org/TR/webrtc-stats/)と各実装の対応を確認する。

## 5. 呼出側が遅くても内部メモリを増やし続けない

購読callbackから接続loopへ重い仕事を戻さない。各SDKは内部queueとアプリへ渡すqueueを分け、receive creditに反映する。Unityではmain threadへ渡すdispatcherを提供し、受信I/Oと描画を分離する。Goではgoroutineとchannelの終了をCloseまで管理する。

stateの購読者が遅い場合は、上限内で差分を渡し、追いつかなくなったら`resetRequired`を通知して最新snapshotへまとめる。履歴をすべて処理する用途には別のdurable log APIが必要になる。latestはkey単位で最新にまとめ、liveは期限切れを破棄する。

バイナリの所有権を定める。通常APIは呼出後の変更に影響されないようcopyする。上級者向けのborrowed / pooled buffer APIは、解放期限・使用終了通知を明示して別入口にする。「透過的」を理由に寿命不明のbufferを渡さない。

## 6. パッケージの境界と実装候補

予定配置は以下。現時点でこれらのソースは存在しない。

```text
protocol/                 wire仕様、kind registry、共通fixture
sdk/js/                   JS / TypeScriptのAPIとbrowser adapter
sdk/csharp/               C#のAPI、Unity dispatcher、LiteNetLib adapter
sdk/go/                   GoのAPI、QUIC adapter
server/core/              membership、認可、state、snapshot、resume
server/gateway/           接続終端、配送、fairness、観測
server/litenetlib/        .NET終端と内部接続
conformance/              言語・transport間の適合試験
bench/                    遅延、飽和、動画、障害の計測
examples/                 三種類のクライアントによる同一セッション
```

MessagePack実装の候補はJSの[@msgpack/msgpack](https://github.com/msgpack/msgpack-javascript)、C#の[MessagePack-CSharp](https://github.com/MessagePack-CSharp/MessagePack-CSharp)、Goの[vmihailenco/msgpack](https://github.com/vmihailenco/msgpack)。バージョン、サポート環境、設定はfixtureの一致を確認した後で固定する。C#ではAOT / IL2CPP用の型生成とdynamic Valueの両経路を確認する。

公開パッケージ名、配布レジストリ、semver、サポートOSは未決。APIを安定扱いにする条件は[検証計画](validation.md)に置く。
