# kokosync

ブラウザとネイティブアプリが、同じセッションで任意のデータを交換するための同期基盤。MessagePackを共通形式にし、JS / TypeScript、C#、Goから使うローカル開発用プロトタイプです。

**任意データの順序付き配送が基本機能です。** アプリが初回同期・更新の適用・保存方法を決められます。共有状態の`state`と、初期状態に後続操作を追いつかせるチェックポイント機能は、必要なときだけ使います。

まず「ローカルで試す」を実行してください。設計全体と今回の実装との差は、[実装範囲](docs/implementation-status.md)に記録しています。

## ローカルで試す

PowerShellからリポジトリのルートで実行します。Node.js 22以上とnpmは事前に用意してください。

```powershell
./scripts/bootstrap.ps1
./scripts/start.ps1
```

`bootstrap.ps1`はGo 1.27.3と.NET SDK 10.0.402を`.tools/`へ導入し、依存パッケージを取得します。`start.ps1`はGoの中継サーバーとLiteNetLibの中継プロセスを起動します。

[ブラウザデモ](http://127.0.0.1:8080/)を2つ開き、同じRoom・異なる名前で参加すると、描画とカーソルを共有できます。初回同期はアプリのチェックポイント機能を使っており、`state`への参加は不要です。「Send a large message」では256 KiB〜2 MiBの通常メッセージと受信検証を試せます。デモの描画は直近120点、ファイル転送は5 MiB以下を扱います。

`latest` / `live`は、対応するブラウザ同士で直結を試し、接続できない相手には中継で送ります。直結が切れても同じAPIから送信を続けられます。既定では最大2人を直結候補とし、Go・C#との通信は引き続き中継します。「Peer routes」で相手ごとの経路・理由・直結の往復時間を確認でき、「Use relay only」で中継への切り替えを試せます。詳しくは[経路選択の仕様](protocol/p2p.md)を参照してください。

特定の相手へ受信確認付きで送る場合は`sendTo(peer, topic, value)`を使います。対応ブラウザ間の直結とコア中継で同じメッセージIDを使い、大きい値は自動分割します。既存の`message`のコア連番・履歴とは独立した配送です。デモの「Reliable peer messages」で、操作と1〜2 MiBの値を相手ごとに送り、受信結果を確認できます。[配送契約](protocol/peer-delivery.md)と[Tonarium向け移行手順](docs/peer-delivery-guide.md)を参照してください。

終了するときは次を実行します。

```powershell
./scripts/stop.ps1
```

### C#とGoも同じ部屋へ参加する

サーバーを起動した状態で、別のターミナルからC#の例を実行します。ブラウザのRoomを`mixed`にすると同じ部屋に参加します。

```powershell
.tools/dotnet/dotnet.exe run --project examples/csharp -- --room mixed --insecure-development
```

Goの例は次のコマンドで15秒間、同じ部屋へメッセージと位置を送ります。

```powershell
.tools/go/bin/go.exe run ./examples/go --insecure-development --smoke --seconds 15
```

軽い動作確認は`./scripts/smoke.ps1`で実行します。Goと.NET SDKが既にPATHにある場合は、通常の`go`・`dotnet`コマンドでもSDKや各例を実行できます。

## JSから任意のデータを送る

```js
import { KokosyncClient } from "./sdk/js/index.js";

const room = await KokosyncClient.connect({
  url: "http://127.0.0.1:8080",
  room: "example",
  peer: "alice", // 同じ部屋で重複しない表示用ID
  transport: "websocket", // Node.jsでは明示。ブラウザの既定はwebrtc
});

room.on("message", ({ topic, value, seq }) => {
  console.log(topic, value, seq);
});
await room.message("canvas.stroke", { color: "blue", points: [0, 1, 2, 3] });
room.latest("cursor", { x: 0.5, y: 0.2 }, 1n);
console.log(room.routes()); // 相手ごとの経路と測定値
room.on("route", route => console.log(route.peer, route.route, route.reason));
room.on("peer_message", frame => console.log(frame.peer, frame.topic, frame.value));
// 相手もpeerDelivery対応SDKを使っている場合。成功は相手SDKの受信確認。
// await room.sendTo("bob", "canvas.operation", { eventId: crypto.randomUUID(), points: [0, 1] });
// 利用終了時に room.close()
```

値はnull・真偽値・有限の安全な数値・文字列・バイト列・配列・文字列キーのマップです。プロトコルの連番は64bit整数を使い、JSでは`bigint`として扱います。

### 大きいメッセージと初期状態も自動で分割する

`message`へ大きい値を渡すと、SDKが分割転送し、受信側でサイズとSHA-256を検証してから一つのメッセージとして通知します。`state`の参加時スナップショット、`snapshot`、`checkpoint`、`catchup`も同じ転送処理を使います。

```js
await room.message("canvas.snapshot", largeCanvas);
```

既定の論理メッセージ上限は4 MiB、通信片は12 KiBです。上限には外側のメタデータも含みます。サーバー側は[`config/limits.example.json`](config/limits.example.json)を参考にJSONを用意し、`--limits`で指定できます。

```powershell
.tools/go/bin/go.exe run ./cmd/kokosync --limits config/limits.example.json
```

SDKでも`fragmentation`オプションを指定でき、接続時にサーバーとSDKの小さい側へ合わせます。転送中は制御・状態・イベント・通常メッセージを別々に送出し、位置更新は非再送の経路を使います。同じ順序付きメッセージ列の後続操作は、先行メッセージの完成を待ちます。通信帯域や下位層の混雑を超える低遅延は保証しません。詳しくは[実装範囲と上限](docs/implementation-status.md)を参照してください。

| 入口 | 用途 |
| --- | --- |
| `message` | アプリが意味を決める順序付き操作。部屋内の連番を付与し、大きい値を自動分割 |
| `event` | 現在の参加者への通知。途中参加者への再生なし |
| `latest` | カーソルや位置。古い連番を破棄 |
| `set` / `delete` | 参加時に`state: true`を指定して使う共有状態。版を条件にした更新に対応 |
| `follow` / `checkpoint` | アプリの初期状態と、その後の操作を連続して適用するJSヘルパー |
| `sendBlob` / `acceptBlob` | 受信側の処理速度に合わせた分割転送とSHA-256確認 |
| `live` | 小さな期限付きバイト列。動画の撮影・圧縮・再生はアプリが担当 |
| `stats` | 送受信量・待機量・破棄数・実際の通信方式を取得 |

ブラウザはWebRTC、GoはQUIC、C#はLiteNetLibで共通の中継サーバーへ接続します。WebSocketは明示して選ぶ代替経路です。状態やファイルは信頼性のある経路、位置などは非再送の経路を使います。通信方式ごとの制約は[実装範囲](docs/implementation-status.md)を参照してください。

## SDKと仕様を読む

IPアドレスで公開したHTTPSデプロイの構成・運用方法は[公開サーバー](docs/deployment.md)を参照してください。ローカル起動の既定値は変更していません。

| 文書 | 内容 |
| --- | --- |
| [JS / TypeScript SDK](sdk/js/README.md) | API、初回同期ヘルパー、ファイル転送 |
| [C# SDK](sdk/csharp/README.md) | .NETからの接続、非同期受信、ファイル転送 |
| [Go SDK](sdk/go/README.md) | QUIC接続、共有状態、ストリーム転送 |
| [実行版の通信仕様](protocol/prototype.md) | 今回実装したメッセージ・上限・接続方法 |
| [自動分割の通信仕様](protocol/fragmentation.md) | 上限の合意、credit、検証、送信順序 |
| [実装範囲と確認結果](docs/implementation-status.md) | 動く範囲、未実装、設計との差 |
| [全体設計](docs/design.md) | 目指す構成と設計判断 |
| [通信・同期設計](docs/protocol.md) | 完成形の初期同期、再接続、流量制御 |
| [SDK設計](docs/sdk.md) | 三言語の共通契約 |
| [検証計画](docs/validation.md) | 将来の負荷・障害・動画検証 |

通常起動の接続先はループバックに限定しています。公開モードはHTTPSのアクセス認証を通してブラウザを接続します。保存先はメモリで、サーバー終了時にデータが消えます。アプリ内のユーザー認証、永続化、自動再接続、P2P、動画フレームの分割と再生は今後の実装です。性能目標の達成は未計測です。

更新日: 2026-10-21。
