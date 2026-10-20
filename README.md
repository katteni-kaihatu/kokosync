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

[ブラウザデモ](http://127.0.0.1:8080/)を2つ開き、同じRoom・異なる名前で参加すると、描画とカーソルを共有できます。初回同期はアプリのチェックポイント機能を使っており、`state`への参加は不要です。ファイル転送も試せます。デモは直近120点と5 MiB以下のファイルを扱います。

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
// 利用終了時に room.close()
```

値はnull・真偽値・有限の安全な数値・文字列・バイト列・配列・文字列キーのマップです。プロトコルの連番は64bit整数を使い、JSでは`bigint`として扱います。

| 入口 | 用途 |
| --- | --- |
| `message` | アプリが意味を決める順序付き操作。部屋内の連番を付与 |
| `event` | 現在の参加者への通知。途中参加者への再生なし |
| `latest` | カーソルや位置。古い連番を破棄 |
| `set` / `delete` | 参加時に`state: true`を指定して使う共有状態。版を条件にした更新に対応 |
| `follow` / `checkpoint` | アプリの初期状態と、その後の操作を連続して適用するJSヘルパー |
| `sendBlob` / `acceptBlob` | 受信側の処理速度に合わせた分割転送とSHA-256確認 |
| `live` | 小さな期限付きバイト列。動画の撮影・圧縮・再生はアプリが担当 |
| `stats` | 送受信量・待機量・破棄数・実際の通信方式を取得 |

ブラウザはWebRTC、GoはQUIC、C#はLiteNetLibで共通の中継サーバーへ接続します。WebSocketは明示して選ぶ代替経路です。状態やファイルは信頼性のある経路、位置などは非再送の経路を使います。通信方式ごとの制約は[実装範囲](docs/implementation-status.md)を参照してください。

## SDKと仕様を読む

| 文書 | 内容 |
| --- | --- |
| [JS / TypeScript SDK](sdk/js/README.md) | API、初回同期ヘルパー、ファイル転送 |
| [C# SDK](sdk/csharp/README.md) | .NETからの接続、非同期受信、ファイル転送 |
| [Go SDK](sdk/go/README.md) | QUIC接続、共有状態、ストリーム転送 |
| [実行版の通信仕様](protocol/prototype.md) | 今回実装したメッセージ・上限・接続方法 |
| [実装範囲と確認結果](docs/implementation-status.md) | 動く範囲、未実装、設計との差 |
| [全体設計](docs/design.md) | 目指す構成と設計判断 |
| [通信・同期設計](docs/protocol.md) | 完成形の初期同期、再接続、流量制御 |
| [SDK設計](docs/sdk.md) | 三言語の共通契約 |
| [検証計画](docs/validation.md) | 将来の負荷・障害・動画検証 |

接続先はループバックに限定しています。保存先はメモリで、サーバー終了時にデータが消えます。本番認証、永続化、自動再接続、P2P、動画フレームの分割と再生は今後の実装です。性能目標の達成は未計測です。

更新日: 2026-10-21。
