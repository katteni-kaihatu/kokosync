# Tonarium向け: peer間の確実配送へ移す

このガイドは、既存のkokosync接続へ宛先付き配送を追加する開発者向けです。最初に「配送APIを選ぶ」「操作を送る」「失敗を扱う」を読み、初回同期や保存の節は各機能を移す際に参照してください。配送契約の詳細は[peer-delivery.md](../protocol/peer-delivery.md)にあります。

## 配送APIを選ぶ

| 用途 | APIと保証 |
| --- | --- |
| 位置、向き、カーソル | 既存の`latest`。欠落を許容し、新しい値を優先 |
| 期限付きの小さいライブデータ | 既存の`live`。古いデータの再送を求めない |
| 全員でコアの同じ連番を使う操作 | 既存の`message`。checkpoint/catchupの履歴へ残る |
| 特定の相手への操作や大きい値 | 新しい`sendTo`。送信者・宛先・接続世代・トピックごとの順序と、SDKの受信確認 |
| ファイルをストリームとして扱う既存処理 | 既存の`sendBlob`。今回の`sendTo`とは独立したAPI |

`sendTo`の値は既存SDKと共通のMessagePack対応値です。null、真偽値、安全な有限数、文字列、バイト列、配列、文字列キーのマップを使います。アプリ固有のクラスやMessagePack拡張型はそのまま送りません。大きい値はSDKが自動で分割・再構成・ハッシュ検証します。送信側と受信側は、上限内の値全体を保持するメモリが必要です。

## 操作を送る

受信処理は送信前に登録します。`frame.peer`と接続世代は配送層の情報です。本文に含まれるユーザー名を本人確認の代わりに使わず、Tonarium側でそのpeerの権限を確認してください。

```js
room.on("peer_message", frame => {
  if (frame.topic !== "tonarium.operation") return;
  if (!app.mayApply(frame.peer, frame.value)) return;
  app.applyOnce(frame.value.eventId, frame.value);
});

const event = {
  eventId: crypto.randomUUID(), // アプリの再接続・保存をまたぐ識別子
  kind: "canvas.stroke.add",
  objectId: "stroke-123",
  points: [0.1, 0.2, 0.3, 0.4],
};
const receipt = await room.sendTo(targetPeer, "tonarium.operation", event);
console.log(receipt); // 相手SDKが受け付けた。適用・保存完了の意味ではない
```

この`app`はTonariumが実装する処理の例です。`applyOnce`は、アプリのイベントIDを使って適用済みの操作を判定します。SDKの重複排除は接続世代の範囲内なので、再接続後の再送や永続保存にはアプリのIDも必要です。

SDKは順序どおりに通知しますが、非同期のイベントハンドラーの完了順までは管理しません。適用処理が非同期なら、アプリ側でも該当する送信者・トピックの処理を直列にし、その待機量に上限を設けます。

複数人への送信は、相手ごとに結果を確認します。一人への成功を全員への成功として扱わないでください。

```js
const outcomes = await Promise.allSettled(
  authorizedPeers.map(peer => room.sendTo(peer, "tonarium.operation", event))
);
outcomes.forEach((result, index) => {
  app.recordDelivery(authorizedPeers[index], event.eventId, result);
});
```

Goでは`SendTo(ctx, peer, topic, value)`、C#では`SendToAsync(peer, topic, value, ct)`を使います。通知はそれぞれ`Messages()`、`Events()`の`peer_message`です。直結用アダプターのないクライアントでも、同じ契約で中継できます。

## 失敗を扱う

SDKは期限や再送回数を超えた際に失敗を返します。確認が失われた場合は、相手が既に受け付けている可能性があります。失敗を「未適用」の証明にせず、アプリのイベントIDで状態を確認してください。

| 主なエラーコード | アプリ側の扱い |
| --- | --- |
| `UnsupportedCapability` | 相手やサーバーの対応を確認し、必要ならアプリが定義した旧経路を使う |
| `PeerUnavailable` | 退出や世代変更を確認する。同じ名前への自動的な付け替えはしない |
| `Backpressure` | 待機量・受信量を減らす。再試行前に`outcome`と配送列の状態を確認する |
| `Timeout` / `TransportFailed` / `Closed` | 到着済みの可能性を考慮し、イベントIDで確認して再同期する |
| `LaneFailed` | 新しい接続世代で同期状態を確認してから再開する |
| `InvalidArgument` / `InvalidValue` / `PayloadTooLarge` | 宛先・トピック・値・サイズを修正する |
| `ProtocolError` | 相手のSDKバージョンや通信記録を調べる |

送信開始後に回復不能になったトピックの配送列は停止し、後続には`LaneFailed`を返します。欠けた操作を飛ばして処理を進めないためです。他の相手・トピックは継続できます。再参加後は新しい接続世代となるので、アプリが同期状態を確認してから送信を再開します。

```js
const controller = new AbortController();
try {
  await room.sendTo(targetPeer, "tonarium.snapshot", snapshot, {
    signal: controller.signal,
    timeoutMs: 30000,
  });
} catch (error) {
  app.showDeliveryFailure(error.code, error.outcome);
  // 同じ操作を新しいイベントとして作り直さず、適用済みか確認する。
}
// 必要な時点で controller.abort()。既に届いたデータの取り消しではない。
```

送信開始前のキャンセルは、その待機中の送信だけを外します。開始後のキャンセルは配送列の失敗になり得ます。キャンセルや退出と受信完了が競合した場合も、既に適用・保存された操作をSDKが巻き戻すことはありません。

ここで送信開始とは、待ち行列の先頭になってSDKが連番を確定した時点です。その後のローカル送信失敗では、通信片が未送信でも配送列を停止する場合があります。`not-sent`は相手へ送信していないことを表し、その配送列が継続可能であることまでは表しません。

## スナップショットと操作を別トピックにする

大きい値の通信片の間に、小さい操作や位置更新を送れます。ただし同じトピックの後続は先行データを追い越しません。

```js
const bulk = room.sendTo(targetPeer, "tonarium.snapshot", snapshot);
const small = room.sendTo(targetPeer, "tonarium.operation", operation);
await Promise.all([bulk, small]);
```

別トピックにすると通知順序も独立するため、受信した操作がスナップショットの適用前に届く可能性があります。Tonariumはスナップショットへ取り込み済みのイベント範囲を含め、取得中の操作を容量制限付きで待機させ、適用後に未取り込み分だけを反映します。

初回スナップショットの生成・適用・保存先の選択はTonarium側です。まず既存サーバーからの初回同期を維持し、操作の配送だけを移すと、変更範囲を限定できます。新しいpeer配送は既存のコア履歴へ記録されないため、その操作を`catchup`で回収できるとは扱わないでください。

## 保存完了にはアプリの応答を使う

受信確認と保存確認を分けて表示します。例えばサーバー側のアプリがイベントを保存し終えた後、`tonarium.saved`に`eventId`を入れて返信します。送信側は、認可した保存担当から来た応答かも確認します。

`sendTo`の成功は受信済み、`tonarium.saved`の受信はアプリが定義する保存済みです。この区別は、直結と中継のどちらを通っても変わりません。

## 段階的に移行する

1. コアと各SDKを更新し、相手の`peerDelivery`対応能力を確認する。旧版への自動的な`message`置換は行わない。全体順序・履歴・宛先の意味が異なるため、Tonariumが旧経路の使用可否を決める。
2. チャットやサイコロなど、適用ルールを定義済みの操作から移す。認可済み宛先とアプリのイベントIDを使い、全員への部分的な失敗を扱う。
3. キャンバスを追加し、重複、削除済みID、同時更新の規則を確認する。同じ送信者の順序だけでは、複数人の競合は解決しない。
4. スナップショット・再参加・保存確認を接続する。初回同期とライブ操作の切り替え境界をアプリで検証する。
5. 経路別の統計を監視し、実データ量に合わせて容量と期限を調整する。経路が直結になったことだけを、低遅延や保存成功の証拠にしない。

TonariumのようにJS SDKを`vendor/`へ取り込む場合は、`index.js`だけでなく`sdk/js/`内の実行モジュールと型定義を一緒に更新します。`peer-delivery.js`、`routing.js`、`fragmentation.js`も必要です。依存するMessagePackとSHA-256のパッケージは、SDKのREADMEに従ってバンドラーまたはimport mapで解決します。

## サンプルを動かす

ローカルサーバーを起動してから、同じ部屋へブラウザとサンプルを参加させます。受信側の名前を`browser-a`とした例です。

```powershell
# 先にブラウザをpeer-deliveryへ、browser-aという名前で参加させる。
node examples/js/peer-delivery.js http://127.0.0.1:8080 peer-delivery node-peer browser-a
.tools/go/bin/go.exe run ./examples/go --room peer-delivery --peer go-peer --send-to browser-a --insecure-development --seconds 30
.tools/dotnet/dotnet.exe run --project examples/csharp -- --room peer-delivery --peer csharp-peer --send-to browser-a --insecure-development --smoke --seconds 30
```

各サンプルは小さい操作を3件、1 MiBの値を1件送り、受信確認を表示します。受信した大容量データは全文をログへ出さず、長さとSHA-256を表示します。ブラウザの「Reliable peer messages」からも相手を指定して返信できます。JSのNode、Go、C#のサンプルは中継経路です。

## 用語と調査先

| 用語 | 意味 |
| --- | --- |
| messageId | SDKが一回の論理送信へ付けるID。再送や経路変更で変えない |
| eventId | アプリが操作に付けるID。再接続や保存をまたぐ重複排除に使う |
| 接続世代 | 同じpeer名でも、再参加前後を区別する識別子 |
| 配送列 | 同じ送信者・宛先・接続世代・トピックの順序を管理する単位 |
| SDK受信確認 | 検証済みデータをSDKが受け付けた確認。適用・保存確認とは別 |

不具合を調べるときは、アプリのeventId、SDKのmessageId、両端の接続世代、トピック、失敗コード、`deliveryStats()`の対象行を記録してください。秘密情報を含む本文全体の記録は不要です。配送動作はkokosync、認可・適用・保存結果はTonariumで調査します。更新日: 2026-10-21。
