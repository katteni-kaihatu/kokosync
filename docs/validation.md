# 検証計画: 混在接続と回線飽和で同期契約を確かめる

状態: v0草案 / 2026-10-21。
数値はすべて、これから確かめる測定目標である。

今回行ったプロトタイプの動作確認は[実装状況](implementation-status.md)を参照。以下の負荷・性能・動画品質の計画とは区別する。

## 1. 最初の完成条件は三種類のクライアントが同じ状態へ戻れること

ブラウザJS + WebRTC、C# + LiteNetLib、Go + QUICを同時に同じセッションへ接続する。三組の双方向通信と、三者へのfan-outを確認する。片方のtransportだけが動いても混在対応の完了としない。

任意のmapを作成し、CASで競合を起こし、途中参加で同じ状態を取得する。次に一台を切断し、その間に作成・変更・削除を行い、復帰後の一致を確認する。アプリ型をサーバーへ追加せずに動くことも条件にする。

## 2. wire適合と同期の正しさを先に固定する

| 試験 | 合格条件 |
| --- | --- |
| 三言語encode/decode | 共通fixtureで値と整数精度が一致する。map順の違いは意味上許容 |
| Value境界 | Unicode、null/欠損、bytes、空map、深さ上限、64bit protocol値を正しく扱う |
| 不正入力 | 巨大長、重複map key、未知必須機能、圧縮展開超過を、巨大確保前に拒否 |
| 同時書込 | 同版CASは一件のみ成功。無条件setは確定順に一致 |
| ACK消失 | 送信後に切断し同じopIdを再送しても、revision増加は一回 |
| サーバー停止 | 永続モードの成功済み操作が復元される。memoryモードの喪失は明示される |
| snapshot中の更新 | 連続した作成・削除・更新でも、ready時の状態が確定ログと一致 |
| 購読範囲 | 非購読・非認可データを配送しない。変更時に古いcacheを破棄 |
| 世代変更 | 古い接続、古いpublisher、古い所有者からの更新を拒否 |
| 期限・重複 | expired live/eventを再生せず、重複イベントを再実行しない |

fixtureは値の意味と期待byte列を分ける。MessagePack全体のmap順を固定しない限り、意味が同じ別encodeを不合格にしない。hashは転送した具体的byte列に対して検証する。

## 3. 「大容量でも詰まらない」を飽和時の振る舞いで判定する

以下を同時実行する。値は初回profileで、調整時は結果に旧値も残す。

- 3 peerが各60 Hzで256 byte程度のlatestを送る。
- stateは部屋全体で毎秒20操作、1操作1 KiB以下を送る。
- 一つのsourceが6 Mbps・30 fpsのencoded video相当を送る。
- 一つのbulk senderが1 GiBの転送を続ける。
- 一つのreceiverの読取を停止し、別のreceiverは通常動作を続ける。

通常profileは、各クライアントと中継サーバーの区間を100 Mbps、区間RTT 20ms、loss 0%とする。制約profileでは各区間を10 Mbpsへ落とし、loss 1%、jitter 5msを加える。条件は中継の各区間に別々に適用する。

6 Mbpsの配信を複数人へ送れば、宛先数に応じてrelayの上り帯域を使う。利用可能帯域と宛先合計の送出量を記録する。

制約profileで要求量が容量を超えた場合、bulkの進捗低下やliveの品質低下は許容する。一方、queue増大を隠して古い動画を数秒後に流す振る舞いは不合格とする。遅い一台のせいで他のreceiverが止まらないことを確認する。

## 4. 遅延は内訳と比較対象を揃えて測る

| 指標 | 初回の目標 |
| --- | --- |
| 同一経路・低負荷でのSDK/relay追加遅延 | native timestamp計測でp95 10ms以下を目標 |
| bulk追加によるlatest到着遅延の悪化 | 上記制約profileでp95差分25ms以内を目標 |
| realtimeのローカル送出待ち | p95 10ms以内を目標。超過時に減速・破棄が働く |
| state確定応答 | ファイル転送中もp95差分50ms以内を目標。永続化時間は別掲 |
| queue | 設定上限以下を維持し、10分の飽和継続後も増加し続けない |
| live | maxLatencyMs超過と依存欠落を検知して破棄。正常回線では連続再生できる |
| 復旧 | 100 KiB snapshot・3 peer・通常profileで再接続開始から3秒以内を初回目標 |

配送量にも最低条件を置く。通常profileでは各receiverのlatestが55 Hz以上、30 fpsの動画が復号28 fps以上、有効フレームbytesが入力の95%以上、連続した映像停止は250ms未満を初回目標にする。再生開始と意図的なkeyframe待ちは別に記録する。画質評価には元映像と復号映像の比較を残す。

制約profileでは推奨bitrate通知から5秒以内のencoder減速を確認し、その後latest 30 Hz以上、動画の復号20 fps以上、連続停止1秒未満を初回目標にする。共通codecがこの条件を満たさなければloss対策・分割サイズ・FEC等を検討し、未達のまま対応済みとしない。live負荷を止めた後はbulkが2秒以内に再開し、通常profileで60秒間の平均goodputが同じ経路のbulk単独基準の70%以上へ戻ることも測る。

目標は回線や端末を問わない製品保証には使わない。p50/p95/p99、最大値、サンプル数、warm-up、実行時間、CPU・メモリ・GC、端末、OS、ブラウザ、依存バージョンを併記する。各profileを最低3回、warm-up後60秒以上実行する。

transport単体の小さなping/echoを基準にし、SDK込みでも同じ経路を通して比較する。中継を使う測定にP2Pの基準を混ぜない。一方向の追加処理時間は同一ホストの単調時計で測れる範囲を使う。別ホストのclock差を引けない場合はecho RTTと推定値として報告する。

ネットワークpacket loss、latestの送信前置換、frame期限切れ、アプリが購読しなかったデータを分けて数える。遅延が低く見えても、ほとんど送っていなければ成功とはしない。送信Hz、有効受信Hz、goodput、画像品質やdecoderエラーを同時に確認する。

## 5. 動画は実フレームの混在再生まで試す

synthetic bytesだけでは「動画が使える」と判定しない。共通codecを選び、encoded frameを実際にJS・C#・Goクライアントへ配送して、利用可能なdecoderで復元する。codec対応のない環境にはUnsupportedCapabilityを返す。

途中参加のcodec設定取得、keyframe要求、packet lossによる依存frame破棄、bitrate変更、source切替、再接続を試す。30 fps・6 Mbpsは初回目標であり、特定の画質を保証する数値ではない。encoder/decoder遅延と輸送遅延を分け、通常回線と飽和回線の双方を測る。

WebRTC RTP track経由の再生を、DataChannel/LiteNetLib/QUIC混在経路の成功と取り違えない。RTP経路を追加した場合は別profileで計測する。

## 6. 障害はgatewayと保存の境界でも起こす

WebRTCのICE再接続、TURN強制、UDP遮断、LiteNetLib接続断、QUIC再接続、IP変更、重複・順序入替・packet lossを試す。P2P最適化を追加したら、relayへの切替、両経路での重複、資格失効、旧経路からの遅着も対象にする。

latest/liveのin-flight windowを埋める全パケットを捨て、その後に回線を回復させる試験を加える。RETIRE/RETIRE_ACKまたは接続世代の更新で枠が戻り、手動操作なしで配信を再開することを確認する。control枠までunreliableの予算に巻き込まれて停止すれば不合格とする。

server core、.NET gateway、内部IPCをそれぞれ停止する。ACK直前・保存直後・snapshot中に止め、操作結果が不明な場合にunknownを返せるか確認する。disk fullと破損snapshotも再現し、成功を返したのに復元できない状態を防ぐ。

再接続を繰り返してもhandler、goroutine、event listener、buffer pool、Unity main-thread queueが残り続けないことを調べる。slow consumerを100回入れ替え、close後に使用量が落ち着くことを確認する。

## 7. 検証結果から段階的に公開範囲を広げる

| 段階 | 作るもの | 終了条件 |
| --- | --- | --- |
| A: 契約の確定 | kind registry、MessagePack fixture、共通error、APIの小さな例 | JS/C#/Goでfixture一致、レビュー完了 |
| B: 混在する基本同期 | 三adapter、認証、state、snapshot、resume、stats | 三者同時参加と主要障害試験が通る |
| C: 大容量とライブ | credit、queue予算、公平制御、blob、encoded frame | 飽和profile、動画混在、slow receiverで契約を守る |
| D: 配布可能なSDK | API整備、Unity/AOT、例、環境別能力表 | 対応環境で再現手順と実測値を公開できる |
| E: P2Pの追加 | 経路選択、認可、切替、TURN/relay比較 | 中継と同じ契約を守り、効果を計測できる |

P2Pの前でも三種類のクライアントを使ったプロトタイプは作れる。まず中継で正しさと観測を固め、追加した経路が速さやコストに寄与したかを比較する。

## 8. 実装開始前に記録する判断

初回の対象OS・ブラウザ・Unity/.NET環境、LiteNetLib暗号化、永続ストア、live codec、依存の固定版、公開API名、実測機の構成をADRに残す。今はその値を仮置きしており、未指定の人数や性能を達成済みと扱わない。

このリポジトリには現在、設計書のみを置く。上記の実行可能な試験・SDK・サーバーは今後の実装対象である。
