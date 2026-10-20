# 宛先付き配送の検証記録

実施日: 2026-10-21。対象は[peer配送v1](../protocol/peer-delivery.md)。同一Windows端末のChrome、Go QUIC、C# LiteNetLibを、ローカルの共通コアへ接続して確認した。異なる回線・NAT、インターネットでの長時間運転、スループット上限は未検証。

## 自動テストで失敗時の契約を確認

Goのパッケージテストと追加の回帰テスト、JSの35件の単体テストと実WebSocket転送、C#の単体テストとWebSocket・LiteNetLib結合テストを実施した。全体の実行入口は次のスクリプト。

```powershell
./scripts/start.ps1
./scripts/smoke.ps1
```

| 確認項目 | 確認した挙動 |
| --- | --- |
| 最終受信確認の欠落 | 同じIDで再送し、アプリ通知は一回 |
| 分割片の重複・順序逆転 | 受信済み位置から回復し、ハッシュ一致後に通知 |
| 不正な確認・受信許容量 | 未送信の位置や完了前の確認で成功にしない |
| 送信待機中のキャンセル | 連番を消費せず、待機データを解放 |
| 開始後の失敗 | 該当する配送列の後続も失敗させ、順序の穴を隠さない |
| 大容量による枠の占有 | 小さい別トピック用の転送数・容量を残す |
| 受信待ちのアプリ | 完成済みのデータも容量制限の対象とし、受け付けられない場合に成功確認を返さない |
| 送信者の退出 | 受け付け済みのデータは元の接続世代のまま一回取り出せる |
| 世代の変更 | 古い相手宛ての転送を新しい接続へ付け替えない |
| メモリの解放 | 終了した転送の本文、待機列の空き要素、退出した世代の統計を保持し続けない |
| 既存APIとの互換性 | peer配送でコア連番・履歴が増えず、既存message・初期同期・blobの確認も通る |

## 5人を同時参加させて相互配送を確認

共通の部屋`reliable-five`へ、Chromeを`browser-a`・`browser-b`・`browser-c`の名前で参加させた。aとbはWebRTC、cはWebSocketで「Use relay only」を有効にした。さらに次の2プロセスを参加させ、5人構成にした。

```powershell
.tools/go/bin/go.exe run ./examples/go --room reliable-five --peer go-five --send-to browser-a --insecure-development --seconds 300 --smoke
.tools/dotnet/dotnet.exe run --project examples/csharp -- --room reliable-five --peer cs-five --send-to browser-b --insecure-development --seconds 300 --smoke
```

GoとC#からそれぞれ操作1・2・3と1 MiBの値を送り、ブラウザ側で順序とSHA-256一致を確認した。続いてaから残り4人へ1 MiBを送り、全員のSDK受信確認を得た。bは直結可能な経路、c・Go・C#は中継経路で通信した。

aの大容量送信が待機中である間に、cから小さい操作を全員へ送り、aから`latest`と`live`も送った。小さい操作の受信確認と位置・ライブ更新が進むことを確認した。Go・C#が受け取った大きい値も、長さとハッシュが一致した。

最初の試行では、直結の送信待ちが一時的に増えた際に中継へ切り替わった。この結果を受け、健全なチャネルの一時的な送信待ちと接続断を区別した。修正後のa→bの1 MiB転送は、再送なしで直結のまま完了した。

## 転送途中の切断・キャンセル・再参加を確認

a→bの2 MiB転送で「Sending」が表示されている間に、bの「Use relay only」を有効にして直結を閉じた。送信統計は直結と中継の両方で増加し、同じメッセージID・連番2の受信確認を得た。bの通知は一回で、2 MiBのハッシュが一致した。再送が発生したため、RTTはnullだった。

別トピック`demo.cancel`で開始した2 MiB転送をキャンセルすると、結果は`Cancelled / unknown`となった。同じトピックの後続は`LaneFailed / not-sent`で失敗し、bの受信件数は増えなかった。bを同じ名前で再参加させた後は、新しい相手の接続世代に対する連番1として、小さい操作を受信できた。

Node用の[使用例](../examples/js/peer-delivery.js)も同じサーバーで実行し、操作3件と1 MiBの受信確認を得た。これらは小規模な動作確認であり、性能保証の測定値ではない。
