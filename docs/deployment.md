# 公開サーバー: IPアドレスのHTTPSで接続する

2026-10-21に `45.77.130.146` へデプロイした。公開URLは **https://45.77.130.146/**。アクセスにはHTTP Basic認証を使い、ユーザー名は`demo`。生成したパスワードはリポジトリへ含めず、サーバーの`/root/kokosync-access.txt`と作業端末の`.run/deployment-access.txt`へ保存している。

## 起動と停止

SSHでサーバーに入り、次のコマンドを使う。

```sh
systemctl status kokosync kokosync-litenetlib nginx
systemctl restart kokosync kokosync-litenetlib
journalctl -u kokosync -u kokosync-litenetlib -n 100 --no-pager
```

両サービスは専用の`kokosync`ユーザーで動き、サーバー再起動時に自動起動する。状態はメモリ保存なので、`kokosync`の再起動で部屋・描画・操作履歴が消える。

| 項目 | 配置 |
| --- | --- |
| 公開するリリース | `/opt/kokosync/current`から`releases/`以下へのリンク |
| Goサーバー | `current/bin/kokosync` |
| .NET中継 | `current/bin/sidecar/KokoSync.Sidecar`。自己完結型のLinux版 |
| サービス定義 | `/etc/systemd/system/kokosync.service`、`kokosync-litenetlib.service` |
| nginx設定 | `/etc/nginx/sites-available/kokosync` |
| アクセス認証のハッシュ | `/etc/kokosync/htpasswd` |
| 内部接続用secret | `/etc/kokosync/bridge.env`。rootのみ読取可 |
| メッセージ・転送容量の上限 | `/opt/kokosync/limits.json`。再デプロイ時も既存の設定を維持 |
| 証明書 | `/etc/letsencrypt/live/kokosync-ip/` |

## 公開するのはブラウザ経路

TCP 80はHTTPSへの転送と証明書の検証に使う。TCP 443は認証付きHTTPS、UDP 40000–40100はWebRTC用。HTTPの8080、QUICの8081、LiteNetLibの8082はループバックだけで待ち受ける。公開側の`/bridge`は拒否する。

ブラウザはWebRTCで操作と位置を交換できる。UDPを使えないネットワークでは、画面からWebSocketを明示して選ぶ。C#・Goのネイティブ接続をインターネットへ直接公開する設定は追加していない。

ブラウザ同士の`latest` / `live`は、サーバーで参加を確認して接続情報を交換し、可能なら直結する。直結先は相手のブラウザであり、上記のUDP公開ポートはブラウザとサーバーのWebRTC接続用。直結できなければ、接続済みのサーバー経由で送り続ける。ICEサーバーは既定で未指定のため、異なるNAT間での接続成立は保証しない。アプリから`routing.iceServers`を指定できる。

アクセス認証はサイト全体で共通であり、利用者別の権限管理ではない。アプリの認証・永続化・ネイティブ向け直結などの未実装範囲は[実装状況](implementation-status.md)を参照。

## 自動分割転送の上限を変える

`/opt/kokosync/limits.json`を編集し、`systemctl restart kokosync kokosync-litenetlib`で反映する。初期値は[limits.example.json](../config/limits.example.json)と同じ。論理メッセージ、通信片、受信予約、同時転送数、期限、保存容量を個別に設定できる。SDKが提示する上限のほうが小さい場合は、接続時にそちらを採用する。

設定はリリースの外側へ保存するため、再デプロイで上書きしない。不正な設定ではGoサーバーは起動しない。値を上げる際は、同時接続数や復号後のオブジェクトが使うメモリも考慮し、systemdの`MemoryMax=512M`を超えない範囲で確認する。

## 証明書は自動更新する

Let's EncryptのIPアドレス用短期証明書を使い、`certbot.timer`で更新する。更新後はdeploy hookでnginxを再読み込みする。更新時にもTCP 80の検証用パスを公開しておく。

```sh
systemctl list-timers certbot.timer
certbot renew --cert-name kokosync-ip --dry-run --run-deploy-hooks --no-random-sleep-on-renew
```

設定根拠: [Let's EncryptのIP証明書とCertbotの説明](https://letsencrypt.org/2026/03/11/shorter-certs-certbot.html)。初回の実証明書の有効期限は2026-10-27で、自動更新により差し替わる。

## 再デプロイ

Windowsの作業端末からは、リポジトリのルートで次を実行する。

```powershell
./scripts/deploy.ps1 -ReleaseId release-20261021-fragmented
```

Goサーバーと.NET中継をLinux向けにビルドし、ブラウザ用ファイルとSDKをまとめてSSHで送る。接続先はこの文書のIPアドレスに固定し、鍵は既定で`~/.ssh/id_ed25519`を使う。`-IdentityFile`で鍵のパスを変更できる。

リリース名には英数字・ハイフン・アンダースコア・ピリオドが使える。同名のローカル作業フォルダーやサーバー上のリリースがあれば上書きせずに止まる。省略時は日時から名前を作る。各リリースの`build-info.json`には、ソースのコミット番号と未コミット変更の有無を残す。

切替後にサービスの起動・ヘルスチェックが失敗した場合は、直前のリリースと設定へ戻す。メモリ上の部屋は再起動で消えるため、復旧しても再参加が必要になる。

同梱の[`prepare-host.sh`](../deploy/linux/prepare-host.sh)は初回のnginx・証明書・アクセス認証設定用。Ubuntuに`nginx`、`certbot`、`apache2-utils`が必要。[`activate-host.sh`](../deploy/linux/activate-host.sh)は、`/root/kokosync-linux.tar.gz`を新しいリリース名で展開して有効化する。既存のリリース名への上書きは拒否する。

アーカイブの直下には`bin/kokosync`、`bin/sidecar/`、`examples/browser/`、`sdk/js/`、`config/limits.example.json`、ブラウザ用の`node_modules/@msgpack/`と`node_modules/@noble/`を置く。Goは`GOOS=linux GOARCH=amd64 CGO_ENABLED=0`でビルドし、.NETは`dotnet publish -r linux-x64 --self-contained true`で出力する。切替スクリプトはサービスの再起動まで行う。

```sh
bash /root/kokosync-activate-host.sh 20261021-hosted-02
```

## 確認結果

手元のChromeから公開IPへ接続し、WebRTCの2画面で同じ描画と操作番号を確認した。WebSocketへの切替後も、WebRTC側へ操作が届いた。認証付きの`/health`は200、未認証は401、公開側の`/bridge`は403。TLS証明書のIP一致と信頼チェーン、サービスの自動起動設定を確認し、証明書更新のdry-runとnginx再読み込みも成功した。

自動分割転送の追加版は`release-20261021-fragmented-01`として配置した。公開WebRTCで1 MiBの通常メッセージを送り、送受信側で同じSHA-256を確認。後から参加したブラウザも、そのメッセージを含む大容量のcatchupを復元した。ローカルではGo・JS・C#の分割、容量上限、期限、キャンセルの軽いテストを通している。速度の数値は製品保証として測定していない。

相手ごとの経路選択は`release-20261021-peer-routes-01`として配置した。公開URLへ接続したChromeの2画面で、一方の中継接続をWebRTC、もう一方をWebSocketとして、ブラウザ間の直結で`latest` / `live`を受信した。片方で直結を終了した後も、中継への復帰と両データの受信を確認した。画面には経路、切り替え理由、直結の往復時間、経路別の送受信数を表示する。

この直結確認は同じ端末上のブラウザ同士で行った。公開サーバーを通した接続情報の交換と中継は検証できたが、異なるNAT間での直結や混雑時の遅延は未測定。Go・C#を含む4クライアントの混在確認はローカルで実施し、ネイティブ側が中継で`latest` / `live`を受信することを確認した。

宛先付きの確実配送は、2026-10-21に`release-20261021-peer-delivery-b55ed5d`として配置した。ソースはコミット`b55ed5d0ad5beb7f64161d904294b40fc442f69b`で、未コミットの変更は含まない。既存の容量設定は維持した。

公開URLへ接続した同じ端末のChrome 2画面で、直結による1 MiBの配送とSDK受信確認を得た。続いて片方を中継専用へ切り替え、2 MiBを配送した。受信側では連番1・2、メッセージID、SHA-256一致を確認した。2 MiBの送信中にも逆方向の小さい操作と`latest` / `live`が届いた。異なるNAT間の直結や長時間の負荷試験は今回の確認範囲に含まない。
