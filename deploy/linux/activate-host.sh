#!/usr/bin/env bash
set -euo pipefail
release_id=${1:?Pass a new release ID, for example 20261021-hosted-02}
if [[ ! "$release_id" =~ ^[A-Za-z0-9][A-Za-z0-9._-]*$ ]]; then echo 'Invalid release ID' >&2; exit 1; fi
release=/opt/kokosync/releases/$release_id
if [ -e "$release" ]; then echo 'Release already exists; choose a new ID' >&2; exit 1; fi
previous=$(readlink -f /opt/kokosync/current || true)
case "$previous" in /opt/kokosync/releases/*) ;; *) previous='' ;; esac
backup=$(mktemp -d /etc/kokosync/deploy-backup.XXXXXX)
for path in /etc/systemd/system/kokosync.service /etc/systemd/system/kokosync-litenetlib.service /etc/nginx/sites-available/kokosync; do
    if [ -f "$path" ]; then cp -p "$path" "$backup/$(basename "$path")"; fi
done
rollback() {
    status=$?
    trap - ERR
    echo 'Deployment failed; restoring previous service configuration.' >&2
    for path in /etc/systemd/system/kokosync.service /etc/systemd/system/kokosync-litenetlib.service /etc/nginx/sites-available/kokosync; do
        if [ -f "$backup/$(basename "$path")" ]; then cp -p "$backup/$(basename "$path")" "$path"; fi
    done
    if [ -n "$previous" ]; then
        ln -sfn "$previous" /opt/kokosync/current
        systemctl daemon-reload
        systemctl restart kokosync.service kokosync-litenetlib.service || true
        nginx -t && systemctl reload nginx || true
        echo "Previous release restored: $previous" >&2
    else
        systemctl stop kokosync.service kokosync-litenetlib.service || true
    fi
    exit "$status"
}
trap rollback ERR
install -d -m 755 "$release"
tar -xzf /root/kokosync-linux.tar.gz -C "$release"
chmod 755 "$release/bin/kokosync" "$release/bin/sidecar/KokoSync.Sidecar"
if [ ! -f /opt/kokosync/limits.json ]; then
    install -m 644 "$release/config/limits.example.json" /opt/kokosync/limits.json
fi
if ! grep -q '^KOKOSYNC_LIMITS=' /etc/kokosync/bridge.env; then
    printf '\nKOKOSYNC_LIMITS=/opt/kokosync/limits.json\n' >>/etc/kokosync/bridge.env
fi
ln -sfn "$release" /opt/kokosync/current
cat >/etc/systemd/system/kokosync.service <<'UNIT'
[Unit]
Description=Kokosync browser relay
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=kokosync
Group=kokosync
WorkingDirectory=/opt/kokosync/current
EnvironmentFile=/etc/kokosync/bridge.env
ExecStart=/opt/kokosync/current/bin/kokosync --root /opt/kokosync/current --http 127.0.0.1:8080 --quic 127.0.0.1:8081 --public-ip 45.77.130.146 --rtc-udp-min 40000 --rtc-udp-max 40100
Restart=on-failure
RestartSec=3
KillSignal=SIGINT
TimeoutStopSec=15
NoNewPrivileges=true
PrivateTmp=true
ProtectHome=true
ProtectSystem=strict
LimitNOFILE=4096
MemoryMax=512M

[Install]
WantedBy=multi-user.target
UNIT
cat >/etc/systemd/system/kokosync-litenetlib.service <<'UNIT'
[Unit]
Description=Kokosync local LiteNetLib adapter
After=network-online.target kokosync.service
Wants=kokosync.service

[Service]
Type=simple
User=kokosync
Group=kokosync
WorkingDirectory=/opt/kokosync/current
EnvironmentFile=/etc/kokosync/bridge.env
Environment=DOTNET_CLI_TELEMETRY_OPTOUT=1
ExecStart=/opt/kokosync/current/bin/sidecar/KokoSync.Sidecar --insecure-development
Restart=on-failure
RestartSec=3
KillSignal=SIGINT
TimeoutStopSec=15
NoNewPrivileges=true
PrivateTmp=true
ProtectHome=true
ProtectSystem=strict
LimitNOFILE=4096
MemoryMax=512M

[Install]
WantedBy=multi-user.target
UNIT
cat >/etc/nginx/sites-available/kokosync <<'NGINX'
server {
    listen 80 default_server;
    listen [::]:80 default_server;
    server_name 45.77.130.146;
    location /.well-known/acme-challenge/ { root /var/www/kokosync-acme; }
    location / { return 301 https://45.77.130.146$request_uri; }
}
server {
    listen 443 ssl default_server;
    listen [::]:443 ssl default_server;
    server_name 45.77.130.146;
    ssl_certificate /etc/letsencrypt/live/kokosync-ip/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/kokosync-ip/privkey.pem;
    ssl_protocols TLSv1.2 TLSv1.3;
    auth_basic "Kokosync prototype";
    auth_basic_user_file /etc/kokosync/htpasswd;
    client_max_body_size 1m;
    add_header X-Content-Type-Options nosniff always;
    add_header Referrer-Policy same-origin always;
    add_header Cache-Control "no-store" always;
    location = /bridge { return 403; }
    location / {
        proxy_pass http://127.0.0.1:8080;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Forwarded-Proto https;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header X-Kokosync-Bridge "";
        proxy_buffering off;
        proxy_read_timeout 3600s;
        proxy_send_timeout 60s;
    }
}
NGINX
nginx -t
systemctl daemon-reload
systemctl enable kokosync.service kokosync-litenetlib.service
systemctl restart kokosync.service kokosync-litenetlib.service
systemctl reload nginx
ready=false
for attempt in {1..30}; do
    if systemctl is-active --quiet kokosync kokosync-litenetlib && curl --fail --silent http://127.0.0.1:8080/health >/dev/null; then ready=true; break; fi
    sleep 1
done
if [ "$ready" != true ]; then echo 'Services did not become healthy.' >&2; false; fi
systemctl is-active kokosync kokosync-litenetlib nginx
curl --fail --silent http://127.0.0.1:8080/health
printf '\n'
ss -lntup | grep -E '(:443|:8080|:8081|:8082)\b'
trap - ERR
echo "Activated release: $release"
