#!/usr/bin/env bash
set -euo pipefail
install -d -m 755 /var/www/kokosync-acme/.well-known/acme-challenge
install -d -m 750 /etc/kokosync
chgrp www-data /etc/kokosync
cat >/etc/nginx/sites-available/kokosync <<'NGINX'
server {
    listen 80 default_server;
    listen [::]:80 default_server;
    server_name 45.77.130.146;
    location /.well-known/acme-challenge/ { root /var/www/kokosync-acme; }
    location / { return 301 https://45.77.130.146$request_uri; }
}
NGINX
if [ -L /etc/nginx/sites-enabled/default ]; then unlink /etc/nginx/sites-enabled/default; fi
ln -sfn /etc/nginx/sites-available/kokosync /etc/nginx/sites-enabled/kokosync
nginx -t
systemctl enable --now nginx
systemctl reload nginx
ufw allow 80/tcp
ufw allow 443/tcp
ufw allow 40000:40100/udp
certbot certonly --non-interactive --agree-tos --register-unsafely-without-email \
    --webroot -w /var/www/kokosync-acme --ip-address 45.77.130.146 \
    --preferred-profile shortlived --cert-name kokosync-ip \
    --deploy-hook 'systemctl reload nginx'
if ! id kokosync >/dev/null 2>&1; then useradd --system --home-dir /opt/kokosync --shell /usr/sbin/nologin kokosync; fi
install -d -m 755 /opt/kokosync/releases
if [ ! -f /etc/kokosync/bridge.env ]; then
    printf 'KOKOSYNC_BRIDGE_SECRET=%s\n' "$(openssl rand -hex 32)" >/etc/kokosync/bridge.env
    chmod 600 /etc/kokosync/bridge.env
fi
if [ ! -f /etc/kokosync/htpasswd ]; then
    password="$(openssl rand -hex 16)"
    printf '%s\n' "$password" | htpasswd -iBc /etc/kokosync/htpasswd demo
    printf 'URL: https://45.77.130.146/\nUsername: demo\nPassword: %s\n' "$password" >/root/kokosync-access.txt
    chmod 600 /root/kokosync-access.txt
    chown root:www-data /etc/kokosync/htpasswd
    chmod 640 /etc/kokosync/htpasswd
fi
systemctl enable --now certbot.timer
printf '\nHost prepared; access credentials saved in /root/kokosync-access.txt\n'
