#!/usr/bin/env bash
# Installs an extracted flowd release (bin/flowd, lib/*.so from
# deploy/ec2/build-release.sh) as a systemd service on an EC2 instance
# (Amazon Linux 2023 or Ubuntu). Run as root on the instance:
#
#   sudo ./install-worker.sh /opt/flowd [worker.env]
#
# The release must be built for the instance's architecture.
set -euo pipefail

RELEASE="${1:?usage: install-worker.sh <release dir> [worker.env]}"
ENV_FILE="${2:-$(dirname "$0")/worker.env.example}"
HERE="$(cd "$(dirname "$0")" && pwd)"

if [[ $EUID -ne 0 ]]; then
    echo "must run as root" >&2
    exit 1
fi

id flowd >/dev/null 2>&1 || useradd --system --home-dir /var/lib/flowd --shell /sbin/nologin flowd
install -d -o flowd -g flowd -m 0750 /var/lib/flowd
install -d -m 0755 /etc/flowd
install -m 0755 "$RELEASE/bin/flowd" /usr/local/bin/flowd
# The binary's RPATH is $ORIGIN/../lib, i.e. /usr/local/lib.
if compgen -G "$RELEASE/lib/*.so*" >/dev/null; then
    cp -a "$RELEASE"/lib/*.so* /usr/local/lib/
    ldconfig || true
fi
if [[ ! -f /etc/flowd/worker.env ]]; then
    install -m 0644 "$ENV_FILE" /etc/flowd/worker.env
fi
install -m 0644 "$HERE/flowd-worker.service" /etc/systemd/system/flowd-worker.service

# Leases are wall-clock timestamps compared across machines: keep the clock
# synchronised (Amazon Time Sync Service via chrony).
if command -v chronyc >/dev/null 2>&1; then
    systemctl enable --now chronyd 2>/dev/null || systemctl enable --now chrony 2>/dev/null || true
fi

systemctl daemon-reload
systemctl enable --now flowd-worker.service
systemctl --no-pager status flowd-worker.service || true
