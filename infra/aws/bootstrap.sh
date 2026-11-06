#!/usr/bin/env bash
set -euo pipefail

# Arm the secondary stop guard before network-dependent setup.
cat >/etc/systemd/system/environmental-demo-stop.service <<'UNIT'
[Unit]
Description=Stop the environmental streaming demo host

[Service]
Type=oneshot
ExecStart=/usr/bin/systemctl --no-block poweroff
UNIT

cat >/etc/systemd/system/environmental-demo-stop.timer <<'UNIT'
[Unit]
Description=Stop the demo host eight hours after boot

[Timer]
OnBootSec=8h
AccuracySec=1s

[Install]
WantedBy=timers.target
UNIT

systemctl daemon-reload
systemctl enable --now environmental-demo-stop.timer

export DEBIAN_FRONTEND=noninteractive
apt-get -o Acquire::Retries=3 update
apt-get -o Acquire::Retries=3 install -y --no-install-recommends \
    ca-certificates docker.io docker-compose-v2 openjdk-21-jre-headless python3.12-venv

systemctl enable --now docker
usermod -aG docker ubuntu
