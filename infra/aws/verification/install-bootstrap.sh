#!/usr/bin/env bash
set -euo pipefail

# Exercise the unmodified bootstrap's real apt/usermod/file operations. Only
# service activation is intercepted because the container has no systemd PID 1.
systemctl() {
    printf '%s\n' "$*" >>/opt/probe/systemctl-requests.log
    case "$*" in
        daemon-reload) ;;
        'enable --now environmental-demo-stop.timer')
            command systemctl --root=/ enable environmental-demo-stop.timer ;;
        'enable --now docker')
            command systemctl --root=/ enable docker ;;
        *) printf 'Unexpected systemctl request: %s\n' "$*" >&2; return 1 ;;
    esac
}
export -f systemctl
bash /opt/probe/bootstrap.sh
systemd-analyze verify /etc/systemd/system/environmental-demo-stop.service \
    /etc/systemd/system/environmental-demo-stop.timer
