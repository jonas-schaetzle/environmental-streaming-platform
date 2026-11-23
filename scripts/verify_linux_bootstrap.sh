#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
mkdir -p "$root/data/verification"
run="$(mktemp -d "$root/data/verification/linux_bootstrap_XXXXXX")"
context="$run/context"
mkdir -p "$context" "$run/evidence"

# Send only reviewed source files to Docker, never .env, AWS files or runtime data.
cp "$root/infra/aws/bootstrap.sh" "$root/requirements.txt" "$root/pyproject.toml" "$context/"
cp "$root/infra/aws/verification/"* "$context/"
python3 - "$root/src" "$context/src" <<'PY'
import shutil
import sys
from pathlib import Path

source, target = map(Path, sys.argv[1:])
for path in source.rglob("*.py"):
    destination = target / path.relative_to(source)
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(path, destination)
PY

printf 'Evidence directory: %s\n' "$run"
git -C "$root" rev-parse HEAD >"$run/source-revision.txt"
shasum -a 256 "$root/infra/aws/bootstrap.sh" >"$run/bootstrap.sha256"
image="environmental-streaming-bootstrap:$(basename "$run" | tr '[:upper:]' '[:lower:]')"
docker build --platform linux/amd64 --progress plain --tag "$image" "$context" \
    >"$run/build.log" 2>&1
docker image inspect "$image" >"$run/image.json"
# No privileged mode, host Docker socket, API keys or existing project data.
docker run --name "$(basename "$run")" --platform linux/amd64 --cpus 2 --memory 4g \
    --mount "type=bind,source=$run/evidence,target=/evidence" "$image" \
    >"$run/probe.log" 2>&1
cat "$run/evidence/report.json"
