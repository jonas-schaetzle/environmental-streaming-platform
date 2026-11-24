#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
target=bootstrap-probe
prefix=linux_bootstrap
memory=4g
if [[ "${1:-}" == --sizing && $# == 1 ]]; then
    : "${OPENAQ_API_KEY:?Set OPENAQ_API_KEY privately before the live sizing run}"
    target=runtime-sizing
    prefix=runtime_sizing
    memory=6g
elif [[ $# != 0 ]]; then
    printf 'Usage: bash scripts/verify_linux_bootstrap.sh [--sizing]\n' >&2
    exit 2
fi
environment=(--memory "$memory" --memory-swap "$memory")
if [[ "$target" == runtime-sizing ]]; then environment+=(--env OPENAQ_API_KEY); fi
mkdir -p "$root/data/verification"
run="$(mktemp -d "$root/data/verification/${prefix}_XXXXXX")"
context="$run/context"
mkdir -p "$context" "$run/evidence"

# Send only reviewed source files to Docker, never .env, AWS files or runtime data.
cp "$root/infra/aws/bootstrap.sh" "$root/requirements.txt" "$root/pyproject.toml" "$context/"
cp "$root/infra/aws/verification/"* "$context/"
mkdir "$context/config"
cp "$root/config/openaq_locations.json" "$root/config/weather_locations.json" "$context/config/"
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
docker build --platform linux/amd64 --target "$target" --progress plain --tag "$image" "$context" \
    >"$run/build.log" 2>&1
docker image inspect "$image" >"$run/image.json"
# No privileged mode, host Docker socket or existing project data.
destination=/evidence
if [[ "$target" == runtime-sizing ]]; then destination=/opt/project/data; fi
docker run --name "$(basename "$run")" --platform linux/amd64 --cpus 2 \
    "${environment[@]}" \
    --mount "type=bind,source=$run/evidence,target=$destination" "$image" \
    >"$run/probe.log" 2>&1
cat "$run/evidence/report.json"
