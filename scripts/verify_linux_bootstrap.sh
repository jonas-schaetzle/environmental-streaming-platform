#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
target=bootstrap-probe
prefix=linux_bootstrap
memory=4g
archive=""
if [[ "${1:-}" == --sizing && $# == 1 ]]; then
    : "${OPENAQ_API_KEY:?Set OPENAQ_API_KEY privately before the live sizing run}"
    target=runtime-sizing
    prefix=runtime_sizing
    memory=6g
elif [[ "${1:-}" == --restart && $# == 2 ]]; then
    archive="$(cd "$2" && pwd -P)"
    case "$archive" in
        "$root"/data/verification/*/evidence) ;;
        *) printf 'Use a retained sizing evidence directory under data/verification/.\n' >&2; exit 2 ;;
    esac
    target=runtime-restart
    prefix=linux_restart
    memory=6g
elif [[ $# != 0 ]]; then
    printf 'Usage: bash scripts/verify_linux_bootstrap.sh [--sizing | --restart EVIDENCE]\n' >&2
    exit 2
fi
environment=(--memory "$memory" --memory-swap "$memory")
if [[ "$target" == runtime-sizing ]]; then environment+=(--env OPENAQ_API_KEY); fi
mkdir -p "$root/data/verification"
run="$(mktemp -d "$root/data/verification/${prefix}_XXXXXX")"
context="$run/context"
mkdir -p "$context" "$run/evidence"
if [[ -n "$archive" ]]; then
    # Never copy a running broker as a consistent recovery source.
    running="$(docker ps --filter "name=^/$(basename "$(dirname "$archive")")$" --format '{{.Names}}')"
    [[ -z "$running" ]] || { printf 'Stop the source broker before copying.\n' >&2; exit 2; }
    python3 - "$archive" "$run" <<'PY'
import hashlib
import json
import shutil
import sys
from pathlib import Path

source, run = map(Path, sys.argv[1:])
report = json.loads((source / "report.json").read_text())
assert all(phase["exit_code"] == 0 for phase in report["phases"])
names = ("kafka", "warehouse", "checkpoints", "state", "broker.properties")
manifest = {}
for name in names:
    path = source / name
    assert path.exists(), f"Missing recovery component: {name}"
    assert not path.is_symlink(), "Recovery source must not contain symlinks."
    for item in ([path] if path.is_file() else sorted(path.rglob("*"))):
        assert not item.is_symlink(), "Recovery source must not contain symlinks."
        if item.is_file():
            manifest[str(item.relative_to(source))] = hashlib.sha256(item.read_bytes()).hexdigest()
    destination = run / "evidence" / name
    if path.is_dir():
        shutil.copytree(path, destination)
    else:
        shutil.copy2(path, destination)
for name, digest in manifest.items():
    assert hashlib.sha256((run / "evidence" / name).read_bytes()).hexdigest() == digest
    assert hashlib.sha256((source / name).read_bytes()).hexdigest() == digest
(run / "archive-manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
PY
fi

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
# No privileged mode, host Docker socket or original data mount.
destination=/evidence
if [[ "$target" != bootstrap-probe ]]; then destination=/opt/project/data; fi
docker run --name "$(basename "$run")" --platform linux/amd64 --cpus 2 \
    "${environment[@]}" \
    --mount "type=bind,source=$run/evidence,target=$destination" "$image" \
    >"$run/probe.log" 2>&1
if [[ -n "$archive" ]]; then
    python3 - "$archive" "$run/archive-manifest.json" <<'PY'
import hashlib
import json
import sys
from pathlib import Path

source, manifest = map(Path, sys.argv[1:])
for name, digest in json.loads(manifest.read_text()).items():
    assert hashlib.sha256((source / name).read_bytes()).hexdigest() == digest, name
print("Original recovery files unchanged.")
PY
fi
cat "$run/evidence/report.json"
