#!/usr/bin/env bash
set -euo pipefail
ARTIFACT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
PY="${BSI_PYTHON:-python3}"
"$PY" "$ARTIFACT_ROOT/scripts/build_engines.py"
"$PY" "$ARTIFACT_ROOT/scripts/run.py" formats
"$PY" "$ARTIFACT_ROOT/scripts/run.py" engines
