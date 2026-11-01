#!/usr/bin/env bash
set -euo pipefail
ARTIFACT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
"${BSI_PYTHON:-python3}" "$ARTIFACT_ROOT/scripts/build_engines.py"
