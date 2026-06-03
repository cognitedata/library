#!/usr/bin/env bash
# Regenerate Toolkit environment configs before cdf build/deploy (Foundation Deployment Pack).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ORG_DIR="${ROOT}/{{ORG_DIR}}"
GEN="${ORG_DIR}/scripts/generate_env_configs.py"

if [[ ! -f "${GEN}" ]]; then
  echo "Missing ${GEN} — run generate_actions.py first."
  exit 1
fi

python3 "${GEN}" \
  --enterprise "{{ENTERPRISE}}" \
  --org-dir "{{ORG_DIR}}" \
  --repo-root "${ROOT}"
