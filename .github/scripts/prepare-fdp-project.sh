#!/usr/bin/env bash
# Link library modules/ into the FDP organization directory for cdf build/deploy.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
FDP_DIR="${ROOT}/foundation-deployment-pack"
MODULES_SRC="${ROOT}/modules"
MODULES_LINK="${FDP_DIR}/modules"

mkdir -p "${FDP_DIR}"

if [[ -L "${MODULES_LINK}" ]]; then
  echo "modules symlink already present: ${MODULES_LINK}"
elif [[ -d "${MODULES_LINK}" ]]; then
  echo "modules directory already exists at ${MODULES_LINK} (not replacing)"
else
  ln -sfn "${MODULES_SRC}" "${MODULES_LINK}"
  echo "Linked ${MODULES_LINK} -> ${MODULES_SRC}"
fi

# Regenerate environment configs from module default.config.yaml files.
if ! python3 "${FDP_DIR}/scripts/generate_env_configs.py"; then
  echo "Config generation skipped (install PyYAML to regenerate configs locally)."
fi
