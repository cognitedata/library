#!/usr/bin/env bash
# Run from repo root or from this directory. Script dir is used as cwd for pytest.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
current_date=$(date +%Y-%m-%d)
echo 'Launching automated tests... This may take a while.'
mkdir -p test-results
pytest --self-contained-html --html=test-results/test-results-${current_date}.html "$@"
