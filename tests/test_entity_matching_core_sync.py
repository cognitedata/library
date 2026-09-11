"""The shared entity matching code is copied, not edited, in the function folders."""

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "scripts"))

from sync_entity_matching_core import (
    CORE_DIR,
    FUNCTIONS_DIR,
    TARGET_FUNCTIONS,
    core_modules,
    rendered,
    sync,
)


def test_core_modules_are_copied_into_every_function() -> None:
    assert core_modules(), f"no shared modules found in {CORE_DIR}"
    for function_name in TARGET_FUNCTIONS:
        for module in core_modules():
            copy = FUNCTIONS_DIR / function_name / module.name
            assert copy.is_file(), f"missing copy: {copy.relative_to(REPO_ROOT)}"


def test_copies_match_the_source() -> None:
    assert sync(check_only=True) == 0, "run: python scripts/sync_entity_matching_core.py"


def test_copy_is_marked_as_generated() -> None:
    module = next(iter(core_modules()))
    assert rendered(module).startswith("# Generated from functions/_entity_matching_core/")
