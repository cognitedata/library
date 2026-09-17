"""Copy the shared entity matching code into the functions that deploy it.

A Cognite Function is deployed as one self-contained folder, so code shared by the
submit and collect functions has to exist in both. It is written once, in
``functions/_entity_matching_core``, and copied here.

Run from the repository root after changing anything in that folder:

    python scripts/sync_entity_matching_core.py

``--check`` reports out-of-date copies without writing, which is what CI runs.
"""

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

FUNCTIONS_DIR = REPO_ROOT / "modules" / "contextualization" / "cdf_entity_matching" / "functions"
CORE_DIR = FUNCTIONS_DIR / "_entity_matching_core"

# Shared modules both functions import.
SHARED_MODULES = (
    "em_config.py",
    "em_constants.py",
    "em_job_state.py",
    "em_logger.py",
    "em_pipeline.py",
    "em_pipeline_optimizations.py",
    "em_pipeline_types.py",
    "em_staging.py",
)

# Only copy what each function actually imports. Extra copies inflate the deploy zip
# and give CodeQL two copies of code that never runs in that function.
FUNCTION_MODULES = {
    "fn_dm_context_entity_matching_submit": (*SHARED_MODULES, "em_submit.py", "em_targets.py"),
    "fn_dm_context_entity_matching_collect": (*SHARED_MODULES, "em_collect.py"),
}

BANNER = (
    "# Generated from functions/_entity_matching_core/{name} - do not edit this copy.\n"
    "# Change the source and run: python scripts/sync_entity_matching_core.py\n"
)


def core_modules() -> list[Path]:
    """Every shared module in the core folder, in a stable order."""
    return sorted(path for path in CORE_DIR.glob("*.py") if path.name != "__init__.py")


def modules_for(function_name: str) -> list[Path]:
    """The shared modules copied into one function."""
    return [CORE_DIR / name for name in FUNCTION_MODULES[function_name]]


def rendered(module: Path) -> str:
    """The content a copy of this module should have."""
    return BANNER.format(name=module.name) + module.read_text(encoding="utf-8")


def sync(check_only: bool) -> int:
    """Write, or verify, the copies of the modules each function needs.

    Args:
        check_only: Report stale copies instead of writing them.

    Returns:
        Number of copies that were written, or that are stale in check mode.
    """
    stale = 0
    for function_name, module_names in FUNCTION_MODULES.items():
        target_dir = FUNCTIONS_DIR / function_name
        target_dir.mkdir(parents=True, exist_ok=True)
        expected = set(module_names)
        for extra in target_dir.glob("em_*.py"):
            if extra.name in expected:
                continue
            stale += 1
            if check_only:
                print(f"Unexpected copy: {extra.relative_to(REPO_ROOT)}")
            else:
                extra.unlink()
                print(f"Removed {extra.relative_to(REPO_ROOT)}")
        for module in modules_for(function_name):
            target = target_dir / module.name
            expected_text = rendered(module)
            if target.exists() and target.read_text(encoding="utf-8") == expected_text:
                continue
            stale += 1
            if check_only:
                print(f"Out of date: {target.relative_to(REPO_ROOT)}")
            else:
                target.write_text(expected_text, encoding="utf-8")
                print(f"Wrote {target.relative_to(REPO_ROOT)}")
    return stale


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="fail if a copy is out of date")
    args = parser.parse_args()

    stale = sync(check_only=args.check)
    if args.check and stale:
        print(f"{stale} copy/copies out of date - run: python scripts/sync_entity_matching_core.py")
        sys.exit(1)
    if not args.check:
        print(f"Synced shared modules into {len(FUNCTION_MODULES)} function(s).")


if __name__ == "__main__":
    main()
