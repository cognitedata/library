"""Tests for deploy_kea_functions_cdf_api (hash + metadata helpers)."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_PKG = Path(__file__).resolve().parents[3]
_SCRIPTS = _PKG / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from deploy_kea_functions_cdf_api import _materialize_staging, _sanitize_metadata, kea_function_source_hash


def test_sanitize_metadata_drops_toolkit_placeholders() -> None:
    assert _sanitize_metadata({"version": "{{key_extraction_function_version}}", "ok": "1"}) == {"ok": "1"}
    assert _sanitize_metadata(None) is None
    assert _sanitize_metadata({}) is None


def test_kea_function_source_hash_stable(tmp_path: Path) -> None:
    root = tmp_path / "functions"
    (root / "cdf_fn_common").mkdir(parents=True)
    (root / "cdf_fn_common" / "mod.py").write_text("x", encoding="utf-8")
    (root / "fn_dm_demo").mkdir(parents=True)
    (root / "fn_dm_demo" / "handler.py").write_text("def handle():\n    pass\n", encoding="utf-8")
    h1 = kea_function_source_hash(root, "fn_dm_demo")
    h2 = kea_function_source_hash(root, "fn_dm_demo")
    assert h1 == h2
    assert len(h1) == 64


def test_kea_function_source_hash_changes_on_edit(tmp_path: Path) -> None:
    root = tmp_path / "functions"
    (root / "cdf_fn_common").mkdir(parents=True)
    (root / "cdf_fn_common" / "shared.py").write_text("v1", encoding="utf-8")
    (root / "fn_dm_demo").mkdir(parents=True)
    (root / "fn_dm_demo" / "handler.py").write_text("def handle():\n    pass\n", encoding="utf-8")
    h1 = kea_function_source_hash(root, "fn_dm_demo")
    (root / "fn_dm_demo" / "handler.py").write_text("def handle():\n    return 1\n", encoding="utf-8")
    h2 = kea_function_source_hash(root, "fn_dm_demo")
    assert h1 != h2


def test_kea_function_source_hash_missing_fn_raises(tmp_path: Path) -> None:
    root = tmp_path / "functions"
    (root / "cdf_fn_common").mkdir(parents=True)
    (root / "cdf_fn_common" / "x.py").write_text("1", encoding="utf-8")
    with pytest.raises(FileNotFoundError):
        kea_function_source_hash(root, "fn_dm_missing")


def test_materialize_staging_copies_requirements_to_zip_root(tmp_path: Path) -> None:
    root = tmp_path / "functions"
    (root / "cdf_fn_common").mkdir(parents=True)
    (root / "cdf_fn_common" / "mod.py").write_text("x", encoding="utf-8")
    (root / "fn_dm_demo").mkdir(parents=True)
    (root / "fn_dm_demo" / "handler.py").write_text("h", encoding="utf-8")
    (root / "fn_dm_demo" / "requirements.txt").write_text("PyYAML>=6.0\n", encoding="utf-8")
    stage = tmp_path / "stage"
    _materialize_staging(root, "fn_dm_demo", stage)
    assert (stage / "requirements.txt").read_text(encoding="utf-8") == "PyYAML>=6.0\n"
    assert not (stage / "fn_dm_demo" / "requirements.txt").exists()
