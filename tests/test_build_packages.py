"""Tests for build_packages: the release archive ships packages.toml and the registered modules only."""

import sys
import zipfile
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from build_packages import create_modules_zip

MODULE_TOML = '[module]\ntitle = "Test"\nid = "dp:test:module"\npackage_id = "dp:test"\n'


def _write(path: Path, content: str = "x") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _registry(*packs: tuple[str, list[str]]) -> str:
    """Render a packages.toml with one pack per (key, module paths) pair."""
    lines = ["[library]", 'description = "Test library"', ""]
    for key, modules in packs:
        module_list = ", ".join(f'"{module}"' for module in modules)
        lines += [
            f"[packages.{key}]",
            f'id = "dp:{key}"',
            f'title = "{key}"',
            f'description = "{key} pack"',
            f"modules = [{module_list}]",
            "",
        ]
    return "\n".join(lines)


def _build(modules: Path, tmp_path: Path) -> list[str]:
    output = tmp_path / "out" / "packages.zip"
    output.parent.mkdir()
    create_modules_zip(str(output), modules_dir=modules)
    with zipfile.ZipFile(output) as archive:
        return archive.namelist()


@pytest.fixture
def modules(tmp_path: Path) -> Path:
    root = tmp_path / "modules"
    _write(root / "__init__.py")
    _write(root / "README.md")
    _write(root / "common" / "README.md")
    for module in ("common/cdf_common", "dashboards/legacy"):
        _write(root / module / "module.toml", MODULE_TOML)
        _write(root / module / "README.md")
    _write(root / "contextualization" / "no_module_toml" / "transformations" / "t.Transformation.yaml")
    return root


def test_unregistered_modules_and_root_files_are_left_out(modules: Path, tmp_path: Path) -> None:
    _write(modules / "packages.toml", _registry(("common", ["common/cdf_common"])))

    names = _build(modules, tmp_path)

    assert sorted(names) == ["common/cdf_common/README.md", "common/cdf_common/module.toml", "packages.toml"]


def test_every_file_of_a_registered_module_is_kept(modules: Path, tmp_path: Path) -> None:
    _write(modules / "packages.toml", _registry(("common", ["common/cdf_common"])))
    kept = [
        "common/cdf_common/default.config.yaml",
        "common/cdf_common/functions/fn_a/handler.py",
        "common/cdf_common/functions/fn_a/tests/test_handler.py",
        "common/cdf_common/data/sample.csv",
        "common/cdf_common/.hidden",
    ]
    for name in kept:
        _write(modules / name)
    _write(modules / "common/cdf_common/functions/fn_a/__pycache__/handler.pyc")

    names = _build(modules, tmp_path)

    assert set(kept) <= set(names)
    assert not any("__pycache__" in name for name in names)


def test_a_module_listed_in_two_packs_is_added_once(modules: Path, tmp_path: Path) -> None:
    _write(
        modules / "packages.toml",
        _registry(("common", ["common/cdf_common"]), ("other", ["common/cdf_common", "dashboards/legacy"])),
    )

    names = _build(modules, tmp_path)

    assert len(names) == len(set(names))
    assert "dashboards/legacy/module.toml" in names


def test_extra_resources_outside_registered_modules_are_kept(modules: Path, tmp_path: Path) -> None:
    _write(modules / "packages.toml", _registry(("common", ["common/cdf_common"])))
    _write(modules / "shared" / "one.DataSet.yaml")
    _write(modules / "shared" / "folder" / "two.yaml")
    _write(
        modules / "common/cdf_common/module.toml",
        '[[extra_resources]]\nlocation = "shared/one.DataSet.yaml"\n\n'
        '[[extra_resources]]\nlocation = "shared/folder"\n',
    )

    names = _build(modules, tmp_path)

    assert {"shared/one.DataSet.yaml", "shared/folder/two.yaml"} <= set(names)
    assert "shared/README.md" not in names


def test_a_missing_registered_module_fails_the_build(modules: Path, tmp_path: Path) -> None:
    _write(modules / "packages.toml", _registry(("common", ["common/cdf_common", "common/does_not_exist"])))
    output = tmp_path / "packages.zip"

    with pytest.raises(SystemExit) as exit_info:
        create_modules_zip(str(output), modules_dir=modules)

    assert exit_info.value.code == 1
    assert not output.exists()


def test_a_missing_packages_toml_fails_the_build(modules: Path, tmp_path: Path) -> None:
    with pytest.raises(SystemExit) as exit_info:
        create_modules_zip(str(tmp_path / "packages.zip"), modules_dir=modules)

    assert exit_info.value.code == 1
