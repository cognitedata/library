"""Unit tests for module function test helpers."""

import sys
import types
from pathlib import Path

from tests.module_test_support import (
    bind_flat_modules_from_test,
    cognite_sdk_available,
    function_dir_for_test_file,
    is_module_function_test_file,
)


def test_is_module_function_test_file_matches_contextualization_tests() -> None:
    path = Path("modules/contextualization/cdf_entity_matching/tests/fn_dm_context_entity_matching/test_submit.py")
    assert is_module_function_test_file(path)


def test_is_module_function_test_file_ignores_root_tests() -> None:
    assert not is_module_function_test_file(Path("tests/test_foundation_setup_wizard.py"))


def test_function_dir_for_test_file_maps_module_tests_to_function_dir() -> None:
    path = Path("modules/contextualization/cdf_entity_matching/tests/fn_dm_context_entity_matching/test_submit.py")
    assert function_dir_for_test_file(path) == Path(
        "modules/contextualization/cdf_entity_matching/functions/fn_dm_context_entity_matching"
    )


def test_function_dir_for_test_file_keeps_legacy_in_function_dir_layout() -> None:
    path = Path(
        "modules/contextualization/cdf_p_and_id_annotation/functions/fn_dm_context_files_annotation/test_pipeline.py"
    )
    assert function_dir_for_test_file(path) == path.parent


def test_function_dir_for_test_file_returns_none_for_non_module_test() -> None:
    assert function_dir_for_test_file(Path("tests/test_foundation_setup_wizard.py")) is None


def test_cognite_sdk_available_matches_import() -> None:
    try:
        import cognite.client  # noqa: F401
    except ModuleNotFoundError:
        assert cognite_sdk_available() is False
    else:
        assert cognite_sdk_available() is True


def test_bind_flat_modules_from_test_restores_imported_module() -> None:
    """String patches resolve via sys.modules; a later function must not leave the wrong one there."""
    real_handler = types.ModuleType("handler")
    impostor = types.ModuleType("handler")
    sys.modules["handler"] = impostor

    test_module = types.ModuleType("test_example")
    test_module.handler = real_handler

    try:
        bind_flat_modules_from_test(test_module)
        assert sys.modules["handler"] is real_handler
    finally:
        sys.modules.pop("handler", None)
