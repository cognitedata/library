"""Unit tests for module function test helpers."""

from pathlib import Path

from tests.module_test_support import (
    cognite_sdk_available,
    function_dir_for_test_file,
    is_module_function_test_file,
)


def test_is_module_function_test_file_matches_contextualization_tests() -> None:
    path = Path(
        "modules/contextualization/cdf_entity_matching/functions/"
        "fn_dm_context_timeseries_entity_matching/test_handler.py"
    )
    assert is_module_function_test_file(path)


def test_is_module_function_test_file_ignores_root_tests() -> None:
    assert not is_module_function_test_file(Path("tests/test_foundation_setup_wizard.py"))


def test_function_dir_for_oid_sync_test() -> None:
    path = Path("modules/sourcesystem/cdf_oid_sync/test_oid_sync.py")
    assert function_dir_for_test_file(path) == Path("modules/sourcesystem/cdf_oid_sync/functions/fn_oid_sync")


def test_cognite_sdk_available_matches_import() -> None:
    try:
        import cognite.client  # noqa: F401
    except ModuleNotFoundError:
        assert cognite_sdk_available() is False
    else:
        assert cognite_sdk_available() is True
