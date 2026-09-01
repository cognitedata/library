"""Tests for resolving instance spaces when writing matches back to the data model."""

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent))

from pipeline import remember_link_spaces


def test_existing_link_keeps_its_own_space() -> None:
    """A link to a target outside the current target set must not be moved."""
    target_spaces: dict[str, str] = {}

    remember_link_spaces(target_spaces, [{"space": "inst_asset_sap", "externalId": "23-KA-9101"}])

    assert target_spaces == {"23-KA-9101": "inst_asset_sap"}


def test_space_from_target_view_wins_over_link() -> None:
    """The target query is authoritative; a stale link must not overwrite it."""
    target_spaces = {"23-KA-9101": "inst_asset_workmate"}

    remember_link_spaces(target_spaces, [{"space": "inst_asset_sap", "externalId": "23-KA-9101"}])

    assert target_spaces == {"23-KA-9101": "inst_asset_workmate"}


def test_incomplete_links_are_ignored() -> None:
    """Links without both space and externalId carry no usable space."""
    target_spaces: dict[str, str] = {}

    remember_link_spaces(target_spaces, [{"externalId": "23-KA-9101"}, {"space": "inst_asset_sap"}])

    assert target_spaces == {}
