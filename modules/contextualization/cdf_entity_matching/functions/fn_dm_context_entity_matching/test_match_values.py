"""Which search-property values are handed to the matching model."""

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent))

from em_pipeline import match_values  # isort: skip


def test_match_values_keeps_only_the_longest_list_entry_for_entities() -> None:
    assert match_values(
        {"aliases": ["DB_9101", "23_DB_9101", "DB-9101"]},
        "aliases",
        "23-DB-9101",
        list_selection="longest",
    ) == ["23_DB_9101"]


def test_match_values_keeps_all_list_entries_for_targets() -> None:
    assert match_values(
        {"aliases": ["DB_9101", "23_DB_9101", "DB-9101"]},
        "aliases",
        "23-DB-9101",
        list_selection="all",
    ) == ["DB_9101", "23_DB_9101", "DB-9101"]


def test_match_values_falls_back_to_name_when_the_list_is_empty() -> None:
    assert match_values({"aliases": ["", "  "]}, "aliases", "23-DB-9101") == ["23-DB-9101"]
    assert match_values({"aliases": "23_KA_9101"}, "aliases", "23-KA-9101") == ["23_KA_9101"]
