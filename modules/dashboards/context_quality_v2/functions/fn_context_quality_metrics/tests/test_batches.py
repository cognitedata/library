"""Counting a space in batches (slices of one kind, with `cross` answering what lies outside) gives the numbers of
counting it whole."""

from collections import defaultdict
from typing import Any

import pytest
from cq.config import build_config
from cq.cross import full_cross
from cq.metrics import KINDS, compute
from parity_dataset import build_dataset
from sample_rules import SAMPLE_RULES

CONFIGS = {
    "default": {},
    "tiered": {**SAMPLE_RULES, "tieringEnabled": True},
}


def chunks(items: list[Any], count: int) -> list[list[Any]]:
    return [items[i::count] for i in range(count)]


def summed(parts: list[dict[str, Any]], key: str, width: int) -> dict[Any, list[float]]:
    """Adds the rows of the same grain across batches, as a consumer does, leaving out rows of nothing."""
    total: dict[Any, list[float]] = defaultdict(lambda: [0.0] * width)
    for part in parts:
        for row_key, values in part[key].items():
            vals = values if isinstance(values, list) else [values]
            for i, value in enumerate(vals):
                total[row_key][i] += value
    return {k: v for k, v in total.items() if any(v)}


def batched(config: dict[str, Any], batches_per_kind: int) -> list[dict[str, Any]]:
    cfg = build_config(config)
    parts: list[dict[str, Any]] = []
    for kind in KINDS:
        for piece in chunks(build_dataset()[0][kind], batches_per_kind):
            if not piece:
                continue
            data, latest, now = build_dataset()
            ids = {(entity["space"], entity["id"]) for entity in piece}
            for other in KINDS:
                data[other] = [e for e in data[other] if other == kind and (e["space"], e["id"]) in ids]
            # `cross` is built from the whole space, as the queries would answer for the batch
            parts.append(
                compute(data, cfg, latest, now=now, kinds=frozenset({kind}), cross=full_cross(build_dataset()[0]))  # type: ignore[arg-type]
            )
    return parts


@pytest.mark.parametrize("name", CONFIGS)
@pytest.mark.parametrize("batches_per_kind", [1, 2, 3])
def test_counting_in_batches_gives_the_numbers_of_counting_the_space_whole(name: str, batches_per_kind: int) -> None:
    data, latest, now = build_dataset()
    whole = compute(data, build_config(CONFIGS[name]), latest, now=now)  # type: ignore[arg-type]
    parts = batched(CONFIGS[name], batches_per_kind)

    assert summed(parts, "agg", 4) == summed([whole], "agg", 4)
    assert summed(parts, "exclusions", 1) == summed([whole], "exclusions", 1)
    assert summed(parts, "classifications", 2) == summed([whole], "classifications", 2)
    assert summed(parts, "annotation_buckets", 2) == summed([whole], "annotation_buckets", 2)
    listed = sorted(map(str, (item for part in parts for item in part["drilldown"])))
    assert listed == sorted(map(str, whole["drilldown"]))


def test_a_batch_only_counts_the_kinds_it_is_responsible_for() -> None:
    data, latest, now = build_dataset()

    result = compute(data, build_config({}), latest, now=now, kinds=frozenset({"equipment"}))  # type: ignore[arg-type]

    dimensions = {key[1] for key in result["agg"]}
    assert dimensions == {"equipment-asset"}
    assert {item["dim"] for item in result["drilldown"]} == {"equipment-asset"}
    assert {key[0] for key in result["classifications"]} == {"equipmentType"}
