"""Splitting one run into shards that run in parallel, and putting what they report back together.

A run is three steps of one workflow: `plan` decides the shards and starts the run, a dynamic task calls the function
once per shard (`compute_shard`), and `finalize` writes the complete marker. A shard holds whole facility spaces, so
every counted row (its reporting unit is its instance space) belongs to exactly one shard and shards never write the
same record.
"""

import json
import math
from typing import Any

# What `plan` returns is a task's output, which a workflow limits to 0.2 MB; stay well below it.
MAX_TASKS_BYTES = 150_000

# Warnings that say how many of something there was: the shards' counts add up.
_SUMMED = ("assets", "dropped", "unmapped", "units", "untyped")
MAX_LISTED_CODES = 10


def pack_spaces(weights: dict[str, int], max_shards: int, target: int) -> list[list[str]]:
    """Every space into exactly one shard. -> the spaces of each shard, sorted, shards in a fixed order.

    The number of shards is what `target` entities per shard asks for, but at most `max_shards` and at most one per
    space. Spaces go largest first into the lightest shard, so the shards come out about equally heavy.
    """
    if not weights:
        return []
    count = min(max_shards, len(weights), max(1, math.ceil(sum(weights.values()) / target)))
    shards: list[list[str]] = [[] for _ in range(count)]
    loads = [0] * count
    for space, weight in sorted(weights.items(), key=lambda item: (-item[1], item[0])):
        lightest = loads.index(min(loads))
        shards[lightest].append(space)
        loads[lightest] += weight
    return sorted((sorted(spaces) for spaces in shards), key=lambda spaces: spaces[0])


def shard_task(index: int, count: int, function_external_id: str, data: dict[str, Any]) -> dict[str, Any]:
    """A workflow task definition that runs one shard. `retries` is 0: a shard writes records, which are immutable, so a
    second attempt could not write the same rows again; a failed shard fails the run and the run is started again."""
    return {
        "externalId": f"shard_{index:03d}",
        "type": "function",
        "name": f"Context Quality shard {index + 1} of {count}",
        "description": "Counts the metrics of the facility spaces of one shard and appends them to the run.",
        "parameters": {"function": {"externalId": function_external_id, "data": data}},
        "retries": 0,
        "timeout": 600,
        "onFailure": "abortWorkflow",
        "dependsOn": [],
    }


def build_tasks(
    shards_of: dict[int, list[list[str]]], function_external_id: str, make_data: Any
) -> list[dict[str, Any]]:
    """The task definitions for the first packing whose JSON fits the task output limit.

    `shards_of` maps a shard count to its packing, largest count first; `make_data(index, count, spaces)` is the data one
    shard's task carries. Fewer, larger shards are used when many tasks, each carrying the configuration, would not fit.
    """
    for count in sorted(shards_of, reverse=True):
        packing = shards_of[count]
        tasks = [
            shard_task(index, count, function_external_id, make_data(index, count, spaces))
            for index, spaces in enumerate(packing)
        ]
        if len(json.dumps(tasks, separators=(",", ":"))) <= MAX_TASKS_BYTES:
            return tasks
    raise ValueError(
        "The configuration is too large to pass to even one shard: a task's data may not exceed the workflow's "
        "limit on task output. Shorten the configuration file."
    )


def merge_warnings(parts: list[tuple[bool, list[dict[str, Any]]]]) -> list[dict[str, Any]]:
    """The run's warnings from the shards' warnings. Each part is (computed the built-in metrics, its warnings).

    Warnings with the same code, subject and reason are one warning; counts in them add up and the listed type codes are
    pooled. `no-source-entities` is the exception: it says nothing of that kind exists, which is only true when every
    shard that counted the built-in metrics found none.
    """
    merged: dict[tuple[str, str, str], dict[str, Any]] = {}
    codes: dict[tuple[str, str, str], set[str]] = {}
    seen_by: dict[tuple[str, str, str], int] = {}
    for _computed, warnings in parts:
        for warning in warnings:
            if warning["code"] == "warnings-truncated":
                continue
            key = (warning["code"], warning.get("subject") or "", warning.get("reason") or "")
            if key not in merged:
                merged[key] = dict(warning)
                codes[key] = set()
                seen_by[key] = 0
            else:
                for name in _SUMMED:
                    if name in warning:
                        merged[key][name] = merged[key].get(name, 0) + warning[name]
                if "limit" in warning:
                    merged[key]["limit"] = max(merged[key].get("limit", 0), warning["limit"])
            seen_by[key] += 1
            codes[key].update(warning.get("codes", []))
    computing = sum(1 for computed, _ in parts if computed)
    out: list[dict[str, Any]] = []
    for key, warning in merged.items():
        if key[0] == "no-source-entities" and seen_by[key] < computing:
            continue
        if "codes" in warning:
            warning["codes"] = sorted(codes[key])[:MAX_LISTED_CODES]
        out.append(warning)
    return sorted(out, key=lambda w: (w["code"], w.get("subject") or "", w.get("reason") or ""))
