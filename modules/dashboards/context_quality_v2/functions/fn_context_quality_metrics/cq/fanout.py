"""Splitting one run into shards that run in parallel, and putting what they report back together.

A run is three steps of one workflow: `plan` decides the shards and starts the run, a dynamic task calls the function
once per shard (`compute_shard`), and `finalize` writes the complete marker. A shard holds whole facility spaces, so
every counted row (its reporting unit is its instance space) belongs to exactly one shard and shards never write the
same record.
"""

import json
import math
from typing import Any

from cq.batching import chain_tasks

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
    """A workflow task definition that runs one piece of the run. `retries` is 0: a piece writes records, which are
    immutable, so a second attempt could not write the same rows again; a failed piece fails the run and the run is
    started again."""
    if data["action"] == "compute_batch":
        what = ", ".join([*data["kinds"], *(view for _, view in data["views"])])
        name = f"Context Quality batch {index + 1} of {count}: {data['space']} ({what})"
        description = "Counts one slice of a big facility space and appends its rows to the run."
    else:
        name = f"Context Quality shard {index + 1} of {count}"
        description = "Counts the metrics of the facility spaces of one shard and appends them to the run."
    return {
        "externalId": f"shard_{index:03d}",
        "type": "function",
        "name": name,
        "description": description,
        "parameters": {"function": {"externalId": function_external_id, "data": data}},
        "retries": 0,
        "timeout": 600,
        "onFailure": "abortWorkflow",
        "dependsOn": [],
    }


def finish_tasks(
    items: list[dict[str, Any]], function_external_id: str, common: dict[str, Any], lanes: int
) -> list[dict[str, Any]]:
    """The tasks of a run from its work items (cq.planning), numbered in order, at most `lanes` running at once."""
    count = len(items)
    tasks = [
        shard_task(index, count, function_external_id, {**item["data"], "shard": index, "shardCount": count, **common})
        for index, item in enumerate(items)
    ]
    return chain_tasks(tasks, lanes)


def tasks_fit(tasks: list[dict[str, Any]]) -> bool:
    """Whether the tasks, which `plan` returns as its output, stay within the limit on a task's output."""
    return len(json.dumps(tasks, separators=(",", ":"))) <= MAX_TASKS_BYTES


def merge_warnings(parts: list[list[dict[str, Any]]]) -> list[dict[str, Any]]:
    """The run's warnings from the warnings of its pieces, one list each.

    Warnings with the same code, subject and reason are one warning; counts in them add up and the listed type codes are
    pooled (at most ten).
    """
    merged: dict[tuple[str, str, str], dict[str, Any]] = {}
    codes: dict[tuple[str, str, str], set[str]] = {}
    for warnings in parts:
        for warning in warnings:
            if warning["code"] == "warnings-truncated":
                continue
            key = (warning["code"], warning.get("subject") or "", warning.get("reason") or "")
            if key not in merged:
                merged[key] = dict(warning)
                codes[key] = set()
            else:
                for name in _SUMMED:
                    if name in warning:
                        merged[key][name] = merged[key].get(name, 0) + warning[name]
                if "limit" in warning:
                    merged[key]["limit"] = max(merged[key].get("limit", 0), warning["limit"])
            codes[key].update(warning.get("codes", []))
    for key, warning in merged.items():
        if "codes" in warning:
            warning["codes"] = sorted(codes[key])[:MAX_LISTED_CODES]
    return sorted(merged.values(), key=lambda w: (w["code"], w.get("subject") or "", w.get("reason") or ""))
