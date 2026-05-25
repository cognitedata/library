"""Create, run, and delete short-lived CDF transformations for local Spark nodes."""

from __future__ import annotations

import re
from typing import Any, Dict, Optional


def _sanitize_external_id_part(value: str, *, max_len: int = 48) -> str:
    part = re.sub(r"[^a-zA-Z0-9_]", "_", value.strip()).lower()
    part = re.sub(r"_+", "_", part).strip("_")
    return (part or "task")[:max_len]


def ephemeral_transformation_external_id(*, task_id: str, run_id: str) -> str:
    """``tr_<task>__local__<run>`` — unique per local pipeline run."""
    task_part = _sanitize_external_id_part(task_id)
    run_part = _sanitize_external_id_part(run_id.replace(".", "_"), max_len=64)
    ext = f"tr_{task_part}__local__{run_part}"
    return ext[:255]


def _coerce_destination(raw: Any) -> Any:
    from cognite.client.data_classes import TransformationDestination

    if raw is None:
        return TransformationDestination.raw("etl_staging", "local_spark_out")
    if isinstance(raw, TransformationDestination):
        return raw
    if not isinstance(raw, dict):
        return TransformationDestination.raw("etl_staging", "local_spark_out")

    type_raw = str(raw.get("type") or raw.get("destinationType") or "").strip().lower()
    if type_raw in ("raw", "rawtable"):
        db = str(raw.get("database") or raw.get("databaseName") or raw.get("raw_db") or "etl_staging")
        table = str(raw.get("table") or raw.get("tableName") or raw.get("raw_table") or "local_spark_out")
        return TransformationDestination.raw(db, table)
    if type_raw in ("asset", "assets"):
        return TransformationDestination.assets()
    if type_raw in ("event", "events"):
        return TransformationDestination.events()
    if type_raw in ("file", "files"):
        return TransformationDestination.files()
    if type_raw in ("timeseries", "time_series"):
        return TransformationDestination.time_series()
    if type_raw in ("sequence", "sequences"):
        return TransformationDestination.sequences()
    if type_raw in ("relationship", "relationships"):
        return TransformationDestination.relationships()
    return TransformationDestination.raw("etl_staging", "local_spark_out")


def run_ephemeral_transformation(
    client: Any,
    *,
    query: str,
    destination: Optional[Dict[str, Any]] = None,
    external_id: str,
    timeout_sec: float = 600.0,
    log: Any = None,
) -> Dict[str, Any]:
    """Create transformation, run to completion, then delete it."""
    if client is None:
        raise RuntimeError("CDF client required to run Spark transformation locally")

    sql = str(query or "").strip()
    if not sql:
        raise ValueError("Spark transformation query is empty")

    from cognite.client.data_classes import TransformationWrite

    created = None
    try:
        tw = TransformationWrite(
            external_id=external_id,
            name=f"local-{external_id}"[:256],
            query=sql,
            destination=_coerce_destination(destination),
            ignore_null_fields=True,
        )
        created = client.transformations.create(tw)
        job = client.transformations.run(
            transformation_id=created.id,
            wait=True,
            timeout=timeout_sec,
        )
        status = str(getattr(job, "status", "") or "").upper()
        if status == "FAILED":
            err = str(getattr(job, "error", "") or "transformation job failed")
            raise RuntimeError(err)
        if log is not None:
            log.info(
                "Ephemeral transformation %s finished with status %s (job_id=%s)",
                external_id,
                status or "unknown",
                getattr(job, "id", None),
            )
        return {
            "status": status.lower() if status else "completed",
            "transformation_external_id": external_id,
            "job_id": getattr(job, "id", None),
        }
    finally:
        if created is not None:
            try:
                client.transformations.delete(id=created.id, ignore_unknown_ids=True)
            except Exception as exc:
                if log is not None:
                    log.warning("Could not delete ephemeral transformation %s: %s", external_id, exc)
