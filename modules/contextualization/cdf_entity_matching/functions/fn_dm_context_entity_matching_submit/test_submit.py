"""Tests for the predict job queue and the staging submit hands to collect."""

import sys
import unittest
from pathlib import Path

sys.path.append(str(Path(__file__).parent))

from cognite.client.data_classes import Row  # isort: skip

from config import Config, ConfigData, Parameters, ViewPropertyConfig  # isort: skip
from constants import STAT_STORE_MATCH_MODEL_ID, STAT_STORE_VALUE  # isort: skip
from job_state import append_predict_job, job_row_key, list_predict_jobs  # isort: skip
from logger import CogniteFunctionLogger  # isort: skip
from staging import (  # isort: skip
    clear_finished_matches,
    read_staged_matches,
    staging_prefix,
    write_staged_matches,
)


class FakeRowsAPI:
    def __init__(self) -> None:
        self.tables: dict[tuple[str, str], dict[str, dict]] = {}

    def _table(self, db: str, table: str) -> dict[str, dict]:
        return self.tables.setdefault((db, table), {})

    def insert(self, db_name: str, table_name: str, row: Row) -> None:
        self._table(db_name, table_name)[row.key] = dict(row.columns or {})

    def list(self, db_name: str, table_name: str, columns=None, limit=None) -> list[Row]:
        rows = self._table(db_name, table_name)
        if columns == []:
            return [Row(key, {}) for key in rows]
        return [Row(key, dict(values)) for key, values in rows.items()]

    def delete(self, db_name: str, table_name: str, key) -> None:
        keys = [key] if isinstance(key, str) else key
        table = self._table(db_name, table_name)
        for row_key in keys:
            table.pop(row_key, None)


class FakeCreateAPI:
    def create(self, *args, **kwargs) -> None:
        return None


class FakeRawAPI:
    def __init__(self) -> None:
        self.rows = FakeRowsAPI()
        self.databases = FakeCreateAPI()
        self.tables = FakeCreateAPI()


class FakeClient:
    def __init__(self) -> None:
        self.raw = FakeRawAPI()


class FakeUploadQueue:
    def __init__(self, client: FakeClient) -> None:
        self.client = client
        self.queued: list[tuple[str, str, Row]] = []

    def add_to_upload_queue(self, db: str, table: str, row: Row) -> None:
        self.queued.append((db, table, row))

    def upload(self) -> None:
        for db, table, row in self.queued:
            self.client.raw.rows.insert(db, table, row)
        self.queued = []


def build_config() -> Config:
    view = ViewPropertyConfig(
        schemaSpace="cdf_cdm",
        instanceSpace="inst_location",
        externalId="CogniteTimeSeries",
        version="v1",
    )
    return Config(
        parameters=Parameters(
            debug=False,
            dmUpdate=False,
            runAll=False,
            removeOldLinks=False,
            rawDb="db",
            rawTableState="state",
            rawTableCtxGood="good",
            rawTableCtxBad="bad",
            autoApprovalThreshold=0.85,
        ),
        data=ConfigData(entityView=view, targetView=view),
    )


class TestPredictJobQueue(unittest.TestCase):
    def setUp(self) -> None:
        self.client = FakeClient()
        self.config = build_config()
        self.logger = CogniteFunctionLogger("DEBUG")

    def _append(self, job_id: str) -> None:
        append_predict_job(
            self.client,  # type: ignore[arg-type]
            self.config,
            self.logger,
            job_id=job_id,
            job_token=f"token-{job_id}",
            staging_prefix=staging_prefix(job_id),
        )

    def test_each_job_gets_its_own_row(self) -> None:
        self._append("1001")
        self._append("1002")

        rows = self.client.raw.rows.tables[("db", "state")]
        self.assertIn(job_row_key("1001"), rows)
        self.assertIn(job_row_key("1002"), rows)

    def test_submitting_does_not_overwrite_a_queued_job(self) -> None:
        self._append("1001")
        first = dict(self.client.raw.rows.tables[("db", "state")][job_row_key("1001")])

        self._append("1002")

        self.assertEqual(self.client.raw.rows.tables[("db", "state")][job_row_key("1001")], first)

    def test_model_id_row_is_not_a_job(self) -> None:
        self.client.raw.rows.insert("db", "state", Row(STAT_STORE_MATCH_MODEL_ID, {STAT_STORE_VALUE: "42"}))
        self._append("1001")

        jobs = list_predict_jobs(self.client, self.config, self.logger)  # type: ignore[arg-type]

        self.assertEqual([job.job_id for job in jobs], ["1001"])

    def test_jobs_are_returned_oldest_first(self) -> None:
        for job_id, created_at in (("1003", "2026-09-11T10:00:02"), ("1001", "2026-09-11T10:00:00")):
            self.client.raw.rows.insert(
                "db",
                "state",
                Row(
                    job_row_key(job_id),
                    {
                        "jobId": job_id,
                        "jobToken": "token",
                        "status": "submitted",
                        "createdAt": created_at,
                        "stagingPrefix": staging_prefix(job_id),
                    },
                ),
            )

        jobs = list_predict_jobs(self.client, self.config, self.logger)  # type: ignore[arg-type]

        self.assertEqual([job.job_id for job in jobs], ["1001", "1003"])


class TestStaging(unittest.TestCase):
    def setUp(self) -> None:
        self.client = FakeClient()
        self.config = build_config()
        self.logger = CogniteFunctionLogger("DEBUG")
        self.uploader = FakeUploadQueue(self.client)

    def test_staged_matches_round_trip(self) -> None:
        matches = [
            {"entity_ext_id": "TS-1", "asset_ext_id": "A-1", "match_type": "Manual Mapping"},
            {"entity_ext_id": "TS-2", "asset_ext_id": "A-2", "match_type": "Rule Based Mapping"},
        ]

        write_staged_matches(self.client, self.config, self.uploader, self.logger, "1001", matches)  # type: ignore[arg-type]
        restored = read_staged_matches(self.client, self.config, self.logger, "1001")  # type: ignore[arg-type]

        self.assertEqual(sorted(restored, key=lambda m: m["entity_ext_id"]), matches)

    def test_staged_matches_of_another_job_are_not_read(self) -> None:
        write_staged_matches(self.client, self.config, self.uploader, self.logger, "1001", [{"entity_ext_id": "TS-1"}])  # type: ignore[arg-type]
        write_staged_matches(self.client, self.config, self.uploader, self.logger, "1002", [{"entity_ext_id": "TS-2"}])  # type: ignore[arg-type]

        self.assertEqual(read_staged_matches(self.client, self.config, self.logger, "1002"), [{"entity_ext_id": "TS-2"}])  # type: ignore[arg-type]

    def test_run_all_clears_results_but_keeps_staged_matches(self) -> None:
        self.client.raw.rows.insert("db", "good", Row("TS-9", {"entity_ext_id": "TS-9"}))
        write_staged_matches(self.client, self.config, self.uploader, self.logger, "1001", [{"entity_ext_id": "TS-1"}])  # type: ignore[arg-type]

        clear_finished_matches(self.client, self.config, self.logger, "good")  # type: ignore[arg-type]

        remaining = list(self.client.raw.rows.tables[("db", "good")])
        self.assertEqual(remaining, [f"{staging_prefix('1001')}TS-1"])


if __name__ == "__main__":
    unittest.main()
