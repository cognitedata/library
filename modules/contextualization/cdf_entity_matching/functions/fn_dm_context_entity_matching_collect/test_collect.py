"""Tests for polling, the time budget and the queue order collect works through."""

import itertools
import sys
import time
import unittest
from pathlib import Path

sys.path.append(str(Path(__file__).parent))

import collect  # isort: skip
from collect import poll_intervals, wait_for_job  # isort: skip
from job_state import PredictJob  # isort: skip
from logger import CogniteFunctionLogger  # isort: skip


class StubJob:
    """Stands in for the SDK job, handing out a prepared sequence of statuses."""

    def __init__(self, statuses: list[str]) -> None:
        self.statuses = iter(statuses)
        self.status = ""
        self.error_message = None
        self.polls = 0

    def update_status(self) -> str:
        self.polls += 1
        self.status = next(self.statuses)
        return self.status


def queued_job(job_id: str = "1001") -> PredictJob:
    return PredictJob(
        job_id=job_id,
        job_token="token",  # noqa: S106 - test double, not a credential
        status="submitted",
        created_at="2026-09-11T10:00:00+00:00",
        staging_prefix=f"pending:{job_id}:",
    )


class TestPollIntervals(unittest.TestCase):
    def test_backs_off_to_thirty_seconds(self) -> None:
        self.assertEqual(list(itertools.islice(poll_intervals(), 5)), [5, 15, 30, 30, 30])


class TestWaitForJob(unittest.TestCase):
    def setUp(self) -> None:
        self.logger = CogniteFunctionLogger("DEBUG")
        self.slept: list[float] = []

        def fake_sleep(seconds: float) -> None:
            self.slept.append(seconds)

        self._real_sleep = time.sleep
        time.sleep = fake_sleep  # type: ignore[assignment]

    def tearDown(self) -> None:
        time.sleep = self._real_sleep  # type: ignore[assignment]

    def _wait(self, statuses: list[str], seconds_left: float) -> tuple[StubJob, str]:
        stub = StubJob(statuses)
        original = collect._as_contextualization_job
        collect._as_contextualization_job = lambda client, job: stub  # type: ignore[assignment]
        try:
            _, status = wait_for_job(None, self.logger, queued_job(), seconds_left)  # type: ignore[arg-type]
        finally:
            collect._as_contextualization_job = original  # type: ignore[assignment]
        return stub, status

    def test_waits_five_then_fifteen_then_thirty_seconds(self) -> None:
        _, status = self._wait(["Running", "Running", "Running", "Completed"], seconds_left=600)

        self.assertEqual(status, "Completed")
        self.assertEqual(self.slept, [5, 15, 30])

    def test_returns_as_soon_as_the_job_is_done(self) -> None:
        stub, status = self._wait(["Completed"], seconds_left=600)

        self.assertEqual(status, "Completed")
        self.assertEqual(stub.polls, 1)
        self.assertEqual(self.slept, [])

    def test_gives_up_when_the_budget_is_spent(self) -> None:
        stub, status = self._wait(["Running"], seconds_left=0)

        self.assertEqual(status, "Running")
        self.assertEqual(stub.polls, 1)
        self.assertEqual(self.slept, [])

    def test_a_failed_job_stops_the_polling(self) -> None:
        stub, status = self._wait(["Running", "Failed"], seconds_left=600)

        self.assertEqual(status, "Failed")
        self.assertEqual(stub.polls, 2)


if __name__ == "__main__":
    unittest.main()
