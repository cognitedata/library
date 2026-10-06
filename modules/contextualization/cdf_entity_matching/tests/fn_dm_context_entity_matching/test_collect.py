"""Tests for polling, the time budget and the queue order collect works through."""

import itertools
import sys
import threading
import time
import unittest
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2] / "functions" / "fn_dm_context_entity_matching"))

import em_collect  # isort: skip
from em_job_state import PredictJob  # isort: skip
from em_logger import CogniteFunctionLogger  # isort: skip


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
    def test_waits_thirty_seconds_between_polls(self) -> None:
        self.assertEqual(list(itertools.islice(em_collect.poll_intervals(), 5)), [30, 30, 30, 30, 30])


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
        original = em_collect._as_contextualization_job
        em_collect._as_contextualization_job = lambda client, job: stub  # type: ignore[assignment]
        try:
            _, status = em_collect.wait_for_job(None, self.logger, queued_job(), seconds_left)  # type: ignore[arg-type]
        finally:
            em_collect._as_contextualization_job = original  # type: ignore[assignment]
        return stub, status

    def test_waits_thirty_seconds_between_polls(self) -> None:
        _, status = self._wait(["Running", "Running", "Completed"], seconds_left=600)

        self.assertEqual(status, "Completed")
        self.assertEqual(self.slept, [30, 30])

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


class TestDrainJobQueue(unittest.TestCase):
    def test_polls_several_jobs_at_once_instead_of_one_after_another(self) -> None:
        started: list[str] = []
        lock = threading.Lock()
        barrier = threading.Barrier(3)

        def collect_one(job: PredictJob, seconds_left: float) -> em_collect.CollectJobOutcome:
            with lock:
                started.append(job.job_id)
            barrier.wait(timeout=2)
            return em_collect.CollectJobOutcome(job.job_id, "Completed")

        outcomes = em_collect.drain_job_queue(
            [queued_job("1"), queued_job("2"), queued_job("3")],
            collect_one,
            deadline=time.monotonic() + 10,
            max_workers=10,
        )

        self.assertEqual(sorted(started), ["1", "2", "3"])
        self.assertEqual(sorted(o.job_id for o in outcomes), ["1", "2", "3"])

    def test_starts_another_job_when_a_worker_frees_within_the_time_budget(self) -> None:
        currently = 0
        peak = 0
        lock = threading.Lock()
        started: list[str] = []

        def collect_one(job: PredictJob, seconds_left: float) -> em_collect.CollectJobOutcome:
            nonlocal currently, peak
            with lock:
                currently += 1
                peak = max(peak, currently)
                started.append(job.job_id)
            time.sleep(0.05)
            with lock:
                currently -= 1
            return em_collect.CollectJobOutcome(job.job_id, "Completed")

        jobs = [queued_job(str(i)) for i in range(12)]
        outcomes = em_collect.drain_job_queue(
            jobs,
            collect_one,
            deadline=time.monotonic() + 10,
            max_workers=10,
        )

        self.assertEqual(peak, 10)
        self.assertEqual(len(outcomes), 12)
        self.assertEqual({o.job_id for o in outcomes}, {str(i) for i in range(12)})
        self.assertEqual(set(started), {str(i) for i in range(12)})

    def test_does_not_start_further_jobs_after_the_deadline(self) -> None:
        started: list[str] = []
        lock = threading.Lock()

        def collect_one(job: PredictJob, seconds_left: float) -> em_collect.CollectJobOutcome:
            with lock:
                started.append(job.job_id)
            time.sleep(0.2)
            return em_collect.CollectJobOutcome(job.job_id, "Running")

        jobs = [queued_job(str(i)) for i in range(12)]
        em_collect.drain_job_queue(
            jobs,
            collect_one,
            deadline=time.monotonic() + 0.05,
            max_workers=10,
        )

        self.assertLessEqual(len(started), 10)
        self.assertGreaterEqual(len(started), 1)


if __name__ == "__main__":
    unittest.main()
