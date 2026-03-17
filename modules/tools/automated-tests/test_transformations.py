import unittest

from helper import get_cdf_client


class BasicsTestCase(unittest.TestCase):
    client = None

    def setUp(self):
        if not self.client:
            self.client = get_cdf_client()

    def test_transformations(self):
        jobs = self.client.transformations.jobs.list(limit=1000)
        self.assertTrue(len(jobs) > 0)
        total = len(jobs)
        success = 0
        for job in jobs:
            if job.status in ['Completed', 'Running']:
                success += 1
        self.assertGreater(success, 90, f'Expect at least 90% of the jobs to succeed. [{success} of {total} succeeded]')


if __name__ == '__main__':
    unittest.main()
