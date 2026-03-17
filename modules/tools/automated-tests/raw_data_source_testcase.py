import os
import unittest
import json

from helper import get_cdf_client


class TestTargetColumn:
    __test__ = False  # This class will not be collected as a test class

    def __init__(self, database, table, column):
        self.database = database
        self.table = table
        self.column = column


class RawDataSourceTestCase(unittest.TestCase):
    client = None
    project = None  # Set from client config or CDF_PROJECT env in setUp
    max_count = 400000
    profile_cache = {}

    def setUp(self):
        if not self.client:
            self.client = get_cdf_client()
        if self.project is None:
            config = getattr(self.client, "config", None)
            self.project = getattr(config, "project", None) if config else None
            if not self.project:
                self.project = os.environ.get("CDF_PROJECT", "")

    def get_profile_result(self, database, table):
        request = {
            "database": database,
            "table": table,
            "limit": 10000  # GUI uses 1000000 as limit, but it seems large?
        }

        result = self.client.post(f"/api/v1/projects/{self.project}/profiler/raw", json=request)

        return json.loads(result.text)

    def compare_expected_equal_lists(self, expected, actual, name):

        expected_columns = set(expected)
        actual_columns = set(actual)

        actual_only = actual_columns - expected_columns
        expected_only = expected_columns - actual_columns

        self.assertEqual(len(actual_only), 0,
                         f'{name}: These entries were not expected: {actual_only}. These entries were missing: {expected_only}')

        self.assertEqual(len(expected_only), 0, f'{name}: These entries were missing: {expected_only}')

    def get_count(self, database, table, limit=None):
        # TODO: Check if replacing with a "count(*)" using a transformations preview will work better.
        if limit is None:
            limit = self.max_count

        result = self.client.raw.rows.list(database, table, limit=limit)
        return len(result)

    def check_count_range(self, database, table, min_val, max_val):

        self.assertGreater(self.max_count, min_val,
                           f'Cannot expect test range to start ({min_val}) higher than max count ({self.max_count})')

        limit = (min_val + 1) if not max_val else (max_val + 1)
        events = self.get_count(database, table, limit)
        msg = f'Count {events} for raw table {database}->{table} is not in expected range ({min_val},{max_val})'

        self.assertGreater(events, min_val, msg)
        if max_val:
            self.assertLess(events, max_val, msg)

    def get_profile(self, database, table):
        key = f'{database}#{table}'
        if key not in self.profile_cache:
            self.profile_cache[key] = self.get_profile_result(database, table)
        return self.profile_cache[key]

    def check_content_presence(self, target: TestTargetColumn, value):

        profile = self.get_profile(target.database, target.table)
        keys = list(profile['columns'].keys())

        self.assertIn(target.column, keys,
                      f'Column {target.column} not found in profile for {target.database} -> {target.table}')

        column = profile['columns'][target.column]
        values = column['string']['valueCounts'][0]
        self.assertIn(value, values)

    def confirm_no_nulls(self, target: TestTargetColumn):
        profile = self.get_profile(target.database, target.table)
        column = profile['columns'][target.column]
        self.assertEqual(column['nullCount'], 0)

    def check_distribution_count(self, target: TestTargetColumn, min_val, max_val):
        profile = self.get_profile(target.database, target.table)
        column = profile['columns'][target.column]
        self.assertGreater(column['string']['distinctCount'], min_val)
        self.assertLess(column['string']['distinctCount'], max_val)


if __name__ == '__main__':
    unittest.main()
