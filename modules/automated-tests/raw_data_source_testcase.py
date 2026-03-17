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

    # TODO: Remove hard coding of project.
    project = 'cdf-mpc-dev'

    max_count = 400000

    profile_cache = {}

    def setUp(self):
        if not self.client:
            self.client = get_cdf_client()

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

        # Report both missing and expected in the same message as it will stop on first failed assert.
        self.assertEqual(len(actual_only), 0,
                         f'{name}: These entries were not expected: {actual_only}. These entries were missing: {expected_only}')

        self.assertEqual(len(expected_only), 0, f'{name}: These entries were missing: {expected_only}')

    def get_count(self, database, table, limit=None):
        # Sad, but there is no good way to count raw rows.
        # TODO: Check if replacing with a "count(*)" using a transformations preview will work better.

        if limit == None:
            limit = self.max_count

        result = self.client.raw.rows.list(database, table, limit=limit)
        return len(result)

    # Check that the number of rows in a table is within the expected ranges
    def check_count_range(self, database, table, min, max):

        self.assertGreater(self.max_count, min,
                           f'Cannot expect test range to start ({min}) higher than max count ({self.max_count})')

        # Only ask for as many rows as we need to validate range
        limit = None
        if not max:
            limit = min + 1
        else:
            limit = max + 1

        events = self.get_count(database, table, limit)
        msg = f'Count {events} for raw table {database}->{table} is not in expected range ({min},{max})'

        self.assertGreater(events, min, msg)

        if max:
            self.assertLess(events, max, msg)

    def get_profile(self, database, table):
        key = f'{database}#{table}'

        if not key in self.profile_cache:
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

    def check_distribution_count(self, target: TestTargetColumn, min, max):
        profile = self.get_profile(target.database, target.table)
        column = profile['columns'][target.column]

        # Should have a goldilocks number of unique primary classes, not too few, not too many
        self.assertGreater(column['string']['distinctCount'], min)
        self.assertLess(column['string']['distinctCount'], max)


if __name__ == '__main__':
    unittest.main()






