"""
Check that configured RAW tables have row counts within optional min/max.
Configure in test_config.yaml under object_counts. Uses .env only.
Skipped if object_counts is missing or empty.
"""
import unittest

from helper import get_cdf_client, load_test_config
from raw_data_source_testcase import RawDataSourceTestCase


class ObjectCountTestCase(RawDataSourceTestCase):
    """
    Test that RAW tables listed in test_config.yaml object_counts have
    a reasonable number of rows (optional min/max per table).
    """
    client = None
    _config_entries = None

    @classmethod
    def get_entries(cls):
        if cls._config_entries is None:
            config = load_test_config()
            cls._config_entries = config.get("object_counts") or []
        return cls._config_entries

    def setUp(self):
        if not self.get_entries():
            self.skipTest("No object_counts entries in test_config.yaml; add database/table (and optional min/max) to run.")
        if not self.client:
            self.client = get_cdf_client()
        super().setUp()

    def test_object_count_ranges(self):
        for entry in self.get_entries():
            db = entry.get("database")
            table = entry.get("table")
            if not db or not table:
                continue
            min_val = entry.get("min")
            max_val = entry.get("max")
            with self.subTest(database=db, table=table):
                self.check_count_range(db, table, min_val or 0, max_val)


if __name__ == '__main__':
    unittest.main()
