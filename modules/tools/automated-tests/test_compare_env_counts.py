"""
Compare raw table counts between two CDF projects (e.g. stage vs prod).
Configure database/table pairs in test_config.yaml under compare_env.
Requires .env.TEST and .env.PROD. Skipped if compare_env is missing or empty.
"""
import unittest

from helper import get_cdf_client, load_test_config


class CompareEnvTestCase(unittest.TestCase):
    """
    Validate that different CDF projects (stage vs prod) are close to identical
    in terms of raw object counts. Entries come from test_config.yaml compare_env.
    """
    test_client = None
    prod_client = None
    _config_entries = None

    @classmethod
    def get_entries(cls):
        if cls._config_entries is None:
            config = load_test_config()
            cls._config_entries = config.get("compare_env") or []
        return cls._config_entries

    def setUp(self):
        if not self.get_entries():
            self.skipTest("No compare_env entries in test_config.yaml; add database/table pairs to run.")
        if not self.test_client:
            self.test_client = get_cdf_client('.env.TEST')
        if not self.prod_client:
            self.prod_client = get_cdf_client('.env.PROD')

    def compare(self, database, table):
        test_result = self.test_client.raw.rows.list(database, table, limit=51000)
        prod_result = self.prod_client.raw.rows.list(database, table, limit=51000)
        t1 = len(test_result)
        t2 = len(prod_result)
        self.assertGreater(t1, 0, f"{database}.{table}: test project has 0 rows")
        self.assertGreater(t2, 0, f"{database}.{table}: prod project has 0 rows")
        ratio = t1 / t2
        self.assertGreater(ratio, 0.9, f"{database}.{table}: test/prod ratio {ratio:.2f} < 0.9")
        self.assertLess(ratio, 1.1, f"{database}.{table}: test/prod ratio {ratio:.2f} > 1.1")

    def test_compare_env_tables(self):
        for entry in self.get_entries():
            db = entry.get("database")
            table = entry.get("table")
            if not db or not table:
                continue
            with self.subTest(database=db, table=table):
                self.compare(db, table)


if __name__ == '__main__':
    unittest.main()
