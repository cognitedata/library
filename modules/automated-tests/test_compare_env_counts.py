import unittest

from helper import get_cdf_client

# TODO: This was a very early test, so may have been replaced by something?
# At least we should split out the test case shared logic from the actual test cases.

class CompareEnvTestCase(unittest.TestCase):
    """
        Validate that different CDF projects (stage vs prod) are close to identical in terms of raw object counts.
    """
    test_client = None
    prod_client = None

    def setUp(self):
        if not self.test_client:
            self.test_client = get_cdf_client('.env.TEST')

        if not self.prod_client:
            self.prod_client = get_cdf_client('.env.PROD')

    def compare(self, database, table):

        # TODO: Must be a better way to count than dump all? So for now we have a low threshold as otherwise things will
        # run very slowly
        test_result = self.test_client.raw.rows.list(database, table, limit=51000)
        prod_result = self.prod_client.raw.rows.list(database, table, limit=51000)

        t1 = len(test_result)
        t2 = len(prod_result)

        self.assertGreater(t1, 0)
        self.assertGreater(t2, 0)

        diff = abs(t1 / t2)

        self.assertGreater(diff, 0.9)
        self.assertLess(diff, 1.1)

    def test_DocLib(self):
        self.compare("db_dl_files_gvl", "tbl_gvlrs253_doclib_gvl")

    def test_iTAR_events(self):
        self.compare("db_indp_itar_gvl", "tbl_indp_event_gvl")

    def test_audit_Salus(self):
        self.compare("db_salus_spec_sheet_gvl", "tbl_salus_u10_gvl")

    def test_audit_SI(self):
        self.compare("db_si_spec_sheet_gvl", "tbl_si_spec_sheet_gvl")


if __name__ == '__main__':
    unittest.main()






