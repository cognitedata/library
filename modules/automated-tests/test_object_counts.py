import unittest

from helper import get_cdf_client
from raw_data_source_testcase import RawDataSourceTestCase


# TODO - example with hard codes. This is the type of stuff we would expect to add as part of
# customer implementation.

class ObjectCountTestCase(RawDataSourceTestCase):
    """
        Test that all relevant tables have a reasonable number of objects. We should not do this too narrow,
        as it will create lots of false positives as we grow / have different sites that have differnet counts.

        TODO: Parameterize so we can have one test per refinery / unit + also make it easy to override those with known exceptions.
    """
    client = None

    def setUp(self):
        if not self.client:
            self.client = get_cdf_client()

    def test_DocLib(self):
        self.check_count_range("db_dl_files_gvl", "tbl_gvlrs253_doclib_gvl", 200000, 500000)

    def test_intelex(self):
        # TODO: Many tables, should check more.
        self.check_count_range("db_indp_intelex_gvl", "tbl_indp_incident_equipment_gvl", 10000, 50000)

    def test_audit_Salus(self):
        self.check_count_range("db_salus_spec_sheet_gvl", "tbl_salus_u10_gvl", 100, 500)

    def test_audit_SI(self):
        self.check_count_range("db_si_spec_sheet_gvl", "tbl_si_spec_sheet_gvl", 2000, 10000)


if __name__ == '__main__':
    unittest.main()






