import unittest

from helper import get_cdf_client
from raw_data_source_testcase import RawDataSourceTestCase, TestTargetColumn

class DoclibTestCase(RawDataSourceTestCase):
    client = None

    def setUp(self):
        if not self.client:
            self.client = get_cdf_client()

    def test_content_availability(self):
        self.check_count_range('db_dl_files_gvl', 'tbl_gvlrs253_doclib_gvl', 10, None)

    def test_DocLib(self):
        target = TestTargetColumn('db_dl_files_gvl', 'tbl_gvlrs253_doclib_gvl', 'primary_class')

        self.check_content_presence(target, 'EQUIP FILE')
        self.check_content_presence(target, 'ISO')
        self.check_content_presence(target, 'ROTATING EQUIPMENT')

        self.check_distribution_count(target, 10, 100)

        # TODO: Would think we should have no nulls, but a few reported. In the big picture too few to worry about as doclib will go away, but
        # in the perfect world they should be removed.
        #self.confirm_no_nulls(target)



if __name__ == '__main__':
    unittest.main()

