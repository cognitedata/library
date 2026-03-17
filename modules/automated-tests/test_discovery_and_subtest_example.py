import unittest
import os, json

from helper import get_cdf_client
from raw_data_source_testcase import RawDataSourceTestCase, TestTargetColumn

# TODO: A lot of customer specific stuff in here that should go away. But some of the ideas for subtests are generic and should be kept

class EDMSTestCase(RawDataSourceTestCase):
    """
        Validate that metadata extracted from EDMS has plausible values and distribution.
    """
    client = None
    db = 'db_indp_edms_files_metadata_ref'
    edms_tables = []

    # Tables accidentally created in the wrong place that cause test failures.
    # We need to use a black list approach instead of a white list to make sure we pick up new sites as they are added.
    skip_tables = ['tbl_indp_edms_files_timestamps_ref']

    # These are utilized fields that cannot have an empty value
    utilized_fields = [
        "Unit_Code",
        "Title",
        # "Name", Marathon validates this field. There are migration cases for legacy data where it is expected to be empty.
        "Plant_Code",
        "Document_Type",
        "Document_Sub_Type",
        "File_Type_Short_Name",
        "Cognite_Annotate"
    ]

    # These are new extra fields that may not be present across all when introduced. Eventually we will just accept
    # new field arriving and ignore them
    incoming_fields = ['primary_key', 'Cognite_Id']

    def setUp(self):
        if not self.client:
            self.client = get_cdf_client()
            tables = self.client.raw.tables.list(db_name=self.db)
            self.edms_tables = list(
                filter(lambda x: not 'psv_audit' in x and x not in self.skip_tables, tables.as_names()))

    def test_content_availability(self):
        for table in self.edms_tables:
            with self.subTest(table=table):
                rows = self.client.raw.rows.list(self.db, table, limit=10)
                self.assertTrue(len(rows) > 0, f'edms table {table} has no data')

    def test_dwg_subtypes(self):
        target = TestTargetColumn(self.db, 'tbl_indp_edms_files_metadata_spp', 'DWG_Sub_Type_Code')

        # Should have a goldilocks number of unique primary classes, not too few, not too many
        self.check_distribution_count(target, 10, 100)

        # Preview only samples top entries, and it keeps changing. So just testing one candidate tha seems to always be present.
        self.check_content_presence(target, 'Isometric Diagram')

    def compare_edms_tables(self, expected, actual, table):
        # Filter out known inconsistencies. The views are shared with other consumers, so the source will not be fixed.
        expected_modified = list(filter(lambda x: not x.startswith('ZDWG '), expected))
        actual_modified = list(filter(lambda x: not x.startswith('ZDWG ') and x not in self.incoming_fields, actual))

        self.compare_expected_equal_lists(expected_modified, actual_modified, table)

    def test_expected_columns_current_edms_version(self):
        """ Test that all current tables have an identical column layout. """

        # This file is just a copy/paste from the network trace in CDF when opening the SP raw table.
        # The /rows?limit=1 call, copying out the columns element.
        with open(os.path.join('data', 'edms-sample-row.json'), 'r') as f:
            expected_columns = json.load(f).keys()

            for table in self.edms_tables:
                with self.subTest(table=table):
                    rows = self.client.raw.rows.list(self.db, table, limit=1)
                    self.assertEqual(len(rows), 1, f'Expected a row in {self.db} -> {table}')
                    self.compare_edms_tables(expected_columns, rows[0].columns.keys(), table)

    def test_utilized_columns(self):
        """ Test that all current tables have an identical column layout. """

        for table in self.edms_tables:

            with self.subTest(table=table):

                # If we pull X rows at least one of them should have Cognite_Annotate != 'N'. We just want the first.
                rows = self.client.raw.rows.list(self.db, table, limit=1000)
                self.assertGreaterEqual(len(rows), 1, f'Expected at least one row in {self.db} -> {table}')

                row = next((item for item in list(rows) if not item.get('Cognite_Annotate') == 'N'), None)

                self.assertIsNotNone(row, f'Did not find any items with Cognite_Annotate != N in {self.db} -> {table} ')

                for field in self.utilized_fields:
                    val = row.get(field, '') or ''
                    self.assertGreater(len(val), 0,
                                       f'Expected table {self.db} -> {table} to have a value for key {field}. Full record = {row.dump()}')


if __name__ == '__main__':
    unittest.main()

