import unittest
import os
import json

from helper import get_cdf_client
from raw_data_source_testcase import RawDataSourceTestCase, TestTargetColumn

# TODO: Remove customer-specific DB/tables/fields; keep generic subtest-per-table pattern and config-driven list.


class EDMSTestCase(RawDataSourceTestCase):
    """
    Validate that metadata extracted from EDMS has plausible values and distribution.
    """
    client = None
    db = 'db_indp_edms_files_metadata_ref'
    edms_tables = []
    skip_tables = ['tbl_indp_edms_files_timestamps_ref']
    utilized_fields = [
        "Unit_Code", "Title", "Plant_Code", "Document_Type",
        "Document_Sub_Type", "File_Type_Short_Name", "Cognite_Annotate"
    ]
    incoming_fields = ['primary_key', 'Cognite_Id']

    def setUp(self):
        if not self.client:
            self.client = get_cdf_client()
            tables = self.client.raw.tables.list(db_name=self.db)
            self.edms_tables = list(
                filter(lambda x: 'psv_audit' not in x and x not in self.skip_tables, tables.as_names())
            )

    def test_content_availability(self):
        for table in self.edms_tables:
            with self.subTest(table=table):
                rows = self.client.raw.rows.list(self.db, table, limit=10)
                self.assertTrue(len(rows) > 0, f'edms table {table} has no data')

    def test_dwg_subtypes(self):
        target = TestTargetColumn(self.db, 'tbl_indp_edms_files_metadata_spp', 'DWG_Sub_Type_Code')
        self.check_distribution_count(target, 10, 100)
        self.check_content_presence(target, 'Isometric Diagram')

    def compare_edms_tables(self, expected, actual, table):
        expected_modified = list(filter(lambda x: not x.startswith('ZDWG '), expected))
        actual_modified = list(filter(lambda x: not x.startswith('ZDWG ') and x not in self.incoming_fields, actual))
        self.compare_expected_equal_lists(expected_modified, actual_modified, table)

    def test_expected_columns_current_edms_version(self):
        sample_path = os.path.join(os.path.dirname(__file__), 'data', 'edms-sample-row.json')
        if not os.path.isfile(sample_path):
            self.skipTest(f'Project-specific file not found: {sample_path}')
        with open(sample_path, 'r') as f:
            expected_columns = json.load(f).keys()
        for table in self.edms_tables:
            with self.subTest(table=table):
                rows = self.client.raw.rows.list(self.db, table, limit=1)
                self.assertEqual(len(rows), 1, f'Expected a row in {self.db} -> {table}')
                self.compare_edms_tables(expected_columns, rows[0].columns.keys(), table)

    def test_utilized_columns(self):
        for table in self.edms_tables:
            with self.subTest(table=table):
                rows = self.client.raw.rows.list(self.db, table, limit=1000)
                self.assertGreaterEqual(len(rows), 1, f'Expected at least one row in {self.db} -> {table}')
                row = next((item for item in list(rows) if item.get('Cognite_Annotate') != 'N'), None)
                self.assertIsNotNone(row, f'Did not find any items with Cognite_Annotate != N in {self.db} -> {table}')
                for field in self.utilized_fields:
                    val = row.get(field, '') or ''
                    self.assertGreater(len(val), 0, f'Expected table {self.db} -> {table} to have value for {field}. Record = {row.dump()}')


if __name__ == '__main__':
    unittest.main()
