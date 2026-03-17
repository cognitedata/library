import unittest
from test_file_extract_convert import FileExtractionAndConversionTestCase


class TestSPPFiles(FileExtractionAndConversionTestCase):

    # TODO: This one test is very customer specific, so remove this method from the base class.
    #def test_compare_failures_test_prod(self):
    #    self.check_failure_files_across_envs('spp', 'test', 'prod')


    # TODO: Not sure if it makes sense to have to make a method for each test or if
    # we should make a test config object per site.

    def test_extractor_refresh_time_test(self):
        self.check_failure_file_age('spp', 'test', 5)

    def test_extractor_refresh_time_prod(self):
        self.check_failure_file_age('spp', 'prod', 2)

    def test_file_extractor_time_test(self):
        self.check_file_extractor_log_age('spp', 'test', 5)

    def test_file_extractor_test_prod(self):
        self.check_file_extractor_log_age('spp', 'prod', 2)

    def test_most_recent_document_change_test(self):
        self.check_most_recent_modified_time('spp', 'test', 7)

    def test_most_recent_document_change_prod(self):
        self.check_most_recent_modified_time('spp', 'prod', 7)

    # def test_produce_csv_prod(self):
    #   self.run_all_tests('spp', ['prod'])


if __name__ == '__main__':
    unittest.main()
