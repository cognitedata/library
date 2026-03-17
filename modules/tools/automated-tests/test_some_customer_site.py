"""
Per-site file-extraction checks (failure file age, log age, last modified).
Configure file_extraction.sites in test_config.yaml. Skipped if sites is missing or empty.
Requires file_extraction.env_folders and .env.TEST / .env.PROD.
"""
import unittest
from helper import load_test_config
from test_file_extract_convert import FileExtractionAndConversionTestCase


def _get_sites():
    config = load_test_config()
    fe = config.get("file_extraction") or {}
    return fe.get("sites") or []


class TestFileExtractionBySite(FileExtractionAndConversionTestCase):
    """
    Run failure-file age, file-extractor log age, and last-modified checks per site.
    Sites come from test_config.yaml file_extraction.sites (e.g. ["spp", "cbg"]).
    """

    def setUp(self):
        super().setUp()
        sites = _get_sites()
        if not sites:
            self.skipTest("file_extraction.sites not set in test_config.yaml; add site names to run per-site checks.")

    def test_failure_file_age(self):
        sites = _get_sites()
        for site in sites:
            with self.subTest(site=site):
                self.check_failure_file_age(site, 'test', 5)
                self.check_failure_file_age(site, 'prod', 2)

    def test_file_extractor_log_age(self):
        sites = _get_sites()
        for site in sites:
            with self.subTest(site=site):
                self.check_file_extractor_log_age(site, 'test', 5)
                self.check_file_extractor_log_age(site, 'prod', 2)

    def test_most_recent_document_change(self):
        sites = _get_sites()
        for site in sites:
            with self.subTest(site=site):
                self.check_most_recent_modified_time(site, 'test', 7)
                self.check_most_recent_modified_time(site, 'prod', 7)


if __name__ == '__main__':
    unittest.main()
