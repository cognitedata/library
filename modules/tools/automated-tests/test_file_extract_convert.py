import unittest
import os
import datetime
from csv import DictWriter
from dataclasses import dataclass
from typing import List

from cognite.client import CogniteClient
from helper import get_cdf_client, load_test_config, parse_time_string


@dataclass
class FileEntry:
    file_name: str
    modified: str


@dataclass
class EnvConfig:
    env_name: str
    client: CogniteClient
    vm_folder: str


def _get_file_extraction_folders():
    """Read env_folders from test_config.yaml file_extraction section. Returns None if not configured."""
    config = load_test_config()
    fe = config.get("file_extraction") or {}
    folders = fe.get("env_folders") or {}
    prod = (folders.get("prod") or "").strip()
    test = (folders.get("test") or "").strip()
    dev = (folders.get("dev") or "").strip()
    if not prod and not test and not dev:
        return None
    return {"prod": prod or None, "test": test or None, "dev": dev or None}


class FileExtractionAndConversionTestCase(unittest.TestCase):
    """
    File extractor/converter checks (log age, failure files). Configure in test_config.yaml
    under file_extraction.env_folders (prod, test, dev paths). Skipped if not configured.
    """
    test_client = None
    prod_client = None
    dev_client = None
    data_entries = {}
    prod_folder = None
    test_folder = None
    dev_folder = None
    permissions_checked = False
    database = 'db_indp_edms_files_metadata_ref'

    def setUp(self):
        folders = _get_file_extraction_folders()
        if not folders or not any(folders.values()):
            self.skipTest(
                "file_extraction.env_folders not set in test_config.yaml; add prod/test/dev paths to run file-extraction tests."
            )
        if self.prod_folder is None:
            self.prod_folder = folders.get("prod") or ""
            self.test_folder = folders.get("test") or ""
            self.dev_folder = folders.get("dev") or ""
        if not self.test_client:
            self.test_client = get_cdf_client('.env.TEST')
        if not self.prod_client:
            self.prod_client = get_cdf_client('.env.PROD')
        if not self.dev_client:
            self.dev_client = get_cdf_client('.env')

        if not self.permissions_checked:
            for name, location in [("prod", self.prod_folder), ("test", self.test_folder), ("dev", self.dev_folder)]:
                if not location:
                    continue
                path = os.path.join(location, 'Data')
                try:
                    os.listdir(path)
                except (OSError, FileNotFoundError) as e:
                    self.skipTest(f"file_extraction env_folders.{name} not accessible: {path} ({e})")
            self.permissions_checked = True

    def get_columns(self, envs: list) -> list:
        columns = [
            'key',
            'orig_file_name',
            'extension',
        ]
        for env in envs:
            columns.append(f'meta_modified_time_{env}')
            columns.append(f'meta_refreshed_time_{env}')
            columns.append(f'drop_file_modified_time_{env}')
            columns.append(f'target_file_modified_time_{env}')
            columns.append(f'failure_time_{env}')
        if 'prod' in envs and 'test' in envs:
            columns.append('drop_missing_prod')
        return columns

    def get_env_config(self, env: str) -> EnvConfig:
        if env == 'test':
            return EnvConfig(env_name='test', client=self.test_client, vm_folder=self.test_folder)
        if env == 'prod':
            return EnvConfig(env_name='prod', client=self.prod_client, vm_folder=self.prod_folder)
        if env == 'dev':
            return EnvConfig(env_name='dev', client=self.dev_client, vm_folder=self.dev_folder)
        raise ValueError(f'Unknown env: {env}')

    def read_files(self, path) -> List[FileEntry]:
        entries = []
        for entry in os.scandir(path):
            if entry.is_file():
                modified_date = datetime.datetime.fromtimestamp(entry.stat().st_mtime).strftime('%Y-%m-%d %H:%M:%S')
                entries.append(FileEntry(entry.name, modified_date))
        return entries

    def add_value(self, data_entries, doc_id: str, key: str, value: str):
        if doc_id not in data_entries:
            data_entries[doc_id] = {'key': doc_id}
        data_entries[doc_id][key] = value

    def find_latest_file_extractor_file(self, env: str, site: str):
        logs_folder = self.get_logs_folder(site).replace('FilesConverter', 'FileExtractor')
        folder = os.path.join(self.get_env_config(env).vm_folder, logs_folder)
        run_completion_date, latest_file = self.find_latest_file(folder, '')
        if not latest_file:
            print(f'Did not find a file extractor file in {folder}')
        return run_completion_date, latest_file

    def find_latest_file(self, folder: str, pattern: str):
        run_completion_date = ''
        latest_file = None
        latest_ts = 0
        for entry in os.scandir(folder):
            if entry.is_file() and entry.name.startswith(pattern):
                modified_timestamp = entry.stat().st_mtime
                if modified_timestamp > latest_ts:
                    latest_ts = modified_timestamp
                    run_completion_date = datetime.datetime.fromtimestamp(modified_timestamp).strftime('%Y-%m-%d %H:%M:%S')
                    latest_file = entry.name
        return run_completion_date, latest_file

    def read_failures(self, env_folder: str, site: str):
        folder = os.path.join(env_folder, self.get_logs_folder(site))
        run_completion_date, latest_file = self.find_latest_file(folder, 'failures-first')
        if not latest_file:
            print(f'Did not find a failures file for [{env_folder}: {site}] looking in {folder}')
            return ['2010-01-01 00:00:00', []]
        full_path = os.path.join(folder, latest_file)
        print(f'Last failures file: {full_path}.  {run_completion_date}')
        with open(full_path, 'r') as f:
            urls = f.read().splitlines()
        print(f'Found {len(urls)} urls in {full_path}')
        return [run_completion_date, urls]

    def check_failure_files_across_envs(self, site, env1: str, env2: str):
        env1config = self.get_env_config(env1)
        env2config = self.get_env_config(env2)
        env1_failures = len(self.read_failures(env1config.vm_folder, site)[1])
        env2_failures = len(self.read_failures(env2config.vm_folder, site)[1])
        if env1_failures < 10 and env2_failures < 10:
            print(f'All is well, both {env1} and {env2} have fewer than 10 failures')
            return
        msg = f'{env1} failures: {env1_failures}. {env2} failures: {env2_failures}'
        self.assertGreater(env1_failures, 0, msg)
        self.assertGreater(env2_failures, 0, msg)
        self.assertGreater(env1_failures / env2_failures, 0.7, msg)
        self.assertLess(env2_failures / env1_failures, 1.3, msg)

    def check_failure_file_age(self, site: str, env: str, max_days_ago: int):
        envconfig = self.get_env_config(env)
        env_failure_time = parse_time_string(self.read_failures(envconfig.vm_folder, site)[0])
        oldest_time = (datetime.datetime.now() - datetime.timedelta(days=max_days_ago))
        self.assertGreater(env_failure_time, oldest_time)

    def check_file_extractor_log_age(self, site: str, env: str, max_days_ago: int):
        last_log_time, last_log_name = self.find_latest_file_extractor_file(env, site)
        self.assertIsNotNone(last_log_name, f'File extractor file for {env}: {site} is missing')
        print(f'CHECK {last_log_time} - {last_log_name}')
        log_time = parse_time_string(last_log_time)
        oldest_time = (datetime.datetime.now() - datetime.timedelta(days=max_days_ago))
        self.assertGreater(log_time, oldest_time)

    def check_most_recent_modified_time(self, site: str, env: str, max_days_ago: int):
        oldest_time = (datetime.datetime.now() - datetime.timedelta(days=max_days_ago))
        forever_ago = datetime.datetime(2000, 1, 1)
        data = self.get_data_entries(site, [env])
        field = f'meta_modified_time_{env}'
        mod_time = forever_ago
        for entry in data.values():
            if field in entry:
                t = parse_time_string(entry[field])
                if t and t > mod_time:
                    mod_time = t
        self.assertGreater(mod_time, oldest_time)

    def get_dwg_folder(self, site: str) -> str:
        if site == 'gvl':
            return os.path.join('Data', 'DocLib', 'GVL-DWG-DROP')
        return os.path.join('Data', 'edms', 'dwg-drop', site)

    def get_target_folder(self, site: str):
        if site == 'gvl':
            return os.path.join('Data', 'DocLib', 'GVL')
        return os.path.join('Data', 'edms', 'target', site)

    def get_logs_folder(self, site: str):
        if site == 'gvl':
            return os.path.join('Cognite', 'FilesConverter', 'Logs', 'doclib')
        return os.path.join('Cognite', 'FilesConverter', 'Logs', site)

    def get_data_entries(self, site: str, envs: list):
        env_key = site + '_' + '#'.join(envs)
        if env_key in self.data_entries:
            return self.data_entries[env_key]
        data_entries = {}
        for env in envs:
            env_config = self.get_env_config(env)
            failures_time, failures_urls = self.read_failures(env_config.vm_folder, site)
            print(f'Found {len(failures_urls)} in last run')
            print(f'Querying for ALL meta rows in {env} for {site}. This may take a while.')
            if site == 'gvl':
                meta = env_config.client.raw.rows.list('db_dl_files_gvl', 'tbl_gvlrs253_doclib_gvl', limit=-1)
            else:
                meta = env_config.client.raw.rows.list(self.database, f'tbl_indp_edms_files_metadata_{site}', limit=-1)
            print(f'CDF returned {len(meta)} meta rows')
            for row in meta:
                key = row.key
                refreshed = datetime.datetime.fromtimestamp(row.last_updated_time / 1000).strftime('%Y-%m-%d %H:%M:%S')
                modified = row.columns.get('Modified_Date', '')
                self.add_value(data_entries, key, f'meta_refreshed_time_{env}', refreshed)
                self.add_value(data_entries, key, f'meta_modified_time_{env}', modified)
                url = row.get('UrlPath_Text')
                if url in failures_urls:
                    self.add_value(data_entries, key, f'failure_time_{env}', failures_time)
                    self.add_value(data_entries, key, 'extension', url.split('.')[-1])
            dwg = self.read_files(os.path.join(env_config.vm_folder, self.get_dwg_folder(site)))
            for entry in dwg:
                doc_id = '.'.join(entry.file_name.split('.')[:-1])
                self.add_value(data_entries, doc_id, 'orig_file_name', entry.file_name)
                self.add_value(data_entries, doc_id, f'drop_file_modified_time_{env}', entry.modified)
            target = self.read_files(os.path.join(env_config.vm_folder, self.get_target_folder(site)))
            for entry in target:
                extension = entry.file_name.split('.')[-1]
                clip = 1
                if entry.file_name.endswith('.dwg.pdf'):
                    extension, clip = 'dwg.pdf', 2
                elif entry.file_name.endswith('.dgn.pdf'):
                    extension, clip = 'dgn.pdf', 2
                doc_id = '.'.join(entry.file_name.split('.')[:-clip])
                self.add_value(data_entries, doc_id, 'orig_file_name', entry.file_name)
                self.add_value(data_entries, doc_id, f'target_file_modified_time_{env}', entry.modified)
                self.add_value(data_entries, doc_id, 'extension', extension)
        print(f'Populating data entries for {env_key} with {len(data_entries)} entries')
        self.data_entries[env_key] = data_entries
        return data_entries

    def run_all_tests(self, site: str, envs: list):
        os.makedirs('test-results', exist_ok=True)
        report_path = os.path.join('test-results', f'report-{site}-{"_".join(envs)}-{datetime.datetime.now().isoformat().split("T")[0]}.csv')
        with open(report_path, 'w', newline='') as report_file:
            w = DictWriter(report_file, fieldnames=self.get_columns(envs))
            w.writeheader()
            entries = list(self.get_data_entries(site, envs).values())
            for entry in entries:
                if 'prod' in envs and 'test' in envs:
                    if 'drop_file_modified_time_test' in entry and 'drop_file_modified_time_prod' not in entry:
                        entry['drop_missing_prod'] = 'Y'
                w.writerow(entry)

    def test_produce_reports(self):
        # Placeholder: run_all_tests('site', ['prod']) etc. when configured
        pass


if __name__ == '__main__':
    unittest.main()
