import sys
import unittest
import os
import datetime
from csv import DictWriter
from helper import *

from cognite.client import CogniteClient

from helper import get_cdf_client
from dataclasses import dataclass
from typing import List

# TODO: This contains a lot of marathon specific stuff. The idea for how to make it re-usable:
# Read the toolkit configs to sort out where log files should exist and run subtests for those locations.
#


@dataclass
class FileEntry:
    file_name: str
    modified: str


@dataclass
class EnvConfig:
    env_name: str
    client: CogniteClient
    vm_folder: str


class FileExtractionAndConversionTestCase(unittest.TestCase):
    test_client = None
    prod_client = None
    dev_client = None


    data_entries = {}

    prod_folder = '\\\\somevmshare'
    test_folder = '\\\\someothervmshare'
    dev_folder  = '\\\\yetanothervmshare'

    # Keep a flag and check that we can access the above location before we do anything else.
    # This allows us to fail early instead of after minutes of test execution
    permissions_checked = False

    database = 'db_indp_edms_files_metadata_ref'

    def setUp(self):
        if not self.test_client:
            self.test_client = get_cdf_client('.env.TEST')

        if not self.prod_client:
            self.prod_client = get_cdf_client('.env.PROD')

        if not self.dev_client:
            self.dev_client = get_cdf_client('.env')


        if not self.permissions_checked:
            for location in [self.prod_folder, self.test_folder, self.dev_folder]:
                # Will throw exception if no permissions
                os.listdir(os.path.join(location, 'Data'))

            self.permissions_checked = True

    def get_columns(self, envs: list[str]) -> list[str]:

        columns =    columns = [
            'key',
            'orig_file_name',
            'extension',
        ]

        for env in envs:
            # Modified_Date time stamp from raw metadata table (if it exists)
            columns.append(f'meta_modified_time_{env}')

            # The time this metadata entry was last updated/refreshed
            columns.append(f'meta_refreshed_time_{env}')

            # The modified time stamp for the file in the dwg-drop / diagram file folder
            columns.append(f'drop_file_modified_time_{env}')

            # The modified time stamp for the file in the target folder
            columns.append(f'target_file_modified_time_{env}')

            # The timestamp of the last failures-*.txt file. Will contain an entry
            # for all files that failed in that run
            columns.append(f'failure_time_{env}')

        if 'prod' in envs and 'test' in envs:
            # Set to "Y" if we have a diagram file in test but not in prod
            columns.append('drop_missing_prod')

        return columns


    def get_env_config(self, env: str) -> EnvConfig:

        if env == 'test':
            return EnvConfig(
                env_name = 'test',
                client = self.test_client,
                vm_folder = self.test_folder
            )
        elif env == 'prod':
            return EnvConfig(
                env_name = 'prod',
                client = self.prod_client,
                vm_folder = self.prod_folder
            )
        elif env == 'dev':
            return EnvConfig(
                env_name = 'dev',
                client = self.dev_client,
                vm_folder = self.dev_folder
            )
        else:
            raise Exception(f'Unknown env: {env}')


    def read_files(self, path) -> List[FileEntry]:

        entries: List[FileEntry] = []

        for entry in os.scandir(path):
            if entry.is_file():
                modified_timestamp = entry.stat().st_mtime
                modified_date = datetime.datetime.fromtimestamp(modified_timestamp).strftime('%Y-%m-%d %H:%M:%S')
                entries.append(FileEntry(entry.name, modified_date))

        return entries



    def add_value(self, data_entries, doc_id: str, key: str, value: str ):

        if not doc_id in data_entries:
            data_entries[doc_id] = {
                'key': doc_id
            }

        data_entries[doc_id][key] = value


    def find_latest_file_extractor_file(self, env: str, site: str):

        logs_folder = self.get_logs_folder(site).replace('FilesConverter', 'FileExtractor')

        folder =  os.path.join(self.get_env_config(env).vm_folder, logs_folder)

        # File patterns vary, so don't add any. We really SHOULD standardize on these.
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

        folder =  os.path.join(env_folder, self.get_logs_folder(site))

        # We use failures-first instead of the retried file so the system gets one more day to try to pick
        # up updated files. Also, then we will have identical urls with what is in the metadata table
        run_completion_date, latest_file = self.find_latest_file(folder, 'failures-first')

        if not latest_file:
            # Doclib does not have failures files. And when nothing has failed (yet) we don't care
            print(f'Did not find a failures file for [{env_folder}: {site}] looking in {folder}')
            return ['2010-01-01 00:00:00', []]

        full_path = os.path.join(folder, latest_file)
        print(f'Last failures file: {full_path}.  {run_completion_date}')

        with open(full_path, 'r') as f:
            urls = f.read().splitlines()

        print(f'Found {len(urls)} urls in {full_path}')

        return [ run_completion_date, urls]


    #def test_cbg_compare_failure_files_across_envs(self):
    #   self.check_failure_files_across_envs('cbg')

    #def test_spp_compare_failure_files_across_envs(self):
    #   self.check_failure_files_across_envs('spp')

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
        print (f'CHECK {last_log_time} - {last_log_name}')
        log_time = parse_time_string(last_log_time)
        oldest_time = (datetime.datetime.now() - datetime.timedelta(days=max_days_ago))
        self.assertGreater(log_time, oldest_time)


    def check_most_recent_modified_time(self, site: str, env: str, max_days_ago: int):
        oldest_time = (datetime.datetime.now() - datetime.timedelta(days=max_days_ago))

        forever_ago = datetime.datetime(2000,1,1)

        data = self.get_data_entries(site, [env])

        entries = data.values()

        field = f'meta_modified_time_{env}'

        mod_time = forever_ago

        for entry in entries:
            if field in entry:
                t = parse_time_string(entry[field])
                if t and t > mod_time:
                    mod_time = t

        # print (f'mod time for {site}:{env}= {mod}')
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

    def get_data_entries(self, site: str, envs: list[str]):

        env_key = site + '_' + '#'.join(envs)

        if env_key in self.data_entries:
            return self.data_entries[env_key]

        data_entries = {}

        for env in envs:
            env_config = self.get_env_config(env)

            failures_time, failures_urls = self.read_failures(env_config.vm_folder, site)
            print(f'Found {len(failures_urls)}  in last run')

            print(f'Querying for ALL meta rows in {env} for {site}. This may take a while.')
            if site == 'gvl':
                # Legacy site, with different db and table
                meta = env_config.client.raw.rows.list('db_dl_files_gvl', 'tbl_gvlrs253_doclib_gvl', limit=-1)
            else:
                meta =  env_config.client.raw.rows.list(self.database, f'tbl_indp_edms_files_metadata_{site}', limit=-1)
            print (f'CDF returned {len(meta)} meta rows')

            for row in meta:
                key = row.key
                refreshed = datetime.datetime.fromtimestamp(row.last_updated_time / 1000).strftime('%Y-%m-%d %H:%M:%S')
                modified = row.columns.get('Modified_Date', '')

                self.add_value(data_entries, key, f'meta_refreshed_time_{env}', refreshed)
                self.add_value(data_entries, key, f'meta_modified_time_{env}', modified)

                url = row.get('UrlPath_Text')

                if url in failures_urls:
                    self.add_value(data_entries, key, f'failure_time_{env}', failures_time)
                    # We may overwrite with a  better extension later, but start with what we find from the url
                    # in case we find nothing else later.
                    self.add_value(data_entries, key, 'extension', url.split('.')[-1])

            dwg = self.read_files(os.path.join(env_config.vm_folder,self.get_dwg_folder(site)))

            for entry in dwg:
                doc_id = '.'.join(entry.file_name.split('.')[:-1])
                self.add_value(data_entries, doc_id, 'orig_file_name', entry.file_name)
                self.add_value(data_entries, doc_id, f'drop_file_modified_time_{env}', entry.modified)


            target = self.read_files(os.path.join(env_config.vm_folder, self.get_target_folder(site)))

            for entry in target:
                # Since file names have periods in them, we just do special checks for the magic special case where
                # we know there is a period in the extension also.
                extension = entry.file_name.split('.')[-1]
                clip = 1
                if entry.file_name.endswith('.dwg.pdf'):
                    extension ='dwg.pdf'
                    clip = 2
                elif entry.file_name.endswith('.dgn.pdf'):
                    extension ='dgn.pdf'
                    clip = 2

                doc_id = '.'.join(entry.file_name.split('.')[:-clip])

                self.add_value(data_entries, doc_id, 'orig_file_name', entry.file_name)
                self.add_value(data_entries, doc_id, f'target_file_modified_time_{env}', entry.modified)
                self.add_value(data_entries, doc_id, 'extension', extension)

        print (f'Populating data entries for {env_key} with {len(data_entries)} entries')

        self.data_entries[env_key] = data_entries
        return data_entries



    def run_all_tests(self, site: str, envs: list[str]):

        # Open file early in case we get access denied, so it won't fail after many minutes of execution.
        report_file = open(os.path.join('test-results',f'report-{site}-{'_'.join(envs)}-{datetime.datetime.now().isoformat().split('T')[0]}.csv'), 'w')
        w = DictWriter(report_file, fieldnames=self.get_columns(envs))
        w.writeheader()

        entries = self.get_data_entries(site, envs).values()


        for entry in entries:
            if 'prod' in envs and 'test' in envs:
                if 'drop_file_modified_time_test' in entry and not 'drop_file_modified_time_prod' in entry:
                    entry['drop_missing_prod'] = 'Y'

            w.writerow(entry)

        report_file.close()





    def test_produce_reports(self):
        # Not really a test, just producing reports for what we are digging into.
        # This will be factored out and run on schedule.
        # self.run_all_tests('cbg', ['prod'])
        # self.run_all_tests('spp', ['prod'])
        # self.run_all_tests('gvl', ['prod'])
        # self.run_all_tests('gbr', ['test', 'prod'])
        # self.run_all_tests('anr', ['test', 'prod'])
        # self.run_all_tests('anr', ['dev', 'test'])
        # self.run_all_tests('lar', ['dev', 'test'])
        pass


if __name__ == '__main__':
    unittest.main()

