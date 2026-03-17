from pathlib import Path
from string import Template
from dotenv import dotenv_values
import yaml
import datetime
import os
import time

from cognite.client import CogniteClient, global_config


# # This is just a copy of the SDK example, switching out using environment variables with
# # content from an .env file
# def get_cdf_client(env_file=".env"):
#     file_path = Path("cognite_sdk_config.yaml")

#     # Read in yaml file and substitute variables in the file string
#     env_sub_template = Template(file_path.read_text())
#     file_env_parsed = env_sub_template.substitute(dotenv_values(env_file))

#     # Load yaml file string into a dictionary to parse global and client configurations
#     cognite_config = yaml.safe_load(file_env_parsed)

#     # If you want to set a global configuration it must be done before creating the client
#     global_config.apply_settings(cognite_config["global"])
#     return CogniteClient.load(cognite_config["client"])


def get_cdf_client(env_file=".env"):

    if not os.path.isfile(env_file):
        raise Exception(f'Env file "{env_file}" not found. Please create a .env file with the necessary environment variables to run the tests.')

    file_path = Path("cognite_sdk_config.yaml")

    # Read in yaml file and substitute variables in the file string
    env_sub_template = Template(file_path.read_text())
    file_env_parsed = env_sub_template.substitute(dotenv_values(env_file))

    # Load yaml file string into a dictionary to parse global and client configurations
    cognite_config = yaml.safe_load(file_env_parsed)

    # If you want to set a global configuration it must be done before creating the client
    global_config.apply_settings(cognite_config["global"])
    return CogniteClient.load(cognite_config["client"])


def parse_time_string(time_string):
    """
    Parses a time string that may or may not include milliseconds.
    """
    formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f"
    ]

    for fmt in formats:
        try:
            return datetime.datetime.strptime(time_string, fmt)
        except ValueError:
            continue  # Try the next format

    print(f'Invalid time stamp: [{time_string}]')
    return None


def is_expired_file(file_path, max_days_ago):
    many_days_ago = (datetime.datetime.now() - datetime.timedelta(days=max_days_ago)).timestamp()
    last_modified = os.path.getmtime(file_path)
    return last_modified < many_days_ago


def estimate_remaining_time(start_time, current_items, total_items):
    if current_items == 0 or current_items > total_items:
        return "..."

    elapsed_time = time.perf_counter() - start_time
    time_per_item = elapsed_time / current_items
    remaining_items = total_items - current_items
    remaining_time_seconds = remaining_items * time_per_item
    timedelta_remaining = datetime.timedelta(seconds=int(remaining_time_seconds))
    return str(timedelta_remaining)
