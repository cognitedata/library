import pandas as pd
import re
from datetime import timedelta
from constants import FieldNames

class DataProcessor:
    @staticmethod
    def parse_run_message(message: str) -> dict:
        if not message:
            return {}

        pattern = re.compile(
            r"\(caller:(?P<caller>\w+), function_id:(?P<function_id>[\w\.-]+), call_id:(?P<call_id>[\w\.-]+)\) - "
            r"total files processed: (?P<total>\d+) - "
            r"successful files: (?P<success>\d+) - "
            r"failed files: (?P<failed>\d+)"
        )

        match = pattern.search(message)

        if match:
            data = match.groupdict()

            for key in [FieldNames.TOTAL_LOWER_CASE, FieldNames.SUCCESS_LOWER_CASE, FieldNames.FAILED_LOWER_CASE]:
                if key in data:
                    data[key] = int(data[key])
            return data
        return {}

    @staticmethod
    def filter_log_lines(logs: str, file_ext_id: str, context_lines: int = 2) -> str:
        if not logs or not file_ext_id:
            return ""

        lines = logs.splitlines()
        matching_indices = [i for i, ln in enumerate(lines) if file_ext_id in ln]

        if not matching_indices:
            return ""

        selected_ranges = []

        for idx in matching_indices:
            start = max(0, idx - context_lines)
            end = min(len(lines), idx + context_lines + 1)
            selected_ranges.append((start, end))

        merged = []

        for start, end in sorted(selected_ranges):
            if not merged or start > merged[-1][1]:
                merged.append([start, end])
            else:
                merged[-1][1] = max(merged[-1][1], end)

        out_blocks = []

        for start, end in merged:
            block = lines[start:end]
            out_blocks.append("\n".join(block))

        return "\n\n---\n\n".join(out_blocks)

    @staticmethod
    def process_runs_for_graphing(runs: list) -> pd.DataFrame:
        if not runs:
            return pd.DataFrame()

        launch_data = []
        finalize_runs_to_agg = []

        for run in runs:
            if run.status != FieldNames.SUCCESS_LOWER_CASE:
                continue

            parsed = DataProcessor.parse_run_message(run.message)

            if not parsed:
                continue

            timestamp = pd.to_datetime(run.created_time, unit="ms").tz_localize("UTC")
            count = parsed.get(FieldNames.TOTAL_LOWER_CASE, 0)
            caller = parsed.get(FieldNames.CALLER_LOWER_CASE)

            if caller == FieldNames.LAUNCH_TITLE_CASE:
                launch_data.append({FieldNames.TIMESTAMP_LOWER_CASE: timestamp, FieldNames.COUNT_LOWER_CASE: count, FieldNames.TYPE_LOWER_CASE: FieldNames.LAUNCH_TITLE_CASE})
            elif caller == FieldNames.FINALIZE_TITLE_CASE:
                finalize_runs_to_agg.append({FieldNames.TIMESTAMP_LOWER_CASE: timestamp, FieldNames.COUNT_LOWER_CASE: count})

        aggregated_finalize_data = []

        if finalize_runs_to_agg:
            finalize_runs_to_agg.sort(key=lambda x: x[FieldNames.TIMESTAMP_LOWER_CASE])
            current_group_start_time = finalize_runs_to_agg[0][FieldNames.TIMESTAMP_LOWER_CASE]
            current_group_count = 0

            for run in finalize_runs_to_agg:
                if run[FieldNames.TIMESTAMP_LOWER_CASE] < current_group_start_time + timedelta(minutes=10):
                    current_group_count += run[FieldNames.COUNT_LOWER_CASE]
                else:
                    aggregated_finalize_data.append({FieldNames.TIMESTAMP_LOWER_CASE: current_group_start_time, FieldNames.COUNT_LOWER_CASE: current_group_count, FieldNames.TYPE_LOWER_CASE: FieldNames.FINALIZE_TITLE_CASE})
                    current_group_start_time = run[FieldNames.TIMESTAMP_LOWER_CASE]
                    current_group_count = run[FieldNames.COUNT_LOWER_CASE]

            if current_group_count > 0:
                aggregated_finalize_data.append({FieldNames.TIMESTAMP_LOWER_CASE: current_group_start_time, FieldNames.COUNT_LOWER_CASE: current_group_count, FieldNames.TYPE_LOWER_CASE: FieldNames.FINALIZE_TITLE_CASE})

        df_launch = pd.DataFrame(launch_data) if launch_data else pd.DataFrame()
        df_finalize = pd.DataFrame(aggregated_finalize_data) if aggregated_finalize_data else pd.DataFrame()

        if df_launch.empty and df_finalize.empty:
            return pd.DataFrame()

        return pd.concat([df_launch, df_finalize], ignore_index=True)
