
from constants import FieldNames
from data_structures import CallerType, FunctionRunConfig


class FunctionRunConfigRegistry:
    _CONFIGS: list[FunctionRunConfig] = [
        FunctionRunConfig(
            caller_type=CallerType.LAUNCH,
            function_id_field=FieldNames.LAUNCH_FUNCTION_ID_CAMEL_CASE,
            function_call_id_field=FieldNames.LAUNCH_FUNCTION_CALL_ID_CAMEL_CASE,
            log_title=FieldNames.LAUNCH_LOG_TITLE_CASE,
            log_snake_case=FieldNames.LAUNCH_LOG_SNAKE_CASE,
        ),
        FunctionRunConfig(
            caller_type=CallerType.FINALIZE,
            function_id_field=FieldNames.FINALIZE_FUNCTION_ID_CAMEL_CASE,
            function_call_id_field=FieldNames.FINALIZE_FUNCTION_CALL_ID_CAMEL_CASE,
            log_title=FieldNames.FINALIZE_LOG_TITLE_CASE,
            log_snake_case=FieldNames.FINALIZE_LOG_SNAKE_CASE,
        ),
        FunctionRunConfig(
            caller_type=CallerType.PREPARE,
            function_id_field=FieldNames.PREPARE_FUNCTION_ID_CAMEL_CASE,
            function_call_id_field=FieldNames.PREPARE_FUNCTION_CALL_ID_CAMEL_CASE,
            log_title=FieldNames.PREPARE_LOG_TITLE_CASE,
            log_snake_case=FieldNames.PREPARE_LOG_SNAKE_CASE,
        ),
        FunctionRunConfig(
            caller_type=CallerType.PROMOTE,
            function_id_field=FieldNames.PROMOTE_FUNCTION_ID_CAMEL_CASE,
            function_call_id_field=FieldNames.PROMOTE_FUNCTION_CALL_ID_CAMEL_CASE,
            log_title=FieldNames.PROMOTE_LOG_TITLE_CASE,
            log_snake_case=FieldNames.PROMOTE_LOG_SNAKE_CASE,
        ),
    ]

    @classmethod
    def get_available_function_configs_for_row(cls, selected_row: dict) -> list[FunctionRunConfig]:
        available_functions: list[FunctionRunConfig] = []
        for config in cls._CONFIGS:
            function_id = selected_row.get(config.function_id_field)
            call_id = selected_row.get(config.function_call_id_field)

            if function_id is not None and call_id is not None:
                available_functions.append(config)
        return available_functions

    @classmethod
    def get_all_configs(cls) -> list[FunctionRunConfig]:
        return list(cls._CONFIGS)
