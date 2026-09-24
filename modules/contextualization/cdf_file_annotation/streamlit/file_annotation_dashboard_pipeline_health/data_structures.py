from dataclasses import dataclass
from enum import StrEnum

from cognite.client.data_classes.data_modeling.ids import ViewId
from constants import FieldNames


@dataclass
class KPI:
    awaiting_processing: int = 0
    processed_total: int = 0
    failed_total: int = 0
    failure_rate_total: float = 0.0


@dataclass
class ViewPropertyConfig:
    schema_space: str
    external_id: str
    version: str
    instance_space: str | None = None

    def as_view_id(self) -> ViewId:
        return ViewId(space=self.schema_space, external_id=self.external_id, version=self.version)

    def as_property_ref(self, property_name: str) -> list[str]:
        return [self.schema_space, f"{self.external_id}/{self.version}", property_name]


class CallerType(StrEnum):
    PREPARE = "Prepare"
    LAUNCH = "Launch"
    FINALIZE = "Finalize"
    PROMOTE = "Promote"


@dataclass
class FunctionRunConfig:
    caller_type: CallerType
    function_id_field: str
    function_call_id_field: str
    log_title: str
    log_snake_case: str


@dataclass
class ExtractionPipelineConfig:
    file_view_cfg: ViewPropertyConfig | None = None
    annotation_state_view_cfg: ViewPropertyConfig | None = None

    @classmethod
    def from_dict(cls, d: dict | None):
        if not isinstance(d, dict):
            return cls()

        # fn_file_annotation keeps its views under "data"; pipelines from the four-function
        # version keep them under "dataModelViews".
        if "parameters" in d and "data" in d:
            views = d["data"] or {}
        else:
            views = d.get(FieldNames.DATA_MODEL_VIEWS_CAMEL_CASE, {}) or {}

        def _build_view(cfg_dict):
            if not cfg_dict:
                return None
            return ViewPropertyConfig(
                schema_space=cfg_dict.get(FieldNames.SCHEMA_SPACE_CAMEL_CASE),
                external_id=cfg_dict.get(FieldNames.EXTERNAL_ID_CAMEL_CASE),
                version=str(cfg_dict.get(FieldNames.VERSION_CAMEL_CASE)),
                instance_space=cfg_dict.get(FieldNames.INSTANCE_SPACE_CAMEL_CASE),
            )

        return cls(
            file_view_cfg=_build_view(views.get(FieldNames.FILE_VIEW_CAMEL_CASE)),
            annotation_state_view_cfg=_build_view(views.get(FieldNames.ANNOTATION_STATE_VIEW_CAMEL_CASE)),
        )
