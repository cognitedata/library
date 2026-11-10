"""Pydantic contracts for the Data Quality bootstrap Function Schedule ``data`` payload."""

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class FunctionInput(BaseModel):
    """Validated Schedule ``data`` / call payload for bootstrap.

    Toolkit substitutes module variables into Schedule.data (not into the function code
    bundle). Defaults match ``default.config.yaml`` so local calls with ``{}`` still work.
    """

    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    force: bool = False
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = Field(default="INFO", alias="logLevel")

    data_quality_space: str | None = Field(default=None, alias="dataQualitySpace")
    records_stream_id: str | None = Field(default=None, alias="recordsStreamId")
    function_code_dataset_external_id: str | None = Field(default=None, alias="functionCodeDatasetExternalId")
    data_product_external_id: str | None = Field(default=None, alias="dataProductExternalId")
    data_product_version: str | None = Field(default=None, alias="dataProductVersion")

    schema_space: str = Field(default="cdf_cdm", alias="schemaSpace")
    secondary_schema_space: str = Field(default="cdf_idm", alias="secondarySchemaSpace")
    view_version: str = Field(default="v1", alias="viewVersion")
    view_asset_external_id: str = Field(default="CogniteAsset", alias="viewAssetExternalId")
    view_equipment_external_id: str = Field(default="CogniteEquipment", alias="viewEquipmentExternalId")
    view_time_series_external_id: str = Field(default="CogniteTimeSeries", alias="viewTimeSeriesExternalId")
    view_maintenance_order_external_id: str = Field(
        default="CogniteMaintenanceOrder", alias="viewMaintenanceOrderExternalId"
    )
    view_operation_external_id: str = Field(default="CogniteOperation", alias="viewOperationExternalId")
    view_notification_external_id: str = Field(default="CogniteNotification", alias="viewNotificationExternalId")
    instance_space: str = Field(default="inst_location", alias="instanceSpace")
    time_series_external_id_prefix: str = Field(default="pi:", alias="timeSeriesExternalIdPrefix")
    rule_set_version: str = Field(default="1.0.0", alias="ruleSetVersion")

    def settings_overrides(self) -> dict[str, object]:
        """Return only Schedule keys that were explicitly set (for settings.yaml merge)."""
        return self.model_dump(by_alias=True, exclude_unset=True, exclude_none=True, exclude={"force", "log_level"})

    def substitution_variables(self) -> dict[str, str]:
        """All placeholder variables for bundled views/timeseries YAML (includes defaults)."""
        dumped = self.model_dump(by_alias=True, exclude={"force", "log_level"})
        return {key: "" if value is None else str(value) for key, value in dumped.items()}


class FunctionOutput(BaseModel):
    """Minimal success/failure envelope; package deploy fields are preserved via ``extra``."""

    model_config = ConfigDict(extra="allow")

    status: str
