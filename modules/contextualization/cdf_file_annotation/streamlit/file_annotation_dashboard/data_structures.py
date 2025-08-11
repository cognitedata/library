from cognite.client.data_classes.data_modeling import ViewId
from dataclasses import dataclass


# Configuration Classes
@dataclass
class ViewPropertyConfig:
    schema_space: str
    external_id: str
    version: str
    instance_space: str | None = None

    def as_view_id(self) -> ViewId:
        return ViewId(
            space=self.schema_space, external_id=self.external_id, version=self.version
        )

    def as_property_ref(self, property) -> list[str]:
        return [self.schema_space, f"{self.external_id}/{self.version}", property]
