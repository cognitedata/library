import sys
from typing import ClassVar

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes import RowWrite
from pydantic import BaseModel, model_validator
from pydantic.alias_generators import to_camel

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


FUNCTION_ID = "connection_writer"


class ViewProperty(BaseModel, alias_generator=to_camel):
    space: str
    external_id: str
    version: str
    direct_relation_property: str | None = None

    def as_view_id(self) -> dm.ViewId:
        return dm.ViewId(space=self.space, external_id=self.external_id, version=self.version)


class DirectRelationMapping(BaseModel, alias_generator=to_camel):
    start_node_view: ViewProperty
    end_node_view: ViewProperty

    @model_validator(mode="after")
    def direct_relation_is_set(self) -> Self:
        if (
            sum(
                1
                for prop in (self.start_node_view.direct_relation_property, self.end_node_view.direct_relation_property)
                if prop is not None
            )
            != 1
        ):
            raise ValueError("You must set 'directRelationProperty' for at either of 'startNode' or 'endNode'")
        return self


class ConfigData(BaseModel, alias_generator=to_camel):
    annotation_space: str
    direct_relation_mappings: list[DirectRelationMapping]


class ConfigState(BaseModel, alias_generator=to_camel):
    raw_database: str
    raw_table: str


class Config(BaseModel, alias_generator=to_camel):
    data: ConfigData
    state: ConfigState


class State(BaseModel):
    key: ClassVar[str] = FUNCTION_ID
    last_cursor: str | None = None

    @classmethod
    def from_cdf(cls, client: CogniteClient, state: ConfigState) -> "State":
        row = client.raw.rows.retrieve(db_name=state.raw_database, table_name=state.raw_table, key=cls.key)
        if row is None:
            return cls()
        return cls.model_validate(row.columns)

    def to_cdf(self, client: CogniteClient, state: ConfigState) -> None:
        client.raw.rows.insert(
            db_name=state.raw_database,
            table_name=state.raw_table,
            row=self._as_row(),
        )

    def _as_row(self) -> RowWrite:
        return RowWrite(
            key=self.key,
            columns=self.model_dump(),
        )

