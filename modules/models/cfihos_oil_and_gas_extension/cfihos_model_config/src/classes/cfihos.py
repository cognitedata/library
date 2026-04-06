import re

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, ValidationError, field_validator

from classes.presence import Presence


class Property(BaseModel):
    """A class representing a property of a CFIHOS class."""

    model_config = ConfigDict(extra="ignore", frozen=True, use_enum_values=True)
    name: str = Field(alias="property_name")
    id_: str = Field(
        alias="tag_property_CFIHOS_unique_id",
        validation_alias=AliasChoices("tag_property_CFIHOS_unique_id", "property_CFIHOS_unique_code"),
    )
    dtype: str = Field(alias="property_data_type")
    description: str = Field(alias="property_definition")
    uom_required: bool = Field(
        default=False,
        alias="unit_of_measure_dimension_code",
        validation_alias=AliasChoices("UomRequired", "unit_of_measure_dimension_code"),
    )
    uom_name: str | None = Field(
        default=None,
        alias="unit_of_measure_dimension_name",
        validation_alias=AliasChoices("UomClassName", "unit_of_measure_dimension_name"),
        description="The unit of measure dimension class / code, if any, associated with this property.",
    )
    presence: Presence = Field(
        default=Presence.NOT_APPLICABLE.value, validation_alias=AliasChoices("Presence", "presence")
    )

    @field_validator("presence", mode="before")
    def validate_presence(cls, value: str) -> Presence:
        """Validate the presence of the property."""
        if not value:
            return Presence.NOT_APPLICABLE
        try:
            return Presence.from_string(value)
        except ValueError as ve:
            raise ValidationError(f"Invalid presence value: {value}") from ve

    @field_validator("uom_required", mode="before")
    def validate_uom_required(cls, value: str | bool) -> bool:
        """Validate the uom_required field."""
        if value is None:
            return False
        if isinstance(value, bool):
            return value
        return value.lower() != "false"

    @property
    def clean_id(self) -> str:
        """Format the id to be used as external id."""
        id_ = self.id_.replace("-", "_")
        # this is to adhere to the DMS API
        pat = re.compile("^[a-zA-Z0-9][a-zA-Z0-9_-]{0,253}[a-zA-Z0-9]?$")
        if not pat.fullmatch(id_):
            raise ValidationError(f"Invalid id: {id_}")

        id_ = id_ + "_UOM" if self.clean_name.endswith("_UOM") else id_

        return id_

    @property
    def snake_case_name(self) -> str:
        """Format the name to snake_case."""
        # name parsing be quite funky
        name = self.name.lower().split("(")[0].strip()
        name = re.sub(r"[^a-zA-Z0-9]", "_", name)
        name = name.removesuffix("_")
        # this is to adhere to the DMS API
        pat = re.compile("^[a-zA-Z0-9][a-zA-Z0-9_-]{0,253}[a-zA-Z0-9]?$")
        if not pat.fullmatch(name):
            raise ValidationError(f"Invalid name: {name}")

        return name

    @property
    def clean_name(self) -> str:
        """Format the name to camelCase."""
        name = self.name.lower()
        name = re.sub(r"[^a-zA-Z0-9]", " ", name)
        name = name.removesuffix("_")
        split = name.split(" ")

        first = split[0].lower()
        remainder = "".join(word.capitalize() for word in split[1:]) if len(split) > 1 else ""

        name = first + remainder
        # this is to adhere to the DMS API
        pat = re.compile("^[a-zA-Z0-9][a-zA-Z0-9_-]{0,253}[a-zA-Z0-9]?$")
        if not pat.fullmatch(name):
            raise ValidationError(f"Invalid name: {name}")

        name = name.replace("UOM", "_UOM")  # special case for UOM in names
        return name


class CfihosClass(BaseModel):
    """A class representing a CFIHOS class."""

    model_config = ConfigDict(extra="ignore")
    name: str = Field(alias="tag_class_name")
    id_: str = Field(alias="CFIHOS_unique_id")
    level: int = Field(alias="level")
    description: str | None = Field(alias="tag_class_definition", default=None)
    background: str | None = Field(alias="reason_for_having_class", default=None)
    properties: dict[str, Property] | None = None
    children_by_name: list[str] | None = None
    children_by_id: list[str] | None = None
    background_children: list[str] | None = Field(alias="reason_for_having_children", default=None)
    parent_by_name: str | None = None
    parent_by_id: str | None = None

    @property
    def clean_name(self) -> str:
        """Clean the name of the class."""
        name = self.name.strip()
        name = re.sub(r"[^a-zA-Z0-9]", " ", name)
        name = "".join(word.capitalize() for word in name.split(" "))
        # validating
        pat = re.compile("^[a-zA-Z]([a-zA-Z0-9_]{0,253}[a-zA-Z0-9])?$")
        if not pat.fullmatch(name):
            raise ValidationError(f"Invalid name: {name}")

        return name

    @property
    def sorted_properties(self) -> list[Property]:
        """Sort the properties of the class by name."""
        if self.properties is None:
            return []
        all_properties = list(self.properties.values()) + self.uom_properties
        return sorted(all_properties, key=lambda x: x.name)

    @property
    def uom_properties(self) -> list[Property]:
        """Get the properties of the class that require a unit of measure.

        The properties need to be created as separate Property instances in order to distinguish them.
        """
        if self.properties is None:
            return []
        props = [prop for prop in self.properties.values() if prop.uom_required]
        uom_properties: list[Property] = []
        for prop in props:
            uom_property = Property(
                # hacky :3
                property_name=f"{prop.name}_U_O_M",
                tag_property_CFIHOS_unique_id=prop.id_,
                property_data_type="Text",
                property_definition=f"{prop.name.capitalize()} UOM, {prop.uom_name}.",
                unit_of_measure_dimension_code=prop.uom_required,
                unit_of_measure_dimension_name=prop.uom_name,
                presence=prop.presence,
            )
            uom_properties.append(uom_property)
        return sorted(uom_properties, key=lambda x: x.name)

    @property
    def applicable_properties(self) -> list[Property]:
        """Get the applicable properties of the class."""
        if self.properties is None:
            return []
        props = [prop for prop in self.sorted_properties if prop.presence != Presence.NOT_APPLICABLE.value]
        return props

    @property
    def required_properties(self) -> list[Property]:
        """Get the required properties of the class."""
        if self.properties is None:
            return []
        return [prop for prop in self.sorted_properties if prop.presence == Presence.REQUIRED.value]

    @property
    def preferred_properties(self) -> list[Property]:
        """Get the preferred properties of the class."""
        if self.properties is None:
            return []
        return [prop for prop in self.sorted_properties if prop.presence == Presence.PREFERRED.value]

    @property
    def optional_properties(self) -> list[Property]:
        """Get the optional properties of the class."""
        if self.properties is None:
            return []
        return [prop for prop in self.sorted_properties if prop.presence == Presence.OPTIONAL.value]

    @property
    def not_applicable_properties(self) -> list[Property]:
        """Get the not applicable properties of the class."""
        if self.properties is None:
            return []
        return [prop for prop in self.sorted_properties if prop.presence == Presence.NOT_APPLICABLE.value]


class CfihosClassList(BaseModel):
    """A class representing a list of CFIHOS classes."""

    classes: list[CfihosClass]

    def drop_duplicates(self) -> None:
        """Drop duplicates from the list of classes."""
        seen = set()
        unique_classes = []
        for class_ in self.classes:
            if class_.name not in seen:
                seen.add(class_.name)
                unique_classes.append(class_)
        self.classes = unique_classes
