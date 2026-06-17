from cognite.client.data_classes.data_modeling import (
    Boolean,
    Constraint,
    ContainerApply,
    ContainerProperty,
    Float32,
    Json,
    Text,
)

from classes.cfihos import CfihosClass

DATATYPE_MAP = {
    "Text": Text,
    "Number": Float32,
    "Boolean": Boolean,
    "String": Text,
    "Decimal": Float32,
}


def create_cfihos_container(
    class_: CfihosClass, space: str, constraints: dict[str, Constraint] | None
) -> ContainerApply:
    """Create a CFIHOS container."""
    if class_.properties is None:
        class_.properties = {}
        # suppose we would raise an error here usually
        print(f"Class {class_.name} has no properties")

    properties = {
        prop.clean_id: ContainerProperty(
            type=DATATYPE_MAP[prop.dtype](), name=prop.clean_name, description=prop.description
        )
        for prop in class_.sorted_properties  # can be edited based on presence
    }
    properties["additionalProperties"] = ContainerProperty(
        type=Json(),
        name="additionalProperties",
        description=f"Additional properties for the CFIHOS class {class_.name}",
    )
    container = ContainerApply(
        space=space,
        external_id=class_.clean_name,
        description=f"{class_.description}\n{class_.background}",
        constraints=constraints,
        properties=properties,
    )
    return container
