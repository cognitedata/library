from cognite.client.data_classes.data_modeling import (
    ContainerId,
    MappedPropertyApply,
    ViewApply,
    ViewId,
)
from cognite.client.data_classes.filters import Equals, In

from classes.cfihos import CfihosClass


def _create_cfihos_view_filter(
    class_: CfihosClass, container_space: str, view_filters: dict | None
) -> Equals | In | None:
    """Create a view filter for a CFIHOS view.

    TODO: fix wording - Relies on the hardcoded property `cfihosTagClassCode` in the Tag container.

    Args:
        class_ (CfihosClass): CFIHOS class.
        container_space (str): Container space where the Tag container is located.
        view_filters (dict | None): View filters as defined in the config.

    Returns:
        Equals | In | None: The potential view filter.
    """
    filter_: Equals | In | None

    if not view_filters:
        return None

    property_ref = [container_space, "Tag", "cfihosTagClassCode"]
    default_behaviour = view_filters.get("default", "all")

    if not view_filters.get(class_.name):
        return In(
            property=property_ref,
            values=[class_.id_, *(class_.children_by_id or [])],
        )

    filter_type = view_filters[class_.name].get("type", default_behaviour)

    match filter_type:
        case "self":
            filter_ = Equals(property=property_ref, value=class_.id_)
        case "all":
            filter_ = In(
                property=property_ref,
                values=[class_.id_, *(class_.children_by_id or [])],
            )
        case _:
            raise ValueError(f"Unspported filter type: {filter_type}")

    return filter_


def create_cfihos_view(
    class_: CfihosClass,
    space: str,
    version: str,
    implements: list[ViewId] | None,
    view_filters: dict | None,
) -> ViewApply:
    """Create a CFIHOS view."""
    if class_.properties is None:
        class_.properties = {}
        # suppose we would raise an error here usually
        print(f"Class {class_.name} has no properties")

    if implements is None:
        implements = []

    filter_ = _create_cfihos_view_filter(class_, space, view_filters)

    properties = {
        prop.clean_name: MappedPropertyApply(
            container=ContainerId(space=space, external_id=class_.clean_name),
            container_property_identifier=prop.clean_id,  # or prop.clean_name
            # name=prop.id_,    # or prop.name
            description=prop.description,
        )
        for prop in class_.sorted_properties  # can be edited based on presence
    }
    properties["additionalProperties"] = MappedPropertyApply(
        container=ContainerId(space=space, external_id=class_.clean_name),
        container_property_identifier="additionalProperties",
    )
    view = ViewApply(
        space=space,
        external_id=class_.clean_name,
        version=version,
        properties=properties,
        # name=class_.id_,
        description=class_.description,
        implements=implements,
        filter=filter_,
    )
    return view
