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
    if not view_filters:
        return None

    property_ref = [container_space, "Tag", "cfihosTagClassCode"]
    default_behaviour = view_filters.get("default", "all")

    # Resolve the filter type: when the class has no entry in view_filters
    # (or an empty entry), behave as "all" — same as the previous code path
    # that short-circuited to In(...) for that case.
    class_filters = view_filters.get(class_.name)
    if class_filters:
        filter_type = class_filters.get("type", default_behaviour)
    else:
        filter_type = "all"

    # Every branch below explicitly returns or raises, so there is no
    # fall-through path; this avoids CodeQL py/mixed-returns by making the
    # control flow obvious to static analyzers.
    if filter_type == "self":
        return Equals(property=property_ref, value=class_.id_)
    if filter_type == "all":
        return In(
            property=property_ref,
            values=[class_.id_, *(class_.children_by_id or [])],
        )
    raise ValueError(f"Unsupported filter type: {filter_type}")


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
