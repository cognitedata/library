import json

from cfihos_utils.filter import FilterParser
from classes.cfihos import CfihosClass, CfihosClassList, Property
from classes.presence import PresenceRanking


def trim_properties_from_classes(
    class_list: CfihosClassList, excludable_properties: list[str] | None = None
) -> CfihosClassList:
    """Trim Common and or Core properties from the CFIHOS classes.

    As they are already represented as part of Tag or CommonLCIProperties, it's redundant to have them in the CFIHOS classes.

    Note that this only applies to the properties given some presence ranking.

    Args:
        class_list (CfihosClassList): List of CFIHOS classes.
        excludable_properties (list[str] | None, optional): list of properties by clean name. Defaults to None.

    Returns:
        CfihosClassList: List of CFIHOS classes with trimmed properties.
    """
    if excludable_properties is None:
        return class_list

    # remove the excludable properties from the classes
    for class_ in class_list.classes:
        if class_.properties is None:
            continue
        print(f"Length of properties for {class_.name}: {len(class_.properties)}")
        for prop in class_.applicable_properties:
            if prop.clean_name in excludable_properties:
                # remove the property from the class
                class_.properties.pop(prop.name, None)
        print(f"New length of properties for {class_.name}: {len(class_.properties)}\n")
    return class_list


def collect_properties(class_map: dict[str, CfihosClass], class_ids: list[str]) -> dict[str, Property]:
    """Collect properties from a list of class ids, including inherited properties.

    Args:
        class_map (dict[str, CfihosClass]): Map of class id to CfihosClass.
        class_ids (list[str]): List of class ids.

    Returns:
        dict[str, Property]: Map of property name to Property.
    """
    properties: dict[str, Property] = {}
    for class_id in class_ids:
        class_ = class_map.get(class_id)
        if not class_:
            # likely that the class isn't found due to it being terminated
            continue
        if not class_.properties:
            continue
        for prop_name, prop in class_.properties.items():
            if (
                prop_name in properties
                # if the existing property has a higher presence ranking, we skip the new one
                and PresenceRanking.rank[prop.presence] < PresenceRanking.rank[properties[prop_name].presence]
            ):
                continue
            # standardize name to uppercase to avoid duplicates due to casing
            properties[prop_name] = prop
    return properties


def filter_cfihos_classes(class_list: CfihosClassList, filter_: dict | None = None) -> CfihosClassList:
    """Filter CFIHOS tag classes by given filter.

    If no filter is provided, all classes are returned.

    Filters are based upon including or excluding classes either by levels or lists tag class names.

    The filter is structured in a RESTful fashion.

    Args:
        class_list (CfihosClassList): List of CFIHOS classes.
        filter_ (dict): Filter dictionary.

    Returns:
        CfihosClassList: Filtered list of CFIHOS classes.
    """
    filter_parser = FilterParser(filter_)
    if not filter_:
        return class_list
    print("Applying filter: ", json.dumps(filter_, indent=2))
    filtered_classes = [class_ for class_ in class_list.classes if filter_parser.matches(class_.model_dump())]

    class_map = {c.id_: c for c in class_list.classes}
    filtered_classes_map = {class_.id_: class_ for class_ in filtered_classes}
    for tag_class in filtered_classes:
        # one scenario: tag_class is a parent of another filtered class -> exclude the filtered class' hierarchy
        # => trim sub hierarchy prior to collecting properties
        # if tag class exists in another filtered class' children, remove it and its children
        children_among_filtered = {fc for fc in filtered_classes_map if fc in (tag_class.children_by_id or [])}

        children = tag_class.children_by_id or []
        for child in children_among_filtered:
            print(f"Trimming {child}'s sub-hierarchy from parent {tag_class.id_} hierarchy")
            their_children = filtered_classes_map[child].children_by_id or []
            children = [c for c in children if c not in their_children]
            children.remove(child)
        tag_class.children_by_id = children

        properties = collect_properties(class_map, [*children, tag_class.id_])
        tag_class.properties = properties

    filtered_cfihos_classes = CfihosClassList.model_validate({"classes": filtered_classes})

    return filtered_cfihos_classes
