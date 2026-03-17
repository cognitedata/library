import json

from cognite.client.data_classes.data_modeling import (
    Constraint,
    ContainerApply,
    ViewApply,
    ViewId,
)
from pydantic import ValidationError

from cfihos_utils.container import create_cfihos_container
from cfihos_utils.trimming import trim_properties_from_classes
from cfihos_utils.view import create_cfihos_view
from classes.cfihos import CfihosClassList


def load_cfihos_input(file: str) -> CfihosClassList:
    """Load CFIHOS classes from a JSON file."""
    with open(file) as f:
        data = json.load(f)
    try:
        classes = CfihosClassList.model_validate({"classes": data})
    except ValidationError as e:
        raise e
    if not classes:
        raise ValueError(f"Invalid CFIHOS classes: {file}")
    return classes


def generate(
    class_list: CfihosClassList,
    space: str,
    version: str,
    excludable_properties: list[str] | None = None,
    implements: list[ViewId] | None = None,
    constraints: dict[str, Constraint] | None = None,
    view_filters: dict | None = None,
) -> tuple[list[ContainerApply], list[ViewApply]]:
    """Generate CFIHOS containers and views.

    Args:
        class_list (list[CfihosClass]): List of CFIHOS class_list.
        space (str): Space name.
        version (str): Data model version.
        excludable_properties (list[str]): List of properties to exclude. Could be from the core Tag container.
        Defaults to None.
        implements (list[ViewId], optional): List of views to implement. Defaults to None.
        constraints (dict[str, Constraint], optional): Constraints to apply to the containers. Defaults to None.
        view_filters (dict, optional): Filters to apply to the views. Defaults to None.

    Returns:
        tuple[list[ContainerApply], list[ViewApply]]: Tuple of containers and views.
    """
    if implements is None:
        implements = []

    class_list = trim_properties_from_classes(class_list, excludable_properties)

    containers = [create_cfihos_container(class_, space, constraints) for class_ in class_list.classes]
    views = [create_cfihos_view(class_, space, version, implements, view_filters) for class_ in class_list.classes]

    for v in sorted(views, key=lambda x: len(x.properties), reverse=True):
        print(f"Number of properties for view {v.external_id}: {len(v.properties)}")
    return containers, views
