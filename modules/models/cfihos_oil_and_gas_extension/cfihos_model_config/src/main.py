import os
from pathlib import Path

from cognite.client.data_classes.data_modeling import (
    Constraint,
    ContainerApply,
    ViewApply,
)
from yaml import safe_load

import cfihos
from cfihos_utils.trimming import filter_cfihos_classes
from classes.config import Config


def handle_config(config_path: Path | str) -> Config:
    """Handle the configuration file and return a Config object."""
    with open(config_path) as f:
        config = safe_load(f)

    return Config.model_validate(config)


def create_cfihos_view_and_containers(
    config: Config, generic_properties: list[str] | None = None
) -> tuple[list[ContainerApply], list[ViewApply]]:
    """Create CFIHOS views and containers based on the configured filter."""
    if not generic_properties:
        generic_properties = []

    cfihos_path = os.path.join(os.path.dirname(__file__), config.cfihos.source)
    cfihos_classes = cfihos.load_cfihos_input(cfihos_path)
    cfihos_filter = config.cfihos.filter_
    cfihos_implements = config.cfihos.implements or []
    cfihos_view_filters = config.cfihos.view_filter or {}

    cfihos_constraints = config.cfihos.constraints or {}
    cfihos_constraints = {k: Constraint.load(v) for k, v in cfihos_constraints.items()}

    filtered_cfihos_classes = filter_cfihos_classes(cfihos_classes, cfihos_filter)

    containers, views = cfihos.generate(
        class_list=filtered_cfihos_classes,
        space=config.space_name,
        version=config.data_model_version,
        excludable_properties=generic_properties,
        implements=cfihos_implements,
        constraints=cfihos_constraints,
        view_filters=cfihos_view_filters,
    )

    return containers, views


def dump_to_cdf_tk(views: list[ViewApply], containers: list[ContainerApply], sub_folder: str = "") -> None:
    dump_folder_views = Path(__file__).parent / "toolkit-output" / "views"
    dump_folder_containers = Path(__file__).parent / "toolkit-output" / "containers"

    if sub_folder:
        dump_folder_views = dump_folder_views / sub_folder
        dump_folder_containers = dump_folder_containers / sub_folder

    if views:
        dump_folder_views.mkdir(parents=True, exist_ok=True)

    if containers:
        dump_folder_containers.mkdir(parents=True, exist_ok=True)

    for view in views:
        name = f"{view.external_id}.View.yaml"
        view.space = "{{space}}" if not sub_folder else view.space
        view.version = "{{dm_version}}" if not sub_folder else view.version
        with open(dump_folder_views / name, "w") as f:
            f.write(view.as_write().dump_yaml())

    for container in containers:
        container.space = "{{space}}"
        name = f"{container.external_id}.Container.yaml"
        with open(dump_folder_containers / name, "w") as f:
            f.write(container.as_write().dump_yaml())


def main(config_path: Path | str) -> None:
    """Main function to generate the CDM extended data model."""
    config = handle_config(config_path)

    containers, views = [], []

    if config.cfihos.include:
        cfihos_containers, cfihos_views = create_cfihos_view_and_containers(config)

        containers += cfihos_containers
        views += cfihos_views

    dump_to_cdf_tk(cfihos_views, cfihos_containers)

    print("Done")


if __name__ == "__main__":
    config_path = Path(__file__).parent / "config.yaml"

    main(config_path)
