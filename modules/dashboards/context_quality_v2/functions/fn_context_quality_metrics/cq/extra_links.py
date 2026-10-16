"""Extra links: `View.property` entries from the run payload, measured per facility next to the built-in metrics.

The number for a link is how many instances of the view have the property set. A list property is set when it holds at
least one reference. This says a link is set, not that it points to the right thing.
"""

from collections import defaultdict
from typing import Any, TypedDict

from cq.discovery import discover_facility_spaces
from cq.types import RunConfig


class ExtraLink(TypedDict):
    model: str  # "space:externalId:version"
    view: str  # externalId of a view of the model
    property: str


type LinkKey = tuple[str, str, str, str]  # model, view, property, facility space
type ViewNodes = dict[tuple[str, str], list[tuple[str, dict[str, Any]]]]  # (model, view) -> [(space, properties)]


def _entries(raw: Any) -> list[Any]:
    if raw is None:
        return []
    if not isinstance(raw, list):
        raise ValueError(f"extraLinks must be a list of data models, not {raw!r}")
    return raw


def _model(entry: dict[str, Any]) -> str:
    model = entry.get("model")
    if not isinstance(model, str) or not model.strip():
        raise ValueError(f"Each extra links entry needs a model, got {entry!r}")
    if len([part for part in model.split(":") if part.strip()]) != 3 or model.count(":") != 2:
        raise ValueError(f"model must be written space:externalId:version, not {model!r}")
    return model


def _link(model: str, text: Any) -> ExtraLink:
    parts = text.split(".") if isinstance(text, str) else []
    if len(parts) != 2 or not all(part.strip() for part in parts):
        raise ValueError(f"A link must be written View.property, not {text!r} (model {model})")
    return {"model": model, "view": parts[0], "property": parts[1]}


def parse_extra_links(raw: Any) -> list[ExtraLink]:
    """The payload's `extraLinks` -> one entry per link. Nothing is guessed: a malformed file fails the run."""
    links: list[ExtraLink] = []
    for entry in _entries(raw):
        if not isinstance(entry, dict):
            raise ValueError(f"Each extra links entry must be a mapping with a model and its links, not {entry!r}")
        model = _model(entry)
        texts = entry.get("links")
        if not isinstance(texts, list) or not texts:
            raise ValueError(f"The model {model} needs links: a non-empty list of View.property")
        for text in texts:
            link = _link(model, text)
            if link in links:
                raise ValueError(f"The link {link['view']}.{link['property']} is listed twice for {model}")
            links.append(link)
    return links


def _is_ref(value: Any) -> bool:
    return isinstance(value, dict) and bool(value.get("externalId"))


def is_set(value: Any) -> bool:
    """A direct relation is set when it names a node; a list of them when it names at least one."""
    if isinstance(value, list):
        return any(_is_ref(item) for item in value)
    return _is_ref(value)


def count_links(links: list[ExtraLink], nodes: ViewNodes, facility_spaces: dict[str, str]) -> dict[LinkKey, list[int]]:
    """-> {(model, view, property, facility space): [eligible, passing, issue]}.

    A link has a count for each facility that holds instances of its view. A link whose view has none in any facility
    still gets a count of zero in every facility, so the person who named it sees that there was nothing to count.
    """
    counts: dict[LinkKey, list[int]] = defaultdict(lambda: [0, 0, 0])
    for link in links:
        found = False
        for space, props in nodes.get((link["model"], link["view"]), []):
            if space not in facility_spaces:
                continue
            found = True
            row = counts[(link["model"], link["view"], link["property"], space)]
            row[0] += 1
            row[1 if is_set(props.get(link["property"])) else 2] += 1
        if not found:
            for space in facility_spaces:
                counts[(link["model"], link["view"], link["property"], space)]
    return dict(counts)


def extra_facility_spaces(nodes: ViewNodes, facility_spaces: dict[str, str], cfg: RunConfig) -> dict[str, str]:
    """The facilities the extra links are counted in: the Core facilities, plus the spaces of the configured views.

    A space joins when it holds at least `minFacilityEntities` instances of the configured views and is not an excluded
    or metrics space (the same rules facility discovery applies to the Core entities), named by `facilityNames` where
    given. An explicit `facilitySpaces` list is the whole set, as for the built-in metrics.
    """
    if cfg["facilitySpaces"]:
        return dict(facility_spaces)
    counts: dict[str, int] = defaultdict(int)
    for entries in nodes.values():
        for space, _ in entries:
            counts[space] += 1
    found = discover_facility_spaces(dict(counts), cfg)
    return {**facility_spaces, **{space: name for space, name in found.items() if space not in facility_spaces}}
