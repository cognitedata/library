from governance_build.dimensions import (
    cartesian_list_combos,
    get_dimensions,
    get_scope_hierarchy,
    require_list_dimension,
)


def test_cartesian_two_lists():
    doc = {
        "scope_hierarchy": {
            "type": "hierarchy",
            "levels": ["site"],
            "locations": [{"id": "S", "locations": []}],
        },
        "dimensions": {
            "a": {"type": "list", "items": [{"id": "1"}, {"id": "2"}]},
            "b": {"type": "list", "items": [{"id": "x"}]},
        },
    }
    dims = get_dimensions(doc)
    combos = list(cartesian_list_combos(dims, ["a", "b"]))
    assert len(combos) == 2
    sh = get_scope_hierarchy(doc)
    assert sh["type"] == "hierarchy"
    require_list_dimension(dims, "a")
