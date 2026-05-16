from governance_build.dimensions import cartesian_list_combos, get_dimensions, require_hierarchy


def test_cartesian_two_lists():
    doc = {
        "dimensions": {
            "a": {"type": "list", "items": [{"id": "1"}, {"id": "2"}]},
            "b": {"type": "list", "items": [{"id": "x"}]},
            "h": {"type": "hierarchy", "levels": ["site"], "locations": [{"id": "S"}]},
        }
    }
    dims = get_dimensions(doc)
    combos = list(cartesian_list_combos(dims, ["a", "b"]))
    assert len(combos) == 2
    require_hierarchy(dims, "h")
