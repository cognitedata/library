"""Rules and resource names for the tests' sample dataset (parity_dataset.py). A TEST FIXTURE, not part of the function.

The rules use the sample dataset's own type codes, tags and owners. Nothing in the function reads this file (a test
enforces it), and the function's defaults carry none of these rules. It imports nothing from the function.
"""

from typing import Any

# What a deployment must supply: the function has no default for these.
SAMPLE_RESOURCES: dict[str, str] = {
    "stream": "test_stream",
    "schemaSpace": "sp_test_schema",
    "instanceSpace": "sp_test_instances",
}

SAMPLE_RULES: dict[str, Any] = {
    "divisionTagPrefix": "division:",
    "tieringEnabled": True,
    # tier from the CogniteAssetType code: 1 = major equipment, 2 = instrumentation
    "tierByTypeCode": {
        "pump": 1,
        "turbine": 1,
        "compressor": 1,
        "motor": 1,
        "heat_exchanger": 1,
        "tank": 1,
        "separator": 1,
        "sensor": 2,
        "transmitter": 2,
        "valve": 2,
        "controller": 2,
    },
    "disciplineByTypeCode": {
        "pump": "mechanical",
        "turbine": "mechanical",
        "compressor": "mechanical",
        "heat_exchanger": "mechanical",
        "tank": "mechanical",
        "separator": "mechanical",
        "motor": "electrical",
        "sensor": "instrumentation",
        "transmitter": "instrumentation",
        "valve": "instrumentation",
        "controller": "instrumentation",
    },
    # equipment type code -> the asset type code it should sit under
    "assetTypeByEquipmentTypeCode": {
        "centrifugal_pump": "pump",
        "gas_turbine": "turbine",
        "reciprocating_compressor": "compressor",
        "electric_motor": "motor",
        "shell_tube_exchanger": "heat_exchanger",
        "storage_tank": "tank",
        "three_phase_separator": "separator",
        "pressure_sensor": "sensor",
        "level_transmitter": "transmitter",
        "control_valve": "valve",
        "plc": "controller",
    },
    "ownerPrefixRules": [{"prefix": "PI-COMP3-", "ownerId": "instrumentation-lead"}],
    "defaultOwners": {
        "asset-hierarchy": "sap-steward",
        "equipment-asset": "sap-steward",
        "time-series": "instrumentation-lead",
        "maintenance": "sap-steward",
        "file-annotation": "pid-review-lead",
        "model-3d": "model-3d-lead",
        "files": "pid-review-lead",
    },
    "exclusions": {
        "syntheticTsPrefixes": ["CALC-", "VIRTUAL-", "SYNTH-"],
        "locationCentricFileCategories": ["safety_log", "procedure"],
        "excludedFileExtensions": [".png"],
        "decommissionedTags": ["lifecycle:decommissioned"],
    },
    "retiredTag": "lifecycle:retired",
}
