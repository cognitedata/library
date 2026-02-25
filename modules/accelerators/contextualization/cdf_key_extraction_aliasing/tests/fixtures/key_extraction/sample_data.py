"""
Shared test fixtures and sample data for all tests.

This module provides reusable test data and fixtures that follow the CDF Core Data Model
structure for CogniteAsset, CogniteFile, and CogniteTimeseries views.
"""

from typing import Any, Dict, List


def _extract_properties_from_cdm(
    entity: Dict[str, Any],
    view_space: str = "cdf_cdm",
    view_id: str = "CogniteAsset/v1",
) -> Dict[str, Any]:
    """
    Extract properties from CDM structure and flatten for engine consumption.

    Args:
        entity: Entity with CDM structure (properties nested under space/view)
        view_space: Data model space (default: "cdf_cdm")
        view_id: View identifier with version (default: "CogniteAsset/v1")

    Returns:
        Flattened dictionary with properties at top level
    """
    # If already flat structure (no properties key), return as-is
    if "properties" not in entity:
        return entity

    # Extract properties from CDM structure
    properties = entity.get("properties", {}).get(view_space, {}).get(view_id, {})

    # Create flattened entity
    flattened = {
        "externalId": entity.get("externalId"),
        "id": entity.get("externalId"),  # Also set id for compatibility
        **properties,  # Spread properties at top level
    }

    # If properties include metadata-like fields, keep them
    if "metadata" in entity:
        flattened["metadata"] = entity["metadata"]
    elif any(key in properties for key in ["equipmentType", "site", "unit"]):
        # Create metadata structure from properties for backward compatibility
        flattened["metadata"] = {
            "equipmentType": properties.get("equipmentType"),
            "site": properties.get("site"),
            "unit": properties.get("unit"),
            "documentType": properties.get("documentType"),
            "drawingNumber": properties.get("drawingNumber"),
        }

    return flattened


def get_cdf_assets() -> List[Dict[str, Any]]:
    """
    Get sample CogniteAsset records following CDF Core Data Model structure.

    CDM CogniteAsset schema structure:
    - externalId: Unique identifier (from instance.external_id)
    - space: Data model space (typically "cdf_cdm")
    - properties: Nested structure containing view-specific properties
      - properties[space][view_external_id/version]: Dictionary of property values
        - name: Display name property
        - description: Description text property
        - equipmentType: Type of equipment property
        - site: Site location property
        - unit: Unit/area within site property
    """
    view_space = "cdf_cdm"
    view_id = "CogniteAsset/v1"

    return [
        {
            "externalId": "ASSET-P-101",
            "space": view_space,
            "properties": {
                view_space: {
                    view_id: {
                        "name": "P-101",
                        "description": "Main feed pump for Tank T-301, controlled by FIC-2001",
                        "equipmentType": "pump",
                        "site": "Plant_A",
                        "unit": "Unit_100",
                    }
                }
            },
        },
        {
            "externalId": "ASSET-FCV-2001",
            "space": view_space,
            "properties": {
                view_space: {
                    view_id: {
                        "name": "FCV-2001",
                        "description": "Flow control valve in line feeding reactor R-401",
                        "equipmentType": "valve",
                        "site": "Plant_A",
                        "unit": "Unit_100",
                    }
                }
            },
        },
        {
            "externalId": "ASSET-T-301",
            "space": view_space,
            "properties": {
                view_space: {
                    view_id: {
                        "name": "T-301",
                        "description": "Feed tank with level control LIC-3001",
                        "equipmentType": "tank",
                        "site": "Plant_A",
                        "unit": "Unit_100",
                    }
                }
            },
        },
        {
            "externalId": "ASSET-R-401",
            "space": view_space,
            "properties": {
                view_space: {
                    view_id: {
                        "name": "R-401",
                        "description": "Reactor with temperature control TIC-4001 and pressure control PIC-4002",
                        "equipmentType": "reactor",
                        "site": "Plant_A",
                        "unit": "Unit_100",
                    }
                }
            },
        },
        {
            "externalId": "ASSET-E-501",
            "space": view_space,
            "properties": {
                view_space: {
                    view_id: {
                        "name": "E-501",
                        "description": "Heat exchanger for reactor cooling",
                        "equipmentType": "heat_exchanger",
                        "site": "Plant_B",
                        "unit": "Unit_200",
                    }
                }
            },
        },
        {
            "externalId": "ASSET-FIC-1001",
            "space": view_space,
            "properties": {
                view_space: {
                    view_id: {
                        "name": "FIC-1001",
                        "description": "Flow Indicator Controller for process line P-101 feeding Tank T-201",
                        "equipmentType": "instrument",
                        "site": "Plant_A",
                        "unit": "Unit_100",
                    }
                }
            },
        },
        {
            "externalId": "ASSET-A-FIC-1001",
            "space": view_space,
            "properties": {
                view_space: {
                    view_id: {
                        "name": "A-FIC-1001",
                        "description": "Flow Indicator Controller for Unit A, process line A-P-101",
                        "equipmentType": "instrument",
                        "site": "Plant_A",
                        "unit": "Unit_A",
                    }
                }
            },
        },
    ]


def get_cdf_files() -> List[Dict[str, Any]]:
    """
    Get sample CogniteFile records following CDF Core Data Model structure.

    CDM CogniteFile fields:
    - externalId: Unique identifier
    - name: File/document name
    - description: Description text
    - metadata.documentType: Type of document
    - metadata.drawingNumber: Drawing number identifier
    """
    return [
        {
            "externalId": "FILE-PID-2001",
            "name": "P&ID-2001-Rev-C",
            "description": "Process & Instrumentation Diagram for Unit 100. Referenced in PFD-2001",
            "metadata": {"documentType": "P&ID", "drawingNumber": "P&ID-2001-Rev-C"},
        },
        {
            "externalId": "FILE-PFD-2001",
            "name": "PFD-2001",
            "description": "Process Flow Diagram for Unit 100",
            "metadata": {"documentType": "PFD", "drawingNumber": "PFD-2001"},
        },
        {
            "externalId": "FILE-ISO-3001",
            "name": "ISO-3001",
            "description": "Isometric drawing for piping system in Unit 100",
            "metadata": {"documentType": "ISO", "drawingNumber": "ISO-3001"},
        },
        {
            "externalId": "FILE-ENG-4001",
            "name": "ENG-4001-Rev-A",
            "description": "Engineering drawing for Reactor R-401 installation",
            "metadata": {
                "documentType": "Engineering",
                "drawingNumber": "ENG-4001-Rev-A",
            },
        },
        {
            "externalId": "FILE-SPEC-5001",
            "name": "SPEC-5001",
            "description": "Specification for Heat Exchanger E-501",
            "metadata": {"documentType": "Specification", "drawingNumber": "SPEC-5001"},
        },
        {
            "externalId": "FILE-DWG-6001-SH1",
            "name": "DWG-6001-Sheet-1",
            "description": "Detailed drawing sheet 1 of 2 for compressor system",
            "metadata": {
                "documentType": "Drawing",
                "drawingNumber": "DWG-6001-Sheet-1",
            },
        },
    ]


def get_cdf_timeseries() -> List[Dict[str, Any]]:
    """
    Get sample CogniteTimeseries records following CDF Core Data Model structure.

    CDM CogniteTimeseries fields:
    - externalId: Unique identifier
    - name: Timeseries name
    - description: Description text
    - metadata.unit: Unit of measurement
    """
    return [
        {
            "externalId": "TS-P-101-FLOW",
            "name": "P-101_Flow",
            "description": "Flow rate for pump P-101",
            "metadata": {"unit": "m3/h"},
        },
        {
            "externalId": "TS-P-101-PRESSURE",
            "name": "P-101_Pressure",
            "description": "Discharge pressure for pump P-101",
            "metadata": {"unit": "bar"},
        },
        {
            "externalId": "TS-FIC-1001-VALUE",
            "name": "FIC-1001_VALUE",
            "description": "Flow indicator value from FIC-1001",
            "metadata": {"unit": "m3/h"},
        },
        {
            "externalId": "TS-PIC-2001-VALUE",
            "name": "PIC-2001_VALUE",
            "description": "Pressure indicator value from PIC-2001 monitoring vessel V-301",
            "metadata": {"unit": "bar"},
        },
        {
            "externalId": "TS-T-301-LEVEL",
            "name": "T-301_Level",
            "description": "Level measurement for tank T-301 using LIC-3001",
            "metadata": {"unit": "%"},
        },
        {
            "externalId": "TS-R-401-TEMP",
            "name": "R-401_Temperature",
            "description": "Reactor temperature measured by TIC-4001",
            "metadata": {"unit": "Celsius"},
        },
    ]


# Legacy functions maintained for backward compatibility
def get_iso_standard_assets() -> List[Dict[str, Any]]:
    """Legacy function - use get_cdf_assets() instead."""
    return get_cdf_assets()


def get_fixed_width_timeseries() -> List[Dict[str, Any]]:
    """Get sample timeseries records with fixed width format for testing."""
    return [
        {
            "externalId": "TS-FIC-1001-VALUE",
            "name": "FIC1001         FIC-1001       FLOW INDICATOR CONTROLLER VALUE",
            "description": "Timeseries for Flow Indicator Controller FIC-1001",
            "metadata": {"unit": "L/h"},
        },
        {
            "externalId": "TS-PIC-2001-VALUE",
            "name": "PIC2001         PIC-2001       PRESSURE INDICATOR CONTROLLER VALUE",
            "description": "Timeseries for Pressure Indicator Controller PIC-2001",
            "metadata": {"unit": "bar"},
        },
        {
            "externalId": "TS-TIC-3001-VALUE",
            "name": "TIC3001         TIC-3001       TEMPERATURE INDICATOR CONTROLLER VALUE",
            "description": "Timeseries for Temperature Indicator Controller TIC-3001",
            "metadata": {"unit": "Celsius"},
        },
        {
            "externalId": "TS-A-FIC-1001-VALUE",
            "name": "AFIC1001        A-FIC-1001     UNIT A FLOW INDICATOR CONTROLLER VALUE",
            "description": "Timeseries for Unit A Flow Indicator Controller",
            "metadata": {"unit": "L/h"},
        },
    ]


def get_token_reassembly_timeseries() -> List[Dict[str, Any]]:
    """Get sample timeseries records for token reassembly testing."""
    return [
        {
            "externalId": "TS-FIC-1001-VALUE",
            "name": "FIC1001         FIC-1001       FLOW INDICATOR CONTROLLER VALUE",
            "description": "Timeseries for Flow Indicator Controller FIC-1001",
            "metadata": {"unit": "L/h"},
        },
        {
            "externalId": "TS-PIC-2001-VALUE",
            "name": "PIC2001         PIC-2001       PRESSURE INDICATOR CONTROLLER VALUE",
            "description": "Timeseries for Pressure Indicator Controller PIC-2001",
            "metadata": {"unit": "bar"},
        },
        {
            "externalId": "TS-TIC-3001-VALUE",
            "name": "TIC3001         TIC-3001       TEMPERATURE INDICATOR CONTROLLER VALUE",
            "description": "Timeseries for Temperature Indicator Controller TIC-3001",
            "metadata": {"unit": "Celsius"},
        },
        {
            "externalId": "TS-LIC-4001-VALUE",
            "name": "LIC4001         LIC-4001       LEVEL INDICATOR CONTROLLER VALUE",
            "description": "Timeseries for Level Indicator Controller LIC-4001",
            "metadata": {"unit": "mm"},
        },
        {
            "externalId": "TS-FCV-5001-VALUE",
            "name": "FCV5001         FCV-5001       FLOW CONTROL VALVE POSITION VALUE",
            "description": "Timeseries for Flow Control Valve FCV-5001",
            "metadata": {"unit": "%"},
        },
        {
            "externalId": "TS-UNIT-A-FIC-1001-VALUE",
            "name": "UNITAFIC1001     UNIT-A-FIC-1001 UNIT A FLOW INDICATOR CONTROLLER VALUE",
            "description": "Timeseries for Unit A Flow Indicator Controller",
            "metadata": {"unit": "L/h"},
        },
    ]


def get_simple_asset(flatten: bool = False) -> Dict[str, Any]:
    """
    Get a simple asset for basic testing.

    Args:
        flatten: If True, return flattened structure for engine consumption.
                 If False, return CDM structure.
    """
    view_space = "cdf_cdm"
    view_id = "CogniteAsset/v1"

    cdm_asset = {
        "externalId": "ASSET-P-101-BASIC",
        "space": view_space,
        "properties": {
            view_space: {
                view_id: {
                    "name": "P-101",
                    "description": "Main feed pump for Tank T-301",
                    "equipmentType": "pump",
                    "site": "Plant_A",
                }
            }
        },
    }

    if flatten:
        return _extract_properties_from_cdm(cdm_asset, view_space, view_id)
    return cdm_asset


def get_cdf_assets_flat() -> List[Dict[str, Any]]:
    """
    Get sample CogniteAsset records in flattened format for engine consumption.

    This helper extracts properties from the CDM structure and flattens them
    to match what the KeyExtractionEngine expects.
    """
    return [_extract_properties_from_cdm(asset) for asset in get_cdf_assets()]


def get_sample_tags() -> List[str]:
    """Get sample tags for aliasing tests."""
    return ["P-101", "P_101", "T-201", "FIC-2001", "LIC-301"]


def get_heuristic_test_assets() -> List[Dict[str, Any]]:
    """Get sample assets for heuristic extraction testing."""
    view_space = "cdf_cdm"
    view_id = "CogniteAsset/v1"

    return [
        {
            "externalId": "ASSET-HEUR001",
            "space": view_space,
            "properties": {
                view_space: {
                    view_id: {
                        "name": "Main Process Line Equipment",
                        "description": "Equipment tag: P1001 is located at position A-Block-1. Reference documents: DOC-P1001-001",
                        "equipmentType": "pump",
                        "site": "Site_A",
                        "unit": "Unit_Production",
                    }
                }
            },
        },
        {
            "externalId": "ASSET-HEUR002",
            "space": view_space,
            "properties": {
                view_space: {
                    view_id: {
                        "name": "Control Valve Assembly",
                        "description": "Tag (FCV-2001) provides flow control. Associated tank: T-3001",
                        "equipmentType": "valve",
                        "site": "Site_B",
                        "unit": "Unit_Process",
                    }
                }
            },
        },
        {
            "externalId": "ASSET-HEUR003",
            "space": view_space,
            "properties": {
                view_space: {
                    view_id: {
                        "name": "Reactor Feed System",
                        "description": "System uses pump P-5001 at start, connects to vessel V-4001 (see P&ID-A-45)",
                        "equipmentType": "system",
                        "site": "Site_C",
                        "unit": "Unit_Reactor",
                    }
                }
            },
        },
        {
            "externalId": "ASSET-HEUR004",
            "space": view_space,
            "properties": {
                view_space: {
                    view_id: {
                        "name": "Instrument Tag After Keyword",
                        "description": "Measurement point: LIC-301 controls the level in tank T-401",
                        "equipmentType": "instrument",
                        "site": "Site_A",
                        "unit": "Unit_Storage",
                    }
                }
            },
        },
        {
            "externalId": "ASSET-HEUR005",
            "space": view_space,
            "properties": {
                view_space: {
                    view_id: {
                        "name": "Complex Equipment Description",
                        "description": "The primary control loop consists of FIC-1001-A, FIC-1001-B (both flow indicators), connected to PIC-2020 (pressure control). All operate under supervision of DCS-SYS-001",
                        "equipmentType": "instrument",
                        "site": "Site_B",
                        "unit": "Unit_Control",
                    }
                }
            },
        },
    ]
