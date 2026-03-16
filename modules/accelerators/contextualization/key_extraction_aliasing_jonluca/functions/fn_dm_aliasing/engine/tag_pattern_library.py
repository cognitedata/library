#!/usr/bin/env python3
"""
CDM Tag Pattern Library
========================

A comprehensive library of industrial asset tag patterns and document naming conventions
for use with the CDM Metadata Extraction Pipeline. This module provides pre-built
patterns loaded from YAML configuration files for common industrial equipment,
instruments, and engineering documents.

Features:
- Industry-standard tag patterns (ISA, ANSI, etc.) loaded from YAML
- Equipment-specific pattern recognition
- Document type classification
- Pattern validation and testing utilities
- Extensible pattern registry

Author: Darren Downtain
Version: 1.0.0
"""

import logging
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import yaml

logger = logging.getLogger(__name__)


class EquipmentType(Enum):
    """Standard equipment types in industrial facilities."""

    PUMP = "pump"
    COMPRESSOR = "compressor"
    VALVE = "valve"
    TANK = "tank"
    VESSEL = "vessel"
    HEAT_EXCHANGER = "heat_exchanger"
    REACTOR = "reactor"
    COLUMN = "column"
    TURBINE = "turbine"
    MOTOR = "motor"
    INSTRUMENT = "instrument"
    PIPE = "pipe"
    FITTING = "fitting"
    GENERIC = "generic"


class InstrumentType(Enum):
    """Standard instrument types based on ISA conventions."""

    FLOW = "flow"
    PRESSURE = "pressure"
    TEMPERATURE = "temperature"
    LEVEL = "level"
    ANALYTICAL = "analytical"
    CONTROL = "control"
    SAFETY = "safety"
    GENERIC = "generic"


class DocumentType(Enum):
    """Engineering document types."""

    PID = "pid"  # Process & Instrumentation Diagram
    PFD = "pfd"  # Process Flow Diagram
    ISO = "iso"  # Isometric Drawing
    PLAN = "plan"  # Plan View
    ELEVATION = "elevation"  # Elevation View
    SECTION = "section"  # Section View
    DETAIL = "detail"  # Detail Drawing
    SPECIFICATION = "specification"
    DATASHEET = "datasheet"
    MANUAL = "manual"
    PROCEDURE = "procedure"
    GENERIC = "generic"


@dataclass
class TagPattern:
    """Represents a tag pattern with metadata."""

    name: str
    pattern: str
    description: str
    equipment_type: EquipmentType
    instrument_type: Optional[InstrumentType] = None
    examples: List[str] = field(default_factory=list)
    priority: int = 100
    validation_rules: List[str] = field(default_factory=list)
    industry_standard: Optional[str] = None  # ISA, ANSI, etc.

    def __post_init__(self):
        """Validate pattern after initialization."""
        try:
            re.compile(self.pattern)
        except re.error as e:
            raise ValueError(f"Invalid regex pattern '{self.pattern}': {e}")


@dataclass
class DocumentPattern:
    """Represents a document naming pattern."""

    name: str
    pattern: str
    description: str
    document_type: DocumentType
    examples: List[str] = field(default_factory=list)
    priority: int = 100
    required_elements: List[str] = field(default_factory=list)
    optional_elements: List[str] = field(default_factory=list)


class IPatternRegistry(ABC):
    """Interface for pattern registries."""

    @abstractmethod
    def get_patterns_by_type(self, equipment_type: EquipmentType) -> List[TagPattern]:
        """Get patterns for a specific equipment type."""
        pass

    @abstractmethod
    def get_all_patterns(self) -> List[TagPattern]:
        """Get all registered patterns."""
        pass

    @abstractmethod
    def register_pattern(self, pattern: TagPattern) -> bool:
        """Register a new pattern."""
        pass


class StandardTagPatternRegistry(IPatternRegistry):
    """Registry for standard industrial tag patterns loaded from YAML configuration."""

    def __init__(self, config_path: Optional[str] = None):
        self.patterns: Dict[str, TagPattern] = {}
        self.equipment_index: Dict[EquipmentType, List[str]] = {}
        self.instrument_index: Dict[InstrumentType, List[str]] = {}

        # Default config path - points to tag_patterns.yaml in fn_dm_aliasing directory
        # Go up from engine/ to fn_dm_aliasing/
        if config_path is None:
            config_path = Path(__file__).parent.parent / "tag_patterns.yaml"

        self.config_path = Path(config_path)
        self._load_patterns_from_yaml()

    def _load_patterns_from_yaml(self):
        """Load patterns from YAML configuration file."""
        try:
            if not self.config_path.exists():
                logger.warning(
                    f"Pattern configuration file not found: {self.config_path}"
                )
                logger.info("Using default patterns...")
                self._initialize_default_patterns()
                return

            with open(self.config_path, "r") as f:
                config_data = yaml.safe_load(f)

            logger.info(f"Loading patterns from: {self.config_path}")

            # Load tag patterns
            tag_patterns_config = config_data.get("tag_patterns", {})

            # Process each pattern category
            for category, patterns in tag_patterns_config.items():
                if isinstance(patterns, list):
                    for pattern_config in patterns:
                        self._load_pattern_from_config(pattern_config)
                else:
                    # Handle nested categories
                    for subcategory, subpatterns in patterns.items():
                        if isinstance(subpatterns, list):
                            for pattern_config in subpatterns:
                                self._load_pattern_from_config(pattern_config)

            # Load document patterns
            document_patterns_config = config_data.get("document_patterns", [])
            for pattern_config in document_patterns_config:
                self._load_document_pattern_from_config(pattern_config)

            logger.info(f"Loaded {len(self.patterns)} patterns from YAML configuration")

        except Exception as e:
            logger.error(f"Error loading patterns from YAML: {e}")
            logger.info("Falling back to default patterns...")
            self._initialize_default_patterns()

    def _load_pattern_from_config(self, pattern_config: Dict[str, Any]):
        """Load a single tag pattern from configuration."""
        try:
            # Convert string equipment type to enum
            equipment_type_str = pattern_config.get("equipment_type", "GENERIC")
            equipment_type = EquipmentType(equipment_type_str.lower())

            # Convert string instrument type to enum if present
            instrument_type = None
            if "instrument_type" in pattern_config:
                instrument_type_str = pattern_config.get("instrument_type")
                instrument_type = InstrumentType(instrument_type_str.lower())

            pattern = TagPattern(
                name=pattern_config["name"],
                pattern=pattern_config["pattern"],
                description=pattern_config["description"],
                equipment_type=equipment_type,
                instrument_type=instrument_type,
                examples=pattern_config.get("examples", []),
                priority=pattern_config.get("priority", 100),
                validation_rules=pattern_config.get("validation_rules", []),
                industry_standard=pattern_config.get("industry_standard"),
            )

            self.register_pattern(pattern)

        except Exception as e:
            logger.error(
                f"Error loading pattern {pattern_config.get('name', 'unknown')}: {e}"
            )

    def _load_document_pattern_from_config(self, pattern_config: Dict[str, Any]):
        """Load a single document pattern from configuration."""
        try:
            # Convert string document type to enum
            document_type_str = pattern_config.get("document_type", "GENERIC")
            document_type = DocumentType(document_type_str.lower())

            pattern = DocumentPattern(
                name=pattern_config["name"],
                pattern=pattern_config["pattern"],
                description=pattern_config["description"],
                document_type=document_type,
                examples=pattern_config.get("examples", []),
                priority=pattern_config.get("priority", 100),
                required_elements=pattern_config.get("required_elements", []),
                optional_elements=pattern_config.get("optional_elements", []),
            )

            # For now, we'll store document patterns in the same registry
            # In a full implementation, you might want a separate document registry
            logger.debug(f"Loaded document pattern: {pattern.name}")

        except Exception as e:
            logger.error(
                f"Error loading document pattern {pattern_config.get('name', 'unknown')}: {e}"
            )

    def _initialize_default_patterns(self):
        """Initialize with a minimal set of default patterns if YAML loading fails."""
        logger.info("Initializing with default patterns...")

        # Create minimal default patterns
        default_patterns = [
            TagPattern(
                name="default_pump",
                pattern=r"\bP[-_]?\d{1,6}[A-Z]?\b",
                description="Default pump pattern",
                equipment_type=EquipmentType.PUMP,
                examples=["P-101", "P101A", "P-10001"],
                priority=50,
                industry_standard="ISA",
            ),
            TagPattern(
                name="default_valve",
                pattern=r"\bV[-_]?\d{1,6}[A-Z]?\b",
                description="Default valve pattern",
                equipment_type=EquipmentType.VALVE,
                examples=["V-101", "V101A", "V-10001"],
                priority=50,
                industry_standard="ISA",
            ),
            TagPattern(
                name="default_instrument",
                pattern=r"\b[A-Z]{2,3}[-_]?\d{1,6}[A-Z]?\b",
                description="Default instrument pattern",
                equipment_type=EquipmentType.INSTRUMENT,
                examples=["FIC-101", "PIC-201", "TIC-10001"],
                priority=40,
                industry_standard="ISA",
            ),
        ]

        for pattern in default_patterns:
            self.register_pattern(pattern)

        logger.info(f"Initialized with {len(default_patterns)} default patterns")

    def register_pattern(self, pattern: TagPattern) -> bool:
        """Register a new pattern in the registry."""
        try:
            self.patterns[pattern.name] = pattern

            # Update equipment index
            if pattern.equipment_type not in self.equipment_index:
                self.equipment_index[pattern.equipment_type] = []
            self.equipment_index[pattern.equipment_type].append(pattern.name)

            # Update instrument index if applicable
            if pattern.instrument_type:
                if pattern.instrument_type not in self.instrument_index:
                    self.instrument_index[pattern.instrument_type] = []
                self.instrument_index[pattern.instrument_type].append(pattern.name)

            logger.debug(f"Registered pattern: {pattern.name}")
            return True

        except Exception as e:
            logger.error(f"Error registering pattern {pattern.name}: {e}")
            return False

    def get_patterns_by_type(self, equipment_type: EquipmentType) -> List[TagPattern]:
        """Get patterns for a specific equipment type."""
        pattern_names = self.equipment_index.get(equipment_type, [])
        return [self.patterns[name] for name in pattern_names]

    def get_patterns_by_instrument_type(
        self, instrument_type: InstrumentType
    ) -> List[TagPattern]:
        """Get patterns for a specific instrument type."""
        pattern_names = self.instrument_index.get(instrument_type, [])
        return [self.patterns[name] for name in pattern_names]

    def get_all_patterns(self) -> List[TagPattern]:
        """Get all registered patterns."""
        return list(self.patterns.values())

    def get_pattern_by_name(self, name: str) -> Optional[TagPattern]:
        """Get a specific pattern by name."""
        return self.patterns.get(name)

    def search_patterns(self, query: str) -> List[TagPattern]:
        """Search patterns by description or name."""
        query_lower = query.lower()
        results = []

        for pattern in self.patterns.values():
            if (
                query_lower in pattern.name.lower()
                or query_lower in pattern.description.lower()
            ):
                results.append(pattern)

        return sorted(results, key=lambda p: p.priority)


class DocumentPatternRegistry:
    """Registry for engineering document naming patterns loaded from YAML configuration."""

    def __init__(self, config_path: Optional[str] = None):
        self.patterns: Dict[str, DocumentPattern] = {}
        self.document_type_index: Dict[DocumentType, List[str]] = {}

        # Default config path - points to tag_patterns.yaml in fn_dm_aliasing directory
        # Go up from engine/ to fn_dm_aliasing/
        if config_path is None:
            config_path = Path(__file__).parent.parent / "tag_patterns.yaml"

        self.config_path = Path(config_path)
        self._load_patterns_from_yaml()

    def _load_patterns_from_yaml(self):
        """Load document patterns from YAML configuration file."""
        try:
            if not self.config_path.exists():
                logger.warning(
                    f"Pattern configuration file not found: {self.config_path}"
                )
                logger.info("Using default document patterns...")
                self._initialize_default_patterns()
                return

            with open(self.config_path, "r") as f:
                config_data = yaml.safe_load(f)

            logger.info(f"Loading document patterns from: {self.config_path}")

            # Load document patterns
            document_patterns_config = config_data.get("document_patterns", [])
            for pattern_config in document_patterns_config:
                self._load_document_pattern_from_config(pattern_config)

            logger.info(
                f"Loaded {len(self.patterns)} document patterns from YAML configuration"
            )

        except Exception as e:
            logger.error(f"Error loading document patterns from YAML: {e}")
            logger.info("Falling back to default document patterns...")
            self._initialize_default_patterns()

    def _load_document_pattern_from_config(self, pattern_config: Dict[str, Any]):
        """Load a single document pattern from configuration."""
        try:
            # Convert string document type to enum
            document_type_str = pattern_config.get("document_type", "GENERIC")
            document_type = DocumentType(document_type_str.lower())

            pattern = DocumentPattern(
                name=pattern_config["name"],
                pattern=pattern_config["pattern"],
                description=pattern_config["description"],
                document_type=document_type,
                examples=pattern_config.get("examples", []),
                priority=pattern_config.get("priority", 100),
                required_elements=pattern_config.get("required_elements", []),
                optional_elements=pattern_config.get("optional_elements", []),
            )

            self.register_pattern(pattern)

        except Exception as e:
            logger.error(
                f"Error loading document pattern {pattern_config.get('name', 'unknown')}: {e}"
            )

    def _initialize_default_patterns(self):
        """Initialize with a minimal set of default document patterns if YAML loading fails."""
        logger.info("Initializing with default document patterns...")

        # Create minimal default document patterns
        default_patterns = [
            DocumentPattern(
                name="default_pid",
                pattern=r"\bP&?ID[-_]?\d{4,6}[-_]?[A-Z0-9]*\b",
                description="Default P&ID pattern",
                document_type=DocumentType.PID,
                examples=["P&ID-2001", "PID_2001_A"],
                priority=30,
                required_elements=["drawing_number"],
                optional_elements=["revision"],
            ),
            DocumentPattern(
                name="default_drawing",
                pattern=r"\b[A-Z]{2,4}[-_]?\d{4,8}[-_]?[A-Z0-9]*\b",
                description="Default engineering drawing pattern",
                document_type=DocumentType.GENERIC,
                examples=["ENG-2001", "DWG_123456"],
                priority=80,
                required_elements=["drawing_number"],
                optional_elements=[],
            ),
        ]

        for pattern in default_patterns:
            self.register_pattern(pattern)

        logger.info(
            f"Initialized with {len(default_patterns)} default document patterns"
        )

    def register_pattern(self, pattern: DocumentPattern) -> bool:
        """Register a new document pattern."""
        try:
            # Validate pattern
            re.compile(pattern.pattern)

            self.patterns[pattern.name] = pattern

            # Update document type index
            if pattern.document_type not in self.document_type_index:
                self.document_type_index[pattern.document_type] = []
            self.document_type_index[pattern.document_type].append(pattern.name)

            logger.debug(f"Registered document pattern: {pattern.name}")
            return True

        except Exception as e:
            logger.error(f"Error registering document pattern {pattern.name}: {e}")
            return False

    def get_patterns_by_type(
        self, document_type: DocumentType
    ) -> List[DocumentPattern]:
        """Get patterns for a specific document type."""
        pattern_names = self.document_type_index.get(document_type, [])
        return [self.patterns[name] for name in pattern_names]

    def get_all_patterns(self) -> List[DocumentPattern]:
        """Get all registered patterns."""
        return list(self.patterns.values())

    def get_pattern_by_name(self, name: str) -> Optional[DocumentPattern]:
        """Get a specific pattern by name."""
        return self.patterns.get(name)


class PatternValidator:
    """Utility class for validating and testing tag patterns."""

    def __init__(
        self,
        tag_registry: StandardTagPatternRegistry,
        doc_registry: DocumentPatternRegistry,
    ):
        self.tag_registry = tag_registry
        self.doc_registry = doc_registry

    def validate_tag_pattern(
        self, pattern: TagPattern, test_cases: List[Tuple[str, bool]]
    ) -> Dict[str, Any]:
        """Validate a tag pattern against test cases."""
        results = {
            "pattern_name": pattern.name,
            "pattern": pattern.pattern,
            "total_tests": len(test_cases),
            "passed": 0,
            "failed": 0,
            "failures": [],
        }

        try:
            compiled_pattern = re.compile(pattern.pattern)

            for test_input, expected_match in test_cases:
                match = bool(compiled_pattern.search(test_input))

                if match == expected_match:
                    results["passed"] += 1
                else:
                    results["failed"] += 1
                    results["failures"].append(
                        {
                            "input": test_input,
                            "expected": expected_match,
                            "actual": match,
                        }
                    )

            results["success_rate"] = results["passed"] / results["total_tests"]

        except re.error as e:
            results["error"] = f"Pattern compilation error: {e}"

        return results

    def test_pattern_coverage(self, text: str) -> Dict[str, Any]:
        """Test how well patterns cover tags in a given text."""
        results = {
            "text_length": len(text),
            "tag_matches": {},
            "document_matches": {},
            "total_tag_matches": 0,
            "total_document_matches": 0,
            "coverage_analysis": {},
        }

        # Test tag patterns
        for pattern in self.tag_registry.get_all_patterns():
            try:
                compiled_pattern = re.compile(pattern.pattern)
                matches = compiled_pattern.findall(text)

                if matches:
                    results["tag_matches"][pattern.name] = {
                        "pattern": pattern.pattern,
                        "matches": matches,
                        "count": len(matches),
                        "equipment_type": pattern.equipment_type.value,
                        "priority": pattern.priority,
                    }
                    results["total_tag_matches"] += len(matches)

            except re.error as e:
                logger.error(f"Error testing pattern {pattern.name}: {e}")

        # Test document patterns
        for pattern in self.doc_registry.get_all_patterns():
            try:
                compiled_pattern = re.compile(pattern.pattern)
                matches = compiled_pattern.findall(text)

                if matches:
                    results["document_matches"][pattern.name] = {
                        "pattern": pattern.pattern,
                        "matches": matches,
                        "count": len(matches),
                        "document_type": pattern.document_type.value,
                        "priority": pattern.priority,
                    }
                    results["total_document_matches"] += len(matches)

            except re.error as e:
                logger.error(f"Error testing document pattern {pattern.name}: {e}")

        # Coverage analysis
        results["coverage_analysis"] = {
            "patterns_with_matches": len(results["tag_matches"])
            + len(results["document_matches"]),
            "total_patterns_tested": len(self.tag_registry.get_all_patterns())
            + len(self.doc_registry.get_all_patterns()),
            "pattern_hit_rate": (
                len(results["tag_matches"]) + len(results["document_matches"])
            )
            / (
                len(self.tag_registry.get_all_patterns())
                + len(self.doc_registry.get_all_patterns())
            ),
        }

        return results

    def suggest_patterns(
        self, text: str, min_frequency: int = 2
    ) -> List[Dict[str, Any]]:
        """Suggest new patterns based on text analysis."""
        suggestions = []

        # Find potential tag-like patterns
        potential_tags = re.findall(r"\b[A-Z]{1,4}[-_]?\d{2,6}[A-Z0-9]?\b", text)

        # Count frequency
        tag_frequency = {}
        for tag in potential_tags:
            tag_frequency[tag] = tag_frequency.get(tag, 0) + 1

        # Analyze patterns
        pattern_groups = {}
        for tag, freq in tag_frequency.items():
            if freq >= min_frequency:
                # Extract pattern structure
                pattern_key = re.sub(r"\d+", "N", re.sub(r"[A-Z]$", "X", tag))

                if pattern_key not in pattern_groups:
                    pattern_groups[pattern_key] = []
                pattern_groups[pattern_key].append((tag, freq))

        # Generate suggestions
        for pattern_key, examples in pattern_groups.items():
            if len(examples) >= min_frequency:
                # Create regex pattern
                regex_pattern = pattern_key.replace("N", r"\d+").replace("X", r"[A-Z]?")
                regex_pattern = f"\\b{regex_pattern}\\b"

                suggestions.append(
                    {
                        "suggested_pattern": regex_pattern,
                        "pattern_structure": pattern_key,
                        "examples": [ex[0] for ex in examples],
                        "total_frequency": sum(ex[1] for ex in examples),
                        "unique_instances": len(examples),
                    }
                )

        return sorted(suggestions, key=lambda x: x["total_frequency"], reverse=True)


# Example usage and testing
def main():
    """Example usage of the tag pattern library with YAML configuration."""

    # Initialize registries with YAML configuration
    print("Loading tag patterns from YAML configuration...")
    tag_registry = StandardTagPatternRegistry()
    doc_registry = DocumentPatternRegistry()
    validator = PatternValidator(tag_registry, doc_registry)

    # Example text with various tags and document references
    test_text = """
    The main feed pump P-101 is connected to flow control valve FCV-2001A.
    Temperature indicator TIC-301 monitors the reactor R-201 outlet.
    Refer to P&ID-2001-Rev-C for complete piping layout.
    Safety valve PSV-101 protects vessel V-301 from overpressure.
    See specification SPEC-4567 and datasheet DS-1234 for equipment details.
    Heat exchanger E-401 cooling water pump CWP-501B.
    Isometric drawing ISO-3001-A shows pipe routing details.
    """

    print("Tag Pattern Library Example")
    print("=" * 50)

    # Test pattern coverage
    print("\n1. Pattern Coverage Analysis:")
    coverage_results = validator.test_pattern_coverage(test_text)

    print(f"Total tag matches: {coverage_results['total_tag_matches']}")
    print(f"Total document matches: {coverage_results['total_document_matches']}")
    print(
        f"Pattern hit rate: {coverage_results['coverage_analysis']['pattern_hit_rate']:.2%}"
    )

    print("\nTag Matches:")
    for pattern_name, match_info in coverage_results["tag_matches"].items():
        print(
            f"  {pattern_name}: {match_info['matches']} ({match_info['equipment_type']})"
        )

    print("\nDocument Matches:")
    for pattern_name, match_info in coverage_results["document_matches"].items():
        print(
            f"  {pattern_name}: {match_info['matches']} ({match_info['document_type']})"
        )

    # Test pattern suggestions
    print("\n2. Pattern Suggestions:")
    suggestions = validator.suggest_patterns(test_text, min_frequency=1)

    for i, suggestion in enumerate(suggestions[:5], 1):
        print(f"  {i}. Pattern: {suggestion['suggested_pattern']}")
        print(f"     Examples: {suggestion['examples']}")
        print(f"     Frequency: {suggestion['total_frequency']}")
        print()

    # Show available patterns by equipment type
    print("3. Available Patterns by Equipment Type:")
    for equipment_type in EquipmentType:
        patterns = tag_registry.get_patterns_by_type(equipment_type)
        if patterns:
            print(f"\n{equipment_type.value.title()}:")
            for pattern in patterns[:3]:  # Show first 3
                print(f"  - {pattern.name}: {pattern.description}")
                print(f"    Pattern: {pattern.pattern}")
                print(f"    Examples: {pattern.examples[:3]}")


if __name__ == "__main__":
    main()
