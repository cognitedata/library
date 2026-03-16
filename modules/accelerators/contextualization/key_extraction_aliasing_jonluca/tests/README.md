# Tests Directory

This directory contains comprehensive tests for the Key Extraction and Aliasing System, organized into unit and integration tests.

## Directory Structure

```
tests/
├── __init__.py                            # Test package initialization
├── conftest.py                            # Pytest configuration and shared fixtures
├── fixtures/                              # Shared test data and fixtures
│   ├── __init__.py
│   └── sample_data.py                     # Sample data following CDF Core Data Model
├── unit/                                  # Unit tests - test individual components
│   ├── __init__.py
│   ├── test_basic.py                      # Basic functionality tests
│   ├── test_extraction_handlers.py        # Individual extraction method handlers (low-level)
│   ├── test_extraction_engine.py          # Full KeyExtractionEngine (high-level)
│   ├── test_aliasing_handlers.py          # Individual aliasing transformers (low-level)
│   └── test_aliasing_engine.py            # Full AliasingEngine (high-level)
├── integration/                           # Integration tests - test multiple components
│   ├── __init__.py
│   ├── test_key_extraction_scenarios.py   # Real-world extraction scenarios
│   └── test_workflow.py                   # End-to-end workflow tests
└── results/                               # Test output and results
    ├── __init__.py
    ├── *.json                            # Test result files (gitignored)
    └── *.md                              # Test documentation
```

## Test Organization

### Unit Tests (`tests/unit/`)

Test individual components in isolation:

#### test_basic.py (15 tests)
- Basic initialization and configuration
- Placeholder tests for future implementation

#### test_extraction_handlers.py (20 tests)
- Individual extraction method handler classes
- Regex extraction handler
- Fixed width extraction handler
- Token reassembly handler
- Heuristic handler
- Rule priority and ordering
- Source field handling
- Confidence calculation
- Validation and filtering

#### test_extraction_engine.py (11 tests) ✅
- KeyExtractionEngine initialization and orchestration
- Single and batch asset extraction
- Multi-field extraction (name, description, metadata)
- ISA standard patterns
- Confidence filtering and validation
- Edge cases (empty fields, special characters, missing data)
- CDF Core Data Model integration
- **Status**: All tests passing

#### test_aliasing_handlers.py (12 tests)
- Individual transformer classes tested in isolation
- CharacterSubstitutionTransformer
- PrefixSuffixTransformer
- RegexSubstitutionTransformer
- CaseTransformationTransformer
- SeparatorNormalizationTransformer
- EquipmentTypeExpansionTransformer
- RelatedInstrumentsTransformer
- HierarchicalExpansionTransformer
- DocumentAliasesTransformer

#### test_aliasing_engine.py (28 tests)
- AliasingEngine initialization and orchestration
- Rule types through full engine
- Context handling (equipment_type, site, unit)
- Validation mechanisms
- Rule priority and ordering
- Edge cases and error handling
- Performance and scalability
- Sample data integration

### Integration Tests (`tests/integration/`)

Test multiple components working together:

#### test_key_extraction_scenarios.py (14 tests) ✅
Real-world extraction scenarios using CDF Core Data Model:

**TestRegexExtraction** (3 tests)
- Regex extraction on ISO standard instrument names
- Unit prefix extraction (e.g., `A-FIC-1001`)
- ISA standard ISA naming conventions

**TestFixedWidthExtraction** (3 tests)
- Fixed width parsing on timeseries metadata
- Column-based tag extraction
- Position-based pattern matching

**TestTokenReassemblyExtraction** (3 tests)
- Token reassembly extraction
- Hierarchical tag assembly
- Format validation

**TestHeuristicExtraction** (7 tests)
- Positional detection after keywords
- Extraction from parentheses
- Keyword variant patterns
- Frequency analysis strategy
- Context inference strategy
- Multiple strategy combination
- Confidence modifiers

#### test_workflow.py
- End-to-end extraction and aliasing workflows
- Configuration management
- Pattern library integration

## Test Fixtures and Sample Data

### Location: `tests/fixtures/sample_data.py`

All sample data follows the **CDF Core Data Model** structure:

#### CogniteAsset Functions
- `get_cdf_assets()` - Sample CogniteAsset records
  - Pumps, Valves, Tanks, Reactors, Heat Exchangers
  - Instruments (flow, pressure, temperature, level)

#### CogniteFile Functions
- `get_cdf_files()` - Sample CogniteFile records
  - P&ID documents
  - PFD documents
  - Isometric drawings
  - Engineering drawings
  - Specifications

#### CogniteTimeseries Functions
- `get_cdf_timeseries()` - Sample CogniteTimeseries records
- `get_fixed_width_timeseries()` - Fixed width format for testing
- `get_token_reassembly_timeseries()` - Token-based formats

#### Utility Functions
- `get_simple_asset()` - Simple asset for basic testing
- `get_sample_tags()` - Sample tags for aliasing tests
- `get_heuristic_test_assets()` - Complex descriptions for heuristic extraction

## Running Tests

### Run All Tests
```bash
pytest tests/ -v
```

### Run Unit Tests
```bash
pytest tests/unit/ -v
```

### Run Integration Tests
```bash
pytest tests/integration/ -v
```

### Run Specific Test File
```bash
pytest tests/unit/test_extraction_engine.py -v
pytest tests/integration/test_key_extraction_scenarios.py -v
```

### Run Specific Test Class
```bash
pytest tests/integration/test_key_extraction_scenarios.py::TestRegexExtraction -v
pytest tests/integration/test_key_extraction_scenarios.py::TestHeuristicExtraction -v
```

### Run with Coverage
```bash
pytest tests/ --cov=src --cov-report=html
```

### Run Quiet Mode
```bash
pytest tests/ -q
```

## Test Results Summary

### Current Test Count
- **Unit Tests**: 86 tests
  - test_extraction_engine.py: 11 tests ✅
  - test_aliasing_engine.py: 28 tests ⚠️
  - test_aliasing_handlers.py: 12 tests ⚠️
  - test_extraction_handlers.py: 20 tests ⚠️
  - test_basic.py: 15 tests

- **Integration Tests**: 14 tests ✅
  - test_key_extraction_scenarios.py: 14 tests ✅

**Total**: 100+ tests

### Passing Tests
- ✅ All integration scenario tests (14/14)
- ✅ All extraction engine tests (11/11)
- ⚠️ Some handler and aliasing tests need fixes

## Test Coverage

### Key Extraction
- ✅ Regex pattern extraction
- ✅ Fixed width parsing
- ✅ Token reassembly
- ✅ Heuristic extraction
- ✅ Multi-field extraction
- ✅ ISA standard patterns
- ✅ Confidence filtering
- ✅ Validation and limits
- ✅ CDF Core Data Model integration

### Aliasing
- ✅ Character substitution
- ✅ Separator normalization
- ✅ Prefix/suffix operations
- ✅ Equipment type expansion
- ✅ Related instrument generation
- ✅ Hierarchical expansion
- ✅ Context-based transformations
- ✅ Validation and limits

### Integration Scenarios
- ✅ ISO standard instrument tag extraction
- ✅ Fixed width timeseries parsing
- ✅ Token reassembly for hierarchical tags
- ✅ Unit prefix handling
- ✅ Heuristic multi-strategy extraction
- ✅ CDF CDM structure compliance

## Test Organization Principles

1. **Unit Tests**: Test individual components/handlers in isolation
2. **Integration Tests**: Test multiple components working together
3. **Fixtures**: Shared test data to avoid duplication
4. **Clear Naming**: Test names describe what they test
5. **Isolation**: Each test is independent and doesn't rely on other tests
6. **CDF Compliance**: All sample data follows CDF Core Data Model structure

## Test Data

All test data uses the CDF Core Data Model structure for:
- **CogniteAsset**: `externalId`, `name`, `description`, `metadata`
- **CogniteFile**: `externalId`, `name`, `description`, `metadata.documentType`
- **CogniteTimeseries**: `externalId`, `name`, `description`, `metadata.unit`

See `tests/fixtures/sample_data.py` for available fixtures.

## Documentation

- `TEST_STRUCTURE.md` - Detailed test organization
- `TEST_SUITE_SUMMARY.md` - Test suite overview
- `docs/TEST_RENAME_SUMMARY.md` - Naming convention explanation
- `docs/TEST_SEPARATION_COMPLETE.md` - Test separation details
- `docs/UNIT_TESTS_SUMMARY.md` - Unit test details

## Test Naming Convention

### Handlers Tests (Low-Level)
- `test_*_handlers.py` - Test individual component classes directly
- Example: `test_extraction_handlers.py` tests `RegexExtractionHandler.extract()`

### Engine Tests (High-Level)
- `test_*_engine.py` - Test full engine orchestration
- Example: `test_extraction_engine.py` tests `KeyExtractionEngine.extract_keys()`

## Troubleshooting

### Import Errors
If you see import errors, ensure you're in the project root:
```bash
cd /path/to/key_extraction_aliasing
pytest tests/
```

### Missing Test Data
All test fixtures are in `tests/fixtures/sample_data.py`. Import them:
```python
from tests.fixtures.sample_data import get_cdf_assets, get_cdf_timeseries
```

### Running Specific Tests
Use pytest markers or test paths to run specific tests:
```bash
pytest tests/integration/test_key_extraction_scenarios.py::TestRegexExtraction::test_regex_extraction -v
```

## Contributing

When adding new tests:
1. Use CDF Core Data Model structure in sample data
2. Add fixtures to `tests/fixtures/sample_data.py`
3. Place unit tests in `tests/unit/`
4. Place integration tests in `tests/integration/`
5. Follow naming convention (handlers vs engine)
6. Document test purpose in docstrings
7. Ensure tests are isolated and independent
