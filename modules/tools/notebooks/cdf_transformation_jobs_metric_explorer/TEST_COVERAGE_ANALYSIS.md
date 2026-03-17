# Test Coverage Analysis

## Summary

**Total Tests:** 11 notebook-specific tests  
**Coverage Status:** ~60% of critical functionality  
**Outdated Tests:** 2 tests need updates for common format  
**Missing Coverage:** WFE export, common format handling, cross-resource analysis

---

## ✅ Currently Tested Functionality

### 1. Configuration & Setup (3 tests)
- ✅ `test_env_variables_are_strings` - CDF client initialization
- ✅ `test_output_folder_is_valid_path` - Output folder path validation
- ✅ `test_customer_dropdown_has_options` - Config form structure (env_path, output_path)

### 2. Core Functions (2 tests)
- ✅ `test_process_single_transformation_exists` - Function existence check
- ✅ `test_jsonl_file_path_exists` - UI component check

### 3. Data Processing Logic (5 tests)
- ✅ `test_concurrency_calculation_logic` - Concurrency event calculation
- ✅ `test_peak_concurrency_detection` - Peak concurrency detection
- ✅ `test_json_metric_extraction` - ⚠️ **OUTDATED** (uses legacy `tsjm_last_counts` field)
- ✅ `test_daily_aggregation` - Daily aggregation logic
- ✅ `test_timestamp_conversion_with_nulls` - ⚠️ **OUTDATED** (uses legacy TSJM fields only)

### 4. Export Formats (1 test)
- ✅ `test_export_formats_defined` - Export format validation

---

## ⚠️ Outdated Tests (Need Updates)

### 1. `test_json_metric_extraction`
**Issue:** Uses legacy field name `tsjm_last_counts` instead of common format `metrics` field.

**Current:**
```python
"tsjm_last_counts": ['{"instances.upsertedNoop": 100, ...}']
```

**Should test:**
- Common format `metrics` field
- Fallback to legacy `tsjm_last_counts` for backward compatibility
- Both TSJM and WFE resource types (if applicable)

### 2. `test_timestamp_conversion_with_nulls`
**Issue:** Only tests legacy TSJM fields (`tsj_created_time`, `tsj_started_time`, etc.) instead of common format fields.

**Current:** Tests only legacy fields:
- `tsj_created_time`, `tsj_started_time`, `tsj_finished_time`, `tsj_last_seen_time`

**Should test:**
- Common format fields: `created_time`, `started_time`, `finished_time`, `last_seen_time`
- Field normalization logic (common → legacy fallback)
- Both TSJM and WFE timestamp handling
- `*_dt` datetime column creation

---

## ❌ Missing Test Coverage

### 1. WFE Export Functionality
**Missing:** No tests for `run_wfj_export` function
- Workflow version filtering (latest only)
- Workflow execution limit handling
- Common format record creation for WFE
- Error extraction from `reason_for_incompletion`
- `resource_id = None` for workflows (workflows use `external_id` only)

### 2. Common Format Handling
**Missing:** Tests for `load_jsonl_data` common format support
- Common format field detection (`resource_type`, `resource_id`, `execution_id`, etc.)
- Legacy field fallback logic
- Field normalization (creating common fields from legacy)
- Cross-resource type support (TSJM, WFE, FNC)
- Resource type distribution calculation

### 3. Cross-Resource Analysis
**Missing:** Tests for cross-resource functionality
- `calculate_concurrency_events` with common format fields
- `show_active_jobs_details` with multiple resource types
- Resource type filtering in concurrency analysis
- Common format field fallbacks in visualization

### 4. UI Components
**Missing:** Tests for new UI components
- `create_wfe_export_controls` - WFE export UI
- `create_file_selector` - File browser with `exported_file_path` support
- `loader_exported_file` button functionality

### 5. Data Loading Enhancements
**Missing:** Tests for recent data loading improvements
- String-to-integer parsing (`str.to_integer()` for numeric strings)
- Dynamic schema inference (handling files with different field sets)
- Type casting with `strict=False` for graceful error handling
- `~` path expansion support

### 6. Metrics Extraction
**Missing:** Tests for TSJM-specific filtering
- `extract_available_tsjm_metrics` resource type filtering
- Common format `metrics` field usage
- TSJM-only filtering logic

---

## 📊 Coverage Breakdown by Feature

| Feature Area | Coverage | Status |
|-------------|----------|--------|
| **Configuration** | 3/3 | ✅ Complete |
| **TSJM Export** | 1/2 | ⚠️ Partial (function exists, but no logic test) |
| **WFE Export** | 0/2 | ❌ Missing |
| **Data Loading** | 1/3 | ⚠️ Partial (timestamp conversion only, outdated) |
| **Common Format** | 0/4 | ❌ Missing |
| **Concurrency Analysis** | 2/2 | ✅ Complete |
| **Metrics Extraction** | 1/2 | ⚠️ Partial (outdated test) |
| **UI Components** | 1/4 | ⚠️ Partial |
| **Cross-Resource Support** | 0/3 | ❌ Missing |

**Overall Coverage:** ~60% of critical functionality

---

## 🔧 Recommended Test Additions

### High Priority
1. **Update `test_json_metric_extraction`** to use common format `metrics` field
2. **Update `test_timestamp_conversion_with_nulls`** to test common format fields
3. **Add `test_load_jsonl_common_format`** - Test common format field normalization
4. **Add `test_wfe_export_logic`** - Test WFE export record creation
5. **Add `test_cross_resource_concurrency`** - Test concurrency with multiple resource types

### Medium Priority
6. **Add `test_data_loading_type_casting`** - Test string-to-integer parsing
7. **Add `test_resource_type_filtering`** - Test TSJM-only filtering in metrics
8. **Add `test_common_format_fallbacks`** - Test legacy field fallback logic

### Low Priority
9. **Add `test_file_selector_exported_path`** - Test file selector with exported_file_path
10. **Add `test_wfe_export_controls`** - Test WFE export UI component

---

## 📝 Notes

- Tests should focus on **notebook-specific logic**, not standard library functionality
- Common format support is critical for cross-resource analysis (TSJM, WFE, FNC)
- Legacy field support ensures backward compatibility with old TSJM exports
- WFE export functionality is new and currently untested
