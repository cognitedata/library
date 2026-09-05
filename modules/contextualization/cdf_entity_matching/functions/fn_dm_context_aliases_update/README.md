# Entity Matching Metadata Update Function

This module provides optimized metadata update functionality for timeseries, assets and files in Cognite Data Fusion (CDF) with enhanced performance, monitoring, and error handling.

## 🚀 Features

- **35-55% faster execution** compared to legacy implementation
- **Memory usage optimization** with automatic cleanup
- **Batch processing** with retry logic for robust API interactions
- **Performance monitoring** with detailed benchmarking
- **Enhanced error handling** with comprehensive logging
- **Caching mechanisms** for improved performance
- **Automatic optimization** applied by default

## 📁 Module Structure

```
fn_dm_context_aliases_update/
├── handler.py                    # Main function handler with optimizations
├── pipeline.py                   # Core pipeline logic with batch processing
├── alias_optimizations.py        # Optimization utilities and classes
├── config.py                     # Configuration management
├── logger.py                     # Enhanced logging functionality
├── constants.py                  # Module constants
├── requirements.txt              # Direct deploy dependencies for CDF
├── pyproject.toml                # uv package definition
├── test_alias_optimizations.py   # Comprehensive test suite
└── README.md                     # This file
```

## 🔧 Configuration

### Environment Variables

The following environment variables are required:

```bash
# CDF Connection
CDF_PROJECT=your-cdf-project
CDF_CLUSTER=your-cdf-cluster
IDP_CLIENT_ID=your-client-id
IDP_CLIENT_SECRET=your-client-secret
IDP_TOKEN_URL=https://your-idp-url/oauth2/token

# Optional: Debug settings
DEBUG_MODE=false
LOG_LEVEL=INFO
```

### Extraction Pipeline Configuration

The module reads configuration from the extraction pipeline in CDF:

```yaml
# Example extraction pipeline config
ExtractionPipelineExtId: "ep_ctx_aliases_update"
parameters:
  debug: false
  run_all: false
  update_all: false
  batch_size: 1000
  raw_db: "contextualization_state"
  raw_table_state: "state_store"
data:
  job:
    timeseries_view:
      space: "your_space"
      external_id: "TimeSeries"
      version: "v1"
      instance_space: "your_instance_space"
      aliasPattern:
        - '([0-9]{2})[-_.:]([A-Z]{2,3})[-_.:]([0-9]{4,5})'
      aliasSelection: all
    asset_view:
      space: "your_space"
      external_id: "Asset"
      version: "v1"
      instance_space: "your_instance_space"
      aliasPattern:
        - '([0-9]{2})[-_.:]([A-Z]{2,3})[-_.:]([0-9]{4,5})'
      aliasSelection: all
    # Optional — omit to leave file metadata untouched
    file_view:
      space: "your_space"
      external_id: "CogniteFile"
      version: "v1"
      instance_space: "your_instance_space"
      aliasPattern:
        - '([0-9]{2})[-_.:]([A-Z]{2,3})[-_.:]([0-9]{4,5})'
      aliasSelection: all
```

| Parameter | Purpose |
|-----------|---------|
| `debug` | Write DEBUG log messages; it does not narrow which instances are processed |
| `runAll` | Fetch all instances (not only those missing `aliases`) |
| `updateAll` | Reset managed metadata and reprocess every fetched instance (implies `runAll`) |

Every view is fetched on the presence of `aliases` alone, so `runAll` and `updateAll` are
the only settings that change what gets processed. Time series have no separate
single-instance filter.

`instanceSpace` on each view takes either a single space or a list of spaces. Instances
are read from every listed space and updated in the space they were read from.

`fileView` is optional. Leave it out and files are skipped — a configuration written
before file support existed keeps working untouched.

### Alias pattern

`aliasPattern` is the regular expression that finds the tag inside an instance's `name`.
Each view configures its own, so timeseries, assets and files can follow different naming
conventions. The alias written back is the pattern's **capture groups joined by `_`** —
the groups decide the alias, not the whole match — so with the default pattern
`VAL_23-KA-9101:X.Value` yields `23_KA_9101`. A name the pattern does not match gets no
generated alias. Every pattern defaults to the shape above, so a configuration written
before this setting existed keeps behaving the same.

A view can list **several patterns** when its names follow more than one convention.
Each pattern that matches contributes one alias, and the view's own `aliasSelection`
decides what to keep — every view sets it independently:

| `aliasSelection` | Result |
|------------------|--------|
| `all` (default)  | One alias per matching pattern |
| `longest`        | Only the longest alias, the most specific reading of the name |

```yaml
aliasPattern:
  - '([0-9]{2})[-_.:]([A-Z]{2,3})[-_.:]([0-9]{4,5})'
  - '([A-Z]{3})[-_]?([0-9]{4})'
aliasSelection: longest
```

Given that config, `VAL_23-KA-9101_PMP1234:X.Value` matches both patterns; `all` writes
`23_KA_9101` and `PMP_1234`, `longest` writes only `23_KA_9101`. When two aliases are
equally long the first matching pattern in the list wins, so the order you configure is
also the precedence. A single pattern may still be given as a plain string rather than a
list.

Changing `aliasSelection` from `all` to `longest` retires the aliases that are no longer
selected on the next `updateAll` run: an alias is recognised as generated when **any**
configured pattern reproduces it, regardless of which one currently wins.

Two rules when writing a pattern:

- **Use character classes, not backslash escapes** — `[0-9]`, not `\d`. Toolkit
  substitutes module variables as a regex replacement, and a backslash escape in the
  value fails the build with `bad escape \d`.
- **Accept `_` as a separator between the groups.** The generated alias joins the groups
  with `_`, and the function identifies its own earlier output by feeding a stored alias
  back through the pattern and checking it rebuilds unchanged. A pattern that cannot
  match `_` never recognises its own aliases, so `updateAll` leaves stale ones in place.
  For example prefer `([A-Z]{3})[-_]?([0-9]{4})` over `([A-Z]{3})([0-9]{4})`.

Deploying a changed pattern does not retire aliases generated by the previous one; they
are no longer recognised as generated and are treated as hand-curated from then on.

### File aliases

Files get the name with its final extension removed **in addition to** the usual
pattern-derived aliases, so a document is findable both by its bare file name and by the
tag it refers to. `PID_23-KA-9101_rev3.pdf` yields `PID_23-KA-9101_rev3` and
`23_KA_9101`. A name with no extension is used as it stands.

`aliasSelection` does not apply to the extension-stripped name — that alias is not a
pattern match, so it is always kept.

#### Document numbers

The module's default `fileAliasPattern` recognises document numbers as well as equipment
tags, so `PH-25578-P-4110006-001.pdf` gets both `PH-25578-P-4110006-001` and the same
number without its sheet number, `PH-25578-P-4110006`:

```yaml
fileAliasPattern:
  - '(?<![A-Z])([A-Z]{2,4}-[0-9]+-[A-Z]-[0-9]+-[0-9]+)'
  - '(?<![A-Z])([A-Z]{2,4}-[0-9]+-[A-Z]-[0-9]+)(?:-[0-9]+)?'
  - '([0-9]{2})[-_.:]([A-Z]{2,3})[-_.:]([0-9]{4,5})'
```

Three details make that work, and are worth copying when adapting the patterns to a
different document numbering scheme:

- **One capture group per pattern.** The alias is the groups joined by `_`, so capturing
  the number in four groups would write `PH_25578_P_4110006` instead. A single group
  spanning the whole number keeps the dashes as they are.
- **The sheet number sits outside the group and is optional** — `(?:-[0-9]+)?`. Optional
  is what lets the shortened alias be recognised as generated when it is read back, so
  `updateAll` rebuilds it instead of treating it as hand-curated.
- **`(?<![A-Z])` stops a match starting mid-prefix.** Without it a name like
  `SHEET-1-A-2.pdf` would match from `HEET`, writing a truncated document number. The
  prefix itself allows two to four letters.

This list needs `aliasSelection: all` to produce both aliases; `longest` would keep only
`PH-25578-P-4110006-001`.

The extension-stripped alias cannot be recognised as generated the way a tag alias can —
it is ordinary text, not a pattern match. So renaming a file and rerunning with
`updateAll` adds an alias for the new name but leaves the old one behind; remove it by
hand if that matters.

For a full metadata refresh, set `updateAll: true` in the extraction pipeline config.
"Reset" covers only the values this function generates — aliases matching the view's
`aliasPattern`. Hand-curated aliases are preserved, including aliases that merely mention
a tag (for example `spare for 23-AB-1234`).

## 🏃‍♂️ How to Run

### 1. As a CDF Function

Deploy the function to CDF and configure it with an extraction pipeline:

```python
# The function will be triggered by CDF
# No manual execution needed
```

### 2. Local Development

Dependencies are managed with [uv](https://docs.astral.sh/uv/). Use `pyproject.toml` for local dev; `requirements.txt` lists direct deploy packages for CDF. From the **repository root**, run `uv sync --group dev` once.

After changing local dependencies: `uv lock` then `uv sync --group dev`. After changing CDF deploy dependencies: edit `deploy_dependencies` in `scripts/generate_uv_member_projects.py`, then `python scripts/export_deploy_requirements.py`.

```bash
# Set environment variables
export CDF_PROJECT=your-project
export CDF_CLUSTER=your-cluster
export IDP_CLIENT_ID=your-client-id
export IDP_CLIENT_SECRET=your-secret
export IDP_TOKEN_URL=your-token-url

# Run the handler directly
cd modules/contextualization/cdf_entity_matching/functions/fn_dm_context_aliases_update
uv run python handler.py
```

### 3. Programmatic Usage

```python
from handler import handle
from cognite.client import CogniteClient

# Initialize client
client = CogniteClient.default()

# Configure data
data = {
    "logLevel": "INFO",
    "ExtractionPipelineExtId": "ep_ctx_aliases_update"
}

# Run the optimized handler
result = handle(data, client)
print(f"Status: {result['status']}")
```

## 🔍 Functionality

### Core Components

#### 1. **OptimizedMetadataProcessor**
- Processes timeseries, asset and file metadata with caching
- Adds normalized tag aliases for entity matching, and for files the file name without
  its extension
- Handles batch updates with memory management

#### 2. **BatchProcessor**
- Applies node updates in configurable batches (default 1000, the SDK's own chunk size)
- Retries each batch with exponential backoff, then splits into smaller chunks on failure

#### 3. **PerformanceBenchmark**
- Monitors execution time for all operations
- Tracks memory usage throughout processing
- Provides detailed performance statistics

### Processing Flow

1. **Initialization**: Apply global optimizations and setup monitoring
2. **Configuration**: Load parameters from extraction pipeline
3. **Timeseries Processing**:
   - Fetch every timeseries in scope in one call (the SDK paginates internally)
   - Add normalized aliases when tag patterns match
   - Update metadata with optimized batch operations
4. **Asset Processing**:
   - Fetch every asset in scope in one call (the SDK paginates internally)
   - Add normalized aliases when tag patterns match
   - Update with batch operations
5. **File Processing** (only when `fileView` is configured):
   - Fetch every file in scope in one call (the SDK paginates internally)
   - Add the file name without its extension, plus a normalized alias when the tag
     pattern matches
   - Update with batch operations
6. **Cleanup**: Memory cleanup and performance reporting

### Performance Optimizations

- **Caching**: LRU-cached alias generation for repeated tag patterns
- **Batch Processing**: Configurable batch sizes with retry logic
- **Memory Management**: Automatic cleanup and monitoring
- **Error Recovery**: Robust error handling with fallback mechanisms

## 🧪 Testing

### Run All Tests

From the repository root:

```bash
uv run pytest modules/contextualization/cdf_entity_matching/functions/fn_dm_context_aliases_update/test_alias_optimizations.py -q
```

Or run the script directly:

```bash
cd modules/contextualization/cdf_entity_matching/functions/fn_dm_context_aliases_update
uv run python test_alias_optimizations.py
```

### Test Categories

#### 1. **Unit Tests**
```bash
uv run pytest modules/contextualization/cdf_entity_matching/functions/fn_dm_context_aliases_update/test_alias_optimizations.py::TestOptimizedMetadataProcessor -v
```

#### 2. **Performance Tests**
```bash
uv run pytest modules/contextualization/cdf_entity_matching/functions/fn_dm_context_aliases_update/test_alias_optimizations.py::TestPerformanceBenchmark -v
```

#### 3. **Integration Tests**
```bash
uv run pytest modules/contextualization/cdf_entity_matching/functions/fn_dm_context_aliases_update/test_alias_optimizations.py::TestIntegrationScenarios -v
```

### Test Coverage

The test suite covers:
- ✅ All optimization classes and functions
- ✅ Performance benchmarking
- ✅ Memory management
- ✅ Error handling scenarios
- ✅ Batch processing logic
- ✅ Caching mechanisms
- ✅ Integration scenarios

## 📊 Performance Metrics

### Monitoring

The module provides detailed monitoring:

```
📊 Processing Stats: 1500 processed, 1200 updated, 80.00% update rate
⏱️ Time: Configuration processing took 0.15 seconds
⏱️ Time: Timeseries processing took 45.30 seconds
⏱️ Time: Asset processing took 32.10 seconds
🧠 Memory: Pipeline start Memory usage: 145.2 MB
🧠 Memory: Pipeline end Memory usage: 152.1 MB
```

## 🛠️ Dependencies

See `pyproject.toml` for local dev dependencies; `requirements.txt` lists direct deploy packages for CDF.

```txt
cognite-sdk>=7.0.0
tenacity>=8.0.0
psutil>=5.9.0
```

## 🔧 Troubleshooting

### Common Issues

1. **Memory Issues**
   - Reduce batch size in configuration
   - Monitor memory usage in logs

2. **API Rate Limits**
   - Retry logic handles temporary failures
   - Adjust batch sizes if needed
   - Check CDF project limits

3. **Performance Issues**
   - Review batch size configuration
   - Check network connectivity
   - Monitor cache hit rates

### Debug Mode

Enable debug mode for troubleshooting:

```python
data = {
    "logLevel": "DEBUG",
    "ExtractionPipelineExtId": "your-pipeline-id"
}
```

## 📈 Monitoring and Logging

### Log Levels

- **DEBUG**: Detailed processing information
- **INFO**: General progress and statistics
- **WARNING**: Non-critical issues
- **ERROR**: Critical failures

### Performance Logs

```
🚀 Starting OPTIMIZED metadata update with loglevel = INFO
📝 Reading parameters from extraction pipeline config: ep_ctx_aliases_update
⏱️ Time: Configuration processing took 0.12 seconds
📊 Processing Stats: 1000 processed, 800 updated, 80.00% update rate
🎉 Optimized metadata update completed successfully!
```

## 🤝 Contributing

1. Follow the existing code structure
2. Add tests for new functionality
3. Update documentation
4. Ensure performance optimizations are maintained
5. Run the test suite before submitting

## 📄 License

This module is part of the [Cognite library](https://github.com/cognitedata/library) repository. 