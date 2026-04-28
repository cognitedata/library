# Troubleshooting Guide

This guide helps you resolve common issues with the **`cdf_key_extraction_aliasing`** module. The local pipeline CLI is **`module.py`** at the module root; run it from the **repository root** with `PYTHONPATH=.` (see [Quickstart — local `module.py`](../guides/howto_quickstart.md)).

## Common Issues

### 1. Import Errors

#### Problem: ModuleNotFoundError
```
ModuleNotFoundError: No module named 'cognite'
```

#### Solution:
```bash
# Install CDF SDK
pip install cognite-sdk

# Install all dependencies
pip install -r requirements.txt
```

#### Problem: Import errors for project modules
```
ImportError: cannot import name 'KeyExtractionEngine'
```

#### Solution:
```bash
# Ensure you're in the project root directory
cd /path/to/KeyExtraction

# Check Python path
python -c "import sys; print(sys.path)"

# Add current directory to Python path if needed
export PYTHONPATH="${PYTHONPATH}:$(pwd)"
```

### 2. CDF Connection Issues

#### Problem: Authentication failed
```
CogniteAPIError: Authentication failed
```

#### Solutions:
1. **Check API Key**
   ```bash
   # Check .env file exists and contains required variables
   cat .env | grep CDF_PROJECT
   # Verify CDF_API_KEY is set correctly
   ```

2. **Verify Project Name**
   ```bash
   # Check CDF_PROJECT environment variable
   echo $CDF_PROJECT
   ```

3. **Test Connection**
   ```python
   from cognite.client import CogniteClient
   client = CogniteClient(api_key="your_key", project="your_project")
   print(client.assets.list(limit=1))
   ```

#### Problem: Network connectivity issues
```
CogniteAPIError: Connection timeout
```

#### Solutions:
1. **Check Network**
   ```bash
   ping api.cognite.com
   ```

2. **Increase Timeout**
   ```yaml
   cdf:
     timeout: 60
   ```

3. **Check Proxy Settings**
   ```bash
   export HTTP_PROXY=your_proxy
   export HTTPS_PROXY=your_proxy
   ```

### 3. Configuration Issues

#### Problem: YAML syntax errors
```
yaml.scanner.ScannerError: while scanning for the next token
```

#### Solution:
```bash
# Validate YAML syntax
python -c "import yaml; yaml.safe_load(open('config.yaml'))"

# Use online YAML validator
# https://www.yamllint.com/
```

#### Problem: Missing required fields
```
ValueError: Required field 'api_key' is missing
```

#### Solution:
```bash
# Check environment variables
# Verify .env at repository root and run module.py to test connection (from repo root, PYTHONPATH=.)
python modules/accelerators/contextualization/cdf_key_extraction_aliasing/module.py run --dry-run --limit 5

# Create missing environment variables
# Copy env.template to .env and edit with your credentials
cp env.template .env
```

### 4. Key Extraction Issues

#### Problem: No keys extracted
```
ExtractionResult(candidate_keys=[], foreign_key_references=[], document_references=[])
```

#### Solutions:
1. **Timeseries names like `VAL_45-TT-92506:…`** — The default shared regex uses **`(?:\b|(?<=_))`** so a tag can start after `VAL_`; plain `\b` alone does not fire between `_` and a digit in Python. Ensure your config uses the current **`alphanumeric_tag`** from `config/tag_patterns.yaml` / default scope anchor **`&alphanumeric_tag`**.

2. **Check Input Data**
   ```python
   # Verify input data format
   print("Sample data:", sample_data)
   print("Data types:", [type(item) for item in sample_data])
   ```

3. **Review Extraction Rules**
   ```python
   from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_key_extraction.engine.key_extraction_engine import (
       KeyExtractionEngine,
   )

   engine = KeyExtractionEngine({"extraction_rules": [rule], "validation": {}})
   result = engine.extract_keys({"name": "P-101", "id": "t"}, "asset")
   ```

4. **Lower Confidence Threshold**
   ```yaml
   validation:
     min_confidence: 0.3  # Lower from default 0.5
   ```

#### Problem: Low confidence scores
```
Key(value='P-101', confidence=0.2, extraction_type='candidate_key')
```

#### Solutions:
1. **Improve Patterns**
   ```yaml
   # More specific regex patterns
   pattern: "\bP[-_]?\d{2,4}[A-Z]?\b"
   ```

2. **Add More Source Fields**
   ```yaml
   source_fields:
     - field_name: "name"
       required: true
     - field_name: "description"
       required: false
     - field_name: "equipmentType"
       required: false
   ```

### 5. Aliasing Issues

#### Problem: No aliases generated
```
AliasResult(aliases=[], confidence=0.0)
```

#### Solutions:
1. **Check Aliasing Rules**
   ```python
   from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_aliasing.engine.tag_aliasing_engine import (
       AliasingEngine,
   )

   engine = AliasingEngine({"rules": [], "validation": {}})
   result = engine.generate_aliases("P-101", "asset")
   print(result.aliases)
   ```

2. **Enable more rules in `aliasing.config.data.aliasing_rules`**
   ```yaml
   aliasing:
     config:
       data:
         aliasing_rules:
           - name: "character_substitution"
             type: "character_substitution"
             enabled: true
   ```
   The **default CDM scope** only ships a few rules for assets/files; copy patterns from `config/examples/aliasing/` if you need a larger stack.

#### Problem: Too many aliases
```
Generated 150 aliases (limit: 50)
```

#### Solution:
Tune **`aliasing.config.data.validation`** (e.g. `max_aliases_per_tag`) or per-rule `max_aliases_per_input` in the aliasing config loaded by your scope.

### 6. CDF Deployment Issues

#### Problem: Function deployment failed
```
CogniteAPIError: Function deployment failed
```

#### Solutions:
1. **Check Function Code**
   ```python
   # Validate function syntax
   python -c "import main; print('Function code valid')"
   ```

2. **Verify CDF Permissions**
   - Ensure API key has Functions write permissions
   - Check project access rights

3. **Check Resource Limits**
   ```yaml
   deployment:
     function_name: "shorter-name"  # CDF has name length limits
   ```

#### Problem: Workflow execution failed
```
WorkflowRun failed with status: FAILED
```

#### Solutions:
1. **Check Task Dependencies**
   ```python
   # Verify workflow task dependencies
   workflow_config = system.get_workflow_config()
   print("Task dependencies:", workflow_config.tasks)
   ```

2. **Increase Timeouts**
   ```yaml
   deployment:
     workflow_timeout: 7200  # Increase from 3600
   ```

3. **Check Input Data**
   ```python
   # Verify input data format for workflow
   input_data = system.prepare_workflow_input()
   print("Input data:", input_data)
   ```

### 7. Performance Issues

#### Problem: Slow extraction
```
Extraction took 300 seconds for 1000 records
```

#### Solutions:
1. **Reduce Batch Size**
   ```yaml
   cdf:
     batch_size: 50  # Reduce from default (1000) or your current value
   ```

2. **Limit Concurrent Requests**
   ```yaml
   cdf:
     max_concurrent_requests: 5  # Reduce from 10
   ```

3. **Optimize Rules**
   ```yaml
   # Use more efficient regex patterns
   pattern: "P-\d{3}"  # Instead of "P[-_]?\d{2,4}[A-Z]?\b"
   ```

#### Problem: Memory usage high
```
Memory usage: 2GB for 1000 records
```

#### Solutions:
1. **Process in Smaller Batches**
   ```python
   # Process data in chunks
   chunk_size = 100
   for i in range(0, len(data), chunk_size):
       chunk = data[i:i+chunk_size]
       results = engine.extract_keys(chunk)
   ```

2. **Clear Intermediate Results**
   ```python
   # Clear results after processing
   del results
   import gc
   gc.collect()
   ```

### Key Discovery FDM not deployed (incremental mode)

**Symptoms:** Log lines about **RAW** watermark or hash fallback; **`workflow_scope`** validation errors only when FDM views are present.

**Expected behavior:** If **`key_discovery_instance_space`** is set but **Key Discovery** views under [`data_modeling/`](../../data_modeling/) are not deployed (or FDM calls fail), functions **fall back** to legacy RAW watermarks (`scope_wm_*`) and **`EXTRACTION_INPUTS_HASH`** for skip logic. No separate fix is required for correctness.

**To use FDM state:** Deploy the Key Discovery containers/views/datamodel with Cognite Toolkit, set instance/schema spaces to match **`key_discovery_instance_space`** / **`key_discovery_schema_space`**, and ensure **`workflow_scope`** on each trigger matches the leaf **`scope.id`** from **`module.py build`**.

## Debugging Tools

### 1. Enable Debug Logging

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

### 2. Use Environment Status Check

```bash
python env_utils.py --status
```

### 3. Validate configuration

Load pipeline YAMLs with `load_config_from_yaml` / run `module.py run --dry-run` against a small `--limit` to verify CDF connectivity and rules without writing aliases.

### 4. Test individual components

```python
from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_key_extraction.engine.key_extraction_engine import (
    KeyExtractionEngine,
)

engine = KeyExtractionEngine({"extraction_rules": [], "validation": {}})
result = engine.extract_keys({"name": "P-101", "id": "x"}, "asset")

from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_aliasing.engine.tag_aliasing_engine import (
    AliasingEngine,
)

aliasing_engine = AliasingEngine({"rules": [], "validation": {}})
out = aliasing_engine.generate_aliases("test-tag", "asset")
```

### 5. Check CDF Connection

```python
from cognite.client import CogniteClient
client = CogniteClient(api_key="key", project="project")
print("Assets:", len(client.assets.list(limit=1)))
```

## Getting Help

### 1. Check Logs

```bash
# Enable verbose logging (from repository root, PYTHONPATH=.)
export LOG_LEVEL=DEBUG
python modules/accelerators/contextualization/cdf_key_extraction_aliasing/module.py run --verbose --limit 10
```

### 2. Review Documentation

- [Documentation map](../README.md)
- [Quickstart — `.env` and `module.py run`](../guides/howto_quickstart.md)
- [Scoped deployment — `module.py build` and Toolkit](../guides/howto_scoped_deployment.md)
- [Configuration guide](../guides/configuration_guide.md)
- [Key extraction / aliasing report](../key_extraction_aliasing_report.md)

### 3. Test with Sample Data

```python
# Use provided sample data
from example_usage import main
main()
```

### 4. Contact Support

If issues persist:
1. Collect error logs and configuration
2. Test with minimal configuration
3. Provide sample data and expected results
4. Include system information (Python version, OS, etc.)
