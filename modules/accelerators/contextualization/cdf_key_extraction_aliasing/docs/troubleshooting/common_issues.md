# Troubleshooting Guide

This guide helps you resolve common issues with the CDF Key Extraction System.

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
# Verify .env file and run main.py to test connection
poetry run python main.py

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
1. **Check Input Data**
   ```python
   # Verify input data format
   print("Sample data:", sample_data)
   print("Data types:", [type(item) for item in sample_data])
   ```

2. **Review Extraction Rules**
   ```python
   # Test individual rules
   from key_extraction import KeyExtractionEngine
   engine = KeyExtractionEngine([rule])
   result = engine.extract_keys(sample_data)
   ```

3. **Lower Confidence Threshold**
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
   # Test individual aliasing rules
   from aliasing import AliasingEngine
   engine = AliasingEngine()
   aliases = engine.generate_aliases("P-101")
   ```

2. **Enable More Rule Types**
   ```yaml
   aliasing:
     rules:
       - name: "character_substitution"
         type: "character_substitution"
         enabled: true
       - name: "case_transformation"
         type: "case_transformation"
         enabled: true
   ```

#### Problem: Too many aliases
```
Generated 150 aliases (limit: 50)
```

#### Solution:
```yaml
aliasing:
  max_aliases_per_key: 25  # Reduce limit
```

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
     batch_size: 50  # Reduce from 100
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

### 3. Validate Configuration

```bash
python main.py validate
```

### 4. Test Individual Components

```python
# Test key extraction
from key_extraction import KeyExtractionEngine
engine = KeyExtractionEngine(rules)
result = engine.extract_keys(sample_data)

# Test aliasing
from aliasing import AliasingEngine
aliasing_engine = AliasingEngine()
aliases = aliasing_engine.generate_aliases("test-tag")
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
# Enable verbose logging
export LOG_LEVEL=DEBUG
python main.py run
```

### 2. Review Documentation

- [API Documentation](../api/)
- [Configuration Guide](configuration.md)
- [System Architecture](../architecture/system_architecture.md)

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
