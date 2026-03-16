# Quick Start Guide

This guide will help you get up and running with the Key Extraction and Aliasing Workflow quickly.

## Prerequisites

- Python 3.8 or higher
- Cognite Data Fusion (CDF) account and API key
- Basic understanding of Python and YAML configuration

## Installation

1. **Clone or download the project**
   ```bash
   git clone <repository-url>
   cd KeyExtraction
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Install CDF SDK**
   ```bash
   pip install cognite-sdk
   ```

## Environment Setup

1. **Create environment configuration**
   ```bash
   # Copy the template and edit manually
   cp env.template .env
   # Edit .env with your CDF credentials
   ```

2. **Configure CDF connection**
   ```bash
   # Edit .env file with your CDF credentials
   CDF_API_KEY=your_api_key_here
   CDF_PROJECT=your_project_name
   ```

3. **Validate configuration**
   ```bash
   # Verify .env file exists and contains required variables
   # Run main.py to test connection
   poetry run python main.py
   ```

## Basic Usage

### 1. Using the Key Extraction Engine

```python
from modules.contextualization.key_extraction_aliasing.functions.fn_dm_key_extraction.engine.key_extraction_engine import KeyExtractionEngine
from modules.contextualization.key_extraction_aliasing.functions.fn_dm_key_extraction.cdf_adapter import load_config_from_yaml

# Load configuration from pipeline config
config_dict = load_config_from_yaml("modules/contextualization/key_extraction_aliasing/pipelines/ctx_key_extraction_default.config.yaml")

# Initialize engine with extraction rules
engine = KeyExtractionEngine(config_dict)

# Sample data
sample_data = [
    {"name": "P-101", "description": "Main Process Pump"},
    {"name": "T-201", "description": "Storage Tank"},
    {"name": "V-301", "description": "Control Valve"}
]

# Extract keys
results = engine.extract_keys(sample_data)

print(f"Extracted {len(results.candidate_keys)} candidate keys")
for key in results.candidate_keys:
    print(f"  - {key.value} (confidence: {key.confidence})")
```

### 2. Using the Aliasing Engine

```python
from aliasing import AliasingEngine

# Initialize aliasing engine
aliasing_engine = AliasingEngine()

# Generate aliases for a tag
tag = "P-101"
aliases = aliasing_engine.generate_aliases(tag)

print(f"Aliases for '{tag}':")
for alias in aliases:
    print(f"  - {alias}")
```

### 3. Running the Complete System

```python
from main import CDFKeyExtractionSystem
from modules.contextualization.key_extraction_aliasing.config.configuration_manager import load_config_from_env

# Load configuration from environment
config = load_config_from_env()

# Initialize system
system = CDFKeyExtractionSystem(config)

# Deploy the pipeline
deployment_results = system.deploy_pipeline()
print("Deployment Results:", deployment_results)

# Run extraction
results = system.run_extraction()
print("Extraction Results:", results)
```

## Command Line Usage

### Deploy Key Extraction Only
```bash
python main.py deploy --key-extraction-only
```

### Deploy Complete Pipeline
```bash
python main.py deploy
```

### Run Extraction
```bash
python main.py run
```

### Validate Configuration
```bash
python main.py validate
```

## Next Steps

- Review the [Configuration Guide](configuration.md) for detailed configuration options
- Check the [API Documentation](../api/) for detailed API references
- Explore [Examples](../examples/) for more complex usage scenarios
- Read the [System Architecture](../architecture/system_architecture.md) for understanding the overall design

## Troubleshooting

If you encounter issues:

1. **Import Errors**: Ensure all dependencies are installed
   ```bash
   pip install -r requirements.txt
   pip install cognite-sdk
   ```

2. **CDF Connection Issues**: Verify your API key and project name in the `.env` file

3. **Configuration Errors**: Use the validation tools
   ```bash
   python env_utils.py --validate-env
   python main.py validate
   ```

4. **Module Not Found**: Ensure you're running from the project root directory

For more detailed troubleshooting, see the [Troubleshooting Guide](../troubleshooting/common_issues.md).
