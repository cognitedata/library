# CDF Performance Testing Framework

A comprehensive framework for testing the performance of Cognite Data Fusion (CDF) operations using Jupyter notebooks.

## 📁 Folder Structure

```
CDF Performance/
├── notebooks/                    # Jupyter notebooks for performance testing
│   ├── data_ingestion/           # Data ingestion performance tests
│   ├── time_series/              # Time series query performance tests
│   ├── events/                   # Event operations performance tests
│   ├── files/                    # File operations performance tests
│   ├── 3d/                       # 3D operations performance tests
│   ├── raw/                      # RAW table operations performance tests
│   ├── transformations/          # Transformation performance tests
│   ├── data_modeling/            # Data modeling performance tests
│   └── benchmarks/               # Comprehensive benchmark suites
├── utilities/                    # Utility modules and helper functions
├── configs/                      # Configuration files
├── results/                      # Performance test results (auto-generated)
├── scripts/                      # Setup and utility scripts
├── requirements.txt              # Python dependencies
└── README.md                     # This file
```

## 🚀 Quick Start

### 1. Environment Setup

Run the setup script to install dependencies and create configuration files:

```bash
python scripts/setup_environment.py
```

### 2. Configure CDF Connection

Edit the `.env` file created by the setup script with your CDF credentials:

```env
CDF_PROJECT=your-project-name
CDF_CLUSTER=your-cluster
CDF_CLIENT_ID=your-client-id
CDF_CLIENT_SECRET=your-client-secret
CDF_TENANT_ID=your-tenant-id
```

### 3. Start Jupyter

```bash
jupyter notebook
```

### 4. Run Performance Tests

Navigate to any notebook in the `notebooks/` directory and start testing!

## 📊 Available Performance Tests

### Data Ingestion (`notebooks/data_ingestion/`)
- **Time Series Ingestion**: Test performance of time series data ingestion
- Single datapoint vs batch ingestion
- Concurrent ingestion tests
- Memory usage analysis

### Time Series (`notebooks/time_series/`)
- **Query Performance**: Test time series query operations
- Range queries with different time spans
- Aggregate queries performance
- Multi-series query optimization

### Events (`notebooks/events/`)
- **Event Operations**: Test event creation, querying, and updates
- Event filtering and search performance
- Bulk event operations
- Event aggregation tests

### Files (`notebooks/files/`)
- **File Operations**: Test file upload, download, and metadata operations
- File size impact on performance
- Concurrent file operations
- File streaming performance

### 3D (`notebooks/3d/`)
- **3D Operations**: Test 3D model operations
- 3D model upload and processing
- 3D asset mapping performance
- 3D node operations

### RAW (`notebooks/raw/`)
- **RAW Tables**: Test RAW table operations
- Row insertion performance (single vs batch)
- RAW table query performance
- RAW table management operations

### Transformations (`notebooks/transformations/`)
- **Transformation Performance**: Test transformation operations
- Transformation job execution time
- Data pipeline performance
- Resource utilization analysis

### Data Modeling (`notebooks/data_modeling/`)
- **Data Model Operations**: Test data modeling performance
- Data model creation and updates
- Instance operations performance
- Query performance on data models

### Benchmarks (`notebooks/benchmarks/`)
- **Comprehensive Benchmarks**: Full system performance tests
- End-to-end workflow performance
- Cross-service integration tests
- Performance regression testing

## 🛠️ Utilities

### Performance Tracker (`utilities/performance_utils.py`)

The `PerformanceTracker` class provides easy-to-use performance monitoring:

```python
from utilities.performance_utils import PerformanceTracker

# Track a single operation
tracker = PerformanceTracker("timeseries_query")
tracker.start()
# ... your CDF operation ...
duration = tracker.stop()

# Get statistics
stats = tracker.get_stats()
print(f"Average time: {stats['mean']:.4f}s")

# Save results
tracker.save_results("results/timeseries_query_results.json")
```

### Benchmarking Functions

```python
from utilities.performance_utils import benchmark_operation

# Benchmark a function with multiple iterations
results = benchmark_operation(
    operation=my_cdf_function,
    iterations=100,
    warmup=5,
    *args, **kwargs
)
```

### Configuration Management (`configs/cdf_config.py`)

```python
from configs.cdf_config import config

# Get CDF client configuration
client_config = config.get_client_config()

# Get test configuration
test_config = config.get_test_config()
```

## 📈 Results and Analysis

Performance test results are automatically saved in the `results/` directory with timestamps:

```
results/
├── 20231201_143022/              # Timestamp directory
│   ├── timeseries_query_results.json
│   ├── event_operations_results.json
│   └── benchmark_summary.json
```

Each result file contains:
- Individual measurement data
- Statistical analysis (min, max, mean, median, std dev)
- Test metadata and configuration
- Timestamps for each measurement

## 🔧 Customization

### Adding New Tests

1. Create a new notebook in the appropriate category folder
2. Import utility functions:
   ```python
   import sys
   sys.path.append('../..')
   from utilities.performance_utils import PerformanceTracker
   from configs.cdf_config import config
   ```
3. Use the `PerformanceTracker` class to measure operations
4. Save results using the provided utilities

### Modifying Configuration

Edit `configs/cdf_config.py` to adjust:
- Default batch sizes
- Number of test iterations
- Timeout settings
- Test data sizes

## 📝 Best Practices

1. **Warm-up Iterations**: Always include warm-up iterations to account for cold start effects
2. **Multiple Measurements**: Run multiple iterations for statistical significance
3. **Consistent Environment**: Run tests in consistent environments for comparable results
4. **Document Changes**: Keep track of CDF version and configuration changes
5. **Regular Monitoring**: Set up regular performance monitoring to catch regressions

## 🤝 Contributing

1. Add new performance tests in the appropriate category
2. Update documentation when adding new features
3. Follow the existing code structure and naming conventions
4. Include proper error handling and logging

## 📄 License

This framework is intended for internal use with Cognite Data Fusion performance testing.

## 🆘 Support

For issues or questions:
1. Check the existing notebooks for examples
2. Review the utility functions documentation
3. Ensure your CDF credentials are correctly configured
4. Check the CDF SDK documentation for API changes 