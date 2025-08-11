# CDF Performance Testing Framework

A comprehensive framework for testing the performance of
Cognite Data Fusion (CDF) operations using Jupyter notebooks.
This framework provides ready-to-use performance tests with
automated benchmarking, visualization, and cleanup capabilities.

## 🎯 What We're Testing & Why It Matters

### 📊 **Performance Testing Overview**

This framework systematically tests **all critical CDF operations** that impact
project delivery success:

| **Operation Category** | **What We Test** | **Project Impact** |
|------------------------|------------------|-------------------|
| **🏭 Data Ingestion** | Time series batch/streaming ingestion |
Data pipeline throughput & reliability |
| **🔍 Query Performance** | Range, aggregate, multi-series queries |
Application response times |
| **🏗️ Data Modeling** | Schema ops, instance CRUD, relationships |
Data model scalability |
| **🗃️ RAW Operations** | Bulk insert/query, table management |
Data lake performance |
| **📁 File Operations** | Upload/download, metadata handling |
Asset management efficiency |
| **🔄 Transformations** | Pipeline execution, resource usage |
Data processing workflows |

### 🚀 **Why Performance Testing is Critical for CDF Projects**

#### **1. Project Delivery Success**

- **⏱️ Meet SLA Requirements:** Ensure applications meet response time commitments
- **📈 Scale Validation:** Verify system can handle projected data volumes
- **🎯 User Experience:** Prevent slow dashboards and frustrated end users
- **💰 Cost Optimization:** Identify inefficient operations that waste CDF quota

#### **2. Production Readiness**

- **🔥 Avoid Go-Live Issues:** Catch performance bottlenecks before production
- **📊 Capacity Planning:** Size infrastructure correctly for data loads
- **🛡️ System Stability:** Prevent timeouts and failures under load
- **⚡ Optimization Opportunities:** Find areas for significant performance gains

#### **3. Development Efficiency**

- **🧪 Early Detection:** Identify performance regressions during development
- **📐 Design Validation:** Verify data model and architecture decisions
- **🔧 Tuning Guidance:** Get specific recommendations for optimization
- **📈 Baseline Establishment:** Track performance improvements over time

### 💡 **Real-World Project Benefits**

### Before Performance Testing

```bash
❌ "Dashboard takes 30 seconds to load"
❌ "Data ingestion pipeline fails with large batches" 
❌ "Users complain about slow search results"
❌ "Hitting CDF quota limits unexpectedly"
```text

### After Performance Testing:
```json
✅ "Dashboard loads in <3 seconds"
✅ "Ingestion handles 10,000 datapoints/second reliably"
✅ "Search results return in <1 second"
✅ "Optimized operations reduce CDF costs by 40%"
```text

### 🎪 **Typical Project Performance Issues We Catch**

1. **🐌 Inefficient Queries**
   - Large time range queries without proper limits
   - Querying multiple time series inefficiently
   - Missing aggregate optimizations

1. **💾 Data Ingestion Bottlenecks**
   - Sub-optimal batch sizes causing timeouts
   - Lack of concurrent ingestion strategies
   - Memory leaks in long-running processes

1. **🏗️ Data Model Problems**
   - Complex schemas causing slow instance operations
   - Inefficient relationship queries
   - Poor container/view design patterns

1. **📁 File Operation Issues**
   - Large file uploads timing out
   - Inefficient metadata handling
   - Missing streaming optimizations

### 📈 **Performance Success Metrics**

### Target Performance Benchmarks:
- **Data Ingestion:** >1,000 datapoints/second sustained
- **Query Response:** <2 seconds for typical dashboard queries
- **Data Model Ops:** <1 second for instance creation/updates
- **File Operations:** >1 MB/s upload/download throughput
- **System Reliability:** <1% error rate under normal load

## �� Project Structure

```python
CDF Performance Testing/
├── 📂 notebooks/                    # Interactive Jupyter notebooks for 
performance testing
│   ├── 📂 data_ingestion/           # Time series data ingestion performance
│   │   └── timeseries_ingestion_performance.ipynb
│   ├── 📂 time_series/              # Time series query performance testing  
│   │   └── timeseries_query_performance.ipynb
│   ├── 📂 data_modeling/            # Data modeling operations performance
│   │   └── data_modeling_performance.ipynb
│   ├── 📂 raw/                      # RAW table operations performance
│   │   └── raw_performance.ipynb
│   ├── 📂 files/                    # File operations performance testing
│   │   └── files_performance.ipynb
│   └── 📂 transformations/          # Transformation operations performance
│       └── transformations_performance.ipynb
├── 📂 utilities/                    # Performance utilities and helper functions
│   ├── client_setup.py             # CDF client configuration and testing
│   └── performance_utils.py        # Benchmarking and measurement tools
├── 📂 configs/                      # Configuration files and settings
│   └── cdf_config.py               # CDF connection configuration
├── 📂 scripts/                      # Setup and utility scripts
│   └── setup_environment.py        # Environment setup automation
├── 📂 results/                      # Auto-generated performance test results
├── 📄 requirements.txt              # Python dependencies
├── 📄 test_connection.py           # CDF connection testing utility
└── 📄 README.md                    # This documentation
```python

## 🚀 Getting Started

### Step 1: Environment Setup

1. **Clone/Download** this performance testing framework
2. **Install Python dependencies:**

   ```bash
   pip install -r requirements.txt
   ```yaml

1. **Run the setup script** (creates configuration files):

   ```bash
   python scripts/setup_environment.py
   ```yaml

### Step 2: Configure CDF Connection

1. **Create a `.env` file** in the root directory with your CDF credentials:

   ```env
   # CDF Connection Settings
   CDF_PROJECT=your-project-name
   CDF_CLUSTER=your-cluster-name  
   CDF_BASE_URL=https://your-cluster.cognitedata.com
   
   # Authentication (Service Principal)
   CDF_CLIENT_ID=your-client-id
   CDF_CLIENT_SECRET=your-client-secret
   CDF_TENANT_ID=your-tenant-id
   
   # Performance Test Settings
   DEFAULT_BATCH_SIZE=1000
   DEFAULT_ITERATIONS=10
   LOG_LEVEL=INFO
   ```yaml

1. **Test your connection:**

   ```bash
   python test_connection.py
   ```yaml

   **Expected Output:**

   ```yaml

   ✓ Successfully connected to CDF project: your-project
   ✓ Configuration is valid
   ✓ Token is valid and accessible
   🎉 CDF connection test successful!
   ```yaml

### Step 3: Start Jupyter Notebook

```bash
jupyter notebook
```yaml

Navigate to the `notebooks/` directory and choose a performance test to run!

## 📊 Available Performance Tests

### 🏭 Data Ingestion Performance (`data_ingestion/`)

### 📓 `timeseries_ingestion_performance.ipynb`
Tests time series data ingestion performance with comprehensive benchmarking.

### 🔬 What it tests:
- Single vs batch datapoint ingestion
- Concurrent ingestion performance  
- Memory usage during ingestion
- Different batch sizes optimization
- Error handling and resilience

### 📈 Visual Output Examples:
```text
### * Testing Batch Ingestion Performance *
==================================================

1. Testing batch sizes: [100, 500, 1000, 5000]
   Batch 100   | 0.234s avg | 427.35 datapoints/s
   Batch 500   | 0.891s avg | 561.28 datapoints/s  
   Batch 1000  | 1.456s avg | 686.81 datapoints/s
   Batch 5000  | 6.234s avg | 802.05 datapoints/s

1. Testing concurrent ingestion (4 threads):
   Thread performance: 1,247.3 datapoints/s per thread
   Total throughput: 4,989.2 datapoints/s

📊 Performance Visualization:
[Generated charts showing batch size vs throughput]
```yaml

**🧹 Cleanup:** Automatically removes test time series and data

---

### 🔍 Time Series Query Performance (`time_series/`)

### 📓 `timeseries_query_performance.ipynb`
Comprehensive testing of time series query operations and 
optimization strategies.

### 🔬 What it tests:
- Range queries with different time spans (1 hour to 1 week)
- Aggregate queries with various granularities  
- Multiple time series simultaneous queries
- Limit parameter optimization
- Latest value query performance

### 📈 Visual Output Examples:
```text
🔍 Range Query Performance:
  1 hour     | 0.073s avg | 13.64 ops/s | 150 datapoints/s
  6 hours    | 0.063s avg | 15.77 ops/s | 1,120 datapoints/s  
  1 day      | 0.064s avg | 15.75 ops/s | 4,520 datapoints/s
  1 week     | 0.073s avg | 13.76 ops/s | 27,718 datapoints/s

📊 Aggregate Query Performance:
  1m granularity  | 0.082s avg | 24,567 points/s
  15m granularity | 0.074s avg | 9,067 points/s
  1h granularity  | 0.071s avg | 2,397 points/s

🔗 Multiple Series Performance:
  1 series   | 16.01 datapoints/s per series
  2 series   | 14.96 datapoints/s per series  
  3 series   | 14.77 datapoints/s per series

📊 [Auto-generated performance charts and visualizations]
```yaml

**🧹 Cleanup:** Automatically removes test time series (3 series with 
~6K datapoints)

---

### 🏗️ Data Modeling Performance (`data_modeling/`)

### 📓 `data_modeling_performance.ipynb`
Tests performance of data modeling operations including schema management and 
instance operations.

### 🔬 What it tests:
- **Test 1:** Instance creation performance (different batch sizes)
- **Test 2:** Instance query performance (filters, limits)
- **Test 3:** Instance update performance
- **Test 4:** Edge (relationship) creation and queries
- **Test 5:** Schema operations (containers, views, data models) ⚠️ *Fixed ViewPropertyApply error*
- **Test 6:** Instance deletion performance

### 📈 Visual Output Examples:
```text
### * Testing Data Modeling Performance *
================================================

🏭 Instance Creation Performance:
  Batch  10 | 0.106s avg | 94.23 instances/s
  Batch  50 | 0.149s avg | 336.78 instances/s
  Batch 100 | 0.477s avg | 209.69 instances/s
  Batch 500 | 0.659s avg | 759.08 instances/s

🔍 Instance Query Performance:
  limit_100       | 0.115s avg | 86.84 instances/s
  status_filter   | 0.094s avg | 10.64 ops/s
  value_range     | 0.103s avg | 9.71 ops/s

🔗 Edge Performance:
  edge_creation_10    | 0.109s avg | 91.62 edges/s
  edge_query          | 0.086s avg | 11.58 ops/s

📋 Schema Operations Performance: (FIXED)
  container_creation  | 1.33 containers/s
  view_creation      | 0.82 views/s (using MappedPropertyApply)
  data_model_creation | 0.91 data models/s

📊 [Comprehensive performance visualizations with 6 charts]
```yaml

**🧹 Cleanup:** Enhanced cleanup with proper data model version handling

---

### 🗃️ RAW Table Performance (`raw/`)

### 📓 `raw_performance.ipynb`
Tests performance of RAW table operations for unstructured data storage.

### 🔬 What it tests:
- RAW row insertion (single vs batch)
- RAW table query and filtering performance
- RAW database and table management
- Cursor-based pagination performance
- RAW data retrieval optimization

### 📈 Expected Output Examples:
```text
### * Testing RAW Table Performance *
=======================================

🗃️ Row Insertion Performance:
  Single rows    | 0.145s avg | 6.90 rows/s
  Batch 100     | 1.234s avg | 81.03 rows/s
  Batch 1000    | 8.567s avg | 116.74 rows/s

🔍 Query Performance:
  Filter queries    | 0.089s avg | 11.24 ops/s
  Pagination (1000) | 0.156s avg | 6.41 ops/s
  Full table scan   | 2.345s avg | 0.43 ops/s

📊 [Performance charts and optimization recommendations]
```yaml

**🧹 Cleanup:** Removes test RAW databases and tables

---

### 📁 File Operations Performance (`files/`)

### 📓 `files_performance.ipynb`
Tests performance of file upload, download, and metadata operations.

### 🔬 What it tests:
- File upload performance (different sizes)
- File download and streaming performance
- File metadata operations
- Concurrent file operations
- File processing pipeline performance

### 📈 Expected Output Examples:
```text
### * Testing File Operations Performance *
==========================================

📤 Upload Performance:
  1MB files     | 2.34s avg | 0.43 MB/s
  10MB files    | 15.67s avg | 0.64 MB/s  
  100MB files   | 89.23s avg | 1.12 MB/s

📥 Download Performance:
  1MB files     | 1.23s avg | 0.81 MB/s
  10MB files    | 8.45s avg | 1.18 MB/s
  Streaming     | 0.95s avg | 1.35 MB/s

📊 [File performance visualizations and recommendations]
```yaml

**🧹 Cleanup:** Removes test files and metadata

---

### 🔄 Transformations Performance (`transformations/`)

### 📓 `transformations_performance.ipynb`
Tests performance of transformation operations and data pipeline processing.

### 🔬 What it tests:
- Transformation job execution performance
- Data pipeline throughput testing
- Resource utilization analysis
- Transformation scheduling performance
- Error handling and retry logic

### 📈 Expected Output Examples:
```text
### * Testing Transformation Performance *
=========================================

🔄 Job Execution Performance:
  Simple transforms  | 45.6s avg | 2,190 rows/s
  Complex transforms | 123.4s avg | 810 rows/s
  Scheduled jobs     | 67.8s avg | 1,475 rows/s

📊 Resource Utilization:
  CPU usage     | 65.4% avg
  Memory usage  | 2.3GB peak
  Network I/O   | 45.6 MB/s avg

📊 [Pipeline performance charts and optimization insights]
```yaml

**🧹 Cleanup:** Removes test transformation jobs and configurations

## 🛠️ Performance Utilities

The framework includes powerful utilities for 
consistent performance measurement:

### PerformanceTracker Class

```python
from utilities.performance_utils import PerformanceTracker

# Simple operation tracking
tracker = PerformanceTracker("my_operation")
tracker.start()
# ... your CDF operation ...
duration = tracker.stop()

# Get detailed statistics
stats = tracker.get_stats()
print(f"Mean: {stats['mean']:.4f}s")
print(f"Std Dev: {stats['std_dev']:.4f}s")
```text

### Benchmark Operation Function

```python
from utilities.performance_utils import benchmark_operation

# Automated benchmarking with statistics
results = benchmark_operation(
    operation=my_cdf_function,
    iterations=100,
    warmup=5,
    *args, **kwargs
)

print(f"Mean time: {results['mean_time']:.4f}s")
print(f"Operations/sec: {results['operations_per_second']:.2f}")
```text

### Data Generation Utilities

```python
from utilities.performance_utils import generate_test_data

# Generate realistic test data
timeseries_data = generate_test_data(
    data_type="timeseries",
    count=1000,
    start_time="2023-01-01",
    interval="5m"
)
```yaml

## 📈 Results and Visualization

### Automatic Results Storage

All performance tests automatically save results with timestamps:

```text
results/
├── 20231201_143022_timeseries_ingestion/
│   ├── batch_performance.json
│   ├── concurrent_performance.json
│   ├── memory_usage.json
│   └── performance_summary.png
├── 20231201_143155_query_performance/
│   ├── range_queries.json
│   ├── aggregate_queries.json
│   └── visualization_charts.png
```yaml

### Built-in Visualizations

Each notebook generates comprehensive charts:

- **Performance trends** over time
- **Throughput comparisons** across different configurations
- **Resource utilization** graphs
- **Error rate** analysis
- **Optimization recommendations**

## 🔧 Customization and Best Practices

### Running Tests in Different Environments

1. **Development Environment:**

   ```python
   # Small test datasets
   test_series = setup_test_timeseries(num_series=3, days_of_data=7)
   ```yaml

1. **Production-like Testing:**

   ```python
   # Larger test datasets
   test_series = setup_test_timeseries(num_series=50, days_of_data=90)
   ```yaml

### Performance Testing Best Practices

1. **📊 Always run warm-up iterations** to account for cold start effects
2. **🔄 Use multiple iterations** for statistical significance (minimum 10)
3. **🏷️ Tag your results** with environment and CDF version information
4. **🧹 Always run cleanup** to avoid cluttering your CDF project
5. **📈 Monitor trends** over time to catch performance regressions
6. **⚖️ Test realistic data volumes** that match your production usage

### Configuration Management

Edit `configs/cdf_config.py` to customize:

- Default batch sizes for different operations
- Number of test iterations
- Timeout settings and retry logic
- Test data generation parameters

## 🚨 Important Notes

### Cleanup and Data Management

- ✅ **All notebooks include automatic cleanup** functions
- ⚠️ **Test data is created in your CDF project** - cleanup is essential
- 🎯 **Use test/development projects** for performance testing when possible
- 📊 **Monitor your CDF quota usage** during large-scale tests

### Performance Testing Considerations

- 🌐 **Network latency affects results** - test from consistent locations
- 🔄 **CDF API rate limits** may impact high-throughput tests
- 📈 **Baseline your results** against known good performance metrics
- 🕒 **Time of day can affect results** due to CDF load variations

## 🤝 Contributing

To add new performance tests:

1. **Create a new notebook** in the appropriate category directory
2. **Follow the established pattern:**

   ```python
   # Standard imports for all notebooks
   import sys
   sys.path.append('../..')
   from utilities.performance_utils import PerformanceTracker, 
benchmark_operation
   from utilities.client_setup import get_client, test_connection
   ```

1. **Include setup, tests, visualization, and cleanup sections**
2. **Update this README** with your new test descriptions
3. **Test thoroughly** and ensure cleanup works properly

## 🆘 Support and Troubleshooting

### Common Issues

1. **Connection Errors:**
   - Verify `.env` file configuration
   - Run `python test_connection.py`
   - Check CDF service principal permissions

1. **Performance Test Failures:**
   - Ensure sufficient CDF quota
   - Check for API rate limiting
   - Verify test data cleanup from previous runs

1. **Notebook Import Errors:**
   - Confirm `pip install -r requirements.txt` completed
   - Check Python path configuration
   - Verify notebook kernel is using correct environment

### Getting Help

1. 📖 **Check notebook outputs** for detailed error messages
2. 🔍 **Review utility function documentation** in `utilities/`
3. 🌐 **Consult CDF SDK documentation** for API changes
4. 🧪 **Start with smaller test datasets** to isolate issues

---

**Ready to start performance testing?** 🚀

1. `pip install -r requirements.txt`
2. `python scripts/setup_environment.py`
3. Configure your `.env` file
4. `python test_connection.py`
5. `jupyter notebook`
6. Open any notebook and start testing!

*Happy performance testing!* 📊✨
