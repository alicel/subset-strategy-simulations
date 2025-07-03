# Simple Database Migration Simulation

A streamlined discrete event simulation for analyzing single-threaded worker strategies in database migration scenarios. Each worker processes subset files sequentially, with configurable concurrency limits.

## Installation

### 1. Create a Virtual Environment (Recommended)

```bash
# Navigate to the simple simulation directory
cd simple/

# Create virtual environment
python -m venv venv

# Activate virtual environment
# On macOS/Linux:
source venv/bin/activate
# On Windows:
# venv\Scripts\activate
```

### 2. Install Dependencies

```bash
# Install required packages
pip install -r requirements.txt
```

### 3. Verify Installation

```bash
# Test with help
python3 run_simple_simulation.py --help
```

## Quick Start

### Generate Test Data

```bash
# Generate sample subset files
python3 tools/generate_test_files.py --output-dir test_data --num-files 10
```

### Run Basic Simulation

```bash
# Run with default settings (4 concurrent workers)
python3 run_simple_simulation.py test_data/mig_simple001/

# Run with custom worker count
python3 run_simple_simulation.py test_data/mig_simple001/ --max-workers 2

# Generate comprehensive visualizations
python3 run_simple_simulation.py test_data/mig_simple001/ --plotly-comprehensive
```

## Usage

### Command Line Options

```bash
python3 run_simple_simulation.py <directory> [options]
```

**Required:**
- `directory` - Directory containing subset files to process

**Options:**
- `--max-workers N` - Maximum concurrent workers (default: 4)
- `--output-name NAME` - Base name for output files (default: simple_simulation_results)
- `--output-dir DIR` - Output directory (default: output_files)
- `--no-plotly` - Skip interactive Plotly visualizations  
- `--plotly-comprehensive` - Generate comprehensive Plotly visualizations

### Input File Format

The simulation expects subset definition files with this path structure:
```
<migrationId>/metadata/subsets/<Label>/<subsetId>/<numSSTablesInSubset>/<dataSizeOfSubset>/subset-<subsetId>
```

Each subset file should contain SSTable definitions in format:
```
# Comments start with #
sstable_id,size_in_bytes
sst_001,1234567
sst_002,2345678
```

## Output Files

The simulation generates several output files:

1. **HTML Results** (`<name>.html`) - Comprehensive results with worker details
2. **Configuration** (`config_<name>.txt`) - Simulation parameters and command line
3. **Plotly Timeline** (`<name>_plotly.html`) - Interactive timeline visualization
4. **Plotly Details** (`<name>_plotly_details.html`) - Worker analysis charts
5. **Plotly Distribution** (`<name>_plotly_distribution.html`) - Work item distribution

## Examples

```bash
# Basic simulation
python3 run_simple_simulation.py /path/to/migration/data

# High concurrency test
python3 run_simple_simulation.py /path/to/migration/data --max-workers 8

# Detailed analysis with visualizations
python3 run_simple_simulation.py /path/to/migration/data \
    --plotly-comprehensive \
    --output-name detailed_analysis \
    --output-dir ./results

# Sequential processing (1 worker)
python3 run_simple_simulation.py /path/to/migration/data --max-workers 1
```

## Dependencies

- **Python 3.7+**
- **plotly>=5.18.0** - Interactive visualizations
- **pandas>=2.0.0** - Data processing (required by plotly)

All other dependencies are part of Python's standard library.

## Features

- ✅ **Discrete Event Simulation** - Accurate virtual clock advancement
- ✅ **Sequential Processing** - Single-threaded workers process SSTables in order
- ✅ **Configurable Concurrency** - Control maximum simultaneous workers
- ✅ **Interactive Visualizations** - Timeline, analysis, and distribution charts
- ✅ **Comprehensive Reporting** - HTML results with statistics and timelines
- ✅ **Compatible File Format** - Works with existing subset definition files
- ✅ **Robust Error Handling** - Clear error messages and validation

## Simulation Model

The simple simulation models:
- **Workers**: Single-threaded processes that handle one subset each
- **Sequential Processing**: SSTables within a subset are processed in file order
- **Concurrency Control**: Maximum number of workers running simultaneously
- **Event-Driven**: Virtual time advances based on worker completion events

This provides a simplified but accurate model for understanding the impact of worker concurrency on overall migration time. 