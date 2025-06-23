# Simple vs Tiered Migration Simulation Comparison Tool

A comprehensive tool for comparing the performance of Simple and Tiered migration simulation strategies. This tool analyzes actual execution metrics from both simulation types to provide detailed performance comparisons.

## Overview

The comparison tool extracts and compares key performance metrics between Simple and Tiered simulation runs, including:

- **Total execution time** (end-to-end simulation time)
- **Worker utilization** (actual workers used vs configured)
- **CPU resources** (total threads/cores used)
- **CPU time** (total compute time across all workers)
- **Per-tier breakdown** (for tiered strategy)

## Prerequisites

- Python 3.7 or higher
- Both simulation types must be run first to generate output data
- Simulations must process common migration IDs for meaningful comparison

## Required Data Structure

The tool expects the following output structure (relative to the project root):

```
simple/output/{execution_name}/
├── exec_reports/
└── {migration_id}/
    └── migration_exec_results/
        ├── *_workers.csv           # Added by instrumentation
        ├── *_summary.csv           # Added by instrumentation
        └── config_*.txt

tiered/output/{execution_name}/
├── exec_reports/
└── {migration_id}/
    └── migration_exec_results/
        ├── *_execution_report.json
        ├── *_workers.csv
        ├── *_summary.csv
        └── config_*.txt

comparison/                         # Comparison tool directory
├── comparison_tool.py              # Main comparison script
└── README.md                       # This documentation
```

## Usage

### Basic Comparison

Compare two execution runs using execution names (recommended):

```bash
cd comparison
python comparison_tool.py --simple-execution alice_test_run --tiered-execution test_new_5
```

Short form:
```bash
cd comparison
python comparison_tool.py -s alice_test_run -t test_new_5
```

### Save Results to CSV

**Organized Output (Recommended)**:

Create organized comparison analysis with automatic directory structure:

```bash
cd comparison
python comparison_tool.py -s alice_test_run -t test_new_5 --comparison-exec-name my_analysis --output-csv
```

This creates:
```
comparison/output/my_analysis/
├── comparison_report_my_analysis.csv      # Detailed CSV data
└── comparison_summary_my_analysis.txt     # Formatted tabular report
```

Short form:
```bash
cd comparison
python comparison_tool.py -s alice_test_run -t test_new_5 -c my_analysis --output-csv
```

**Custom Output Path**:

Export to a specific file location:

```bash
cd comparison
python comparison_tool.py -s alice_test_run -t test_new_5 --output custom_results.csv
```

### Full Path Mode (Advanced)

For custom directory structures (use absolute paths or paths relative to the comparison directory):

```bash
cd comparison
python comparison_tool.py --simple-path ../simple/output/custom_run --tiered-path ../tiered/output/custom_run
```

## Command Line Options

### Required Arguments (choose one method)

**Method 1: Execution Names (Recommended)**
- `--simple-execution`, `-s`: Simple execution name (e.g., `alice_test_run`)
- `--tiered-execution`, `-t`: Tiered execution name (e.g., `test_new_5`)

**Method 2: Full Paths (Advanced)**
- `--simple-path`: Full path to simple simulation output directory
- `--tiered-path`: Full path to tiered simulation output directory

### Optional Arguments

**Output Organization**
- `--comparison-exec-name`, `-c`: Name for this comparison analysis
  - Creates organized output directory: `comparison/output/{name}/`
  - Required when using `--output-csv`

**Output Generation**
- `--output-csv`: Generate both CSV and tabular reports in organized directory structure
  - Requires `--comparison-exec-name`
  - Creates: `comparison/output/{name}/comparison_report_{name}.csv` (CSV data)
  - Creates: `comparison/output/{name}/comparison_summary_{name}.txt` (tabular report)
- `--output`, `-o`: Custom CSV file path (overrides organized structure)

### Examples by Use Case

**Quick console comparison:**
```bash
python comparison_tool.py -s run1 -t run2
```

**Organized analysis with CSV:**
```bash
python comparison_tool.py -s run1 -t run2 -c analysis_name --output-csv
```

**Custom output location:**
```bash
python comparison_tool.py -s run1 -t run2 -o /path/to/custom.csv
```

## Output Format

### Console Output

The tool displays a formatted table with the following columns:

| Column | Description |
|--------|-------------|
| **Migration ID** | Common migration identifier |
| **Execution Time** | Simple vs Tiered total execution time |
| **Workers** | Actual number of workers used |
| **CPUs** | Total CPU cores/threads used |
| **CPU Time** | Total compute time (worker_duration × threads) |
| **T/S Ratio** | Tiered/Simple ratio (efficiency: <1.0 = tiered better) |
| **S/T Ratio** | Simple/Tiered ratio (speedup: >1.0 = tiered better) |

### Aggregate Analysis

Summary statistics across all common migrations:
- Total execution time comparison
- Total worker count comparison  
- Total CPU utilization comparison
- Total CPU time comparison

### File Output

**Tabular Report (`*_summary.txt`)**
- Formatted console output saved to text file
- Includes execution names and comparison metadata
- Shows exclusive migrations (found in only one execution)
- Human-readable format matching console display
- Perfect for reports and documentation

**CSV Export (`*_report.csv`)**
- Detailed data in spreadsheet format
- Execution names in header metadata comments
- Per-migration metrics for both strategies
- Calculated ratios (tiered/simple and simple/tiered)
- Per-tier breakdown for tiered strategy
- All raw values for further analysis and plotting

## Example Output

```
=============================================================================================================================
SIMPLE vs TIERED SIMULATION COMPARISON SUMMARY
=============================================================================================================================

Comparison Details:
  Simple Execution:  my_simple_run
  Tiered Execution:  my_tiered_run
  Common Migrations: 2

Exclusive Migrations:
  Simple Only (1): ['mig109']
  Tiered Only (1): ['mig112']

Migration    Execution Time                   Workers                        CPUs                          CPU Time                   
ID           Simple     Tiered  T/S   S/T   Simple  Tiered T/S   S/T   Simple  Tiered T/S   S/T   Simple    Tiered    T/S   S/T  
-----------------------------------------------------------------------------------------------------------------------------
mig110       17.1B      1.4B    0.08  12.2  4       1      0.25  4.0   4       6      1.50  0.67  17.1B      8.2B      0.48  2.1 
mig111       12.5B      915.4M  0.07  13.7  3       1      0.33  3.0   3       6      2.00  0.50  12.5B      5.5B      0.44  2.3 

=============================================================================================================================
AGGREGATE ANALYSIS
=============================================================================================================================

Total Execution Time:
  Simple:      29.6B
  Tiered:      2.3B
  Tiered/Simple: 0.08 (efficiency: <1.0 = tiered faster)
  Simple/Tiered: 12.9 (speedup: >1.0 = tiered faster)

Total Workers:
  Simple:      7
  Tiered:      2
  Tiered/Simple: 0.29
  Simple/Tiered: 3.5

Total CPUs:
  Simple:      7
  Tiered:      12
  Tiered/Simple: 1.71
  Simple/Tiered: 0.58

Total CPU Time:
  Simple:      29.6B
  Tiered:      13.7B
  Tiered/Simple: 0.46
  Simple/Tiered: 2.2
```

## Metrics Explanation

### Execution Time
- **Simple**: Time from simulation start to last worker completion
- **Tiered**: Time from simulation start to last tier completion
- **Interpretation**: Lower is better; ratio < 1.0 means tiered is faster

### Workers
- **Simple**: Actual number of workers spawned (≤ max_workers config)
- **Tiered**: Sum of workers across all tiers that processed data
- **Interpretation**: Shows resource utilization efficiency

### CPUs
- **Simple**: Workers × 1 (single-threaded workers)
- **Tiered**: Sum of (workers × threads_per_tier) across all tiers
- **Interpretation**: Total parallel processing capacity

### CPU Time
- **Simple**: Sum of actual worker execution durations
- **Tiered**: Sum of (worker_duration × threads_per_tier)
- **Interpretation**: Total compute work performed; accounts for parallelization

### Ratio Interpretation

The tool provides two complementary ratio perspectives:

#### **Tiered/Simple (T/S) Ratios**
- **Values < 1.0**: Tiered strategy is more efficient/faster
- **Values > 1.0**: Simple strategy is more efficient/faster
- **Example**: T/S = 0.08 means tiered takes 8% of the time simple takes

#### **Simple/Tiered (S/T) Ratios** 
- **Values > 1.0**: Tiered strategy provides speedup (tiered is faster)
- **Values < 1.0**: Simple strategy is faster
- **Example**: S/T = 12.2 means simple takes 12.2x longer than tiered

Both ratios show the same relationship from different perspectives:
- **T/S = 0.08** and **S/T = 12.5** both indicate tiered is ~12x faster
- Use T/S for efficiency metrics (fractions easier for small improvements)
- Use S/T for speedup metrics (whole numbers easier for large improvements)

## Troubleshooting

### "No common migrations found"
- Ensure both runs processed migrations with identical IDs
- Check that migration directories exist in both output folders
- Verify migration ID naming consistency (e.g., `mig110`, `mig111`)
- Review the "Exclusive Migrations" section to see what migrations exist in each run

### "Could not find CSV files"
- Simple simulation: Ensure you're using the updated version with CSV export
- Tiered simulation: Verify `*_workers.csv` files were generated
- Check file permissions and paths

### "Error parsing data"
- Verify CSV files are properly formatted
- Check for incomplete simulation runs
- Ensure all required data fields are present

### Missing Data
```bash
# Check what migrations are available (from project root)
ls simple/output/your_execution_name/
ls tiered/output/your_execution_name/

# Verify CSV files exist (from project root)
ls simple/output/your_execution_name/*/migration_exec_results/*.csv
ls tiered/output/your_execution_name/*/migration_exec_results/*.csv

# Or from comparison directory
cd comparison
ls ../simple/output/your_execution_name/
ls ../tiered/output/your_execution_name/
```

## Technical Details

### Data Sources

| Strategy | Execution Time | Workers | CPU Time |
|----------|----------------|---------|----------|
| **Simple** | Summary CSV: `Total_Simulation_Time` | Workers CSV: row count | Summary CSV: `Total_CPU_Time` |
| **Tiered** | JSON: `total_execution_time` | JSON: `by_tier` totals | Workers CSV: sum(duration × tier_threads) |

### Calculation Methods

**CPU Time Calculation:**
- Simple: `sum(worker_duration × 1_thread)`
- Tiered: `sum(worker_duration × threads_per_tier)`

**Worker Counts:**
- Uses actual workers that executed, not configuration maximums
- Only counts workers that processed data (non-zero duration)

## Contributing

When adding new metrics or modifying calculations:

1. Ensure both simulation types export the required data
2. Update the comparison tool to parse new data fields
3. Add corresponding test cases
4. Update this README with new metric descriptions

## Version History

- **v1.0**: Initial implementation with execution name CLI
- **v1.1**: Added CSV export instrumentation for simple simulations
- **v1.2**: Enhanced CPU time calculation using actual worker durations
- **v1.3**: Added organized output structure with `--comparison-exec-name` and `--output-csv` options
- **v1.4**: Added tabular report file output and execution names in both console and file reports
- **v1.5**: Added exclusive migration tracking to identify migrations present in only one execution 