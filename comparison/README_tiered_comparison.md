# Tiered vs Tiered Migration Simulation Comparison Tool

A comprehensive tool for comparing the performance between two different Tiered migration simulation executions. This tool analyzes actual execution metrics from both tiered simulation runs to provide detailed performance comparisons.

## Overview

The tiered comparison tool extracts and compares key performance metrics between two different tiered simulation runs of the same migrations, including:

- **Total execution time** (end-to-end simulation time)
- **Worker utilization** (actual workers used across tiers)
- **CPU resources** (total threads/cores used across tiers)
- **CPU time** (total compute time across all workers)
- **Per-tier breakdown** (SMALL, MEDIUM, LARGE tier comparison)
- **Tier-by-tier analysis** (worker and CPU distribution by tier)

## Prerequisites

- Python 3.7 or higher
- Both tiered simulation runs must be completed first to generate output data
- Simulations must process common migration IDs for meaningful comparison

## Required Data Structure

The tool expects the following output structure (relative to the project root):

```
tiered/output/{execution1_name}/
├── exec_reports/
└── {migration_id}/
    └── migration_exec_results/
        ├── *_execution_report.json    # Main metrics source
        ├── *_workers.csv              # Worker execution details
        ├── *_summary.csv              # Summary metrics
        └── config_*.txt

tiered/output/{execution2_name}/
├── exec_reports/
└── {migration_id}/
    └── migration_exec_results/
        ├── *_execution_report.json
        ├── *_workers.csv
        ├── *_summary.csv
        └── config_*.txt

comparison/                            # Comparison tool directory
├── tiered_comparison_tool.py          # Main tiered comparison script
└── README_tiered_comparison.md        # This documentation
```

## Usage

### Basic Comparison

Compare two tiered execution runs using execution names (recommended):

```bash
cd comparison
python tiered_comparison_tool.py --exec1 test_new_5 --exec2 test_new_6
```

Short form:
```bash
cd comparison
python tiered_comparison_tool.py -1 test_new_5 -2 test_new_6
```

### Save Results to CSV

**Organized Output (Recommended)**:

Create organized comparison analysis with automatic directory structure:

```bash
cd comparison
python tiered_comparison_tool.py --exec1 test_new_5 --exec2 test_new_6 --comparison-exec-name my_tiered_analysis --output-csv
```

This creates:
```
comparison/output/tiered_comparison/my_tiered_analysis/
├── tiered_comparison_report_my_tiered_analysis.csv      # Detailed CSV data
└── tiered_comparison_summary_my_tiered_analysis.txt     # Formatted tabular report
```

Short form:
```bash
cd comparison
python tiered_comparison_tool.py -1 test_new_5 -2 test_new_6 -c my_tiered_analysis --output-csv
```

**Custom Output Path**:

Export to a specific file location:

```bash
cd comparison
python tiered_comparison_tool.py --exec1 test_new_5 --exec2 test_new_6 --output custom_tiered_results.csv
```

### Full Path Mode (Advanced)

For custom directory structures (use absolute paths or paths relative to the comparison directory):

```bash
cd comparison
python tiered_comparison_tool.py --exec1-path ../tiered/output/custom_run1 --exec2-path ../tiered/output/custom_run2
```

## Command Line Options

### Required Arguments (choose one method)

**Method 1: Execution Names (Recommended)**
- `--exec1`, `-1`: First tiered execution name (e.g., `test_new_5`)
- `--exec2`, `-2`: Second tiered execution name (e.g., `test_new_6`)

**Method 2: Full Paths (Advanced)**
- `--exec1-path`: Full path to first tiered simulation output directory
- `--exec2-path`: Full path to second tiered simulation output directory

### Optional Arguments

**Output Organization**
- `--comparison-exec-name`, `-c`: Name for this comparison analysis
  - Creates organized output directory: `comparison/output/tiered_comparison/{name}/`
  - Required when using `--output-csv`

**Output Generation**
- `--output-csv`: Generate both CSV and tabular reports in organized directory structure
  - Requires `--comparison-exec-name`
  - Creates: `comparison/output/tiered_comparison/{name}/tiered_comparison_report_{name}.csv` (CSV data)
  - Creates: `comparison/output/tiered_comparison/{name}/tiered_comparison_summary_{name}.txt` (tabular report)
- `--output`, `-o`: Custom CSV file path (overrides organized structure)

### Examples by Use Case

**Quick console comparison:**
```bash
python tiered_comparison_tool.py -1 run1 -2 run2
```

**Organized analysis with CSV:**
```bash
python tiered_comparison_tool.py -1 run1 -2 run2 -c analysis_name --output-csv
```

**Custom output location:**
```bash
python tiered_comparison_tool.py -1 run1 -2 run2 -o /path/to/custom.csv
```

## Output Format

### Console Output

The tool displays a formatted table with the following columns:

| Column | Description |
|--------|-------------|
| **Migration ID** | Common migration identifier |
| **Execution Time** | Exec1 vs Exec2 total execution time |
| **Workers** | Actual number of workers used in each execution |
| **CPUs** | Total CPU cores/threads used in each execution |
| **CPU Time** | Total compute time (worker_duration × threads) |
| **2/1 Ratio** | Exec2/Exec1 ratio (efficiency: <1.0 = exec2 better) |
| **1/2 Ratio** | Exec1/Exec2 ratio (speedup: >1.0 = exec2 better) |

### Tier-by-Tier Breakdown

Additional analysis showing:
- Worker distribution across SMALL, MEDIUM, LARGE tiers
- CPU allocation across tiers
- Tier-by-tier ratios between executions

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
- Per-migration metrics for both executions
- Calculated ratios (exec2/exec1 and exec1/exec2)
- Per-tier breakdown for both executions
- All raw values for further analysis and plotting

## Key Indicators Compared

The tool compares the same key performance indicators as the original simple vs tiered comparison tool:

1. **Total Execution Time**: Overall time to complete the migration
2. **Worker Count**: Number of workers actually used
3. **CPU Count**: Total CPU threads/cores allocated
4. **CPU Time**: Total computational time used (duration × threads)
5. **Tier Distribution**: Breakdown by SMALL, MEDIUM, LARGE tiers

Plus additional tiered-specific analysis:
- **Tier-by-tier comparison**: How each tier performed between executions
- **Resource allocation patterns**: How CPU resources were distributed across tiers

## Example Output

```
========================================================================================================================
TIERED vs TIERED SIMULATION COMPARISON SUMMARY
========================================================================================================================

Comparison Details:
  Execution 1:       test_new_5
  Execution 2:       test_new_6
  Common Migrations: 2

Migration    Execution Time                      Workers                       CPUs                          CPU Time                           
ID           test_new_5 test_new_6 2/1   1/2   test_new_5 test_new_6 2/1   1/2   test_new_5 test_new_6 2/1   1/2   test_new_5 test_new_6 2/1   1/2  
---------------------------------------------------------------------------------------------------------------------------------
mig110       3.0B       2.8B       0.93  1.07  9          9          1.00  1.00  54         54         1.00  1.00  69.7B      68.9B      0.99  1.01
mig111       128.2M     127.5M     0.99  1.01  1          1          1.00  1.00  6          6          1.00  1.00  769.1M     765.0M     0.99  1.01

=========================================================================================================================
AGGREGATE ANALYSIS
=========================================================================================================================

Total Execution Time:
  Exec1:      3.1B
  Exec2:      2.9B
  Exec2/Exec1: 0.94 (efficiency: <1.0 = exec2 faster)
  Exec1/Exec2: 1.06 (speedup: >1.0 = exec2 faster)

Total Workers:
  Exec1:      10
  Exec2:      10
  Exec2/Exec1: 1.00
  Exec1/Exec2: 1.00

Total CPUs:
  Exec1:      60
  Exec2:      60
  Exec2/Exec1: 1.00
  Exec1/Exec2: 1.00

Total CPU Time:
  Exec1:      70.5B
  Exec2:      69.7B
  Exec2/Exec1: 0.99
  Exec1/Exec2: 1.01

=========================================================================================================================
TIER-BY-TIER BREAKDOWN
=========================================================================================================================

Tier     Workers                  CPUs                    
Name     Exec1    Exec2    Ratio    Exec1    Exec2    Ratio   
-----------------------------------------------------------------
SMALL    10       10       1.00     60       60       1.00    
MEDIUM   0        0        N/A      0        0        N/A     
LARGE    0        0        N/A      0        0        N/A     
```

## Comparison with Original Tool

This tiered comparison tool follows the same structure and methodology as the original simple vs tiered comparison tool, but is specifically designed for:

- **Tiered-to-Tiered comparisons** instead of Simple-to-Tiered
- **Enhanced tier analysis** with detailed breakdown by tier
- **Same core metrics** (execution time, workers, CPUs, CPU time)
- **Same output formats** (console, CSV, text reports)
- **Same command-line interface** pattern for consistency

This makes it easy to use if you're already familiar with the original comparison tool, while providing the specific analysis needed for comparing different tiered strategy configurations. 