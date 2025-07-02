# Simple Migration Helper Scripts

This directory contains helper scripts to automate the end-to-end simple migration simulation process.

## Overview

The simple migration runner automates the following workflow:

1. **AWS SSO Login** - Authenticates with AWS using SSO
2. **Environment Setup** - Loads configuration and sets environment variables
3. **Metadata Check** - Verifies that required metadata exists in S3:
   - `s3://bucket/mig<ID>/metadata/subsets/calculationMetadata/desc*`
   - `s3://bucket/mig<ID>/metadata/GlobalStateSummary*`
   - If either is **missing**, the migration is **skipped** (not failed) and logged appropriately
   - If both **exist**, processing continues
4. **Go Command Execution** - Runs the migration-bucket-accessor to calculate subsets
5. **S3 Download** - Downloads subset definition files from S3
6. **Simple Simulation** - Runs the simple simulation on downloaded data
7. **Reporting** - Generates execution reports and summaries

## Files

- `simple_migration_runner.py` - Main helper script
- `requirements_simple_migration.txt` - Python dependencies
- `README.md` - This documentation

## Installation

1. Create a virtual environment (recommended):
```bash
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. Install dependencies (execute from project root):
```bash
# From TieredStrategySimulation/ (project root)
pip install -r simple/helper_scripts/requirements_simple_migration.txt
```

## Configuration

### Create Configuration File

First, create a sample configuration file (execute from project root):

```bash
# From TieredStrategySimulation/ (project root)
python simple/helper_scripts/simple_migration_runner.py --create-sample-config
```

This creates `simple_migration_config_sample.yaml` in the `simple/helper_scripts/` directory, which you should customize.

### Configuration Structure

The configuration file has the following sections:

#### Migration Section
```yaml
migration:
  cloud_provider: "AWS"
  access_key: "YOUR_ACCESS_KEY_HERE"
  secret_key: "YOUR_SECRET_KEY_HERE"
  bucket: "your-bucket-name"
  region: "eu-west-1"
  storage_endpoint: "https://s3.eu-west-1.amazonaws.com"
  log_level: "DEBUG"
  subset_calculation_label: "generalCalculation"
  subset_calculation_strategy: "simple"
  enable_subset_size_cap: true
  enable_subset_num_sstable_cap: true
  max_num_sstables_per_subset: 250
  max_workers: 4
  worker_processing_time_unit: 1000
```

#### Go Command Section
```yaml
go_command:
  executable: "./mba/migration-bucket-accessor"
  args: ["calc_subsets"]
```

#### S3 Section
```yaml
s3:
  path_template: "{migration_id}/metadata/subsets/{subset_calculation_label}/"
```

#### Simulation Section
```yaml
simulation:
  worker_config:
    max_workers: 4
  visualization:
    no_plotly: false
    plotly_comprehensive: true
  output:
    output_name: "simple_migration"
    output_dir: "simulation_outputs/{migration_id}"
  custom_args: []
```

## Usage

### Execution Location

The `simple_migration_runner.py` script should be executed from the **project root directory** (`TieredStrategySimulation/`), not from the `simple/` or `simple/helper_scripts/` directories.

**Correct execution location:**
```bash
# From TieredStrategySimulation/ (project root)
python simple/helper_scripts/simple_migration_runner.py --start-id 1 --end-id 5 --execution-name "test_run"
```

**Why from project root?**
- The Go command path `./mba/migration-bucket-accessor` is relative to the project root
- Configuration file resolution looks in both current directory and helper_scripts directory
- Output paths and directory structures are designed for project root execution

### Configuration File Location

The script will look for `simple_migration_config.yaml` in this order:
1. Current working directory (project root when executed correctly)
2. `simple/helper_scripts/` directory
3. Custom path specified with `--config-path`

You can specify a custom configuration file using the `--config-path` option:

```bash
# Use a specific config file
python simple/helper_scripts/simple_migration_runner.py --config-path "/path/to/my_config.yaml" --start-id 1 --end-id 5 --execution-name "test"

# Use a config file in a different directory
python simple/helper_scripts/simple_migration_runner.py --config-path "configs/production_config.yaml" --start-id 10 --end-id 20 --execution-name "prod"

# Use relative path from project root
python simple/helper_scripts/simple_migration_runner.py --config-path "simple/helper_scripts/my_custom_config.yaml" --start-id 1 --end-id 3 --execution-name "custom"
```

**Configuration File Setup:**
1. Create a sample: `python simple/helper_scripts/simple_migration_runner.py --create-sample-config`
2. Copy the sample: `cp simple/helper_scripts/simple_migration_config_sample.yaml my_config.yaml`
3. Customize your copy with your specific settings
4. Use it: `--config-path "my_config.yaml"`

### Basic Usage

```bash
# Execute from project root (TieredStrategySimulation/)
python simple/helper_scripts/simple_migration_runner.py --start-id 1 --end-id 5 --execution-name "test_run"
```

### Command Line Options

- `--start-id` (required) - Starting migration ID number
- `--end-id` (required) - Ending migration ID number  
- `--execution-name` (required) - Name for this execution (used in report filenames)
- `--prefix` - Prefix for migration IDs (default: "mig")
- `--output-dir` - Output directory for execution reports (default: "exec_output")
- `--config-path` - Path to configuration file (default: searches for `simple_migration_config.yaml`)
- `--bucket` - S3 bucket name (overrides config)
- `--create-sample-config` - Create a sample configuration file

### Examples

#### Process migrations mig001 through mig003:
```bash
# From TieredStrategySimulation/ (project root)
python simple/helper_scripts/simple_migration_runner.py --start-id 1 --end-id 3 --execution-name "initial_test"
```

#### Use custom config file:
```bash
# From TieredStrategySimulation/ (project root)
python simple/helper_scripts/simple_migration_runner.py --start-id 10 --end-id 15 --execution-name "prod_run" --config-path "/path/to/my_config.yaml"
```

#### Override bucket name:
```bash
# From TieredStrategySimulation/ (project root)
python simple/helper_scripts/simple_migration_runner.py --start-id 1 --end-id 2 --execution-name "custom_bucket" --bucket "my-custom-bucket"
```

## Output

The script generates several outputs:

### Per Migration
- HTML results file with simulation summary
- Configuration file showing parameters used
- Plotly visualization files (timeline, details, distribution)

### Execution Reports
- Text report: `simple_execution_report_{execution_name}.txt`
- CSV report: `simple_execution_report_{execution_name}.csv`

### Console Output
- Live logging during execution
- Final summary with file:// links to all generated files

## Environment Variables

The script sets these environment variables for the Go command:

- `CLOUD_PROVIDER`
- `MIGRATION_ACCESS_KEY`
- `MIGRATION_SECRET_KEY`
- `MIGRATION_BUCKET`
- `MIGRATION_REGION`
- `MIGRATION_STORAGE_ENDPOINT`
- `MIGRATION_LOG_LEVEL`
- `MIGRATION_SUBSET_CALCULATION_LABEL`
- `MIGRATION_SUBSET_CALCULATION_STRATEGY`
- `MIGRATION_ENABLE_SUBSET_SIZE_CAP`
- `MIGRATION_ENABLE_SUBSET_NUM_SSTABLE_CAP`
- `MIGRATION_MAX_NUM_SSTABLES_PER_SUBSET`
- `MIGRATION_ID` (automatically set to current migration ID)
- `MIGRATION_MAX_WORKERS`
- `MIGRATION_WORKER_PROCESSING_TIME_UNIT`

## Differences from Tiered Simulation

Key differences from the tiered migration helper:

1. **Simplified Environment Variables** - No tier-specific parameters
2. **Different S3 Path** - Uses `generalCalculation` instead of `mytieredcalc`
3. **Simple Simulation** - Calls `run_simple_simulation.py` instead of tiered version
4. **Worker Configuration** - Only `max_workers` parameter instead of per-tier settings
5. **Visualization Options** - Focuses on plotly visualizations

## Troubleshooting

### AWS Authentication Issues
- Ensure AWS CLI is installed and configured
- Run `aws configure sso` if using SSO
- Check that your profile name matches (default: "astra-conn")

### Configuration File Not Found
- Ensure `simple_migration_config.yaml` exists in current directory or helper_scripts directory
- Use `--config-path` to specify explicit path to your custom configuration file
- Verify the path is correct and the file exists: `ls -la /path/to/your/config.yaml`
- Check file permissions: the script needs read access to the configuration file

### Go Command Fails
- Ensure `mba/migration-bucket-accessor` exists and is executable
- Check that environment variables are set correctly
- Verify AWS credentials have necessary permissions

### S3 Download Issues
- Check bucket name and permissions
- Verify S3 path template matches your data structure
- Ensure AWS credentials are valid

### Missing Metadata Issues
- Some migration IDs may not have required metadata in S3
- Check the execution logs for "Skipped (no metadata)" messages
- Skipped migrations are logged but don't cause execution failure
- **Required metadata files for simple simulation:**
  - `s3://bucket/mig<ID>/metadata/subsets/calculationMetadata/desc*`
  - `s3://bucket/mig<ID>/metadata/GlobalStateSummary*`
- Both files must exist for processing to continue

### Execution Location Issues
- **Error: "Go command not found"** - Ensure you're running from project root where `mba/` directory exists
- **Error: "Configuration file not found"** - Check that `simple_migration_config.yaml` exists in project root or `simple/helper_scripts/`
- **Error: "Simple simulation script not found"** - Make sure you're executing from project root, not from subdirectories

### Simulation Issues
- Check that downloaded files are in expected format
- Verify simple simulation script works independently
- Review simulation logs for specific errors 