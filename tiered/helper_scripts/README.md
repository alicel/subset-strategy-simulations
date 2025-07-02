# Helper Scripts

This directory contains helper scripts for running the tiered strategy simulation.

## Migration Runner

The `tiered_migration_runner.py` script automates the process of running the tiered strategy simulation. It handles:
- AWS SSO login
- Configuration parsing
- Environment variable management
- Execution of the tiered strategy
- Downloading strategy results from S3
- Running the simulation with the downloaded results

### Prerequisites

1. Python 3.6 or higher
2. Required Python packages (install using `pip install -r requirements.txt`):
   - boto3
   - click
   - python-dotenv
3. AWS CLI configured with SSO
4. Go environment set up for the simulation

### Configuration

1. Generate a sample configuration file:
   ```bash
   python3 tiered_migration_runner.py --create-sample-config
   ```
   This will create `migration_config_sample.yaml` in the `tiered/helper_scripts/` directory

2. Edit `migration_config_sample.yaml` with your specific settings:
   ```yaml
   migration:
     # Credentials
     access_key: "YOUR_ACCESS_KEY_HERE"
     secret_key: "YOUR_SECRET_KEY_HERE"
     
     # Basic Settings
     bucket: "your-bucket-name"
     region: "your-aws-region"
     log_level: "DEBUG"
     
     # General Parameters
     max_num_sstables_per_subset: 250
     subset_calculation_label: "mytieredcalc"
     
     # Small Tier Parameters
     small_tier_max_sstable_size_gb: 10
     small_tier_thread_subset_max_size_floor_gb: 2
     small_tier_worker_num_threads: 4
     
     # Medium Tier Parameters
     medium_tier_max_sstable_size_gb: 50
     medium_tier_worker_num_threads: 6
     
     # Optimization
     optimize_packing_medium_subsets: false
   ```

3. **Important**: After customizing the configuration, rename the file to the default name that the script expects:
   ```bash
   mv migration_config_sample.yaml migration_runner_config.yaml
   ```

### Usage

Run the script with:
```bash
python tiered_migration_runner.py
```

The script will:
1. Log in to AWS SSO
2. Parse the configuration
3. Execute the Go command
4. Download results from S3
5. Run the simulation with the downloaded objects

### Environment Variables

The script uses the following environment variables:
- `AWS_PROFILE`: AWS profile to use (default: "default")
- `AWS_REGION`: AWS region to use (default: from config)
- `GO_COMMAND`: Go command to run (default: from config)
- `RESULTS_DIR`: Directory to store results (default: from config)

You can set these in a `.env` file in the same directory as the script.

### Error Handling

The script includes error handling for:
- AWS SSO login failures
- Configuration file issues
- Go command execution errors
- S3 download failures
- Simulation execution errors

### Logging

The script logs all operations to the console with appropriate log levels (INFO, ERROR, etc.).

## Migration Runner Script

The `tiered_migration_runner.py` script automates the process of running migrations and simulations.

### Features

- AWS SSO authentication
- Configuration file parsing (YAML/JSON)
- Environment variable management
- Batch processing of migration IDs
- Go command execution
- S3 result downloads
- Simulation execution

### Setup

1. Install dependencies:
   ```bash
   pip install -r requirements_migration.txt
   ```

2. Generate sample configuration:
   ```bash
   python3 tiered_migration_runner.py --create-sample-config
   ```

3. Customize the generated `migration_config_sample.yaml` file

### Usage

The `--execution-name` parameter is **required** and serves as the top-level identifier for organizing all outputs from a migration run. Choose meaningful names like `production_migration_v1`, `performance_test_jan2024`, or `baseline_comparison`.

```bash
# Basic usage with required parameters
python3 tiered_migration_runner.py --start-id 100 --end-id 200 --execution-name my_test_run

# Using custom configuration file
python3 tiered_migration_runner.py --config-path custom_config.yaml --start-id 100 --end-id 200 --execution-name production_migration

# Full example with all options
python3 tiered_migration_runner.py \
    --start-id 100 \
    --end-id 200 \
    --execution-name performance_test \
    --prefix mig \
    --config-path migration_config.yaml \
    --bucket my-custom-bucket
```

### Command Line Options

- `--start-id`: Starting migration ID number (required)
- `--end-id`: Ending migration ID number (required)
- `--execution-name`: Name for this execution run - used to structure output directories and files (required)
- `--prefix`: Migration ID prefix (default: "mig")
- `--config-path`: Path to configuration file (default: "migration_runner_config.yaml")
- `--bucket`: S3 bucket name (overrides config)
- `--output-dir`: Output directory for execution reports (default: "exec_output")
- `--create-sample-config`: Create sample configuration file

### Execution Naming Best Practices

The `--execution-name` is used throughout the system to organize outputs:

- **File Organization**: All outputs are grouped under `tiered/output/{execution-name}/`
- **Report Generation**: Execution reports are named using this identifier
- **Simulation Output**: Individual migration results are organized by execution name
- **Comparison**: Different execution names allow easy comparison of multiple runs

**Recommended naming patterns:**
- `baseline_v1` - Initial baseline measurements
- `perf_test_2024_01_15` - Performance testing with date
- `prod_migration_batch1` - Production migration batches
- `config_comparison_a` - Configuration comparison runs

## Configuration File

The configuration file supports both YAML and JSON formats and controls all aspects of the migration and simulation process.

### Complete Configuration Structure

```yaml
# Migration-specific environment variables
migration:
  # Credentials
  access_key: "YOUR_ACCESS_KEY_HERE"                       # MIGRATION_ACCESS_KEY
  secret_key: "YOUR_SECRET_KEY_HERE"                       # MIGRATION_SECRET_KEY
  
  # Basic Settings
  bucket: "alice-sst-sdl-test"                            # MIGRATION_BUCKET
  region: "eu-west-1"                                     # MIGRATION_REGION
  log_level: "DEBUG"                                       # MIGRATION_LOG_LEVEL
  
  # General Parameters
  max_num_sstables_per_subset: 250                        # MIGRATION_MAX_NUM_SSTABLES_PER_SUBSET
  subset_calculation_label: "mytieredcalc"                # MIGRATION_SUBSET_CALCULATION_LABEL
  
  # Small Tier Parameters
  small_tier_max_sstable_size_gb: 10                      # MIGRATION_SMALL_TIER_MAX_SSTABLE_SIZE_GB
  small_tier_thread_subset_max_size_floor_gb: 2           # MIGRATION_SMALL_TIER_THREAD_SUBSET_MAX_SIZE_FLOOR_GB
  small_tier_worker_num_threads: 4                        # MIGRATION_SMALL_TIER_WORKER_NUM_THREADS
  
  # Medium Tier Parameters
  medium_tier_max_sstable_size_gb: 50                     # MIGRATION_MEDIUM_TIER_MAX_SSTABLE_SIZE_GB
  medium_tier_worker_num_threads: 6                       # MIGRATION_MEDIUM_TIER_WORKER_NUM_THREADS
  
  # Optimization
  optimize_packing_medium_subsets: false                  # MIGRATION_OPTIMIZE_PACKING_MEDIUM_SUBSETS

# Go command configuration
go_command:
  executable: "./mba/migration-bucket-accessor"           # Path to Go executable
  args: ["calc_subsets"]                                  # Command arguments (hardcoded)

# S3 download configuration
s3:
  path_template: "{migration_id}/metadata/subsets/mytieredcalc/"  # S3 path pattern

# Simulation configuration (maps to run_multi_tier_simulation.py CLI options)
simulation:
  # Analysis options
  analysis:
    # Execution Configuration
    execution_mode: "concurrent"        # --execution-mode: concurrent, sequential, or round_robin
    max_concurrent_workers: 20          # --max-concurrent-workers (required for round_robin mode)
    
    # Worker Allocation
    small_max_workers: 4                # --small-max-workers (required for non-round_robin modes)
    medium_max_workers: 6               # --medium-max-workers (required for non-round_robin modes)
    large_max_workers: 10               # --large-max-workers (required for non-round_robin modes)
    
    # Analysis Features
    enable_straggler_detection: true    # Enable straggler detection analysis
    straggler_threshold: 20.0           # --straggler-threshold (percentage)
    summary_only: false                 # --summary-only flag
  
  # Output configuration
  output:
    output_name: "migration_simulation" # --output-name (migration ID will be appended)
    output_dir: "simulation_outputs/{migration_id}"  # --output-dir (supports templates)
    no_csv: false                       # --no-csv flag
    detailed_page_size: 30              # --detailed-page-size (0 to disable pagination)
  
  # Additional custom arguments
  custom_args: []                       # Any additional CLI arguments
```

### Configuration Sections

#### Migration Section
Controls environment variables passed to the Go program. The script automatically adds the `MIGRATION_` prefix to most variables. Some values are hardcoded for simplicity:
- `cloud_provider`: Hardcoded to "AWS" (not configurable)
- `subset_calculation_strategy`: Hardcoded to "tiered" (not configurable)

**Configuration Parameters (in logical order):**
- **Credentials**: `access_key` / `secret_key` - AWS credentials for S3 access
- **Basic Settings**: `bucket`, `region`, `log_level` - Core AWS and logging configuration
- **General Parameters**: `max_num_sstables_per_subset`, `subset_calculation_label` - General migration settings
- **Small Tier Parameters**: Size limits, threading, and subset configuration for small files
- **Medium Tier Parameters**: Size limits and threading configuration for medium files
- **Optimization**: `optimize_packing_medium_subsets` - Performance tuning option

#### Go Command Section
Configures the execution of the `migration-bucket-accessor` program.

- `executable`: Path to the Go program (relative to project root)
- `args`: Command line arguments (hardcoded to `["calc_subsets"]`)

#### S3 Section
Controls S3 download behavior.

- `path_template`: S3 path pattern with `{migration_id}` placeholder
- Downloads preserve full directory structure in `downloadedSubsetDefinitions/`

#### Simulation Section
Maps directly to `run_multi_tier_simulation.py` CLI options.

**Worker Thread Configuration:**
Worker thread counts are automatically derived from the migration section:
- `large_threads`: Always 1 (hardcoded)
- `medium_threads`: Uses `medium_tier_worker_num_threads` from migration config
- `small_threads`: Uses `small_tier_worker_num_threads` from migration config

**Analysis Options (in logical order):**
- **Execution Configuration**:
  - `execution_mode`: Worker scheduling mode - `concurrent` (all tiers parallel), `sequential` (LARGE→MEDIUM→SMALL), or `round_robin` (global limit with round-robin allocation)
  - `max_concurrent_workers`: Total worker limit across all tiers (required for round_robin mode)
- **Worker Allocation**:
  - `small_max_workers`: Maximum concurrent workers for small tier (required for non-round_robin modes)
  - `medium_max_workers`: Maximum concurrent workers for medium tier (required for non-round_robin modes)
  - `large_max_workers`: Maximum concurrent workers for large tier (required for non-round_robin modes)
- **Analysis Features**:
  - `enable_straggler_detection`: Enable straggler detection analysis (true/false)
  - `straggler_threshold`: Percentage threshold for straggler detection
  - `summary_only`: Generate only summary visualizations

**Output Options:**
- `output_name`: Base name for output files (migration ID appended automatically)
- `output_dir`: Output directory (supports `{migration_id}` template)
- `no_csv`: Skip CSV data export
- `detailed_page_size`: Workers per page in detailed view (0 = no pagination)

### Environment Variables

The script sets the following environment variables:

```bash
# Hardcoded values (not configurable)
CLOUD_PROVIDER=AWS
MIGRATION_SUBSET_CALCULATION_STRATEGY=tiered

# From migration config (in logical order)
# Credentials
MIGRATION_ACCESS_KEY=YOUR_ACCESS_KEY_HERE
MIGRATION_SECRET_KEY=YOUR_SECRET_KEY_HERE

# Basic Settings
MIGRATION_BUCKET=alice-sst-sdl-test
MIGRATION_REGION=eu-west-1
MIGRATION_LOG_LEVEL=DEBUG

# General Parameters
MIGRATION_MAX_NUM_SSTABLES_PER_SUBSET=250
MIGRATION_SUBSET_CALCULATION_LABEL=mytieredcalc

# Small Tier Parameters
MIGRATION_SMALL_TIER_MAX_SSTABLE_SIZE_GB=10
MIGRATION_SMALL_TIER_THREAD_SUBSET_MAX_SIZE_FLOOR_GB=2
MIGRATION_SMALL_TIER_WORKER_NUM_THREADS=4

# Medium Tier Parameters
MIGRATION_MEDIUM_TIER_MAX_SSTABLE_SIZE_GB=50
MIGRATION_MEDIUM_TIER_WORKER_NUM_THREADS=6

# Optimization
MIGRATION_OPTIMIZE_PACKING_MEDIUM_SUBSETS=false

# Automatically generated
MIGRATION_ID=mig100  # Set to current migration ID
```

### Template Variables

The following template variables can be used in configuration values:

- `{migration_id}`: Current migration ID (e.g., "mig100")
- `{download_dir}`: Local download directory path

### Example Configurations

#### Basic Configuration
```yaml
migration:
  access_key: "AKIA..."
  secret_key: "xyz..."
  bucket: "my-migration-bucket"
  region: "us-east-1"
  log_level: "INFO"

simulation:
  output:
    output_name: "test_run"
```

#### High-Performance Configuration
```yaml
migration:
  # ... migration settings ...

simulation:
  analysis:
    execution_mode: "concurrent"
    max_concurrent_workers: 25
    small_max_workers: 10
    medium_max_workers: 8
    large_max_workers: 6
    enable_straggler_detection: true
    straggler_threshold: 15.0
    summary_only: false
  output:
    detailed_page_size: 50
```

#### Summary-Only Configuration
```yaml
migration:
  # ... migration settings ...

simulation:
  analysis:
    execution_mode: "concurrent"
    max_concurrent_workers: 20
    small_max_workers: 4
    medium_max_workers: 6
    large_max_workers: 10
    enable_straggler_detection: false
    summary_only: true
  output:
    no_csv: true
    output_name: "quick_analysis"
```

#### Sequential Execution Configuration
```yaml
migration:
  # ... migration settings ...

simulation:
  analysis:
    execution_mode: "sequential"  # Process LARGE→MEDIUM→SMALL sequentially
    max_concurrent_workers: 20
    small_max_workers: 4
    medium_max_workers: 6
    large_max_workers: 10
    enable_straggler_detection: true
    straggler_threshold: 25.0
    summary_only: false
  output:
    output_name: "sequential_analysis"
```

#### Round-Robin Execution Configuration
```yaml
migration:
  # ... migration settings ...

simulation:
  analysis:
    execution_mode: "round_robin"    # Round-robin allocation across tiers
    max_concurrent_workers: 15       # Total workers across all tiers
    enable_straggler_detection: true
    straggler_threshold: 20.0
    summary_only: false
  output:
    output_name: "round_robin_analysis"
```

### Execution Flow

For each migration ID in the specified range (e.g., `mig100` to `mig200`) with execution name `performance_test`:

1. **Environment Setup**: Set all `MIGRATION_*` environment variables
2. **Metadata Check**: Verify that metadata exists in S3 at `s3://bucket/mig100/metadata/subsets/calculationMetadata/desc*`
   - If metadata is **missing**, the migration is **skipped** (not failed) and logged appropriately
   - If metadata **exists**, processing continues
3. **Go Execution**: Run `./mba/migration-bucket-accessor calc_subsets`
4. **S3 Download**: Download from `s3://bucket/mig100/metadata/subsets/mytieredcalc/` to `downloadedSubsetDefinitions/mig100/`
5. **Simulation**: Execute `run_multi_tier_simulation.py` with configured options
6. **Output Organization**: Move results to `tiered/output/performance_test/mig100/`
7. **Report Generation**: Create execution summary reports in `tiered/output/performance_test/exec_reports/`

### Output Structure

The `--execution-name` parameter structures all output files and directories. For example, with `--execution-name performance_test`:

```
tiered/
├── output/
│   └── performance_test/                          # Organized by execution name
│       ├── exec_reports/
│       │   ├── execution_report_performance_test.txt    # Execution summary
│       │   └── execution_report_performance_test.csv    # Execution metrics
│       ├── mig100/
│       │   ├── plots/
│       │   │   ├── tiered_migration_simulation_mig100.html      # Timeline
│       │   │   └── tiered_migration_simulation_mig100_detailed.html  # Details
│       │   └── data/
│       │       ├── tiered_migration_simulation_mig100_workers.csv
│       │       ├── tiered_migration_simulation_mig100_threads.csv
│       │       └── config_tiered_migration_simulation_mig100.txt
│       └── mig101/
│           └── ... (same structure)
└── helper_scripts/
    └── downloadedSubsetDefinitions/
        ├── mig100/
        │   └── metadata/subsets/mytieredcalc/     # Downloaded S3 files
        └── mig101/
            └── metadata/subsets/mytieredcalc/
```

### Troubleshooting

#### Common Configuration Issues

1. **Missing AWS credentials**: Ensure `access_key` and `secret_key` are set
2. **Invalid S3 bucket**: Check bucket name and permissions
3. **Go program not found**: Verify `./mba/migration-bucket-accessor` exists
4. **Template errors**: Check `{migration_id}` placeholder usage
5. **Missing metadata**: Some migration IDs may not have metadata in S3
   - Check the execution logs for "Skipped (no metadata)" messages
   - Skipped migrations are logged but don't cause execution failure
   - Metadata is checked at: `s3://bucket/mig<ID>/metadata/subsets/calculationMetadata/desc*`

#### Validation

Test with a single migration ID for detailed logging:
```bash
python3 tiered_migration_runner.py --config-path config.yaml --start-id 100 --end-id 100 --execution-name test_run
```

#### AWS SSO Issues

If AWS SSO login fails:
- Check AWS CLI installation
- Verify profile configuration
- Ensure SSO session is valid 