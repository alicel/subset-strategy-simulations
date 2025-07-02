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
     cloud_provider: "AWS"
     access_key: "YOUR_ACCESS_KEY_HERE"
     bucket: "your-bucket-name"
     log_level: "DEBUG"
     medium_tier_max_sstable_size_gb: 50
     medium_tier_worker_num_threads: 6
     optimize_packing_medium_subsets: false
     region: "your-aws-region"
     secret_key: "YOUR_SECRET_KEY_HERE"
     small_tier_max_sstable_size_gb: 10
     small_tier_thread_subset_max_size_floor_gb: 2
     small_tier_worker_num_threads: 4
     subset_calculation_label: "mytieredcalc"
     subset_calculation_strategy: "tiered"
     max_num_sstables_per_subset: 250
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

```bash
# Using default configuration file (migration_runner_config.yaml)
python3 tiered_migration_runner.py --start-id 100 --end-id 200

# Using custom configuration file
python3 tiered_migration_runner.py --config your_config.yaml --start-id 100 --end-id 200
```

### Command Line Options

- `--config, -c`: Path to configuration file (default: "migration_runner_config.yaml")
- `--start-id, -s`: Starting migration ID number (required)
- `--end-id, -e`: Ending migration ID number (required)
- `--prefix, -p`: Migration ID prefix (default: "mig")
- `--bucket, -b`: S3 bucket name (overrides config)
- `--create-sample-config`: Create sample configuration file
- `--verbose, -v`: Enable verbose logging

## Configuration File

The configuration file supports both YAML and JSON formats and controls all aspects of the migration and simulation process.

### Complete Configuration Structure

```yaml
# Migration-specific environment variables
migration:
  cloud_provider: "AWS"                                    # CLOUD_PROVIDER
  access_key: "YOUR_ACCESS_KEY_HERE"                       # MIGRATION_ACCESS_KEY
  bucket: "alice-sst-sdl-test"                            # MIGRATION_BUCKET
  log_level: "DEBUG"                                       # MIGRATION_LOG_LEVEL
  medium_tier_max_sstable_size_gb: 50                     # MIGRATION_MEDIUM_TIER_MAX_SSTABLE_SIZE_GB
  medium_tier_worker_num_threads: 6                       # MIGRATION_MEDIUM_TIER_WORKER_NUM_THREADS
  optimize_packing_medium_subsets: false                  # MIGRATION_OPTIMIZE_PACKING_MEDIUM_SUBSETS
  region: "eu-west-1"                                     # MIGRATION_REGION
  secret_key: "YOUR_SECRET_KEY_HERE"                      # MIGRATION_SECRET_KEY
  small_tier_max_sstable_size_gb: 10                      # MIGRATION_SMALL_TIER_MAX_SSTABLE_SIZE_GB
  small_tier_thread_subset_max_size_floor_gb: 2           # MIGRATION_SMALL_TIER_THREAD_SUBSET_MAX_SIZE_FLOOR_GB
  small_tier_worker_num_threads: 4                        # MIGRATION_SMALL_TIER_WORKER_NUM_THREADS
  subset_calculation_label: "mytieredcalc"                # MIGRATION_SUBSET_CALCULATION_LABEL
  subset_calculation_strategy: "tiered"                   # MIGRATION_SUBSET_CALCULATION_STRATEGY
  max_num_sstables_per_subset: 250

# Go command configuration
go_command:
  executable: "./mba/migration-bucket-accessor"           # Path to Go executable
  args: ["calc_subsets"]                                  # Command arguments (hardcoded)

# S3 download configuration
s3:
  path_template: "{migration_id}/metadata/subsets/mytieredcalc/"  # S3 path pattern

# Simulation configuration (maps to run_multi_tier_simulation.py CLI options)
simulation:
  # Worker tier configuration
  worker_config:
    small_threads: 6                    # --small-threads
    medium_threads: 4                   # --medium-threads
    large_threads: 1                    # --large-threads
    small_max_workers: 4                # --small-max-workers
    medium_max_workers: 6               # --medium-max-workers
    large_max_workers: 10               # --large-max-workers
  
  # Analysis options
  analysis:
    straggler_threshold: 20.0           # --straggler-threshold (percentage)
    summary_only: false                 # --summary-only flag
    no_stragglers: false                # --no-stragglers flag
    execution_mode: "concurrent"        # --execution-mode: concurrent, sequential, or round_robin
    max_concurrent_workers: 20          # --max-concurrent-workers (required for round_robin mode)
  
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
Controls environment variables passed to the Go program. The script automatically adds the `MIGRATION_` prefix to most variables (except `cloud_provider` → `CLOUD_PROVIDER`).

**Key Variables:**
- `access_key` / `secret_key`: AWS credentials for S3 access
- `bucket`: S3 bucket name for downloads
- `region`: AWS region
- `log_level`: Logging level for the Go program
- `*_tier_*`: Worker tier configurations
- `subset_calculation_*`: Subset calculation parameters

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

**Worker Configuration:**
- `*_threads`: Number of threads per worker in each tier
- `*_max_workers`: Maximum concurrent workers per tier

**Analysis Options:**
- `straggler_threshold`: Percentage threshold for straggler detection
- `summary_only`: Generate only summary visualizations
- `no_stragglers`: Skip straggler analysis
- `execution_mode`: Worker scheduling mode - `concurrent` (all tiers parallel), `sequential` (LARGE→MEDIUM→SMALL), or `round_robin` (global limit with round-robin allocation)
- `max_concurrent_workers`: Total worker limit across all tiers (required for round_robin mode)

**Output Options:**
- `output_name`: Base name for output files (migration ID appended automatically)
- `output_dir`: Output directory (supports `{migration_id}` template)
- `no_csv`: Skip CSV data export
- `detailed_page_size`: Workers per page in detailed view (0 = no pagination)

### Environment Variables

The script sets the following environment variables from the `migration` section:

```bash
CLOUD_PROVIDER=AWS
MIGRATION_ACCESS_KEY=YOUR_ACCESS_KEY_HERE
MIGRATION_BUCKET=alice-sst-sdl-test
MIGRATION_ID=mig100  # Automatically set to current migration ID
MIGRATION_LOG_LEVEL=DEBUG
MIGRATION_MEDIUM_TIER_MAX_SSTABLE_SIZE_GB=50
MIGRATION_MEDIUM_TIER_WORKER_NUM_THREADS=6
MIGRATION_OPTIMIZE_PACKING_MEDIUM_SUBSETS=false
MIGRATION_REGION=eu-west-1
MIGRATION_SECRET_KEY=YOUR_SECRET_KEY_HERE
MIGRATION_SMALL_TIER_MAX_SSTABLE_SIZE_GB=10
MIGRATION_SMALL_TIER_THREAD_SUBSET_MAX_SIZE_FLOOR_GB=2
MIGRATION_SMALL_TIER_WORKER_NUM_THREADS=4
MIGRATION_SUBSET_CALCULATION_LABEL=mytieredcalc
MIGRATION_SUBSET_CALCULATION_STRATEGY=tiered
MIGRATION_MAX_NUM_SSTABLES_PER_SUBSET=250
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
  worker_config:
    small_threads: 4
    medium_threads: 6
    large_threads: 2
  output:
    output_name: "test_run"
```

#### High-Performance Configuration
```yaml
migration:
  # ... migration settings ...

simulation:
  worker_config:
    small_threads: 8
    medium_threads: 12
    large_threads: 4
    small_max_workers: 10
    medium_max_workers: 8
    large_max_workers: 6
  analysis:
    straggler_threshold: 15.0
  output:
    detailed_page_size: 50
```

#### Summary-Only Configuration
```yaml
migration:
  # ... migration settings ...

simulation:
  analysis:
    summary_only: true
    no_stragglers: true
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
    straggler_threshold: 25.0
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
    straggler_threshold: 20.0
  output:
    output_name: "round_robin_analysis"
```

### Execution Flow

For each migration ID (e.g., `mig100` to `mig200`):

1. **Environment Setup**: Set all `MIGRATION_*` environment variables
2. **Go Execution**: Run `./mba/migration-bucket-accessor calc_subsets`
3. **S3 Download**: Download from `s3://bucket/mig100/metadata/subsets/mytieredcalc/` to `downloadedSubsetDefinitions/mig100/`
4. **Simulation**: Execute `run_multi_tier_simulation.py` with configured options
5. **Output**: Generate results in `simulation_outputs/mig100/`

### Output Structure

```
simulation_outputs/
├── mig100/
│   ├── migration_simulation_mig100.html           # Timeline visualization
│   ├── migration_simulation_mig100_detailed.html  # Thread-level details
│   ├── migration_simulation_mig100_workers.csv    # Worker data
│   ├── migration_simulation_mig100_threads.csv    # Thread data
│   └── config_migration_simulation_mig100.txt     # Configuration record
├── mig101/
│   └── ... (same structure)
downloadedSubsetDefinitions/
├── mig100/
│   └── metadata/subsets/mytieredcalc/             # Downloaded S3 files
├── mig101/
│   └── metadata/subsets/mytieredcalc/
```

### Troubleshooting

#### Common Configuration Issues

1. **Missing AWS credentials**: Ensure `access_key` and `secret_key` are set
2. **Invalid S3 bucket**: Check bucket name and permissions
3. **Go program not found**: Verify `./mba/migration-bucket-accessor` exists
4. **Template errors**: Check `{migration_id}` placeholder usage

#### Validation

Use `--verbose` flag for detailed logging:
```bash
python3 tiered_migration_runner.py --config config.yaml --start-id 100 --end-id 100 --verbose
```

#### AWS SSO Issues

If AWS SSO login fails:
- Check AWS CLI installation
- Verify profile configuration
- Ensure SSO session is valid 