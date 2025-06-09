# Multi-Tier Database Migration Simulation

A discrete event simulation system for analyzing multi-tiered worker strategies for processing database migration files. This simulation helps optimize worker allocation and identify performance bottlenecks in large-scale data migration scenarios.

## Overview

The simulation models a three-tier worker system (SMALL/MEDIUM/LARGE) processing database subset files using a greedy task scheduling algorithm with discrete event simulation. Each worker tier has configurable thread counts and concurrency limits, allowing for detailed performance analysis and bottleneck identification.

## Codebase Structure

### Core Simulation Files

**`simulation.py`** - **Low-level thread simulation engine**
- Contains `WorkItem` (represents work units with key/size), `ThreadSimulator` (tracks individual thread state), and `CompletionEvent` (for event-driven scheduling)
- Main function `run_simulation()` implements work scheduling using largest-first assignment and event-driven completion handling
- Uses a min-heap for efficient event processing (threads completing work and becoming available)

**`worker_simulation.py`** - **Multi-tier worker orchestration** 
- Defines `Worker` class that wraps multiple threads and processes file subsets
- `MultiTierSimulation` class manages the overall simulation with three tiers (SMALL/MEDIUM/LARGE workers)
- Handles worker lifecycle: spawning workers up to tier limits, processing completion events, straggler detection
- Contains the main simulation loop that processes files through available workers

**`file_processor.py`** - **Input file parsing and metadata**
- `FileMetadata` class parses file paths to extract migration info, subset IDs, tier assignments, and data sizes
- Parses actual SSTable definitions from subset files (currently supports CSV/space-separated formats)
- Directory scanning functions to discover all subset files in the input hierarchy

**`run_multi_tier_simulation.py`** - **Main entry point and CLI**
- Command line interface for configuring simulation parameters (thread counts, worker limits, thresholds)
- Orchestrates the full pipeline: parse input → configure simulation → run → output results
- Handles output file generation and CSV export

### Data Flow

The simulation flow is: **Input files** → **FileMetadata parsing** → **MultiTierSimulation** (which uses **Worker** instances running **thread simulations**) → **Results and visualizations**

## Input Requirements

### Subset Definition Files

The simulation expects a directory containing subset definition files with a specific path structure:

```
<base_directory>/<migrationId>/metadata/subsets/<Label>/<subsetId>/<tier>/<numSSTablesInSubset>/<dataSizeOfSubset>/subset-<subsetId>
```

**Path Components:**
- `<migrationId>`: Unique identifier for the migration
- `<Label>`: Migration label/name
- `<subsetId>`: Unique subset identifier
- `<tier>`: Worker tier assignment (`SMALL`, `MEDIUM`, or `LARGE`)
- `<numSSTablesInSubset>`: Number of SSTables in this subset (integer)
- `<dataSizeOfSubset>`: Total data size in bytes (integer)
- `subset-<subsetId>`: The actual subset file name

**Example:**
```
test_data/mig007/metadata/subsets/test_migration/61/MEDIUM/7/1071940830/subset-61
```

This represents:
- Migration ID: `mig007`
- Label: `test_migration`
- Subset ID: `61`
- Tier: `MEDIUM`
- SSTable count: `7`
- Data size: `1,071,940,830` bytes (~1.07 GB)

## Installation

1. **Install Python dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Required packages:**
   - `plotly>=5.18.0` - Interactive visualizations
   - `pandas>=2.0.0` - Data processing
   - `rich==14.0.0` - Terminal output formatting

## Usage

### Basic Usage

```bash
python run_multi_tier_simulation.py <directory>
```

### Command Line Options

#### Required Arguments
- `directory` - Directory containing subset files to process

#### Worker Configuration
- `--small-threads <int>` - Number of threads for SMALL tier workers (default: 6)
- `--medium-threads <int>` - Number of threads for MEDIUM tier workers (default: 4)
- `--large-threads <int>` - Number of threads for LARGE tier workers (default: 1)
- `--small-max-workers <int>` - Maximum concurrent SMALL tier workers (default: 4)
- `--medium-max-workers <int>` - Maximum concurrent MEDIUM tier workers (default: 6)
- `--large-max-workers <int>` - Maximum concurrent LARGE tier workers (default: 10)

#### Analysis Options
- `--straggler-threshold <float>` - Percentage threshold above average completion time to identify straggler threads (default: 20.0)
- `--summary-only` - Show only summary and global timeline, skip detailed views
- `--no-stragglers` - Skip straggler analysis and reporting

#### Output Options
- `--output-name <string>` - Base name for output files (default: "simulation_results")
- `--output-dir <path>` - Directory to store output files (default: output_files)
- `--no-csv` - Skip CSV data export for automated analysis

### Examples

**Basic simulation with default settings:**
```bash
python run_multi_tier_simulation.py test_data/
```

**Custom worker configuration:**
```bash
python run_multi_tier_simulation.py test_data/ \
    --small-threads 4 --medium-threads 6 --large-threads 12 \
    --small-max-workers 5 --medium-max-workers 3 --large-max-workers 2
```

**Custom output location and naming:**
```bash
python run_multi_tier_simulation.py test_data/ \
    --output-dir ./simulation_outputs \
    --output-name migration_analysis_v2
```

**Summary only with custom straggler threshold:**
```bash
python run_multi_tier_simulation.py test_data/ \
    --summary-only --straggler-threshold 15.0
```

## Output Files

The simulation generates several output files with detailed analysis results:

### HTML Visualizations

1. **`<output-name>.html`** - Timeline visualization showing worker-level execution
   - Interactive timeline with worker lifespans
   - Color-coded by tier (Green=SMALL, Red=MEDIUM, Blue=LARGE)
   - Straggler workers highlighted with gold borders
   - Zoom and pan capabilities with range slider

2. **`<output-name>_detailed.html`** - Thread-level detailed view
   - Individual thread timelines within each worker
   - Task-level granularity showing SSTable processing
   - Straggler threads highlighted with gold borders
   - Hover tooltips with task details

### CSV Data Export (when `--no-csv` is not used)

1. **`<output-name>_workers.csv`** - Worker-level data
   - Columns: `worker_id`, `tier`, `start_time`, `completion_time`, `duration`, `file_name`, `sstable_count`, `data_size_gb`, `is_straggler`

2. **`<output-name>_threads.csv`** - Thread-level data
   - Columns: `worker_id`, `tier`, `thread_id`, `start_time`, `completion_time`, `duration`, `task_count`, `total_data_processed`, `is_straggler`

3. **`<output-name>_summary.csv`** - Simulation summary statistics
   - Overall completion time, tier utilization, straggler analysis

## Simulation Algorithm

The simulation uses a **discrete event simulation** approach with:

- **Greedy task assignment**: Largest files assigned first to available workers
- **Event-driven processing**: Uses priority queues to efficiently jump between significant events
- **Multi-tier architecture**: Different worker types optimized for different file sizes
- **Realistic data processing**: Uses actual file sizes without compression scaling

### Key Metrics

- **Completion Time**: Total simulation time units
- **Worker Utilization**: Per-tier worker usage patterns
- **Straggler Analysis**: Identification of slow workers/threads that extend overall completion time
- **Thread Efficiency**: Analysis of thread-level performance within workers

## Straggler Analysis

### What are Stragglers?

**Stragglers** are workers or threads that take significantly longer to complete their tasks compared to their peers. In distributed processing systems, stragglers are critical to identify because they often determine the overall completion time of the entire job.

A **straggler thread** is an individual thread within a worker that takes longer than other threads in the same worker to process its assigned SSTables. In the simulation, this occurs when a thread has a larger total amount of data to process than the other threads in the same worker.

A **straggler worker** is an entire worker instance that has at least one straggler thread.

### Straggler Detection in This Simulation

The simulation identifies stragglers using a **percentage-based threshold**:

- **Threads** are flagged as stragglers if their completion time exceeds the average of all other threads in their worker by more than the specified threshold (default: 20%)
- **Workers** are flagged as stragglers if they contain at least one straggler thread
- **Calculation**: `completion_time > average_completion_time × (1 + threshold/100)`
- **Example**: With 20% threshold, if average completion time is 1000 units, any worker taking more than 1200 units is flagged as a straggler


## Interpreting Results

### Timeline Visualization
- **X-axis**: Time units (displayed as billions for realistic data sizes)
- **Y-axis**: Workers (grouped by tier)
- **Bar length**: Worker execution duration
- **Colors**: Green (SMALL), Red (MEDIUM), Blue (LARGE)
- **Gold borders**: Straggler workers

### Detailed Thread View
- **Individual tasks**: Each SSTable processing task shown separately
- **Thread labels**: Format "W{workerId}-T{threadId}"
- **Task borders**: Dark borders for normal tasks, gold for straggler threads

### Straggler Analysis
Workers/threads are flagged as stragglers if their completion time exceeds the average by the specified threshold percentage (default: 20%).

## Troubleshooting

### Common Issues

1. **"No valid files found"**: Check that subset files follow the exact path structure
2. **"Invalid file path format"**: Ensure file paths match the expected pattern
3. **"Invalid tier"**: Tier names must be exactly `SMALL`, `MEDIUM`, or `LARGE`

### File Path Validation

The simulation expects exact compliance with the path structure. Use this regex pattern for validation:
```
.*/([^/]+)/metadata/subsets/([^/]+)/([^/]+)/([^/]+)/(\d+)/(\d+)/subset-\3$
```

### Performance Notes

- Large datasets may take significant time to process
- HTML files can become large with many workers/threads
- CSV export provides lighter-weight data for analysis
- Use `--summary-only` for faster execution on large datasets

## Integration

The CSV outputs are designed for integration with automated analysis pipelines:
- Standard CSV format for easy parsing
- Consistent column naming across files
- Boolean flags for easy filtering (e.g., straggler identification)
- Numeric data types for statistical analysis # tiered-subset-strategy-simulation
