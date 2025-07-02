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
- `<subsetId>`: Unique subset identifier (**must be an integer**)
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

### Subset File Content Format

Each subset file must contain the individual SSTable definitions in one of the following formats:

**Comma-separated format:**
```
sstable_id,size_in_bytes
```

**Space-separated format:**
```
sstable_id size_in_bytes
```

**Requirements:**
- Each line represents one SSTable
- `sstable_id` must be an integer
- `size_in_bytes` must be an integer representing the SSTable size in bytes
- Empty lines and lines starting with `#` are ignored (comments)
- Both comma-separated and space-separated formats are supported

**Example subset file content:**
```
# SSTables for subset 61
1001,156789012
1002,234567890
1003,198765432
1004,145678901
1005,267890123
1006,189012345
1007,123456789
```

This example defines 7 SSTables with IDs from 1001 to 1007 and their respective sizes in bytes. The simulation will process these SSTables in the exact order they appear in the file.

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

**Important Note**: The script must be run from the root directory of the project. If you're using the helper scripts (like `migration_runner.py`), make sure to run them from the project root directory:

```bash
# Correct way (from project root):
python helper_scripts/migration_runner.py --start-id 100 --end-id 102

# Incorrect way (from helper_scripts directory):
cd helper_scripts
python migration_runner.py --start-id 100 --end-id 102  # This will fail
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

#### Execution Mode Options
- `--execution-mode {concurrent,sequential,round_robin}` - Worker scheduling strategy (default: concurrent)
  - **concurrent**: All tiers can run workers simultaneously (respects per-tier max limits)
  - **sequential**: Process tiers one at a time in order LARGE → MEDIUM → SMALL
  - **round_robin**: Round-robin allocation across tiers with global worker limit
- `--max-concurrent-workers <int>` - Maximum total concurrent workers across all tiers (required for round_robin mode)

#### Output Options
- `--output-name <string>` - Base name for output files (default: "simulation_results")
- `--output-dir <path>` - Directory to store output files (default: output_files)
- `--no-csv` - Skip CSV data export for automated analysis
- `--detailed-page-size <int>` - Maximum number of workers per page in detailed visualization (default: 30, set to 0 to disable pagination)

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

**Large dataset with custom pagination:**
```bash
python run_multi_tier_simulation.py test_data/ \
    --detailed-page-size 50
```

**Disable pagination (single detailed file):**
```bash
python run_multi_tier_simulation.py test_data/ \
    --detailed-page-size 0
```

**Sequential execution (LARGE → MEDIUM → SMALL):**
```bash
python run_multi_tier_simulation.py test_data/ \
    --execution-mode sequential
```

**Round-robin execution with global worker limit:**
```bash
python run_multi_tier_simulation.py test_data/ \
    --execution-mode round_robin \
    --max-concurrent-workers 10
```

**Round-robin with custom configuration:**
```bash
python run_multi_tier_simulation.py test_data/ \
    --execution-mode round_robin \
    --max-concurrent-workers 15 \
    --straggler-threshold 25.0 \
    --output-name round_robin_analysis
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
   - **Pagination support**: For large datasets, automatically splits into multiple pages (e.g., `<output-name>_detailed_page1.html`, `<output-name>_detailed_page2.html`, etc.)
   - **Navigation**: Page navigation with Previous/Next buttons and page numbers
   - **Configurable page size**: Use `--detailed-page-size` to control workers per page (default: 30)

### Configuration File

**`config_<output-name>.txt`** - Simulation configuration record
- Complete configuration used for the simulation run
- Input directory and file count
- Worker tier settings (threads per worker, max concurrent workers)
- Analysis settings (straggler threshold, enabled features)
- Output settings and simulation results

## Performance Considerations

### Large Dataset Handling

For simulations with hundreds or thousands of workers, the detailed visualization can become very large and slow to load. The pagination feature automatically handles this:

- **Default behavior**: Splits detailed view into pages of 30 workers each
- **Customizable**: Use `--detailed-page-size` to adjust (e.g., 50 for larger pages, 10 for smaller pages)
- **Single file option**: Set `--detailed-page-size 0` to disable pagination and create one large file
- **Navigation**: Easy browsing between pages with intuitive navigation controls

**Recommendations:**
- For datasets with **< 50 workers**: Consider disabling pagination (`--detailed-page-size 0`)
- For datasets with **50-200 workers**: Use default pagination (30 workers per page)
- For datasets with **> 200 workers**: Consider smaller page sizes (10-20 workers per page)
- **Timeline visualization** is not paginated and remains fast even with many workers

### Processing Order Requirements

**Critical Design Requirement**: This simulation accurately models a real production system that processes data in a specific order by design. The simulation enforces the exact same ordering requirements:

#### 1. Subset Processing Order
- **Requirement**: All subsets must be processed in **ascending numerical order** of their subset ID
- **Implementation**: Files are automatically sorted numerically by subset ID within each tier
- **Result**: Workers process subsets in order: W35, W50, W57, W566, W708, etc. (not filesystem order)

#### 2. SSTable Processing Order  
- **Requirement**: SSTables within each subset must be processed in the **exact order** they appear in the subset definition file
- **Implementation**: SSTables are read sequentially from the file and processed in first-in-first-out (FIFO) order
- **Result**: If a subset file lists SSTable IDs as `1001, 1003, 1002, 1005`, they are processed in exactly that order

#### Why This Matters
- **Accuracy**: Simulation results match real system behavior
- **Consistency**: Reproducible results across multiple runs
- **Validation**: Performance analysis reflects actual production constraints
- **Debugging**: Issues in simulation correspond to real system bottlenecks

## Execution Modes

The simulation supports three different worker scheduling strategies:

### 1. Concurrent Mode (Default)
**Behavior**: All tiers can run workers simultaneously, respecting individual tier limits.

**Worker Allocation**: 
- SMALL workers: Up to `--small-max-workers` (default: 4)
- MEDIUM workers: Up to `--medium-max-workers` (default: 6) 
- LARGE workers: Up to `--large-max-workers` (default: 10)
- **Total possible concurrent workers**: Up to 20 (4+6+10)

**Use Case**: Maximum parallelism when you have sufficient resources for all tiers simultaneously.

### 2. Sequential Mode
**Behavior**: Process one tier at a time in order: LARGE → MEDIUM → SMALL.

**Worker Allocation**:
- LARGE tier: Process all LARGE files first using up to `--large-max-workers`
- MEDIUM tier: After all LARGE files complete, process MEDIUM files using up to `--medium-max-workers`
- SMALL tier: After all MEDIUM files complete, process SMALL files using up to `--small-max-workers`

**Use Case**: Resource-constrained environments or when tier dependencies exist.

### 3. Round-Robin Mode  
**Behavior**: Allocate workers in round-robin across tiers with a global worker limit.

**Worker Allocation**:
- **Global limit**: Never exceed `--max-concurrent-workers` total workers across all tiers
- **Round-robin pattern**: LARGE → MEDIUM → SMALL → LARGE → MEDIUM → SMALL...
- **Sequential within tiers**: Subsets within each tier processed in numerical order
- **Adaptive**: Automatically skips empty tiers and continues with available tiers

**Examples**:
```
# With max-concurrent-workers=5 and files in all tiers:
Worker 1: LARGE subset-0
Worker 2: MEDIUM subset-0  
Worker 3: SMALL subset-0
Worker 4: LARGE subset-1
Worker 5: MEDIUM subset-1
# When LARGE subset-0 completes: → SMALL subset-1
```

**Edge Case Handling**:
- **Missing tiers**: If no LARGE files exist, alternates between MEDIUM and SMALL
- **Tier exhaustion**: When LARGE files finish, continues round-robin with MEDIUM ↔ SMALL
- **Single tier**: If only MEDIUM files exist, processes them sequentially up to the global limit

**Use Case**: Balanced resource utilization across tiers while maintaining global resource limits.

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
