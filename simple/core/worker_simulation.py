from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
from heapq import heappush, heappop
from .simulation import WorkItem, run_single_thread_simulation, SingleThreadSimulator
import sys

class SimulationError(Exception):
    """Custom exception for simulation errors"""
    pass

@dataclass
class SimpleConfig:
    max_concurrent_workers: int
    
    def __post_init__(self):
        """Validate configuration parameters after initialization."""
        if self.max_concurrent_workers <= 0:
            raise ValueError(f"max_concurrent_workers must be positive, got {self.max_concurrent_workers}")

class SimpleWorker:
    """Represents a single worker processing one subset sequentially."""
    
    def __init__(self, subset_id: str, start_time: float):
        # Handle both direct numeric IDs and "subset-X" format
        try:
            if subset_id.startswith("subset-"):
                self.worker_id = int(subset_id.split('-')[1])
            else:
                self.worker_id = int(subset_id)
        except (IndexError, ValueError) as e:
            raise SimulationError(f"Invalid subset ID format: {subset_id}. Expected either a number or 'subset-X' format.") from e
        
        self.subset_id = subset_id
        self.start_time = start_time
        self.file = None  # Will store FileMetadata
        self.completion_time: Optional[float] = None
        self.simulator: Optional[SingleThreadSimulator] = None
    
    def process_file(self, file, processing_time_unit: float = 1.0):
        """Process a subset file with this worker."""
        self.file = file
        
        if file.num_sstables == 0:
            self.completion_time = self.start_time
            # Create empty simulator for consistency
            self.simulator = SingleThreadSimulator(self.worker_id)
            self.simulator.available_time = self.start_time
            return self.completion_time
            
        # Read actual SSTable definitions from the subset file
        try:
            items = file.get_sstables()  # Get actual SSTable definitions from file
        except Exception as e:
            raise SimulationError(f"Failed to read SSTable definitions from {file.full_path}: {str(e)}") from e
        
        if not items:
            # Fallback: if no SSTable data in file, treat as single work item with total size
            items = [WorkItem(f"SST0", file.data_size)]
        
        try:
            self.simulator = run_single_thread_simulation(
                items, self.worker_id, self.start_time, processing_time_unit
            )
            self.completion_time = self.simulator.available_time
            return self.completion_time
        except Exception as e:
            raise SimulationError(f"Error processing file {file.full_path} in worker {self.worker_id}: {str(e)}") from e

class WorkerCompletionEvent:
    """Event representing a worker completing its work."""
    
    def __init__(self, completion_time: float, worker: SimpleWorker):
        self.completion_time = completion_time
        self.worker = worker
    
    def __lt__(self, other):
        # For heap ordering - earlier completion times have higher priority
        if self.completion_time != other.completion_time:
            return self.completion_time < other.completion_time
        # Break ties with worker ID for deterministic ordering
        return self.worker.worker_id < other.worker.worker_id

class SimpleSimulation:
    """Manages the overall simple simulation with configurable worker concurrency."""
    
    def __init__(self, config: SimpleConfig):
        self.config = config
        self.current_time = 0.0
        self.active_workers: List[SimpleWorker] = []
        self.completed_workers: List[SimpleWorker] = []
        self.completion_events: List[WorkerCompletionEvent] = []  # Min-heap
        self.simulation_completed = False
        
    def can_add_worker(self) -> bool:
        """Check if we can add another worker without exceeding max_concurrent_workers limit."""
        return len(self.active_workers) < self.config.max_concurrent_workers
    
    def add_worker(self, file) -> SimpleWorker:
        """Add a new worker to process the given file."""
        if not self.can_add_worker():
            raise SimulationError(f"Cannot add worker: already at max capacity ({self.config.max_concurrent_workers})")
        
        worker = SimpleWorker(file.subset_id, self.current_time)
        self.active_workers.append(worker)
        
        # Process the file and schedule completion event
        try:
            completion_time = worker.process_file(file)
            completion_event = WorkerCompletionEvent(completion_time, worker)
            heappush(self.completion_events, completion_event)
            
            print(f"Started worker {worker.worker_id} for subset {file.subset_id} at time {self.current_time:.2f}")
            return worker
            
        except Exception as e:
            # Remove worker from active list if processing failed
            self.active_workers.remove(worker)
            raise e
    
    def remove_worker(self, worker: SimpleWorker):
        """Remove a worker from active list and add to completed list."""
        if worker in self.active_workers:
            self.active_workers.remove(worker)
        self.completed_workers.append(worker)
    
    def run_simulation(self, files: List) -> float:
        """
        Run the simple simulation on the given files.
        
        Args:
            files: List of FileMetadata objects to process
            
        Returns:
            Total simulation time
        """
        if not files:
            print("No files to process")
            return 0.0
        
        print(f"\nStarting Simple Simulation with max {self.config.max_concurrent_workers} concurrent workers")
        print(f"Processing {len(files)} subset files...")
        
        # Sort files by subset_id for deterministic processing order
        sorted_files = sorted(files, key=lambda f: int(f.subset_id) if f.subset_id.isdigit() else float('inf'))
        
        # Track remaining files to process
        remaining_files = sorted_files.copy()
        
        # Initial worker spawning - fill up to max capacity
        while remaining_files and self.can_add_worker():
            file = remaining_files.pop(0)
            try:
                self.add_worker(file)
            except Exception as e:
                print(f"Error starting worker for {file.subset_id}: {e}", file=sys.stderr)
                continue
        
        # Main simulation loop - process completion events
        while self.completion_events or remaining_files:
            if not self.completion_events:
                # No active workers but still have files - this shouldn't happen
                print("Warning: No active workers but files remaining")
                break
            
            # Get next completion event
            completion_event = heappop(self.completion_events)
            self.current_time = completion_event.completion_time
            completed_worker = completion_event.worker
            
            print(f"Worker {completed_worker.worker_id} completed at time {self.current_time:.2f}")
            
            # Move worker from active to completed
            self.remove_worker(completed_worker)
            
            # If there are more files to process, start a new worker
            if remaining_files:
                file = remaining_files.pop(0)
                try:
                    self.add_worker(file)
                except Exception as e:
                    print(f"Error starting worker for {file.subset_id}: {e}", file=sys.stderr)
                    continue
        
        self.simulation_completed = True
        print(f"\nSimulation completed at time {self.current_time:.2f}")
        return self.current_time
    
    def print_results(self):
        """Print detailed simulation results."""
        if not self.simulation_completed:
            print("Simulation has not completed yet")
            return
        
        print("\n" + "="*60)
        print("SIMPLE SIMULATION RESULTS")
        print("="*60)
        
        print(f"\nConfiguration:")
        print(f"Max concurrent workers: {self.config.max_concurrent_workers}")
        print(f"Total simulation time: {self.current_time:.2f} time units")
        print(f"Workers processed: {len(self.completed_workers)}")
        
        if self.completed_workers:
            # Sort workers by completion time for reporting
            sorted_workers = sorted(self.completed_workers, key=lambda w: w.completion_time or 0)
            
            print(f"\nWorker Summary:")
            print("-" * 40)
            for worker in sorted_workers:
                if worker.simulator and worker.file:
                    total_work = sum(item.size for item in worker.simulator.processed_items)
                    print(f"Worker {worker.worker_id:3d}: {len(worker.simulator.processed_items):3d} items, "
                          f"{total_work:10,} total size, completed at {worker.completion_time:.2f}")
                else:
                    print(f"Worker {worker.worker_id:3d}: No work processed")
            
            # Calculate some basic statistics
            completion_times = [w.completion_time for w in self.completed_workers if w.completion_time]
            if completion_times:
                avg_completion = sum(completion_times) / len(completion_times)
                print(f"\nStatistics:")
                print(f"Average worker completion time: {avg_completion:.2f}")
                print(f"Earliest completion: {min(completion_times):.2f}")
                print(f"Latest completion: {max(completion_times):.2f}")
        
        print("\n" + "="*60)
    
    def export_data_to_csv(self, base_filename: str = "simple_simulation_data"):
        """Export simulation data to CSV files for automated analysis."""
        import csv
        
        # Export worker-level data (matching tiered format)
        worker_file = f"{base_filename}_workers.csv"
        with open(worker_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            # Header row - matching tiered format
            writer.writerow([
                'Worker_ID', 'Tier', 'Start_Time', 'End_Time', 'Duration', 
                'SSTable_Count', 'Data_Size_GB', 'Is_Straggler_Worker'
            ])
            
            # Worker data rows
            for worker in self.completed_workers:
                duration = (worker.completion_time or worker.start_time) - worker.start_time
                sstable_count = len(worker.simulator.processed_items) if worker.simulator else 0
                data_size_gb = worker.file.data_size / (1024*1024*1024) if worker.file else 0.0
                
                writer.writerow([
                    worker.worker_id,
                    'UNIVERSAL',  # Simple strategy uses universal tier
                    f"{worker.start_time:.2f}",
                    f"{worker.completion_time or worker.start_time:.2f}",
                    f"{duration:.2f}",
                    sstable_count,
                    f"{data_size_gb:.2f}",
                    False  # Simple simulation doesn't track stragglers
                ])
        
        # Export summary statistics
        summary_file = f"{base_filename}_summary.csv"
        with open(summary_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            
            # Overall simulation metrics
            writer.writerow(['Metric', 'Value'])
            writer.writerow(['Total_Simulation_Time', f"{self.current_time:.2f}"])
            writer.writerow(['Total_Workers', len(self.completed_workers)])
            writer.writerow(['Strategy', 'SIMPLE'])
            writer.writerow(['Threads_Per_Worker', 1])  # Simple strategy uses 1 thread per worker
            writer.writerow(['Total_CPUs', len(self.completed_workers)])  # 1 CPU per worker
            
            # Calculate total CPU time from actual worker durations
            total_cpu_time = sum(
                ((w.completion_time or w.start_time) - w.start_time) 
                for w in self.completed_workers
            )
            writer.writerow(['Total_CPU_Time', f"{total_cpu_time:.2f}"])
        
        print(f"\nData exported to CSV files:")
        print(f"- Worker data: {worker_file}")
        print(f"- Summary statistics: {summary_file}")
    
    def get_all_simulators(self) -> List[SingleThreadSimulator]:
        """Get all worker simulators for detailed analysis or visualization."""
        simulators = []
        for worker in self.completed_workers:
            if worker.simulator:
                simulators.append(worker.simulator)
        return simulators 