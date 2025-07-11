from core.simulation import run_single_thread_simulation, run_multi_thread_simulation, SingleThreadSimulator, MultiThreadSimulator, WorkItem
from dataclasses import dataclass
from typing import List, Optional, Union, Dict, Any
from heapq import heappush, heappop
import sys

class SimulationError(Exception):
    """Custom exception for simulation errors"""
    pass

@dataclass
class SimpleConfig:
    max_concurrent_workers: int
    threads_per_worker: int = 1
    
    def __post_init__(self):
        """Validate configuration parameters after initialization."""
        if self.max_concurrent_workers <= 0:
            raise ValueError(f"max_concurrent_workers must be positive, got {self.max_concurrent_workers}")
        if self.threads_per_worker <= 0:
            raise ValueError(f"threads_per_worker must be positive, got {self.threads_per_worker}")

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
        self.simulator: Optional[Union[SingleThreadSimulator, MultiThreadSimulator]] = None
    
    def process_file(self, file, processing_time_unit: float = 1.0, threads_per_worker: int = 1):
        """Process a subset file with this worker."""
        self.file = file
        
        if file.num_sstables == 0:
            self.completion_time = self.start_time
            # Create empty simulator for consistency
            if threads_per_worker == 1:
                self.simulator = SingleThreadSimulator(self.worker_id)
            else:
                self.simulator = MultiThreadSimulator(self.worker_id, threads_per_worker)
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
            if threads_per_worker == 1:
                self.simulator = run_single_thread_simulation(
                    items, self.worker_id, self.start_time, processing_time_unit
                )
            else:
                self.simulator = run_multi_thread_simulation(
                    items, self.worker_id, threads_per_worker, self.start_time, processing_time_unit
                )
            self.completion_time = self.simulator.available_time
            return self.completion_time
        except Exception as e:
            raise SimulationError(f"Error processing file {file.full_path} in worker {self.worker_id}: {str(e)}") from e

    def get_total_sstable_size(self) -> int:
        """Calculate the total size of all SSTables processed by this worker.
        
        Returns:
            Total size in bytes of all SSTables processed by this worker.
            Returns 0 if no simulator or no work was processed.
        """
        if not self.simulator:
            return 0
        
        total_size = 0
        for item in self.simulator.processed_items:
            total_size += item.size
        
        return total_size

    def get_cpu_efficiency_metrics(self, threads_per_worker: int) -> Dict[str, float]:
        """Calculate CPU efficiency metrics for this worker.
        
        Args:
            threads_per_worker: Number of threads allocated to this worker
        
        Returns:
            Dictionary containing:
            - total_used_cpu_time: worker_duration * threads_per_worker (all CPUs allocated)
            - total_active_cpu_time: sum of actual thread processing times
            - cpu_inefficiency: difference between used and active (idle CPU time)
            - cpu_efficiency_percent: percentage of CPU time actually used for work
        """
        if not self.simulator:
            return {
                'total_used_cpu_time': 0.0,
                'total_active_cpu_time': 0.0,
                'cpu_inefficiency': 0.0,
                'cpu_efficiency_percent': 0.0
            }
        
        # Calculate worker duration (completion_time - start_time)
        worker_duration = (self.completion_time or self.start_time) - self.start_time
        
        # Total Used CPU Time: All threads allocated for the entire worker duration
        total_used_cpu_time = worker_duration * threads_per_worker
        
        # Total Active CPU Time: Sum of actual processing time across all threads
        if hasattr(self.simulator, 'threads'):  # MultiThreadSimulator
            total_active_cpu_time = sum(thread.total_processing_time for thread in self.simulator.threads)
        else:  # SingleThreadSimulator
            total_active_cpu_time = self.simulator.total_processing_time
        
        # CPU Inefficiency: Idle/wasted CPU time
        cpu_inefficiency = total_used_cpu_time - total_active_cpu_time
        
        # CPU Efficiency Percentage: How much of allocated CPU time was actually used
        cpu_efficiency_percent = (total_active_cpu_time / total_used_cpu_time * 100) if total_used_cpu_time > 0 else 0.0
        
        return {
            'total_used_cpu_time': total_used_cpu_time,
            'total_active_cpu_time': total_active_cpu_time,
            'cpu_inefficiency': cpu_inefficiency,
            'cpu_efficiency_percent': cpu_efficiency_percent
        }

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
            completion_time = worker.process_file(file, threads_per_worker=self.config.threads_per_worker)
            completion_event = WorkerCompletionEvent(completion_time, worker)
            heappush(self.completion_events, completion_event)
            
            threads_info = f" with {self.config.threads_per_worker} threads" if self.config.threads_per_worker > 1 else ""
            print(f"Started worker {worker.worker_id} for subset {file.subset_id}{threads_info} at time {self.current_time:.2f}")
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
        print(f"Threads per worker: {self.config.threads_per_worker}")
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
            # Header row - including CPU efficiency metrics to match tiered format
            writer.writerow([
                'Worker_ID', 'Tier', 'Start_Time', 'End_Time', 'Duration', 
                'SSTable_Count', 'Data_Size_GB', 'Is_Straggler_Worker',
                'Num_Threads', 'Total_Used_CPU_Time', 'Total_Active_CPU_Time',
                'CPU_Inefficiency', 'CPU_Efficiency_Percent'
            ])
            
            # Worker data rows
            for worker in self.completed_workers:
                duration = (worker.completion_time or worker.start_time) - worker.start_time
                sstable_count = len(worker.simulator.processed_items) if worker.simulator else 0
                # Use calculated SSTable size instead of file metadata size
                total_sstable_size = worker.get_total_sstable_size()
                data_size_gb = total_sstable_size / (1024*1024*1024)
                
                # Get CPU efficiency metrics
                efficiency_metrics = worker.get_cpu_efficiency_metrics(self.config.threads_per_worker)
                
                writer.writerow([
                    worker.worker_id,
                    'UNIVERSAL',  # Simple strategy uses universal tier
                    f"{worker.start_time:.2f}",
                    f"{worker.completion_time or worker.start_time:.2f}",
                    f"{duration:.2f}",
                    sstable_count,
                    f"{data_size_gb:.2f}",
                    False,  # Simple simulation doesn't track stragglers
                    self.config.threads_per_worker,
                    f"{efficiency_metrics['total_used_cpu_time']:.2f}",
                    f"{efficiency_metrics['total_active_cpu_time']:.2f}",
                    f"{efficiency_metrics['cpu_inefficiency']:.2f}",
                    f"{efficiency_metrics['cpu_efficiency_percent']:.1f}"
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
            writer.writerow(['Threads_Per_Worker', self.config.threads_per_worker])
            writer.writerow(['Total_CPUs', len(self.completed_workers) * self.config.threads_per_worker])  # threads per worker * workers
            
            # Calculate total CPU time from actual worker durations and thread count
            total_cpu_time = sum(
                ((w.completion_time or w.start_time) - w.start_time) * self.config.threads_per_worker
                for w in self.completed_workers
            )
            writer.writerow(['Total_CPU_Time', f"{total_cpu_time:.2f}"])
        
        print(f"\nData exported to CSV files:")
        print(f"- Worker data: {worker_file}")
        print(f"- Summary statistics: {summary_file}")
    
    def get_execution_report_data(self) -> Dict[str, Any]:
        """
        Get comprehensive execution report data for this simulation.
        
        Returns:
            Dictionary containing all metrics needed for the execution report
        """
        if not self.simulation_completed:
            raise SimulationError("Simulation must be completed before generating execution report")
        
        # Calculate total migration size from all workers
        total_migration_size_bytes = sum(worker.get_total_sstable_size() for worker in self.completed_workers)
        total_migration_size_gb = total_migration_size_bytes / (1024 * 1024 * 1024)
        
        # Calculate aggregated CPU efficiency metrics
        total_used_cpu_time = 0.0
        total_active_cpu_time = 0.0
        total_cpu_inefficiency = 0.0
        efficiency_percentages = []
        
        for worker in self.completed_workers:
            efficiency_metrics = worker.get_cpu_efficiency_metrics(self.config.threads_per_worker)
            total_used_cpu_time += efficiency_metrics['total_used_cpu_time']
            total_active_cpu_time += efficiency_metrics['total_active_cpu_time']
            total_cpu_inefficiency += efficiency_metrics['cpu_inefficiency']
            if efficiency_metrics['cpu_efficiency_percent'] > 0:
                efficiency_percentages.append(efficiency_metrics['cpu_efficiency_percent'])
        
        # Calculate average CPU efficiency
        average_cpu_efficiency_percent = sum(efficiency_percentages) / len(efficiency_percentages) if efficiency_percentages else 0.0
        
        report_data = {
            "total_execution_time": self.current_time,
            "total_migration_size_bytes": total_migration_size_bytes,
            "total_migration_size_gb": total_migration_size_gb,
            "simulation_config": {
                "strategy": "simple",
                "max_concurrent_workers": self.config.max_concurrent_workers,
                "threads_per_worker": self.config.threads_per_worker,
                "total_workers": len(self.completed_workers),
                "total_cpus": len(self.completed_workers) * self.config.threads_per_worker  # threads per worker * workers
            },
            "worker_summary": {
                "total_workers": len(self.completed_workers),
                "total_sstables": sum(len(worker.simulator.processed_items) for worker in self.completed_workers if worker.simulator),
                "total_cpu_time": total_used_cpu_time
            },
            # Add CPU efficiency metrics to match tiered simulation
            "cpu_efficiency": {
                "total_used_cpu_time": total_used_cpu_time,
                "total_active_cpu_time": total_active_cpu_time,
                "cpu_inefficiency": total_cpu_inefficiency,
                "average_cpu_efficiency_percent": average_cpu_efficiency_percent
            }
        }
        
        return report_data
    
    def export_execution_report_data(self, output_file: str):
        """Export execution report data as JSON for helper script consumption."""
        import json
        
        report_data = self.get_execution_report_data()
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(report_data, f, indent=2)
        
        print(f"Execution report data exported to {output_file}")
    
    def get_all_simulators(self) -> List[Union[SingleThreadSimulator, MultiThreadSimulator]]:
        """Get all worker simulators for detailed analysis or visualization."""
        simulators = []
        for worker in self.completed_workers:
            if worker.simulator:
                simulators.append(worker.simulator)
        return simulators 