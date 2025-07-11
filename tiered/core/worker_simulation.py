from typing import List, Dict, Optional, Tuple, Any
from dataclasses import dataclass
from heapq import heappush, heappop
from .file_processor import FileMetadata, parse_input_files
from visualization.visualization_base import WorkerTier
from .simulation import WorkItem, run_simulation
from visualization.timeline_visualization import save_timeline_visualization
from visualization.detailed_visualization import save_detailed_visualization
import math
import sys
import random
from enum import Enum

class SimulationError(Exception):
    """Custom exception for simulation errors"""
    pass

class ExecutionMode(Enum):
    """Enum for different execution modes"""
    CONCURRENT = "concurrent"
    SEQUENTIAL = "sequential"
    ROUND_ROBIN = "round_robin"

@dataclass
class TierConfig:
    num_threads: int
    max_workers: int

    def __post_init__(self):
        """Validate configuration parameters after initialization."""
        if self.num_threads <= 0:
            raise ValueError(f"num_threads must be positive, got {self.num_threads}")
        if self.max_workers <= 0:
            raise ValueError(f"max_workers must be positive, got {self.max_workers}")

@dataclass
class WorkerConfig:
    small: TierConfig
    medium: TierConfig
    large: TierConfig

class Worker:
    def __init__(self, subset_id: str, tier: WorkerTier, num_threads: int, start_time: float):
        # Handle both direct numeric IDs and "subset-X" format
        try:
            if subset_id.startswith("subset-"):
                self.worker_id = int(subset_id.split('-')[1])
            else:
                self.worker_id = int(subset_id)
        except (IndexError, ValueError) as e:
            raise SimulationError(f"Invalid subset ID format: {subset_id}. Expected either a number or 'subset-X' format.") from e
        self.tier = tier
        self.num_threads = num_threads
        self.start_time = start_time
        self.file: Optional[FileMetadata] = None
        self.completion_time: Optional[float] = None
        self.threads = None  # Will store the thread simulation results
        self.straggler_threads: List[int] = []  # List of thread IDs that are stragglers
        self.is_straggler_worker: bool = False  # True if this worker contains any straggler threads
    
    def process_file(self, file: FileMetadata, processing_time_unit: float = 1.0):
        self.file = file
        
        if file.num_sstables == 0:
            self.completion_time = self.start_time
            return self.completion_time
            
        # Read actual SSTable definitions from the subset file
        # This should NOT modify the actual SSTable IDs or sizes in any way
        try:
            items = file.get_sstables()  # Get actual SSTable definitions from file
        except Exception as e:
            raise SimulationError(f"Failed to read SSTable definitions from {file.full_path}: {str(e)}") from e
        
        if not items:
            # Fallback: if no SSTable data in file, treat as single work item with total size
            # This maintains backward compatibility with test files
            items = [WorkItem(f"SST0", file.data_size)]
        
        try:
            self.threads = run_simulation(items, self.num_threads, processing_time_unit)
            if not self.threads:  # Check if thread simulation returned empty results
                raise SimulationError("Thread simulation returned no results")
            self.completion_time = self.start_time + max(thread.available_time for thread in self.threads)
            return self.completion_time
        except Exception as e:
            raise SimulationError(f"Error processing file {file.full_path} in worker {self.worker_id}: {str(e)}") from e
    
    def identify_stragglers(self, straggler_threshold_percent: float = 20.0):
        """
        Identify straggler threads within this worker.
        
        Note: Straggler analysis only applies to workers with 2 or more threads.
        Single-thread workers cannot have stragglers by definition.
        
        Args:
            straggler_threshold_percent: Percentage threshold above average to be considered a straggler
        """
        if not self.threads or len(self.threads) <= 1:
            # Single-thread or no-thread workers cannot have stragglers
            self.straggler_threads = []
            self.is_straggler_worker = False
            return
        
        # Calculate completion times for each thread
        thread_completion_times = [thread.available_time for thread in self.threads]
        
        # Filter out idle threads (threads with very low completion times)
        # Use median as a more robust measure, then filter out threads significantly below it
        sorted_times = sorted(thread_completion_times)
        median_time = sorted_times[len(sorted_times) // 2]
        
        # Only include threads that did meaningful work (at least 10% of median)
        # This filters out truly idle threads while keeping threads that did some work
        meaningful_threshold = max(median_time * 0.1, 1.0)  # At least 10% of median or 1 time unit
        working_threads = [thread for thread in self.threads 
                          if thread.available_time >= meaningful_threshold]
        
        # Need at least 2 working threads to identify stragglers
        if len(working_threads) < 2:
            self.straggler_threads = []
            self.is_straggler_worker = False
            return
        
        # Calculate average completion time only for working threads
        working_completion_times = [thread.available_time for thread in working_threads]
        avg_completion_time = sum(working_completion_times) / len(working_completion_times)
        
        # Calculate threshold: average + X% of average
        threshold = avg_completion_time * (1 + straggler_threshold_percent / 100.0)
        
        # Identify straggler threads among working threads
        self.straggler_threads = []
        for thread in working_threads:
            if thread.available_time > threshold:
                self.straggler_threads.append(thread.thread_id)
        
        # Mark this worker as a straggler worker if it has any straggler threads
        self.is_straggler_worker = len(self.straggler_threads) > 0
    
    def get_straggler_info(self) -> Dict[str, any]:
        """
        Get detailed information about stragglers in this worker.
        
        Returns:
            Dictionary containing straggler analysis information
        """
        if not self.threads:
            return {"has_stragglers": False, "analysis_applicable": False, "reason": "No threads"}
        
        if len(self.threads) <= 1:
            return {
                "has_stragglers": False, 
                "analysis_applicable": False, 
                "reason": "Single-thread worker - straggler analysis not applicable",
                "total_threads": len(self.threads)
            }
        
        thread_completion_times = [thread.available_time for thread in self.threads]
        avg_completion_time = sum(thread_completion_times) / len(thread_completion_times)
        max_completion_time = max(thread_completion_times)
        min_completion_time = min(thread_completion_times)
        
        straggler_details = []
        for thread_id in self.straggler_threads:
            thread = next(t for t in self.threads if t.thread_id == thread_id)
            delay_percent = ((thread.available_time - avg_completion_time) / avg_completion_time) * 100
            straggler_details.append({
                "thread_id": thread_id,
                "completion_time": thread.available_time,
                "delay_percent": delay_percent
            })
        
        return {
            "has_stragglers": self.is_straggler_worker,
            "analysis_applicable": True,
            "num_straggler_threads": len(self.straggler_threads),
            "total_threads": len(self.threads),
            "avg_completion_time": avg_completion_time,
            "max_completion_time": max_completion_time,
            "min_completion_time": min_completion_time,
            "completion_time_spread": max_completion_time - min_completion_time,
            "straggler_details": straggler_details
        }

    def get_cpu_efficiency_metrics(self) -> Dict[str, float]:
        """Calculate CPU efficiency metrics for this worker.
        
        Returns:
            Dictionary containing:
            - total_used_cpu_time: worker_duration * num_threads (all CPUs allocated)
            - total_active_cpu_time: sum of actual thread processing times
            - cpu_inefficiency: difference between used and active (idle CPU time)
            - cpu_efficiency_percent: percentage of CPU time actually used for work
        """
        if not self.threads:
            return {
                'total_used_cpu_time': 0.0,
                'total_active_cpu_time': 0.0,
                'cpu_inefficiency': 0.0,
                'cpu_efficiency_percent': 0.0
            }
        
        # Calculate worker duration (completion_time - start_time)
        worker_duration = self.completion_time - self.start_time
        
        # Total Used CPU Time: All threads allocated for the entire worker duration
        total_used_cpu_time = worker_duration * self.num_threads
        
        # Total Active CPU Time: Sum of actual processing time across all threads
        total_active_cpu_time = sum(thread.total_processing_time for thread in self.threads)
        
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

    def get_total_sstable_size(self) -> int:
        """Calculate the total size of all SSTables processed by this worker.
        
        Returns:
            Total size in bytes of all SSTables processed by this worker's threads.
            Returns 0 if no threads or no work was processed.
        """
        if not self.threads:
            return 0
        
        total_size = 0
        for thread in self.threads:
            for item in thread.processed_items:
                total_size += item.size
        
        return total_size

class MultiTierSimulation:
    def __init__(self, config: WorkerConfig, straggler_threshold_percent: float = 20.0, 
                 execution_mode: ExecutionMode = ExecutionMode.CONCURRENT, 
                 max_concurrent_workers: int = None):
        self.config = config
        self.straggler_threshold_percent = straggler_threshold_percent
        self.execution_mode = execution_mode
        self.max_concurrent_workers = max_concurrent_workers  # For round-robin mode
        self.current_time = 0.0
        self.active_workers: Dict[WorkerTier, List[Worker]] = {
            WorkerTier.SMALL: [],
            WorkerTier.MEDIUM: [],
            WorkerTier.LARGE: []
        }
        self.completed_workers: List[Worker] = []
        # Store tuples of (completion_time, counter, worker) to ensure stable sorting
        self.completion_events: List[Tuple[float, int, Worker]] = []
        self.event_counter = 0  # Unique counter for heap stability
        self.simulation_completed = False
        
        # Round-robin specific state
        self.round_robin_position = 0  # Current position in round-robin cycle
        self.tier_order = [WorkerTier.LARGE, WorkerTier.MEDIUM, WorkerTier.SMALL]  # Round-robin order
        self.tier_next_index: Dict[WorkerTier, int] = {  # Next subset index to process for each tier
            WorkerTier.SMALL: 0,
            WorkerTier.MEDIUM: 0,
            WorkerTier.LARGE: 0
        }
        
    def can_add_worker(self, tier: WorkerTier) -> bool:
        if self.execution_mode == ExecutionMode.ROUND_ROBIN:
            # For round-robin, check global limit
            total_active_workers = sum(len(workers) for workers in self.active_workers.values())
            return total_active_workers < self.max_concurrent_workers
        else:
            # For concurrent/sequential, check per-tier limits
            max_workers = {
                WorkerTier.SMALL: self.config.small.max_workers,
                WorkerTier.MEDIUM: self.config.medium.max_workers,
                WorkerTier.LARGE: self.config.large.max_workers
            }
            return len(self.active_workers[tier]) < max_workers[tier]
    
    def get_num_threads(self, tier: WorkerTier) -> int:
        return {
            WorkerTier.SMALL: self.config.small.num_threads,
            WorkerTier.MEDIUM: self.config.medium.num_threads,
            WorkerTier.LARGE: self.config.large.num_threads
        }[tier]
    
    def add_worker(self, tier: WorkerTier, file: FileMetadata) -> Worker:
        worker = Worker(file.subset_id, tier, self.get_num_threads(tier), self.current_time)
        try:
            completion_time = worker.process_file(file)
            self.active_workers[tier].append(worker)
            # Include counter in the heap tuple to ensure stable sorting
            heappush(self.completion_events, (completion_time, self.event_counter, worker))
            self.event_counter += 1
            return worker
        except SimulationError as e:
            raise SimulationError(f"Error adding worker for tier {tier.value}: {str(e)}")
    
    def remove_worker(self, worker: Worker):
        self.active_workers[worker.tier].remove(worker)
        # Identify stragglers before adding to completed workers
        worker.identify_stragglers(self.straggler_threshold_percent)
        self.completed_workers.append(worker)
    
    def run_simulation(self, files: List[FileMetadata]) -> float:
        if not files:
            raise SimulationError("No files provided for simulation")
        
        if self.execution_mode == ExecutionMode.CONCURRENT:
            return self._run_concurrent_simulation(files)
        elif self.execution_mode == ExecutionMode.SEQUENTIAL:
            return self._run_sequential_simulation(files)
        elif self.execution_mode == ExecutionMode.ROUND_ROBIN:
            return self._run_round_robin_simulation(files)
        else:
            raise SimulationError(f"Unknown execution mode: {self.execution_mode}")
    
    def _run_concurrent_simulation(self, files: List[FileMetadata]) -> float:
        """Original parallel execution mode - all tiers can run simultaneously."""
        # Group files by tier
        files_by_tier: Dict[WorkerTier, List[FileMetadata]] = {
            WorkerTier.SMALL: [],
            WorkerTier.MEDIUM: [],
            WorkerTier.LARGE: []
        }
        for file in files:
            files_by_tier[file.tier].append(file)
        
        # Track failed files to report at the end
        failed_files = []
        
        # Initial assignment of files to workers
        for tier in WorkerTier:
            while files_by_tier[tier] and self.can_add_worker(tier):
                try:
                    file = files_by_tier[tier].pop(0)
                    self.add_worker(tier, file)
                except SimulationError as e:
                    print(f"Warning: Failed to process file {file.full_path}: {str(e)}", file=sys.stderr)
                    failed_files.append((file, str(e)))
                    continue
        
        if not self.completion_events:
            failed_files_str = "\n".join(f"- {f[0].full_path}: {f[1]}" for f in failed_files)
            raise SimulationError(
                f"Failed to start any workers. All initial file assignments failed:\n{failed_files_str}"
            )
        
        # Process events until all files are processed
        while self.completion_events:
            completion_time, _, completed_worker = heappop(self.completion_events)
            self.current_time = completion_time
            self.remove_worker(completed_worker)
            
            # Try to assign new file to the same tier
            if files_by_tier[completed_worker.tier]:
                try:
                    file = files_by_tier[completed_worker.tier].pop(0)
                    self.add_worker(completed_worker.tier, file)
                except SimulationError as e:
                    print(f"Warning: Failed to process file {file.full_path}: {str(e)}", file=sys.stderr)
                    failed_files.append((file, str(e)))
                    continue
        
        if failed_files:
            print("\nWarning: Some files failed to process:", file=sys.stderr)
            for file, error in failed_files:
                print(f"- {file.full_path}: {error}", file=sys.stderr)
        
        self.simulation_completed = True
        return self.current_time
    
    def _run_sequential_simulation(self, files: List[FileMetadata]) -> float:
        """Sequential execution mode - process one tier at a time: LARGE -> MEDIUM -> SMALL."""
        # Group files by tier
        files_by_tier: Dict[WorkerTier, List[FileMetadata]] = {
            WorkerTier.SMALL: [],
            WorkerTier.MEDIUM: [],
            WorkerTier.LARGE: []
        }
        for file in files:
            files_by_tier[file.tier].append(file)
        
        # Process tiers in order: LARGE -> MEDIUM -> SMALL
        tier_order = [WorkerTier.LARGE, WorkerTier.MEDIUM, WorkerTier.SMALL]
        
        print("\nSequential execution mode: Processing tiers in order LARGE -> MEDIUM -> SMALL")
        
        for tier in tier_order:
            tier_files = files_by_tier[tier]
            if not tier_files:
                print(f"No {tier.value} files to process, skipping tier.")
                continue
                
            print(f"\nProcessing {tier.value} tier: {len(tier_files)} files")
            tier_start_time = self.current_time
            
            # Process all files for this tier before moving to the next
            failed_files = []
            
            # Initial assignment of files to workers for this tier only
            while tier_files and self.can_add_worker(tier):
                try:
                    file = tier_files.pop(0)
                    self.add_worker(tier, file)
                except SimulationError as e:
                    print(f"Warning: Failed to process file {file.full_path}: {str(e)}", file=sys.stderr)
                    failed_files.append((file, str(e)))
                    continue
            
            # Process completion events until all files for this tier are done
            while self.completion_events and tier_files:
                completion_time, _, completed_worker = heappop(self.completion_events)
                self.current_time = completion_time
                
                # Only process workers from the current tier
                if completed_worker.tier == tier:
                    self.remove_worker(completed_worker)
                    
                    # Assign next file from the same tier if available
                    if tier_files:
                        try:
                            file = tier_files.pop(0)
                            self.add_worker(tier, file)
                        except SimulationError as e:
                            print(f"Warning: Failed to process file {file.full_path}: {str(e)}", file=sys.stderr)
                            failed_files.append((file, str(e)))
                            continue
                else:
                    # Re-add events from other tiers back to the queue (shouldn't happen in sequential mode)
                    heappush(self.completion_events, (completion_time, self.event_counter, completed_worker))
                    self.event_counter += 1
            
            # Wait for all remaining workers of this tier to complete
            remaining_events = []
            while self.completion_events:
                completion_time, counter, completed_worker = heappop(self.completion_events)
                if completed_worker.tier == tier:
                    self.current_time = completion_time
                    self.remove_worker(completed_worker)
                else:
                    # Keep events from other tiers for later (shouldn't happen in sequential mode)
                    remaining_events.append((completion_time, counter, completed_worker))
            
            # Restore any remaining events (shouldn't happen in sequential mode)
            for event in remaining_events:
                heappush(self.completion_events, event)
            
            if failed_files:
                print(f"\nWarning: Some {tier.value} files failed to process:", file=sys.stderr)
                for file, error in failed_files:
                    print(f"- {file.full_path}: {error}", file=sys.stderr)
            
            tier_duration = self.current_time - tier_start_time
            print(f"Completed {tier.value} tier in {tier_duration:.2f} time units")
        
        self.simulation_completed = True
        return self.current_time
    
    def _run_round_robin_simulation(self, files: List[FileMetadata]) -> float:
        """Round-robin execution mode - allocate workers in round-robin across tiers with global limit."""
        print(f"\nRound-robin execution mode: Max {self.max_concurrent_workers} concurrent workers across all tiers")
        
        # Group files by tier and sort within each tier by numeric index
        files_by_tier: Dict[WorkerTier, List[FileMetadata]] = {
            WorkerTier.SMALL: [],
            WorkerTier.MEDIUM: [],
            WorkerTier.LARGE: []
        }
        for file in files:
            files_by_tier[file.tier].append(file)
        
        # Sort files within each tier by numeric subset ID for sequential processing
        for tier in WorkerTier:
            files_by_tier[tier].sort(key=lambda f: int(f.subset_id) if f.subset_id.isdigit() else float('inf'))
        
        # Track failed files to report at the end
        failed_files = []
        
        # Initial allocation using round-robin
        while self._has_remaining_files(files_by_tier) and self._get_total_active_workers() < self.max_concurrent_workers:
            assigned = False
            for _ in range(len(self.tier_order)):  # Try each tier once per round
                current_tier = self.tier_order[self.round_robin_position]
                
                # Check if this tier has files to process
                if files_by_tier[current_tier]:
                    try:
                        file = files_by_tier[current_tier].pop(0)
                        self.add_worker(current_tier, file)
                        assigned = True
                        print(f"Round-robin: Assigned {current_tier.value} subset {file.subset_id} (total active: {self._get_total_active_workers()})")
                    except SimulationError as e:
                        print(f"Warning: Failed to process file {file.full_path}: {str(e)}", file=sys.stderr)
                        failed_files.append((file, str(e)))
                
                # Move to next tier in round-robin
                self.round_robin_position = (self.round_robin_position + 1) % len(self.tier_order)
                
                # If we're at capacity, break
                if self._get_total_active_workers() >= self.max_concurrent_workers:
                    break
            
            # If we couldn't assign any work in a full round, break to avoid infinite loop
            if not assigned:
                break
        
        # Process completion events and continue round-robin allocation
        while self.completion_events:
            completion_time, _, completed_worker = heappop(self.completion_events)
            self.current_time = completion_time
            self.remove_worker(completed_worker)
            
            # Continue round-robin allocation if there are remaining files
            if self._has_remaining_files(files_by_tier):
                # Try to assign new work using round-robin
                assigned = False
                attempts = 0
                while not assigned and attempts < len(self.tier_order) and self._get_total_active_workers() < self.max_concurrent_workers:
                    current_tier = self.tier_order[self.round_robin_position]
                    
                    if files_by_tier[current_tier]:
                        try:
                            file = files_by_tier[current_tier].pop(0)
                            self.add_worker(current_tier, file)
                            assigned = True
                            print(f"Round-robin: Assigned {current_tier.value} subset {file.subset_id} (total active: {self._get_total_active_workers()})")
                        except SimulationError as e:
                            print(f"Warning: Failed to process file {file.full_path}: {str(e)}", file=sys.stderr)
                            failed_files.append((file, str(e)))
                    
                    # Move to next tier in round-robin
                    self.round_robin_position = (self.round_robin_position + 1) % len(self.tier_order)
                    attempts += 1
        
        if failed_files:
            print("\nWarning: Some files failed to process:", file=sys.stderr)
            for file, error in failed_files:
                print(f"- {file.full_path}: {error}", file=sys.stderr)
        
        self.simulation_completed = True
        return self.current_time
    
    def _has_remaining_files(self, files_by_tier: Dict[WorkerTier, List[FileMetadata]]) -> bool:
        """Check if there are any remaining files to process across all tiers."""
        return any(files for files in files_by_tier.values())
    
    def _get_total_active_workers(self) -> int:
        """Get the total number of active workers across all tiers."""
        return sum(len(workers) for workers in self.active_workers.values())
    
    def export_data_to_csv(self, base_filename: str = "simulation_data"):
        """Export simulation data to CSV files for automated analysis."""
        import csv
        
        # Export worker-level data (timeline visualization data)
        worker_file = f"{base_filename}_workers.csv"
        with open(worker_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            # Header row - including new CPU efficiency metrics
            writer.writerow([
                'Worker_ID', 'Tier', 'Start_Time', 'End_Time', 'Duration', 
                'SSTable_Count', 'Data_Size_GB', 'Is_Straggler_Worker',
                'Num_Threads', 'Total_Used_CPU_Time', 'Total_Active_CPU_Time',
                'CPU_Inefficiency', 'CPU_Efficiency_Percent'
            ])
            
            # Worker data rows
            for worker in self.completed_workers:
                efficiency_metrics = worker.get_cpu_efficiency_metrics()
                # Use calculated SSTable size instead of file metadata size
                total_sstable_size = worker.get_total_sstable_size()
                writer.writerow([
                    worker.worker_id,
                    worker.tier.value,
                    f"{worker.start_time:.2f}",
                    f"{worker.completion_time:.2f}",
                    f"{worker.completion_time - worker.start_time:.2f}",
                    worker.file.num_sstables if worker.file else 0,
                    f"{total_sstable_size / (1024*1024*1024):.2f}",
                    worker.is_straggler_worker,
                    worker.num_threads,
                    f"{efficiency_metrics['total_used_cpu_time']:.2f}",
                    f"{efficiency_metrics['total_active_cpu_time']:.2f}",
                    f"{efficiency_metrics['cpu_inefficiency']:.2f}",
                    f"{efficiency_metrics['cpu_efficiency_percent']:.1f}"
                ])
        
        # Export thread-level data (detailed visualization data)
        thread_file = f"{base_filename}_threads.csv"
        with open(thread_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            # Header row
            writer.writerow([
                'Worker_ID', 'Tier', 'Thread_ID', 'Task_Name', 'Start_Time', 
                'End_Time', 'Task_Size', 'Is_Straggler_Thread'
            ])
            
            # Thread/task data rows
            for worker in self.completed_workers:
                if worker.threads:
                    for thread in worker.threads:
                        is_straggler = thread.thread_id in worker.straggler_threads
                        for item, start_time in zip(thread.processed_items, thread.task_start_times):
                            writer.writerow([
                                worker.worker_id,
                                worker.tier.value,
                                thread.thread_id,
                                item.key,
                                f"{start_time:.2f}",
                                f"{start_time + item.size:.2f}",
                                f"{item.size:.2f}",
                                is_straggler
                            ])
        
        # Export summary statistics
        summary_file = f"{base_filename}_summary.csv"
        with open(summary_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            
            # Overall simulation metrics
            writer.writerow(['Metric', 'Value'])
            writer.writerow(['Total_Simulation_Time', f"{self.current_time:.2f}"])
            writer.writerow(['Total_Workers', len(self.completed_workers)])
            writer.writerow(['Straggler_Threshold_Percent', self.straggler_threshold_percent])
            
            # Add straggler analysis if available
            try:
                analysis = self.analyze_stragglers()
                writer.writerow(['Analyzable_Workers', analysis['analyzable_workers']])
                writer.writerow(['Non_Analyzable_Workers', analysis['non_analyzable_workers']])
                writer.writerow(['Straggler_Workers_Count', analysis['straggler_workers_count']])
                writer.writerow(['Straggler_Workers_Percent', f"{analysis['straggler_workers_percent']:.1f}"])
                
                # Per-tier breakdown
                writer.writerow([])  # Empty row separator
                writer.writerow(['Tier', 'Total_Workers', 'Analyzable_Workers', 'Straggler_Workers', 'Straggler_Percent'])
                for tier_name, tier_data in analysis["by_tier"].items():
                    writer.writerow([
                        tier_name,
                        tier_data['total_workers'],
                        tier_data['analyzable_workers'],
                        tier_data['straggler_workers'],
                        f"{tier_data['straggler_percent']:.1f}"
                    ])
            except SimulationError:
                writer.writerow(['Straggler_Analysis', 'Not_Available'])
        
        print(f"\nData exported to CSV files:")
        print(f"- Worker data: {worker_file}")
        print(f"- Thread/task data: {thread_file}")
        print(f"- Summary statistics: {summary_file}")

    def print_results(self, output_file: str = "simulation_results.html", show_details: bool = True, show_stragglers: bool = True, export_csv: bool = True, csv_base: str = None, detailed_page_size: int = None, detailed_per_worker: bool = None):
        """Print simulation results and save visualization."""
        print("\nSimulation Results:")
        print(f"Total workers: {len(self.completed_workers)}")
        
        # Print straggler report if requested
        if show_stragglers:
            self.print_straggler_report()
        
        # Export CSV data if requested
        if export_csv:
            if csv_base is None:
                csv_base = output_file.replace('.html', '')
            self.export_data_to_csv(csv_base)
        
        # Save visualizations
        timeline_file = output_file.replace('.html', '_timeline.html')
        detailed_file = output_file.replace('.html', '_detailed.html')
        
        save_timeline_visualization(self.completed_workers, timeline_file)
        if show_details:
            # Auto-detect if we should use per-worker mode based on migration size
            if detailed_per_worker is None:
                # Use per-worker mode for migrations with multiple workers (>5 workers or significant thread count)
                total_threads = sum(w.num_threads for w in self.completed_workers)
                use_per_worker = len(self.completed_workers) > 5 or total_threads > 25
            else:
                use_per_worker = detailed_per_worker
            
            if use_per_worker:
                print(f"\nUsing per-worker detailed visualization mode (recommended for large migrations)")
                print(f"Total workers: {len(self.completed_workers)}, Total threads: {sum(w.num_threads for w in self.completed_workers)}")
                save_detailed_visualization(self.completed_workers, detailed_file, per_worker=True)
            else:
                save_detailed_visualization(self.completed_workers, detailed_file, detailed_page_size)
        
        print(f"\nVisualization saved to {output_file}")
        print("Open this file in your web browser to view the interactive timeline visualization.")
        
        if show_details:
            if detailed_per_worker is None:
                total_threads = sum(w.num_threads for w in self.completed_workers)
                use_per_worker = len(self.completed_workers) > 5 or total_threads > 25
            else:
                use_per_worker = detailed_per_worker
                
            if use_per_worker:
                print(f"\nDetailed visualization has been split by worker for better performance")
                print(f"Start browsing from: {detailed_file}")
                print("Click 'Browse All Workers' to access individual worker details")
            elif detailed_page_size and detailed_page_size > 0 and len(self.completed_workers) > detailed_page_size:
                print(f"\nDetailed visualization has been split into multiple pages ({detailed_page_size} workers per page)")
                print("Use the navigation buttons to browse between pages")
        
        print("\nFeatures available in the visualization:")
        print("- Zoom and pan using mouse wheel and drag")
        print("- Hover over bars to see detailed information")
        print("- Click and drag to select regions")
        print("- Double click to reset the view")
        print("- Use the range slider at the bottom to navigate")
        print("- Click legend items to toggle visibility") 
    
    def analyze_stragglers(self) -> Dict[str, any]:
        """
        Analyze straggler patterns across all completed workers.
        
        Returns:
            Dictionary containing overall straggler analysis
        """
        if not self.simulation_completed:
            raise SimulationError("Simulation must be completed before analyzing stragglers")
        
        total_workers = len(self.completed_workers)
        
        # Separate workers into analyzable and non-analyzable
        analyzable_workers = [w for w in self.completed_workers if w.threads and len(w.threads) > 1]
        non_analyzable_workers = [w for w in self.completed_workers if not w.threads or len(w.threads) <= 1]
        
        straggler_workers = [w for w in analyzable_workers if w.is_straggler_worker]
        
        # Group by tier (only analyzable workers)
        straggler_by_tier = {
            WorkerTier.SMALL: [w for w in straggler_workers if w.tier == WorkerTier.SMALL],
            WorkerTier.MEDIUM: [w for w in straggler_workers if w.tier == WorkerTier.MEDIUM],
            WorkerTier.LARGE: [w for w in straggler_workers if w.tier == WorkerTier.LARGE]
        }
        
        analyzable_by_tier = {
            tier: len([w for w in analyzable_workers if w.tier == tier])
            for tier in WorkerTier
        }
        
        non_analyzable_by_tier = {
            tier: len([w for w in non_analyzable_workers if w.tier == tier])
            for tier in WorkerTier
        }
        
        # Calculate statistics
        analysis = {
            "threshold_percent": self.straggler_threshold_percent,
            "total_workers": total_workers,
            "analyzable_workers": len(analyzable_workers),
            "non_analyzable_workers": len(non_analyzable_workers),
            "straggler_workers_count": len(straggler_workers),
            "straggler_workers_percent": (len(straggler_workers) / len(analyzable_workers) * 100) if len(analyzable_workers) > 0 else 0,
            "by_tier": {}
        }
        
        for tier in WorkerTier:
            tier_stragglers = len(straggler_by_tier[tier])
            tier_analyzable = analyzable_by_tier[tier]
            tier_non_analyzable = non_analyzable_by_tier[tier]
            analysis["by_tier"][tier.value] = {
                "total_workers": tier_analyzable + tier_non_analyzable,
                "analyzable_workers": tier_analyzable,
                "non_analyzable_workers": tier_non_analyzable,
                "straggler_workers": tier_stragglers,
                "straggler_percent": (tier_stragglers / tier_analyzable * 100) if tier_analyzable > 0 else 0
            }
        
        return analysis
    
    def print_straggler_report(self):
        """Print a detailed report of straggler analysis."""
        try:
            analysis = self.analyze_stragglers()
        except SimulationError as e:
            print(f"Cannot generate straggler report: {e}")
            return
        
        print("\n" + "="*60)
        print("STRAGGLER ANALYSIS REPORT")
        print("="*60)
        print(f"Straggler Threshold: {analysis['threshold_percent']:.1f}% above average")
        print(f"Total Workers: {analysis['total_workers']}")
        print(f"Analyzable Workers: {analysis['analyzable_workers']} (multi-thread workers)")
        print(f"Non-analyzable Workers: {analysis['non_analyzable_workers']} (single/no-thread workers)")
        
        if analysis['analyzable_workers'] > 0:
            print(f"Straggler Workers: {analysis['straggler_workers_count']} ({analysis['straggler_workers_percent']:.1f}% of analyzable)")
        else:
            print("No workers available for straggler analysis")
        
        print("\nBreakdown by Tier:")
        print("-" * 50)
        for tier_name, tier_data in analysis["by_tier"].items():
            total = tier_data['total_workers']
            analyzable = tier_data['analyzable_workers']
            non_analyzable = tier_data['non_analyzable_workers']
            stragglers = tier_data['straggler_workers']
            straggler_pct = tier_data['straggler_percent']
            
            print(f"{tier_name:>6}: {total:>3} total ({analyzable:>2} analyzable, {non_analyzable:>2} single-thread)")
            if analyzable > 0:
                print(f"        {stragglers:>3}/{analyzable:>3} straggler workers ({straggler_pct:>5.1f}%)")
            else:
                print(f"        No workers available for analysis")
        
        # Show detailed information only for analyzable workers with stragglers
        analyzable_straggler_workers = [w for w in self.completed_workers 
                                      if w.is_straggler_worker and w.threads and len(w.threads) > 1]
        
        if analyzable_straggler_workers:
            print("\nDetailed Straggler Workers:")
            print("-" * 40)
            for worker in analyzable_straggler_workers:
                info = worker.get_straggler_info()
                print(f"Worker {worker.worker_id} ({worker.tier.value}):")
                print(f"  Straggler threads: {info['num_straggler_threads']}/{info['total_threads']}")
                print(f"  Completion time spread: {info['completion_time_spread']:.2f} units")
                for detail in info['straggler_details']:
                    print(f"    Thread {detail['thread_id']}: +{detail['delay_percent']:.1f}% slower")
        else:
            print("\nNo straggler workers found among analyzable workers.")

    def analyze_idle_threads(self) -> Dict[str, any]:
        """
        Analyze workers with idle threads across all completed workers.
        
        Returns:
            Dictionary containing idle thread analysis per tier
        """
        if not self.simulation_completed:
            raise SimulationError("Simulation must be completed before analyzing idle threads")
        
        analysis = {
            "total_workers": len(self.completed_workers),
            "by_tier": {}
        }
        
        for tier in WorkerTier:
            tier_workers = [w for w in self.completed_workers if w.tier == tier]
            workers_with_idle = []
            
            for worker in tier_workers:
                if worker.threads and len(worker.threads) > 1:
                    # Check for idle threads (threads that did no meaningful work)
                    thread_completion_times = [thread.available_time for thread in worker.threads]
                    if thread_completion_times:
                        median_time = sorted(thread_completion_times)[len(thread_completion_times) // 2]
                        meaningful_threshold = max(median_time * 0.1, 1.0)
                        
                        idle_threads = [thread for thread in worker.threads 
                                      if thread.available_time < meaningful_threshold]
                        
                        if idle_threads:
                            workers_with_idle.append({
                                'worker': worker,
                                'idle_thread_count': len(idle_threads),
                                'total_threads': len(worker.threads)
                            })
            
            analysis["by_tier"][tier.value] = {
                "total_workers": len(tier_workers),
                "workers_with_idle_threads": len(workers_with_idle),
                "idle_thread_details": workers_with_idle
            }
        
        return analysis

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
        
        report_data = {
            "total_execution_time": self.current_time,
            "total_migration_size_bytes": total_migration_size_bytes,
            "total_migration_size_gb": total_migration_size_gb,
            "simulation_config": {
                "small_threads": self.config.small.num_threads,
                "medium_threads": self.config.medium.num_threads,
                "large_threads": self.config.large.num_threads,
                "small_max_workers": self.config.small.max_workers,
                "medium_max_workers": self.config.medium.max_workers,
                "large_max_workers": self.config.large.max_workers,
                "straggler_threshold_percent": self.straggler_threshold_percent,
                "execution_mode": self.execution_mode.value,
                "max_concurrent_workers": self.max_concurrent_workers
            },
            "by_tier": {}
        }
        
        # Analyze idle threads
        idle_analysis = self.analyze_idle_threads()
        
        for tier in WorkerTier:
            tier_workers = [w for w in self.completed_workers if w.tier == tier]
            
            # Count different worker categories
            total_workers = len(tier_workers)
            straggler_workers = len([w for w in tier_workers if w.is_straggler_worker])
            
            # Workers with idle threads
            workers_with_idle = 0
            workers_with_both_straggler_and_idle = 0
            
            for worker in tier_workers:
                if worker.threads and len(worker.threads) > 1:
                    # Check for idle threads
                    thread_completion_times = [thread.available_time for thread in worker.threads]
                    if thread_completion_times:
                        median_time = sorted(thread_completion_times)[len(thread_completion_times) // 2]
                        meaningful_threshold = max(median_time * 0.1, 1.0)
                        
                        idle_threads = [thread for thread in worker.threads 
                                      if thread.available_time < meaningful_threshold]
                        
                        has_idle = len(idle_threads) > 0
                        has_straggler = worker.is_straggler_worker
                        
                        if has_idle:
                            workers_with_idle += 1
                        
                        if has_idle and has_straggler:
                            workers_with_both_straggler_and_idle += 1
            
            report_data["by_tier"][tier.value] = {
                "total_workers": total_workers,
                "straggler_workers": straggler_workers,
                "workers_with_idle_threads": workers_with_idle,
                "workers_with_both_straggler_and_idle": workers_with_both_straggler_and_idle
            }
        
        return report_data

    def export_execution_report_data(self, output_path: str):
        """Export execution report data as JSON for consumption by the helper script."""
        import json
        
        try:
            report_data = self.get_execution_report_data()
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(report_data, f, indent=2)
            print(f"Execution report data exported to {output_path}")
        except Exception as e:
            print(f"Warning: Failed to export execution report data: {e}")