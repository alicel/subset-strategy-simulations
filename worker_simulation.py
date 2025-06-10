from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
from heapq import heappush, heappop
from file_processor import FileMetadata, parse_input_files
from visualization_base import WorkerTier
from simulation import WorkItem, run_simulation
from timeline_visualization import save_timeline_visualization
from detailed_visualization import save_detailed_visualization
import math
import sys
import random

class SimulationError(Exception):
    """Custom exception for simulation errors"""
    pass

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

class MultiTierSimulation:
    def __init__(self, config: WorkerConfig, straggler_threshold_percent: float = 20.0):
        self.config = config
        self.straggler_threshold_percent = straggler_threshold_percent
        self.current_time = 0.0
        self.active_workers: Dict[WorkerTier, List[Worker]] = {
            WorkerTier.SMALL: [],
            WorkerTier.MEDIUM: [],
            WorkerTier.LARGE: []
        }
        self.completed_workers: List[Worker] = []
        # Store tuples of (completion_time, worker_id, worker) to ensure stable sorting
        self.completion_events: List[Tuple[float, int, Worker]] = []
        self.simulation_completed = False
        
    def can_add_worker(self, tier: WorkerTier) -> bool:
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
            # Include worker_id in the heap tuple to ensure stable sorting
            heappush(self.completion_events, (completion_time, worker.worker_id, worker))
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
    
    def export_data_to_csv(self, base_filename: str = "simulation_data"):
        """Export simulation data to CSV files for automated analysis."""
        import csv
        
        # Export worker-level data (timeline visualization data)
        worker_file = f"{base_filename}_workers.csv"
        with open(worker_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            # Header row
            writer.writerow([
                'Worker_ID', 'Tier', 'Start_Time', 'End_Time', 'Duration', 
                'SSTable_Count', 'Data_Size_GB', 'Is_Straggler_Worker'
            ])
            
            # Worker data rows
            for worker in self.completed_workers:
                writer.writerow([
                    worker.worker_id,
                    worker.tier.value,
                    f"{worker.start_time:.2f}",
                    f"{worker.completion_time:.2f}",
                    f"{worker.completion_time - worker.start_time:.2f}",
                    worker.file.num_sstables if worker.file else 0,
                    f"{worker.file.data_size / (1024*1024*1024):.2f}" if worker.file else "0.00",
                    worker.is_straggler_worker
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

    def print_results(self, output_file: str = "simulation_results.html", show_details: bool = True, show_stragglers: bool = True, export_csv: bool = True, csv_base: str = None):
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
            save_detailed_visualization(self.completed_workers, detailed_file)
        print(f"\nVisualization saved to {output_file}")
        print("Open this file in your web browser to view the interactive timeline visualization.")
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