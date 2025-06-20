from typing import List, NamedTuple
from dataclasses import dataclass
from heapq import heappush, heappop

@dataclass
class WorkItem:
    key: str
    size: int

class CompletionEvent(NamedTuple):
    completion_time: float
    worker_id: int
    item: WorkItem

class WorkerWork(NamedTuple):
    start_time: float
    item: WorkItem

class SingleThreadSimulator:
    """Simulates a single worker processing work items sequentially."""
    
    def __init__(self, worker_id: int):
        self.worker_id = worker_id
        self.processed_items: List[WorkItem] = []
        self.work_timeline: List[WorkerWork] = []  # List of (start_time, item) pairs
        self.total_processing_time = 0.0
        self.available_time = 0.0  # When this worker will be available next
        self.task_start_times: List[float] = []  # Track start time for each task

def run_single_thread_simulation(items: List[WorkItem], worker_id: int, start_time: float = 0.0, processing_time_unit: float = 1.0) -> SingleThreadSimulator:
    """
    Simulate sequential processing of work items by a single worker.
    
    Args:
        items: List of WorkItem objects to process
        worker_id: Unique identifier for this worker
        start_time: When this worker starts processing
        processing_time_unit: Time units per size unit
        
    Returns:
        SingleThreadSimulator with complete processing timeline
    """
    # CRITICAL: Process items in the EXACT order they appear in the subset definition file
    # This matches the real production system's behavior where SSTables are processed sequentially
    work_items = items.copy()
    
    # Initialize worker simulator
    worker = SingleThreadSimulator(worker_id)
    current_time = start_time
    
    # Process each work item sequentially
    for item in work_items:
        processing_time = item.size * processing_time_unit
        
        # Record work start
        worker.task_start_times.append(current_time)
        worker.work_timeline.append(WorkerWork(current_time, item))
        
        # Process the item
        current_time += processing_time
        worker.total_processing_time += processing_time
        worker.processed_items.append(item)
    
    # Set final availability time
    worker.available_time = current_time
    
    return worker

# Timeline visualization functions removed - using plotly visualizations only 