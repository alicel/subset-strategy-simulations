from typing import List, NamedTuple
from dataclasses import dataclass
from heapq import heappush, heappop

@dataclass
class WorkItem:
    key: str
    size: int


class WorkerWork(NamedTuple):
    start_time: float
    item: WorkItem

class ThreadState:
    """Represents the state of a single thread within a worker."""
    def __init__(self, thread_id: int, worker_id: int):
        self.thread_id = thread_id
        self.worker_id = worker_id
        self.processed_items: List[WorkItem] = []
        self.work_timeline: List[WorkerWork] = []
        self.total_processing_time = 0.0
        self.available_time = 0.0
        self.task_start_times: List[float] = []

class SingleThreadSimulator:
    """Simulates a single worker processing work items sequentially."""
    
    def __init__(self, worker_id: int):
        self.worker_id = worker_id
        self.processed_items: List[WorkItem] = []
        self.work_timeline: List[WorkerWork] = []  # List of (start_time, item) pairs
        self.total_processing_time = 0.0
        self.available_time = 0.0  # When this worker will be available next
        self.task_start_times: List[float] = []  # Track start time for each task

class MultiThreadSimulator:
    """Simulates multiple threads within a single worker processing work items in order."""
    
    def __init__(self, worker_id: int, num_threads: int):
        self.worker_id = worker_id
        self.num_threads = num_threads
        self.threads = [ThreadState(i, worker_id) for i in range(num_threads)]
        self.processed_items: List[WorkItem] = []  # All items processed by this worker
        self.work_timeline: List[WorkerWork] = []  # Combined timeline from all threads
        self.total_processing_time = 0.0
        self.available_time = 0.0  # When this worker (all threads) will be available next
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

def run_multi_thread_simulation(items: List[WorkItem], worker_id: int, num_threads: int, start_time: float = 0.0, processing_time_unit: float = 1.0) -> MultiThreadSimulator:
    """
    Simulate multi-threaded processing of work items by a single worker.
    All threads consume items from the same list in order.
    
    Args:
        items: List of WorkItem objects to process
        worker_id: Unique identifier for this worker
        num_threads: Number of threads for this worker
        start_time: When this worker starts processing
        processing_time_unit: Time units per size unit
        
    Returns:
        MultiThreadSimulator with complete processing timeline
    """
    # CRITICAL: Process items in the EXACT order they appear in the subset definition file
    # All threads consume from the same ordered list
    work_items = items.copy()
    
    # Initialize multi-thread simulator
    worker = MultiThreadSimulator(worker_id, num_threads)
    
    # Initialize thread availability times
    for thread in worker.threads:
        thread.available_time = start_time
    
    # Process each work item in order, assigning to the earliest available thread
    for item in work_items:
        processing_time = item.size * processing_time_unit
        
        # Find the thread that will be available earliest
        earliest_thread = min(worker.threads, key=lambda t: t.available_time)
        
        # Record work start
        start_time_for_item = earliest_thread.available_time
        worker.task_start_times.append(start_time_for_item)
        worker.work_timeline.append(WorkerWork(start_time_for_item, item))
        
        # Record work for the specific thread
        earliest_thread.task_start_times.append(start_time_for_item)
        earliest_thread.work_timeline.append(WorkerWork(start_time_for_item, item))
        
        # Process the item
        earliest_thread.available_time += processing_time
        earliest_thread.total_processing_time += processing_time
        earliest_thread.processed_items.append(item)
        
        # Add to worker's processed items
        worker.processed_items.append(item)
    
    # Set final availability time as the latest thread completion time
    worker.available_time = max(thread.available_time for thread in worker.threads)
    worker.total_processing_time = sum(thread.total_processing_time for thread in worker.threads)
    
    return worker

# Timeline visualization functions removed - using plotly visualizations only 