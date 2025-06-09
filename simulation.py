from typing import List, NamedTuple
from dataclasses import dataclass
from heapq import heappush, heappop

@dataclass
class WorkItem:
    key: str
    size: int

class CompletionEvent(NamedTuple):
    completion_time: float
    thread_id: int
    item: WorkItem

class ThreadWork(NamedTuple):
    start_time: float
    item: WorkItem

class ThreadSimulator:
    def __init__(self, thread_id: int):
        self.thread_id = thread_id
        self.processed_items: List[WorkItem] = []
        self.work_timeline: List[ThreadWork] = []  # List of (start_time, item) pairs
        self.total_processing_time = 0.0
        self.available_time = 0.0  # When this thread will be available next
        self.task_start_times: List[float] = []  # Track start time for each task

def run_simulation(items: List[WorkItem], num_threads: int, processing_time_unit: float = 1.0) -> List['ThreadSimulator']:
    # Sort items by size in descending order
    work_items = sorted(items, key=lambda x: x.size, reverse=True)
    
    # Initialize threads
    threads = [ThreadSimulator(i) for i in range(num_threads)]
    
    # Event queue for completion events
    event_queue = []  # Will be used as a min-heap
    current_time = 0.0
    
    # Initial assignment of work
    available_threads = threads.copy()
    while work_items and available_threads:
        thread = available_threads.pop(0)
        item = work_items.pop(0)
        processing_time = item.size * processing_time_unit
        completion_time = current_time + processing_time
        
        thread.available_time = completion_time
        thread.total_processing_time += processing_time
        thread.processed_items.append(item)
        thread.task_start_times.append(current_time)  # Track start time
        
        heappush(event_queue, CompletionEvent(completion_time, thread.thread_id, item))
    
    # Process events until all work is done
    while event_queue:
        # Get next completion event
        event = heappop(event_queue)
        current_time = event.completion_time
        completed_thread = threads[event.thread_id]
        
        # If there's more work, assign it to the now-available thread
        if work_items:
            item = work_items.pop(0)
            processing_time = item.size * processing_time_unit
            completion_time = current_time + processing_time
            
            completed_thread.available_time = completion_time
            completed_thread.total_processing_time += processing_time
            completed_thread.processed_items.append(item)
            completed_thread.task_start_times.append(current_time)  # Track start time
            
            heappush(event_queue, CompletionEvent(completion_time, completed_thread.thread_id, item))
    
    return threads

def create_timeline_visualization(threads: List[ThreadSimulator], max_width: int = 100) -> str:
    """Create a timeline visualization of thread work.
    
    Args:
        threads: List of ThreadSimulator objects
        max_width: Maximum width of the visualization in characters
    """
    max_time = max(thread.available_time for thread in threads)
    if max_time == 0:
        return "No work performed"
        
    # Calculate scale factor to fit within max_width
    scale_factor = min(1.0, max_width / max_time)
    timeline_width = int(max_time * scale_factor)
    visualization = []
    
    # Create the header with time markers
    time_header = "Time: "
    marker_interval = max(1, timeline_width // 10)  # Show about 10 time markers
    for i in range(0, timeline_width + 1, marker_interval):
        actual_time = int(i / scale_factor)
        time_header += str(actual_time).rjust(marker_interval)
    visualization.append(time_header)
    visualization.append("=" * len(time_header))  # Use = for the header separator
    
    # Create timeline for each thread
    for thread in threads:
        # Initialize empty timeline
        timeline = [" "] * timeline_width
        
        # Fill in work items
        for item in thread.processed_items:
            start_time = sum(prev_item.size for prev_item in thread.processed_items[:thread.processed_items.index(item)])
            start_pos = int(start_time * scale_factor)
            length = int(item.size * scale_factor)
            if length == 0:  # Ensure at least 1 character for very small items
                length = 1
                
            # Create the bar with the task key and use - for the execution bar
            bar = f"[{item.key}:" + "-" * (length - len(item.key) - 3) + "]"
            # Make sure we don't exceed the timeline width
            end_pos = min(start_pos + length, timeline_width)
            # Fill in the bar
            for i in range(start_pos, end_pos):
                if i - start_pos < len(bar):
                    timeline[i] = bar[i - start_pos]
                else:
                    timeline[i] = "-"  # Use - for continuation of execution
        
        # Add thread label and timeline to visualization
        thread_line = f"Thread {thread.thread_id}: {''.join(timeline)}"
        visualization.append(thread_line)
    
    return "\n".join(visualization)

def print_simulation_results(threads: List[ThreadSimulator]):
    print("\nSimulation Results:")
    print("=" * 50)
    
    for thread in threads:
        print(f"\nThread {thread.thread_id}:")
        print(f"Total processing time: {thread.total_processing_time:.2f} time units")
        print("Processed items (in order):")
        for item in thread.processed_items:
            print(f"  - Key: {item.key}, Size: {item.size}")
    
    print("\nSummary:")
    print("=" * 50)
    max_time = max(thread.available_time for thread in threads)
    print(f"Total simulation time: {max_time:.2f} time units")
    
    # Calculate and print thread utilization
    print("\nThread Utilization:")
    for thread in threads:
        utilization = (thread.total_processing_time / max_time) * 100
        print(f"Thread {thread.thread_id}: {utilization:.1f}%")
    
    # Add timeline visualization
    print("\nTimeline Visualization:")
    print("-" * 50)
    # Calculate appropriate scale factor based on terminal width
    try:
        import os
        terminal_width = os.get_terminal_size().columns
        scale_factor = (terminal_width - 10) / max_time  # Leave some margin
    except:
        scale_factor = 1.0  # Default scale if can't get terminal width
    
    print(create_timeline_visualization(threads, scale_factor)) 