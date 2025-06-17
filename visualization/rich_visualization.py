from typing import List, Optional, Dict
from dataclasses import dataclass
from rich.console import Console
from rich.table import Table, Column
from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn
from rich.panel import Panel
from rich.align import Align
from rich.rule import Rule
from rich.text import Text
from rich.style import Style
from .visualization_base import Worker, WorkerTier
from rich.console import Group
from rich.layout import Layout
from rich import box
from rich.measure import Measurement

def create_worker_timeline(worker: Worker, total_width: int = 200) -> Table:
    """Create a detailed timeline visualization for a single worker's threads."""
    if not worker.threads:
        return None
        
    table = Table(
        box=box.MINIMAL,
        show_header=True,
        header_style="bold",
        show_edge=False,
        expand=True,
        width=None  # Allow table to expand beyond terminal width
    )
    table.add_column("Thread", ratio=1)
    table.add_column("Timeline", ratio=20, no_wrap=True)  # Use ratio for better space distribution
    
    max_time = max(thread.available_time for thread in worker.threads)
    if max_time == 0:
        return None
        
    for thread in worker.threads:
        # Create timeline visualization
        timeline = ["·"] * total_width  # Use middle dot for empty space
        
        # Calculate positions for each work item
        for item in thread.processed_items:
            start_time = sum(prev_item.size for prev_item in thread.processed_items[:thread.processed_items.index(item)])
            start_pos = int((start_time / max_time) * total_width)
            length = max(1, int((item.size / max_time) * total_width))
            
            # Make sure we don't exceed the timeline width
            end_pos = min(start_pos + length, total_width)
            
            # Create timeline text with colored segments
            timeline_text = Text()
            
            # Add the part before the bar
            if start_pos > 0:
                timeline_text.append("".join(timeline[:start_pos]), style="dim")
            
            # Add the execution bar
            bar_text = ["━"] * (end_pos - start_pos)
            timeline_text.append("".join(bar_text), style="bright_blue bold")
            
            # Add the part after the bar
            if end_pos < total_width:
                timeline_text.append("".join(timeline[end_pos:]), style="dim")
            
            timeline = list(str(timeline_text))
        
        table.add_row(f"Thread {thread.thread_id}", Text("".join(timeline)))
    
    return table

def create_global_timeline(workers: List[Worker], min_width: int = 200) -> Table:
    """Create a global timeline visualization showing all workers across tiers."""
    if not workers:
        return None
        
    # Group workers by tier
    workers_by_tier = {tier: [] for tier in WorkerTier}
    for worker in sorted(workers, key=lambda w: (w.tier.value, w.worker_id)):
        workers_by_tier[worker.tier].append(worker)
    
    # Find global time range
    global_start = min(worker.start_time for worker in workers)
    global_end = max(worker.completion_time for worker in workers)
    duration = global_end - global_start
    
    # Create main table
    table = Table(
        box=box.MINIMAL,
        show_header=True,
        header_style="bold",
        show_edge=False,
        expand=True,
        width=None  # Allow table to expand beyond terminal width
    )
    table.add_column("Tier/Worker", ratio=1)
    table.add_column("Timeline", ratio=20, no_wrap=True)  # Use ratio for better space distribution
    
    # Add time markers
    time_markers = ["·"] * min_width
    for i in range(0, min_width, min_width // 10):
        time = global_start + (i / min_width) * duration
        time_str = f"{time:.1f}"
        # Place time markers without overlapping
        for j, char in enumerate(time_str):
            if i + j < min_width:
                time_markers[i + j] = char
    
    table.add_row("Time", Text("".join(time_markers), style="bold cyan"))
    table.add_row("", "")
    
    # Add worker timelines for each tier
    for tier in WorkerTier:
        tier_workers = workers_by_tier[tier]
        if not tier_workers:
            continue
            
        table.add_row(tier.value, "", style="bold")
        for worker in tier_workers:
            timeline = ["·"] * min_width
            start_pos = int(((worker.start_time - global_start) / duration) * min_width)
            end_pos = int(((worker.completion_time - global_start) / duration) * min_width)
            
            # Ensure at least one character width
            if end_pos == start_pos:
                end_pos = start_pos + 1
            
            # Create timeline text with colored segments
            timeline_text = Text()
            
            # Add the part before the bar
            if start_pos > 0:
                timeline_text.append("".join(timeline[:start_pos]), style="dim")
            
            # Add the execution bar
            bar_text = ["━"] * (end_pos - start_pos)
            timeline_text.append("".join(bar_text), style="bright_blue bold")
            
            # Add the part after the bar
            if end_pos < min_width:
                timeline_text.append("".join(timeline[end_pos:]), style="dim")
            
            table.add_row(f"  Subset {worker.worker_id:2d}", timeline_text)
        
        table.add_row("", "")  # Add spacing between tiers
    
    return table

def create_tier_summary(workers: List[Worker]) -> Table:
    """Create a summary table with statistics for each tier."""
    table = Table(
        box=box.MINIMAL,
        show_header=True,
        header_style="bold",
        show_edge=False,
        expand=True
    )
    table.add_column("Tier", ratio=1)
    table.add_column("Workers", ratio=1)
    table.add_column("Files", ratio=1)
    table.add_column("SSTables", ratio=1)
    table.add_column("Data Size", ratio=1)
    
    # Group workers by tier
    workers_by_tier = {tier: [] for tier in WorkerTier}
    for worker in workers:
        workers_by_tier[worker.tier].append(worker)
    
    for tier in WorkerTier:
        tier_workers = workers_by_tier[tier]
        if not tier_workers:
            continue
            
        total_files = len(tier_workers)
        total_sstables = sum(w.file.num_sstables for w in tier_workers)
        total_data_size = sum(w.file.data_size for w in tier_workers)
        
        table.add_row(
            tier.value,
            str(len(set(w.worker_id for w in tier_workers))),
            str(total_files),
            str(total_sstables),
            f"{total_data_size / (1024*1024*1024):.1f}GB"
        )
    
    return table

def print_rich_visualization(workers: List[Worker], show_details: bool = True):
    """Print a rich visualization of the simulation results."""
    console = Console(force_terminal=True)
    
    # Create title
    title = Panel("Multi-tier Simulation Results", style="bold")
    console.print(title)
    console.line()
    
    # Print global timeline with horizontal scroll hint
    timeline_panel = Panel(
        create_global_timeline(workers, min_width=200),  # Fixed larger width for better detail
        title="Global Timeline",
        subtitle="[dim](Use arrow keys or trackpad to scroll horizontally →)"
    )
    console.print(timeline_panel)
    console.line()
    
    # Print summary
    summary_panel = Panel(create_tier_summary(workers), title="Tier Summary")
    console.print(summary_panel)
    
    # Show detailed timelines if requested
    if show_details:
        console.line()
        console.rule("[bold]Detailed Worker Timelines")
        for worker in workers:
            timeline = create_worker_timeline(worker)
            if timeline:
                console.print(Panel(
                    timeline,
                    title=f"Worker {worker.worker_id} ({worker.tier.value})",
                    subtitle=f"[dim]Processing time: {worker.completion_time - worker.start_time:.1f} units"
                ))
                console.line() 