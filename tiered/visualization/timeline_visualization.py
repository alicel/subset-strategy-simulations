from typing import List, Dict
import plotly.express as px
import plotly.graph_objects as go
from .visualization_base import Worker, WorkerTier

def create_timeline_visualization(workers: List[Worker]) -> go.Figure:
    """Create an interactive Plotly visualization of the worker timeline."""
    # Debug logging
    print("\nDebug: Creating visualization")
    print(f"Number of workers: {len(workers)}")
    
    # Sort workers by tier, start time, and worker_id for consistent ordering
    # When start times are identical (common with concurrent workers), use worker_id for numerical order
    # Use explicit tier ordering: LARGE first, then MEDIUM, then SMALL
    tier_order = {'LARGE': 0, 'MEDIUM': 1, 'SMALL': 2}
    workers = sorted(workers, key=lambda w: (tier_order[w.tier.value], w.start_time, w.worker_id))
    
    # Reverse for visual display so that W0 appears at top and higher worker IDs appear below
    workers = list(reversed(workers))
    
    # Create figure
    fig = go.Figure()
    
    # Color map for tiers (normal workers)
    colors = {'S': '#00CC96', 'M': '#EF553B', 'L': '#636EFA'}
    
    # Group workers by tier
    tier_groups = {}
    for worker in workers:
        tier_label = {'SMALL': 'S', 'MEDIUM': 'M', 'LARGE': 'L'}[worker.tier.value]
        if tier_label not in tier_groups:
            tier_groups[tier_label] = []
        tier_groups[tier_label].append(worker)
    
    # Track overall index for consistent y-positioning
    current_idx = 0
    worker_labels = []  # Track worker labels for y-axis
    
    # Add bars for each tier
    for tier in ['S', 'M', 'L']:
        if tier in tier_groups:
            tier_workers = tier_groups[tier]
            for worker in tier_workers:
                # Create worker label in format "S-W12" 
                worker_label = f"{tier}-W{worker.worker_id}"
                worker_labels.append(worker_label)
                
                label = f"{tier} - Wrk {worker.worker_id}"
                
                # Determine if worker has idle threads
                has_idle_threads = False
                if worker.threads:
                    # Count threads that actually did work (have processed_items)
                    active_thread_count = sum(1 for thread in worker.threads if thread.processed_items)
                    has_idle_threads = active_thread_count < worker.num_threads
                else:
                    # If no threads at all, all are considered idle
                    has_idle_threads = worker.num_threads > 0
                
                # Set background color based on performance status
                # Tier information is now shown in the y-axis labels (e.g., "S-W12")
                if worker.is_straggler_worker and has_idle_threads:
                    # Purple background for both straggler and idle threads
                    bar_color = '#8B00FF'  # Purple
                    status_suffix = " (STRAGGLER + IDLE)"
                elif worker.is_straggler_worker:
                    # Fluorescent yellow background for straggler threads only
                    bar_color = '#FFFF00'  # Fluorescent yellow
                    status_suffix = " (STRAGGLER)"
                elif has_idle_threads:
                    # Bright orange background for idle threads only
                    bar_color = '#FF8C00'  # Bright orange
                    status_suffix = " (IDLE)"
                else:
                    # Normal tier color for regular workers
                    bar_color = colors[tier]
                    status_suffix = ""
                
                # Add the bar with just the worker ID as text
                fig.add_trace(go.Bar(
                    x=[worker.completion_time - worker.start_time],
                    y=[current_idx],  # Use consistent index for positioning
                    orientation='h',
                    name=tier,
                    base=[worker.start_time],
                    width=0.8,  # Thicker bars
                    marker_color=bar_color,  # Use the determined background color
                    marker_line=dict(width=0),  # No border needed since tier info is in y-axis labels
                    text=[str(worker.worker_id)],  # Just the worker ID
                    textposition='inside',
                    textfont=dict(
                        size=14,  # Larger font size
                        color='white' if bar_color != '#FFFF00' else 'black',  # Black text on yellow background
                        family='Arial Black'
                    ),
                    textangle=0,  # Force horizontal text
                    insidetextanchor='middle',  # Center the text in the bar
                    hovertemplate="<br>".join([
                        "Worker: %{customdata[3]}%{customdata[4]}",
                        "Start Time: %{base:.2f} units",
                        "End Time: %{x:.2f} units",
                        "Duration: %{customdata[0]:.2f} units",
                        "SSTable Count: %{customdata[1]}",
                        "Data Size: %{customdata[2]} [%{customdata[5]:.2f} MB | %{customdata[6]:.2f} GB]",
                        "<extra>Click and drag to zoom. Double-click to reset.</extra>"
                    ]),
                    customdata=[[
                        worker.completion_time - worker.start_time,
                        worker.file.num_sstables if worker.file else 0,
                        worker.file.data_size if worker.file else 0,
                        label,
                        status_suffix,
                        worker.file.data_size / (1024*1024) if worker.file else 0,  # MB
                        worker.file.data_size / (1024*1024*1024) if worker.file else 0  # GB
                    ]],
                    showlegend=False  # Disable legend - y-axis grouping and colors show tier info
                ))
                current_idx += 1
    
    # Update layout
    fig.update_layout(
        title={
            'text': "Multi-tier Simulation Results<br><sup>Worker labels show tier (S/M/L) and ID. Numbers in bars indicate worker IDs.<br>Highlights: Yellow=Stragglers, Orange=Idle Threads, Purple=Both</sup>",
            'x': 0.5,
            'xanchor': 'center',
            'y': 0.95,  # Moved down from 0.99 to 0.95 to prevent cutoff
            'yanchor': 'top'  # Anchor to top of title
        },

        height=max(800, current_idx * 25),  # Ensure minimum height and proper spacing
        showlegend=False,
        hovermode="closest",
        barmode='stack',  # Changed from 'overlay' to prevent overlap
        bargap=0,  # Remove gap between bars since we're controlling spacing with y-values
        bargroupgap=0,
        yaxis=dict(
            showticklabels=True,
            showgrid=True,
            gridwidth=1,
            gridcolor='rgba(211, 211, 211, 0.5)',
            side='left',
            range=[-0.5, current_idx - 0.5],  # Tight range to avoid extra space
            ticktext=worker_labels,
            tickvals=list(range(current_idx))
        ),
        xaxis=dict(
            showgrid=True,
            gridwidth=1,
            gridcolor='rgba(211, 211, 211, 0.5)',
            zeroline=False,
            rangemode='tozero',  # Ensure x-axis starts from 0
            rangeslider=dict(
                visible=True,
                thickness=0.10,
                bgcolor='#E2E2E2'
            )
        ),
        margin=dict(
            l=80,  # Increased left margin to accommodate worker labels like "S-W12"
            r=20,
            t=150,  # Increased top margin from 120 to 150 for better title space
            b=30,
            pad=4
        ),
        plot_bgcolor='rgba(240, 245, 250, 0.95)'
    )
    
    return fig

def save_timeline_visualization(workers: List[Worker], output_path: str = "timeline_results.html"):
    """Save the timeline visualization to an HTML file."""
    fig = create_timeline_visualization(workers)
    fig.write_html(output_path)
    print(f"Timeline visualization saved to {output_path}") 