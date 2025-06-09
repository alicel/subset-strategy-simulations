from typing import List, Dict
import plotly.express as px
import plotly.figure_factory as ff
import pandas as pd
from visualization_base import Worker, WorkerTier
import plotly.graph_objects as go
from plotly.subplots import make_subplots

def create_detailed_visualization(workers: List[Worker]) -> go.Figure:
    """Create a detailed visualization showing thread-level execution for each worker."""
    # Create thread timeline figure
    thread_fig = go.Figure()
    
    # Define alternating colors for each tier
    tier_colors = {
        'SMALL': ['#00CC96', '#00A37A'],  # Green shades
        'MEDIUM': ['#EF553B', '#CC3F2A'],  # Red shades
        'LARGE': ['#636EFA', '#4B54CC']    # Blue shades
    }
    
    # Group by worker first, then by thread - show ALL threads including idle ones
    current_idx = 0
    current_worker_idx = {}  # Keep track of worker index per tier for color alternation
    thread_labels = []  # Track the actual order of threads for y-axis labels
    
    for worker in sorted(workers, key=lambda w: (w.tier.value, w.worker_id)):
        worker_name = f"{worker.tier.value} - Worker {worker.worker_id}"
        
        # Get worker tier for coloring
        tier = worker.tier.value
        
        # Initialize worker index for this tier if not exists
        if tier not in current_worker_idx:
            current_worker_idx[tier] = 0
            
        # Get color based on worker index (alternating)
        color = tier_colors[tier][current_worker_idx[tier] % 2]
        
        # Increment worker index for this tier
        current_worker_idx[tier] += 1
        
        # Show ALL threads for this worker (including idle ones)
        for thread_id in range(worker.num_threads):
            # Extract numeric IDs for compact labeling
            compact_label = f"W{worker.worker_id}-T{thread_id}"
            
            # Track the thread label in the correct order
            thread_labels.append(compact_label)
            
            # Find the actual thread data if it exists
            actual_thread = None
            if worker.threads:
                for thread in worker.threads:
                    if thread.thread_id == thread_id:
                        actual_thread = thread
                        break
            
            if actual_thread and actual_thread.processed_items:
                # This thread did work - show its tasks
                is_straggler_thread = thread_id in worker.straggler_threads
                
                # Set border properties for straggler threads (gold border) or normal borders (dark border)
                straggler_border = dict(width=3, color='#FFD700') if is_straggler_thread else None
                
                # Add each task as a separate bar trace to enable individual borders
                for idx, (item, start_time) in enumerate(zip(actual_thread.processed_items, actual_thread.task_start_times)):
                    # Set border for each individual task
                    if straggler_border:
                        # Straggler threads get gold borders
                        task_border = straggler_border
                    else:
                        # Normal threads get dark borders to separate tasks
                        task_border = dict(width=1, color='#2E2E2E')
                    
                    thread_fig.add_trace(go.Bar(
                        x=[item.size],
                        y=[current_idx],
                        orientation='h',
                        name=worker_name,
                        base=[start_time],
                        width=0.8,  # Thicker bars
                        marker_color=color,
                        marker_line=task_border,  # Add border for each individual task
                        text=[item.key],  # Show task ID in the bar
                        textposition='inside',
                        textfont=dict(
                            size=14,  # Larger font size
                            color='white',
                            family='Arial Black'
                        ),
                        textangle=0,
                        insidetextanchor='middle',
                        hovertemplate="<br>".join([
                            "Worker: %{customdata[2]}",
                            "Thread: %{customdata[3]}%{customdata[4]}",
                            "Task: %{customdata[0]}",
                            "Start: %{base:.2f}",
                            "End: %{x:.2f}",
                            "Size: %{customdata[1]:.2f}"
                        ]),
                        customdata=[[
                            item.key,
                            item.size,
                            worker_name,
                            f"Thread {thread_id}",
                            " (STRAGGLER)" if is_straggler_thread else ""
                        ]],
                        showlegend=False  # Disable legend - y-axis labels provide worker/thread info
                    ))
            else:
                # This thread was idle - show it as a label but no bars
                # We don't add any bars, but the label will still appear on the y-axis
                pass
            
            current_idx += 1
    
    if current_idx == 0:
        return None
    
    # Update layout
    thread_fig.update_layout(
        title={
            'text': "Detailed Thread Timelines<br><sup>Thread-level execution details for each worker</sup>",
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
            showticklabels=True,  # Show thread labels
            ticktext=thread_labels,
            tickvals=list(range(len(thread_labels))),
            showgrid=True,
            gridwidth=1,
            gridcolor='rgba(211, 211, 211, 0.5)',
            side='left',
            range=[-0.5, current_idx - 0.5]  # Tight range to avoid extra space
        ),
        xaxis=dict(
            title="Time Units",
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
            l=150,  # Increased left margin for thread labels
            r=20,
            t=150,  # Increased top margin from 120 to 150 for better title space
            b=30,
            pad=4
        ),
        plot_bgcolor='rgba(240, 245, 250, 0.95)'
    )
    
    return thread_fig

def save_detailed_visualization(workers: List[Worker], output_path: str = "detailed_results.html"):
    """Save the detailed thread visualization to an HTML file."""
    thread_fig = create_detailed_visualization(workers)
    if thread_fig is not None:
        thread_fig.write_html(output_path)
        print(f"Detailed visualization saved to {output_path}")
    else:
        print("No thread data available for detailed visualization") 