from typing import List, Dict
import plotly.express as px
import plotly.graph_objects as go
from core.worker_simulation import SimpleWorker

def create_timeline_visualization(workers: List[SimpleWorker]) -> go.Figure:
    """Create an interactive Plotly timeline visualization for the simple simulation."""
    print("\nDebug: Creating simple simulation visualization")
    print(f"Number of workers: {len(workers)}")
    
    if not workers:
        print("Warning: No workers to visualize")
        return go.Figure()
    
    # Sort workers by worker_id for consistent ordering
    workers = sorted(workers, key=lambda w: w.worker_id)
    
    # Create figure
    fig = go.Figure()
    
    # Single color for all workers since there are no tiers
    worker_color = '#636EFA'  # Blue color
    
    # Add bars for each worker
    for i, worker in enumerate(workers):
        if worker.completion_time is None or worker.start_time is None:
            continue
            
        worker_label = f"Worker {worker.worker_id}"
        duration = worker.completion_time - worker.start_time
        
        # Calculate additional metrics
        num_items = len(worker.simulator.processed_items) if worker.simulator else 0
        total_work = sum(item.size for item in worker.simulator.processed_items) if worker.simulator else 0
        
        # Add the bar
        fig.add_trace(go.Bar(
            x=[duration],
            y=[i],  # Use index for positioning
            orientation='h',
            name='Workers',
            base=[worker.start_time],
            width=0.7,  # Bar thickness
            marker_color=worker_color,
            text=[str(worker.worker_id)],  # Worker ID as text
            textposition='inside',
            textfont=dict(
                size=12,
                color='white',
                family='Arial Black'
            ),
            textangle=0,
            insidetextanchor='middle',
            hovertemplate="<br>".join([
                "Worker: %{customdata[4]}",
                "Subset ID: %{customdata[5]}",
                "Start Time: %{base:.2f} units",
                "End Time: %{customdata[0]:.2f} units",
                "Duration: %{customdata[1]:.2f} units",
                "Items Processed: %{customdata[2]}",
                "Total Work Size: %{customdata[3]:,.0f} bytes",
                "<extra>Click and drag to zoom. Double-click to reset.</extra>"
            ]),
            customdata=[[
                worker.completion_time,
                duration,
                num_items,
                total_work,
                worker_label,
                worker.subset_id
            ]],
            showlegend=i == 0  # Show legend only for first worker
        ))
    
    # Create worker labels for y-axis
    worker_labels = [f"W{worker.worker_id}" for worker in workers]
    
    # Update layout
    fig.update_layout(
        title={
            'text': "Simple Simulation Results<br><sup>Workers processing subsets with configurable thread counts. Numbers indicate worker IDs.</sup>",
            'x': 0.5,
            'xanchor': 'center',
            'y': 0.95,
            'yanchor': 'top'
        },
        height=max(400, len(workers) * 30),  # Ensure minimum height and proper spacing
        showlegend=True,
        legend_title="Workers",
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
        hovermode="closest",
        barmode='overlay',
        bargap=0.2,
        bargroupgap=0.1,
        yaxis=dict(
            showticklabels=True,
            showgrid=True,
            gridwidth=1,
            gridcolor='rgba(211, 211, 211, 0.5)',
            side='left',
            range=[-0.5, len(workers) - 0.5],
            ticktext=worker_labels,
            tickvals=list(range(len(workers))),
            title="Workers"
        ),
        xaxis=dict(
            title="Time Units",
            showgrid=True,
            gridwidth=1,
            gridcolor='rgba(211, 211, 211, 0.5)',
            zeroline=False,
            rangemode='tozero',
            rangeslider=dict(
                visible=True,
                thickness=0.10,
                bgcolor='#E2E2E2'
            )
        ),
        margin=dict(
            l=60,
            r=20,
            t=120,
            b=80,
            pad=4
        ),
        plot_bgcolor='rgba(240, 245, 250, 0.95)'
    )
    
    return fig

def create_worker_details_visualization(workers: List[SimpleWorker]) -> go.Figure:
    """Create a detailed visualization showing work distribution across workers."""
    print("\nDebug: Creating worker details visualization")
    
    if not workers:
        return go.Figure()
    
    # Sort workers by completion time for analysis
    workers = sorted(workers, key=lambda w: w.completion_time or 0)
    
    # Extract data for visualization
    worker_ids = [f"W{w.worker_id}" for w in workers]
    completion_times = [w.completion_time or 0 for w in workers]
    durations = [(w.completion_time or 0) - w.start_time for w in workers]
    item_counts = [len(w.simulator.processed_items) if w.simulator else 0 for w in workers]
    work_sizes = [sum(item.size for item in w.simulator.processed_items) if w.simulator else 0 for w in workers]
    
    # Create subplots
    from plotly.subplots import make_subplots
    
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=('Completion Times', 'Processing Durations', 'Items Processed', 'Total Work Size'),
        specs=[[{"secondary_y": False}, {"secondary_y": False}],
               [{"secondary_y": False}, {"secondary_y": False}]]
    )
    
    # Completion times
    fig.add_trace(
        go.Bar(x=worker_ids, y=completion_times, name='Completion Time', 
               marker_color='lightblue', showlegend=False),
        row=1, col=1
    )
    
    # Processing durations
    fig.add_trace(
        go.Bar(x=worker_ids, y=durations, name='Duration', 
               marker_color='lightgreen', showlegend=False),
        row=1, col=2
    )
    
    # Items processed
    fig.add_trace(
        go.Bar(x=worker_ids, y=item_counts, name='Items', 
               marker_color='lightcoral', showlegend=False),
        row=2, col=1
    )
    
    # Total work size (in MB for readability)
    work_sizes_mb = [size / (1024 * 1024) for size in work_sizes]
    fig.add_trace(
        go.Bar(x=worker_ids, y=work_sizes_mb, name='Work Size (MB)', 
               marker_color='lightsalmon', showlegend=False),
        row=2, col=2
    )
    
    # Update layout
    fig.update_layout(
        title={
            'text': "Simple Simulation - Worker Analysis",
            'x': 0.5,
            'xanchor': 'center'
        },
        height=600,
        showlegend=False,
        plot_bgcolor='rgba(240, 245, 250, 0.95)'
    )
    
    # Update axis labels
    fig.update_yaxes(title_text="Time Units", row=1, col=1)
    fig.update_yaxes(title_text="Time Units", row=1, col=2)
    fig.update_yaxes(title_text="Count", row=2, col=1)
    fig.update_yaxes(title_text="MB", row=2, col=2)
    
    return fig

def create_work_distribution_visualization(workers: List[SimpleWorker]) -> go.Figure:
    """Create a visualization showing the distribution of work items within workers."""
    print("\nDebug: Creating work distribution visualization")
    
    if not workers:
        return go.Figure()
    
    fig = go.Figure()
    
    # Color palette for different work items
    colors = px.colors.qualitative.Set3
    
    # Track y-position for stacking
    y_pos = 0
    worker_labels = []
    
    for worker in sorted(workers, key=lambda w: w.worker_id):
        if not worker.simulator or not worker.simulator.processed_items:
            continue
            
        worker_labels.append(f"W{worker.worker_id}")
        current_time = worker.start_time
        
        # Add each work item as a segment
        for i, item in enumerate(worker.simulator.processed_items):
            item_duration = item.size  # Assuming processing_time_unit = 1.0
            
            fig.add_trace(go.Bar(
                x=[item_duration],
                y=[y_pos],
                orientation='h',
                base=[current_time],
                name=f"Item {item.key}",
                marker_color=colors[i % len(colors)],
                width=0.6,
                text=[item.key],
                textposition='inside',
                textfont=dict(size=10, color='black'),
                hovertemplate="<br>".join([
                    f"Worker: W{worker.worker_id}",
                    f"Item: {item.key}",
                    f"Start: {current_time:.2f}",
                    f"End: {current_time + item_duration:.2f}",
                    f"Size: {item.size:,} bytes",
                    "<extra></extra>"
                ]),
                showlegend=False
            ))
            
            current_time += item_duration
        
        y_pos += 1
    
    # Update layout
    fig.update_layout(
        title={
            'text': "Simple Simulation - Work Item Distribution<br><sup>Shows sequential processing of items within each worker</sup>",
            'x': 0.5,
            'xanchor': 'center'
        },
        height=max(300, len(worker_labels) * 40),
        xaxis_title="Time Units",
        yaxis=dict(
            title="Workers",
            ticktext=worker_labels,
            tickvals=list(range(len(worker_labels))),
            range=[-0.5, len(worker_labels) - 0.5]
        ),
        showlegend=False,
        plot_bgcolor='rgba(240, 245, 250, 0.95)',
        margin=dict(l=60, r=20, t=80, b=60)
    )
    
    return fig

def save_timeline_visualization(workers: List[SimpleWorker], output_path: str = "simple_timeline_results.html"):
    """Save the timeline visualization to an HTML file."""
    fig = create_timeline_visualization(workers)
    fig.write_html(output_path)
    print(f"Simple timeline visualization saved to {output_path}")

def save_comprehensive_visualization(workers: List[SimpleWorker], output_path: str = "simple_comprehensive_results.html"):
    """Save a comprehensive visualization with multiple views to an HTML file."""
    # Create all visualizations
    timeline_fig = create_timeline_visualization(workers)
    details_fig = create_worker_details_visualization(workers)
    # Note: distribution visualization is no longer generated as requested
    
    # Save timeline as primary visualization
    timeline_fig.write_html(output_path)
    
    # Save additional visualizations with different names
    base_path = output_path.replace('.html', '')
    details_fig.write_html(f"{base_path}_details.html")
    # distribution_fig.write_html(f"{base_path}_distribution.html")  # Commented out to skip distribution
    
    print(f"Comprehensive visualizations saved:")
    print(f"  - Timeline: {output_path}")
    print(f"  - Details: {base_path}_details.html")
    # print(f"  - Distribution: {base_path}_distribution.html")  # Commented out 