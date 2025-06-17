from typing import List, Dict
import plotly.express as px
import plotly.graph_objects as go
from .visualization_base import Worker, WorkerTier

def create_timeline_visualization(workers: List[Worker]) -> go.Figure:
    """Create an interactive Plotly visualization of the worker timeline."""
    # Debug logging
    print("\nDebug: Creating visualization")
    print(f"Number of workers: {len(workers)}")
    
    # Sort workers by tier and start time
    workers = sorted(workers, key=lambda w: (w.tier.value, w.start_time))
    
    # Create figure
    fig = go.Figure()
    
    # Color map for tiers
    colors = {'S': '#00CC96', 'M': '#EF553B', 'L': '#636EFA'}
    
    # Group workers by tier
    tier_groups = {}
    for worker in workers:
        tier_label = {'SMALL': 'S', 'MEDIUM': 'M', 'LARGE': 'L'}[worker.tier.value]
        if tier_label not in tier_groups:
            tier_groups[tier_label] = []
        tier_groups[tier_label].append(worker)
    
    # Add bars for each tier
    for tier in ['S', 'M', 'L']:
        if tier in tier_groups:
            tier_workers = tier_groups[tier]
            for i, worker in enumerate(tier_workers):
                label = f"{tier} - Wrk {worker.worker_id}"
                # Add the bar with just the worker ID as text
                fig.add_trace(go.Bar(
                    x=[worker.completion_time - worker.start_time],
                    y=[i],  # Use numeric index for positioning
                    orientation='h',
                    name=tier,
                    base=[worker.start_time],
                    width=0.5,  # Slightly increased bar thickness
                    marker_color=colors[tier],
                    text=[str(worker.worker_id)],  # Just the worker ID
                    textposition='inside',
                    textfont=dict(
                        size=13,  # Increased font size
                        color='white',
                        family='Arial Black'
                    ),
                    textangle=0,  # Force horizontal text
                    insidetextanchor='middle',  # Center the text in the bar
                    hovertemplate="<br>".join([
                        "Worker: %{customdata[3]}",
                        "Start Time: %{base:.2f} units",
                        "End Time: %{x:.2f} units",
                        "Duration: %{customdata[0]:.2f} units",
                        "SSTable Count: %{customdata[1]}",
                        "Data Size: %{customdata[2]:.2f} GB",
                        "<extra>Click and drag to zoom. Double-click to reset.</extra>"
                    ]),
                    customdata=[[
                        worker.completion_time - worker.start_time,
                        worker.file.num_sstables if worker.file else 0,
                        worker.file.data_size / (1024*1024*1024) if worker.file else 0,
                        label
                    ]],
                    showlegend=i == 0  # Show legend only for first worker in tier
                ))
    
    # Update layout
    fig.update_layout(
        title={
            'text': "Multi-tier Simulation Results<br><sup>S/M/L tiers shown in green/red/blue. Numbers indicate worker IDs.</sup>",
            'x': 0.5,
            'xanchor': 'center',
            'y': 0.95
        },
        height=len(workers) * 16,  # Increased spacing between bars
        showlegend=True,
        legend_title="Tiers",
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
        hovermode="closest",
        barmode='overlay',
        bargap=0.3,  # Increased gap between bars
        bargroupgap=0.2,  # Increased gap between groups
        yaxis=dict(
            showticklabels=False,
            showgrid=True,
            gridwidth=1,
            gridcolor='rgba(211, 211, 211, 0.5)',
            side='left',
            range=[-1, len(workers)]
        ),
        xaxis=dict(
            showgrid=True,
            gridwidth=1,
            gridcolor='rgba(211, 211, 211, 0.5)',
            zeroline=False,
            rangeslider=dict(
                visible=True,
                thickness=0.10,
                bgcolor='#E2E2E2'
            )
        ),
        margin=dict(
            l=20,  # Reduced left margin since we don't have labels there anymore
            r=20,
            t=60,
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