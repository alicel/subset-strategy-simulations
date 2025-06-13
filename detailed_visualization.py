from typing import List, Dict
import plotly.express as px
import plotly.figure_factory as ff
import pandas as pd
from visualization_base import Worker, WorkerTier
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os
import math

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
    
    # Sort workers consistently - LARGE first, then MEDIUM, then SMALL, with ascending worker IDs within each tier
    # Reverse for visual display so that W0 appears at top and higher worker IDs appear below
    tier_order = {'LARGE': 0, 'MEDIUM': 1, 'SMALL': 2}  # Lower numbers sort first
    for worker in reversed(sorted(workers, key=lambda w: (tier_order[w.tier.value], w.worker_id))):
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
            # Find the actual thread data if it exists
            actual_thread = None
            if worker.threads:
                for thread in worker.threads:
                    if thread.thread_id == thread_id:
                        actual_thread = thread
                        break
            
            # Create enhanced thread label with totals
            if actual_thread and actual_thread.processed_items:
                total_sstables = len(actual_thread.processed_items)
                total_data_bytes = sum(item.size for item in actual_thread.processed_items)
                total_data_gb = total_data_bytes / (1024*1024*1024)
                compact_label = f"W{worker.worker_id}-T{thread_id} ({total_sstables} SSTs, {total_data_gb:.1f}GB)"
            else:
                # Idle thread
                compact_label = f"W{worker.worker_id}-T{thread_id} (IDLE)"
            
            # Track the thread label in the correct order
            thread_labels.append(compact_label)
            
            if actual_thread and actual_thread.processed_items:
                # This thread did work - show its tasks
                is_straggler_thread = thread_id in worker.straggler_threads
                
                # Calculate thread totals for enhanced display
                total_data_bytes = sum(item.size for item in actual_thread.processed_items)
                total_sstables = len(actual_thread.processed_items)
                total_data_mb = total_data_bytes / (1024*1024)
                total_data_gb = total_data_bytes / (1024*1024*1024)
                
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
                            "<b>THREAD TOTALS:</b>",
                            "  Total SSTables: %{customdata[7]}",
                            "  Total Data: %{customdata[8]} bytes [%{customdata[9]:.2f} MB | %{customdata[10]:.2f} GB]",
                            "",
                            "<b>THIS TASK:</b>",
                            "  Task: %{customdata[0]}",
                            "  Start: %{base:.2f}",
                            "  End: %{x:.2f}",
                            "  Size: %{customdata[1]} [%{customdata[5]:.2f} MB | %{customdata[6]:.2f} GB]"
                        ]),
                        customdata=[[
                            item.key,
                            item.size,
                            worker_name,
                            f"Thread {thread_id}",
                            " (STRAGGLER)" if is_straggler_thread else "",
                            item.size / (1024*1024),  # MB
                            item.size / (1024*1024*1024),  # GB
                            total_sstables,  # Thread total SSTables
                            total_data_bytes,  # Thread total bytes
                            total_data_mb,  # Thread total MB
                            total_data_gb   # Thread total GB
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
            'text': "Detailed Thread Timelines<br><sup>Thread-level execution details with total SSTable count and data processed per thread</sup>",
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
            l=250,  # Increased left margin for enhanced thread labels with totals
            r=20,
            t=150,  # Increased top margin from 120 to 150 for better title space
            b=30,
            pad=4
        ),
        plot_bgcolor='rgba(240, 245, 250, 0.95)'
    )
    
    return thread_fig

def create_navigation_html(current_page: int, total_pages: int, base_filename: str) -> str:
    """Create HTML navigation links for pagination with first page as _detailed.html."""
    nav_html = '<div style="text-align: center; padding: 20px; font-family: Arial, sans-serif; background-color: #f8f9fa; border: 1px solid #dee2e6; border-radius: 5px; margin: 10px 0;">'
    nav_html += f'<h3 style="color: #495057; margin: 0 0 15px 0;">Page {current_page} of {total_pages}</h3>'
    nav_html += '<div style="display: inline-block;">'
    
    def get_page_filename(page_num):
        """Get the correct filename for a given page number."""
        if page_num == 1:
            return f"{base_filename}.html"  # First page is always _detailed.html
        else:
            return f"{base_filename}_page{page_num}.html"
    
    # Previous button
    if current_page > 1:
        prev_filename = get_page_filename(current_page - 1)
        nav_html += f'<a href="{os.path.basename(prev_filename)}" style="display: inline-block; padding: 8px 16px; margin: 0 5px; background-color: #007bff; color: white; text-decoration: none; border-radius: 4px; font-weight: bold;">« Previous</a>'
    else:
        nav_html += '<span style="display: inline-block; padding: 8px 16px; margin: 0 5px; background-color: #6c757d; color: white; border-radius: 4px; font-weight: bold;">« Previous</span>'
    
    # Page numbers (show a few around current page)
    start_page = max(1, current_page - 2)
    end_page = min(total_pages, current_page + 2)
    
    if start_page > 1:
        page1_filename = get_page_filename(1)
        nav_html += f'<a href="{os.path.basename(page1_filename)}" style="display: inline-block; padding: 8px 12px; margin: 0 2px; background-color: #e9ecef; color: #495057; text-decoration: none; border-radius: 4px;">1</a>'
        if start_page > 2:
            nav_html += '<span style="display: inline-block; padding: 8px 12px; margin: 0 2px; color: #6c757d;">...</span>'
    
    for page in range(start_page, end_page + 1):
        if page == current_page:
            nav_html += f'<span style="display: inline-block; padding: 8px 12px; margin: 0 2px; background-color: #007bff; color: white; border-radius: 4px; font-weight: bold;">{page}</span>'
        else:
            page_filename = get_page_filename(page)
            nav_html += f'<a href="{os.path.basename(page_filename)}" style="display: inline-block; padding: 8px 12px; margin: 0 2px; background-color: #e9ecef; color: #495057; text-decoration: none; border-radius: 4px;">{page}</a>'
    
    if end_page < total_pages:
        if end_page < total_pages - 1:
            nav_html += '<span style="display: inline-block; padding: 8px 12px; margin: 0 2px; color: #6c757d;">...</span>'
        last_page_filename = get_page_filename(total_pages)
        nav_html += f'<a href="{os.path.basename(last_page_filename)}" style="display: inline-block; padding: 8px 12px; margin: 0 2px; background-color: #e9ecef; color: #495057; text-decoration: none; border-radius: 4px;">{total_pages}</a>'
    
    # Next button
    if current_page < total_pages:
        next_filename = get_page_filename(current_page + 1)
        nav_html += f'<a href="{os.path.basename(next_filename)}" style="display: inline-block; padding: 8px 16px; margin: 0 5px; background-color: #007bff; color: white; text-decoration: none; border-radius: 4px; font-weight: bold;">Next »</a>'
    else:
        nav_html += '<span style="display: inline-block; padding: 8px 16px; margin: 0 5px; background-color: #6c757d; color: white; border-radius: 4px; font-weight: bold;">Next »</span>'
    
    nav_html += '</div></div>'
    return nav_html

def save_detailed_visualization_paginated(workers: List[Worker], output_path: str = "detailed_results.html", workers_per_page: int = 30):
    """Save the detailed thread visualization to paginated HTML files."""
    if not workers:
        print("No worker data available for detailed visualization")
        return []
    
    # Sort workers consistently - LARGE first, then MEDIUM, then SMALL, with ascending worker IDs within each tier
    tier_order = {'LARGE': 0, 'MEDIUM': 1, 'SMALL': 2}  # Lower numbers sort first
    sorted_workers = sorted(workers, key=lambda w: (tier_order[w.tier.value], w.worker_id))
    
    # Calculate pagination
    total_workers = len(sorted_workers)
    total_pages = math.ceil(total_workers / workers_per_page)
    
    # If we have fewer workers than the page size, just create a single page
    if total_pages <= 1:
        thread_fig = create_detailed_visualization(sorted_workers)
        if thread_fig is not None:
            thread_fig.write_html(output_path)
            print(f"Detailed visualization saved to {output_path}")
            return [output_path]
        else:
            print("No thread data available for detailed visualization")
            return []
    
    # Generate base filename for pagination
    base_path = output_path.replace('.html', '')
    generated_files = []
    
    print(f"Generating {total_pages} pages for detailed visualization ({workers_per_page} workers per page)")
    
    for page_num in range(1, total_pages + 1):
        # Calculate worker subset for this page
        start_idx = (page_num - 1) * workers_per_page
        end_idx = min(start_idx + workers_per_page, total_workers)
        page_workers = sorted_workers[start_idx:end_idx]
        
        # Create visualization for this page
        thread_fig = create_detailed_visualization(page_workers)
        if thread_fig is None:
            continue
            
        # Update title to include page information
        title_text = f"Detailed Thread Timelines - Page {page_num} of {total_pages}<br><sup>Workers {start_idx + 1}-{end_idx} of {total_workers} (Thread-level execution with SSTable count and data totals)</sup>"
        thread_fig.update_layout(title={'text': title_text})
        
        # Generate page filename - first page is always _detailed.html
        if page_num == 1:
            page_filename = f"{base_path}.html"  # First page: _detailed.html
        else:
            page_filename = f"{base_path}_page{page_num}.html"  # Other pages: _detailed_page2.html, etc.
        
        # Save the plot to get the initial HTML
        thread_fig.write_html(page_filename)
        
        # Read the generated HTML and add navigation
        with open(page_filename, 'r', encoding='utf-8') as f:
            html_content = f.read()
        
        # Insert navigation at the beginning and end of the body
        nav_html = create_navigation_html(page_num, total_pages, base_path)
        
        # Find the body tag and insert navigation
        body_start = html_content.find('<body>')
        if body_start != -1:
            body_start += len('<body>')
            html_content = html_content[:body_start] + nav_html + html_content[body_start:]
        
        # Find the end of body and insert navigation
        body_end = html_content.rfind('</body>')
        if body_end != -1:
            html_content = html_content[:body_end] + nav_html + html_content[body_end:]
        
        # Write the modified HTML back
        with open(page_filename, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        generated_files.append(page_filename)
    
    if generated_files:
        print(f"Detailed visualization saved to {len(generated_files)} pages:")
        for i, filename in enumerate(generated_files, 1):
            print(f"  Page {i}: {filename}")
        print(f"Start browsing from: {generated_files[0]}")
    
    return generated_files

def save_detailed_visualization(workers: List[Worker], output_path: str = "detailed_results.html", workers_per_page: int = None):
    """Save the detailed thread visualization to HTML file(s).
    
    Args:
        workers: List of Worker objects to visualize
        output_path: Path for the output HTML file
        workers_per_page: If provided, split into paginated files with this many workers per page.
                         If None or 0, create a single file with all workers.
    """
    if workers_per_page is None or workers_per_page <= 0:
        # Original behavior - single file
        thread_fig = create_detailed_visualization(workers)
        if thread_fig is not None:
            thread_fig.write_html(output_path)
            print(f"Detailed visualization saved to {output_path}")
        else:
            print("No thread data available for detailed visualization")
    else:
        # New paginated behavior
        save_detailed_visualization_paginated(workers, output_path, workers_per_page) 