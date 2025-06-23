from core.worker_simulation import SimpleConfig, SimpleSimulation
from core.file_processor import parse_input_directory
from visualization.plotly_visualization import save_timeline_visualization, save_comprehensive_visualization
import argparse
import sys
from datetime import datetime
import os

def save_configuration(args, config, config_file, total_time, num_files):
    """Save the simulation configuration to a file."""
    with open(config_file, 'w', encoding='utf-8') as f:
        f.write("Simple Database Migration Simulation Configuration\n")
        f.write("=" * 50 + "\n\n")
        
        # Timestamp
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        # Input configuration
        f.write("Input Configuration:\n")
        f.write("-" * 20 + "\n")
        f.write(f"Input directory: {args.directory}\n")
        f.write(f"Files processed: {num_files}\n\n")
        
        # Worker configuration
        f.write("Worker Configuration:\n")
        f.write("-" * 21 + "\n")
        f.write(f"Max concurrent workers: {config.max_workers}\n\n")
        
        # Output configuration
        f.write("Output Configuration:\n")
        f.write("-" * 21 + "\n")
        f.write(f"Output directory: {args.output_dir}\n")
        f.write(f"Output base name: {args.output_name}\n\n")
        
        # Simulation results
        f.write("Simulation Results:\n")
        f.write("-" * 19 + "\n")
        f.write(f"Total simulation time: {total_time:.2f} time units\n\n")
        
        # Command line used (reconstructed)
        f.write("Equivalent Command Line:\n")
        f.write("-" * 25 + "\n")
        cmd_parts = [f"python run_simple_simulation.py {args.directory}"]
        
        # Add non-default arguments
        if args.max_workers != 4:
            cmd_parts.append(f"--max-workers {args.max_workers}")
        if args.output_name != 'simple_simulation_results':
            cmd_parts.append(f"--output-name {args.output_name}")
        if args.output_dir != 'output_files':
            cmd_parts.append(f"--output-dir {args.output_dir}")
        if args.config_dir and args.config_dir != args.output_dir:
            cmd_parts.append(f"--config-dir {args.config_dir}")
        if args.no_plotly:
            cmd_parts.append("--no-plotly")
        if args.plotly_comprehensive:
            cmd_parts.append("--plotly-comprehensive")
        
        # Format command line nicely
        if len(" ".join(cmd_parts)) > 80:
            f.write(cmd_parts[0] + " \\\n")
            for part in cmd_parts[1:]:
                f.write(f"    {part} \\\n")
            # Remove the last backslash
            f.seek(f.tell() - 3)
            f.write("\n")
        else:
            f.write(" ".join(cmd_parts) + "\n")
    
    print(f"Configuration saved to {config_file}")

def save_results_to_file(simulation: SimpleSimulation, output_file: str):
    """Save detailed simulation results to an HTML file."""
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("""<!DOCTYPE html>
<html>
<head>
    <title>Simple Simulation Results</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 40px; }
        h1, h2 { color: #333; }
        .config { background-color: #f5f5f5; padding: 15px; border-radius: 5px; }
        .summary { background-color: #e8f4f8; padding: 15px; border-radius: 5px; }
        
        table { border-collapse: collapse; width: 100%; }
        th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
        th { background-color: #f2f2f2; }
        .worker-details { margin-top: 20px; }
    </style>
</head>
<body>
""")
        
        f.write("<h1>Simple Simulation Results</h1>\n")
        f.write(f"<p>Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>\n")
        
        # Configuration section
        f.write("<h2>Configuration</h2>\n")
        f.write('<div class="config">\n')
        f.write(f"<p><strong>Max concurrent workers:</strong> {simulation.config.max_workers}</p>\n")
        f.write(f"<p><strong>Total simulation time:</strong> {simulation.current_time:.2f} time units</p>\n")
        f.write(f"<p><strong>Workers processed:</strong> {len(simulation.completed_workers)}</p>\n")
        f.write("</div>\n")
        
        # Summary statistics
        if simulation.completed_workers:
            completion_times = [w.completion_time for w in simulation.completed_workers if w.completion_time]
            if completion_times:
                avg_completion = sum(completion_times) / len(completion_times)
                f.write("<h2>Summary Statistics</h2>\n")
                f.write('<div class="summary">\n')
                f.write(f"<p><strong>Average worker completion time:</strong> {avg_completion:.2f}</p>\n")
                f.write(f"<p><strong>Earliest completion:</strong> {min(completion_times):.2f}</p>\n")
                f.write(f"<p><strong>Latest completion:</strong> {max(completion_times):.2f}</p>\n")
                f.write("</div>\n")
        
        # Timeline visualization removed - using plotly visualizations instead
        
        # Worker details table
        if simulation.completed_workers:
            f.write("<h2>Worker Details</h2>\n")
            f.write('<div class="worker-details">\n')
            f.write("<table>\n")
            f.write("<tr><th>Worker ID</th><th>Subset ID</th><th>Items Processed</th><th>Total Work Size</th><th>Completion Time</th></tr>\n")
            
            sorted_workers = sorted(simulation.completed_workers, key=lambda w: w.worker_id)
            for worker in sorted_workers:
                if worker.simulator and worker.file:
                    total_work = sum(item.size for item in worker.simulator.processed_items)
                    f.write(f"<tr>")
                    f.write(f"<td>{worker.worker_id}</td>")
                    f.write(f"<td>{worker.subset_id}</td>")
                    f.write(f"<td>{len(worker.simulator.processed_items)}</td>")
                    f.write(f"<td>{total_work:,}</td>")
                    f.write(f"<td>{worker.completion_time:.2f}</td>")
                    f.write(f"</tr>\n")
                else:
                    f.write(f"<tr>")
                    f.write(f"<td>{worker.worker_id}</td>")
                    f.write(f"<td>{worker.subset_id}</td>")
                    f.write(f"<td>0</td>")
                    f.write(f"<td>0</td>")
                    f.write(f"<td>{worker.completion_time or 0:.2f}</td>")
                    f.write(f"</tr>\n")
            
            f.write("</table>\n")
            f.write("</div>\n")
        
        f.write("</body>\n</html>\n")
    
    print(f"Detailed results saved to {output_file}")

def main():
    """Main entry point for the simple simulation."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Run simple simulation on subset files')
    parser.add_argument('directory', help='Directory containing subset files to process')
    parser.add_argument('--max-workers', type=int, default=4, 
                       help='Maximum number of concurrent workers (default: 4)')
    parser.add_argument('--output-name', type=str, default='simple_simulation_results', 
                       help='Base name for output files (default: simple_simulation_results)')
    parser.add_argument('--output-dir', type=str, default='output_files', 
                       help='Directory to store output files (default: output_files)')
    parser.add_argument('--config-dir', type=str, default=None,
                       help='Directory to store config file (default: same as output-dir)')
    parser.add_argument('--no-plotly', action='store_true',
                       help='Skip interactive Plotly visualizations')
    parser.add_argument('--plotly-comprehensive', action='store_true',
                       help='Generate comprehensive Plotly visualizations (timeline, details, distribution)')
    
    args = parser.parse_args()
    
    # Validate arguments
    if args.max_workers <= 0:
        print("Error: max-workers must be positive", file=sys.stderr)
        sys.exit(1)
    
    # Configure the simulation
    config = SimpleConfig(max_workers=args.max_workers)
    
    # Parse input files from directory
    try:
        files = parse_input_directory(args.directory)
    except Exception as e:
        print(f"Error scanning directory: {e}", file=sys.stderr)
        sys.exit(1)
    
    if not files:
        print("No valid files found to process.", file=sys.stderr)
        sys.exit(1)
    
    # Print configuration
    print("\nSimple Simulation Configuration:")
    print("=" * 50)
    print(f"Input directory: {args.directory}")
    print(f"Max concurrent workers: {config.max_workers}")
    print(f"Files to process: {len(files)}")
    
    # Create and run simulation
    print("\nStarting simulation...")
    simulation = SimpleSimulation(config)
    total_time = simulation.run_simulation(files)
    
    # Print results to console
    simulation.print_results()
    
    # Create output directory if it doesn't exist
    output_dir = args.output_dir
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Create config directory if specified and different from output_dir
    config_dir = args.config_dir if args.config_dir else output_dir
    if config_dir != output_dir and not os.path.exists(config_dir):
        os.makedirs(config_dir)
    
    # Construct full paths for output files
    output_base = args.output_name
    output_file = os.path.join(output_dir, f"{output_base}.html")
    config_file = os.path.join(config_dir, f"config_{output_base}.txt")
    
    # Save configuration to file
    save_configuration(args, config, config_file, total_time, len(files))
    
    # Save detailed results to HTML file  
    save_results_to_file(simulation, output_file)
    
    # Export CSV data for comparison analysis
    csv_base = os.path.join(config_dir, output_base)
    simulation.export_data_to_csv(csv_base)
    
    # Generate Plotly visualizations if requested
    if not args.no_plotly:
        try:
            workers = simulation.completed_workers
            if workers:
                plotly_output = os.path.join(output_dir, f"{output_base}_plotly.html")
                
                if args.plotly_comprehensive:
                    # Generate comprehensive visualizations
                    save_comprehensive_visualization(workers, plotly_output)
                else:
                    # Generate basic timeline visualization
                    save_timeline_visualization(workers, plotly_output)
                    
        except ImportError:
            print("Warning: Plotly not available. Install with: pip install plotly>=5.18.0")
        except Exception as e:
            print(f"Warning: Could not generate Plotly visualizations: {e}")
    
    # Plotly visualizations are the primary output - no console timeline needed
    
    print(f"\nSimulation completed successfully!")
    print(f"Total time: {total_time:.2f} time units")
    print(f"Results saved to: {output_file}")

if __name__ == "__main__":
    main() 