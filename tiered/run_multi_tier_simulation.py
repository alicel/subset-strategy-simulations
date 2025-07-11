from core.worker_simulation import WorkerConfig, TierConfig, MultiTierSimulation, ExecutionMode
from core.file_processor import parse_input_directory
import argparse
import sys
from datetime import datetime

def save_configuration(args, config, config_file, total_time, num_files):
    """Save the simulation configuration to a file."""
    with open(config_file, 'w', encoding='utf-8') as f:
        f.write("Multi-Tier Database Migration Simulation Configuration\n")
        f.write("=" * 55 + "\n\n")
        
        # Timestamp
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        # Input configuration
        f.write("Input Configuration:\n")
        f.write("-" * 20 + "\n")
        f.write(f"Input directory: {args.directory}\n")
        f.write(f"Files processed: {num_files}\n\n")
        
        # Worker tier configuration
        f.write("Worker Tier Configuration:\n")
        f.write("-" * 26 + "\n")
        f.write(f"SMALL tier:\n")
        f.write(f"  Threads per worker: {config.small.num_threads}\n")
        f.write(f"  Max concurrent workers: {config.small.max_workers}\n\n")
        
        f.write(f"MEDIUM tier:\n")
        f.write(f"  Threads per worker: {config.medium.num_threads}\n")
        f.write(f"  Max concurrent workers: {config.medium.max_workers}\n\n")
        
        f.write(f"LARGE tier:\n")
        f.write(f"  Threads per worker: {config.large.num_threads}\n")
        f.write(f"  Max concurrent workers: {config.large.max_workers}\n\n")
        
        # Analysis configuration
        f.write("Analysis Configuration:\n")
        f.write("-" * 22 + "\n")
        execution_mode_desc = {
            'concurrent': 'Concurrent (all tiers parallel)',
            'sequential': 'Sequential (LARGE->MEDIUM->SMALL)',
            'round_robin': f'Round-robin (max {args.max_concurrent_workers} total workers)'
        }
        f.write(f"Execution mode: {execution_mode_desc.get(args.execution_mode, args.execution_mode)}\n")
        f.write(f"Straggler threshold: {args.straggler_threshold:.1f}% above average\n")
        f.write(f"Straggler analysis: {'Disabled' if args.no_stragglers else 'Enabled'}\n")
        f.write(f"CSV export: {'Disabled' if args.no_csv else 'Enabled'}\n")
        f.write(f"Detailed visualization: {'Disabled' if args.summary_only else 'Enabled'}\n")
        if not args.summary_only:
            if args.detailed_per_worker:
                f.write(f"Detailed visualization: Per-worker mode (forced)\n")
            elif args.detailed_page_size > 0:
                f.write(f"Detailed pagination: {args.detailed_page_size} workers per page\n")
            else:
                f.write(f"Detailed pagination: Disabled (single file)\n")
        f.write("\n")
        
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
        cmd_parts = [f"python run_multi_tier_simulation.py {args.directory}"]
        
        # Add non-default arguments
        if args.small_threads != 6:
            cmd_parts.append(f"--small-threads {args.small_threads}")
        if args.medium_threads != 4:
            cmd_parts.append(f"--medium-threads {args.medium_threads}")
        if args.large_threads != 1:
            cmd_parts.append(f"--large-threads {args.large_threads}")
        if args.small_max_workers != 4:
            cmd_parts.append(f"--small-max-workers {args.small_max_workers}")
        if args.medium_max_workers != 6:
            cmd_parts.append(f"--medium-max-workers {args.medium_max_workers}")
        if args.large_max_workers != 10:
            cmd_parts.append(f"--large-max-workers {args.large_max_workers}")
        if args.straggler_threshold != 20.0:
            cmd_parts.append(f"--straggler-threshold {args.straggler_threshold}")
        if args.summary_only:
            cmd_parts.append("--summary-only")
        if args.no_stragglers:
            cmd_parts.append("--no-stragglers")
        if args.no_csv:
            cmd_parts.append("--no-csv")
        if args.output_name != 'simulation_results':
            cmd_parts.append(f"--output-name {args.output_name}")
        if args.output_dir != 'output_files':
            cmd_parts.append(f"--output-dir {args.output_dir}")
        if args.detailed_page_size != 30:
            cmd_parts.append(f"--detailed-page-size {args.detailed_page_size}")
        if args.detailed_per_worker:
            cmd_parts.append("--detailed-per-worker")
        if args.execution_mode != 'concurrent':
            cmd_parts.append(f"--execution-mode {args.execution_mode}")
        if args.execution_mode == 'round_robin' and args.max_concurrent_workers:
            cmd_parts.append(f"--max-concurrent-workers {args.max_concurrent_workers}")
        
        # Format command line nicely (break long lines)
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

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Run multi-tier simulation on subset files')
    parser.add_argument('directory', help='Directory containing subset files to process')
    parser.add_argument('--small-threads', type=int, default=6, help='Number of threads for SMALL tier workers')
    parser.add_argument('--medium-threads', type=int, default=4, help='Number of threads for MEDIUM tier workers')
    parser.add_argument('--large-threads', type=int, default=1, help='Number of threads for LARGE tier workers')
    parser.add_argument('--small-max-workers', type=int, default=4, help='Maximum concurrent SMALL tier workers')
    parser.add_argument('--medium-max-workers', type=int, default=6, help='Maximum concurrent MEDIUM tier workers')
    parser.add_argument('--large-max-workers', type=int, default=10, help='Maximum concurrent LARGE tier workers')
    parser.add_argument('--straggler-threshold', type=float, default=20.0, 
                       help='Percentage threshold above average completion time to identify straggler threads (default: 20.0)')
    parser.add_argument('--summary-only', action='store_true', help='Show only summary and global timeline, skip detailed views')
    parser.add_argument('--no-stragglers', action='store_true', help='Skip straggler analysis and reporting')
    parser.add_argument('--no-csv', action='store_true', help='Skip CSV data export for automated analysis')
    parser.add_argument('--output-name', type=str, default='simulation_results', 
                       help='Base name for output files (default: simulation_results)')
    parser.add_argument('--output-dir', type=str, default='output_files', 
                       help='Directory to store output files (default: output_files)')
    parser.add_argument('--detailed-page-size', type=int, default=30,
                       help='Maximum number of workers per page in detailed visualization (default: 30, set to 0 to disable pagination)')
    parser.add_argument('--detailed-per-worker', action='store_true',
                       help='Generate per-worker detailed visualization files (recommended for large migrations, auto-detected if not specified)')
    parser.add_argument('--execution-mode', choices=['concurrent', 'sequential', 'round_robin'], default='concurrent',
                       help='Worker execution mode: concurrent (all tiers parallel), sequential (LARGE->MEDIUM->SMALL), or round_robin (global limit with round-robin allocation)')
    parser.add_argument('--max-concurrent-workers', type=int, default=None,
                       help='Maximum number of concurrent workers across all tiers (required for round-robin mode)')
    
    args = parser.parse_args()
    
    # Validate arguments
    if args.execution_mode == 'round_robin' and args.max_concurrent_workers is None:
        parser.error("--max-concurrent-workers is required when using round-robin execution mode")
    if args.max_concurrent_workers is not None and args.max_concurrent_workers <= 0:
        parser.error("--max-concurrent-workers must be positive")
    
    # Configure the tiers
    config = WorkerConfig(
        small=TierConfig(num_threads=args.small_threads, max_workers=args.small_max_workers),
        medium=TierConfig(num_threads=args.medium_threads, max_workers=args.medium_max_workers),
        large=TierConfig(num_threads=args.large_threads, max_workers=args.large_max_workers)
    )
    
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
    print("\nSimulation Configuration:")
    print("=" * 50)
    
    # Print execution mode
    execution_mode_desc = {
        'concurrent': 'Concurrent (all tiers parallel)',
        'sequential': 'Sequential (LARGE->MEDIUM->SMALL)',
        'round_robin': f'Round-robin (max {args.max_concurrent_workers} total workers)'
    }
    print(f"Execution mode: {execution_mode_desc.get(args.execution_mode, args.execution_mode)}")
    
    for tier in ['SMALL', 'MEDIUM', 'LARGE']:
        threads = getattr(args, f'{tier.lower()}_threads')
        max_workers = getattr(args, f'{tier.lower()}_max_workers')
        print(f"{tier} tier: {threads} threads per worker, max {max_workers} workers")
    
    if not args.no_stragglers:
        print(f"Straggler threshold: {args.straggler_threshold:.1f}% above average")
    
    # Create and run simulation
    print("\nStarting simulation...")
    execution_mode = ExecutionMode(args.execution_mode)
    simulation = MultiTierSimulation(
        config, 
        straggler_threshold_percent=args.straggler_threshold,
        execution_mode=execution_mode,
        max_concurrent_workers=args.max_concurrent_workers
    )
    total_time = simulation.run_simulation(files)
    
    # Print results
    print(f"\nSimulation completed in {total_time:.2f} time units")
    
    # Create output directory if it doesn't exist
    import os
    output_dir = args.output_dir
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Construct full paths for output files
    output_base = args.output_name
    output_file = os.path.join(output_dir, f"{output_base}.html")
    csv_base = os.path.join(output_dir, output_base)
    config_file = os.path.join(output_dir, f"config_{output_base}.txt")
    
    # Save configuration to file
    save_configuration(args, config, config_file, total_time, len(files))
    
    simulation.print_results(
        output_file=output_file,
        show_details=not args.summary_only, 
        show_stragglers=not args.no_stragglers, 
        export_csv=not args.no_csv,
        csv_base=csv_base,
        detailed_page_size=args.detailed_page_size if args.detailed_page_size > 0 else None,
        detailed_per_worker=args.detailed_per_worker if args.detailed_per_worker else None
    )
    
    # Export execution report data for helper script consumption
    execution_report_path = os.path.join(output_dir, f"{output_base}_execution_report.json")
    simulation.export_execution_report_data(execution_report_path)

if __name__ == "__main__":
    main() 