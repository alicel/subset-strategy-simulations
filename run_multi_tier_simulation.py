from worker_simulation import WorkerConfig, TierConfig, MultiTierSimulation
from file_processor import parse_input_directory
import argparse
import sys

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
    
    args = parser.parse_args()
    
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
    for tier in ['SMALL', 'MEDIUM', 'LARGE']:
        threads = getattr(args, f'{tier.lower()}_threads')
        max_workers = getattr(args, f'{tier.lower()}_max_workers')
        print(f"{tier} tier: {threads} threads per worker, max {max_workers} workers")
    
    if not args.no_stragglers:
        print(f"Straggler threshold: {args.straggler_threshold:.1f}% above average")
    
    # Create and run simulation
    print("\nStarting simulation...")
    simulation = MultiTierSimulation(config, straggler_threshold_percent=args.straggler_threshold)
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
    
    simulation.print_results(
        output_file=output_file,
        show_details=not args.summary_only, 
        show_stragglers=not args.no_stragglers, 
        export_csv=not args.no_csv,
        csv_base=csv_base
    )

if __name__ == "__main__":
    main() 