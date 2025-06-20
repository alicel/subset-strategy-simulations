#!/usr/bin/env python3
"""
Migration Runner Script

This script:
1. Performs AWS SSO login
2. Parses configuration file and sets environment variables
3. Loops through migration IDs and for each:
   - Executes Go command with environment variables
   - Downloads results from S3 bucket
   - Runs simulation with downloaded data
"""

import os
import sys
import subprocess
import json
import yaml
import boto3
from pathlib import Path
from typing import Dict, List, Optional
import argparse
import logging
import csv

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MigrationRunner:
    def __init__(self, config_path: str, bucket_name: str = None):
        self.config_path = config_path
        self.config = {}
        self.bucket_name = bucket_name
        self.s3_client = None
        
    def check_sso_session(self, profile: str = "astra-conn") -> bool:
        """Check if there is an active AWS SSO session.
        
        Args:
            profile: AWS profile name to check
            
        Returns:
            bool: True if there is an active session, False otherwise
        """
        try:
            # Try to get caller identity which will fail if session is expired
            result = subprocess.run(
                ["aws", "sts", "get-caller-identity", f"--profile={profile}"],
                check=True,
                capture_output=True,
                text=True
            )
            return True
        except subprocess.CalledProcessError:
            return False
    
    def aws_sso_login(self, profile: str = "astra-conn"):
        """Perform AWS SSO login with the specified profile if no active session exists."""
        if self.check_sso_session(profile):
            logger.info(f"AWS SSO session is already active for profile: {profile}")
            return True
        
        logger.info(f"No active AWS SSO session found. Performing login with profile: {profile}")
        try:
            result = subprocess.run(
                ["aws", "sso", "login", f"--profile={profile}"],
                check=True,
                capture_output=True,
                text=True
            )
            logger.info("AWS SSO login successful")
            return True
        except subprocess.CalledProcessError as e:
            logger.error(f"AWS SSO login failed: {e}")
            logger.error(f"Error output: {e.stderr}")
            return False
    
    def parse_config_file(self):
        """Parse the configuration file and extract values."""
        logger.info(f"Parsing configuration file: {self.config_path}")
        
        if not os.path.exists(self.config_path):
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")
        
        # Support both JSON and YAML formats
        with open(self.config_path, 'r') as f:
            if self.config_path.endswith('.yaml') or self.config_path.endswith('.yml'):
                self.config = yaml.safe_load(f)
            else:
                self.config = json.load(f)
        
        logger.info("Configuration loaded successfully")
        
        # Set bucket name from config if not provided via command line
        if not self.bucket_name and 'migration' in self.config:
            self.bucket_name = self.config['migration'].get('bucket')
        
        return self.config
    
    def set_environment_variables(self, migration_id: str):
        """Set environment variables from configuration for a specific migration ID."""
        logger.info(f"Setting environment variables for migration: {migration_id}")
        
        # Get migration configuration
        migration_config = self.config.get('migration', {})
        
        # Define the mapping from config keys to environment variable names (with MIGRATION_ prefix)
        env_var_mapping = {
            'cloud_provider': 'CLOUD_PROVIDER',  # This one doesn't get MIGRATION_ prefix
            'access_key': 'MIGRATION_ACCESS_KEY',
            'bucket': 'MIGRATION_BUCKET',
            'log_level': 'MIGRATION_LOG_LEVEL',
            'medium_tier_max_sstable_size_gb': 'MIGRATION_MEDIUM_TIER_MAX_SSTABLE_SIZE_GB',
            'medium_tier_worker_num_threads': 'MIGRATION_MEDIUM_TIER_WORKER_NUM_THREADS',
            'optimize_packing_medium_subsets': 'MIGRATION_OPTIMIZE_PACKING_MEDIUM_SUBSETS',
            'region': 'MIGRATION_REGION',
            'secret_key': 'MIGRATION_SECRET_KEY',
            'small_tier_max_sstable_size_gb': 'MIGRATION_SMALL_TIER_MAX_SSTABLE_SIZE_GB',
            'small_tier_thread_subset_max_size_floor_gb': 'MIGRATION_SMALL_TIER_THREAD_SUBSET_MAX_SIZE_FLOOR_GB',
            'small_tier_worker_num_threads': 'MIGRATION_SMALL_TIER_WORKER_NUM_THREADS',
            'subset_calculation_label': 'MIGRATION_SUBSET_CALCULATION_LABEL',
            'subset_calculation_strategy': 'MIGRATION_SUBSET_CALCULATION_STRATEGY',
            'max_num_sstables_per_subset': 'MIGRATION_MAX_NUM_SSTABLES_PER_SUBSET'
        }
        
        # Set environment variables from config
        for config_key, env_var_name in env_var_mapping.items():
            if config_key in migration_config:
                os.environ[env_var_name] = str(migration_config[config_key])
                # Redact sensitive values in logs
                if any(s in env_var_name.upper() for s in ["KEY", "SECRET", "ACCESS"]):
                    logger.info(f"Set {env_var_name}=***REDACTED***")
                else:
                    logger.info(f"Set {env_var_name}={migration_config[config_key]}")
        
        # Always set MIGRATION_ID to the current migration ID
        os.environ['MIGRATION_ID'] = migration_id
        logger.info(f"Set MIGRATION_ID={migration_id}")
    
    def execute_go_command(self, migration_id: str) -> bool:
        """Execute the Go command for a specific migration ID."""
        logger.info(f"Executing Go command for migration ID: {migration_id}")
        
        go_command = self.config.get('go_command', {})
        command = go_command.get('executable', './mba/migration-bucket-accessor')
        args = go_command.get('args', ['calc_subsets'])
        
        # Replace placeholders in arguments with migration_id (though not needed for this specific command)
        processed_args = []
        for arg in args:
            if isinstance(arg, str):
                processed_args.append(arg.replace('{migration_id}', migration_id))
            else:
                processed_args.append(str(arg))
        
        full_command = [command] + processed_args
        
        # Run from the TieredStrategySimulation root directory so that ./mba/migration-bucket-accessor path works
        parent_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        
        logger.info(f"Executing command: {' '.join(full_command)}")
        logger.info(f"Working directory: {parent_dir}")
        
        try:
            result = subprocess.run(
                full_command,
                check=True,
                capture_output=True,
                text=True,
                env=os.environ.copy(),  # Pass all environment variables
                cwd=parent_dir  # Run from parent directory
            )
            logger.info(f"Go command completed successfully for {migration_id}")
            logger.debug(f"Go command output: {result.stdout}")
            if result.stdout:
                logger.info(f"Go command stdout: {result.stdout}")
            return True
        except subprocess.CalledProcessError as e:
            logger.error(f"Go command failed for {migration_id}: {e}")
            logger.error(f"Error output: {e.stderr}")
            if e.stdout:
                logger.error(f"Standard output: {e.stdout}")
            return False
    
    def download_from_s3(self, migration_id: str) -> Optional[str]:
        """Download results from S3 for a specific migration ID."""
        logger.info(f"Downloading results from S3 for migration ID: {migration_id}")
        
        if not self.s3_client:
            self.s3_client = boto3.client('s3')
        
        # S3 path structure: <migrationId>/metadata/subsets/<subset_calculation_label>/
        s3_config = self.config.get('s3', {})
        migration_config = self.config.get('migration', {})
        subset_calculation_label = migration_config.get('subset_calculation_label', 'mytieredcalc')
        
        # Use subset_calculation_label in path template if not explicitly provided
        default_path_template = f'{{migration_id}}/metadata/subsets/{subset_calculation_label}/'
        path_template = s3_config.get('path_template', default_path_template)
        
        # Replace placeholders in path template
        s3_path = path_template.replace('{migration_id}', migration_id)
        s3_path = s3_path.replace('{subset_calculation_label}', subset_calculation_label)
        
        # Local directory: data/downloadedSubsetDefinitions/
        # Preserve full S3 path structure starting from migration ID
        base_download_dir = "../data/downloadedSubsetDefinitions"
        os.makedirs(base_download_dir, exist_ok=True)
        
        logger.info(f"Downloading from S3 path: s3://{self.bucket_name}/{s3_path}")
        logger.info(f"Local base directory: {base_download_dir}")
        
        try:
            # List objects in the S3 path
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=s3_path
            )
            
            if 'Contents' not in response:
                logger.warning(f"No objects found in S3 path: s3://{self.bucket_name}/{s3_path}")
                return None
            
            downloaded_files = []
            for obj in response['Contents']:
                s3_key = obj['Key']
                
                # Preserve the full S3 path structure starting from migration ID
                # Example: s3_key = "mig100/metadata/subsets/mytieredcalc/file.json"
                # local_file_path = "downloadedSubsetDefinitions/mig100/metadata/subsets/mytieredcalc/file.json"
                local_file_path = os.path.join(base_download_dir, s3_key)
                
                # Create directory if needed
                local_dir = os.path.dirname(local_file_path)
                os.makedirs(local_dir, exist_ok=True)
                
                # Download the file
                self.s3_client.download_file(self.bucket_name, s3_key, local_file_path)
                downloaded_files.append(local_file_path)
                logger.info(f"Downloaded: {s3_key} -> {local_file_path}")
            
            logger.info(f"Downloaded {len(downloaded_files)} files for {migration_id}")
            
            # Return the migration-specific directory for the simulation
            migration_specific_dir = os.path.join(base_download_dir, migration_id)
            return migration_specific_dir
        
        except Exception as e:
            logger.error(f"Failed to download from S3 for {migration_id}: {e}")
            return None
    
    def run_simulation(self, migration_id: str, download_dir: str) -> tuple[bool, dict]:
        """Run the simulation using downloaded data.
        
        Returns:
            tuple: (success: bool, output_files: dict) where output_files contains paths to generated files
        """
        logger.info(f"Running simulation for migration ID: {migration_id}")
        
        simulation_config = self.config.get('simulation', {})
        
        # Build the simulation command
        # The input directory should be the full path to the downloaded subset definitions
        input_directory = f"data/downloadedSubsetDefinitions/{migration_id}"
        command = ['python', 'run_multi_tier_simulation.py', input_directory]
        
        # Worker Configuration
        worker_config = simulation_config.get('worker_config', {})
        if 'small_threads' in worker_config:
            command.extend(['--small-threads', str(worker_config['small_threads'])])
        if 'medium_threads' in worker_config:
            command.extend(['--medium-threads', str(worker_config['medium_threads'])])
        if 'large_threads' in worker_config:
            command.extend(['--large-threads', str(worker_config['large_threads'])])
        if 'small_max_workers' in worker_config:
            command.extend(['--small-max-workers', str(worker_config['small_max_workers'])])
        if 'medium_max_workers' in worker_config:
            command.extend(['--medium-max-workers', str(worker_config['medium_max_workers'])])
        if 'large_max_workers' in worker_config:
            command.extend(['--large-max-workers', str(worker_config['large_max_workers'])])
        
        # Analysis Options
        analysis_config = simulation_config.get('analysis', {})
        if 'straggler_threshold' in analysis_config:
            command.extend(['--straggler-threshold', str(analysis_config['straggler_threshold'])])
        if analysis_config.get('summary_only', False):
            command.append('--summary-only')
        if analysis_config.get('no_stragglers', False):
            command.append('--no-stragglers')
        if analysis_config.get('sequential_execution', False):
            command.append('--sequential-execution')
        
        # Output Options
        output_config = simulation_config.get('output', {})
        output_name = output_config.get('output_name', 'migration_simulation')
        if output_name:
            # Append migration ID to output name if not already present
            if not output_name.endswith(migration_id):
                output_name = f"{output_name}_{migration_id}"
            command.extend(['--output-name', output_name])
        
        # Define output directory relative to the TieredStrategySimulation directory
        output_dir = f"data/simulation_outputs/{migration_id}"
        command.extend(['--output-dir', output_dir])
        
        # Run from the tiered directory
        parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        # Ensure we're in the tiered directory where run_multi_tier_simulation.py is located
        if not os.path.exists(os.path.join(parent_dir, 'run_multi_tier_simulation.py')):
            parent_dir = os.path.join(parent_dir, 'tiered')
        
        logger.info(f"Executing simulation command: {' '.join(command)}")
        logger.info(f"Working directory: {parent_dir}")
        
        try:
            result = subprocess.run(
                command,
                check=True,
                capture_output=True,
                text=True,
                cwd=parent_dir
            )
            logger.info(f"Simulation completed successfully for {migration_id}")
            
            # Calculate output file paths
            output_files = {}
            if output_dir and output_name:
                abs_output_dir = os.path.abspath(os.path.join(parent_dir, output_dir))
                output_files['timeline'] = os.path.join(abs_output_dir, f"{output_name}_timeline.html")
                output_files['detailed'] = os.path.join(abs_output_dir, f"{output_name}_detailed.html")
                
                logger.debug(f"Expected timeline file: {output_files['timeline']}")
                logger.debug(f"Expected detailed file: {output_files['detailed']}")
                
                # Check for paginated detailed files
                detailed_pages = []
                page_num = 1
                while True:
                    if page_num == 1:
                        page_file = output_files['detailed']
                    else:
                        page_file = os.path.join(abs_output_dir, f"{output_name}_detailed_page{page_num}.html")
                    
                    if os.path.exists(page_file):
                        detailed_pages.append(page_file)
                        page_num += 1
                    else:
                        break
                
                output_files['detailed_pages'] = detailed_pages
                output_files['total_pages'] = len(detailed_pages)
            
            return True, output_files
        except subprocess.CalledProcessError as e:
            logger.error(f"Simulation failed for {migration_id}: {e}")
            logger.error(f"Error output: {e.stderr}")
            if e.stdout:
                logger.error(f"Standard output: {e.stdout}")
            return False, {}
    
    def process_migration_range(self, start_id: int, end_id: int, prefix: str = "mig"):
        """Process a range of migration IDs."""
        logger.info(f"Processing migration range: {prefix}{start_id} to {prefix}{end_id}")
        
        successful_migrations = []
        failed_migrations = []
        migration_results = {}  # Track output files for each successful migration
        
        for migration_num in range(start_id, end_id + 1):
            migration_id = f"{prefix}{migration_num}"
            logger.info(f"Processing migration: {migration_id}")
            
            try:
                # Set environment variables for this specific migration
                self.set_environment_variables(migration_id)
                
                # Execute Go command
                if not self.execute_go_command(migration_id):
                    failed_migrations.append(migration_id)
                    continue
                
                # Download from S3
                download_dir = self.download_from_s3(migration_id)
                if not download_dir:
                    failed_migrations.append(migration_id)
                    continue
                
                # Run simulation
                success, output_files = self.run_simulation(migration_id, download_dir)
                if not success:
                    failed_migrations.append(migration_id)
                    continue
                
                successful_migrations.append(migration_id)
                migration_results[migration_id] = output_files
                logger.info(f"Successfully processed migration: {migration_id}")
                
            except Exception as e:
                logger.error(f"Unexpected error processing {migration_id}: {e}")
                failed_migrations.append(migration_id)
        
        # Summary
        logger.info(f"Migration processing complete:")
        logger.info(f"  Successful: {len(successful_migrations)} - {successful_migrations}")
        logger.info(f"  Failed: {len(failed_migrations)} - {failed_migrations}")
        
        return successful_migrations, failed_migrations, migration_results
    
    def collect_execution_report_data(self, migration_results: dict) -> dict:
        """Collect execution report data from all successful migrations."""
        import json
        import os
        
        execution_data = {
            "migration_config": None,
            "migrations": {}
        }
        
        # Extract migration config from the first successful migration
        if self.config.get('migration'):
            execution_data["migration_config"] = {
                key: self.config['migration'].get(key)
                for key in [
                    'medium_tier_max_sstable_size_gb',
                    'medium_tier_worker_num_threads', 
                    'optimize_packing_medium_subsets',
                    'small_tier_max_sstable_size_gb',
                    'small_tier_thread_subset_max_size_floor_gb',
                    'small_tier_worker_num_threads',
                    'max_num_sstables_per_subset'
                ]
                if key in self.config['migration']
            }
        
        # Collect data from each migration's execution report JSON
        for migration_id, output_files in migration_results.items():
            # Look for execution report JSON files
            json_files = []
            
            try:
                # Check the simulation output directory (from helper_scripts directory)
                sim_output_dir = f"../data/simulation_outputs/{migration_id}"
                if os.path.exists(sim_output_dir):
                    for file in os.listdir(sim_output_dir):
                        if file.endswith('_execution_report.json'):
                            json_path = os.path.join(sim_output_dir, file)
                            json_files.append(json_path)
                            logger.info(f"Found execution report JSON: {json_path}")
                
            except Exception as e:
                logger.warning(f"Error searching for execution report JSON files for {migration_id}: {e}")
            
            # Load data from found JSON files
            if json_files:
                try:
                    # Use the first JSON file found (should only be one per migration)
                    with open(json_files[0], 'r', encoding='utf-8') as f:
                        migration_data = json.load(f)
                        execution_data["migrations"][migration_id] = migration_data
                except Exception as e:
                    logger.warning(f"Failed to read execution report for {migration_id}: {e}")
            else:
                logger.warning(f"No execution report JSON found for {migration_id}")
        
        return execution_data
    
    def generate_execution_report(self, execution_data: dict, output_path: str):
        """Generate the overall execution report."""
        from datetime import datetime
        
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write("="*80 + "\n")
                f.write("OVERALL MIGRATION EXECUTION REPORT\n")
                f.write("="*80 + "\n\n")
                
                # Timestamp
                f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
                
                # Migration Configuration Section
                f.write("MIGRATION CONFIGURATION\n")
                f.write("-"*50 + "\n")
                migration_config = execution_data.get("migration_config", {})
                if migration_config:
                    for key, value in migration_config.items():
                        if value is not None:
                            f.write(f"{key}: {value}\n")
                else:
                    f.write("Migration configuration not available\n")
                f.write("\n")
                
                # Simulation Configuration Section
                f.write("SIMULATION CONFIGURATION\n")
                f.write("-"*50 + "\n")
                # Extract simulation config from first migration
                first_migration_data = next(iter(execution_data["migrations"].values()), {})
                sim_config = first_migration_data.get("simulation_config", {})
                if sim_config:
                    f.write(f"small_threads: {sim_config.get('small_threads')}\n")
                    f.write(f"medium_threads: {sim_config.get('medium_threads')}\n")
                    f.write(f"large_threads: {sim_config.get('large_threads')}\n")
                    f.write(f"small_max_workers: {sim_config.get('small_max_workers')}\n")
                    f.write(f"medium_max_workers: {sim_config.get('medium_max_workers')}\n")
                    f.write(f"large_max_workers: {sim_config.get('large_max_workers')}\n")
                else:
                    f.write("Simulation configuration not available\n")
                f.write("\n")
                
                # Per-Migration Analysis
                f.write("PER-MIGRATION ANALYSIS\n")
                f.write("-"*70 + "\n")
                
                for migration_id in sorted(execution_data["migrations"].keys()):
                    migration_data = execution_data["migrations"][migration_id]
                    by_tier = migration_data.get("by_tier", {})
                    total_time = migration_data.get("total_execution_time", 0)
                    
                    f.write(f"Migration ID: {migration_id}\n")
                    f.write(f"Total Execution Time: {total_time:.2f} time units\n")
                    f.write(f"{'Tier':<8} {'Total':<8} {'Straggler':<12} {'Idle':<8} {'Both':<8}\n")
                    f.write(f"{'':^8} {'Workers':<8} {'Workers':<12} {'Workers':<8} {'S+I':<8}\n")
                    f.write("-"*50 + "\n")
                    
                    for tier in ['SMALL', 'MEDIUM', 'LARGE']:
                        if tier in by_tier:
                            tier_data = by_tier[tier]
                            f.write(f"{tier:<8} {tier_data.get('total_workers', 0):<8} "
                                  f"{tier_data.get('straggler_workers', 0):<12} "
                                  f"{tier_data.get('workers_with_idle_threads', 0):<8} "
                                  f"{tier_data.get('workers_with_both_straggler_and_idle', 0):<8}\n")
                    f.write("\n")
                
                # Summary Statistics
                f.write("SUMMARY STATISTICS\n")
                f.write("-"*50 + "\n")
                
                # Aggregate totals across all migrations
                totals = {
                    'SMALL': {'total': 0, 'straggler': 0, 'idle': 0, 'both': 0},
                    'MEDIUM': {'total': 0, 'straggler': 0, 'idle': 0, 'both': 0},
                    'LARGE': {'total': 0, 'straggler': 0, 'idle': 0, 'both': 0}
                }
                
                for migration_data in execution_data["migrations"].values():
                    by_tier = migration_data.get("by_tier", {})
                    for tier in ['SMALL', 'MEDIUM', 'LARGE']:
                        if tier in by_tier:
                            tier_data = by_tier[tier]
                            totals[tier]['total'] += tier_data.get('total_workers', 0)
                            totals[tier]['straggler'] += tier_data.get('straggler_workers', 0)
                            totals[tier]['idle'] += tier_data.get('workers_with_idle_threads', 0)
                            totals[tier]['both'] += tier_data.get('workers_with_both_straggler_and_idle', 0)
                
                f.write(f"{'Tier':<8} {'Total':<8} {'Straggler':<12} {'Idle':<8} {'Both':<8} {'Straggler %':<12} {'Idle %':<8}\n")
                f.write(f"{'':^8} {'Workers':<8} {'Workers':<12} {'Workers':<8} {'S+I':<8} {'':^12} {'':^8}\n")
                f.write("-"*70 + "\n")
                
                for tier in ['SMALL', 'MEDIUM', 'LARGE']:
                    data = totals[tier]
                    total = data['total']
                    straggler_pct = (data['straggler'] / total * 100) if total > 0 else 0
                    idle_pct = (data['idle'] / total * 100) if total > 0 else 0
                    
                    f.write(f"{tier:<8} {total:<8} {data['straggler']:<12} {data['idle']:<8} "
                          f"{data['both']:<8} {straggler_pct:<11.1f}% {idle_pct:<7.1f}%\n")
                
                f.write("\n" + "="*80 + "\n")
            
            logger.info(f"Execution report generated: {output_path}")
            
        except Exception as e:
            logger.error(f"Failed to generate execution report: {e}")

    def generate_execution_report_csv(self, execution_data: dict, output_path: str):
        """Generate CSV export of per-migration analysis data."""
        try:
            with open(output_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                
                # Header row
                writer.writerow([
                    'Migration_ID', 'Total_Execution_Time', 'Tier', 'Total_Workers', 'Straggler_Workers', 
                    'Idle_Workers', 'Both_Straggler_And_Idle', 'Straggler_Percentage', 'Idle_Percentage'
                ])
                
                # Data rows for each migration and tier
                for migration_id in sorted(execution_data["migrations"].keys()):
                    migration_data = execution_data["migrations"][migration_id]
                    by_tier = migration_data.get("by_tier", {})
                    total_time = migration_data.get("total_execution_time", 0)
                    
                    for tier in ['SMALL', 'MEDIUM', 'LARGE']:
                        if tier in by_tier:
                            tier_data = by_tier[tier]
                            total = tier_data.get('total_workers', 0)
                            straggler = tier_data.get('straggler_workers', 0)
                            idle = tier_data.get('workers_with_idle_threads', 0)
                            both = tier_data.get('workers_with_both_straggler_and_idle', 0)
                            
                            # Calculate percentages
                            straggler_pct = (straggler / total * 100) if total > 0 else 0
                            idle_pct = (idle / total * 100) if total > 0 else 0
                            
                            writer.writerow([
                                migration_id, f"{total_time:.2f}", tier, total, straggler, idle, both,
                                f"{straggler_pct:.1f}", f"{idle_pct:.1f}"
                            ])
            
            logger.info(f"Execution report CSV generated: {output_path}")
            
        except Exception as e:
            logger.error(f"Failed to generate execution report CSV: {e}")

    def print_results_summary(self, migration_results: dict):
        """Print a summary of simulation results to stdout."""
        if not migration_results:
            print("\nNo successful simulations to report.")
            return
        
        print("\n" + "="*80)
        print("SIMULATION RESULTS SUMMARY")
        print("="*80)
        
        for migration_id, output_files in migration_results.items():
            print(f"\nSimulation executed for {migration_id}. Results available at:")
            
            # Always print timeline path with file:// prefix
            if 'timeline' in output_files:
                print(f"  file://{output_files['timeline']}")
            
            # Always print detailed path with file:// prefix
            if 'detailed_pages' in output_files and output_files['detailed_pages']:
                total_pages = output_files['total_pages']
                if total_pages == 1:
                    print(f"  file://{output_files['detailed_pages'][0]}")
                else:
                    print(f"  file://{output_files['detailed_pages'][0]} [{total_pages} total pages]")
            elif 'detailed' in output_files:
                # Fallback if detailed_pages is empty but detailed path exists
                print(f"  file://{output_files['detailed']}")
        
        print("\n" + "="*80)
    
    def run(self, start_id: int, end_id: int, prefix: str = "mig", execution_name: str = None, output_dir: str = "exec_output"):
        """Main execution method."""
        logger.info("Starting Migration Runner")
        
        # Step 1: AWS SSO Login
        if not self.aws_sso_login():
            logger.error("AWS SSO login failed. Exiting.")
            return False
        
        # Step 2: Parse configuration
        try:
            self.parse_config_file()
        except Exception as e:
            logger.error(f"Failed to parse configuration: {e}")
            return False
        
        # Step 3: Process migration range (environment variables are set per migration)
        successful, failed, migration_results = self.process_migration_range(start_id, end_id, prefix)
        
        # Step 4: Collect execution report data
        execution_data = self.collect_execution_report_data(migration_results)
        
        # Step 5: Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Step 6: Generate execution report with custom naming
        report_txt_path = os.path.join(output_dir, f"execution_report_{execution_name}.txt")
        self.generate_execution_report(execution_data, report_txt_path)
        
        # Step 7: Generate execution report CSV with custom naming
        report_csv_path = os.path.join(output_dir, f"execution_report_{execution_name}.csv")
        self.generate_execution_report_csv(execution_data, report_csv_path)
        
        # Step 8: Print results summary
        self.print_results_summary(migration_results)
        
        return len(failed) == 0

def create_sample_config():
    """Create a sample configuration file for reference."""
    sample_config = {
        "migration": {
            "cloud_provider": "AWS",
            "access_key": "YOUR_ACCESS_KEY_HERE",
            "bucket": "alice-sst-sdl-test",
            "log_level": "DEBUG",
            "medium_tier_max_sstable_size_gb": 50,
            "medium_tier_worker_num_threads": 6,
            "optimize_packing_medium_subsets": False,
            "region": "eu-west-1",
            "secret_key": "YOUR_SECRET_KEY_HERE",
            "small_tier_max_sstable_size_gb": 10,
            "small_tier_thread_subset_max_size_floor_gb": 2,
            "small_tier_worker_num_threads": 4,
            "subset_calculation_label": "mytieredcalc",
            "subset_calculation_strategy": "tiered",
            "max_num_sstables_per_subset": 250
        },
        "go_command": {
            "executable": "./mba/migration-bucket-accessor",
            "args": ["calc_subsets"]
        },
        "s3": {
            "path_template": "{migration_id}/metadata/subsets/{subset_calculation_label}/"
        },
        "simulation": {
            "worker_config": {
                "small_threads": 6,
                "medium_threads": 4,
                "large_threads": 1,
                "small_max_workers": 4,
                "medium_max_workers": 6,
                "large_max_workers": 10
            },
            "analysis": {
                "straggler_threshold": 20.0,
                "summary_only": False,
                "no_stragglers": False,
                "sequential_execution": False
            },
            "output": {
                "output_name": "tiered_migration_simulation",
                "output_dir": "tiered_simulation_outputs/{migration_id}",
                "no_csv": False,
                "detailed_page_size": 30
            },
            "custom_args": []
        }
    }
    
    # Create the config file in the same directory as this script (helper_scripts)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_file_path = os.path.join(script_dir, "migration_config_sample.yaml")
    
    with open(config_file_path, "w") as f:
        yaml.dump(sample_config, f, default_flow_style=False, indent=2)
    
    print(f"Sample configuration file created: {config_file_path}")
    print("Please customize it according to your needs.")
    print()
    print("The following environment variables will be set from the migration config:")
    print("  CLOUD_PROVIDER")
    print("  MIGRATION_ACCESS_KEY")
    print("  MIGRATION_BUCKET")
    print("  MIGRATION_ID (automatically set to current migration ID)")
    print("  MIGRATION_LOG_LEVEL")
    print("  MIGRATION_MEDIUM_TIER_MAX_SSTABLE_SIZE_GB")
    print("  MIGRATION_MEDIUM_TIER_WORKER_NUM_THREADS")
    print("  MIGRATION_OPTIMIZE_PACKING_MEDIUM_SUBSETS")
    print("  MIGRATION_REGION")
    print("  MIGRATION_SECRET_KEY")
    print("  MIGRATION_SMALL_TIER_MAX_SSTABLE_SIZE_GB")
    print("  MIGRATION_SMALL_TIER_THREAD_SUBSET_MAX_SIZE_FLOOR_GB")
    print("  MIGRATION_SMALL_TIER_WORKER_NUM_THREADS")
    print("  MIGRATION_SUBSET_CALCULATION_LABEL")
    print("  MIGRATION_SUBSET_CALCULATION_STRATEGY")
    print("  MIGRATION_MAX_NUM_SSTABLES_PER_SUBSET")
    print()
    print("Simulation options configured:")
    print("  Worker Configuration: threads per tier, max workers per tier")
    print("  Analysis Options: straggler threshold, summary mode, straggler analysis, execution mode")
    print("  Output Options: naming, directory structure, CSV export, pagination")
    print()
    print("Execution modes:")
    print("  sequential_execution: false (default) - All tiers process concurrently")
    print("  sequential_execution: true - Process tiers sequentially (LARGE->MEDIUM->SMALL)")

def find_config_file(config_path: str = None) -> str:
    """Find the configuration file in the current directory or helper_scripts directory.
    
    Args:
        config_path: Optional explicit path to config file
        
    Returns:
        Path to the found config file
        
    Raises:
        FileNotFoundError: If no config file is found
    """
    if config_path:
        if os.path.exists(config_path):
            return config_path
        raise FileNotFoundError(f"Specified configuration file not found: {config_path}")
    
    # Try to find migration_runner_config.yaml in current directory
    current_dir = os.getcwd()
    config_file = os.path.join(current_dir, 'migration_runner_config.yaml')
    if os.path.exists(config_file):
        return config_file
    
    # Try to find migration_runner_config.yaml in helper_scripts directory
    helper_scripts_dir = os.path.dirname(os.path.abspath(__file__))
    config_file = os.path.join(helper_scripts_dir, 'migration_runner_config.yaml')
    if os.path.exists(config_file):
        return config_file
    
    raise FileNotFoundError("No configuration file found. Please create migration_runner_config.yaml in the current directory or helper_scripts directory.")

def main():
    parser = argparse.ArgumentParser(description='Run migration processing for a range of IDs')
    parser.add_argument('--start-id', type=int, help='Starting migration ID')
    parser.add_argument('--end-id', type=int, help='Ending migration ID')
    parser.add_argument('--execution-name', type=str, help='Name for this execution (used in report filenames)')
    parser.add_argument('--prefix', type=str, default='mig', help='Prefix for migration IDs (default: mig)')
    parser.add_argument('--output-dir', type=str, default='exec_output', help='Output directory for execution reports (default: exec_output)')
    parser.add_argument('--config-path', type=str, help='Path to configuration file (default: migration_runner_config.yaml)')
    parser.add_argument('--bucket', type=str, help='S3 bucket name')
    parser.add_argument('--create-sample-config', action='store_true', help='Create a sample configuration file')
    args = parser.parse_args()
    
    if args.create_sample_config:
        create_sample_config()
        return
    
    # Check required arguments for normal execution
    if not args.start_id or not args.end_id or not args.execution_name:
        parser.error("--start-id, --end-id, and --execution-name are required for normal execution")
    
    try:
        # Find the config file
        config_path = find_config_file(args.config_path)
        logger.info(f"Using configuration file: {config_path}")
        
        # Initialize and run the migration processor
        runner = MigrationRunner(config_path, args.bucket)
        runner.run(args.start_id, args.end_id, args.prefix, args.execution_name, args.output_dir)
    except Exception as e:
        logger.error(f"Error: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main() 