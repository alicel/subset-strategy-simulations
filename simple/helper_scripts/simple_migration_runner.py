#!/usr/bin/env python3
"""
Simple Migration Runner Script

This script:
1. Performs AWS SSO login
2. Parses configuration file and sets environment variables
3. Loops through migration IDs and for each:
   - Executes Go command with environment variables
   - Downloads results from S3 bucket
   - Runs simple simulation with downloaded data
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

class SimpleMigrationRunner:
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
        
        # Define the mapping from config keys to environment variable names
        # Simple simulation has different environment variables than tiered
        env_var_mapping = {
            'cloud_provider': 'CLOUD_PROVIDER',
            'access_key': 'MIGRATION_ACCESS_KEY',
            'secret_key': 'MIGRATION_SECRET_KEY',
            'bucket': 'MIGRATION_BUCKET',
            'region': 'MIGRATION_REGION',
            'storage_endpoint': 'MIGRATION_STORAGE_ENDPOINT',
            'log_level': 'MIGRATION_LOG_LEVEL',
            'subset_calculation_label': 'MIGRATION_SUBSET_CALCULATION_LABEL',
            'subset_calculation_strategy': 'MIGRATION_SUBSET_CALCULATION_STRATEGY',
            'enable_subset_size_cap': 'MIGRATION_ENABLE_SUBSET_SIZE_CAP',
            'enable_subset_num_sstable_cap': 'MIGRATION_ENABLE_SUBSET_NUM_SSTABLE_CAP',
            'max_num_sstables_per_subset': 'MIGRATION_MAX_NUM_SSTABLES_PER_SUBSET',
            # Simple simulation specific parameters
            'max_workers': 'MIGRATION_MAX_WORKERS',
            'worker_processing_time_unit': 'MIGRATION_WORKER_PROCESSING_TIME_UNIT'
        }
        
        # Set environment variables from config
        for config_key, env_var_name in env_var_mapping.items():
            if config_key in migration_config:
                value = migration_config[config_key]
                # Handle boolean values for environment variables
                if isinstance(value, bool):
                    env_value = "true" if value else "false"
                else:
                    env_value = str(value)
                
                os.environ[env_var_name] = env_value
                # Redact sensitive values in logs
                if any(s in env_var_name.upper() for s in ["KEY", "SECRET", "ACCESS"]):
                    logger.info(f"Set {env_var_name}=***REDACTED***")
                else:
                    logger.info(f"Set {env_var_name}={env_value}")
        
        # Always set MIGRATION_ID to the current migration ID
        os.environ['MIGRATION_ID'] = migration_id
        logger.info(f"Set MIGRATION_ID={migration_id}")
    
    def execute_go_command(self, migration_id: str) -> bool:
        """Execute the Go command for a specific migration ID."""
        logger.info(f"Executing Go command for migration ID: {migration_id}")
        
        go_command = self.config.get('go_command', {})
        command = go_command.get('executable', './mba/migration-bucket-accessor')
        args = go_command.get('args', ['calc_subsets'])
        
        # Replace placeholders in arguments with migration_id
        processed_args = []
        for arg in args:
            if isinstance(arg, str):
                processed_args.append(arg.replace('{migration_id}', migration_id))
            else:
                processed_args.append(str(arg))
        
        full_command = [command] + processed_args
        
        # Run from the TieredStrategySimulation root directory so that ./mba/migration-bucket-accessor path works
        script_dir = os.path.dirname(os.path.abspath(__file__))  # helper_scripts directory
        simple_dir = os.path.dirname(script_dir)  # simple directory
        parent_dir = os.path.dirname(simple_dir)  # TieredStrategySimulation directory
        
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
    
    def download_from_s3(self, migration_id: str, execution_name: str = None) -> Optional[str]:
        """Download results from S3 for a specific migration ID."""
        logger.info(f"Downloading results from S3 for migration ID: {migration_id}")
        
        if not self.s3_client:
            self.s3_client = boto3.client('s3')
        
        # S3 path structure for simple simulation
        s3_config = self.config.get('s3', {})
        migration_config = self.config.get('migration', {})
        subset_calculation_label = migration_config.get('subset_calculation_label', 'generalCalculation')
        
        # Use subset_calculation_label in path template if not explicitly provided
        default_path_template = f'{{migration_id}}/metadata/subsets/{subset_calculation_label}/'
        path_template = s3_config.get('path_template', default_path_template)
        
        # Replace placeholders in path template
        s3_path = path_template.replace('{migration_id}', migration_id)
        s3_path = s3_path.replace('{subset_calculation_label}', subset_calculation_label)
        
        # Local directory: simple/helper_scripts/downloadedSubsetDefinitions/
        # Use absolute path to ensure we always download to simple directory
        script_dir = os.path.dirname(os.path.abspath(__file__))  # helper_scripts directory
        simple_dir = os.path.dirname(script_dir)  # simple directory
        
        # Organize by execution name if provided
        if execution_name:
            # Use new structure: simple/helper_scripts/downloadedSubsetDefinitions/{execution_name}/
            base_download_dir = os.path.join(simple_dir, "helper_scripts", "downloadedSubsetDefinitions", execution_name)
        else:
            # Fallback to old structure for backward compatibility
            base_download_dir = os.path.join(simple_dir, "helper_scripts", "downloadedSubsetDefinitions")
        
        os.makedirs(base_download_dir, exist_ok=True)
        
        # Full local path maintaining S3 structure
        local_dir = os.path.join(base_download_dir, migration_id)
        os.makedirs(local_dir, exist_ok=True)
        
        try:
            # List objects in the S3 bucket with the specified prefix
            response = self.s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix=s3_path)
            
            if 'Contents' not in response:
                logger.warning(f"No objects found in S3 bucket {self.bucket_name} with prefix {s3_path}")
                return None
            
            downloaded_count = 0
            for obj in response['Contents']:
                # Extract relative path from S3 key by removing the migration_id prefix
                s3_key = obj['Key']
                
                # Create local file path maintaining directory structure
                local_file_path = os.path.join(base_download_dir, s3_key)
                
                # Create directories if they don't exist
                os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
                
                # Download the file
                self.s3_client.download_file(self.bucket_name, s3_key, local_file_path)
                downloaded_count += 1
                logger.debug(f"Downloaded: {s3_key} -> {local_file_path}")
            
            logger.info(f"Downloaded {downloaded_count} files from S3")
            # local_dir is now an absolute path like "/path/to/simple/helper_scripts/downloadedSubsetDefinitions/{execution_name}/mig100"
            # We need to return the path relative to simple directory where simulation runs
            return_path = os.path.relpath(local_dir, simple_dir)
            # Return path relative to simple directory where simulation runs
            return return_path
        except Exception as e:
            logger.error(f"Failed to download from S3: {e}")
            return None
    
    def run_simulation(self, migration_id: str, download_dir: str) -> tuple[bool, dict]:
        """Run the simple simulation on downloaded data."""
        logger.info(f"Running simple simulation for migration ID: {migration_id}")
        
        # Get simulation configuration
        sim_config = self.config.get('simulation', {})
        
        # Prepare simulation command
        simulation_script = "run_simple_simulation.py"
        input_directory = download_dir
        
        # Build command arguments
        command_args = [sys.executable, simulation_script, input_directory]
        
        # Worker configuration
        worker_config = sim_config.get('worker_config', {})
        if 'max_workers' in worker_config:
            command_args.extend(['--max-workers', str(worker_config['max_workers'])])
        
        # Output configuration
        output_config = sim_config.get('output', {})
        output_name = output_config.get('output_name', f'simple_migration_{migration_id}')
        command_args.extend(['--output-name', output_name])
        
        # Create output directories with execution name and migration ID - relative to simple directory
        # Get execution name from the run method (passed down through process_migration_range)
        execution_name = getattr(self, '_current_execution_name', 'default_execution')
        output_dir_template = output_config.get('output_dir', 'output/{execution_name}/{migration_id}/plots')
        plots_dir = output_dir_template.replace('{migration_id}', migration_id).replace('{execution_name}', execution_name)
        
        # Also create migration_exec_results directory for config files
        migration_exec_dir = plots_dir.replace('/plots', '/migration_exec_results')
        
        # Create both directories using absolute paths
        # Get simple directory path
        script_dir = os.path.dirname(os.path.abspath(__file__))  # helper_scripts directory
        simple_dir = os.path.dirname(script_dir)  # simple directory
        
        full_plots_dir = os.path.join(simple_dir, plots_dir)
        full_exec_dir = os.path.join(simple_dir, migration_exec_dir)
        os.makedirs(full_plots_dir, exist_ok=True)
        os.makedirs(full_exec_dir, exist_ok=True)
        
        command_args.extend(['--output-dir', plots_dir])
        command_args.extend(['--config-dir', migration_exec_dir])
        
        # Visualization options
        viz_config = sim_config.get('visualization', {})
        if viz_config.get('no_plotly', False):
            command_args.append('--no-plotly')
        if viz_config.get('plotly_comprehensive', False):
            command_args.append('--plotly-comprehensive')
        
        # Add any custom arguments
        custom_args = sim_config.get('custom_args', [])
        command_args.extend(custom_args)
        
        # Run simulation from the simple simulation directory
        simple_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        
        logger.info(f"Running simulation command: {' '.join(command_args)}")
        logger.info(f"Working directory: {simple_dir}")
        
        try:
            result = subprocess.run(
                command_args,
                check=True,
                capture_output=True,
                text=True,
                cwd=simple_dir
            )
            
            logger.info(f"Simple simulation completed successfully for {migration_id}")
            if result.stdout:
                logger.info(f"Simulation output: {result.stdout}")
            
            # Collect output file paths
            output_files = {}
            output_files['html'] = os.path.join(simple_dir, plots_dir, f"{output_name}.html")
            output_files['config'] = os.path.join(simple_dir, migration_exec_dir, f"config_{output_name}.txt")
            
            # Plotly files if generated (go in plots directory)
            if not viz_config.get('no_plotly', False):
                plotly_base = os.path.join(simple_dir, plots_dir, f"{output_name}_plotly")
                output_files['plotly_timeline'] = f"{plotly_base}.html"
                
                if viz_config.get('plotly_comprehensive', False):
                    output_files['plotly_details'] = f"{plotly_base}_details.html"
                    # output_files['plotly_distribution'] = f"{plotly_base}_distribution.html"  # Disabled per request
            
            return True, output_files
            
        except subprocess.CalledProcessError as e:
            logger.error(f"Simple simulation failed for {migration_id}: {e}")
            logger.error(f"Error output: {e.stderr}")
            if e.stdout:
                logger.error(f"Standard output: {e.stdout}")
            return False, {}
    
    def process_migration_range(self, start_id: int, end_id: int, prefix: str = "mig", execution_name: str = None):
        """Process a range of migration IDs."""
        successful = []
        failed = []
        migration_results = {}
        
        for migration_id_num in range(start_id, end_id + 1):
            migration_id = f"{prefix}{migration_id_num:03d}"
            logger.info(f"Processing migration ID: {migration_id}")
            
            try:
                # Step 1: Set environment variables for this migration
                self.set_environment_variables(migration_id)
                
                # Step 2: Execute Go command
                if not self.execute_go_command(migration_id):
                    logger.error(f"Go command failed for {migration_id}, skipping...")
                    failed.append(migration_id)
                    continue
                
                # Step 3: Download from S3
                download_dir = self.download_from_s3(migration_id, execution_name)
                if not download_dir:
                    logger.error(f"S3 download failed for {migration_id}, skipping...")
                    failed.append(migration_id)
                    continue
                
                # Step 4: Run simulation
                sim_success, output_files = self.run_simulation(migration_id, download_dir)
                if not sim_success:
                    logger.error(f"Simulation failed for {migration_id}")
                    failed.append(migration_id)
                    continue
                
                successful.append(migration_id)
                migration_results[migration_id] = output_files
                logger.info(f"Successfully processed {migration_id}")
                
            except Exception as e:
                logger.error(f"Error processing {migration_id}: {e}")
                failed.append(migration_id)
        
        logger.info(f"Processing complete. Successful: {len(successful)}, Failed: {len(failed)}")
        if failed:
            logger.warning(f"Failed migrations: {failed}")
        
        return successful, failed, migration_results
    
    def collect_execution_report_data(self, migration_results: dict) -> dict:
        """Collect data for execution report."""
        execution_data = {
            'total_migrations': len(migration_results),
            'migrations': migration_results,
            'summary': {
                'successful': len(migration_results),
                'failed': 0,  # Assuming only successful ones are in migration_results
                'total': len(migration_results)
            }
        }
        return execution_data
    
    def generate_execution_report(self, execution_data: dict, output_path: str):
        """Generate a text execution report."""
        logger.info(f"Generating execution report: {output_path}")
        
        with open(output_path, 'w') as f:
            f.write("Simple Migration Runner - Execution Report\n")
            f.write("=" * 50 + "\n\n")
            
            # Summary
            summary = execution_data['summary']
            f.write(f"Total Migrations Processed: {summary['total']}\n")
            f.write(f"Successful: {summary['successful']}\n")
            f.write(f"Failed: {summary['failed']}\n\n")
            
            # Details
            f.write("Migration Details:\n")
            f.write("-" * 30 + "\n")
            
            for migration_id, output_files in execution_data['migrations'].items():
                f.write(f"\nMigration ID: {migration_id}\n")
                f.write(f"Status: SUCCESS\n")
                f.write("Output Files:\n")
                
                for file_type, file_path in output_files.items():
                    f.write(f"  {file_type}: {file_path}\n")
        
        logger.info(f"Execution report generated: {output_path}")
    
    def generate_execution_report_csv(self, execution_data: dict, output_path: str):
        """Generate a CSV execution report."""
        logger.info(f"Generating CSV execution report: {output_path}")
        
        with open(output_path, 'w', newline='') as csvfile:
            fieldnames = ['migration_id', 'status', 'html_output', 'config_output', 'plotly_timeline', 'plotly_details', 'plotly_distribution']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            writer.writeheader()
            
            for migration_id, output_files in execution_data['migrations'].items():
                row = {
                    'migration_id': migration_id,
                    'status': 'SUCCESS',
                    'html_output': output_files.get('html', ''),
                    'config_output': output_files.get('config', ''),
                    'plotly_timeline': output_files.get('plotly_timeline', ''),
                    'plotly_details': output_files.get('plotly_details', ''),
                    'plotly_distribution': output_files.get('plotly_distribution', '')
                }
                writer.writerow(row)
        
        logger.info(f"CSV execution report generated: {output_path}")
    
    def print_results_summary(self, migration_results: dict):
        """Print a summary of results to the console."""
        print("\n" + "="*80)
        print("SIMPLE MIGRATION RUNNER - EXECUTION SUMMARY")
        print("="*80)
        
        print(f"\nSuccessfully processed {len(migration_results)} migration(s):")
        
        for migration_id, output_files in migration_results.items():
            print(f"\nSimulation executed for {migration_id}. Results available at:")
            
            # Always print HTML results path with file:// prefix
            if 'html' in output_files:
                print(f"  file://{output_files['html']}")
            
            # Print plotly visualizations if available
            if 'plotly_timeline' in output_files:
                print(f"  file://{output_files['plotly_timeline']}")
            
            if 'plotly_details' in output_files:
                print(f"  file://{output_files['plotly_details']}")
                
            # Distribution visualization has been disabled per request
            # if 'plotly_distribution' in output_files:
            #     print(f"  file://{output_files['plotly_distribution']}")
        
        print("\n" + "="*80)
    
    def run(self, start_id: int, end_id: int, prefix: str = "mig", execution_name: str = None, output_dir: str = None):
        """Main execution method."""
        logger.info("Starting Simple Migration Runner")
        
        # Store execution name for use in subdirectory creation
        self._current_execution_name = execution_name
        
        # Set default output directory with execution name using absolute path
        if output_dir is None:
            script_dir = os.path.dirname(os.path.abspath(__file__))  # helper_scripts directory
            simple_dir = os.path.dirname(script_dir)  # simple directory
            output_dir = os.path.join(simple_dir, "output", execution_name, "exec_reports")
        
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
        successful, failed, migration_results = self.process_migration_range(start_id, end_id, prefix, execution_name)
        
        # Step 4: Collect execution report data
        execution_data = self.collect_execution_report_data(migration_results)
        
        # Step 5: Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Step 6: Generate execution report with custom naming
        report_txt_path = os.path.join(output_dir, f"simple_execution_report_{execution_name}.txt")
        self.generate_execution_report(execution_data, report_txt_path)
        
        # Step 7: Generate execution report CSV with custom naming
        report_csv_path = os.path.join(output_dir, f"simple_execution_report_{execution_name}.csv")
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
            "secret_key": "YOUR_SECRET_KEY_HERE",
            "bucket": "your-bucket-name",
            "region": "eu-west-1",
            "storage_endpoint": "https://s3.eu-west-1.amazonaws.com",
            "log_level": "DEBUG",
            "subset_calculation_label": "generalCalculation",
            "subset_calculation_strategy": "simple",
            "enable_subset_size_cap": True,
            "enable_subset_num_sstable_cap": True,
            "max_num_sstables_per_subset": 250,
            # Simple simulation specific parameters
            "max_workers": 4,
            "worker_processing_time_unit": 1000
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
                "max_workers": 4
            },
            "visualization": {
                "no_plotly": False,
                "plotly_comprehensive": True
            },
            "output": {
                "output_name": "simple_migration_simulation",
                "output_dir": "output/{execution_name}/{migration_id}/plots"
            },
            "custom_args": []
        }
    }
    
    # Create the config file in the same directory as this script (helper_scripts)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_file_path = os.path.join(script_dir, "simple_migration_config_sample.yaml")
    
    with open(config_file_path, "w") as f:
        yaml.dump(sample_config, f, default_flow_style=False, indent=2)
    
    print(f"Sample configuration file created: {config_file_path}")
    print("Please customize it according to your needs.")
    print()
    print("The following environment variables will be set from the migration config:")
    print("  CLOUD_PROVIDER")
    print("  MIGRATION_ACCESS_KEY")
    print("  MIGRATION_SECRET_KEY")
    print("  MIGRATION_BUCKET")
    print("  MIGRATION_REGION")
    print("  MIGRATION_STORAGE_ENDPOINT")
    print("  MIGRATION_LOG_LEVEL")
    print("  MIGRATION_SUBSET_CALCULATION_LABEL")
    print("  MIGRATION_SUBSET_CALCULATION_STRATEGY")
    print("  MIGRATION_ENABLE_SUBSET_SIZE_CAP")
    print("  MIGRATION_ENABLE_SUBSET_NUM_SSTABLE_CAP")
    print("  MIGRATION_MAX_NUM_SSTABLES_PER_SUBSET")
    print("  MIGRATION_ID (automatically set to current migration ID)")
    print("  MIGRATION_MAX_WORKERS")
    print("  MIGRATION_WORKER_PROCESSING_TIME_UNIT")
    print()
    print("Simulation options configured:")
    print("  Worker Configuration: max_workers")
    print("  Visualization Options: plotly generation and comprehensive mode")
    print("  Output Options: naming and directory structure")
    print()
    print("Simple simulation runs with single-threaded workers up to max_workers concurrency.")

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
    
    # Try to find simple_migration_config.yaml in current directory
    current_dir = os.getcwd()
    config_file = os.path.join(current_dir, 'simple_migration_config.yaml')
    if os.path.exists(config_file):
        return config_file
    
    # Try to find simple_migration_config.yaml in helper_scripts directory
    helper_scripts_dir = os.path.dirname(os.path.abspath(__file__))
    config_file = os.path.join(helper_scripts_dir, 'simple_migration_config.yaml')
    if os.path.exists(config_file):
        return config_file
    
    raise FileNotFoundError("No configuration file found. Please create simple_migration_config.yaml in the current directory or helper_scripts directory.")

def main():
    parser = argparse.ArgumentParser(description='Run simple migration processing for a range of IDs')
    parser.add_argument('--start-id', type=int, help='Starting migration ID')
    parser.add_argument('--end-id', type=int, help='Ending migration ID')
    parser.add_argument('--execution-name', type=str, help='Name for this execution (used in report filenames)')
    parser.add_argument('--prefix', type=str, default='mig', help='Prefix for migration IDs (default: mig)')
    parser.add_argument('--output-dir', type=str, default=None, help='Output directory for execution reports (default: simple/output/<execution_name>/exec_reports)')
    parser.add_argument('--config-path', type=str, help='Path to configuration file (default: simple_migration_config.yaml)')
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
        runner = SimpleMigrationRunner(config_path, args.bucket)
        runner.run(args.start_id, args.end_id, args.prefix, args.execution_name, args.output_dir)
    except Exception as e:
        logger.error(f"Error: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main() 