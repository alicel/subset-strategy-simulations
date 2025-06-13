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

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MigrationRunner:
    def __init__(self, config_path: str, bucket_name: str = None):
        self.config_path = config_path
        self.config = {}
        self.bucket_name = bucket_name
        self.s3_client = None
        
    def aws_sso_login(self, profile: str = "astra-conn"):
        """Perform AWS SSO login with the specified profile."""
        logger.info(f"Performing AWS SSO login with profile: {profile}")
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
            'subset_calculation_strategy': 'MIGRATION_SUBSET_CALCULATION_STRATEGY'
        }
        
        # Set environment variables from config
        for config_key, env_var_name in env_var_mapping.items():
            if config_key in migration_config:
                os.environ[env_var_name] = str(migration_config[config_key])
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
        
        # Run from the parent directory so that ./mba/migration-bucket-accessor path works
        parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        
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
        
        # S3 path structure: <migrationId>/metadata/subsets/mytieredcalc/
        s3_config = self.config.get('s3', {})
        path_template = s3_config.get('path_template', '{migration_id}/metadata/subsets/mytieredcalc/')
        s3_path = path_template.replace('{migration_id}', migration_id)
        
        # Local directory: downloadedSubsetDefinitions/
        # Preserve full S3 path structure starting from migration ID
        base_download_dir = "downloadedSubsetDefinitions"
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
    
    def run_simulation(self, migration_id: str, download_dir: str) -> bool:
        """Run the simulation using downloaded data."""
        logger.info(f"Running simulation for migration ID: {migration_id}")
        
        simulation_config = self.config.get('simulation', {})
        
        # Build the command with all simulation options
        command = ['python3', 'run_multi_tier_simulation.py']
        
        # Required argument: directory
        command.append(download_dir)
        
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
        
        # Output Options
        output_config = simulation_config.get('output', {})
        
        # Generate output name with migration ID
        base_output_name = output_config.get('output_name', 'simulation_results')
        output_name = f"{base_output_name}_{migration_id}"
        command.extend(['--output-name', output_name])
        
        if 'output_dir' in output_config:
            command.extend(['--output-dir', output_config['output_dir']])
        else:
            # Default output directory with migration ID
            default_output_dir = f"simulation_outputs/{migration_id}"
            command.extend(['--output-dir', default_output_dir])
        
        if output_config.get('no_csv', False):
            command.append('--no-csv')
        if 'detailed_page_size' in output_config:
            command.extend(['--detailed-page-size', str(output_config['detailed_page_size'])])
        
        # Add any additional custom arguments
        custom_args = simulation_config.get('custom_args', [])
        for arg in custom_args:
            if isinstance(arg, str):
                processed_arg = arg.replace('{migration_id}', migration_id).replace('{download_dir}', download_dir)
                command.append(processed_arg)
            else:
                command.append(str(arg))
        
        # Run from the parent directory (project root)
        parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        
        logger.info(f"Executing simulation command: {' '.join(command)}")
        logger.info(f"Working directory: {parent_dir}")
        
        try:
            result = subprocess.run(
                command,
                check=True,
                capture_output=True,
                text=True,
                cwd=parent_dir  # Run from project root
            )
            logger.info(f"Simulation completed successfully for {migration_id}")
            logger.debug(f"Simulation output: {result.stdout}")
            if result.stdout:
                logger.info(f"Simulation stdout: {result.stdout}")
            return True
        except subprocess.CalledProcessError as e:
            logger.error(f"Simulation failed for {migration_id}: {e}")
            logger.error(f"Error output: {e.stderr}")
            if e.stdout:
                logger.error(f"Standard output: {e.stdout}")
            return False
    
    def process_migration_range(self, start_id: int, end_id: int, prefix: str = "mig"):
        """Process a range of migration IDs."""
        logger.info(f"Processing migration range: {prefix}{start_id} to {prefix}{end_id}")
        
        successful_migrations = []
        failed_migrations = []
        
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
                if not self.run_simulation(migration_id, download_dir):
                    failed_migrations.append(migration_id)
                    continue
                
                successful_migrations.append(migration_id)
                logger.info(f"Successfully processed migration: {migration_id}")
                
            except Exception as e:
                logger.error(f"Unexpected error processing {migration_id}: {e}")
                failed_migrations.append(migration_id)
        
        # Summary
        logger.info(f"Migration processing complete:")
        logger.info(f"  Successful: {len(successful_migrations)} - {successful_migrations}")
        logger.info(f"  Failed: {len(failed_migrations)} - {failed_migrations}")
        
        return successful_migrations, failed_migrations
    
    def run(self, start_id: int, end_id: int, prefix: str = "mig"):
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
        successful, failed = self.process_migration_range(start_id, end_id, prefix)
        
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
            "subset_calculation_strategy": "tiered"
        },
        "go_command": {
            "executable": "./mba/migration-bucket-accessor",
            "args": ["calc_subsets"]
        },
        "s3": {
            "path_template": "{migration_id}/metadata/subsets/mytieredcalc/"
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
                "no_stragglers": False
            },
            "output": {
                "output_name": "migration_simulation",
                "output_dir": "simulation_outputs/{migration_id}",
                "no_csv": False,
                "detailed_page_size": 30
            },
            "custom_args": []
        }
    }
    
    with open("migration_config_sample.yaml", "w") as f:
        yaml.dump(sample_config, f, default_flow_style=False, indent=2)
    
    print("Sample configuration file created: migration_config_sample.yaml")
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
    print()
    print("Simulation options configured:")
    print("  Worker Configuration: threads per tier, max workers per tier")
    print("  Analysis Options: straggler threshold, summary mode, straggler analysis")
    print("  Output Options: naming, directory structure, CSV export, pagination")

def main():
    parser = argparse.ArgumentParser(description="Migration Runner Script")
    parser.add_argument("--config", "-c", default="migration_runner_config.yaml", help="Path to configuration file (default: migration_runner_config.yaml)")
    parser.add_argument("--start-id", "-s", type=int, help="Starting migration ID number")
    parser.add_argument("--end-id", "-e", type=int, help="Ending migration ID number")
    parser.add_argument("--prefix", "-p", default="mig", help="Migration ID prefix (default: mig)")
    parser.add_argument("--bucket", "-b", help="S3 bucket name (overrides config)")
    parser.add_argument("--create-sample-config", action="store_true", help="Create a sample configuration file")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    if args.create_sample_config:
        create_sample_config()
        return
    
    # Check required arguments when not creating sample config
    if args.start_id is None:
        parser.error("--start-id/-s is required when not using --create-sample-config")
    if args.end_id is None:
        parser.error("--end-id/-e is required when not using --create-sample-config")
    
    if not os.path.exists(args.config):
        logger.error(f"Configuration file not found: {args.config}")
        logger.info("Use --create-sample-config to generate a sample configuration file")
        logger.info(f"Or create your configuration file at the default location: {args.config}")
        sys.exit(1)
    
    runner = MigrationRunner(args.config, args.bucket)
    success = runner.run(args.start_id, args.end_id, args.prefix)
    
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main() 