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
        
        # Set hardcoded values for cloud provider and subset calculation strategy
        os.environ['CLOUD_PROVIDER'] = 'AWS'
        os.environ['MIGRATION_SUBSET_CALCULATION_STRATEGY'] = 'tiered'
        logger.info("Set CLOUD_PROVIDER=AWS (hardcoded)")
        logger.info("Set MIGRATION_SUBSET_CALCULATION_STRATEGY=tiered (hardcoded)")
        
        # Define the mapping from config keys to environment variable names (with MIGRATION_ prefix)
        env_var_mapping = {
            'access_key': 'MIGRATION_ACCESS_KEY',
            'bucket': 'MIGRATION_BUCKET',
            'log_level': 'MIGRATION_LOG_LEVEL',
            'medium_tier_max_sstable_size_gb': 'MIGRATION_MEDIUM_TIER_MAX_SSTABLE_SIZE_GB',
            'medium_tier_worker_num_threads': 'MIGRATION_MEDIUM_TIER_WORKER_NUM_THREADS',
            'medium_tier_thread_subset_max_size_floor_gb': 'MIGRATION_MEDIUM_TIER_THREAD_SUBSET_MAX_SIZE_FLOOR_GB',
            'optimize_packing_medium_subsets': 'MIGRATION_OPTIMIZE_PACKING_MEDIUM_SUBSETS',
            'region': 'MIGRATION_REGION',
            'secret_key': 'MIGRATION_SECRET_KEY',
            'skip_small_subsets': 'MIGRATION_SKIP_SMALL_SUBSETS',
            'small_tier_max_sstable_size_gb': 'MIGRATION_SMALL_TIER_MAX_SSTABLE_SIZE_GB',
            'small_tier_thread_subset_max_size_floor_gb': 'MIGRATION_SMALL_TIER_THREAD_SUBSET_MAX_SIZE_FLOOR_GB',
            'small_tier_worker_num_threads': 'MIGRATION_SMALL_TIER_WORKER_NUM_THREADS',
            'subset_calculation_label': 'MIGRATION_SUBSET_CALCULATION_LABEL',
            'max_num_sstables_per_subset': 'MIGRATION_MAX_NUM_SSTABLES_PER_SUBSET'
        }
        
        # Set environment variables from config
        for config_key, env_var_name in env_var_mapping.items():
            if config_key in migration_config:
                # Convert boolean values to lowercase strings for Go compatibility
                value = migration_config[config_key]
                if isinstance(value, bool):
                    env_value = str(value).lower()
                else:
                    env_value = str(value)
                os.environ[env_var_name] = env_value
                # Redact sensitive values in logs
                if any(s in env_var_name.upper() for s in ["KEY", "SECRET", "ACCESS"]):
                    logger.info(f"Set {env_var_name}=***REDACTED***")
                else:
                    logger.info(f"Set {env_var_name}={env_value}")
        
        # Set default values for environment variables if not specified in config
        if 'skip_small_subsets' not in migration_config:
            os.environ['MIGRATION_SKIP_SMALL_SUBSETS'] = 'false'
            logger.info(f"Set MIGRATION_SKIP_SMALL_SUBSETS=false (default)")
        
        if 'medium_tier_thread_subset_max_size_floor_gb' not in migration_config:
            os.environ['MIGRATION_MEDIUM_TIER_THREAD_SUBSET_MAX_SIZE_FLOOR_GB'] = '15'
            logger.info(f"Set MIGRATION_MEDIUM_TIER_THREAD_SUBSET_MAX_SIZE_FLOOR_GB=15 (default)")
        
        if 'optimize_packing_medium_subsets' not in migration_config:
            os.environ['MIGRATION_OPTIMIZE_PACKING_MEDIUM_SUBSETS'] = 'true'
            logger.info(f"Set MIGRATION_OPTIMIZE_PACKING_MEDIUM_SUBSETS=true (default)")
        
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
        
        # Debug: Log all MIGRATION_ environment variables
        migration_env_vars = {k: v for k, v in os.environ.items() if k.startswith('MIGRATION_')}
        logger.info(f"MIGRATION environment variables being passed to Go program:")
        for k, v in sorted(migration_env_vars.items()):
            if any(s in k.upper() for s in ["KEY", "SECRET", "ACCESS"]):
                logger.info(f"  {k}=***REDACTED***")
            else:
                logger.info(f"  {k}={v}")
        
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
    
    def download_from_s3(self, migration_id: str, execution_name: str) -> Optional[str]:
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
        # Download to tiered directory's data folder, organized by execution name
        # Use absolute path to ensure we always download to tiered directory
        script_dir = os.path.dirname(os.path.abspath(__file__))  # helper_scripts directory
        tiered_dir = os.path.dirname(script_dir)  # tiered directory
        
        # Use execution-name based structure: data/downloadedSubsetDefinitions/{execution_name}/
        base_download_dir = os.path.join(tiered_dir, "data", "downloadedSubsetDefinitions", execution_name)
            
        os.makedirs(base_download_dir, exist_ok=True)
        
        logger.info(f"Downloading from S3 path: s3://{self.bucket_name}/{s3_path}")
        logger.info(f"Local base directory: {base_download_dir}")
        
        try:
            # List objects in the S3 path (with pagination support)
            all_objects = []
            continuation_token = None
            
            while True:
                # Build the list_objects_v2 request
                list_kwargs = {
                    'Bucket': self.bucket_name,
                    'Prefix': s3_path
                }
                if continuation_token:
                    list_kwargs['ContinuationToken'] = continuation_token
                
                response = self.s3_client.list_objects_v2(**list_kwargs)
                
                # Add objects from this page to our collection
                if 'Contents' in response:
                    all_objects.extend(response['Contents'])
                
                # Check if there are more pages
                if response.get('IsTruncated', False):
                    continuation_token = response.get('NextContinuationToken')
                    logger.debug(f"Found {len(response['Contents'])} objects in this page, continuing with next page...")
                else:
                    break
            
            if not all_objects:
                logger.warning(f"No objects found in S3 path: s3://{self.bucket_name}/{s3_path}")
                return None
            
            logger.info(f"Found {len(all_objects)} objects total in S3 (pagination handled)")
            
            downloaded_files = []
            for obj in all_objects:
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
    
    def check_metadata_exists(self, migration_id: str) -> bool:
        """Check if metadata exists in S3 for the given migration ID.
        
        Args:
            migration_id: The migration ID (e.g., 'mig119')
            
        Returns:
            bool: True if metadata exists, False otherwise
        """
        logger.info(f"Checking if metadata exists for migration ID: {migration_id}")
        
        if not self.s3_client:
            self.s3_client = boto3.client('s3')
        
        # Key prefix pattern: mig<numericID>/metadata/subsets/calculationMetadata/desc
        key_prefix = f"{migration_id}/metadata/subsets/calculationMetadata/desc"
        
        try:
            # List objects with the specific prefix
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=key_prefix,
                MaxKeys=1  # We only need to know if at least one object exists
            )
            
            # Check if any objects were found
            if 'Contents' in response and len(response['Contents']) > 0:
                logger.info(f"Metadata found for {migration_id}: {response['Contents'][0]['Key']}")
                return True
            else:
                logger.warning(f"No metadata found for {migration_id} with prefix: {key_prefix}")
                return False
                
        except Exception as e:
            logger.error(f"Failed to check metadata existence for {migration_id}: {e}")
            return False
    
    def organize_migration_outputs(self, migration_id: str, original_output_dir: str, execution_name: str) -> dict:
        """Organize migration outputs into the new directory structure.
        
        Args:
            migration_id: The migration ID (e.g., 'mig100')
            original_output_dir: The original simulation output directory
            execution_name: The execution name for organizing outputs
            
        Returns:
            dict: Updated paths to organized files
        """
        import shutil
        import glob
        
        # Create the new directory structure
        script_dir = os.path.dirname(os.path.abspath(__file__))  # helper_scripts directory
        tiered_dir = os.path.dirname(script_dir)  # tiered directory
        
        # Base directories for this execution and migration
        execution_output_dir = os.path.join(tiered_dir, "output", execution_name)
        migration_base_dir = os.path.join(execution_output_dir, migration_id)
        migration_exec_results_dir = os.path.join(migration_base_dir, "migration_exec_results")
        plots_dir = os.path.join(migration_base_dir, "plots")
        
        # Create directories
        os.makedirs(migration_exec_results_dir, exist_ok=True)
        os.makedirs(plots_dir, exist_ok=True)
        
        organized_files = {}
        
        if os.path.exists(original_output_dir):
            # Get all files in the original output directory
            all_files = glob.glob(os.path.join(original_output_dir, "*"))
            
            for file_path in all_files:
                if os.path.isfile(file_path):
                    filename = os.path.basename(file_path)
                    
                    # Determine destination based on file type
                    if filename.endswith('.html'):
                        # HTML files go to plots directory
                        dest_path = os.path.join(plots_dir, filename)
                        organized_files.setdefault('plots', []).append(dest_path)
                    else:
                        # Non-HTML files go to migration_exec_results directory
                        dest_path = os.path.join(migration_exec_results_dir, filename)
                        organized_files.setdefault('migration_exec_results', []).append(dest_path)
                    
                    # Copy the file to the new location
                    shutil.copy2(file_path, dest_path)
                    logger.info(f"Organized: {filename} -> {dest_path}")
        
        return organized_files

    def organize_html_files_to_plots(self, migration_id: str, migration_exec_results_dir: str, execution_name: str) -> dict:
        """Move HTML files from migration_exec_results to plots directory.
        
        Args:
            migration_id: The migration ID (e.g., 'mig100')
            migration_exec_results_dir: The directory containing all simulation outputs
            execution_name: The execution name for organizing outputs
            
        Returns:
            dict: Paths to organized files
        """
        import shutil
        import glob
        
        # Create the plots directory
        script_dir = os.path.dirname(os.path.abspath(__file__))  # helper_scripts directory
        tiered_dir = os.path.dirname(script_dir)  # tiered directory
        plots_dir = os.path.join(tiered_dir, "output", execution_name, migration_id, "plots")
        os.makedirs(plots_dir, exist_ok=True)
        
        organized_files = {
            'plots': [],
            'migration_exec_results': []
        }
        
        if os.path.exists(migration_exec_results_dir):
            # Get all files and directories in the migration_exec_results directory
            all_items = glob.glob(os.path.join(migration_exec_results_dir, "*"))
            
            for item_path in all_items:
                item_name = os.path.basename(item_path)
                
                if os.path.isfile(item_path):
                    if item_name.endswith('.html'):
                        # Move HTML files to plots directory
                        dest_path = os.path.join(plots_dir, item_name)
                        shutil.move(item_path, dest_path)
                        organized_files['plots'].append(dest_path)
                        logger.info(f"Moved HTML file: {item_name} -> plots/")
                    else:
                        # Non-HTML files stay in migration_exec_results
                        organized_files['migration_exec_results'].append(item_path)
                elif os.path.isdir(item_path):
                    # Check if this is a per-worker directory (contains HTML files)
                    if 'per_worker' in item_name:
                        # Move the entire per-worker directory to plots
                        dest_dir = os.path.join(plots_dir, item_name)
                        shutil.move(item_path, dest_dir)
                        organized_files['plots'].append(dest_dir)
                        logger.info(f"Moved per-worker directory: {item_name} -> plots/")
                    else:
                        # Other directories stay in migration_exec_results
                        organized_files['migration_exec_results'].append(item_path)
        
        return organized_files

    def run_simulation(self, migration_id: str, download_dir: str, execution_name: str) -> tuple[bool, dict]:
        """Run the simulation using downloaded data.
        
        Returns:
            tuple: (success: bool, output_files: dict) where output_files contains paths to generated files
        """
        logger.info(f"Running simulation for migration ID: {migration_id}")
        
        simulation_config = self.config.get('simulation', {})
        
        # Build the simulation command
        # The input directory should be the full path to the downloaded subset definitions
        # Files are downloaded to tiered directory, simulation runs from tiered directory
        input_directory = f"data/downloadedSubsetDefinitions/{execution_name}/{migration_id}"
        command = ['python', 'run_multi_tier_simulation.py', input_directory]
        
        # Worker Configuration - using values from migration section
        migration_config = self.config.get('migration', {})
        # Large threads is always 1
        command.extend(['--large-threads', '1'])
        # Medium threads uses medium_tier_worker_num_threads from migration config
        if 'medium_tier_worker_num_threads' in migration_config:
            command.extend(['--medium-threads', str(migration_config['medium_tier_worker_num_threads'])])
        # Small threads uses small_tier_worker_num_threads from migration config
        if 'small_tier_worker_num_threads' in migration_config:
            command.extend(['--small-threads', str(migration_config['small_tier_worker_num_threads'])])
        
        # Analysis Options
        analysis_config = simulation_config.get('analysis', {})
        if 'straggler_threshold' in analysis_config:
            command.extend(['--straggler-threshold', str(analysis_config['straggler_threshold'])])
        if analysis_config.get('summary_only', False):
            command.append('--summary-only')
        # Handle enable_straggler_detection (inverted logic from old no_stragglers)
        if not analysis_config.get('enable_straggler_detection', True):
            command.append('--no-stragglers')
        # Execution mode (default to concurrent for backward compatibility)
        execution_mode = analysis_config.get('execution_mode', 'concurrent')
        if execution_mode != 'concurrent':
            command.extend(['--execution-mode', execution_mode])
        
        # Max workers configuration - now in analysis section and conditional on execution mode
        if execution_mode != 'round_robin':
            # These parameters are required for non-round-robin modes
            if 'small_max_workers' in analysis_config:
                command.extend(['--small-max-workers', str(analysis_config['small_max_workers'])])
            else:
                logger.warning(f"Non-round-robin execution mode specified but small_max_workers not set for {migration_id}")
            if 'medium_max_workers' in analysis_config:
                command.extend(['--medium-max-workers', str(analysis_config['medium_max_workers'])])
            else:
                logger.warning(f"Non-round-robin execution mode specified but medium_max_workers not set for {migration_id}")
            if 'large_max_workers' in analysis_config:
                command.extend(['--large-max-workers', str(analysis_config['large_max_workers'])])
            else:
                logger.warning(f"Non-round-robin execution mode specified but large_max_workers not set for {migration_id}")
        
        # Max concurrent workers for round-robin mode
        if execution_mode == 'round_robin':
            max_concurrent_workers = analysis_config.get('max_concurrent_workers')
            if max_concurrent_workers:
                command.extend(['--max-concurrent-workers', str(max_concurrent_workers)])
            else:
                logger.warning(f"Round-robin execution mode specified but max_concurrent_workers not set for {migration_id}")
        
        # Legacy support for sequential_execution flag
        if analysis_config.get('sequential_execution', False):
            command.extend(['--execution-mode', 'sequential'])
        
        # Output Options
        output_config = simulation_config.get('output', {})
        output_name = output_config.get('output_name', 'migration_simulation')
        if output_name:
            # Append migration ID to output name if not already present
            if not output_name.endswith(migration_id):
                output_name = f"{output_name}_{migration_id}"
            command.extend(['--output-name', output_name])
        
        # Define output directory using execution-name based structure
        script_dir = os.path.dirname(os.path.abspath(__file__))  # helper_scripts directory
        tiered_dir = os.path.dirname(script_dir)  # tiered directory
        migration_exec_results_dir = os.path.join(tiered_dir, "output", execution_name, migration_id, "migration_exec_results")
        # Make sure the directory exists
        os.makedirs(migration_exec_results_dir, exist_ok=True)
        # Output directory relative to tiered directory (where simulation runs)
        output_dir = os.path.relpath(migration_exec_results_dir, tiered_dir)
        
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
    
    def process_migration_range(self, start_id: int, end_id: int, execution_name: str, prefix: str = "mig"):
        """Process a range of migration IDs."""
        logger.info(f"Processing migration range: {prefix}{start_id} to {prefix}{end_id}")
        
        successful_migrations = []
        failed_migrations = []
        skipped_migrations = []
        migration_results = {}  # Track output files for each successful migration
        
        for migration_num in range(start_id, end_id + 1):
            migration_id = f"{prefix}{migration_num}"
            logger.info(f"Processing migration: {migration_id}")
            
            try:
                # Set environment variables for this specific migration
                self.set_environment_variables(migration_id)
                
                # Check if metadata exists in S3 before proceeding
                if not self.check_metadata_exists(migration_id):
                    logger.warning(f"Skipping {migration_id}: metadata not found in S3")
                    skipped_migrations.append(migration_id)
                    continue
                
                # Execute Go command
                if not self.execute_go_command(migration_id):
                    failed_migrations.append(migration_id)
                    continue
                
                # Download from S3
                download_dir = self.download_from_s3(migration_id, execution_name)
                if not download_dir:
                    failed_migrations.append(migration_id)
                    continue
                
                # Run simulation
                success, output_files = self.run_simulation(migration_id, download_dir, execution_name)
                if not success:
                    failed_migrations.append(migration_id)
                    continue
                
                # Organize outputs into directory structure
                if output_files:
                    # Get the original output directory from the simulation
                    original_output_dir = None
                    if 'timeline' in output_files:
                        original_output_dir = os.path.dirname(output_files['timeline'])
                    
                    if original_output_dir:
                        # Move HTML files to plots directory, leave other files in migration_exec_results
                        organized_files = self.organize_html_files_to_plots(migration_id, original_output_dir, execution_name)
                        # Update output_files with organized paths
                        output_files['organized'] = organized_files
                
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
        logger.info(f"  Skipped (no metadata): {len(skipped_migrations)} - {skipped_migrations}")
        
        return successful_migrations, failed_migrations, migration_results
    
    def clear_previous_execution_data(self, execution_name: str):
        """Clear previous execution data for the given execution name.
        
        This removes:
        1. Downloaded subset definitions: data/downloadedSubsetDefinitions/{execution_name}/
        2. Output files: output/{execution_name}/
        
        Args:
            execution_name: The execution name to clear data for
        """
        import shutil
        
        logger.info(f"Clearing previous execution data for execution: {execution_name}")
        
        # Get paths relative to tiered directory
        script_dir = os.path.dirname(os.path.abspath(__file__))  # helper_scripts directory
        tiered_dir = os.path.dirname(script_dir)  # tiered directory
        
        # Clear downloaded subset definitions
        downloaded_subsets_dir = os.path.join(tiered_dir, "data", "downloadedSubsetDefinitions", execution_name)
        if os.path.exists(downloaded_subsets_dir):
            logger.info(f"Removing downloaded subset definitions: {downloaded_subsets_dir}")
            shutil.rmtree(downloaded_subsets_dir)
        else:
            logger.info(f"No previous downloaded subset definitions found: {downloaded_subsets_dir}")
        
        # Clear output directory
        output_dir = os.path.join(tiered_dir, "output", execution_name)
        if os.path.exists(output_dir):
            logger.info(f"Removing output directory: {output_dir}")
            shutil.rmtree(output_dir)
        else:
            logger.info(f"No previous output directory found: {output_dir}")
        
        logger.info(f"Cleanup completed for execution: {execution_name}")

    def collect_execution_report_data(self, migration_results: dict, execution_name: str) -> dict:
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
                # Check if files are in the organized structure
                if 'organized' in output_files and 'migration_exec_results' in output_files['organized']:
                    # Look in the new organized structure
                    logger.debug(f"Looking for JSON files in organized structure for {migration_id}")
                    for file_path in output_files['organized']['migration_exec_results']:
                        logger.debug(f"Checking file: {file_path}")
                        if file_path.endswith('_execution_report.json'):
                            json_files.append(file_path)
                            logger.info(f"Found execution report JSON: {file_path}")
                else:
                    # Fallback: Check the simulation output directory - look in multiple possible locations
                    logger.debug(f"Using fallback paths for {migration_id}")
                    
                    # Also try looking in the timeline file directory as a fallback
                    if 'timeline' in output_files:
                        timeline_dir = os.path.dirname(output_files['timeline'])
                        logger.debug(f"Checking timeline directory: {timeline_dir}")
                        if os.path.exists(timeline_dir):
                            for file in os.listdir(timeline_dir):
                                if file.endswith('_execution_report.json'):
                                    json_path = os.path.join(timeline_dir, file)
                                    json_files.append(json_path)
                                    logger.info(f"Found execution report JSON in timeline dir: {json_path}")
                    
                    # Original fallback paths
                    possible_paths = [
                        f"../data/simulation_outputs/{migration_id}",  # From helper_scripts directory
                        f"data/simulation_outputs/{migration_id}",     # From tiered directory  
                        f"tiered/data/simulation_outputs/{migration_id}" # From project root
                    ]
                    
                    for sim_output_dir in possible_paths:
                        logger.debug(f"Checking fallback path: {sim_output_dir}")
                        if os.path.exists(sim_output_dir):
                            for file in os.listdir(sim_output_dir):
                                if file.endswith('_execution_report.json'):
                                    json_path = os.path.join(sim_output_dir, file)
                                    json_files.append(json_path)
                                    logger.info(f"Found execution report JSON: {json_path}")
                            break  # Found the directory, no need to check other paths
                
            except Exception as e:
                logger.warning(f"Error searching for execution report JSON files for {migration_id}: {e}")
            
            # Load data from found JSON files
            if json_files:
                try:
                    # Use the first JSON file found (should only be one per migration)
                    with open(json_files[0], 'r', encoding='utf-8') as f:
                        migration_data = json.load(f)
                        logger.info(f"Successfully loaded execution data for {migration_id}")
                        logger.debug(f"Migration data keys: {list(migration_data.keys())}")
                        execution_data["migrations"][migration_id] = migration_data
                except Exception as e:
                    logger.warning(f"Failed to read execution report for {migration_id}: {e}")
            else:
                logger.warning(f"No execution report JSON found for {migration_id}")
                # Create empty migration data to prevent KeyError later
                execution_data["migrations"][migration_id] = {
                    'total_execution_time': 0,
                    'total_migration_size_bytes': 0,
                    'total_migration_size_gb': 0,
                    'by_tier': {}
                }
        
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
                # Thread configuration is derived from migration config
                migration_config = execution_data.get("migration_config", {})
                if migration_config:
                    f.write(f"large_threads: 1 (hardcoded)\n")
                    f.write(f"medium_threads: {migration_config.get('medium_tier_worker_num_threads', 'N/A')}\n")
                    f.write(f"small_threads: {migration_config.get('small_tier_worker_num_threads', 'N/A')}\n")
                else:
                    f.write(f"large_threads: 1 (hardcoded)\n")
                    f.write(f"medium_threads: N/A\n")
                    f.write(f"small_threads: N/A\n")
                
                # Max workers are in analysis section
                first_migration_data = next(iter(execution_data["migrations"].values()), {})
                sim_config = first_migration_data.get("simulation_config", {})
                if sim_config:
                    analysis_config = sim_config.get('analysis', {})
                    f.write(f"small_max_workers: {analysis_config.get('small_max_workers', 'N/A')}\n")
                    f.write(f"medium_max_workers: {analysis_config.get('medium_max_workers', 'N/A')}\n")
                    f.write(f"large_max_workers: {analysis_config.get('large_max_workers', 'N/A')}\n")
                else:
                    f.write(f"small_max_workers: N/A\n")
                    f.write(f"medium_max_workers: N/A\n")
                    f.write(f"large_max_workers: N/A\n")
                f.write("\n")
                
                # Per-Migration Analysis
                f.write("PER-MIGRATION ANALYSIS\n")
                f.write("-"*70 + "\n")
                
                for migration_id in sorted(execution_data["migrations"].keys()):
                    migration_data = execution_data["migrations"][migration_id]
                    by_tier = migration_data.get("by_tier", {})
                    total_time = migration_data.get("total_execution_time", 0)
                    total_size_bytes = migration_data.get("total_migration_size_bytes", 0)
                    total_size_gb = migration_data.get("total_migration_size_gb", 0)
                    
                    f.write(f"Migration ID: {migration_id}\n")
                    f.write(f"Total Execution Time: {total_time:.2f} time units\n")
                    f.write(f"Total Migration Size: {total_size_bytes:,} bytes ({total_size_gb:.2f} GB)\n")
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
                    'Migration_ID', 'Total_Execution_Time', 'Total_Migration_Size_Bytes', 'Total_Migration_Size_GB',
                    'Tier', 'Total_Workers', 'Straggler_Workers', 
                    'Idle_Workers', 'Both_Straggler_And_Idle', 'Straggler_Percentage', 'Idle_Percentage'
                ])
                
                # Data rows for each migration and tier
                for migration_id in sorted(execution_data["migrations"].keys()):
                    migration_data = execution_data["migrations"][migration_id]
                    by_tier = migration_data.get("by_tier", {})
                    total_time = migration_data.get("total_execution_time", 0)
                    total_size_bytes = migration_data.get("total_migration_size_bytes", 0)
                    total_size_gb = migration_data.get("total_migration_size_gb", 0)
                    
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
                                migration_id, f"{total_time:.2f}", total_size_bytes, f"{total_size_gb:.2f}",
                                tier, total, straggler, idle, both,
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
            
            # Check if files were organized into new structure
            if 'organized' in output_files and 'plots' in output_files['organized']:
                # Use organized file paths
                plots = output_files['organized']['plots']
                
                # Find timeline and detailed files
                timeline_file = next((f for f in plots if 'timeline' in f), None)
                detailed_files = [f for f in plots if 'detailed' in f]
                
                # Sort detailed files to ensure first page (without page number) comes first
                def detailed_file_sort_key(filename):
                    """Sort key function to put the first page (without page number) first."""
                    basename = os.path.basename(filename)
                    if 'detailed_page' in basename:
                        # Extract page number for sorting
                        import re
                        match = re.search(r'detailed_page(\d+)', basename)
                        if match:
                            return int(match.group(1))
                        return 999  # fallback for malformed filenames
                    else:
                        # First page (no page number) should come first
                        return 0
                
                detailed_files.sort(key=detailed_file_sort_key)
                
                if timeline_file:
                    print(f"  file://{timeline_file}")
                
                if detailed_files:
                    if len(detailed_files) == 1:
                        print(f"  file://{detailed_files[0]}")
                    else:
                        print(f"  file://{detailed_files[0]} [{len(detailed_files)} total pages]")
                        
            else:
                # Fallback to original file paths
                if 'timeline' in output_files:
                    print(f"  file://{output_files['timeline']}")
                
                if 'detailed_pages' in output_files and output_files['detailed_pages']:
                    total_pages = output_files['total_pages']
                    if total_pages == 1:
                        print(f"  file://{output_files['detailed_pages'][0]}")
                    else:
                        print(f"  file://{output_files['detailed_pages'][0]} [{total_pages} total pages]")
                elif 'detailed' in output_files:
                    print(f"  file://{output_files['detailed']}")
        
        print("\n" + "="*80)
    
    def run(self, start_id: int, end_id: int, execution_name: str, prefix: str = "mig", output_dir: str = "exec_output"):
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
        
        # Step 3: Clear previous execution data
        self.clear_previous_execution_data(execution_name)
        
        # Step 4: Process migration range (environment variables are set per migration)
        successful, failed, migration_results = self.process_migration_range(start_id, end_id, execution_name, prefix)
        
        # Step 5: Collect execution report data
        execution_data = self.collect_execution_report_data(migration_results, execution_name)
        
        # Step 6: Create new directory structure: tiered/output/{execution_name}/exec_reports/
        script_dir = os.path.dirname(os.path.abspath(__file__))  # helper_scripts directory
        tiered_dir = os.path.dirname(script_dir)  # tiered directory
        execution_output_dir = os.path.join(tiered_dir, "output", execution_name)
        exec_reports_dir = os.path.join(execution_output_dir, "exec_reports")
        os.makedirs(exec_reports_dir, exist_ok=True)
        
        # Step 7: Generate execution report with new structure
        report_txt_path = os.path.join(exec_reports_dir, f"execution_report_{execution_name}.txt")
        self.generate_execution_report(execution_data, report_txt_path)
        
        # Step 8: Generate execution report CSV with new structure
        report_csv_path = os.path.join(exec_reports_dir, f"execution_report_{execution_name}.csv")
        self.generate_execution_report_csv(execution_data, report_csv_path)
        
        # Step 9: Print results summary
        self.print_results_summary(migration_results)
        
        return len(failed) == 0

def create_sample_config():
    """Create a sample configuration file for reference."""
    # Create the config file in the same directory as this script (helper_scripts)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_file_path = os.path.join(script_dir, "migration_config_sample.yaml")
    
    # Write the config file manually to preserve exact ordering
    config_content = """go_command:
  args:
  - calc_subsets
  executable: ./mba/migration-bucket-accessor
migration:
  access_key: YOUR_ACCESS_KEY_HERE
  secret_key: YOUR_SECRET_KEY_HERE
  bucket: alice-sst-sdl-test
  region: eu-west-1
  log_level: DEBUG
  max_num_sstables_per_subset: 250
  subset_calculation_label: mytieredcalc
  small_tier_max_sstable_size_gb: 10
  small_tier_thread_subset_max_size_floor_gb: 2
  small_tier_worker_num_threads: 4
  medium_tier_max_sstable_size_gb: 50
  medium_tier_worker_num_threads: 6
  medium_tier_thread_subset_max_size_floor_gb: 15
  optimize_packing_medium_subsets: true
  skip_small_subsets: false
s3:
  path_template: '{migration_id}/metadata/subsets/{subset_calculation_label}/'
simulation:
  analysis:
    execution_mode: concurrent
    max_concurrent_workers: 20
    small_max_workers: 4
    medium_max_workers: 6
    large_max_workers: 10
    enable_straggler_detection: true
    straggler_threshold: 10.0
    summary_only: false
  custom_args: []
  output:
    detailed_page_size: 30
    no_csv: false
    output_dir: tiered_simulation_outputs/{migration_id}
    output_name: tiered_migration_simulation
"""
    
    with open(config_file_path, "w") as f:
        f.write(config_content)
    
    print(f"Sample configuration file created: {config_file_path}")
    print("Please customize it according to your needs.")
    print()
    print("The following environment variables will be set:")
    print("  CLOUD_PROVIDER=AWS (hardcoded)")
    print("  MIGRATION_SUBSET_CALCULATION_STRATEGY=tiered (hardcoded)")
    print("  MIGRATION_ACCESS_KEY (from config)")
    print("  MIGRATION_SECRET_KEY (from config)")
    print("  MIGRATION_BUCKET (from config)")
    print("  MIGRATION_REGION (from config)")
    print("  MIGRATION_LOG_LEVEL (from config)")
    print("  MIGRATION_MAX_NUM_SSTABLES_PER_SUBSET (from config)")
    print("  MIGRATION_SUBSET_CALCULATION_LABEL (from config)")
    print("  MIGRATION_SMALL_TIER_MAX_SSTABLE_SIZE_GB (from config)")
    print("  MIGRATION_SMALL_TIER_THREAD_SUBSET_MAX_SIZE_FLOOR_GB (from config)")
    print("  MIGRATION_SMALL_TIER_WORKER_NUM_THREADS (from config)")
    print("  MIGRATION_MEDIUM_TIER_MAX_SSTABLE_SIZE_GB (from config)")
    print("  MIGRATION_MEDIUM_TIER_WORKER_NUM_THREADS (from config)")
    print("  MIGRATION_MEDIUM_TIER_THREAD_SUBSET_MAX_SIZE_FLOOR_GB (from config, default: 15)")
    print("  MIGRATION_OPTIMIZE_PACKING_MEDIUM_SUBSETS (from config, default: true)")
    print("  MIGRATION_SKIP_SMALL_SUBSETS (from config, default: false)")
    print("  MIGRATION_ID (automatically set to current migration ID)")
    print()
    print("Simulation options configured:")
    print("  Worker Configuration: threads per tier (derived from migration config), max workers per tier")
    print("  Analysis Options: straggler threshold, summary mode, straggler analysis, execution mode")
    print("  Output Options: naming, directory structure, CSV export, pagination")
    print()
    print("Worker thread configuration:")
    print("  large_threads: Always 1 (hardcoded)")
    print("  medium_threads: Uses medium_tier_worker_num_threads from migration config")
    print("  small_threads: Uses small_tier_worker_num_threads from migration config")
    print()
    print("Execution modes:")
    print("  execution_mode: 'concurrent' (default) - All tiers process concurrently")
    print("  execution_mode: 'sequential' - Process tiers sequentially (LARGE->MEDIUM->SMALL)")
    print("  execution_mode: 'round_robin' - Round-robin allocation with global worker limit")
    print("  max_concurrent_workers: Required for round_robin mode - total worker limit across all tiers")

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
    
    # Try to find tiered_migration_runner_config.yaml in current directory
    current_dir = os.getcwd()
    config_file = os.path.join(current_dir, 'tiered_migration_runner_config.yaml')
    if os.path.exists(config_file):
        return config_file
    
    # Try to find tiered_migration_runner_config.yaml in helper_scripts directory
    helper_scripts_dir = os.path.dirname(os.path.abspath(__file__))
    config_file = os.path.join(helper_scripts_dir, 'tiered_migration_runner_config.yaml')
    if os.path.exists(config_file):
        return config_file
    
    raise FileNotFoundError("No configuration file found. Please create tiered_migration_runner_config.yaml in the current directory or helper_scripts directory.")

def main():
    parser = argparse.ArgumentParser(description='Run migration processing for a range of IDs')
    parser.add_argument('--start-id', type=int, help='Starting migration ID')
    parser.add_argument('--end-id', type=int, help='Ending migration ID')
    parser.add_argument('--execution-name', type=str, required=True, help='Name for this execution (used in report filenames and directory organization)')
    parser.add_argument('--prefix', type=str, default='mig', help='Prefix for migration IDs (default: mig)')
    parser.add_argument('--output-dir', type=str, default='exec_output', help='Output directory for execution reports (default: exec_output)')
    parser.add_argument('--config-path', type=str, help='Path to configuration file (default: tiered_migration_runner_config.yaml)')
    parser.add_argument('--bucket', type=str, help='S3 bucket name')
    parser.add_argument('--create-sample-config', action='store_true', help='Create a sample configuration file')
    args = parser.parse_args()
    
    if args.create_sample_config:
        create_sample_config()
        return
    
    # Check required arguments for normal execution
    if not args.start_id or not args.end_id:
        parser.error("--start-id and --end-id are required for normal execution")
    
    try:
        # Find the config file
        config_path = find_config_file(args.config_path)
        logger.info(f"Using configuration file: {config_path}")
        
        # Initialize and run the migration processor
        runner = MigrationRunner(config_path, args.bucket)
        runner.run(args.start_id, args.end_id, args.execution_name, args.prefix, args.output_dir)
    except Exception as e:
        logger.error(f"Error: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main() 