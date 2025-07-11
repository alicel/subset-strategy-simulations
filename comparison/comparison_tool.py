#!/usr/bin/env python3
"""
Comparison Tool for Simple vs Tiered Migration Simulations

This script compares the execution results between simple and tiered migration simulations
for common migrations, providing detailed metrics comparison.
"""

import argparse
import csv
import json
import os
import re
import sys
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

@dataclass
class MigrationMetrics:
    """Stores metrics for a single migration execution."""
    migration_id: str
    strategy: str  # 'simple' or 'tiered'
    total_execution_time: float
    total_workers: int
    total_cpus: int  # threads across all workers
    cpu_time: float  # total thread time used (current calculation)
    total_data_size_gb: float  # total data size processed in GB
    workers_by_tier: Dict[str, int]  # tier -> worker count
    cpus_by_tier: Dict[str, int]  # tier -> total threads
    stragglers_by_tier: Dict[str, int]  # tier -> straggler worker count
    config: Dict[str, any]  # simulation and migration configuration
    # CPU Efficiency metrics (available for both simple and tiered strategies)
    total_used_cpu_time: float = 0.0  # total allocated CPU time (duration × threads)
    total_active_cpu_time: float = 0.0  # total actual processing time
    cpu_inefficiency: float = 0.0  # idle/wasted CPU time
    average_cpu_efficiency_percent: float = 0.0  # average efficiency across workers
    
    def __post_init__(self):
        """Calculate derived metrics."""
        if self.strategy == 'simple':
            # For simple strategy, all workers are in 'UNIVERSAL' tier
            self.workers_by_tier = {'UNIVERSAL': self.total_workers}
            self.cpus_by_tier = {'UNIVERSAL': self.total_cpus}
            self.stragglers_by_tier = {'UNIVERSAL': 0}  # Simple has no stragglers

@dataclass
class ComparisonResult:
    """Stores comparison results between simple and tiered strategies."""
    migration_id: str
    simple_metrics: MigrationMetrics
    tiered_metrics: MigrationMetrics
    
    @property
    def execution_time_ratio(self) -> float:
        """Ratio of tiered execution time to simple execution time."""
        if self.simple_metrics.total_execution_time == 0:
            return float('inf') if self.tiered_metrics.total_execution_time > 0 else 1.0
        return self.tiered_metrics.total_execution_time / self.simple_metrics.total_execution_time
    
    @property
    def execution_time_ratio_inverse(self) -> float:
        """Ratio of simple execution time to tiered execution time."""
        if self.tiered_metrics.total_execution_time == 0:
            return float('inf') if self.simple_metrics.total_execution_time > 0 else 1.0
        return self.simple_metrics.total_execution_time / self.tiered_metrics.total_execution_time
    
    @property
    def worker_count_ratio(self) -> float:
        """Ratio of tiered worker count to simple worker count."""
        if self.simple_metrics.total_workers == 0:
            return float('inf') if self.tiered_metrics.total_workers > 0 else 1.0
        return self.tiered_metrics.total_workers / self.simple_metrics.total_workers
    
    @property
    def worker_count_ratio_inverse(self) -> float:
        """Ratio of simple worker count to tiered worker count."""
        if self.tiered_metrics.total_workers == 0:
            return float('inf') if self.simple_metrics.total_workers > 0 else 1.0
        return self.simple_metrics.total_workers / self.tiered_metrics.total_workers
    
    @property
    def cpu_count_ratio(self) -> float:
        """Ratio of tiered CPU count to simple CPU count."""
        if self.simple_metrics.total_cpus == 0:
            return float('inf') if self.tiered_metrics.total_cpus > 0 else 1.0
        return self.tiered_metrics.total_cpus / self.simple_metrics.total_cpus
    
    @property
    def cpu_count_ratio_inverse(self) -> float:
        """Ratio of simple CPU count to tiered CPU count."""
        if self.tiered_metrics.total_cpus == 0:
            return float('inf') if self.simple_metrics.total_cpus > 0 else 1.0
        return self.simple_metrics.total_cpus / self.tiered_metrics.total_cpus
    
    @property
    def cpu_time_ratio(self) -> float:
        """Ratio of tiered CPU time to simple CPU time."""
        if self.simple_metrics.cpu_time == 0:
            return float('inf') if self.tiered_metrics.cpu_time > 0 else 1.0
        return self.tiered_metrics.cpu_time / self.simple_metrics.cpu_time
    
    @property
    def cpu_time_ratio_inverse(self) -> float:
        """Ratio of simple CPU time to tiered CPU time."""
        if self.tiered_metrics.cpu_time == 0:
            return float('inf') if self.simple_metrics.cpu_time > 0 else 1.0
        return self.simple_metrics.cpu_time / self.tiered_metrics.cpu_time

    # CPU Efficiency properties (now available for both strategies)
    @property
    def simple_cpu_efficiency_percent(self) -> float:
        """CPU efficiency percentage for simple strategy."""
        return self.simple_metrics.average_cpu_efficiency_percent
    
    @property
    def tiered_cpu_efficiency_percent(self) -> float:
        """CPU efficiency percentage for tiered strategy."""
        return self.tiered_metrics.average_cpu_efficiency_percent
    
    @property
    def simple_cpu_inefficiency_ratio(self) -> float:
        """Ratio of wasted CPU time to total CPU time for simple strategy."""
        if self.simple_metrics.total_used_cpu_time == 0:
            return 0.0
        return self.simple_metrics.cpu_inefficiency / self.simple_metrics.total_used_cpu_time
    
    @property
    def tiered_cpu_inefficiency_ratio(self) -> float:
        """Ratio of wasted CPU time to total CPU time for tiered strategy."""
        if self.tiered_metrics.total_used_cpu_time == 0:
            return 0.0
        return self.tiered_metrics.cpu_inefficiency / self.tiered_metrics.total_used_cpu_time

    # Difference properties (tiered - simple)
    @property
    def execution_time_diff(self) -> float:
        """Difference in execution time (tiered - simple)."""
        return self.tiered_metrics.total_execution_time - self.simple_metrics.total_execution_time
    
    @property
    def worker_count_diff(self) -> int:
        """Difference in worker count (tiered - simple)."""
        return self.tiered_metrics.total_workers - self.simple_metrics.total_workers
    
    @property
    def cpu_count_diff(self) -> int:
        """Difference in CPU count (tiered - simple)."""
        return self.tiered_metrics.total_cpus - self.simple_metrics.total_cpus
    
    @property
    def cpu_time_diff(self) -> float:
        """Difference in CPU time (tiered - simple)."""
        return self.tiered_metrics.cpu_time - self.simple_metrics.cpu_time
    
    @property
    def data_size_diff(self) -> float:
        """Difference in data size (tiered - simple)."""
        return self.tiered_metrics.total_data_size_gb - self.simple_metrics.total_data_size_gb
    
    @property
    def cpu_efficiency_diff(self) -> float:
        """Difference in CPU efficiency percentage (tiered - simple)."""
        return self.tiered_metrics.average_cpu_efficiency_percent - self.simple_metrics.average_cpu_efficiency_percent
    
    def get_config_comparison(self, config_keys: List[str]) -> Dict[str, any]:
        """Compare specific configuration parameters between strategies."""
        comparison = {}
        
        for key in config_keys:
            simple_value = self._get_config_value(self.simple_metrics.config, key)
            tiered_value = self._get_config_value(self.tiered_metrics.config, key)
            
            comparison[key] = {
                'simple': simple_value,
                'tiered': tiered_value,
                'same': simple_value == tiered_value
            }
        
        return comparison
    
    def _get_config_value(self, config: Dict[str, any], key: str):
        """Get a configuration value, looking in both simulation and migration sections."""
        # Handle parameter name mapping from simulation config
        simulation_mapping = {
            'small_tier_worker_num_threads': 'small_threads',
            'medium_tier_worker_num_threads': 'medium_threads',
            'large_tier_worker_num_threads': 'large_threads'  # This is always 1
        }
        
        # Migration parameters that should be looked up directly in migration config
        migration_params = {
            'small_tier_max_sstable_size_gb',
            'small_tier_thread_subset_max_size_floor_gb',
            'medium_tier_max_sstable_size_gb',
            'optimize_packing_medium_subsets',
            'max_num_sstables_per_subset'
        }
        
        # First check if it's a migration parameter
        if key in migration_params:
            if 'migration' in config and key in config['migration']:
                return config['migration'][key]
        
        # Then try direct lookup in simulation config
        if 'simulation' in config and key in config['simulation']:
            return config['simulation'][key]
        
        # Try mapped lookup in simulation config
        if key in simulation_mapping and 'simulation' in config:
            mapped_key = simulation_mapping[key]
            if mapped_key in config['simulation']:
                return config['simulation'][mapped_key]
        
        # Also try migration config for other parameters
        if 'migration' in config and key in config['migration']:
            return config['migration'][key]
        
        # Return None if not found
        return None

class SimulationDataExtractor:
    """Extracts metrics from simulation output files."""
    
    def extract_simple_metrics(self, simple_run_path: str) -> Dict[str, MigrationMetrics]:
        """Extract metrics from simple simulation output."""
        simple_path = Path(simple_run_path)
        if not simple_path.exists():
            raise FileNotFoundError(f"Simple run path not found: {simple_run_path}")
        
        metrics = {}
        
        # Look for migration directories
        for migration_dir in simple_path.iterdir():
            if migration_dir.is_dir() and migration_dir.name.startswith(('mig', 'migration')):
                migration_id = migration_dir.name
                if migration_id == 'exec_reports':
                    continue
                
                # Extract metrics from config file
                config_file = migration_dir / 'migration_exec_results' / 'config_simple_migration_simulation.txt'
                if config_file.exists():
                    migration_metrics = self._parse_simple_config(migration_id, config_file)
                    if migration_metrics:
                        metrics[migration_id] = migration_metrics
        
        return metrics
    
    def _parse_simple_config(self, migration_id: str, config_file: Path) -> Optional[MigrationMetrics]:
        """Parse simple simulation CSV files to extract actual metrics."""
        try:
            # Look for CSV files in the same directory
            csv_dir = config_file.parent
            workers_csv = None
            summary_csv = None
            
            # Sort glob results to ensure deterministic file selection
            workers_csv_files = sorted(csv_dir.glob("*_workers.csv"))
            summary_csv_files = sorted(csv_dir.glob("*_summary.csv"))
            
            workers_csv = workers_csv_files[0] if workers_csv_files else None
            summary_csv = summary_csv_files[0] if summary_csv_files else None
            
            if not workers_csv or not summary_csv:
                print(f"Warning: Could not find CSV files for {migration_id}. Looking for workers and summary CSV files.")
                return None
            
            # Parse worker CSV for actual execution data and CPU efficiency metrics
            actual_workers = 0
            total_cpu_time = 0.0
            total_data_size_gb = 0.0
            total_used_cpu_time = 0.0
            total_active_cpu_time = 0.0
            cpu_inefficiency = 0.0
            cpu_efficiency_values = []
            threads_per_worker = 1  # Default value, will be updated from config
            
            # Extract basic configuration from config file first to get threads_per_worker
            config = {'simulation': {}, 'migration': {}}
            try:
                with open(config_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                    # Extract threads_per_worker first
                    match = re.search(r'Threads per worker:\s*(\d+)', content)
                    if match:
                        threads_per_worker = int(match.group(1))
                        config['simulation']['threads_per_worker'] = threads_per_worker
                    
                    # Extract max_concurrent_workers from config file
                    match = re.search(r'Max concurrent workers:\s*(\d+)', content)
                    if match:
                        config['simulation']['max_concurrent_workers'] = int(match.group(1))
                    
                    # Extract worker_processing_time_unit if present
                    match = re.search(r'Worker processing time unit:\s*(\d+)', content)
                    if match:
                        config['simulation']['worker_processing_time_unit'] = int(match.group(1))
                    
                    # Extract enable_subset_size_cap
                    match = re.search(r'Enable subset size cap:\s*(true|false)', content, re.IGNORECASE)
                    if match:
                        config['migration']['enable_subset_size_cap'] = match.group(1).lower() == 'true'
                    
                    # Extract enable_subset_num_sstable_cap
                    match = re.search(r'Enable subset num sstable cap:\s*(true|false)', content, re.IGNORECASE)
                    if match:
                        config['migration']['enable_subset_num_sstable_cap'] = match.group(1).lower() == 'true'
            except Exception as e:
                print(f"Warning: Could not extract config from {config_file}: {e}")
            
            with open(workers_csv, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    actual_workers += 1
                    # Calculate CPU time as duration × threads_per_worker
                    duration = float(row['Duration'])
                    total_cpu_time += duration * threads_per_worker
                    # Sum up data size (with safety check)
                    if 'Data_Size_GB' in row and row['Data_Size_GB']:
                        total_data_size_gb += float(row['Data_Size_GB'])
                    
                    # Extract CPU efficiency metrics if available (new CSV format with multithreaded support)
                    if 'Total_Used_CPU_Time' in row and 'Total_Active_CPU_Time' in row:
                        total_used_cpu_time += float(row['Total_Used_CPU_Time'])
                        total_active_cpu_time += float(row['Total_Active_CPU_Time'])
                        cpu_inefficiency += float(row['CPU_Inefficiency'])
                        cpu_efficiency_values.append(float(row['CPU_Efficiency_Percent']))
            
            # Parse summary CSV for total execution time and other metrics
            total_execution_time = 0.0
            total_cpus = actual_workers * threads_per_worker  # threads_per_worker CPUs per worker
            
            with open(summary_csv, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                for row in reader:
                    if len(row) >= 2 and row[0] == 'Total_Simulation_Time':
                        total_execution_time = float(row[1])
                    elif len(row) >= 2 and row[0] == 'Total_CPU_Time':
                        total_cpu_time = float(row[1])  # Use the corrected value from summary
                    elif len(row) >= 2 and row[0] == 'Total_CPUs':
                        total_cpus = int(row[1])  # Use the corrected value from summary
            
            # Calculate overall CPU efficiency (more meaningful than simple average)
            average_cpu_efficiency_percent = (total_active_cpu_time / total_used_cpu_time * 100) if total_used_cpu_time > 0 else 0.0
            
            # If we couldn't get data size from CSV, try to get it from JSON as fallback
            if total_data_size_gb == 0.0:
                print(f"Warning: No data size found in CSV for {migration_id}. Looking for JSON fallback.")
                json_files = list(csv_dir.glob("*_execution_report.json"))
                if json_files:
                    try:
                        with open(json_files[0], 'r', encoding='utf-8') as f:
                            json_data = json.load(f)
                        total_data_size_gb = json_data.get('total_migration_size_gb', 0.0)
                        print(f"Found migration size in JSON: {total_data_size_gb:.2f} GB")
                    except Exception as e:
                        print(f"Warning: Could not read JSON fallback for {migration_id}: {e}")
            
            return MigrationMetrics(
                migration_id=migration_id,
                strategy='simple',
                total_execution_time=total_execution_time,
                total_workers=actual_workers,
                total_cpus=total_cpus,
                cpu_time=total_cpu_time,
                total_data_size_gb=total_data_size_gb,
                workers_by_tier={},
                cpus_by_tier={},
                stragglers_by_tier={},
                total_used_cpu_time=total_used_cpu_time,
                total_active_cpu_time=total_active_cpu_time,
                cpu_inefficiency=cpu_inefficiency,
                average_cpu_efficiency_percent=average_cpu_efficiency_percent,
                config=config
            )
            
        except Exception as e:
            print(f"Error parsing simple CSV data for {migration_id}: {e}")
            return None
    
    def extract_tiered_metrics(self, tiered_run_path: str) -> Dict[str, MigrationMetrics]:
        """Extract metrics from tiered simulation output."""
        tiered_path = Path(tiered_run_path)
        if not tiered_path.exists():
            raise FileNotFoundError(f"Tiered run path not found: {tiered_run_path}")
        
        # Try to extract execution-level configuration first
        execution_config = self._extract_execution_config(tiered_path)
        
        metrics = {}
        
        # Look for migration directories
        for migration_dir in tiered_path.iterdir():
            if migration_dir.is_dir() and migration_dir.name.startswith(('mig', 'migration')):
                migration_id = migration_dir.name
                if migration_id == 'exec_reports':
                    continue
                
                # Extract metrics from JSON execution report
                json_files = list(migration_dir.glob('migration_exec_results/*_execution_report.json'))
                if json_files:
                    migration_metrics = self._parse_tiered_json(migration_id, json_files[0], execution_config)
                    if migration_metrics:
                        metrics[migration_id] = migration_metrics
        
        return metrics
    
    def _extract_execution_config(self, tiered_path: Path) -> Dict[str, any]:
        """Extract execution-level configuration from exec_reports directory."""
        execution_config = {}
        
        # Look for execution report files that might contain configuration
        exec_reports_dir = tiered_path / 'exec_reports'
        if exec_reports_dir.exists():
            # Look for execution report text files that contain migration configuration
            for report_file in exec_reports_dir.glob('execution_report_*.txt'):
                try:
                    with open(report_file, 'r', encoding='utf-8') as f:
                        content = f.read()
                        
                    # Parse configuration from the report
                    config_section = False
                    for line in content.split('\n'):
                        line = line.strip()
                        if line.startswith('MIGRATION CONFIGURATION'):
                            config_section = True
                            continue
                        elif line.startswith('SIMULATION CONFIGURATION') or line.startswith('PER-MIGRATION'):
                            config_section = False
                            continue
                        
                        if config_section and ':' in line and not line.startswith('-'):
                            key, value = line.split(':', 1)
                            key = key.strip()
                            value = value.strip()
                            
                            # Convert values to appropriate types
                            if value.lower() in ['true', 'false']:
                                execution_config[key] = value.lower() == 'true'
                            elif value.replace('.', '').replace('-', '').isdigit():
                                execution_config[key] = float(value) if '.' in value else int(value)
                            else:
                                execution_config[key] = value
                    break  # Use first report file found
                except Exception as e:
                    print(f"Warning: Could not parse execution report {report_file}: {e}")
            
            # Also look for JSON execution reports as fallback
            for json_file in exec_reports_dir.glob('*_execution_report.json'):
                try:
                    with open(json_file, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        if 'simulation_config' in data:
                            execution_config.update(data['simulation_config'])
                            break
                except Exception:
                    continue
        
        return execution_config
    
    def _parse_tiered_json(self, migration_id: str, json_file: Path, execution_config: Dict[str, any] = None) -> Optional[MigrationMetrics]:
        """Parse tiered simulation JSON and CSV files to extract actual metrics."""
        try:
            # Parse JSON for basic metrics
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            total_execution_time = data.get('total_execution_time', 0.0)
            simulation_config = data.get('simulation_config', {})
            by_tier = data.get('by_tier', {})
            
            # Merge execution-level config with migration-level config
            if execution_config:
                # Update simulation config with execution-level config
                merged_config = execution_config.copy()
                merged_config.update(simulation_config)
                simulation_config = merged_config
            
            # Look for worker CSV file to get actual execution data
            csv_dir = json_file.parent
            workers_csv = None
            
            # Sort glob results to ensure deterministic file selection
            workers_csv_files = sorted(csv_dir.glob("*_workers.csv"))
            workers_csv = workers_csv_files[0] if workers_csv_files else None
            
            # Calculate totals across all tiers from JSON
            total_workers = 0
            total_cpus = 0
            workers_by_tier = {}
            cpus_by_tier = {}
            stragglers_by_tier = {}
            
            for tier in ['SMALL', 'MEDIUM', 'LARGE']:
                if tier in by_tier:
                    tier_workers = by_tier[tier].get('total_workers', 0)
                    tier_stragglers = by_tier[tier].get('straggler_workers', 0)
                    total_workers += tier_workers
                    workers_by_tier[tier] = tier_workers
                    stragglers_by_tier[tier] = tier_stragglers
                    
                    # Calculate CPUs for this tier
                    if tier_workers > 0:
                        threads_per_worker = simulation_config.get(f'{tier.lower()}_threads', 1)
                        tier_cpus = tier_workers * threads_per_worker
                        total_cpus += tier_cpus
                        cpus_by_tier[tier] = tier_cpus
                    else:
                        cpus_by_tier[tier] = 0
                else:
                    workers_by_tier[tier] = 0
                    cpus_by_tier[tier] = 0
                    stragglers_by_tier[tier] = 0
            
            # Calculate actual CPU time, efficiency metrics, and total data size from worker CSV if available
            cpu_time = 0.0
            total_data_size_gb = 0.0
            total_used_cpu_time = 0.0
            total_active_cpu_time = 0.0
            cpu_inefficiency = 0.0
            cpu_efficiency_values = []
            
            if workers_csv and workers_csv.exists():
                with open(workers_csv, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        tier = row['Tier']
                        duration = float(row['Duration'])
                        threads_per_worker = simulation_config.get(f'{tier.lower()}_threads', 1)
                        cpu_time += duration * threads_per_worker
                        # Sum up data size (with safety check)
                        if 'Data_Size_GB' in row and row['Data_Size_GB']:
                            total_data_size_gb += float(row['Data_Size_GB'])
                        
                        # Extract efficiency metrics if available (new CSV format)
                        if 'Total_Used_CPU_Time' in row and 'Total_Active_CPU_Time' in row:
                            total_used_cpu_time += float(row['Total_Used_CPU_Time'])
                            total_active_cpu_time += float(row['Total_Active_CPU_Time'])
                            cpu_inefficiency += float(row['CPU_Inefficiency'])
                            cpu_efficiency_values.append(float(row['CPU_Efficiency_Percent']))
            else:
                # Fallback to conservative estimate if CSV not available
                cpu_time = total_execution_time * total_cpus if total_cpus > 0 else total_execution_time
                print(f"Warning: Worker CSV not found for {migration_id}, using conservative CPU time estimate")
            
            # If we couldn't get data size from CSV, use JSON data as fallback
            if total_data_size_gb == 0.0:
                print(f"Warning: No data size found in CSV for {migration_id}. Using JSON data as fallback.")
                total_data_size_gb = data.get('total_migration_size_gb', 0.0)
                if total_data_size_gb > 0:
                    print(f"Found migration size in JSON: {total_data_size_gb:.2f} GB")
                else:
                    print(f"Warning: No migration size found in JSON for {migration_id}")
            
            # Calculate overall weighted efficiency (more meaningful than simple average)
            average_cpu_efficiency_percent = (total_active_cpu_time / total_used_cpu_time * 100) if total_used_cpu_time > 0 else 0.0
            
            # Build configuration dict
            config = {
                'simulation': simulation_config,
                'migration': data.get('migration_config', {})
            }
            
            return MigrationMetrics(
                migration_id=migration_id,
                strategy='tiered',
                total_execution_time=total_execution_time,
                total_workers=total_workers,
                total_cpus=total_cpus,
                cpu_time=cpu_time,
                total_data_size_gb=total_data_size_gb,
                workers_by_tier=workers_by_tier,
                cpus_by_tier=cpus_by_tier,
                stragglers_by_tier=stragglers_by_tier,
                total_used_cpu_time=total_used_cpu_time,
                total_active_cpu_time=total_active_cpu_time,
                cpu_inefficiency=cpu_inefficiency,
                average_cpu_efficiency_percent=average_cpu_efficiency_percent,
                config=config
            )
            
        except Exception as e:
            print(f"Error parsing tiered data for {migration_id}: {e}")
            return None

class ComparisonAnalyzer:
    """Analyzes and compares simulation results."""
    
    def __init__(self):
        self.extractor = SimulationDataExtractor()
    
    def compare_runs(self, simple_run_path: str, tiered_run_path: str) -> Tuple[List[ComparisonResult], Set[str], Set[str]]:
        """Compare simple and tiered simulation runs.
        
        Returns:
            Tuple of (comparisons, simple_only_migrations, tiered_only_migrations)
        """
        print(f"Extracting simple metrics from: {simple_run_path}")
        simple_metrics = self.extractor.extract_simple_metrics(simple_run_path)
        
        print(f"Extracting tiered metrics from: {tiered_run_path}")
        tiered_metrics = self.extractor.extract_tiered_metrics(tiered_run_path)
        
        print(f"Found {len(simple_metrics)} simple migrations and {len(tiered_metrics)} tiered migrations")
        
        # Find common and exclusive migrations
        simple_migrations = set(simple_metrics.keys())
        tiered_migrations = set(tiered_metrics.keys())
        common_migrations = simple_migrations.intersection(tiered_migrations)
        simple_only_migrations = simple_migrations - tiered_migrations
        tiered_only_migrations = tiered_migrations - simple_migrations
        
        print(f"Common migrations: {sorted(common_migrations)}")
        
        if not common_migrations:
            print("Warning: No common migrations found between the two runs")
            return [], simple_only_migrations, tiered_only_migrations
        
        # Create comparison results
        comparisons = []
        for migration_id in sorted(common_migrations):
            comparison = ComparisonResult(
                migration_id=migration_id,
                simple_metrics=simple_metrics[migration_id],
                tiered_metrics=tiered_metrics[migration_id]
            )
            comparisons.append(comparison)
        
        return comparisons, simple_only_migrations, tiered_only_migrations
    
    def print_comparison_summary(self, comparisons: List[ComparisonResult], simple_exec_name: str = None, tiered_exec_name: str = None, simple_only: Set[str] = None, tiered_only: Set[str] = None):
        """Print a summary of the comparison results."""
        if not comparisons:
            print("No comparisons to display.")
            return
        
        print("\n" + "="*100)
        print("SIMPLE vs TIERED SIMULATION COMPARISON SUMMARY")
        print("="*100)
        
        if simple_exec_name and tiered_exec_name:
            print(f"\nComparison Details:")
            print(f"  Simple Execution:  {simple_exec_name}")
            print(f"  Tiered Execution:  {tiered_exec_name}")
            print(f"  Common Migrations: {len(comparisons)}")
        else:
            print(f"\nCommon Migrations: {len(comparisons)}")
        
        # Show exclusive migrations if any exist
        if simple_only or tiered_only:
            print(f"\nExclusive Migrations:")
            if simple_only:
                print(f"  Simple Only ({len(simple_only)}): {sorted(simple_only)}")
            if tiered_only:
                print(f"  Tiered Only ({len(tiered_only)}): {sorted(tiered_only)}")
        
        # Header
        print(f"\n{'Migration':<12} {'Data Size (GB)':<25} {'Execution Time':<35} {'Workers':<30} {'CPUs':<30} {'CPU Time':<35} {'CPU Efficiency':<33}")
        print(f"{'ID':<12} {'Simple':<8} {'Tiered':<8} {'Diff':<8} {'Simple':<10} {'Tiered':<10} {'T/S':<5} {'S/T':<5} {'Simple':<8} {'Tiered':<8} {'T/S':<5} {'S/T':<5} {'Simple':<8} {'Tiered':<8} {'T/S':<5} {'S/T':<5} {'Simple':<10} {'Tiered':<10} {'T/S':<5} {'S/T':<5} {'S-Eff%':<8} {'S-Waste%':<8} {'T-Eff%':<8} {'T-Waste%':<8}")
        print("-" * 175)
        
        # Data rows
        for comp in comparisons:
            simple = comp.simple_metrics
            tiered = comp.tiered_metrics
            
            # Format large numbers
            simple_time = self._format_time(simple.total_execution_time)
            tiered_time = self._format_time(tiered.total_execution_time)
            time_ratio_ts = f"{comp.execution_time_ratio:.2f}"
            time_ratio_st = f"{comp.execution_time_ratio_inverse:.2f}"
            
            simple_cpu_time = self._format_time(simple.cpu_time)
            tiered_cpu_time = self._format_time(tiered.cpu_time)
            cpu_time_ratio_ts = f"{comp.cpu_time_ratio:.2f}"
            cpu_time_ratio_st = f"{comp.cpu_time_ratio_inverse:.2f}"
            
            worker_ratio_ts = f"{comp.worker_count_ratio:.2f}"
            worker_ratio_st = f"{comp.worker_count_ratio_inverse:.2f}"
            
            cpu_ratio_ts = f"{comp.cpu_count_ratio:.2f}"
            cpu_ratio_st = f"{comp.cpu_count_ratio_inverse:.2f}"
            
            # CPU efficiency metrics for both strategies
            simple_eff_percent = f"{comp.simple_cpu_efficiency_percent:.1f}%" if comp.simple_cpu_efficiency_percent > 0 else "N/A"
            simple_waste_percent = f"{comp.simple_cpu_inefficiency_ratio * 100:.1f}%" if comp.simple_metrics.total_used_cpu_time > 0 else "N/A"
            tiered_eff_percent = f"{comp.tiered_cpu_efficiency_percent:.1f}%" if comp.tiered_cpu_efficiency_percent > 0 else "N/A"
            tiered_waste_percent = f"{comp.tiered_cpu_inefficiency_ratio * 100:.1f}%" if comp.tiered_metrics.total_used_cpu_time > 0 else "N/A"
            
            # Data size difference for validation
            data_size_diff = tiered.total_data_size_gb - simple.total_data_size_gb
            data_size_diff_str = f"{data_size_diff:+.2f}"
            
            print(f"{comp.migration_id:<12} {simple.total_data_size_gb:<8.2f} {tiered.total_data_size_gb:<8.2f} {data_size_diff_str:<8} {simple_time:<10} {tiered_time:<10} {time_ratio_ts:<5} {time_ratio_st:<5} "
                  f"{simple.total_workers:<8} {tiered.total_workers:<8} {worker_ratio_ts:<5} {worker_ratio_st:<5} "
                  f"{simple.total_cpus:<8} {tiered.total_cpus:<8} {cpu_ratio_ts:<5} {cpu_ratio_st:<5} "
                  f"{simple_cpu_time:<10} {tiered_cpu_time:<10} {cpu_time_ratio_ts:<5} {cpu_time_ratio_st:<5} "
                  f"{simple_eff_percent:<8} {simple_waste_percent:<8} {tiered_eff_percent:<8} {tiered_waste_percent:<8}")
        
        # Summary statistics
        print("\n" + "="*125)
        print("AGGREGATE ANALYSIS")
        print("="*125)
        
        total_simple_time = sum(c.simple_metrics.total_execution_time for c in comparisons)
        total_tiered_time = sum(c.tiered_metrics.total_execution_time for c in comparisons)
        
        total_simple_workers = sum(c.simple_metrics.total_workers for c in comparisons)
        total_tiered_workers = sum(c.tiered_metrics.total_workers for c in comparisons)
        
        total_simple_cpus = sum(c.simple_metrics.total_cpus for c in comparisons)
        total_tiered_cpus = sum(c.tiered_metrics.total_cpus for c in comparisons)
        
        total_simple_cpu_time = sum(c.simple_metrics.cpu_time for c in comparisons)
        total_tiered_cpu_time = sum(c.tiered_metrics.cpu_time for c in comparisons)
        
        total_simple_data_size = sum(c.simple_metrics.total_data_size_gb for c in comparisons)
        total_tiered_data_size = sum(c.tiered_metrics.total_data_size_gb for c in comparisons)
        
        print(f"\nTotal Execution Time:")
        print(f"  Simple:      {self._format_time(total_simple_time)}")
        print(f"  Tiered:      {self._format_time(total_tiered_time)}")
        print(f"  Tiered/Simple: {total_tiered_time/total_simple_time:.2f} (efficiency: <1.0 = tiered faster)")
        print(f"  Simple/Tiered: {total_simple_time/total_tiered_time:.2f} (speedup: >1.0 = tiered faster)")
        
        print(f"\nTotal Workers:")
        print(f"  Simple:      {total_simple_workers:,}")
        print(f"  Tiered:      {total_tiered_workers:,}")
        print(f"  Tiered/Simple: {total_tiered_workers/total_simple_workers:.2f}")
        print(f"  Simple/Tiered: {total_simple_workers/total_tiered_workers:.2f}")
        
        print(f"\nTotal CPUs:")
        print(f"  Simple:      {total_simple_cpus:,}")
        print(f"  Tiered:      {total_tiered_cpus:,}")
        print(f"  Tiered/Simple: {total_tiered_cpus/total_simple_cpus:.2f}")
        print(f"  Simple/Tiered: {total_simple_cpus/total_tiered_cpus:.2f}")
        
        print(f"\nTotal CPU Time:")
        print(f"  Simple:      {self._format_time(total_simple_cpu_time)}")
        print(f"  Tiered:      {self._format_time(total_tiered_cpu_time)}")
        print(f"  Tiered/Simple: {total_tiered_cpu_time/total_simple_cpu_time:.2f}")
        print(f"  Simple/Tiered: {total_simple_cpu_time/total_tiered_cpu_time:.2f}")
        
        print(f"\nTotal Data Size:")
        print(f"  Simple:      {total_simple_data_size:.2f} GB")
        print(f"  Tiered:      {total_tiered_data_size:.2f} GB")
        print(f"  Difference:  {total_tiered_data_size - total_simple_data_size:+.2f} GB (Tiered - Simple)")
        if total_simple_data_size > 0:
            print(f"  Tiered/Simple: {total_tiered_data_size/total_simple_data_size:.2f}")
        if total_tiered_data_size > 0:
            print(f"  Simple/Tiered: {total_simple_data_size/total_tiered_data_size:.2f}")
    
    def _format_time(self, time_units: float) -> str:
        """Format time units for display."""
        if time_units >= 1e9:
            return f"{time_units/1e9:.1f}B"
        elif time_units >= 1e6:
            return f"{time_units/1e6:.1f}M"
        elif time_units >= 1e3:
            return f"{time_units/1e3:.1f}K"
        else:
            return f"{time_units:.1f}"
    
    def save_comparison_csv(self, comparisons: List[ComparisonResult], output_file: str, simple_exec_name: str = None, tiered_exec_name: str = None, data_size_threshold: float = None, efficiency_threshold: float = None):
        """Save comparison results to CSV file."""
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            # Write header comments
            csvfile.write("# Simple vs Tiered Migration Simulation Comparison\n")
            csvfile.write(f"# Simple Execution: {simple_exec_name or 'Unknown'}\n")
            csvfile.write(f"# Tiered Execution: {tiered_exec_name or 'Unknown'}\n")
            csvfile.write(f"# Common Migrations: {len(comparisons)}\n")
            csvfile.write("# Data_Size_GB is the same for both strategies (same data being processed)\n")
            csvfile.write("# Diff columns show Tiered - Simple (positive = Tiered higher, negative = Tiered lower)\n")
            if data_size_threshold is not None:
                csvfile.write(f"# Large Migration Threshold: {data_size_threshold:.2f} GB\n")
            if efficiency_threshold is not None:
                csvfile.write(f"# Low Efficiency Threshold: {efficiency_threshold:.1f}%\n")
            csvfile.write("#\n")
            
            # Write configuration comparison
            if comparisons:
                csvfile.write("# CONFIGURATION COMPARISON\n")
                csvfile.write("# Simple and Tiered strategies use different parameters, so they are listed separately\n")
                csvfile.write("#\n")
                csvfile.write("# SIMPLE CONFIGURATION\n")
                
                simple_config_keys = [
                    'max_concurrent_workers',
                    'threads_per_worker',
                    'worker_processing_time_unit',
                    'enable_subset_size_cap',
                    'enable_subset_num_sstable_cap'
                ]
                first_comp = comparisons[0]
                simple_config = first_comp.simple_metrics.config
                
                for key in simple_config_keys:
                    value = first_comp._get_config_value(simple_config, key)
                    display_value = value if value is not None else 'N/A'
                    csvfile.write(f"# {key}: {display_value}\n")
                
                csvfile.write("#\n")
                csvfile.write("# TIERED CONFIGURATION\n")
                
                tiered_config_keys = [
                    'small_tier_max_sstable_size_gb',
                    'small_tier_thread_subset_max_size_floor_gb', 
                    'small_tier_worker_num_threads',
                    'medium_tier_max_sstable_size_gb',
                    'medium_tier_worker_num_threads',
                    'optimize_packing_medium_subsets',
                    'execution_mode',
                    'max_concurrent_workers'
                ]
                tiered_config = first_comp.tiered_metrics.config
                
                for key in tiered_config_keys:
                    value = first_comp._get_config_value(tiered_config, key)
                    display_value = value if value is not None else 'N/A'
                    csvfile.write(f"# {key}: {display_value}\n")
            
            csvfile.write("\n")
            
            # CSV header
            fieldnames = [
                'Migration_ID',
                'Simple_Data_Size_GB', 'Tiered_Data_Size_GB', 'Data_Size_Diff',
                'Simple_Execution_Time', 'Tiered_Execution_Time', 'Execution_Time_Diff',
                'Simple_Workers', 'Tiered_Workers', 'Worker_Diff',
                'Simple_CPUs', 'Tiered_CPUs', 'CPU_Diff',
                'Simple_CPU_Time', 'Tiered_CPU_Time', 'CPU_Time_Diff',
                'Simple_Active_CPU_Time', 'Simple_CPU_Efficiency_Percent', 'Simple_CPU_Waste_Percent',
                'Tiered_Active_CPU_Time', 'Tiered_CPU_Efficiency_Percent', 'Tiered_CPU_Waste_Percent',
                'Tiered_Small_Workers', 'Tiered_Medium_Workers', 'Tiered_Large_Workers',
                'Tiered_Small_Stragglers', 'Tiered_Medium_Stragglers', 'Tiered_Large_Stragglers',
                'Tiered_Small_CPUs', 'Tiered_Medium_CPUs', 'Tiered_Large_CPUs'
            ]
            
            # Add large migration indicator column if threshold is specified
            if data_size_threshold is not None:
                fieldnames.insert(2, 'Is_Large_Migration')
            
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            # Write data rows
            for comp in comparisons:
                simple = comp.simple_metrics
                tiered = comp.tiered_metrics
                
                row = {
                    'Migration_ID': comp.migration_id,
                    'Simple_Data_Size_GB': f"{simple.total_data_size_gb:.2f}",
                    'Tiered_Data_Size_GB': f"{tiered.total_data_size_gb:.2f}",
                    'Data_Size_Diff': f"{tiered.total_data_size_gb - simple.total_data_size_gb:+.2f}",
                    'Simple_Execution_Time': f"{simple.total_execution_time:.2f}",
                    'Tiered_Execution_Time': f"{tiered.total_execution_time:.2f}",
                    'Execution_Time_Diff': f"{comp.execution_time_diff:.2f}",
                    'Simple_Workers': simple.total_workers,
                    'Tiered_Workers': tiered.total_workers,
                    'Worker_Diff': comp.worker_count_diff,
                    'Simple_CPUs': simple.total_cpus,
                    'Tiered_CPUs': tiered.total_cpus,
                    'CPU_Diff': comp.cpu_count_diff,
                    'Simple_CPU_Time': f"{simple.cpu_time:.2f}",
                    'Tiered_CPU_Time': f"{tiered.cpu_time:.2f}",
                    'CPU_Time_Diff': f"{comp.cpu_time_diff:.2f}",
                    'Simple_Active_CPU_Time': f"{simple.total_active_cpu_time:.2f}",
                    'Simple_CPU_Efficiency_Percent': f"{simple.average_cpu_efficiency_percent:.2f}",
                    'Simple_CPU_Waste_Percent': f"{(simple.cpu_inefficiency / simple.total_used_cpu_time * 100) if simple.total_used_cpu_time > 0 else 0:.2f}",
                    'Tiered_Active_CPU_Time': f"{tiered.total_active_cpu_time:.2f}",
                    'Tiered_CPU_Efficiency_Percent': f"{tiered.average_cpu_efficiency_percent:.2f}",
                    'Tiered_CPU_Waste_Percent': f"{(tiered.cpu_inefficiency / tiered.total_used_cpu_time * 100) if tiered.total_used_cpu_time > 0 else 0:.2f}",
                    'Tiered_Small_Workers': tiered.workers_by_tier.get('SMALL', 0),
                    'Tiered_Medium_Workers': tiered.workers_by_tier.get('MEDIUM', 0),
                    'Tiered_Large_Workers': tiered.workers_by_tier.get('LARGE', 0),
                    'Tiered_Small_Stragglers': tiered.stragglers_by_tier.get('SMALL', 0),
                    'Tiered_Medium_Stragglers': tiered.stragglers_by_tier.get('MEDIUM', 0),
                    'Tiered_Large_Stragglers': tiered.stragglers_by_tier.get('LARGE', 0),
                    'Tiered_Small_CPUs': tiered.cpus_by_tier.get('SMALL', 0),
                    'Tiered_Medium_CPUs': tiered.cpus_by_tier.get('MEDIUM', 0),
                    'Tiered_Large_CPUs': tiered.cpus_by_tier.get('LARGE', 0),
                }
                
                # Add large migration indicator if threshold is specified
                if data_size_threshold is not None:
                    row['Is_Large_Migration'] = 'Yes' if simple.total_data_size_gb >= data_size_threshold else 'No'
                
                writer.writerow(row)
        
        print(f"CSV comparison report saved to: {output_file}")
    
    def generate_comparison_report(self, comparisons: List[ComparisonResult], simple_exec_name: str = None, tiered_exec_name: str = None, simple_only: Set[str] = None, tiered_only: Set[str] = None, data_size_threshold: float = None) -> str:
        """Generate a tabular comparison report for text viewing."""
        lines = []
        lines.append("Simple vs Tiered Migration Simulation Comparison")
        lines.append("=" * 60)
        
        if simple_exec_name and tiered_exec_name:
            lines.append("")
            lines.append("Comparison Details:")
            lines.append(f"  Simple Execution:  {simple_exec_name}")
            lines.append(f"  Tiered Execution:  {tiered_exec_name}")
            lines.append(f"  Common Migrations: {len(comparisons)}")
        else:
            lines.append("")
            lines.append(f"Common Migrations: {len(comparisons)}")
        
        # Show large migration threshold if specified
        if data_size_threshold is not None:
            large_migrations = sum(1 for comp in comparisons if comp.simple_metrics.total_data_size_gb >= data_size_threshold)
            lines.append(f"  Large Migration Threshold: {data_size_threshold:.2f} GB ({large_migrations} migrations)")
        
        # Show exclusive migrations if any exist
        if simple_only or tiered_only:
            lines.append("")
            lines.append("Exclusive Migrations:")
            if simple_only:
                lines.append(f"  Simple Only ({len(simple_only)}): {', '.join(sorted(simple_only))}")
            if tiered_only:
                lines.append(f"  Tiered Only ({len(tiered_only)}): {', '.join(sorted(tiered_only))}")
        
        # Configuration comparison
        if comparisons:
            lines.append("")
            lines.append("="*60)
            lines.append("CONFIGURATION COMPARISON")
            lines.append("="*60)
            lines.append("")
            lines.append("Simple and Tiered strategies use different parameters, so they are listed separately:")
            
            # Simple Configuration
            lines.append("")
            lines.append("SIMPLE CONFIGURATION:")
            lines.append("-" * 25)
            
            simple_config_keys = [
                'max_concurrent_workers',
                'threads_per_worker',
                'worker_processing_time_unit',
                'enable_subset_size_cap',
                'enable_subset_num_sstable_cap'
            ]
            first_comp = comparisons[0]
            simple_config = first_comp.simple_metrics.config
            
            for key in simple_config_keys:
                value = first_comp._get_config_value(simple_config, key)
                display_value = value if value is not None else 'N/A'
                lines.append(f"  {key:<35}: {display_value}")
            
            # Tiered Configuration
            lines.append("")
            lines.append("TIERED CONFIGURATION:")
            lines.append("-" * 25)
            
            tiered_config_keys = [
                'small_tier_max_sstable_size_gb',
                'small_tier_thread_subset_max_size_floor_gb', 
                'small_tier_worker_num_threads',
                'medium_tier_max_sstable_size_gb',
                'medium_tier_worker_num_threads',
                'optimize_packing_medium_subsets',
                'execution_mode',
                'max_concurrent_workers'
            ]
            tiered_config = first_comp.tiered_metrics.config
            
            for key in tiered_config_keys:
                value = first_comp._get_config_value(tiered_config, key)
                display_value = value if value is not None else 'N/A'
                lines.append(f"  {key:<35}: {display_value}")
        
        # Header
        simple_short = (simple_exec_name[:8] + "..") if simple_exec_name and len(simple_exec_name) > 10 else (simple_exec_name or "Simple")
        tiered_short = (tiered_exec_name[:8] + "..") if tiered_exec_name and len(tiered_exec_name) > 10 else (tiered_exec_name or "Tiered")
        
        lines.append("")
        lines.append(f"{'Migration':<12} {'Data Size (GB)':<25} {'Execution Time':<30} {'Workers':<25} {'CPUs':<25} {'CPU Time':<30}")
        lines.append(f"{'ID':<12} {'Simple':<8} {'Tiered':<8} {'Diff':<8} {simple_short:<10} {tiered_short:<10} {'Diff':<8} {simple_short:<8} {tiered_short:<8} {'Diff':<6} {simple_short:<8} {tiered_short:<8} {'Diff':<6} {simple_short:<10} {tiered_short:<10} {'Diff':<8}")
        lines.append("-" * 125)
        
        # Data rows
        for comp in comparisons:
            simple = comp.simple_metrics
            tiered = comp.tiered_metrics
            
            # Format values and differences
            simple_time = self._format_time(simple.total_execution_time)
            tiered_time = self._format_time(tiered.total_execution_time)
            time_diff = f"{comp.execution_time_diff:+.1f}s" if abs(comp.execution_time_diff) < 60 else f"{comp.execution_time_diff/60:+.1f}m"
            
            # Data size formatting
            data_size_diff = tiered.total_data_size_gb - simple.total_data_size_gb
            data_size_diff_str = f"{data_size_diff:+.2f}"
            
            # Worker and CPU differences
            worker_diff = f"{comp.worker_count_diff:+d}"
            cpu_diff = f"{comp.cpu_count_diff:+d}"
            
            # CPU time difference
            cpu_time_diff = f"{comp.cpu_time_diff:+.1f}s" if abs(comp.cpu_time_diff) < 60 else f"{comp.cpu_time_diff/60:+.1f}m"
            
            lines.append(f"{comp.migration_id:<12} {simple.total_data_size_gb:<8.2f} {tiered.total_data_size_gb:<8.2f} {data_size_diff_str:<8} {simple_time:<10} {tiered_time:<10} {time_diff:<8} {simple.total_workers:<8} {tiered.total_workers:<8} {worker_diff:<6} {simple.total_cpus:<8} {tiered.total_cpus:<8} {cpu_diff:<6} {self._format_time(simple.cpu_time):<10} {self._format_time(tiered.cpu_time):<10} {cpu_time_diff:<8}")
        
        # Summary statistics
        lines.append("")
        lines.append("="*115)
        lines.append("AGGREGATE ANALYSIS")
        lines.append("="*115)
        
        total_simple_time = sum(c.simple_metrics.total_execution_time for c in comparisons)
        total_tiered_time = sum(c.tiered_metrics.total_execution_time for c in comparisons)
        total_simple_workers = sum(c.simple_metrics.total_workers for c in comparisons)
        total_tiered_workers = sum(c.tiered_metrics.total_workers for c in comparisons)
        total_simple_cpus = sum(c.simple_metrics.total_cpus for c in comparisons)
        total_tiered_cpus = sum(c.tiered_metrics.total_cpus for c in comparisons)
        total_simple_cpu_time = sum(c.simple_metrics.cpu_time for c in comparisons)
        total_tiered_cpu_time = sum(c.tiered_metrics.cpu_time for c in comparisons)
        
        lines.append("")
        lines.append("Total Execution Time:")
        lines.append(f"  Simple:      {self._format_time(total_simple_time)}")
        lines.append(f"  Tiered:      {self._format_time(total_tiered_time)}")
        time_diff = total_tiered_time - total_simple_time
        time_diff_str = f"{time_diff:+.1f}s" if abs(time_diff) < 60 else f"{time_diff/60:+.1f}m"
        lines.append(f"  Difference:  {time_diff_str} (Tiered {'slower' if time_diff > 0 else 'faster' if time_diff < 0 else 'same'})")
        
        lines.append("")
        lines.append("Total Workers:")
        lines.append(f"  Simple:      {total_simple_workers:,}")
        lines.append(f"  Tiered:      {total_tiered_workers:,}")
        worker_diff = total_tiered_workers - total_simple_workers
        lines.append(f"  Difference:  {worker_diff:+,} (Tiered {'more' if worker_diff > 0 else 'fewer' if worker_diff < 0 else 'same'})")
        
        lines.append("")
        lines.append("Total CPUs:")
        lines.append(f"  Simple:      {total_simple_cpus:,}")
        lines.append(f"  Tiered:      {total_tiered_cpus:,}")
        cpu_diff = total_tiered_cpus - total_simple_cpus
        lines.append(f"  Difference:  {cpu_diff:+,} (Tiered {'more' if cpu_diff > 0 else 'fewer' if cpu_diff < 0 else 'same'})")
        
        lines.append("")
        lines.append("Total CPU Time:")
        lines.append(f"  Simple:      {self._format_time(total_simple_cpu_time)}")
        lines.append(f"  Tiered:      {self._format_time(total_tiered_cpu_time)}")
        cpu_time_diff = total_tiered_cpu_time - total_simple_cpu_time
        cpu_time_diff_str = f"{cpu_time_diff:+.1f}s" if abs(cpu_time_diff) < 60 else f"{cpu_time_diff/60:+.1f}m"
        lines.append(f"  Difference:  {cpu_time_diff_str} (Tiered {'more' if cpu_time_diff > 0 else 'less' if cpu_time_diff < 0 else 'same'})")
        
        # Add data size information
        total_simple_data_size = sum(c.simple_metrics.total_data_size_gb for c in comparisons)
        total_tiered_data_size = sum(c.tiered_metrics.total_data_size_gb for c in comparisons)
        
        lines.append("")
        lines.append("Total Data Size:")
        lines.append(f"  Simple:      {total_simple_data_size:.2f} GB")
        lines.append(f"  Tiered:      {total_tiered_data_size:.2f} GB")
        data_size_diff = total_tiered_data_size - total_simple_data_size
        lines.append(f"  Difference:  {data_size_diff:+.2f} GB (Tiered {'more' if data_size_diff > 0 else 'less' if data_size_diff < 0 else 'same'})")
        
        return "\n".join(lines)
    
    def save_comparison_report(self, comparisons: List[ComparisonResult], output_file: str, simple_exec_name: str = None, tiered_exec_name: str = None, simple_only: Set[str] = None, tiered_only: Set[str] = None, data_size_threshold: float = None):
        """Save the tabular comparison report to a text file."""
        report_text = self.generate_comparison_report(comparisons, simple_exec_name, tiered_exec_name, simple_only, tiered_only, data_size_threshold)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(report_text)
        
        print(f"Tabular comparison report saved to: {output_file}")

    def generate_html_report(self, comparisons: List[ComparisonResult], simple_exec_name: str = None, tiered_exec_name: str = None, simple_only: Set[str] = None, tiered_only: Set[str] = None, data_size_threshold: float = None, efficiency_threshold: float = None, extended_data_size_output: bool = False) -> str:
        """Generate an HTML comparison report for browser viewing."""
        if not comparisons:
            return "<html><body><h1>No comparisons to display.</h1></body></html>"
        
        # Calculate aggregate totals for summary
        total_simple_time = sum(c.simple_metrics.total_execution_time for c in comparisons)
        total_tiered_time = sum(c.tiered_metrics.total_execution_time for c in comparisons)
        total_simple_workers = sum(c.simple_metrics.total_workers for c in comparisons)
        total_tiered_workers = sum(c.tiered_metrics.total_workers for c in comparisons)
        total_simple_cpus = sum(c.simple_metrics.total_cpus for c in comparisons)
        total_tiered_cpus = sum(c.tiered_metrics.total_cpus for c in comparisons)
        total_simple_cpu_time = sum(c.simple_metrics.cpu_time for c in comparisons)
        total_tiered_cpu_time = sum(c.tiered_metrics.cpu_time for c in comparisons)
        total_simple_data_size = sum(c.simple_metrics.total_data_size_gb for c in comparisons)
        total_tiered_data_size = sum(c.tiered_metrics.total_data_size_gb for c in comparisons)
        
        # Generate timestamp
        from datetime import datetime
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        html = f"""<!DOCTYPE html>
<html>
<head>
    <title>Simple vs Tiered Migration Comparison</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 40px; background-color: #fafafa; }}
        h1, h2 {{ color: #333; }}
        .header {{ background-color: #e8f5e8; padding: 20px; border-radius: 8px; margin-bottom: 20px; }}
        .config {{ background-color: #f5f5f5; padding: 15px; border-radius: 5px; margin-bottom: 20px; }}
        .summary {{ background-color: #e8f4f8; padding: 15px; border-radius: 5px; margin-bottom: 20px; }}
        .aggregate {{ background-color: #fff3e0; padding: 15px; border-radius: 5px; margin-bottom: 20px; }}
        .config-comparison {{ background-color: #f0f8ff; padding: 15px; border-radius: 5px; margin-bottom: 20px; }}
        .config-same {{ background-color: #e8f5e8 !important; }}
        .config-different {{ background-color: #ffe8e8 !important; }}
        .has-stragglers {{ background-color: #ffcccc !important; }} /* Light red for cells with stragglers */
        .large-migration {{ background-color: #ffe4b5 !important; border-left: 4px solid #ff8c00; }} /* Light orange with orange border for large migrations */
        .low-efficiency {{ background-color: #ffff00 !important; font-weight: bold; }} /* Bright yellow for low efficiency migrations */
        
        table {{ border-collapse: collapse; width: 100%; margin-bottom: 20px; background-color: white; }}
        th, td {{ border: 1px solid #ddd; padding: 10px; text-align: left; }}
        th {{ background-color: #f2f2f2; font-weight: bold; }}
        tr:nth-child(even) {{ background-color: #f9f9f9; }}
        tr:hover {{ background-color: #f0f0f0; }}
        
        /* Thicker borders for metric group separators */
        .group-separator-left {{ border-left: 3px solid #333 !important; }}
        .group-separator-right {{ border-right: 3px solid #333 !important; }}
        
        .migration-details {{ margin-top: 20px; }}
        .best-time {{ background-color: #c8e6c9 !important; font-weight: bold; }}
        .best-ratio {{ background-color: #c8e6c9 !important; font-weight: bold; }}
        .positive-diff {{ background-color: #add8e6 !important; }} /* Light blue for positive differences */
        .negative-diff {{ background-color: #fff8dc !important; }} /* Light yellow for negative differences */
        
        .metric-section {{ margin-bottom: 30px; }}
        .exclusive-migrations {{ background-color: #fff8e1; padding: 15px; border-radius: 5px; margin-bottom: 20px; }}
        
        .ratio-header {{ font-size: 0.9em; }}
        .number {{ text-align: right; }}
    </style>
</head>
<body>
<div class="header">
    <h1>Simple vs Tiered Migration Simulation Comparison</h1>
    <p><strong>Generated:</strong> {timestamp}</p>
</div>

<div class="config">
    <h2>Comparison Details</h2>"""
        
        if simple_exec_name and tiered_exec_name:
            html += f"""
    <p><strong>Simple Execution:</strong> {simple_exec_name}</p>
    <p><strong>Tiered Execution:</strong> {tiered_exec_name}</p>
    <p><strong>Common Migrations:</strong> {len(comparisons)}</p>"""
        else:
            html += f"""
    <p><strong>Common Migrations:</strong> {len(comparisons)}</p>"""
        
        if data_size_threshold is not None:
            large_migrations = sum(1 for comp in comparisons if comp.simple_metrics.total_data_size_gb >= data_size_threshold)
            html += f"""
    <p><strong>Large Migration Threshold:</strong> {data_size_threshold:.2f} GB ({large_migrations} migrations)</p>"""
        
        if efficiency_threshold is not None:
            low_efficiency_migrations = sum(1 for comp in comparisons if comp.tiered_metrics.average_cpu_efficiency_percent > 0 and comp.tiered_metrics.average_cpu_efficiency_percent < efficiency_threshold)
            html += f"""
    <p><strong>Low Efficiency Threshold:</strong> {efficiency_threshold:.1f}% ({low_efficiency_migrations} migrations)</p>"""
        
        if simple_only or tiered_only:
            html += f"""</div>

<div class="exclusive-migrations">
    <h2>Exclusive Migrations</h2>"""
            if simple_only:
                html += f"""
    <p><strong>Simple Only ({len(simple_only)}):</strong> {', '.join(sorted(simple_only))}</p>"""
            if tiered_only:
                html += f"""
    <p><strong>Tiered Only ({len(tiered_only)}):</strong> {', '.join(sorted(tiered_only))}</p>"""
        
        html += f"""</div>

<div class="summary">
    <h2>Aggregate Analysis</h2>
    <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px;">
        <div>
            <h3>Total Execution Time</h3>
            <p><strong>Simple:</strong> {self._format_time(total_simple_time)}</p>
            <p><strong>Tiered:</strong> {self._format_time(total_tiered_time)}</p>
            <p><strong>Difference:</strong> {'+' if (total_tiered_time - total_simple_time) >= 0 else ''}{self._format_time(total_tiered_time - total_simple_time) if (total_tiered_time - total_simple_time) >= 0 else '-' + self._format_time(abs(total_tiered_time - total_simple_time))} (Tiered {'slower' if total_tiered_time > total_simple_time else 'faster' if total_tiered_time < total_simple_time else 'same'})</p>
        </div>
        <div>
            <h3>Total Workers</h3>
            <p><strong>Simple:</strong> {total_simple_workers:,}</p>
            <p><strong>Tiered:</strong> {total_tiered_workers:,}</p>
            <p><strong>Difference:</strong> {total_tiered_workers - total_simple_workers:+,} (Tiered {'more' if total_tiered_workers > total_simple_workers else 'fewer' if total_tiered_workers < total_simple_workers else 'same'})</p>
        </div>
        <div>
            <h3>Total CPUs</h3>
            <p><strong>Simple:</strong> {total_simple_cpus:,}</p>
            <p><strong>Tiered:</strong> {total_tiered_cpus:,}</p>
            <p><strong>Difference:</strong> {total_tiered_cpus - total_simple_cpus:+,} (Tiered {'more' if total_tiered_cpus > total_simple_cpus else 'fewer' if total_tiered_cpus < total_simple_cpus else 'same'})</p>
        </div>
        <div>
            <h3>Total CPU Time</h3>
            <p><strong>Simple:</strong> {self._format_time(total_simple_cpu_time)}</p>
            <p><strong>Tiered:</strong> {self._format_time(total_tiered_cpu_time)}</p>
            <p><strong>Difference:</strong> {'+' if (total_tiered_cpu_time - total_simple_cpu_time) >= 0 else ''}{self._format_time(total_tiered_cpu_time - total_simple_cpu_time) if (total_tiered_cpu_time - total_simple_cpu_time) >= 0 else '-' + self._format_time(abs(total_tiered_cpu_time - total_simple_cpu_time))} (Tiered {'more' if total_tiered_cpu_time > total_simple_cpu_time else 'less' if total_tiered_cpu_time < total_simple_cpu_time else 'same'})</p>
        </div>
        <div>
            <h3>Total Data Size</h3>
            <p><strong>Simple:</strong> {total_simple_data_size:.2f} GB</p>
            <p><strong>Tiered:</strong> {total_tiered_data_size:.2f} GB</p>
            <p><strong>Difference:</strong> {total_tiered_data_size - total_simple_data_size:+.2f} GB (Tiered {'more' if total_tiered_data_size > total_simple_data_size else 'less' if total_tiered_data_size < total_simple_data_size else 'same'})</p>
        </div>
    </div>
</div>

<div class="config-comparison">
    <h2>Configuration Comparison</h2>
    {self._generate_config_comparison_html(comparisons, simple_exec_name, tiered_exec_name)}
</div>

<div class="migration-details">
    <h2>Per-Migration Comparison</h2>
    <table>
        <thead>
            <tr>
                <th rowspan="2">Migration ID</th>"""
        
        # Conditionally add data size header columns
        if extended_data_size_output:
            html += """
                <th colspan="3" class="group-separator-left">Data Size (GB)</th>"""
        else:
            html += """
                <th class="group-separator-left">Data Size (GB)</th>"""
        
        html += """
                <th colspan="3" class="group-separator-left">Execution Time</th>
                <th colspan="3" class="group-separator-left">Workers</th>
                <th colspan="3" class="group-separator-left">CPUs</th>
                <th colspan="3" class="group-separator-left">CPU Time</th>
                <th colspan="3" class="group-separator-left">CPU Efficiency (Simple)</th>
                <th colspan="3" class="group-separator-left">CPU Efficiency (Tiered)</th>
                <th colspan="3" class="group-separator-left">Tiered Worker Distribution</th>
            </tr>
            <tr>"""
        
        # Conditionally add data size sub-header columns
        if extended_data_size_output:
            html += """
                <th class="group-separator-left">Simple</th>
                <th>Tiered</th>
                <th class="diff-header">T-S</th>"""
        else:
            html += """
                <th class="group-separator-left">Tiered</th>"""
        
        html += """
                <th class="group-separator-left">Simple</th>
                <th>Tiered</th>
                <th class="diff-header">T-S</th>
                <th class="group-separator-left">Simple</th>
                <th>Tiered</th>
                <th class="diff-header">T-S</th>
                <th class="group-separator-left">Simple</th>
                <th>Tiered</th>
                <th class="diff-header">T-S</th>
                <th class="group-separator-left">Simple</th>
                <th>Tiered</th>
                <th class="diff-header">T-S</th>
                <th class="group-separator-left">Active Time</th>
                <th>Eff %</th>
                <th>Waste %</th>
                <th class="group-separator-left">Active Time</th>
                <th>Eff %</th>
                <th>Waste %</th>
                <th class="group-separator-left">Small W</th>
                <th>Med W</th>
                <th>Large W</th>
            </tr>
        </thead>
        <tbody>"""
        
        # Sort comparisons by data size (descending) before processing
        comparisons_sorted = sorted(comparisons, key=lambda comp: comp.simple_metrics.total_data_size_gb, reverse=True)
        
        # Process each comparison and determine best values for highlighting
        for comp in comparisons_sorted:
            simple = comp.simple_metrics
            tiered = comp.tiered_metrics
            
            # Determine best execution time and CPU time (only when genuinely different)
            best_exec_time = "simple" if simple.total_execution_time < tiered.total_execution_time else ("tiered" if tiered.total_execution_time < simple.total_execution_time else None)
            best_cpu_time = "simple" if simple.cpu_time < tiered.cpu_time else ("tiered" if tiered.cpu_time < simple.cpu_time else None)
            
            # Format values
            simple_time_str = self._format_time(simple.total_execution_time)
            tiered_time_str = self._format_time(tiered.total_execution_time)
            simple_cpu_time_str = self._format_time(simple.cpu_time)
            tiered_cpu_time_str = self._format_time(tiered.cpu_time)
            
            # Format differences with appropriate sign and color
            exec_time_diff_str = self._format_time(abs(comp.execution_time_diff))
            exec_time_diff_class = "positive-diff" if comp.execution_time_diff > 0 else "negative-diff" if comp.execution_time_diff < 0 else ""
            
            worker_diff_str = f"{comp.worker_count_diff:+d}"
            worker_diff_class = "positive-diff" if comp.worker_count_diff > 0 else "negative-diff" if comp.worker_count_diff < 0 else ""
            
            cpu_diff_str = f"{comp.cpu_count_diff:+d}"
            cpu_diff_class = "positive-diff" if comp.cpu_count_diff > 0 else "negative-diff" if comp.cpu_count_diff < 0 else ""
            
            cpu_time_diff_str = self._format_time(abs(comp.cpu_time_diff))
            cpu_time_diff_class = "positive-diff" if comp.cpu_time_diff > 0 else "negative-diff" if comp.cpu_time_diff < 0 else ""
            
            # Add sign to time differences for display
            if comp.execution_time_diff > 0:
                exec_time_diff_str = f"+{exec_time_diff_str}"
            elif comp.execution_time_diff < 0:
                exec_time_diff_str = f"-{exec_time_diff_str}"
            
            if comp.cpu_time_diff > 0:
                cpu_time_diff_str = f"+{cpu_time_diff_str}"
            elif comp.cpu_time_diff < 0:
                cpu_time_diff_str = f"-{cpu_time_diff_str}"
            
            # Format worker counts with straggler information
            def format_worker_cell(workers, stragglers, tier_name):
                if workers == 0:
                    return ('0', '')
                elif stragglers > 0:
                    return (f'{workers:,}[{stragglers}]', 'has-stragglers')
                else:
                    return (f'{workers:,}', '')
            
            # Format tiered tier worker cells
            tiered_small_w, tiered_small_w_class = format_worker_cell(
                tiered.workers_by_tier.get('SMALL', 0), 
                tiered.stragglers_by_tier.get('SMALL', 0), 
                'SMALL'
            )
            tiered_medium_w, tiered_medium_w_class = format_worker_cell(
                tiered.workers_by_tier.get('MEDIUM', 0), 
                tiered.stragglers_by_tier.get('MEDIUM', 0), 
                'MEDIUM'
            )
            tiered_large_w, tiered_large_w_class = format_worker_cell(
                tiered.workers_by_tier.get('LARGE', 0), 
                tiered.stragglers_by_tier.get('LARGE', 0), 
                'LARGE'
            )

            # Check if this is a large migration
            is_large_migration = data_size_threshold is not None and simple.total_data_size_gb >= data_size_threshold
            migration_id_class = "large-migration" if is_large_migration else ""
            data_size_class = "large-migration" if is_large_migration else ""
            
            # Format CPU efficiency data for simple strategy
            simple_active_time_str = self._format_time(simple.total_active_cpu_time) if simple.total_active_cpu_time > 0 else "N/A"
            simple_efficiency_str = f"{simple.average_cpu_efficiency_percent:.1f}%" if simple.average_cpu_efficiency_percent > 0 else "N/A"
            simple_waste_percent = (simple.cpu_inefficiency / simple.total_used_cpu_time * 100) if simple.total_used_cpu_time > 0 else 0
            simple_waste_str = f"{simple_waste_percent:.1f}%" if simple_waste_percent > 0 else "N/A"
            
            # Format CPU efficiency data for tiered strategy
            tiered_active_time_str = self._format_time(tiered.total_active_cpu_time) if tiered.total_active_cpu_time > 0 else "N/A"
            tiered_efficiency_str = f"{tiered.average_cpu_efficiency_percent:.1f}%" if tiered.average_cpu_efficiency_percent > 0 else "N/A"
            tiered_waste_percent = (tiered.cpu_inefficiency / tiered.total_used_cpu_time * 100) if tiered.total_used_cpu_time > 0 else 0
            tiered_waste_str = f"{tiered_waste_percent:.1f}%" if tiered_waste_percent > 0 else "N/A"
            
            # Color code efficiency - green for high efficiency, red for low efficiency, bright yellow for threshold
            simple_efficiency_class = ""
            if simple.average_cpu_efficiency_percent > 0:
                if efficiency_threshold is not None and simple.average_cpu_efficiency_percent < efficiency_threshold:
                    simple_efficiency_class = "low-efficiency"  # Bright yellow for threshold violations
                elif simple.average_cpu_efficiency_percent >= 80:
                    simple_efficiency_class = "best-time"  # Green for high efficiency
                elif simple.average_cpu_efficiency_percent < 60:
                    simple_efficiency_class = "has-stragglers"  # Red for low efficiency
            
            tiered_efficiency_class = ""
            if tiered.average_cpu_efficiency_percent > 0:
                if efficiency_threshold is not None and tiered.average_cpu_efficiency_percent < efficiency_threshold:
                    tiered_efficiency_class = "low-efficiency"  # Bright yellow for threshold violations
                elif tiered.average_cpu_efficiency_percent >= 80:
                    tiered_efficiency_class = "best-time"  # Green for high efficiency
                elif tiered.average_cpu_efficiency_percent < 60:
                    tiered_efficiency_class = "has-stragglers"  # Red for low efficiency
            
            # Calculate data size difference and highlight mismatches
            data_size_diff = tiered.total_data_size_gb - simple.total_data_size_gb
            data_size_diff_str = f"{data_size_diff:+.2f}"
            data_size_diff_class = "positive-diff" if data_size_diff > 0.01 else "negative-diff" if data_size_diff < -0.01 else ""
            
            # Data size columns use base styling (no conditional red highlighting for mismatches)
            simple_data_size_class = data_size_class
            tiered_data_size_class = data_size_class
            
            # Calculate efficiency differences
            efficiency_diff = tiered.average_cpu_efficiency_percent - simple.average_cpu_efficiency_percent
            efficiency_diff_str = f"{efficiency_diff:+.1f}%" if simple.average_cpu_efficiency_percent > 0 and tiered.average_cpu_efficiency_percent > 0 else "N/A"
            efficiency_diff_class = "positive-diff" if efficiency_diff > 0 else "negative-diff" if efficiency_diff < 0 else ""
            
            html += f"""
            <tr>
                <td class="{migration_id_class}"><strong>{comp.migration_id}</strong></td>"""
            
            # Conditionally add data size columns
            if extended_data_size_output:
                html += f"""
                <td class="number group-separator-left {simple_data_size_class}">{simple.total_data_size_gb:.2f}</td>
                <td class="number {tiered_data_size_class}">{tiered.total_data_size_gb:.2f}</td>
                <td class="number {data_size_diff_class}">{data_size_diff_str}</td>"""
            else:
                html += f"""
                <td class="number group-separator-left {tiered_data_size_class}">{tiered.total_data_size_gb:.2f}</td>"""
            
            html += f"""
                <td class="number group-separator-left {'best-time' if best_exec_time == 'simple' else ''}">{simple_time_str}</td>
                <td class="number {'best-time' if best_exec_time == 'tiered' else ''}">{tiered_time_str}</td>
                <td class="number {exec_time_diff_class}">{exec_time_diff_str}</td>
                <td class="number group-separator-left">{simple.total_workers:,}</td>
                <td class="number">{tiered.total_workers:,}</td>
                <td class="number {worker_diff_class}">{worker_diff_str}</td>
                <td class="number group-separator-left">{simple.total_cpus:,}</td>
                <td class="number">{tiered.total_cpus:,}</td>
                <td class="number {cpu_diff_class}">{cpu_diff_str}</td>
                <td class="number group-separator-left {'best-time' if best_cpu_time == 'simple' else ''}">{simple_cpu_time_str}</td>
                <td class="number {'best-time' if best_cpu_time == 'tiered' else ''}">{tiered_cpu_time_str}</td>
                <td class="number {cpu_time_diff_class}">{cpu_time_diff_str}</td>
                <td class="number group-separator-left">{simple_active_time_str}</td>
                <td class="number {simple_efficiency_class}">{simple_efficiency_str}</td>
                <td class="number">{simple_waste_str}</td>
                <td class="number group-separator-left">{tiered_active_time_str}</td>
                <td class="number {tiered_efficiency_class}">{tiered_efficiency_str}</td>
                <td class="number">{tiered_waste_str}</td>
                <td class="number group-separator-left {tiered_small_w_class}">{tiered_small_w}</td>
                <td class="number {tiered_medium_w_class}">{tiered_medium_w}</td>
                <td class="number {tiered_large_w_class}">{tiered_large_w}</td>
            </tr>"""
        
        html += """
        </tbody>
    </table>
</div>

<div class="aggregate">
    <h2>Legend</h2>
    <p><span style="background-color: #c8e6c9; padding: 2px 6px; border-radius: 3px;">Green highlighting</span> indicates the best (lowest) execution time and CPU time for each migration.</p>
    <p><strong>Column Abbreviations:</strong></p>
    <ul>
        <li><strong>W:</strong> Workers (number of workers allocated to each tier)</li>
        <li><strong>C:</strong> CPUs/Cores (total threads allocated to each tier = workers × threads per worker)</li>
        <li><strong>Active Time:</strong> Total time spent on actual processing (both strategies)</li>
        <li><strong>Eff %:</strong> CPU efficiency percentage (Active Time / Total Used CPU Time)</li>
        <li><strong>Waste %:</strong> CPU waste percentage (idle thread time / Total Used CPU Time)</li>
    </ul>
    <p><strong>Straggler Information:</strong></p>
    <ul>
        <li><strong>Format:</strong> Total workers shown as "total[stragglers]" (e.g., "15[3]" = 15 workers, 3 stragglers)</li>
        <li><span style="background-color: #ffcccc; padding: 2px 6px; border-radius: 3px;">Light red background</span> indicates cells with straggler workers</li>
        <li><strong>Simple Strategy:</strong> No stragglers (1 thread per worker)</li>
    </ul>
    <p><strong>CPU Efficiency Color Coding:</strong></p>
    <ul>
        <li><span style="background-color: #c8e6c9; padding: 2px 6px; border-radius: 3px;">Green highlighting</span> indicates high CPU efficiency (≥80%)</li>
        <li><span style="background-color: #ffcccc; padding: 2px 6px; border-radius: 3px;">Light red background</span> indicates low CPU efficiency (<60%)</li>
        <li><strong>CPU Efficiency:</strong> Percentage of allocated CPU time that was actually used for processing</li>
        <li><strong>CPU Waste:</strong> Percentage of allocated CPU time that was idle due to thread imbalance</li>
    </ul>
    <p><strong>Difference Interpretation (T-S = Tiered - Simple):</strong></p>
    <ul>
        <li><span style="background-color: #add8e6; padding: 2px 6px; border-radius: 3px;">Light blue</span> indicates positive differences (Tiered > Simple)</li>
        <li><span style="background-color: #fff8dc; padding: 2px 6px; border-radius: 3px;">Light yellow</span> indicates negative differences (Tiered < Simple)</li>
        <li><strong>Positive execution time difference:</strong> Tiered took longer</li>
        <li><strong>Negative execution time difference:</strong> Tiered was faster</li>
        <li><strong>Positive worker/CPU difference:</strong> Tiered used more resources</li>
        <li><strong>Negative worker/CPU difference:</strong> Tiered used fewer resources</li>
    </ul>
    <p><strong>Data Size Validation:</strong></p>
    <ul>
        <li><strong>Expected:</strong> Simple and Tiered data sizes should be identical (0.00 difference)</li>
        <li><strong>Data size mismatches indicate:</strong> Potential data extraction issues or simulation errors</li>
        <li><strong>Normal tolerance:</strong> ≤0.01 GB difference is considered acceptable due to rounding</li>
    </ul>"""
        
        # Add information about data size column display
        if extended_data_size_output:
            html += """
    <p><strong>Data Size Display (Extended Mode):</strong></p>
    <ul>
        <li><strong>Extended data size output enabled:</strong> Shows Simple, Tiered, and T-S difference columns</li>
        <li><strong>Use case:</strong> For detailed validation and debugging of data size extraction issues</li>
    </ul>"""
        else:
            html += """
    <p><strong>Data Size Display (Default Mode):</strong></p>
    <ul>
        <li><strong>Default mode:</strong> Shows only Tiered data size column for cleaner presentation</li>
        <li><strong>Assumption:</strong> Simple and Tiered data sizes should be identical (processing same data)</li>
        <li><strong>To show full data size comparison:</strong> Use --extended-data-size-output flag</li>
    </ul>"""
        
        if data_size_threshold is not None:
            html += f"""
    <p><strong>Large Migration Highlighting:</strong></p>
    <ul>
        <li><span style="background-color: #ffe4b5; padding: 2px 6px; border-radius: 3px; border-left: 4px solid #ff8c00;">Light orange with orange border</span> indicates migrations with data size ≥ {data_size_threshold:.2f} GB</li>
    </ul>"""
        
        if efficiency_threshold is not None:
            html += f"""
    <p><strong>Low Efficiency Highlighting:</strong></p>
    <ul>
        <li><span style="background-color: #ffff00; padding: 2px 6px; border-radius: 3px; font-weight: bold;">Bright yellow</span> indicates tiered migrations with CPU efficiency < {efficiency_threshold:.1f}%</li>
    </ul>"""

        html += """
</body>
</html>"""
        
        return html
    
    def _generate_config_comparison_html(self, comparisons: List[ComparisonResult], simple_exec_name: str = None, tiered_exec_name: str = None) -> str:
        """Generate HTML for configuration comparison section."""
        if not comparisons:
            return "<p>No migrations available for configuration comparison.</p>"
        
        # Get configuration from first comparison (should be same across all migrations in an execution)
        first_comp = comparisons[0]
        simple_config = first_comp.simple_metrics.config
        tiered_config = first_comp.tiered_metrics.config
        
        simple_short = simple_exec_name if simple_exec_name else "Simple"
        tiered_short = tiered_exec_name if tiered_exec_name else "Tiered"
        
        # Define relevant configuration keys for each strategy
        simple_config_keys = [
            'max_concurrent_workers',
            'threads_per_worker',
            'worker_processing_time_unit',
            'enable_subset_size_cap',
            'enable_subset_num_sstable_cap'
        ]
        
        tiered_config_keys = [
            'small_tier_max_sstable_size_gb',
            'small_tier_thread_subset_max_size_floor_gb', 
            'small_tier_worker_num_threads',
            'medium_tier_max_sstable_size_gb',
            'medium_tier_worker_num_threads',
            'optimize_packing_medium_subsets',
            'execution_mode',
            'max_concurrent_workers'
        ]
        
        html = f"""
        <div style="display: flex; gap: 20px; flex-wrap: wrap;">
            <!-- Simple Configuration -->
            <div style="flex: 1; min-width: 300px;">
                <h4>{simple_short} Configuration</h4>
                <table style="width: 100%; border-collapse: collapse;">
                    <thead>
                        <tr>
                            <th style="text-align: left; padding: 8px; border: 1px solid #ddd; background-color: #f9f9f9;">Parameter</th>
                            <th style="text-align: center; padding: 8px; border: 1px solid #ddd; background-color: #f9f9f9;">Value</th>
                        </tr>
                    </thead>
                    <tbody>"""
        
        # Add simple configuration rows
        for key in simple_config_keys:
            value = first_comp._get_config_value(simple_config, key)
            display_value = value if value is not None else "N/A"
            html += f"""
                        <tr>
                            <td style="padding: 8px; border: 1px solid #ddd;"><strong>{key}</strong></td>
                            <td style="text-align: center; padding: 8px; border: 1px solid #ddd;">{display_value}</td>
                        </tr>"""
        
        html += """
                    </tbody>
                </table>
            </div>
            
            <!-- Tiered Configuration -->
            <div style="flex: 1; min-width: 300px;">"""
        
        html += f"""
                <h4>{tiered_short} Configuration</h4>
                <table style="width: 100%; border-collapse: collapse;">
                    <thead>
                        <tr>
                            <th style="text-align: left; padding: 8px; border: 1px solid #ddd; background-color: #f9f9f9;">Parameter</th>
                            <th style="text-align: center; padding: 8px; border: 1px solid #ddd; background-color: #f9f9f9;">Value</th>
                        </tr>
                    </thead>
                    <tbody>"""
        
        # Add tiered configuration rows
        for key in tiered_config_keys:
            value = first_comp._get_config_value(tiered_config, key)
            display_value = value if value is not None else "N/A"
            html += f"""
                        <tr>
                            <td style="padding: 8px; border: 1px solid #ddd;"><strong>{key}</strong></td>
                            <td style="text-align: center; padding: 8px; border: 1px solid #ddd;">{display_value}</td>
                        </tr>"""
        
        html += """
                    </tbody>
                </table>
            </div>
        </div>
        <p style="margin-top: 15px;"><strong>Note:</strong> Simple and Tiered strategies use different configuration parameters, so they are displayed separately for easy comparison.</p>"""
        
        return html

    def save_html_report(self, comparisons: List[ComparisonResult], output_file: str, simple_exec_name: str = None, tiered_exec_name: str = None, simple_only: Set[str] = None, tiered_only: Set[str] = None, data_size_threshold: float = None, efficiency_threshold: float = None, extended_data_size_output: bool = False):
        """Save the HTML comparison report to a file."""
        html_content = self.generate_html_report(comparisons, simple_exec_name, tiered_exec_name, simple_only, tiered_only, data_size_threshold, efficiency_threshold, extended_data_size_output)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        print(f"HTML comparison report saved to: {output_file}")
        print(f"Open in browser: file://{os.path.abspath(output_file)}")

def find_project_root() -> str:
    """Find the project root directory by looking for characteristic files/directories.
    
    Returns:
        Absolute path to the TieredStrategySimulation project root
        
    Raises:
        FileNotFoundError: If project root cannot be determined
    """
    # Start from the script's directory
    current_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Look for indicators that we're in the TieredStrategySimulation project root
    project_indicators = ['simple', 'tiered', 'comparison', 'utils']
    
    # Walk up the directory tree to find the project root
    search_dir = current_dir
    for _ in range(5):  # Limit search to 5 levels up
        # Check if this directory contains the expected project structure
        if all(os.path.exists(os.path.join(search_dir, indicator)) for indicator in project_indicators):
            return search_dir
        
        # Move up one directory
        parent_dir = os.path.dirname(search_dir)
        if parent_dir == search_dir:  # Reached filesystem root
            break
        search_dir = parent_dir
    
    # If not found by walking up, check if we're already in project root
    if all(os.path.exists(indicator) for indicator in project_indicators):
        return os.getcwd()
    
    raise FileNotFoundError(
        "Could not find TieredStrategySimulation project root. "
        "Please run this script from within the project directory."
    )

def main():
    parser = argparse.ArgumentParser(
        description="Compare Simple vs Tiered Migration Simulation Results",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Compare and save results to organized output directory (default - saves to output/simple-tiered/my_analysis/)
  python comparison/comparison_tool.py --simple-execution alice_test_run --tiered-execution test_new_5 --comparison-exec-name my_analysis
  
  # Compare with highlighting of large migrations (≥ 10 GB)
  python comparison/comparison_tool.py --simple-execution alice_test_run --tiered-execution test_new_5 --comparison-exec-name my_analysis --data-size-threshold 10.0
  
  # Compare with highlighting of both large migrations and low efficiency migrations
  python comparison/comparison_tool.py --simple-execution alice_test_run --tiered-execution test_new_5 --comparison-exec-name my_analysis --data-size-threshold 10.0 --efficiency-threshold 70.0
  
  # Compare with extended data size output (show simple, tiered, and difference columns)
  python comparison/comparison_tool.py --simple-execution alice_test_run --tiered-execution test_new_5 --comparison-exec-name my_analysis --extended-data-size-output
  
  # Compare without saving reports (console output only)
  python comparison/comparison_tool.py --simple-execution alice_test_run --tiered-execution test_new_5 --comparison-exec-name my_analysis --omit-reports
  
  # Alternative: specify full paths (backward compatibility)
  python comparison/comparison_tool.py --simple-path simple/output/alice_test_run --tiered-path tiered/output/test_new_5 --comparison-exec-name my_analysis
  
  # Can be run from anywhere within the project:
  cd comparison && python comparison_tool.py --simple-execution alice_test_run --tiered-execution test_new_5 --comparison-exec-name my_analysis
  cd TieredStrategySimulation && python comparison/comparison_tool.py --simple-execution alice_test_run --tiered-execution test_new_5 --comparison-exec-name my_analysis
        """
    )
    
    # Execution name arguments (recommended)
    parser.add_argument('--simple-execution', '-s',
                       help='Simple execution name (e.g., alice_test_run). Path will be: simple/output/{name}')
    parser.add_argument('--tiered-execution', '-t',
                       help='Tiered execution name (e.g., test_new_5). Path will be: tiered/output/{name}')
    
    # Full path arguments (backward compatibility)
    parser.add_argument('--simple-path',
                       help='Full path to simple simulation output directory')
    parser.add_argument('--tiered-path', 
                       help='Full path to tiered simulation output directory')
    
    # Comparison organization
    parser.add_argument('--comparison-exec-name', '-c',
                       help='Name for this comparison analysis (creates output/simple-tiered/{name}/ directory)')
    
    # Output options
    parser.add_argument('--omit-reports',
                       action='store_true',
                       help='Skip saving organized comparison reports (only show console output)')
    
    # Data size threshold for highlighting large migrations
    parser.add_argument('--data-size-threshold',
                       type=float,
                       help='Highlight migrations with data size >= this threshold (in GB). Example: --data-size-threshold 10.0')
    
    # CPU efficiency threshold for highlighting inefficient migrations
    parser.add_argument('--efficiency-threshold',
                       type=float,
                       help='Highlight tiered migrations with CPU efficiency < this threshold (in %%). Example: --efficiency-threshold 70.0')
    
    # Extended data size output option
    parser.add_argument('--extended-data-size-output',
                       action='store_true',
                       help='Show both simple and tiered data size columns with differences (default: only show tiered data size)')
    
    args = parser.parse_args()
    
    # No validation needed - organized reports are created automatically when comparison name is provided
    
    # Determine paths and execution names based on arguments
    simple_run_path = None
    tiered_run_path = None
    simple_exec_name = None
    tiered_exec_name = None
    
    if args.simple_execution and args.tiered_execution:
        # Use execution names (recommended approach)
        simple_exec_name = args.simple_execution
        tiered_exec_name = args.tiered_execution
        
        # Find project root and construct absolute paths
        try:
            project_root = find_project_root()
            simple_run_path = os.path.join(project_root, "simple", "output", simple_exec_name)
            tiered_run_path = os.path.join(project_root, "tiered", "output", tiered_exec_name)
            print(f"Comparing executions: {simple_exec_name} (simple) vs {tiered_exec_name} (tiered)")
            print(f"Project root: {project_root}")
        except FileNotFoundError as e:
            print(f"Error: {e}")
            sys.exit(1)
        
    elif args.simple_path and args.tiered_path:
        # Use full paths (backward compatibility)
        simple_run_path = args.simple_path
        tiered_run_path = args.tiered_path
        # Extract execution names from paths if possible
        simple_exec_name = Path(simple_run_path).name if simple_run_path else None
        tiered_exec_name = Path(tiered_run_path).name if tiered_run_path else None
        print(f"Comparing paths: {simple_run_path} vs {tiered_run_path}")
        
    else:
        print("Error: Must specify either:")
        print("  --simple-execution and --tiered-execution (recommended)")
        print("  OR --simple-path and --tiered-path")
        sys.exit(1)
    
    try:
        analyzer = ComparisonAnalyzer()
        comparisons, simple_only, tiered_only = analyzer.compare_runs(simple_run_path, tiered_run_path)
        
        if not comparisons:
            print("No common migrations found, but showing exclusive migrations if any exist.")
            if simple_only or tiered_only:
                # Show exclusive migrations even when no common migrations exist
                print(f"\nExclusive Migrations:")
                if simple_only:
                    print(f"  Simple Only ({len(simple_only)}): {sorted(simple_only)}")
                if tiered_only:
                    print(f"  Tiered Only ({len(tiered_only)}): {sorted(tiered_only)}")
            print("\nNo performance comparison data available due to lack of common migrations.")
            sys.exit(1)
        
        # Print summary to console
        analyzer.print_comparison_summary(comparisons, simple_exec_name, tiered_exec_name, simple_only, tiered_only)
        
        # Handle output file generation
        if args.comparison_exec_name and not args.omit_reports:
            # Generate organized output under simple-tiered directory (default behavior)
            try:
                project_root = find_project_root()
                output_dir = os.path.join(project_root, "comparison", "output", "simple-tiered", args.comparison_exec_name)
                os.makedirs(output_dir, exist_ok=True)
            except FileNotFoundError as e:
                print(f"Error: {e}")
                sys.exit(1)
            
            # Generate default filenames
            csv_file = f"{output_dir}/comparison_report_{args.comparison_exec_name}.csv"
            txt_file = f"{output_dir}/comparison_summary_{args.comparison_exec_name}.txt"
            html_file = f"{output_dir}/comparison_report_{args.comparison_exec_name}.html"
            
            # Save all report formats
            analyzer.save_comparison_csv(comparisons, csv_file, simple_exec_name, tiered_exec_name, args.data_size_threshold, args.efficiency_threshold)
            analyzer.save_comparison_report(comparisons, txt_file, simple_exec_name, tiered_exec_name, simple_only, tiered_only, args.data_size_threshold)
            analyzer.save_html_report(comparisons, html_file, simple_exec_name, tiered_exec_name, simple_only, tiered_only, args.data_size_threshold, args.efficiency_threshold, args.extended_data_size_output)
            
            print(f"Simple vs Tiered comparison analysis saved to: {output_dir}/")
        
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 