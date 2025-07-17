#!/usr/bin/env python3
"""
Tiered vs Tiered Migration Simulation Comparison Tool

This script compares the execution results between two different tiered migration simulations
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
class TieredMigrationMetrics:
    """Stores metrics for a single tiered migration execution."""
    migration_id: str
    execution_name: str  # Name to identify this execution
    total_execution_time: float
    total_workers: int
    total_cpus: int  # threads across all workers
    cpu_time: float  # total thread time used (current calculation)
    total_data_size_gb: float  # total data size processed in GB
    workers_by_tier: Dict[str, int]  # tier -> worker count
    cpus_by_tier: Dict[str, int]  # tier -> total threads
    stragglers_by_tier: Dict[str, int]  # tier -> straggler worker count
    config: Dict[str, any]  # simulation and migration configuration
    # CPU Efficiency metrics
    total_used_cpu_time: float = 0.0  # total allocated CPU time (duration × threads)
    total_active_cpu_time: float = 0.0  # total actual processing time
    cpu_inefficiency: float = 0.0  # idle/wasted CPU time
    average_cpu_efficiency_percent: float = 0.0  # average efficiency across workers
    
@dataclass
class TieredComparisonResult:
    """Stores comparison results between two tiered strategy executions."""
    migration_id: str
    exec1_metrics: TieredMigrationMetrics
    exec2_metrics: TieredMigrationMetrics
    
    @property
    def execution_time_ratio(self) -> float:
        """Ratio of exec2 execution time to exec1 execution time."""
        if self.exec1_metrics.total_execution_time == 0:
            return float('inf') if self.exec2_metrics.total_execution_time > 0 else 1.0
        return self.exec2_metrics.total_execution_time / self.exec1_metrics.total_execution_time
    
    @property
    def execution_time_ratio_inverse(self) -> float:
        """Ratio of exec1 execution time to exec2 execution time."""
        if self.exec2_metrics.total_execution_time == 0:
            return float('inf') if self.exec1_metrics.total_execution_time > 0 else 1.0
        return self.exec1_metrics.total_execution_time / self.exec2_metrics.total_execution_time
    
    @property
    def worker_count_ratio(self) -> float:
        """Ratio of exec2 worker count to exec1 worker count."""
        if self.exec1_metrics.total_workers == 0:
            return float('inf') if self.exec2_metrics.total_workers > 0 else 1.0
        return self.exec2_metrics.total_workers / self.exec1_metrics.total_workers
    
    @property
    def worker_count_ratio_inverse(self) -> float:
        """Ratio of exec1 worker count to exec2 worker count."""
        if self.exec2_metrics.total_workers == 0:
            return float('inf') if self.exec1_metrics.total_workers > 0 else 1.0
        return self.exec1_metrics.total_workers / self.exec2_metrics.total_workers
    
    @property
    def cpu_count_ratio(self) -> float:
        """Ratio of exec2 CPU count to exec1 CPU count."""
        if self.exec1_metrics.total_cpus == 0:
            return float('inf') if self.exec2_metrics.total_cpus > 0 else 1.0
        return self.exec2_metrics.total_cpus / self.exec1_metrics.total_cpus
    
    @property
    def cpu_count_ratio_inverse(self) -> float:
        """Ratio of exec1 CPU count to exec2 CPU count."""
        if self.exec2_metrics.total_cpus == 0:
            return float('inf') if self.exec1_metrics.total_cpus > 0 else 1.0
        return self.exec1_metrics.total_cpus / self.exec2_metrics.total_cpus
    
    @property
    def cpu_time_ratio(self) -> float:
        """Ratio of exec2 CPU time to exec1 CPU time."""
        if self.exec1_metrics.cpu_time == 0:
            return float('inf') if self.exec2_metrics.cpu_time > 0 else 1.0
        return self.exec2_metrics.cpu_time / self.exec1_metrics.cpu_time
    
    @property
    def cpu_time_ratio_inverse(self) -> float:
        """Ratio of exec1 CPU time to exec2 CPU time."""
        if self.exec2_metrics.cpu_time == 0:
            return float('inf') if self.exec1_metrics.cpu_time > 0 else 1.0
        return self.exec1_metrics.cpu_time / self.exec2_metrics.cpu_time
    
    # Difference properties (exec2 - exec1)
    @property
    def execution_time_diff(self) -> float:
        """Difference in execution time (exec2 - exec1)."""
        return self.exec2_metrics.total_execution_time - self.exec1_metrics.total_execution_time
    
    @property
    def worker_count_diff(self) -> int:
        """Difference in worker count (exec2 - exec1)."""
        return self.exec2_metrics.total_workers - self.exec1_metrics.total_workers
    
    @property
    def cpu_count_diff(self) -> int:
        """Difference in CPU count (exec2 - exec1)."""
        return self.exec2_metrics.total_cpus - self.exec1_metrics.total_cpus
    
    @property
    def cpu_time_diff(self) -> float:
        """Difference in CPU time (exec2 - exec1)."""
        return self.exec2_metrics.cpu_time - self.exec1_metrics.cpu_time
    
    @property
    def data_size_diff(self) -> float:
        """Difference in data size (exec2 - exec1)."""
        return self.exec2_metrics.total_data_size_gb - self.exec1_metrics.total_data_size_gb
    
    def get_config_comparison(self, config_keys: List[str]) -> Dict[str, any]:
        """Compare specific configuration parameters between executions."""
        comparison = {}
        
        for key in config_keys:
            exec1_value = self._get_config_value(self.exec1_metrics.config, key)
            exec2_value = self._get_config_value(self.exec2_metrics.config, key)
            
            comparison[key] = {
                'exec1': exec1_value,
                'exec2': exec2_value,
                'same': exec1_value == exec2_value
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
            'medium_tier_thread_subset_max_size_floor_gb',
            'optimize_packing_medium_subsets',
            'skip_small_subsets',
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

class TieredSimulationDataExtractor:
    """Extracts metrics from tiered simulation output files."""
    
    def extract_tiered_metrics(self, tiered_run_path: str, execution_name: str = None) -> Dict[str, TieredMigrationMetrics]:
        """Extract metrics from tiered simulation output."""
        tiered_path = Path(tiered_run_path)
        if not tiered_path.exists():
            raise FileNotFoundError(f"Tiered run path not found: {tiered_run_path}")
        
        # Use the directory name as execution name if not provided
        if execution_name is None:
            execution_name = tiered_path.name
        
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
                    migration_metrics = self._parse_tiered_json(migration_id, json_files[0], execution_name, execution_config)
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
        
        return execution_config
    
    def _parse_tiered_json(self, migration_id: str, json_file: Path, execution_name: str, execution_config: Dict[str, any] = None) -> Optional[TieredMigrationMetrics]:
        """Parse tiered simulation JSON and CSV files to extract actual metrics."""
        try:
            # Parse JSON for basic metrics
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            total_execution_time = data.get('total_execution_time', 0.0)
            simulation_config = data.get('simulation_config', {})
            by_tier = data.get('by_tier', {})
            
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
                    # Initialize empty tiers
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
                        
                        # Extract data size from CSV (with safety check)
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
            
            # Combine simulation config from JSON with execution config
            combined_config = {
                'simulation': simulation_config,
                'migration': execution_config or {}
            }
            
            return TieredMigrationMetrics(
                migration_id=migration_id,
                execution_name=execution_name,
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
                config=combined_config
            )
            
        except Exception as e:
            print(f"Error parsing tiered data for {migration_id}: {e}")
            return None

class TieredComparisonAnalyzer:
    """Analyzes and compares tiered simulation results."""
    
    def __init__(self):
        self.extractor = TieredSimulationDataExtractor()
    
    def compare_runs(self, exec1_run_path: str, exec2_run_path: str, exec1_name: str = None, exec2_name: str = None) -> Tuple[List[TieredComparisonResult], Set[str], Set[str]]:
        """Compare two tiered simulation runs.
        
        Returns:
            Tuple of (comparisons, exec1_only_migrations, exec2_only_migrations)
        """
        print(f"Extracting exec1 metrics from: {exec1_run_path}")
        exec1_metrics = self.extractor.extract_tiered_metrics(exec1_run_path, exec1_name)
        
        print(f"Extracting exec2 metrics from: {exec2_run_path}")
        exec2_metrics = self.extractor.extract_tiered_metrics(exec2_run_path, exec2_name)
        
        print(f"Found {len(exec1_metrics)} exec1 migrations and {len(exec2_metrics)} exec2 migrations")
        
        # Find common and exclusive migrations
        exec1_migrations = set(exec1_metrics.keys())
        exec2_migrations = set(exec2_metrics.keys())
        common_migrations = exec1_migrations.intersection(exec2_migrations)
        exec1_only_migrations = exec1_migrations - exec2_migrations
        exec2_only_migrations = exec2_migrations - exec1_migrations
        
        print(f"Common migrations: {sorted(common_migrations)}")
        
        if not common_migrations:
            print("Warning: No common migrations found between the two runs")
            return [], exec1_only_migrations, exec2_only_migrations
        
        # Create comparison results
        comparisons = []
        for migration_id in sorted(common_migrations):
            comparison = TieredComparisonResult(
                migration_id=migration_id,
                exec1_metrics=exec1_metrics[migration_id],
                exec2_metrics=exec2_metrics[migration_id]
            )
            comparisons.append(comparison)
        
        return comparisons, exec1_only_migrations, exec2_only_migrations
    
    def print_comparison_summary(self, comparisons: List[TieredComparisonResult], exec1_name: str = None, exec2_name: str = None, exec1_only: Set[str] = None, exec2_only: Set[str] = None, data_size_threshold: float = None):
        """Print a summary of the comparison results."""
        if not comparisons:
            print("No comparisons to display.")
            return
        
        print("\n" + "="*120)
        print("TIERED vs TIERED SIMULATION COMPARISON SUMMARY")
        print("="*120)
        
        if exec1_name and exec2_name:
            print(f"\nComparison Details:")
            print(f"  Execution 1:       {exec1_name}")
            print(f"  Execution 2:       {exec2_name}")
            print(f"  Common Migrations: {len(comparisons)}")
        else:
            print(f"\nCommon Migrations: {len(comparisons)}")
        
        # Data size threshold information
        if data_size_threshold is not None:
            large_migrations = [comp for comp in comparisons if comp.exec1_metrics.total_data_size_gb >= data_size_threshold]
            print(f"  Large Migrations:  {len(large_migrations)} (>= {data_size_threshold} GB)")
        
        # Show exclusive migrations if any exist
        if exec1_only or exec2_only:
            print(f"\nExclusive Migrations:")
            if exec1_only:
                print(f"  Exec1 Only ({len(exec1_only)}): {sorted(exec1_only)}")
            if exec2_only:
                print(f"  Exec2 Only ({len(exec2_only)}): {sorted(exec2_only)}")
        
        # Header - adjust for shorter names
        exec1_short = (exec1_name[:8] + "..") if exec1_name and len(exec1_name) > 10 else (exec1_name or "Exec1")
        exec2_short = (exec2_name[:8] + "..") if exec2_name and len(exec2_name) > 10 else (exec2_name or "Exec2")
        
        print(f"\n{'Migration':<12} {'Data':<8} {'Execution Time':<35} {'Workers':<30} {'CPUs':<30} {'CPU Time':<35}")
        print(f"{'ID':<12} {'Size':<8} {exec1_short:<10} {exec2_short:<10} {'2/1':<5} {'1/2':<5} {exec1_short:<8} {exec2_short:<8} {'2/1':<5} {'1/2':<5} {exec1_short:<8} {exec2_short:<8} {'2/1':<5} {'1/2':<5} {exec1_short:<10} {exec2_short:<10} {'2/1':<5} {'1/2':<5}")
        print(f"{'':12} {'(GB)':<8} {'-'*35} {'-'*30} {'-'*30} {'-'*35}")
        
        # Data rows
        for comp in comparisons:
            exec1 = comp.exec1_metrics
            exec2 = comp.exec2_metrics
            
            # Format large numbers
            exec1_time = self._format_time(exec1.total_execution_time)
            exec2_time = self._format_time(exec2.total_execution_time)
            time_ratio_21 = f"{comp.execution_time_ratio:.2f}"
            time_ratio_12 = f"{comp.execution_time_ratio_inverse:.2f}"
            
            exec1_cpu_time = self._format_time(exec1.cpu_time)
            exec2_cpu_time = self._format_time(exec2.cpu_time)
            cpu_time_ratio_21 = f"{comp.cpu_time_ratio:.2f}"
            cpu_time_ratio_12 = f"{comp.cpu_time_ratio_inverse:.2f}"
            
            worker_ratio_21 = f"{comp.worker_count_ratio:.2f}"
            worker_ratio_12 = f"{comp.worker_count_ratio_inverse:.2f}"
            
            cpu_ratio_21 = f"{comp.cpu_count_ratio:.2f}"
            cpu_ratio_12 = f"{comp.cpu_count_ratio_inverse:.2f}"
            
            # Data size (should be same for both executions)
            data_size_str = f"{exec1.total_data_size_gb:.1f}"
            
            print(f"{comp.migration_id:<12} {data_size_str:<8} {exec1_time:<10} {exec2_time:<10} {time_ratio_21:<5} {time_ratio_12:<5} "
                  f"{exec1.total_workers:<8} {exec2.total_workers:<8} {worker_ratio_21:<5} {worker_ratio_12:<5} "
                  f"{exec1.total_cpus:<8} {exec2.total_cpus:<8} {cpu_ratio_21:<5} {cpu_ratio_12:<5} "
                  f"{exec1_cpu_time:<10} {exec2_cpu_time:<10} {cpu_time_ratio_21:<5} {cpu_time_ratio_12:<5}")
        
        # Summary statistics
        print("\n" + "="*125)
        print("AGGREGATE ANALYSIS")
        print("="*125)
        
        total_exec1_time = sum(c.exec1_metrics.total_execution_time for c in comparisons)
        total_exec2_time = sum(c.exec2_metrics.total_execution_time for c in comparisons)
        
        total_exec1_workers = sum(c.exec1_metrics.total_workers for c in comparisons)
        total_exec2_workers = sum(c.exec2_metrics.total_workers for c in comparisons)
        
        total_exec1_cpus = sum(c.exec1_metrics.total_cpus for c in comparisons)
        total_exec2_cpus = sum(c.exec2_metrics.total_cpus for c in comparisons)
        
        total_exec1_cpu_time = sum(c.exec1_metrics.cpu_time for c in comparisons)
        total_exec2_cpu_time = sum(c.exec2_metrics.cpu_time for c in comparisons)
        
        print("")
        print("Total Execution Time:")
        print(f"  Exec1:      {self._format_time(total_exec1_time)}")
        print(f"  Exec2:      {self._format_time(total_exec2_time)}")
        if total_exec1_time > 0:
            print(f"  Exec2/Exec1: {total_exec2_time/total_exec1_time:.2f} (efficiency: <1.0 = exec2 faster)") 
            print(f"  Exec1/Exec2: {total_exec1_time/total_exec2_time:.2f} (speedup: >1.0 = exec2 faster)")
        
        print("")
        print("Total Workers:")
        print(f"  Exec1:      {total_exec1_workers}")
        print(f"  Exec2:      {total_exec2_workers}")
        if total_exec1_workers > 0:
            print(f"  Exec2/Exec1: {total_exec2_workers/total_exec1_workers:.2f}")
        if total_exec2_workers > 0:
            print(f"  Exec1/Exec2: {total_exec1_workers/total_exec2_workers:.2f}")
        
        print("")
        print("Total CPUs:")
        print(f"  Exec1:      {total_exec1_cpus}")
        print(f"  Exec2:      {total_exec2_cpus}")
        if total_exec1_cpus > 0:
            print(f"  Exec2/Exec1: {total_exec2_cpus/total_exec1_cpus:.2f}")
        if total_exec2_cpus > 0:
            print(f"  Exec1/Exec2: {total_exec1_cpus/total_exec2_cpus:.2f}")
        
        print("")
        print("Total CPU Time:")
        print(f"  Exec1:      {self._format_time(total_exec1_cpu_time)}")
        print(f"  Exec2:      {self._format_time(total_exec2_cpu_time)}")
        if total_exec1_cpu_time > 0:
            print(f"  Exec2/Exec1: {total_exec2_cpu_time/total_exec1_cpu_time:.2f}")
        if total_exec2_cpu_time > 0:
            print(f"  Exec1/Exec2: {total_exec1_cpu_time/total_exec2_cpu_time:.2f}")
            
        # Tier-by-tier breakdown
        print("\n" + "="*125)
        print("TIER-BY-TIER BREAKDOWN")
        print("="*125)
        
        # Calculate tier totals
        tier_totals_exec1 = {'SMALL': 0, 'MEDIUM': 0, 'LARGE': 0}
        tier_totals_exec2 = {'SMALL': 0, 'MEDIUM': 0, 'LARGE': 0}
        tier_cpu_totals_exec1 = {'SMALL': 0, 'MEDIUM': 0, 'LARGE': 0}
        tier_cpu_totals_exec2 = {'SMALL': 0, 'MEDIUM': 0, 'LARGE': 0}
        
        for comp in comparisons:
            for tier in ['SMALL', 'MEDIUM', 'LARGE']:
                tier_totals_exec1[tier] += comp.exec1_metrics.workers_by_tier.get(tier, 0)
                tier_totals_exec2[tier] += comp.exec2_metrics.workers_by_tier.get(tier, 0)
                tier_cpu_totals_exec1[tier] += comp.exec1_metrics.cpus_by_tier.get(tier, 0)
                tier_cpu_totals_exec2[tier] += comp.exec2_metrics.cpus_by_tier.get(tier, 0)
        
        print(f"\n{'Tier':<8} {'Workers':<25} {'CPUs':<25}")
        print(f"{'Name':<8} {'Exec1':<8} {'Exec2':<8} {'Ratio':<8} {'Exec1':<8} {'Exec2':<8} {'Ratio':<8}")
        print("-" * 65)
        
        for tier in ['SMALL', 'MEDIUM', 'LARGE']:
            exec1_workers = tier_totals_exec1[tier]
            exec2_workers = tier_totals_exec2[tier]
            exec1_cpus = tier_cpu_totals_exec1[tier]
            exec2_cpus = tier_cpu_totals_exec2[tier]
            
            worker_ratio = f"{exec2_workers/exec1_workers:.2f}" if exec1_workers > 0 else "N/A"
            cpu_ratio = f"{exec2_cpus/exec1_cpus:.2f}" if exec1_cpus > 0 else "N/A"
            
            print(f"{tier:<8} {exec1_workers:<8} {exec2_workers:<8} {worker_ratio:<8} {exec1_cpus:<8} {exec2_cpus:<8} {cpu_ratio:<8}")
    
    def _format_time(self, time_units: float) -> str:
        """Format time values for readable display."""
        if time_units >= 1000000000:  # billions
            return f"{time_units/1000000000:.1f}B"
        elif time_units >= 1000000:  # millions
            return f"{time_units/1000000:.1f}M"
        elif time_units >= 1000:  # thousands
            return f"{time_units/1000:.1f}K"
        else:
            return f"{time_units:.1f}"
    
    def save_comparison_csv(self, comparisons: List[TieredComparisonResult], output_file: str, exec1_name: str = None, exec2_name: str = None, data_size_threshold: float = None, efficiency_threshold: float = None):
        """Save comparison results to CSV file."""
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            # Write header comments
            csvfile.write("# Tiered vs Tiered Migration Simulation Comparison\n")
            csvfile.write(f"# Execution 1: {exec1_name or 'Unknown'}\n")
            csvfile.write(f"# Execution 2: {exec2_name or 'Unknown'}\n")
            csvfile.write(f"# Common Migrations: {len(comparisons)}\n")
            
            # Data size threshold information
            if data_size_threshold is not None:
                large_migrations = [comp for comp in comparisons if comp.exec1_metrics.total_data_size_gb >= data_size_threshold]
                csvfile.write(f"# Large Migrations: {len(large_migrations)} (>= {data_size_threshold} GB)\n")
                
            # Efficiency threshold information
            if efficiency_threshold is not None:
                low_efficiency_exec1 = sum(1 for comp in comparisons if comp.exec1_metrics.average_cpu_efficiency_percent > 0 and comp.exec1_metrics.average_cpu_efficiency_percent < efficiency_threshold)
                low_efficiency_exec2 = sum(1 for comp in comparisons if comp.exec2_metrics.average_cpu_efficiency_percent > 0 and comp.exec2_metrics.average_cpu_efficiency_percent < efficiency_threshold)
                csvfile.write(f"# Low Efficiency Threshold: {efficiency_threshold:.1f}% (Exec1: {low_efficiency_exec1}, Exec2: {low_efficiency_exec2} migrations)\n")
                
            csvfile.write("# Diff columns show Exec2 - Exec1 (positive = Exec2 higher, negative = Exec2 lower)\n")
            csvfile.write("# Data size is the same for both executions (same data processed)\n")
            csvfile.write("#\n")
            
            # Write configuration comparison
            if comparisons:
                csvfile.write("# CONFIGURATION COMPARISON\n")
                config_keys = [
                    'small_tier_max_sstable_size_gb',
                    'small_tier_thread_subset_max_size_floor_gb', 
                    'small_tier_worker_num_threads',
                    'medium_tier_max_sstable_size_gb',
                    'medium_tier_worker_num_threads',
                    'optimize_packing_medium_subsets',
                    'execution_mode',
                    'max_concurrent_workers'
                ]
                first_comp = comparisons[0]
                config_comparison = first_comp.get_config_comparison(config_keys)
                
                for key, comparison in config_comparison.items():
                    exec1_value = comparison['exec1'] if comparison['exec1'] is not None else 'N/A'
                    exec2_value = comparison['exec2'] if comparison['exec2'] is not None else 'N/A'
                    status = 'Same' if comparison['same'] else 'Different'
                    csvfile.write(f"# {key}: {exec1_value} vs {exec2_value} ({status})\n")
            
            csvfile.write("\n")
            
            # CSV header
            fieldnames = [
                'Migration_ID', 'Data_Size_GB',
                'Exec1_Execution_Time', 'Exec2_Execution_Time', 'Execution_Time_Ex2_minus_Ex1',
                'Exec1_Workers', 'Exec2_Workers', 'Worker_Ex2_minus_Ex1',
                'Exec1_CPUs', 'Exec2_CPUs', 'CPU_Ex2_minus_Ex1',
                'Exec1_CPU_Time', 'Exec2_CPU_Time', 'CPU_Time_Ex2_minus_Ex1',
                'Exec1_CPU_Active_Time', 'Exec1_CPU_Efficiency_Percent', 'Exec1_CPU_Waste_Percent',
                'Exec2_CPU_Active_Time', 'Exec2_CPU_Efficiency_Percent', 'Exec2_CPU_Waste_Percent',
                'Exec1_Small_Workers', 'Exec1_Medium_Workers', 'Exec1_Large_Workers',
                'Exec1_Small_Stragglers', 'Exec1_Medium_Stragglers', 'Exec1_Large_Stragglers',
                'Exec1_Small_CPUs', 'Exec1_Medium_CPUs', 'Exec1_Large_CPUs',
                'Exec2_Small_Workers', 'Exec2_Medium_Workers', 'Exec2_Large_Workers',
                'Exec2_Small_Stragglers', 'Exec2_Medium_Stragglers', 'Exec2_Large_Stragglers',
                'Exec2_Small_CPUs', 'Exec2_Medium_CPUs', 'Exec2_Large_CPUs'
            ]
            
            # Add Is_Large_Migration column if threshold is specified
            if data_size_threshold is not None:
                fieldnames.append('Is_Large_Migration')
                
            # Add efficiency flag columns if threshold is specified
            if efficiency_threshold is not None:
                fieldnames.extend(['Exec1_Is_Low_Efficiency', 'Exec2_Is_Low_Efficiency'])
            
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            # Write data rows
            for comp in comparisons:
                exec1 = comp.exec1_metrics
                exec2 = comp.exec2_metrics
                
                # Calculate efficiency percentages
                exec1_waste_percent = (exec1.cpu_inefficiency / exec1.total_used_cpu_time * 100) if exec1.total_used_cpu_time > 0 else 0
                exec2_waste_percent = (exec2.cpu_inefficiency / exec2.total_used_cpu_time * 100) if exec2.total_used_cpu_time > 0 else 0
                
                row = {
                    'Migration_ID': comp.migration_id,
                    'Data_Size_GB': f"{exec1.total_data_size_gb:.2f}",
                    'Exec1_Execution_Time': f"{exec1.total_execution_time:.2f}",
                    'Exec2_Execution_Time': f"{exec2.total_execution_time:.2f}",
                    'Execution_Time_Ex2_minus_Ex1': f"{comp.execution_time_diff:.2f}",
                    'Exec1_Workers': exec1.total_workers,
                    'Exec2_Workers': exec2.total_workers,
                    'Worker_Ex2_minus_Ex1': comp.worker_count_diff,
                    'Exec1_CPUs': exec1.total_cpus,
                    'Exec2_CPUs': exec2.total_cpus,
                    'CPU_Ex2_minus_Ex1': comp.cpu_count_diff,
                    'Exec1_CPU_Time': f"{exec1.cpu_time:.2f}",
                    'Exec2_CPU_Time': f"{exec2.cpu_time:.2f}",
                    'CPU_Time_Ex2_minus_Ex1': f"{comp.cpu_time_diff:.2f}",
                    'Exec1_CPU_Active_Time': f"{exec1.total_active_cpu_time:.2f}",
                    'Exec1_CPU_Efficiency_Percent': f"{exec1.average_cpu_efficiency_percent:.2f}",
                    'Exec1_CPU_Waste_Percent': f"{exec1_waste_percent:.2f}",
                    'Exec2_CPU_Active_Time': f"{exec2.total_active_cpu_time:.2f}",
                    'Exec2_CPU_Efficiency_Percent': f"{exec2.average_cpu_efficiency_percent:.2f}",
                    'Exec2_CPU_Waste_Percent': f"{exec2_waste_percent:.2f}",
                    'Exec1_Small_Workers': exec1.workers_by_tier.get('SMALL', 0),
                    'Exec1_Medium_Workers': exec1.workers_by_tier.get('MEDIUM', 0),
                    'Exec1_Large_Workers': exec1.workers_by_tier.get('LARGE', 0),
                    'Exec1_Small_Stragglers': exec1.stragglers_by_tier.get('SMALL', 0),
                    'Exec1_Medium_Stragglers': exec1.stragglers_by_tier.get('MEDIUM', 0),
                    'Exec1_Large_Stragglers': exec1.stragglers_by_tier.get('LARGE', 0),
                    'Exec1_Small_CPUs': exec1.cpus_by_tier.get('SMALL', 0),
                    'Exec1_Medium_CPUs': exec1.cpus_by_tier.get('MEDIUM', 0),
                    'Exec1_Large_CPUs': exec1.cpus_by_tier.get('LARGE', 0),
                    'Exec2_Small_Workers': exec2.workers_by_tier.get('SMALL', 0),
                    'Exec2_Medium_Workers': exec2.workers_by_tier.get('MEDIUM', 0),
                    'Exec2_Large_Workers': exec2.workers_by_tier.get('LARGE', 0),
                    'Exec2_Small_Stragglers': exec2.stragglers_by_tier.get('SMALL', 0),
                    'Exec2_Medium_Stragglers': exec2.stragglers_by_tier.get('MEDIUM', 0),
                    'Exec2_Large_Stragglers': exec2.stragglers_by_tier.get('LARGE', 0),
                    'Exec2_Small_CPUs': exec2.cpus_by_tier.get('SMALL', 0),
                    'Exec2_Medium_CPUs': exec2.cpus_by_tier.get('MEDIUM', 0),
                    'Exec2_Large_CPUs': exec2.cpus_by_tier.get('LARGE', 0),
                }
                
                # Add large migration flag if threshold is specified
                if data_size_threshold is not None:
                    row['Is_Large_Migration'] = 'Yes' if exec1.total_data_size_gb >= data_size_threshold else 'No'
                    
                # Add efficiency flags if threshold is specified
                if efficiency_threshold is not None:
                    row['Exec1_Is_Low_Efficiency'] = 'Yes' if (exec1.average_cpu_efficiency_percent > 0 and exec1.average_cpu_efficiency_percent < efficiency_threshold) else 'No'
                    row['Exec2_Is_Low_Efficiency'] = 'Yes' if (exec2.average_cpu_efficiency_percent > 0 and exec2.average_cpu_efficiency_percent < efficiency_threshold) else 'No'
                
                writer.writerow(row)
        
        print(f"CSV comparison report saved to: {output_file}")
    
    def generate_comparison_report(self, comparisons: List[TieredComparisonResult], exec1_name: str = None, exec2_name: str = None, exec1_only: Set[str] = None, exec2_only: Set[str] = None, data_size_threshold: float = None) -> str:
        """Generate a formatted text report of the comparison results."""
        lines = []
        
        if not comparisons:
            lines.append("No comparisons to display.")
            return "\n".join(lines)
        
        lines.append("="*120)
        lines.append("TIERED vs TIERED SIMULATION COMPARISON SUMMARY")
        lines.append("="*120)
        
        if exec1_name and exec2_name:
            lines.append("")
            lines.append("Comparison Details:")
            lines.append(f"  Execution 1:       {exec1_name}")
            lines.append(f"  Execution 2:       {exec2_name}")
            lines.append(f"  Common Migrations: {len(comparisons)}")
        else:
            lines.append(f"\nCommon Migrations: {len(comparisons)}")
        
        # Data size threshold information
        if data_size_threshold is not None:
            large_migrations = [comp for comp in comparisons if comp.exec1_metrics.total_data_size_gb >= data_size_threshold]
            lines.append(f"  Large Migrations:  {len(large_migrations)} (>= {data_size_threshold} GB)")
        
        # Show exclusive migrations if any exist
        if exec1_only or exec2_only:
            lines.append("")
            lines.append("Exclusive Migrations:")
            if exec1_only:
                lines.append(f"  Exec1 Only ({len(exec1_only)}): {sorted(exec1_only)}")
            if exec2_only:
                lines.append(f"  Exec2 Only ({len(exec2_only)}): {sorted(exec2_only)}")
        
        # Configuration comparison
        if comparisons:
            lines.append("")
            lines.append("="*60)
            lines.append("CONFIGURATION COMPARISON")
            lines.append("="*60)
            
            config_keys = [
                'small_tier_max_sstable_size_gb',
                'small_tier_thread_subset_max_size_floor_gb', 
                'small_tier_worker_num_threads',
                'medium_tier_max_sstable_size_gb',
                'medium_tier_thread_subset_max_size_floor_gb',
                'medium_tier_worker_num_threads',
                'optimize_packing_medium_subsets',
                'skip_small_subsets',
                'execution_mode',
                'max_concurrent_workers'
            ]
            first_comp = comparisons[0]
            config_comparison = first_comp.get_config_comparison(config_keys)
            
            lines.append("")
            lines.append(f"{'Parameter':<40} {'Exec1':<15} {'Exec2':<15} {'Status':<10}")
            lines.append("-" * 85)
            
            for key, comparison in config_comparison.items():
                exec1_value = comparison['exec1'] if comparison['exec1'] is not None else 'N/A'
                exec2_value = comparison['exec2'] if comparison['exec2'] is not None else 'N/A'
                status = 'Same' if comparison['same'] else 'Different'
                
                lines.append(f"{key:<40} {str(exec1_value):<15} {str(exec2_value):<15} {status:<10}")
        
        # Header
        exec1_short = (exec1_name[:8] + "..") if exec1_name and len(exec1_name) > 10 else (exec1_name or "Exec1")
        exec2_short = (exec2_name[:8] + "..") if exec2_name and len(exec2_name) > 10 else (exec2_name or "Exec2")
        
        lines.append("")
        lines.append(f"{'Migration':<12} {'Data':<8} {'Execution Time':<30} {'Workers':<25} {'CPUs':<25} {'CPU Time':<30}")
        lines.append(f"{'ID':<12} {'Size':<8} {exec1_short:<10} {exec2_short:<10} {'Ex2 - Ex1':<8} {exec1_short:<8} {exec2_short:<8} {'Ex2 - Ex1':<6} {exec1_short:<8} {exec2_short:<8} {'Ex2 - Ex1':<6} {exec1_short:<10} {exec2_short:<10} {'Ex2 - Ex1':<8}")
        lines.append(f"{'':12} {'(GB)':<8} {'-'*30} {'-'*25} {'-'*25} {'-'*30}")
        
        # Data rows
        for comp in comparisons:
            exec1 = comp.exec1_metrics
            exec2 = comp.exec2_metrics
            
            # Format values and differences
            exec1_time = self._format_time(exec1.total_execution_time)
            exec2_time = self._format_time(exec2.total_execution_time)
            time_diff = f"{comp.execution_time_diff:+.1f}s" if abs(comp.execution_time_diff) < 60 else f"{comp.execution_time_diff/60:+.1f}m"
            
            exec1_cpu_time = self._format_time(exec1.cpu_time)
            exec2_cpu_time = self._format_time(exec2.cpu_time)
            cpu_time_diff = f"{comp.cpu_time_diff:+.1f}s" if abs(comp.cpu_time_diff) < 60 else f"{comp.cpu_time_diff/60:+.1f}m"
            
            # Format worker counts with straggler information
            def format_worker_text(workers, stragglers):
                if workers == 0:
                    return "0"
                elif stragglers > 0:
                    return f"{workers}[{stragglers}]"
                else:
                    return str(workers)
            
            exec1_workers_text = format_worker_text(exec1.total_workers, sum(exec1.stragglers_by_tier.values()))
            exec2_workers_text = format_worker_text(exec2.total_workers, sum(exec2.stragglers_by_tier.values()))
            
            worker_diff = f"{comp.worker_count_diff:+d}"
            cpu_diff = f"{comp.cpu_count_diff:+d}"
            
            # Data size (should be same for both executions)
            data_size_str = f"{exec1.total_data_size_gb:.1f}"
            
            lines.append(f"{comp.migration_id:<12} {data_size_str:<8} {exec1_time:<10} {exec2_time:<10} {time_diff:<8} "
                        f"{exec1_workers_text:<8} {exec2_workers_text:<8} {worker_diff:<6} "
                        f"{exec1.total_cpus:<8} {exec2.total_cpus:<8} {cpu_diff:<6} "
                        f"{exec1_cpu_time:<10} {exec2_cpu_time:<10} {cpu_time_diff:<8}")
        
        # Summary statistics
        lines.append("")
        lines.append("="*115)
        lines.append("AGGREGATE ANALYSIS")
        lines.append("="*115)
        
        total_exec1_time = sum(c.exec1_metrics.total_execution_time for c in comparisons)
        total_exec2_time = sum(c.exec2_metrics.total_execution_time for c in comparisons)
        
        total_exec1_workers = sum(c.exec1_metrics.total_workers for c in comparisons)
        total_exec2_workers = sum(c.exec2_metrics.total_workers for c in comparisons)
        
        total_exec1_cpus = sum(c.exec1_metrics.total_cpus for c in comparisons)
        total_exec2_cpus = sum(c.exec2_metrics.total_cpus for c in comparisons)
        
        total_exec1_cpu_time = sum(c.exec1_metrics.cpu_time for c in comparisons)
        total_exec2_cpu_time = sum(c.exec2_metrics.cpu_time for c in comparisons)
        
        lines.append("")
        lines.append("Total Execution Time:")
        lines.append(f"  Exec1:      {self._format_time(total_exec1_time)}")
        lines.append(f"  Exec2:      {self._format_time(total_exec2_time)}")
        time_diff = total_exec2_time - total_exec1_time
        lines.append(f"  Difference: {time_diff:+.2f}s ({'Exec2 slower' if time_diff > 0 else 'Exec2 faster' if time_diff < 0 else 'Same'})")
        
        lines.append("")
        lines.append("Total Workers:")
        lines.append(f"  Exec1:      {total_exec1_workers}")
        lines.append(f"  Exec2:      {total_exec2_workers}")
        worker_diff = total_exec2_workers - total_exec1_workers
        lines.append(f"  Difference: {worker_diff:+d} ({'Exec2 more' if worker_diff > 0 else 'Exec2 fewer' if worker_diff < 0 else 'Same'})")
        
        lines.append("")
        lines.append("Total CPUs:")
        lines.append(f"  Exec1:      {total_exec1_cpus}")
        lines.append(f"  Exec2:      {total_exec2_cpus}")
        cpu_diff = total_exec2_cpus - total_exec1_cpus
        lines.append(f"  Difference: {cpu_diff:+d} ({'Exec2 more' if cpu_diff > 0 else 'Exec2 fewer' if cpu_diff < 0 else 'Same'})")
        
        lines.append("")
        lines.append("Total CPU Time:")
        lines.append(f"  Exec1:      {self._format_time(total_exec1_cpu_time)}")  
        lines.append(f"  Exec2:      {self._format_time(total_exec2_cpu_time)}")
        cpu_time_diff = total_exec2_cpu_time - total_exec1_cpu_time
        lines.append(f"  Difference: {cpu_time_diff:+.2f}s ({'Exec2 more' if cpu_time_diff > 0 else 'Exec2 less' if cpu_time_diff < 0 else 'Same'})")
        
        return "\n".join(lines)
    
    def save_comparison_report(self, comparisons: List[TieredComparisonResult], output_file: str, exec1_name: str = None, exec2_name: str = None, exec1_only: Set[str] = None, exec2_only: Set[str] = None, data_size_threshold: float = None):
        """Save the comparison report to a text file."""
        report_content = self.generate_comparison_report(comparisons, exec1_name, exec2_name, exec1_only, exec2_only, data_size_threshold)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(report_content)
        
        print(f"Text comparison report saved to: {output_file}")
    
    def generate_html_report(self, comparisons: List[TieredComparisonResult], exec1_name: str = None, exec2_name: str = None, exec1_only: Set[str] = None, exec2_only: Set[str] = None, data_size_threshold: float = None, efficiency_threshold: float = None) -> str:
        """Generate an HTML comparison report for browser viewing."""
        if not comparisons:
            return "<html><body><h1>No comparisons to display.</h1></body></html>"
        
        # Calculate aggregate totals for summary
        total_exec1_time = sum(c.exec1_metrics.total_execution_time for c in comparisons)
        total_exec2_time = sum(c.exec2_metrics.total_execution_time for c in comparisons)
        total_exec1_workers = sum(c.exec1_metrics.total_workers for c in comparisons)
        total_exec2_workers = sum(c.exec2_metrics.total_workers for c in comparisons)
        total_exec1_cpus = sum(c.exec1_metrics.total_cpus for c in comparisons)
        total_exec2_cpus = sum(c.exec2_metrics.total_cpus for c in comparisons)
        total_exec1_cpu_time = sum(c.exec1_metrics.cpu_time for c in comparisons)
        total_exec2_cpu_time = sum(c.exec2_metrics.cpu_time for c in comparisons)
        total_data_size_gb = sum(c.exec1_metrics.total_data_size_gb for c in comparisons)
        
        # Generate timestamp
        from datetime import datetime
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        html = f"""<!DOCTYPE html>
<html>
<head>
    <title>Tiered vs Tiered Migration Comparison</title>
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
        .large-migration {{ background-color: #ffe4b5 !important; border-left: 4px solid #ff8c00 !important; }}
        .low-efficiency {{ background-color: #ffff00 !important; font-weight: bold; }} /* Bright yellow for low efficiency migrations */
        .best-efficiency {{ background-color: #c8e6c9 !important; font-weight: bold; }} /* Light green for best efficiency */
        
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
    <h1>Tiered vs Tiered Migration Simulation Comparison</h1>
    <p><strong>Generated:</strong> {timestamp}</p>
</div>

<div class="config">
    <h2>Comparison Details</h2>"""
        
        if exec1_name and exec2_name:
            html += f"""
    <p><strong>Execution 1:</strong> {exec1_name}</p>
    <p><strong>Execution 2:</strong> {exec2_name}</p>
    <p><strong>Common Migrations:</strong> {len(comparisons)}</p>"""
        else:
            html += f"""
    <p><strong>Common Migrations:</strong> {len(comparisons)}</p>"""
        
        # Data size threshold information
        if data_size_threshold is not None:
            large_migrations = [comp for comp in comparisons if comp.exec1_metrics.total_data_size_gb >= data_size_threshold]
            html += f"""
    <p><strong>Large Migrations:</strong> {len(large_migrations)} (>= {data_size_threshold} GB)</p>"""
        
        if efficiency_threshold is not None:
            low_efficiency_exec1 = sum(1 for comp in comparisons if comp.exec1_metrics.average_cpu_efficiency_percent > 0 and comp.exec1_metrics.average_cpu_efficiency_percent < efficiency_threshold)
            low_efficiency_exec2 = sum(1 for comp in comparisons if comp.exec2_metrics.average_cpu_efficiency_percent > 0 and comp.exec2_metrics.average_cpu_efficiency_percent < efficiency_threshold)
            html += f"""
    <p><strong>Low Efficiency Threshold:</strong> {efficiency_threshold:.1f}% (Exec1: {low_efficiency_exec1}, Exec2: {low_efficiency_exec2} migrations)</p>"""
        
        if exec1_only or exec2_only:
            html += f"""</div>

<div class="exclusive-migrations">
    <h2>Exclusive Migrations</h2>"""
            if exec1_only:
                html += f"""
    <p><strong>Execution 1 Only ({len(exec1_only)}):</strong> {', '.join(sorted(exec1_only))}</p>"""
            if exec2_only:
                html += f"""
    <p><strong>Execution 2 Only ({len(exec2_only)}):</strong> {', '.join(sorted(exec2_only))}</p>"""
        
        html += f"""</div>

<div class="summary">
    <h2>Aggregate Analysis</h2>
    <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px;">
        <div>
            <h3>Total Execution Time</h3>
            <p><strong>Execution 1:</strong> {self._format_time(total_exec1_time)}</p>
            <p><strong>Execution 2:</strong> {self._format_time(total_exec2_time)}</p>
            <p><strong>Difference:</strong> {'+' if (total_exec2_time - total_exec1_time) >= 0 else ''}{self._format_time(total_exec2_time - total_exec1_time) if (total_exec2_time - total_exec1_time) >= 0 else '-' + self._format_time(abs(total_exec2_time - total_exec1_time))} (Exec2 {'slower' if total_exec2_time > total_exec1_time else 'faster' if total_exec2_time < total_exec1_time else 'same'})</p>
        </div>
        <div>
            <h3>Total Data Size</h3>
            <p><strong>Both Executions:</strong> {total_data_size_gb:.1f} GB</p>
            <p><em>Data size is identical for both executions</em></p>
        </div>
        <div>
            <h3>Total Workers</h3>
            <p><strong>Execution 1:</strong> {total_exec1_workers}</p>
            <p><strong>Execution 2:</strong> {total_exec2_workers}</p>
            <p><strong>Difference:</strong> {total_exec2_workers - total_exec1_workers:+,} (Exec2 {'more' if total_exec2_workers > total_exec1_workers else 'fewer' if total_exec2_workers < total_exec1_workers else 'same'})</p>
        </div>
        <div>
            <h3>Total CPUs</h3>
            <p><strong>Execution 1:</strong> {total_exec1_cpus}</p>
            <p><strong>Execution 2:</strong> {total_exec2_cpus}</p>
            <p><strong>Difference:</strong> {total_exec2_cpus - total_exec1_cpus:+,} (Exec2 {'more' if total_exec2_cpus > total_exec1_cpus else 'fewer' if total_exec2_cpus < total_exec1_cpus else 'same'})</p>
        </div>
        <div>
            <h3>Total CPU Time</h3>
            <p><strong>Execution 1:</strong> {self._format_time(total_exec1_cpu_time)}</p>
            <p><strong>Execution 2:</strong> {self._format_time(total_exec2_cpu_time)}</p>
            <p><strong>Difference:</strong> {'+' if (total_exec2_cpu_time - total_exec1_cpu_time) >= 0 else ''}{self._format_time(total_exec2_cpu_time - total_exec1_cpu_time) if (total_exec2_cpu_time - total_exec1_cpu_time) >= 0 else '-' + self._format_time(abs(total_exec2_cpu_time - total_exec1_cpu_time))} (Exec2 {'more' if total_exec2_cpu_time > total_exec1_cpu_time else 'less' if total_exec2_cpu_time < total_exec1_cpu_time else 'same'})</p>
        </div>
    </div>
</div>

<div class="config-comparison">
    <h2>Configuration Comparison</h2>
    {self._generate_config_comparison_html(comparisons, exec1_name, exec2_name)}
</div>

<div class="migration-details">
    <h2>Per-Migration Comparison</h2>"""
        
        # Add legend for large migrations if threshold is specified
        if data_size_threshold is not None:
            html += f"""
    <p><strong>Legend:</strong> 
        <span style="background-color: #ffe4b5; padding: 2px 6px; border-radius: 3px; border-left: 4px solid #ff8c00;">Orange highlighting</span> indicates large migrations (>= {data_size_threshold} GB)
    </p>"""
        
        # Add legend for efficiency highlighting if threshold is specified
        if efficiency_threshold is not None:
            efficiency_legend_text = f'<span style="background-color: #ffff00; padding: 2px 6px; border-radius: 3px; font-weight: bold;">Yellow highlighting</span> indicates low efficiency migrations (< {efficiency_threshold}%)'
            if data_size_threshold is not None:
                # Extend existing legend by finding the specific legend paragraph
                legend_marker = 'indicates large migrations'
                if legend_marker in html:
                    html = html.replace(f'{legend_marker} (>= {data_size_threshold} GB)\n    </p>', f'{legend_marker} (>= {data_size_threshold} GB), {efficiency_legend_text}\n    </p>')
            else:
                # Create new legend
                html += f"""
    <p><strong>Legend:</strong> 
        {efficiency_legend_text}
    </p>"""
        
        html += """
    <table>
        <thead>
            <tr>
                <th rowspan="2">Migration ID</th>
                <th rowspan="2">Data Size (GB)</th>
                <th colspan="3" class="group-separator-left">Execution Time</th>
                <th colspan="3" class="group-separator-left">Workers</th>
                <th colspan="3" class="group-separator-left">CPUs</th>
                <th colspan="3" class="group-separator-left">CPU Time</th>
                <th colspan="3" class="group-separator-left">CPU Efficiency (Exec1)</th>
                <th colspan="3" class="group-separator-left">CPU Efficiency (Exec2)</th>
                <th colspan="6" class="group-separator-left">Exec1 Tier Distribution</th>
                <th colspan="6" class="group-separator-left">Exec2 Tier Distribution</th>
            </tr>
            <tr>
                <th class="group-separator-left">Exec1</th>
                <th>Exec2</th>
                <th class="diff-header">Ex2 - Ex1</th>
                <th class="group-separator-left">Exec1</th>
                <th>Exec2</th>
                <th class="diff-header">Ex2 - Ex1</th>
                <th class="group-separator-left">Exec1</th>
                <th>Exec2</th>
                <th class="diff-header">Ex2 - Ex1</th>
                <th class="group-separator-left">Exec1</th>
                <th>Exec2</th>
                <th class="diff-header">Ex2 - Ex1</th>
                <th class="group-separator-left">Active Time</th>
                <th>Eff %</th>
                <th>Waste %</th>
                <th class="group-separator-left">Active Time</th>
                <th>Eff %</th>
                <th>Waste %</th>
                <th class="group-separator-left">Small W</th>
                <th>Med W</th>
                <th>Large W</th>
                <th>Small C</th>
                <th>Med C</th>
                <th>Large C</th>
                <th class="group-separator-left">Small W</th>
                <th>Med W</th>
                <th>Large W</th>
                <th>Small C</th>
                <th>Med C</th>
                <th>Large C</th>
            </tr>
        </thead>
        <tbody>"""
        
        # Sort comparisons by data size (descending) before processing
        comparisons_sorted = sorted(comparisons, key=lambda comp: comp.exec1_metrics.total_data_size_gb, reverse=True)
        
        # Process each comparison and determine best values for highlighting
        for comp in comparisons_sorted:
            exec1 = comp.exec1_metrics
            exec2 = comp.exec2_metrics
            
            # Determine best execution time and CPU time (only when genuinely different)
            best_exec_time = "exec1" if exec1.total_execution_time < exec2.total_execution_time else ("exec2" if exec2.total_execution_time < exec1.total_execution_time else None)
            best_cpu_time = "exec1" if exec1.cpu_time < exec2.cpu_time else ("exec2" if exec2.cpu_time < exec1.cpu_time else None)
            best_worker_count = "exec1" if exec1.total_workers < exec2.total_workers else ("exec2" if exec2.total_workers < exec1.total_workers else None)
            
            # Format values
            exec1_time_str = self._format_time(exec1.total_execution_time)
            exec2_time_str = self._format_time(exec2.total_execution_time)
            exec1_cpu_time_str = self._format_time(exec1.cpu_time)
            exec2_cpu_time_str = self._format_time(exec2.cpu_time)
            
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
            def format_worker_cell(workers, stragglers, tier_name, exec_name):
                if workers == 0:
                    return ('0', '')
                elif stragglers > 0:
                    return (f'{workers:,}[{stragglers}]', 'has-stragglers')
                else:
                    return (f'{workers:,}', '')
            
            # Format exec1 tier worker cells
            exec1_small_w, exec1_small_w_class = format_worker_cell(
                exec1.workers_by_tier.get('SMALL', 0), 
                exec1.stragglers_by_tier.get('SMALL', 0), 
                'SMALL', 'exec1'
            )
            exec1_medium_w, exec1_medium_w_class = format_worker_cell(
                exec1.workers_by_tier.get('MEDIUM', 0), 
                exec1.stragglers_by_tier.get('MEDIUM', 0), 
                'MEDIUM', 'exec1'
            )
            exec1_large_w, exec1_large_w_class = format_worker_cell(
                exec1.workers_by_tier.get('LARGE', 0), 
                exec1.stragglers_by_tier.get('LARGE', 0), 
                'LARGE', 'exec1'
            )
            
            # Format exec2 tier worker cells
            exec2_small_w, exec2_small_w_class = format_worker_cell(
                exec2.workers_by_tier.get('SMALL', 0), 
                exec2.stragglers_by_tier.get('SMALL', 0), 
                'SMALL', 'exec2'
            )
            exec2_medium_w, exec2_medium_w_class = format_worker_cell(
                exec2.workers_by_tier.get('MEDIUM', 0), 
                exec2.stragglers_by_tier.get('MEDIUM', 0), 
                'MEDIUM', 'exec2'
            )
            exec2_large_w, exec2_large_w_class = format_worker_cell(
                exec2.workers_by_tier.get('LARGE', 0), 
                exec2.stragglers_by_tier.get('LARGE', 0), 
                'LARGE', 'exec2'
            )

            # Determine if this is a large migration
            is_large_migration = data_size_threshold is not None and exec1.total_data_size_gb >= data_size_threshold
            migration_id_class = "large-migration" if is_large_migration else ""
            data_size_class = "large-migration" if is_large_migration else ""
            
            # Calculate efficiency metrics and determine low efficiency highlighting
            exec1_inefficiency_percent = (exec1.cpu_inefficiency / exec1.total_used_cpu_time * 100) if exec1.total_used_cpu_time > 0 else 0
            exec2_inefficiency_percent = (exec2.cpu_inefficiency / exec2.total_used_cpu_time * 100) if exec2.total_used_cpu_time > 0 else 0
            
            # Determine efficiency highlighting
            exec1_is_low_efficiency = (efficiency_threshold is not None and 
                                     exec1.average_cpu_efficiency_percent > 0 and 
                                     exec1.average_cpu_efficiency_percent < efficiency_threshold)
            exec2_is_low_efficiency = (efficiency_threshold is not None and 
                                     exec2.average_cpu_efficiency_percent > 0 and 
                                     exec2.average_cpu_efficiency_percent < efficiency_threshold)
            
            exec1_efficiency_class = "low-efficiency" if exec1_is_low_efficiency else ""
            exec2_efficiency_class = "low-efficiency" if exec2_is_low_efficiency else ""
            
            # Determine best efficiency percentage highlighting (separate from low-efficiency highlighting)
            exec1_efficiency_percent_class = exec1_efficiency_class  # Start with low-efficiency class if applicable
            exec2_efficiency_percent_class = exec2_efficiency_class  # Start with low-efficiency class if applicable
            
            # Add best efficiency highlighting only to percentage columns (only if not already low-efficiency)
            if (exec1.average_cpu_efficiency_percent > 0 and exec2.average_cpu_efficiency_percent > 0 and 
                exec1.average_cpu_efficiency_percent != exec2.average_cpu_efficiency_percent):
                if exec1.average_cpu_efficiency_percent > exec2.average_cpu_efficiency_percent:
                    # Exec1 has better efficiency, highlight only if not low efficiency
                    if not exec1_is_low_efficiency:
                        exec1_efficiency_percent_class = "best-efficiency"
                else:
                    # Exec2 has better efficiency, highlight only if not low efficiency  
                    if not exec2_is_low_efficiency:
                        exec2_efficiency_percent_class = "best-efficiency"

            html += f"""
            <tr>
                <td class="{migration_id_class}"><strong>{comp.migration_id}</strong></td>
                <td class="number {data_size_class}">{exec1.total_data_size_gb:.1f}</td>
                <td class="number group-separator-left {'best-time' if best_exec_time == 'exec1' else ''}">{exec1_time_str}</td>
                <td class="number {'best-time' if best_exec_time == 'exec2' else ''}">{exec2_time_str}</td>
                <td class="number {exec_time_diff_class}">{exec_time_diff_str}</td>
                <td class="number group-separator-left {'best-time' if best_worker_count == 'exec1' else ''}">{exec1.total_workers:,}</td>
                <td class="number {'best-time' if best_worker_count == 'exec2' else ''}">{exec2.total_workers:,}</td>
                <td class="number {worker_diff_class}">{worker_diff_str}</td>
                <td class="number group-separator-left">{exec1.total_cpus:,}</td>
                <td class="number">{exec2.total_cpus:,}</td>
                <td class="number {cpu_diff_class}">{cpu_diff_str}</td>
                <td class="number group-separator-left {'best-time' if best_cpu_time == 'exec1' else ''}">{exec1_cpu_time_str}</td>
                <td class="number {'best-time' if best_cpu_time == 'exec2' else ''}">{exec2_cpu_time_str}</td>
                <td class="number {cpu_time_diff_class}">{cpu_time_diff_str}</td>
                <td class="number group-separator-left {exec1_efficiency_class}">{exec1.total_active_cpu_time:.1f}s</td>
                <td class="number {exec1_efficiency_percent_class}">{exec1.average_cpu_efficiency_percent:.1f}%</td>
                <td class="number {exec1_efficiency_class}">{exec1_inefficiency_percent:.1f}%</td>
                <td class="number group-separator-left {exec2_efficiency_class}">{exec2.total_active_cpu_time:.1f}s</td>
                <td class="number {exec2_efficiency_percent_class}">{exec2.average_cpu_efficiency_percent:.1f}%</td>
                <td class="number {exec2_efficiency_class}">{exec2_inefficiency_percent:.1f}%</td>
                <td class="number group-separator-left {exec1_small_w_class}">{exec1_small_w}</td>
                <td class="number {exec1_medium_w_class}">{exec1_medium_w}</td>
                <td class="number {exec1_large_w_class}">{exec1_large_w}</td>
                <td class="number">{exec1.cpus_by_tier.get('SMALL', 0):,}</td>
                <td class="number">{exec1.cpus_by_tier.get('MEDIUM', 0):,}</td>
                <td class="number">{exec1.cpus_by_tier.get('LARGE', 0):,}</td>
                <td class="number group-separator-left {exec2_small_w_class}">{exec2_small_w}</td>
                <td class="number {exec2_medium_w_class}">{exec2_medium_w}</td>
                <td class="number {exec2_large_w_class}">{exec2_large_w}</td>
                <td class="number">{exec2.cpus_by_tier.get('SMALL', 0):,}</td>
                <td class="number">{exec2.cpus_by_tier.get('MEDIUM', 0):,}</td>
                <td class="number">{exec2.cpus_by_tier.get('LARGE', 0):,}</td>
            </tr>"""
        
        html += """
        </tbody>
    </table>
</div>

<div class="aggregate">
    <h2>Legend</h2>
    <p><span style="background-color: #c8e6c9; padding: 2px 6px; border-radius: 3px;">Green highlighting</span> indicates the best (lowest) execution time, CPU time, worker count, and best CPU efficiency for each migration.</p>
    <p><strong>Column Abbreviations:</strong></p>
    <ul>
        <li><strong>W:</strong> Workers (number of workers allocated to each tier)</li>
        <li><strong>C:</strong> CPUs/Cores (total threads allocated to each tier = workers × threads per worker)</li>
    </ul>
    <p><strong>CPU Efficiency Highlighting:</strong></p>
    <ul>
        <li><span style="background-color: #c8e6c9; padding: 2px 6px; border-radius: 3px; font-weight: bold;">Light green</span> indicates the execution with better CPU efficiency percentage for each migration</li>
        <li><span style="background-color: #ffff00; padding: 2px 6px; border-radius: 3px; font-weight: bold;">Yellow highlighting</span> takes priority and indicates low efficiency percentage (below threshold) in the "Eff %" columns only</li>
    </ul>
    <p><strong>Straggler Information:</strong></p>
    <ul>
        <li><strong>Format:</strong> Total workers shown as "total[stragglers]" (e.g., "15[3]" = 15 workers, 3 stragglers)</li>
        <li><span style="background-color: #ffcccc; padding: 2px 6px; border-radius: 3px;">Light red background</span> indicates cells with straggler workers</li>
    </ul>
    <p><strong>Difference Interpretation (Exec2 - Exec1):</strong></p>
    <ul>
        <li><span style="background-color: #add8e6; padding: 2px 6px; border-radius: 3px;">Light blue</span> indicates positive differences (Exec2 > Exec1)</li>
        <li><span style="background-color: #fff8dc; padding: 2px 6px; border-radius: 3px;">Light yellow</span> indicates negative differences (Exec2 < Exec1)</li>
        <li><strong>Positive execution time difference:</strong> Exec2 took longer</li>
        <li><strong>Negative execution time difference:</strong> Exec2 was faster</li>
    </ul>
</div>

</body>
</html>"""
        
        return html

    def _generate_config_comparison_html(self, comparisons: List[TieredComparisonResult], exec1_name: str = None, exec2_name: str = None) -> str:
        """Generate HTML for configuration comparison section."""
        if not comparisons:
            return "<p>No migrations available for configuration comparison.</p>"
        
        # Configuration keys to compare (as requested by user)
        config_keys = [
            'small_tier_max_sstable_size_gb',
            'small_tier_thread_subset_max_size_floor_gb', 
            'small_tier_worker_num_threads',
            'medium_tier_max_sstable_size_gb',
            'medium_tier_thread_subset_max_size_floor_gb',
            'medium_tier_worker_num_threads',
            'optimize_packing_medium_subsets',
            'skip_small_subsets',
            'execution_mode',
            'max_concurrent_workers'
        ]
        
        # Use first comparison to get configuration values (should be same across all migrations in an execution)
        first_comp = comparisons[0]
        config_comparison = first_comp.get_config_comparison(config_keys)
        
        exec1_short = exec1_name if exec1_name else "Exec1"
        exec2_short = exec2_name if exec2_name else "Exec2"
        
        html = f"""
        <table style="width: 100%; max-width: 800px;">
            <thead>
                <tr>
                    <th style="text-align: left; width: 40%;">Configuration Parameter</th>
                    <th style="text-align: center; width: 25%;">{exec1_short}</th>
                    <th style="text-align: center; width: 25%;">{exec2_short}</th>
                    <th style="text-align: center; width: 10%;">Status</th>
                </tr>
            </thead>
            <tbody>"""
        
        for key, comparison in config_comparison.items():
            exec1_value = comparison['exec1']
            exec2_value = comparison['exec2']
            is_same = comparison['same']
            
            # Format None values
            exec1_display = exec1_value if exec1_value is not None else "N/A"
            exec2_display = exec2_value if exec2_value is not None else "N/A"
            
            status_class = "config-same" if is_same else "config-different"
            status_text = "✓" if is_same else "✗"
            status_title = "Same configuration" if is_same else "Different configuration"
            
            html += f"""
                <tr class="{status_class}">
                    <td><strong>{key}</strong></td>
                    <td style="text-align: center;">{exec1_display}</td>
                    <td style="text-align: center;">{exec2_display}</td>
                    <td style="text-align: center;" title="{status_title}">{status_text}</td>
                </tr>"""
        
        html += """
            </tbody>
        </table>
        <p><strong>Legend:</strong> 
            <span style="background-color: #e8f5e8; padding: 2px 6px; border-radius: 3px;">Green</span> = Same configuration, 
            <span style="background-color: #ffe8e8; padding: 2px 6px; border-radius: 3px;">Light red</span> = Different configuration
        </p>"""
        
        return html

    def save_html_report(self, comparisons: List[TieredComparisonResult], output_file: str, exec1_name: str = None, exec2_name: str = None, exec1_only: Set[str] = None, exec2_only: Set[str] = None, data_size_threshold: float = None, efficiency_threshold: float = None):
        """Save the HTML comparison report to a file."""
        html_content = self.generate_html_report(comparisons, exec1_name, exec2_name, exec1_only, exec2_only, data_size_threshold, efficiency_threshold)
        
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
        description="Compare Two Tiered Migration Simulation Results",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Compare and save results to organized output directory (default - saves to comparison/output/tiered/my_analysis/)
  python comparison/tiered_comparison_tool.py --exec1 test_new_5 --exec2 test_new_6 --comparison-exec-name my_tiered_analysis
  
  # Compare with highlighting for large migrations (>= 5.0 GB)
  python comparison/tiered_comparison_tool.py --exec1 test_new_5 --exec2 test_new_6 --comparison-exec-name my_tiered_analysis --data-size-threshold 5.0
  
  # Compare without saving reports (console output only)
  python comparison/tiered_comparison_tool.py --exec1 test_new_5 --exec2 test_new_6 --comparison-exec-name my_tiered_analysis --omit-reports
  
  # Alternative: specify full paths (backward compatibility)
  python comparison/tiered_comparison_tool.py --exec1-path tiered/output/test_new_5 --exec2-path tiered/output/test_new_6 --comparison-exec-name my_tiered_analysis
  
  # Can be run from anywhere within the project:
  cd comparison && python tiered_comparison_tool.py --exec1 test_new_5 --exec2 test_new_6 --comparison-exec-name my_tiered_analysis
  cd TieredStrategySimulation && python comparison/tiered_comparison_tool.py --exec1 test_new_5 --exec2 test_new_6 --comparison-exec-name my_tiered_analysis
        """
    )
    
    # Execution name arguments (recommended)
    parser.add_argument('--exec1', '-1',
                       help='First tiered execution name (e.g., test_new_5). Path will be: tiered/output/{name}')
    parser.add_argument('--exec2', '-2',
                       help='Second tiered execution name (e.g., test_new_6). Path will be: tiered/output/{name}')
    
    # Full path arguments (backward compatibility)
    parser.add_argument('--exec1-path',
                       help='Full path to first tiered simulation output directory')
    parser.add_argument('--exec2-path', 
                       help='Full path to second tiered simulation output directory')
    
    # Comparison organization
    parser.add_argument('--comparison-exec-name', '-c',
                       help='Name for this comparison analysis (creates comparison/output/tiered/{name}/ directory)')
    
    # Output options
    parser.add_argument('--omit-reports',
                       action='store_true',
                       help='Skip saving organized comparison reports (only show console output)')
    
    # Data size highlighting
    parser.add_argument('--data-size-threshold', type=float,
                       help='Highlight migrations with data size >= this threshold (in GB). Example: --data-size-threshold 5.0')
    
    # CPU efficiency threshold for highlighting inefficient migrations
    parser.add_argument('--efficiency-threshold',
                       type=float,
                       help='Highlight tiered migrations with CPU efficiency < this threshold (in %%). Example: --efficiency-threshold 70.0')
    
    args = parser.parse_args()
    
    # No validation needed - organized reports are created automatically when comparison name is provided
    
    # Determine paths and execution names based on arguments
    exec1_run_path = None
    exec2_run_path = None
    exec1_name = None
    exec2_name = None
    
    if args.exec1 and args.exec2:
        # Use execution names (recommended approach)
        exec1_name = args.exec1
        exec2_name = args.exec2
        # Find project root and construct absolute paths
        try:
            project_root = find_project_root()
            exec1_run_path = os.path.join(project_root, "tiered", "output", exec1_name)
            exec2_run_path = os.path.join(project_root, "tiered", "output", exec2_name)
            print(f"Project root: {project_root}")
        except FileNotFoundError as e:
            print(f"Error: {e}")
            sys.exit(1)
        print(f"Comparing tiered executions: {exec1_name} vs {exec2_name}")
        
    elif args.exec1_path and args.exec2_path:
        # Use full paths (backward compatibility)
        exec1_run_path = args.exec1_path
        exec2_run_path = args.exec2_path
        # Extract execution names from paths if possible
        exec1_name = Path(exec1_run_path).name if exec1_run_path else None
        exec2_name = Path(exec2_run_path).name if exec2_run_path else None
        print(f"Comparing paths: {exec1_run_path} vs {exec2_run_path}")
        
    else:
        print("Error: Must specify either:")
        print("  --exec1 and --exec2 (recommended)")
        print("  OR --exec1-path and --exec2-path")
        sys.exit(1)
    
    try:
        analyzer = TieredComparisonAnalyzer()
        comparisons, exec1_only, exec2_only = analyzer.compare_runs(exec1_run_path, exec2_run_path, exec1_name, exec2_name)
        
        if not comparisons:
            print("No common migrations found, but showing exclusive migrations if any exist.")
            if exec1_only or exec2_only:
                # Show exclusive migrations even when no common migrations exist
                print(f"\nExclusive Migrations:")
                if exec1_only:
                    print(f"  Exec1 Only ({len(exec1_only)}): {sorted(exec1_only)}")
                if exec2_only:
                    print(f"  Exec2 Only ({len(exec2_only)}): {sorted(exec2_only)}")
            print("\nNo performance comparison data available due to lack of common migrations.")
            sys.exit(1)
        
        # Print summary to console
        analyzer.print_comparison_summary(comparisons, exec1_name, exec2_name, exec1_only, exec2_only, args.data_size_threshold)
        
        # Handle output file generation
        if args.comparison_exec_name and not args.omit_reports:
            # Generate organized output under tiered directory (default behavior)
            try:
                project_root = find_project_root()
                output_dir = os.path.join(project_root, "comparison", "output", "tiered", args.comparison_exec_name)
                os.makedirs(output_dir, exist_ok=True)
            except FileNotFoundError as e:
                print(f"Error: {e}")
                sys.exit(1)
            
            # Generate default filenames
            csv_file = f"{output_dir}/tiered_comparison_report_{args.comparison_exec_name}.csv"
            txt_file = f"{output_dir}/tiered_comparison_summary_{args.comparison_exec_name}.txt"
            html_file = f"{output_dir}/tiered_comparison_report_{args.comparison_exec_name}.html"
            
            # Save all report formats
            analyzer.save_comparison_csv(comparisons, csv_file, exec1_name, exec2_name, args.data_size_threshold, args.efficiency_threshold)
            analyzer.save_comparison_report(comparisons, txt_file, exec1_name, exec2_name, exec1_only, exec2_only, args.data_size_threshold)
            analyzer.save_html_report(comparisons, html_file, exec1_name, exec2_name, exec1_only, exec2_only, args.data_size_threshold, args.efficiency_threshold)
            
            print(f"Tiered comparison analysis saved to: {output_dir}/")
        
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()