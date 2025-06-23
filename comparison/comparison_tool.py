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
    cpu_time: float  # total thread time used
    workers_by_tier: Dict[str, int]  # tier -> worker count
    cpus_by_tier: Dict[str, int]  # tier -> total threads
    
    def __post_init__(self):
        """Calculate derived metrics."""
        if self.strategy == 'simple':
            # For simple strategy, all workers are in 'UNIVERSAL' tier
            self.workers_by_tier = {'UNIVERSAL': self.total_workers}
            self.cpus_by_tier = {'UNIVERSAL': self.total_cpus}

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
            
            for csv_file in csv_dir.glob("*_workers.csv"):
                workers_csv = csv_file
                break
            
            for csv_file in csv_dir.glob("*_summary.csv"):
                summary_csv = csv_file
                break
            
            if not workers_csv or not summary_csv:
                print(f"Warning: Could not find CSV files for {migration_id}. Looking for workers and summary CSV files.")
                return None
            
            # Parse worker CSV for actual execution data
            actual_workers = 0
            total_cpu_time = 0.0
            
            with open(workers_csv, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    actual_workers += 1
                    # Calculate CPU time as duration × threads (1 thread per worker for simple)
                    duration = float(row['Duration'])
                    total_cpu_time += duration * 1  # 1 thread per worker
            
            # Parse summary CSV for total execution time and other metrics
            total_execution_time = 0.0
            total_cpus = actual_workers  # 1 CPU per worker for simple strategy
            
            with open(summary_csv, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                for row in reader:
                    if len(row) >= 2 and row[0] == 'Total_Simulation_Time':
                        total_execution_time = float(row[1])
                    elif len(row) >= 2 and row[0] == 'Total_CPU_Time':
                        total_cpu_time = float(row[1])
            
            return MigrationMetrics(
                migration_id=migration_id,
                strategy='simple',
                total_execution_time=total_execution_time,
                total_workers=actual_workers,
                total_cpus=total_cpus,
                cpu_time=total_cpu_time,
                workers_by_tier={},
                cpus_by_tier={}
            )
            
        except Exception as e:
            print(f"Error parsing simple CSV data for {migration_id}: {e}")
            return None
    
    def extract_tiered_metrics(self, tiered_run_path: str) -> Dict[str, MigrationMetrics]:
        """Extract metrics from tiered simulation output."""
        tiered_path = Path(tiered_run_path)
        if not tiered_path.exists():
            raise FileNotFoundError(f"Tiered run path not found: {tiered_run_path}")
        
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
                    migration_metrics = self._parse_tiered_json(migration_id, json_files[0])
                    if migration_metrics:
                        metrics[migration_id] = migration_metrics
        
        return metrics
    
    def _parse_tiered_json(self, migration_id: str, json_file: Path) -> Optional[MigrationMetrics]:
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
            
            for csv_file in csv_dir.glob("*_workers.csv"):
                workers_csv = csv_file
                break
            
            # Calculate totals across all tiers from JSON
            total_workers = 0
            total_cpus = 0
            workers_by_tier = {}
            cpus_by_tier = {}
            
            for tier in ['SMALL', 'MEDIUM', 'LARGE']:
                if tier in by_tier:
                    tier_workers = by_tier[tier].get('total_workers', 0)
                    total_workers += tier_workers
                    workers_by_tier[tier] = tier_workers
                    
                    # Calculate CPUs for this tier
                    if tier_workers > 0:
                        threads_per_worker = simulation_config.get(f'{tier.lower()}_threads', 1)
                        tier_cpus = tier_workers * threads_per_worker
                        total_cpus += tier_cpus
                        cpus_by_tier[tier] = tier_cpus
                    else:
                        cpus_by_tier[tier] = 0
            
            # Calculate actual CPU time from worker CSV if available
            cpu_time = 0.0
            if workers_csv and workers_csv.exists():
                with open(workers_csv, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        tier = row['Tier']
                        duration = float(row['Duration'])
                        threads_per_worker = simulation_config.get(f'{tier.lower()}_threads', 1)
                        cpu_time += duration * threads_per_worker
            else:
                # Fallback to conservative estimate if CSV not available
                cpu_time = total_execution_time * total_cpus if total_cpus > 0 else total_execution_time
                print(f"Warning: Worker CSV not found for {migration_id}, using conservative CPU time estimate")
            
            return MigrationMetrics(
                migration_id=migration_id,
                strategy='tiered',
                total_execution_time=total_execution_time,
                total_workers=total_workers,
                total_cpus=total_cpus,
                cpu_time=cpu_time,
                workers_by_tier=workers_by_tier,
                cpus_by_tier=cpus_by_tier
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
        print(f"\n{'Migration':<12} {'Execution Time':<35} {'Workers':<30} {'CPUs':<30} {'CPU Time':<35}")
        print(f"{'ID':<12} {'Simple':<10} {'Tiered':<10} {'T/S':<5} {'S/T':<5} {'Simple':<8} {'Tiered':<8} {'T/S':<5} {'S/T':<5} {'Simple':<8} {'Tiered':<8} {'T/S':<5} {'S/T':<5} {'Simple':<10} {'Tiered':<10} {'T/S':<5} {'S/T':<5}")
        print("-" * 125)
        
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
            
            print(f"{comp.migration_id:<12} {simple_time:<10} {tiered_time:<10} {time_ratio_ts:<5} {time_ratio_st:<5} "
                  f"{simple.total_workers:<8} {tiered.total_workers:<8} {worker_ratio_ts:<5} {worker_ratio_st:<5} "
                  f"{simple.total_cpus:<8} {tiered.total_cpus:<8} {cpu_ratio_ts:<5} {cpu_ratio_st:<5} "
                  f"{simple_cpu_time:<10} {tiered_cpu_time:<10} {cpu_time_ratio_ts:<5} {cpu_time_ratio_st:<5}")
        
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
        
        print(f"\nTotal Execution Time:")
        print(f"  Simple:      {self._format_time(total_simple_time)}")
        print(f"  Tiered:      {self._format_time(total_tiered_time)}")
        print(f"  Tiered/Simple: {total_tiered_time/total_simple_time:.2f} (efficiency: <1.0 = tiered faster)")
        print(f"  Simple/Tiered: {total_simple_time/total_tiered_time:.2f} (speedup: >1.0 = tiered faster)")
        
        print(f"\nTotal Workers:")
        print(f"  Simple:      {total_simple_workers}")
        print(f"  Tiered:      {total_tiered_workers}")
        print(f"  Tiered/Simple: {total_tiered_workers/total_simple_workers:.2f}")
        print(f"  Simple/Tiered: {total_simple_workers/total_tiered_workers:.2f}")
        
        print(f"\nTotal CPUs:")
        print(f"  Simple:      {total_simple_cpus}")
        print(f"  Tiered:      {total_tiered_cpus}")
        print(f"  Tiered/Simple: {total_tiered_cpus/total_simple_cpus:.2f}")
        print(f"  Simple/Tiered: {total_simple_cpus/total_tiered_cpus:.2f}")
        
        print(f"\nTotal CPU Time:")
        print(f"  Simple:      {self._format_time(total_simple_cpu_time)}")
        print(f"  Tiered:      {self._format_time(total_tiered_cpu_time)}")
        print(f"  Tiered/Simple: {total_tiered_cpu_time/total_simple_cpu_time:.2f}")
        print(f"  Simple/Tiered: {total_simple_cpu_time/total_tiered_cpu_time:.2f}")
    
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
    
    def save_comparison_csv(self, comparisons: List[ComparisonResult], output_file: str, simple_exec_name: str = None, tiered_exec_name: str = None):
        """Save comparison results to CSV file."""
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            
            # Metadata header
            if simple_exec_name and tiered_exec_name:
                writer.writerow(['# Simple vs Tiered Migration Simulation Comparison'])
                writer.writerow([f'# Simple Execution: {simple_exec_name}'])
                writer.writerow([f'# Tiered Execution: {tiered_exec_name}'])
                writer.writerow([f'# Common Migrations: {len(comparisons)}'])
                writer.writerow([])  # Empty row for separation
            
            # Header
            writer.writerow([
                'Migration_ID',
                'Simple_Execution_Time', 'Tiered_Execution_Time', 'Execution_Time_Ratio_T_S', 'Execution_Time_Ratio_S_T',
                'Simple_Workers', 'Tiered_Workers', 'Worker_Ratio_T_S', 'Worker_Ratio_S_T',
                'Simple_CPUs', 'Tiered_CPUs', 'CPU_Ratio_T_S', 'CPU_Ratio_S_T',
                'Simple_CPU_Time', 'Tiered_CPU_Time', 'CPU_Time_Ratio_T_S', 'CPU_Time_Ratio_S_T',
                'Tiered_Small_Workers', 'Tiered_Medium_Workers', 'Tiered_Large_Workers',
                'Tiered_Small_CPUs', 'Tiered_Medium_CPUs', 'Tiered_Large_CPUs'
            ])
            
            # Data rows
            for comp in comparisons:
                simple = comp.simple_metrics
                tiered = comp.tiered_metrics
                
                writer.writerow([
                    comp.migration_id,
                    f"{simple.total_execution_time:.2f}", f"{tiered.total_execution_time:.2f}", 
                    f"{comp.execution_time_ratio:.4f}", f"{comp.execution_time_ratio_inverse:.4f}",
                    simple.total_workers, tiered.total_workers, 
                    f"{comp.worker_count_ratio:.4f}", f"{comp.worker_count_ratio_inverse:.4f}",
                    simple.total_cpus, tiered.total_cpus, 
                    f"{comp.cpu_count_ratio:.4f}", f"{comp.cpu_count_ratio_inverse:.4f}",
                    f"{simple.cpu_time:.2f}", f"{tiered.cpu_time:.2f}", 
                    f"{comp.cpu_time_ratio:.4f}", f"{comp.cpu_time_ratio_inverse:.4f}",
                    tiered.workers_by_tier.get('SMALL', 0),
                    tiered.workers_by_tier.get('MEDIUM', 0),
                    tiered.workers_by_tier.get('LARGE', 0),
                    tiered.cpus_by_tier.get('SMALL', 0),
                    tiered.cpus_by_tier.get('MEDIUM', 0),
                    tiered.cpus_by_tier.get('LARGE', 0)
                ])
        
        print(f"\nDetailed comparison saved to: {output_file}")
    
    def generate_comparison_report(self, comparisons: List[ComparisonResult], simple_exec_name: str = None, tiered_exec_name: str = None, simple_only: Set[str] = None, tiered_only: Set[str] = None) -> str:
        """Generate the tabular comparison report as a string."""
        if not comparisons:
            return "No comparisons to display."
        
        lines = []
        lines.append("="*100)
        lines.append("SIMPLE vs TIERED SIMULATION COMPARISON SUMMARY")
        lines.append("="*100)
        
        if simple_exec_name and tiered_exec_name:
            lines.append("")
            lines.append("Comparison Details:")
            lines.append(f"  Simple Execution:  {simple_exec_name}")
            lines.append(f"  Tiered Execution:  {tiered_exec_name}")
            lines.append(f"  Common Migrations: {len(comparisons)}")
        else:
            lines.append("")
            lines.append(f"Common Migrations: {len(comparisons)}")
        
        # Show exclusive migrations if any exist
        if simple_only or tiered_only:
            lines.append("")
            lines.append("Exclusive Migrations:")
            if simple_only:
                lines.append(f"  Simple Only ({len(simple_only)}): {sorted(simple_only)}")
            if tiered_only:
                lines.append(f"  Tiered Only ({len(tiered_only)}): {sorted(tiered_only)}")
        
        # Header
        lines.append("")
        lines.append(f"{'Migration':<12} {'Execution Time':<35} {'Workers':<30} {'CPUs':<30} {'CPU Time':<35}")
        lines.append(f"{'ID':<12} {'Simple':<10} {'Tiered':<10} {'T/S':<5} {'S/T':<5} {'Simple':<8} {'Tiered':<8} {'T/S':<5} {'S/T':<5} {'Simple':<8} {'Tiered':<8} {'T/S':<5} {'S/T':<5} {'Simple':<10} {'Tiered':<10} {'T/S':<5} {'S/T':<5}")
        lines.append("-" * 125)
        
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
            
            lines.append(f"{comp.migration_id:<12} {simple_time:<10} {tiered_time:<10} {time_ratio_ts:<5} {time_ratio_st:<5} "
                        f"{simple.total_workers:<8} {tiered.total_workers:<8} {worker_ratio_ts:<5} {worker_ratio_st:<5} "
                        f"{simple.total_cpus:<8} {tiered.total_cpus:<8} {cpu_ratio_ts:<5} {cpu_ratio_st:<5} "
                        f"{simple_cpu_time:<10} {tiered_cpu_time:<10} {cpu_time_ratio_ts:<5} {cpu_time_ratio_st:<5}")
        
        # Summary statistics
        lines.append("")
        lines.append("="*125)
        lines.append("AGGREGATE ANALYSIS")
        lines.append("="*125)
        
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
        lines.append(f"  Tiered/Simple: {total_tiered_time/total_simple_time:.2f} (efficiency: <1.0 = tiered faster)")
        lines.append(f"  Simple/Tiered: {total_simple_time/total_tiered_time:.2f} (speedup: >1.0 = tiered faster)")
        
        lines.append("")
        lines.append("Total Workers:")
        lines.append(f"  Simple:      {total_simple_workers}")
        lines.append(f"  Tiered:      {total_tiered_workers}")
        lines.append(f"  Tiered/Simple: {total_tiered_workers/total_simple_workers:.2f}")
        lines.append(f"  Simple/Tiered: {total_simple_workers/total_tiered_workers:.2f}")
        
        lines.append("")
        lines.append("Total CPUs:")
        lines.append(f"  Simple:      {total_simple_cpus}")
        lines.append(f"  Tiered:      {total_tiered_cpus}")
        lines.append(f"  Tiered/Simple: {total_tiered_cpus/total_simple_cpus:.2f}")
        lines.append(f"  Simple/Tiered: {total_simple_cpus/total_tiered_cpus:.2f}")
        
        lines.append("")
        lines.append("Total CPU Time:")
        lines.append(f"  Simple:      {self._format_time(total_simple_cpu_time)}")
        lines.append(f"  Tiered:      {self._format_time(total_tiered_cpu_time)}")
        lines.append(f"  Tiered/Simple: {total_tiered_cpu_time/total_simple_cpu_time:.2f}")
        lines.append(f"  Simple/Tiered: {total_simple_cpu_time/total_tiered_cpu_time:.2f}")
        
        return "\n".join(lines)
    
    def save_comparison_report(self, comparisons: List[ComparisonResult], output_file: str, simple_exec_name: str = None, tiered_exec_name: str = None, simple_only: Set[str] = None, tiered_only: Set[str] = None):
        """Save the tabular comparison report to a text file."""
        report_text = self.generate_comparison_report(comparisons, simple_exec_name, tiered_exec_name, simple_only, tiered_only)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(report_text)
        
        print(f"Tabular comparison report saved to: {output_file}")

def main():
    parser = argparse.ArgumentParser(
        description="Compare Simple vs Tiered Migration Simulation Results",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Compare specific execution runs with comparison name
  python comparison_tool.py --simple-execution alice_test_run --tiered-execution test_new_5 --comparison-exec-name my_analysis
  
  # Compare and save results to organized output directory (saves to output/simple-tiered/my_analysis/)
  python comparison_tool.py --simple-execution alice_test_run --tiered-execution test_new_5 --comparison-exec-name my_analysis --save-reports
  
  # Alternative: specify full paths (backward compatibility)
  python comparison_tool.py --simple-path simple/output/alice_test_run --tiered-path tiered/output/test_new_5 --comparison-exec-name my_analysis
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
    parser.add_argument('--output', '-o', 
                       help='Custom output CSV file path (overrides default organized structure)')
    parser.add_argument('--save-reports',
                       action='store_true',
                       help='Save comparison reports (CSV + text) in organized directory structure (requires --comparison-exec-name)')
    
    args = parser.parse_args()
    
    # Validate arguments
    if args.save_reports and not args.comparison_exec_name:
        print("Error: --save-reports requires --comparison-exec-name")
        sys.exit(1)
    
    # Determine paths and execution names based on arguments
    simple_run_path = None
    tiered_run_path = None
    simple_exec_name = None
    tiered_exec_name = None
    
    if args.simple_execution and args.tiered_execution:
        # Use execution names (recommended approach)
        simple_exec_name = args.simple_execution
        tiered_exec_name = args.tiered_execution
        simple_run_path = f"../simple/output/{simple_exec_name}"
        tiered_run_path = f"../tiered/output/{tiered_exec_name}"
        print(f"Comparing executions: {simple_exec_name} (simple) vs {tiered_exec_name} (tiered)")
        
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
        if args.output:
            # Custom output path specified
            analyzer.save_comparison_csv(comparisons, args.output, simple_exec_name, tiered_exec_name)
        elif args.save_reports and args.comparison_exec_name:
            # Generate organized output under simple-tiered directory
            output_dir = f"output/simple-tiered/{args.comparison_exec_name}"
            os.makedirs(output_dir, exist_ok=True)
            
            # Generate default filenames
            csv_file = f"{output_dir}/comparison_report_{args.comparison_exec_name}.csv"
            txt_file = f"{output_dir}/comparison_summary_{args.comparison_exec_name}.txt"
            
            # Save both tabular and CSV reports
            analyzer.save_comparison_csv(comparisons, csv_file, simple_exec_name, tiered_exec_name)
            analyzer.save_comparison_report(comparisons, txt_file, simple_exec_name, tiered_exec_name, simple_only, tiered_only)
            
            print(f"Simple vs Tiered comparison analysis saved to: {output_dir}/")
        
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 