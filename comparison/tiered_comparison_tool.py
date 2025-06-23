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
    cpu_time: float  # total thread time used
    workers_by_tier: Dict[str, int]  # tier -> worker count
    cpus_by_tier: Dict[str, int]  # tier -> total threads
    
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
                    migration_metrics = self._parse_tiered_json(migration_id, json_files[0], execution_name)
                    if migration_metrics:
                        metrics[migration_id] = migration_metrics
        
        return metrics
    
    def _parse_tiered_json(self, migration_id: str, json_file: Path, execution_name: str) -> Optional[TieredMigrationMetrics]:
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
            
            return TieredMigrationMetrics(
                migration_id=migration_id,
                execution_name=execution_name,
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
    
    def print_comparison_summary(self, comparisons: List[TieredComparisonResult], exec1_name: str = None, exec2_name: str = None, exec1_only: Set[str] = None, exec2_only: Set[str] = None):
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
        
        print(f"\n{'Migration':<12} {'Execution Time':<35} {'Workers':<30} {'CPUs':<30} {'CPU Time':<35}")
        print(f"{'ID':<12} {exec1_short:<10} {exec2_short:<10} {'2/1':<5} {'1/2':<5} {exec1_short:<8} {exec2_short:<8} {'2/1':<5} {'1/2':<5} {exec1_short:<8} {exec2_short:<8} {'2/1':<5} {'1/2':<5} {exec1_short:<10} {exec2_short:<10} {'2/1':<5} {'1/2':<5}")
        print("-" * 125)
        
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
            
            print(f"{comp.migration_id:<12} {exec1_time:<10} {exec2_time:<10} {time_ratio_21:<5} {time_ratio_12:<5} "
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
    
    def save_comparison_csv(self, comparisons: List[TieredComparisonResult], output_file: str, exec1_name: str = None, exec2_name: str = None):
        """Save comparison results to CSV file."""
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            # Write header comments
            csvfile.write("# Tiered vs Tiered Migration Simulation Comparison\n")
            csvfile.write(f"# Execution 1: {exec1_name or 'Unknown'}\n")
            csvfile.write(f"# Execution 2: {exec2_name or 'Unknown'}\n")
            csvfile.write(f"# Common Migrations: {len(comparisons)}\n")
            csvfile.write("\n")
            
            # CSV header
            fieldnames = [
                'Migration_ID',
                'Exec1_Execution_Time', 'Exec2_Execution_Time', 'Execution_Time_Ratio_1_2', 'Execution_Time_Ratio_2_1',
                'Exec1_Workers', 'Exec2_Workers', 'Worker_Ratio_1_2', 'Worker_Ratio_2_1',
                'Exec1_CPUs', 'Exec2_CPUs', 'CPU_Ratio_1_2', 'CPU_Ratio_2_1',
                'Exec1_CPU_Time', 'Exec2_CPU_Time', 'CPU_Time_Ratio_1_2', 'CPU_Time_Ratio_2_1',
                'Exec1_Small_Workers', 'Exec1_Medium_Workers', 'Exec1_Large_Workers',
                'Exec1_Small_CPUs', 'Exec1_Medium_CPUs', 'Exec1_Large_CPUs',
                'Exec2_Small_Workers', 'Exec2_Medium_Workers', 'Exec2_Large_Workers',
                'Exec2_Small_CPUs', 'Exec2_Medium_CPUs', 'Exec2_Large_CPUs'
            ]
            
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            # Write data rows
            for comp in comparisons:
                exec1 = comp.exec1_metrics
                exec2 = comp.exec2_metrics
                
                row = {
                    'Migration_ID': comp.migration_id,
                    'Exec1_Execution_Time': f"{exec1.total_execution_time:.2f}",
                    'Exec2_Execution_Time': f"{exec2.total_execution_time:.2f}",
                    'Execution_Time_Ratio_1_2': f"{comp.execution_time_ratio_inverse:.4f}",
                    'Execution_Time_Ratio_2_1': f"{comp.execution_time_ratio:.4f}",
                    'Exec1_Workers': exec1.total_workers,
                    'Exec2_Workers': exec2.total_workers,
                    'Worker_Ratio_1_2': f"{comp.worker_count_ratio_inverse:.4f}",
                    'Worker_Ratio_2_1': f"{comp.worker_count_ratio:.4f}",
                    'Exec1_CPUs': exec1.total_cpus,
                    'Exec2_CPUs': exec2.total_cpus,
                    'CPU_Ratio_1_2': f"{comp.cpu_count_ratio_inverse:.4f}",
                    'CPU_Ratio_2_1': f"{comp.cpu_count_ratio:.4f}",
                    'Exec1_CPU_Time': f"{exec1.cpu_time:.2f}",
                    'Exec2_CPU_Time': f"{exec2.cpu_time:.2f}",
                    'CPU_Time_Ratio_1_2': f"{comp.cpu_time_ratio_inverse:.4f}",
                    'CPU_Time_Ratio_2_1': f"{comp.cpu_time_ratio:.4f}",
                    'Exec1_Small_Workers': exec1.workers_by_tier.get('SMALL', 0),
                    'Exec1_Medium_Workers': exec1.workers_by_tier.get('MEDIUM', 0),
                    'Exec1_Large_Workers': exec1.workers_by_tier.get('LARGE', 0),
                    'Exec1_Small_CPUs': exec1.cpus_by_tier.get('SMALL', 0),
                    'Exec1_Medium_CPUs': exec1.cpus_by_tier.get('MEDIUM', 0),
                    'Exec1_Large_CPUs': exec1.cpus_by_tier.get('LARGE', 0),
                    'Exec2_Small_Workers': exec2.workers_by_tier.get('SMALL', 0),
                    'Exec2_Medium_Workers': exec2.workers_by_tier.get('MEDIUM', 0),
                    'Exec2_Large_Workers': exec2.workers_by_tier.get('LARGE', 0),
                    'Exec2_Small_CPUs': exec2.cpus_by_tier.get('SMALL', 0),
                    'Exec2_Medium_CPUs': exec2.cpus_by_tier.get('MEDIUM', 0),
                    'Exec2_Large_CPUs': exec2.cpus_by_tier.get('LARGE', 0),
                }
                writer.writerow(row)
        
        print(f"CSV comparison report saved to: {output_file}")
    
    def generate_comparison_report(self, comparisons: List[TieredComparisonResult], exec1_name: str = None, exec2_name: str = None, exec1_only: Set[str] = None, exec2_only: Set[str] = None) -> str:
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
        
        # Show exclusive migrations if any exist
        if exec1_only or exec2_only:
            lines.append("")
            lines.append("Exclusive Migrations:")
            if exec1_only:
                lines.append(f"  Exec1 Only ({len(exec1_only)}): {sorted(exec1_only)}")
            if exec2_only:
                lines.append(f"  Exec2 Only ({len(exec2_only)}): {sorted(exec2_only)}")
        
        # Header
        exec1_short = (exec1_name[:8] + "..") if exec1_name and len(exec1_name) > 10 else (exec1_name or "Exec1")
        exec2_short = (exec2_name[:8] + "..") if exec2_name and len(exec2_name) > 10 else (exec2_name or "Exec2")
        
        lines.append("")
        lines.append(f"{'Migration':<12} {'Execution Time':<35} {'Workers':<30} {'CPUs':<30} {'CPU Time':<35}")
        lines.append(f"{'ID':<12} {exec1_short:<10} {exec2_short:<10} {'1/2':<5} {'2/1':<5} {exec1_short:<8} {exec2_short:<8} {'1/2':<5} {'2/1':<5} {exec1_short:<8} {exec2_short:<8} {'1/2':<5} {'2/1':<5} {exec1_short:<10} {exec2_short:<10} {'1/2':<5} {'2/1':<5}")
        lines.append("-" * 125)
        
        # Data rows
        for comp in comparisons:
            exec1 = comp.exec1_metrics
            exec2 = comp.exec2_metrics
            
            # Format large numbers
            exec1_time = self._format_time(exec1.total_execution_time)
            exec2_time = self._format_time(exec2.total_execution_time)
            time_ratio_12 = f"{comp.execution_time_ratio_inverse:.2f}"
            time_ratio_21 = f"{comp.execution_time_ratio:.2f}"
            
            exec1_cpu_time = self._format_time(exec1.cpu_time)
            exec2_cpu_time = self._format_time(exec2.cpu_time)
            cpu_time_ratio_12 = f"{comp.cpu_time_ratio_inverse:.2f}"
            cpu_time_ratio_21 = f"{comp.cpu_time_ratio:.2f}"
            
            worker_ratio_12 = f"{comp.worker_count_ratio_inverse:.2f}"
            worker_ratio_21 = f"{comp.worker_count_ratio:.2f}"
            
            cpu_ratio_12 = f"{comp.cpu_count_ratio_inverse:.2f}"
            cpu_ratio_21 = f"{comp.cpu_count_ratio:.2f}"
            
            lines.append(f"{comp.migration_id:<12} {exec1_time:<10} {exec2_time:<10} {time_ratio_12:<5} {time_ratio_21:<5} "
                        f"{exec1.total_workers:<8} {exec2.total_workers:<8} {worker_ratio_12:<5} {worker_ratio_21:<5} "
                        f"{exec1.total_cpus:<8} {exec2.total_cpus:<8} {cpu_ratio_12:<5} {cpu_ratio_21:<5} "
                        f"{exec1_cpu_time:<10} {exec2_cpu_time:<10} {cpu_time_ratio_12:<5} {cpu_time_ratio_21:<5}")
        
        # Summary statistics
        lines.append("")
        lines.append("="*125)
        lines.append("AGGREGATE ANALYSIS")
        lines.append("="*125)
        
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
        if total_exec1_time > 0:
            lines.append(f"  Exec1/Exec2: {total_exec1_time/total_exec2_time:.2f} (>1.0 = exec2 faster)")
            lines.append(f"  Exec2/Exec1: {total_exec2_time/total_exec1_time:.2f} (<1.0 = exec2 faster)")
        
        lines.append("")
        lines.append("Total Workers:")
        lines.append(f"  Exec1:      {total_exec1_workers}")
        lines.append(f"  Exec2:      {total_exec2_workers}")
        if total_exec2_workers > 0:
            lines.append(f"  Exec1/Exec2: {total_exec1_workers/total_exec2_workers:.2f}")
        if total_exec1_workers > 0:
            lines.append(f"  Exec2/Exec1: {total_exec2_workers/total_exec1_workers:.2f}")
        
        lines.append("")
        lines.append("Total CPUs:")
        lines.append(f"  Exec1:      {total_exec1_cpus}")
        lines.append(f"  Exec2:      {total_exec2_cpus}")
        if total_exec2_cpus > 0:
            lines.append(f"  Exec1/Exec2: {total_exec1_cpus/total_exec2_cpus:.2f}")
        if total_exec1_cpus > 0:
            lines.append(f"  Exec2/Exec1: {total_exec2_cpus/total_exec1_cpus:.2f}")
        
        lines.append("")
        lines.append("Total CPU Time:")
        lines.append(f"  Exec1:      {self._format_time(total_exec1_cpu_time)}")  
        lines.append(f"  Exec2:      {self._format_time(total_exec2_cpu_time)}")
        if total_exec2_cpu_time > 0:
            lines.append(f"  Exec1/Exec2: {total_exec1_cpu_time/total_exec2_cpu_time:.2f}")
        if total_exec1_cpu_time > 0:
            lines.append(f"  Exec2/Exec1: {total_exec2_cpu_time/total_exec1_cpu_time:.2f}")
        
        return "\n".join(lines)
    
    def save_comparison_report(self, comparisons: List[TieredComparisonResult], output_file: str, exec1_name: str = None, exec2_name: str = None, exec1_only: Set[str] = None, exec2_only: Set[str] = None):
        """Save the tabular comparison report to a text file."""
        report_text = self.generate_comparison_report(comparisons, exec1_name, exec2_name, exec1_only, exec2_only)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(report_text)
        
        print(f"Tabular comparison report saved to: {output_file}")

    def generate_html_report(self, comparisons: List[TieredComparisonResult], exec1_name: str = None, exec2_name: str = None, exec1_only: Set[str] = None, exec2_only: Set[str] = None) -> str:
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
        
        table {{ border-collapse: collapse; width: 100%; margin-bottom: 20px; background-color: white; }}
        th, td {{ border: 1px solid #ddd; padding: 10px; text-align: left; }}
        th {{ background-color: #f2f2f2; font-weight: bold; }}
        tr:nth-child(even) {{ background-color: #f9f9f9; }}
        tr:hover {{ background-color: #f0f0f0; }}
        
        .migration-details {{ margin-top: 20px; }}
        .best-time {{ background-color: #c8e6c9 !important; font-weight: bold; }}
        .best-ratio {{ background-color: #c8e6c9 !important; font-weight: bold; }}
        
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
            <p><strong>Exec1/Exec2:</strong> {total_exec1_time/total_exec2_time:.2f} (>1.0 = exec2 faster)</p>
            <p><strong>Exec2/Exec1:</strong> {total_exec2_time/total_exec1_time:.2f} (<1.0 = exec2 faster)</p>
        </div>
        <div>
            <h3>Total Workers</h3>
            <p><strong>Execution 1:</strong> {total_exec1_workers:,}</p>
            <p><strong>Execution 2:</strong> {total_exec2_workers:,}</p>
            <p><strong>Exec1/Exec2:</strong> {total_exec1_workers/total_exec2_workers:.2f}</p>
            <p><strong>Exec2/Exec1:</strong> {total_exec2_workers/total_exec1_workers:.2f}</p>
        </div>
        <div>
            <h3>Total CPUs</h3>
            <p><strong>Execution 1:</strong> {total_exec1_cpus:,}</p>
            <p><strong>Execution 2:</strong> {total_exec2_cpus:,}</p>
            <p><strong>Exec1/Exec2:</strong> {total_exec1_cpus/total_exec2_cpus:.2f}</p>
            <p><strong>Exec2/Exec1:</strong> {total_exec2_cpus/total_exec1_cpus:.2f}</p>
        </div>
        <div>
            <h3>Total CPU Time</h3>
            <p><strong>Execution 1:</strong> {self._format_time(total_exec1_cpu_time)}</p>
            <p><strong>Execution 2:</strong> {self._format_time(total_exec2_cpu_time)}</p>
            <p><strong>Exec1/Exec2:</strong> {total_exec1_cpu_time/total_exec2_cpu_time:.2f}</p>
            <p><strong>Exec2/Exec1:</strong> {total_exec2_cpu_time/total_exec1_cpu_time:.2f}</p>
        </div>
    </div>
</div>

<div class="migration-details">
    <h2>Per-Migration Comparison</h2>
    <table>
        <thead>
            <tr>
                <th rowspan="2">Migration ID</th>
                <th colspan="4">Execution Time</th>
                <th colspan="4">Workers</th>
                <th colspan="4">CPUs</th>
                <th colspan="4">CPU Time</th>
                <th colspan="6">Exec1 Tier Distribution</th>
                <th colspan="6">Exec2 Tier Distribution</th>
            </tr>
            <tr>
                <th>Exec1</th>
                <th>Exec2</th>
                <th class="ratio-header">1/2</th>
                <th class="ratio-header">2/1</th>
                <th>Exec1</th>
                <th>Exec2</th>
                <th class="ratio-header">1/2</th>
                <th class="ratio-header">2/1</th>
                <th>Exec1</th>
                <th>Exec2</th>
                <th class="ratio-header">1/2</th>
                <th class="ratio-header">2/1</th>
                <th>Exec1</th>
                <th>Exec2</th>
                <th class="ratio-header">1/2</th>
                <th class="ratio-header">2/1</th>
                <th>Small W</th>
                <th>Med W</th>
                <th>Large W</th>
                <th>Small C</th>
                <th>Med C</th>
                <th>Large C</th>
                <th>Small W</th>
                <th>Med W</th>
                <th>Large W</th>
                <th>Small C</th>
                <th>Med C</th>
                <th>Large C</th>
            </tr>
        </thead>
        <tbody>"""
        
        # Process each comparison and determine best values for highlighting
        for comp in comparisons:
            exec1 = comp.exec1_metrics
            exec2 = comp.exec2_metrics
            
            # Determine best execution time and ratio
            best_exec_time = "exec1" if exec1.total_execution_time <= exec2.total_execution_time else "exec2"
            best_exec_ratio = "21" if comp.execution_time_ratio <= comp.execution_time_ratio_inverse else "12"
            
            # Determine best CPU time and ratio
            best_cpu_time = "exec1" if exec1.cpu_time <= exec2.cpu_time else "exec2"
            best_cpu_ratio = "21" if comp.cpu_time_ratio <= comp.cpu_time_ratio_inverse else "12"
            
            # Format values
            exec1_time_str = self._format_time(exec1.total_execution_time)
            exec2_time_str = self._format_time(exec2.total_execution_time)
            exec1_cpu_time_str = self._format_time(exec1.cpu_time)
            exec2_cpu_time_str = self._format_time(exec2.cpu_time)
            
            html += f"""
            <tr>
                <td><strong>{comp.migration_id}</strong></td>
                <td class="number {'best-time' if best_exec_time == 'exec1' else ''}">{exec1_time_str}</td>
                <td class="number {'best-time' if best_exec_time == 'exec2' else ''}">{exec2_time_str}</td>
                <td class="number {'best-ratio' if best_exec_ratio == '12' else ''}">{comp.execution_time_ratio_inverse:.2f}</td>
                <td class="number {'best-ratio' if best_exec_ratio == '21' else ''}">{comp.execution_time_ratio:.2f}</td>
                <td class="number">{exec1.total_workers:,}</td>
                <td class="number">{exec2.total_workers:,}</td>
                <td class="number">{comp.worker_count_ratio_inverse:.2f}</td>
                <td class="number">{comp.worker_count_ratio:.2f}</td>
                <td class="number">{exec1.total_cpus:,}</td>
                <td class="number">{exec2.total_cpus:,}</td>
                <td class="number">{comp.cpu_count_ratio_inverse:.2f}</td>
                <td class="number">{comp.cpu_count_ratio:.2f}</td>
                <td class="number {'best-time' if best_cpu_time == 'exec1' else ''}">{exec1_cpu_time_str}</td>
                <td class="number {'best-time' if best_cpu_time == 'exec2' else ''}">{exec2_cpu_time_str}</td>
                <td class="number {'best-ratio' if best_cpu_ratio == '12' else ''}">{comp.cpu_time_ratio_inverse:.2f}</td>
                <td class="number {'best-ratio' if best_cpu_ratio == '21' else ''}">{comp.cpu_time_ratio:.2f}</td>
                <td class="number">{exec1.workers_by_tier.get('SMALL', 0):,}</td>
                <td class="number">{exec1.workers_by_tier.get('MEDIUM', 0):,}</td>
                <td class="number">{exec1.workers_by_tier.get('LARGE', 0):,}</td>
                <td class="number">{exec1.cpus_by_tier.get('SMALL', 0):,}</td>
                <td class="number">{exec1.cpus_by_tier.get('MEDIUM', 0):,}</td>
                <td class="number">{exec1.cpus_by_tier.get('LARGE', 0):,}</td>
                <td class="number">{exec2.workers_by_tier.get('SMALL', 0):,}</td>
                <td class="number">{exec2.workers_by_tier.get('MEDIUM', 0):,}</td>
                <td class="number">{exec2.workers_by_tier.get('LARGE', 0):,}</td>
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
    <p><span style="background-color: #c8e6c9; padding: 2px 6px; border-radius: 3px;">Green highlighting</span> indicates the best (lowest) execution time, CPU time, and their corresponding best ratios for each migration.</p>
    <p><strong>Ratio Interpretation:</strong></p>
    <ul>
        <li><strong>1/2 > 1.0:</strong> Execution 1 took longer (Execution 2 is faster)</li>
        <li><strong>2/1 < 1.0:</strong> Execution 2 took less time (Execution 2 is faster)</li>
        <li><strong>Higher 1/2 or Lower 2/1 = Better Execution 2 Performance</strong></li>
    </ul>
</div>

</body>
</html>"""
        
        return html

    def save_html_report(self, comparisons: List[TieredComparisonResult], output_file: str, exec1_name: str = None, exec2_name: str = None, exec1_only: Set[str] = None, exec2_only: Set[str] = None):
        """Save the HTML comparison report to a file."""
        html_content = self.generate_html_report(comparisons, exec1_name, exec2_name, exec1_only, exec2_only)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        print(f"HTML comparison report saved to: {output_file}")
        print(f"Open in browser: file://{os.path.abspath(output_file)}")

def main():
    parser = argparse.ArgumentParser(
        description="Compare Two Tiered Migration Simulation Results",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Compare and save results to organized output directory (default - saves to comparison/output/tiered/my_analysis/)
  python tiered_comparison_tool.py --exec1 test_new_5 --exec2 test_new_6 --comparison-exec-name my_tiered_analysis
  
  # Compare without saving reports (console output only)
  python tiered_comparison_tool.py --exec1 test_new_5 --exec2 test_new_6 --comparison-exec-name my_tiered_analysis --omit-reports
  
  # Alternative: specify full paths (backward compatibility)
  python tiered_comparison_tool.py --exec1-path tiered/output/test_new_5 --exec2-path tiered/output/test_new_6 --comparison-exec-name my_tiered_analysis
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
        exec1_run_path = f"../tiered/output/{exec1_name}"
        exec2_run_path = f"../tiered/output/{exec2_name}"
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
        analyzer.print_comparison_summary(comparisons, exec1_name, exec2_name, exec1_only, exec2_only)
        
        # Handle output file generation
        if args.comparison_exec_name and not args.omit_reports:
            # Generate organized output under tiered directory (default behavior)
            output_dir = f"output/tiered/{args.comparison_exec_name}"
            os.makedirs(output_dir, exist_ok=True)
            
            # Generate default filenames
            csv_file = f"{output_dir}/tiered_comparison_report_{args.comparison_exec_name}.csv"
            txt_file = f"{output_dir}/tiered_comparison_summary_{args.comparison_exec_name}.txt"
            html_file = f"{output_dir}/tiered_comparison_report_{args.comparison_exec_name}.html"
            
            # Save all report formats
            analyzer.save_comparison_csv(comparisons, csv_file, exec1_name, exec2_name)
            analyzer.save_comparison_report(comparisons, txt_file, exec1_name, exec2_name, exec1_only, exec2_only)
            analyzer.save_html_report(comparisons, html_file, exec1_name, exec2_name, exec1_only, exec2_only)
            
            print(f"Tiered comparison analysis saved to: {output_dir}/")
        
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()