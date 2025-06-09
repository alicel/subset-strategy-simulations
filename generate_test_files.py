from pathlib import Path
import random
import os
from visualization_base import WorkerTier

def generate_test_files(base_dir: str, num_files: int = 50):
    """Generate test subset files with realistic data distributions.
    
    The files will be created with the following characteristics:
    - SMALL tier: 60% of files, 1-5 SSTables, 50MB-500MB each
    - MEDIUM tier: 30% of files, 3-8 SSTables, 500MB-2GB each
    - LARGE tier: 10% of files, 5-15 SSTables, 2GB-10GB each
    """
    # Create base directory if it doesn't exist
    base_dir = Path(base_dir)
    base_dir.mkdir(parents=True, exist_ok=True)
    
    # Create directory structure
    migration_id = "mig007"
    label = "test_migration"
    base_path = base_dir / migration_id / "metadata" / "subsets" / label
    
    # Create the base path
    base_path.mkdir(parents=True, exist_ok=True)
    
    # Tier distribution
    tier_distribution = {
        WorkerTier.SMALL: int(num_files * 0.6),
        WorkerTier.MEDIUM: int(num_files * 0.3),
        WorkerTier.LARGE: int(num_files * 0.1)
    }
    # Ensure we have at least one file per tier and match total
    remaining = num_files - sum(tier_distribution.values())
    tier_distribution[WorkerTier.SMALL] += remaining
    
    # Size ranges in bytes
    size_ranges = {
        WorkerTier.SMALL: (50 * 1024 * 1024, 500 * 1024 * 1024),  # 50MB-500MB
        WorkerTier.MEDIUM: (500 * 1024 * 1024, 2 * 1024 * 1024 * 1024),  # 500MB-2GB
        WorkerTier.LARGE: (2 * 1024 * 1024 * 1024, 10 * 1024 * 1024 * 1024)  # 2GB-10GB
    }
    
    # SSTable ranges
    sstable_ranges = {
        WorkerTier.SMALL: (1, 5),
        WorkerTier.MEDIUM: (3, 8),
        WorkerTier.LARGE: (5, 15)
    }
    
    print(f"Generating {num_files} test files...")
    
    # Track used subset IDs to avoid duplicates across tiers
    used_subset_ids = set()
    current_id = 0
    
    # Generate files for each tier
    for tier, count in tier_distribution.items():
        print(f"\nGenerating {count} {tier.value} tier files...")
        min_size, max_size = size_ranges[tier]
        min_sst, max_sst = sstable_ranges[tier]
        
        for _ in range(count):
            # Find next available subset ID
            while str(current_id) in used_subset_ids:
                current_id += 1
            subset_id = str(current_id)
            used_subset_ids.add(subset_id)
            current_id += 1
            
            num_sstables = random.randint(min_sst, max_sst)
            data_size = random.randint(min_size, max_size)
            
            # Create the directory structure for this subset
            subset_dir = base_path / subset_id / tier.value / str(num_sstables) / str(data_size)
            subset_dir.mkdir(parents=True, exist_ok=True)
            
            # Create the subset file
            subset_file = subset_dir / f"subset-{subset_id}"
            subset_file.touch()
            
            print(f"Created {tier.value} tier file: subset-{subset_id} with {num_sstables} SSTables, size: {data_size / (1024*1024*1024):.2f}GB")

def main():
    # Get parameters from command line
    import argparse
    parser = argparse.ArgumentParser(description='Generate test subset files for simulation')
    parser.add_argument('--output-dir', default='test_data', help='Directory to create test files in')
    parser.add_argument('--num-files', type=int, default=50, help='Number of test files to generate')
    
    args = parser.parse_args()
    
    # Generate the files
    generate_test_files(args.output_dir, args.num_files)
    print("\nTest file generation complete!")

if __name__ == "__main__":
    main() 