from dataclasses import dataclass
from typing import List
import re
from visualization_base import WorkerTier
import os
from pathlib import Path

@dataclass
class FileMetadata:
    full_path: str
    migration_id: str
    label: str
    subset_id: str
    tier: WorkerTier
    num_sstables: int
    data_size: int
    
    def get_sstables(self) -> List['WorkItem']:
        """Read actual SSTable definitions from the subset file.
        
        Returns:
            List of WorkItem objects with actual SSTable IDs and sizes from the file.
            
        Note: This method should parse the actual subset file content to extract
        the real SSTable definitions. The format needs to be specified.
        """
        from simulation import WorkItem
        
        try:
            # Read the subset file content
            with open(self.full_path, 'r', encoding='utf-8') as f:
                content = f.read().strip()
            
            if not content:
                # Empty file - return empty list to trigger fallback
                return []
            
            # TODO: Parse the actual format once we know what it should be
            # For now, this is a placeholder that expects format: "sstable_id,size"
            # Examples of possible formats:
            # 1. CSV format: "sstable_001,1234567"
            # 2. Space separated: "sstable_001 1234567"  
            # 3. JSON format: {"sstable_001": 1234567, ...}
            
            sstables = []
            lines = content.strip().split('\n')
            
            for line in lines:
                line = line.strip()
                if not line or line.startswith('#'):  # Skip empty lines and comments
                    continue
                    
                # Try comma-separated format first
                if ',' in line:
                    parts = line.split(',')
                    if len(parts) == 2:
                        sstable_id = parts[0].strip()
                        size = int(parts[1].strip())
                        sstables.append(WorkItem(sstable_id, size))
                # Try space-separated format
                elif ' ' in line:
                    parts = line.split()
                    if len(parts) == 2:
                        sstable_id = parts[0].strip()
                        size = int(parts[1].strip())
                        sstables.append(WorkItem(sstable_id, size))
                else:
                    raise ValueError(f"Unrecognized line format: {line}")
            
            return sstables
            
        except FileNotFoundError:
            raise ValueError(f"Subset file not found: {self.full_path}")
        except (ValueError, IOError) as e:
            raise ValueError(f"Error reading SSTable definitions from {self.full_path}: {str(e)}") from e
    
    @staticmethod
    def from_path(path: str) -> 'FileMetadata':
        # Normalize path to use forward slashes
        normalized_path = path.replace(os.sep, '/')
        
        # Expected format: <anything>/<migrationId>/metadata/subsets/<Label>/<subsetId>/<tier>/<numSSTablesInSubset>/<dataSizeOfSubset>/subset-<subsetId>
        pattern = r'.*/([^/]+)/metadata/subsets/([^/]+)/([^/]+)/([^/]+)/(\d+)/(\d+)/subset-\3$'
        match = re.match(pattern, normalized_path)
        if not match:
            raise ValueError(f"Invalid file path format: {path}\nExpected format: <path>/<migrationId>/metadata/subsets/<Label>/<subsetId>/<tier>/<numSSTablesInSubset>/<dataSizeOfSubset>/subset-<subsetId>")
            
        migration_id, label, subset_id, tier_str, num_sstables_str, data_size_str = match.groups()
        
        try:
            tier = WorkerTier(tier_str)  # Now expects uppercase tier names
        except ValueError:
            raise ValueError(f"Invalid tier: {tier_str}. Must be one of: {[t.value for t in WorkerTier]}")
            
        return FileMetadata(
            migration_id=migration_id,
            label=label,
            subset_id=subset_id,
            tier=tier,
            num_sstables=int(num_sstables_str),
            data_size=int(data_size_str),
            full_path=path
        )

def find_subset_files(directory: str) -> List[str]:
    """Find all subset files in the given directory and its subdirectories."""
    subset_files = []
    # Convert to absolute path to ensure consistent handling
    abs_directory = os.path.abspath(directory)
    
    for root, _, files in os.walk(abs_directory):
        for file in files:
            if file.startswith('subset-'):
                full_path = os.path.join(root, file)
                subset_files.append(full_path)
    return subset_files

def parse_input_directory(directory: str) -> List[FileMetadata]:
    """Scan a directory for subset files and parse them into FileMetadata objects."""
    print(f"Scanning directory: {directory}")
    subset_files = find_subset_files(directory)
    print(f"Found {len(subset_files)} subset files")
    
    valid_files = []
    errors = []
    
    for file_path in subset_files:
        try:
            metadata = FileMetadata.from_path(file_path)
            valid_files.append(metadata)
        except ValueError as e:
            errors.append(f"Error parsing {file_path}: {str(e)}")
    
    if errors:
        print("\nWarnings during file parsing:")
        for error in errors:
            print(f"- {error}")
    
    # Group files by tier for summary
    files_by_tier = {tier: [] for tier in WorkerTier}
    for file in valid_files:
        files_by_tier[file.tier].append(file)
    
    print("\nFiles found by tier:")
    for tier in WorkerTier:
        tier_files = files_by_tier[tier]
        print(f"{tier.value}: {len(tier_files)} files")
    
    return valid_files

def parse_input_files(file_paths: List[str]) -> List[FileMetadata]:
    """Parse a list of file paths into FileMetadata objects."""
    return [FileMetadata.from_path(path) for path in file_paths] 