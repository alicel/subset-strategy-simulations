from dataclasses import dataclass
from typing import List
import re
from .simulation import WorkItem
import os
from pathlib import Path

@dataclass
class FileMetadata:
    """Metadata for a subset file in the simple simulation (no tier concept)."""
    full_path: str
    migration_id: str
    label: str
    subset_id: str
    num_sstables: int
    data_size: int
    
    def get_sstables(self) -> List[WorkItem]:
        """Read actual SSTable definitions from the subset file.
        
        Returns:
            List of WorkItem objects with actual SSTable IDs and sizes from the file.
        """
        try:
            # Read the subset file content
            with open(self.full_path, 'r', encoding='utf-8') as f:
                content = f.read().strip()
            
            if not content:
                # Empty file - return empty list to trigger fallback
                return []
            
            # Parse the file content - supports same formats as tiered simulation:
            # 1. CSV format: "sstable_001,1234567"
            # 2. Space separated: "sstable_001 1234567"
            
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
        """Parse FileMetadata from a file path - simple format without tier.
        
        Primary format: <anything>/<migrationId>/metadata/subsets/<Label>/<subsetId>/<numSSTablesInSubset>/<dataSizeOfSubset>/subset-<subsetId>
        Fallback format: <anything>/<migrationId>/metadata/subsets/<Label>/<subsetId>/<tier>/<numSSTablesInSubset>/<dataSizeOfSubset>/subset-<subsetId> (ignores tier)
        """
        # Normalize path to use forward slashes
        normalized_path = path.replace(os.sep, '/')
        
        # Try the simple format first (without tier) - this is the primary format for simple simulation
        # Format: <anything>/<migrationId>/metadata/subsets/<Label>/<subsetId>/<numSSTablesInSubset>/<dataSizeOfSubset>/subset-<subsetId>
        simple_pattern = r'.*/([^/]+)/metadata/subsets/([^/]+)/([^/]+)/(\d+)/(\d+)/subset-\3$'
        match = re.match(simple_pattern, normalized_path)
        
        if match:
            migration_id, label, subset_id, num_sstables_str, data_size_str = match.groups()
            return FileMetadata(
                migration_id=migration_id,
                label=label,
                subset_id=subset_id,
                num_sstables=int(num_sstables_str),
                data_size=int(data_size_str),
                full_path=path
            )
        
        # Fallback: try the tiered format (with tier) for backward compatibility
        # Format: <anything>/<migrationId>/metadata/subsets/<Label>/<subsetId>/<tier>/<numSSTablesInSubset>/<dataSizeOfSubset>/subset-<subsetId>
        tiered_pattern = r'.*/([^/]+)/metadata/subsets/([^/]+)/([^/]+)/([^/]+)/(\d+)/(\d+)/subset-\3$'
        match = re.match(tiered_pattern, normalized_path)
        
        if match:
            # Tiered format: migration_id, label, subset_id, tier, num_sstables, data_size
            migration_id, label, subset_id, tier_str, num_sstables_str, data_size_str = match.groups()
            # Ignore the tier for simple simulation
            return FileMetadata(
                migration_id=migration_id,
                label=label,
                subset_id=subset_id,
                num_sstables=int(num_sstables_str),
                data_size=int(data_size_str),
                full_path=path
            )
        
        # If neither format matches, raise an error
        raise ValueError(
            f"Invalid file path format: {path}\n"
            f"Expected format: <path>/<migrationId>/metadata/subsets/<Label>/<subsetId>/<numSSTablesInSubset>/<dataSizeOfSubset>/subset-<subsetId>\n"
            f"Or fallback format: <path>/<migrationId>/metadata/subsets/<Label>/<subsetId>/<tier>/<numSSTablesInSubset>/<dataSizeOfSubset>/subset-<subsetId>"
        )

def validate_directory_structure(directory: str):
    """Validate that the input directory has the expected structure."""
    if not os.path.exists(directory):
        raise ValueError(f"Input directory does not exist: {directory}")
    
    if not os.path.isdir(directory):
        raise ValueError(f"Input path is not a directory: {directory}")
    
    # Check for permission to read the directory
    try:
        os.listdir(directory)
    except PermissionError:
        raise ValueError(f"Permission denied accessing directory: {directory}")
    
    # Check that the directory directly contains a 'metadata' subdirectory
    metadata_path = os.path.join(directory, "metadata")
    if not os.path.exists(metadata_path):
        raise ValueError(
            f"Invalid directory structure.\n"
            f"The specified directory does not contain a 'metadata' subdirectory: {directory}\n"
            f"Expected structure: <migrationId>/metadata/subsets/<Label>/<subsetId>/...\n"
            f"Please specify the migration directory directly."
        )
    
    if not os.path.isdir(metadata_path):
        raise ValueError(f"'metadata' exists but is not a directory: {metadata_path}")
    
    # Check that metadata contains 'subsets' subdirectory
    subsets_path = os.path.join(metadata_path, "subsets")
    if not os.path.exists(subsets_path):
        raise ValueError(
            f"Invalid metadata structure.\n"
            f"The metadata directory does not contain a 'subsets' subdirectory: {metadata_path}\n"
            f"Expected: {metadata_path}/subsets/..."
        )
    
    if not os.path.isdir(subsets_path):
        raise ValueError(f"'subsets' exists but is not a directory: {subsets_path}")

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
    
    # Validate directory structure before scanning for files
    validate_directory_structure(directory)
    
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
    
    # Sort files by subset_id numerically for deterministic processing
    def sort_key(file_metadata):
        try:
            # Convert subset_id to integer for proper numerical sorting
            subset_id_num = int(file_metadata.subset_id)
        except ValueError:
            # If subset_id is not a number, use string sorting as fallback
            subset_id_num = float('inf')  # Put non-numeric IDs at the end
        return (subset_id_num, file_metadata.subset_id)
    
    valid_files.sort(key=sort_key)
    
    print(f"\nProcessed {len(valid_files)} valid subset files")
    if valid_files:
        print(f"Subset ID range: {valid_files[0].subset_id} to {valid_files[-1].subset_id}")
    
    return valid_files

def parse_input_files(file_paths: List[str]) -> List[FileMetadata]:
    """Parse a list of file paths into FileMetadata objects."""
    valid_files = []
    errors = []
    
    for file_path in file_paths:
        try:
            metadata = FileMetadata.from_path(file_path)
            valid_files.append(metadata)
        except ValueError as e:
            errors.append(f"Error parsing {file_path}: {str(e)}")
    
    if errors:
        print("\nErrors during file parsing:")
        for error in errors:
            print(f"- {error}")
    
    # Sort by subset_id
    def sort_key(file_metadata):
        try:
            subset_id_num = int(file_metadata.subset_id)
        except ValueError:
            subset_id_num = float('inf')
        return (subset_id_num, file_metadata.subset_id)
    
    valid_files.sort(key=sort_key)
    return valid_files 