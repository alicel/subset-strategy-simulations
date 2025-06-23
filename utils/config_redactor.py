#!/usr/bin/env python3
"""
Configuration Redactor Utility

This script displays migration runner configuration files with sensitive credentials redacted.
Perfect for demos where you want to show configuration structure without exposing secrets.
"""

import argparse
import yaml
import json
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Union


class ConfigRedactor:
    """Redacts sensitive information from configuration files."""
    
    # Define sensitive field patterns that should be redacted
    SENSITIVE_PATTERNS = [
        # Direct field names (case-insensitive)
        'access_key',
        'secret_key', 
        'password',
        'api_key',
        'token',
        'credential',
        'auth',
        'secret',
        
        # Pattern-based matches
        r'.*_key$',          # any field ending in _key
        r'.*_secret$',       # any field ending in _secret  
        r'.*_password$',     # any field ending in _password
        r'.*_token$',        # any field ending in _token
        r'.*_credential$',   # any field ending in _credential
    ]
    
    # Additional patterns for values that look like credentials
    VALUE_PATTERNS = [
        r'^[A-Z0-9]{20,}$',           # Long uppercase alphanumeric (like AWS keys)
        r'^[a-z0-9+/=]{20,}$',        # Base64-like strings
        r'^[a-zA-Z0-9+/=]{20,}$',     # Mixed case long strings
        r'.*[Aa][Ww][Ss].*',          # Contains "AWS" 
    ]
    
    def __init__(self, redaction_text: str = "***REDACTED***"):
        """Initialize the redactor with custom redaction text."""
        self.redaction_text = redaction_text
    
    def is_sensitive_key(self, key: str) -> bool:
        """Check if a key should be considered sensitive."""
        key_lower = key.lower()
        
        # Check direct matches
        for pattern in self.SENSITIVE_PATTERNS:
            if pattern.startswith('r'):  # regex pattern
                if re.match(pattern[1:-1], key_lower):  # Remove 'r' and quotes
                    return True
            else:  # direct string match
                if pattern in key_lower:
                    return True
        
        return False
    
    def looks_like_credential(self, value: str) -> bool:
        """Check if a value looks like a credential based on patterns."""
        if not isinstance(value, str) or len(value) < 10:
            return False
        
        # Skip placeholder values
        placeholder_patterns = [
            'your_.*_here',
            'placeholder',
            'example',
            'sample',
            'test',
            'demo'
        ]
        
        value_lower = value.lower()
        for pattern in placeholder_patterns:
            if re.search(pattern, value_lower):
                return False
        
        # Check if it matches credential patterns
        for pattern in self.VALUE_PATTERNS:
            if re.match(pattern, value):
                return True
        
        return False
    
    def redact_value(self, key: str, value: Any) -> Any:
        """Redact a value if it's considered sensitive."""
        # Always redact if key is sensitive
        if self.is_sensitive_key(key):
            return self.redaction_text
        
        # For string values, check if they look like credentials
        if isinstance(value, str) and self.looks_like_credential(value):
            return self.redaction_text
        
        # For bucket names, redact if they look like real bucket names (not samples)
        if key.lower() == 'bucket' and isinstance(value, str):
            if not any(word in value.lower() for word in ['sample', 'example', 'your', 'bucket-name']):
                return self.redaction_text
        
        # For endpoints, redact if they contain real hostnames
        if 'endpoint' in key.lower() and isinstance(value, str):
            if not any(word in value.lower() for word in ['example', 'sample', 'your']):
                # Keep the protocol and general structure, redact the hostname
                if '://' in value:
                    protocol = value.split('://')[0]
                    return f"{protocol}://***REDACTED_ENDPOINT***"
                
        return value
    
    def redact_dict(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Recursively redact sensitive information from a dictionary."""
        redacted = {}
        
        for key, value in data.items():
            if isinstance(value, dict):
                redacted[key] = self.redact_dict(value)
            elif isinstance(value, list):
                redacted[key] = self.redact_list(key, value)
            else:
                redacted[key] = self.redact_value(key, value)
        
        return redacted
    
    def redact_list(self, parent_key: str, data: List[Any]) -> List[Any]:
        """Redact sensitive information from a list."""
        redacted = []
        
        for item in data:
            if isinstance(item, dict):
                redacted.append(self.redact_dict(item))
            elif isinstance(item, list):
                redacted.append(self.redact_list(parent_key, item))
            else:
                redacted.append(self.redact_value(parent_key, item))
        
        return redacted
    
    def redact_config(self, config_data: Union[Dict, List]) -> Union[Dict, List]:
        """Main method to redact configuration data."""
        if isinstance(config_data, dict):
            return self.redact_dict(config_data)
        elif isinstance(config_data, list):
            return self.redact_list("root", config_data)
        else:
            return config_data


def find_config_files() -> Dict[str, List[str]]:
    """Find configuration files in the project."""
    config_files = {
        'simple': [],
        'tiered': []
    }
    
    # Look for simple migration configs
    simple_paths = [
        'simple/helper_scripts/simple_migration_config.yaml',
        'simple/helper_scripts/simple_migration_runner_config.yaml', 
        'simple/helper_scripts/simple_migration_config_sample.yaml'
    ]
    
    for path in simple_paths:
        if os.path.exists(path):
            config_files['simple'].append(path)
    
    # Look for tiered migration configs  
    tiered_paths = [
        'tiered/helper_scripts/migration_runner_config.yaml',
        'tiered/helper_scripts/migration_config.yaml',
        'tiered/helper_scripts/migration_config_sample.yaml'
    ]
    
    for path in tiered_paths:
        if os.path.exists(path):
            config_files['tiered'].append(path)
    
    return config_files


def load_config_file(file_path: str) -> Union[Dict, List]:
    """Load a configuration file (YAML or JSON)."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            if file_path.endswith('.yaml') or file_path.endswith('.yml'):
                return yaml.safe_load(f)
            else:
                return json.load(f)
    except Exception as e:
        raise Exception(f"Error loading config file {file_path}: {e}")


def format_output(data: Union[Dict, List], output_format: str) -> str:
    """Format the redacted data for output."""
    if output_format.lower() == 'json':
        return json.dumps(data, indent=2, sort_keys=True)
    else:  # yaml
        return yaml.dump(data, default_flow_style=False, sort_keys=True, indent=2)


def main():
    parser = argparse.ArgumentParser(
        description="Display migration runner configuration files with credentials redacted",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Display all found configuration files
  python config_redactor.py
  
  # Display specific configuration file
  python config_redactor.py --file simple/helper_scripts/simple_migration_config.yaml
  
  # Display in JSON format
  python config_redactor.py --file tiered/helper_scripts/migration_config_sample.yaml --format json
  
  # Use custom redaction text
  python config_redactor.py --redaction-text "***HIDDEN***"
  
  # List available configuration files
  python config_redactor.py --list-configs
        """
    )
    
    parser.add_argument('--file', '-f',
                       help='Specific configuration file to display')
    parser.add_argument('--format', '-o', 
                       choices=['yaml', 'json'],
                       default='yaml',
                       help='Output format (default: yaml)')
    parser.add_argument('--redaction-text', '-r',
                       default='***REDACTED***',
                       help='Text to use for redacted values (default: ***REDACTED***)')
    parser.add_argument('--list-configs', '-l',
                       action='store_true',
                       help='List all available configuration files')
    parser.add_argument('--no-header',
                       action='store_true', 
                       help='Skip the header information')
    
    args = parser.parse_args()
    
    # Create redactor
    redactor = ConfigRedactor(args.redaction_text)
    
    # List configs mode
    if args.list_configs:
        print("Available Configuration Files:")
        print("=" * 40)
        
        config_files = find_config_files()
        
        if config_files['simple']:
            print("\nSimple Migration Configs:")
            for file_path in config_files['simple']:
                exists = "✓" if os.path.exists(file_path) else "✗"
                print(f"  {exists} {file_path}")
        
        if config_files['tiered']:
            print("\nTiered Migration Configs:")
            for file_path in config_files['tiered']:
                exists = "✓" if os.path.exists(file_path) else "✗"
                print(f"  {exists} {file_path}")
        
        if not config_files['simple'] and not config_files['tiered']:
            print("  No configuration files found.")
        
        print("\nUse --file <path> to display a specific configuration file.")
        return
    
    # Single file mode
    if args.file:
        if not os.path.exists(args.file):
            print(f"Error: Configuration file not found: {args.file}")
            return
        
        try:
            config_data = load_config_file(args.file)
            redacted_data = redactor.redact_config(config_data)
            
            if not args.no_header:
                print("=" * 80)
                print("REDACTED CONFIGURATION FILE")
                print("=" * 80)
                print(f"File: {args.file}")
                print(f"Format: {args.format.upper()}")
                print(f"Redaction: {args.redaction_text}")
                print("=" * 80)
                print()
            
            output = format_output(redacted_data, args.format)
            print(output)
            
        except Exception as e:
            print(f"Error: {e}")
        return
    
    # Auto-discovery mode
    config_files = find_config_files()
    
    if not config_files['simple'] and not config_files['tiered']:
        print("No configuration files found.")
        print("Run with --list-configs to see what files are being looked for.")
        return
    
    if not args.no_header:
        print("=" * 80)
        print("REDACTED CONFIGURATION FILES")
        print("=" * 80)
        print(f"Format: {args.format.upper()}")
        print(f"Redaction: {args.redaction_text}")
        print("=" * 80)
    
    # Display all found configs
    all_files = config_files['simple'] + config_files['tiered']
    
    for i, file_path in enumerate(all_files):
        if i > 0:
            print("\n" + "-" * 80 + "\n")
        
        print(f"File: {file_path}")
        print("-" * len(f"File: {file_path}"))
        
        try:
            config_data = load_config_file(file_path)
            redacted_data = redactor.redact_config(config_data)
            output = format_output(redacted_data, args.format)
            print(output)
            
        except Exception as e:
            print(f"Error loading {file_path}: {e}")


if __name__ == "__main__":
    main() 