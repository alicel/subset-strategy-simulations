# Configuration Redactor Utility

A utility script for displaying migration runner configuration files with sensitive credentials automatically redacted. Perfect for demos, documentation, and sharing configuration examples without exposing secrets.

## Overview

The `config_redactor.py` script automatically detects and redacts sensitive information from YAML and JSON configuration files while preserving the complete structure and non-sensitive values.

## Features

- **Smart Detection**: Automatically identifies sensitive fields like `access_key`, `secret_key`, passwords, tokens, etc.
- **Pattern-Based Redaction**: Uses regex patterns to catch variations like `*_key`, `*_secret`, `*_token`
- **Value Analysis**: Detects credential-like values even when field names don't indicate sensitivity
- **Multiple Formats**: Supports both YAML and JSON input/output
- **Auto-Discovery**: Automatically finds configuration files in the project
- **Customizable**: Configure custom redaction text and output format

## Usage

### Quick Start

```bash
# Show all available configuration files
python3 utils/config_redactor.py --list-configs

# Display a specific configuration file
python3 utils/config_redactor.py --file simple/helper_scripts/simple_migration_config_sample.yaml

# Display in JSON format
python3 utils/config_redactor.py --file tiered/helper_scripts/migration_config_sample.yaml --format json
```

### Command Line Options

| Option | Short | Description |
|--------|-------|-------------|
| `--file` | `-f` | Specific configuration file to display |
| `--format` | `-o` | Output format: `yaml` (default) or `json` |
| `--redaction-text` | `-r` | Custom text for redacted values (default: `***REDACTED***`) |
| `--list-configs` | `-l` | List all available configuration files |
| `--no-header` | | Skip the header information |

### Examples

**List Available Configs:**
```bash
python3 utils/config_redactor.py --list-configs
```

**Display Simple Migration Config:**
```bash
python3 utils/config_redactor.py --file simple/helper_scripts/simple_migration_config_sample.yaml
```

**JSON Output with Custom Redaction:**
```bash
python utils/config_redactor.py --file tiered/helper_scripts/migration_config_sample.yaml --format json --redaction-text "***HIDDEN***"
```

**Clean Output for Piping:**
```bash
python utils/config_redactor.py --file config.yaml --no-header > demo_config.yaml
```

## What Gets Redacted

### Sensitive Field Names
- `access_key`, `secret_key` 
- `password`, `api_key`, `token`
- `credential`, `auth`, `secret`
- Any field ending in `_key`, `_secret`, `_password`, `_token`, `_credential`

### Value Patterns
- Long alphanumeric strings (20+ chars) that look like AWS keys
- Base64-like encoded strings
- Values containing "AWS" 
- Real bucket names (keeps sample/example bucket names)
- Storage endpoints (keeps protocol, redacts hostname)

### Example Output

**Before:**
```yaml
migration:
  access_key: "AKIAIOSFODNN7EXAMPLE" 
  secret_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
  bucket: "my-secret-production-bucket"
  storage_endpoint: "https://s3.eu-west-1.amazonaws.com"
```

**After:**
```yaml
migration:
  access_key: "***REDACTED***"
  secret_key: "***REDACTED***"  
  bucket: "***REDACTED***"
  storage_endpoint: "https://***REDACTED_ENDPOINT***"
```

## Supported Configuration Files

The utility automatically discovers these configuration files:

### Simple Migration Configs
- `simple/helper_scripts/simple_migration_runner_config.yaml`
- `simple/helper_scripts/simple_migration_config_sample.yaml`

### Tiered Migration Configs  
- `comparison/comparison_config.yaml`
- `comparison/tiered_comparison_config.yaml`
- `tiered/helper_scripts/tiered_migration_runner_config.yaml`

## Demo Use Cases

### 1. Screen Sharing / Presentations
```bash
# Clean YAML output perfect for screen sharing
python3 utils/config_redactor.py --file config.yaml --no-header
```

### 2. Documentation Generation
```bash
# Generate redacted examples for documentation
python3 utils/config_redactor.py --file config.yaml --no-header > docs/config_example.yaml
```

### 3. Support/Debugging
```bash
# Share configuration safely when asking for help
python3 utils/config_redactor.py --file config.yaml --format json
```

### 4. Team Demos
```bash
# Show complete configuration structure without exposing secrets
python3 utils/config_redactor.py --list-configs
python3 utils/config_redactor.py --file tiered/helper_scripts/migration_config_sample.yaml
```

## Safety Features

- **Preserves Structure**: Shows complete configuration layout
- **Smart Placeholders**: Keeps obvious placeholder values like `YOUR_KEY_HERE`
- **Protocol Preservation**: For URLs, keeps protocol but redacts hostnames
- **Pattern Matching**: Uses multiple methods to catch credentials
- **No False Positives**: Avoids redacting obvious non-secrets like `"AWS"` (cloud provider name)

## Integration

The utility can be easily integrated into:
- Demo scripts
- Documentation generation pipelines  
- CI/CD for safe config validation
- Support workflows
- Training materials

## Error Handling

- Graceful handling of missing files
- Clear error messages for invalid YAML/JSON
- Continues processing remaining files if one fails
- Validates file existence before processing

Perfect for safely sharing your migration runner configurations during demos and documentation! 