# Project Utilities

This directory contains general-purpose utility scripts for the TieredStrategySimulation project.

## Available Utilities

### Configuration Redactor (`config_redactor.py`)

A utility for safely displaying migration runner configuration files with sensitive credentials automatically redacted. Perfect for demos, documentation, and sharing configuration examples.

**Quick Start:**
```bash
# List available configuration files
python3 utils/config_redactor.py --list-configs

# Display a configuration with credentials redacted
python3 utils/config_redactor.py --file simple/helper_scripts/simple_migration_config_sample.yaml
```

**Use Cases:**
- Demo presentations without exposing secrets
- Documentation generation with safe examples
- Sharing configurations for support/debugging
- Training materials with realistic examples

See [`README_config_redactor.md`](README_config_redactor.md) for detailed documentation.

## Usage

All utilities are designed to be run from the project root directory:

```bash
python3 utils/<utility_name>.py [options]
```

## Integration

These utilities are designed to work with the project's:
- Migration runner configurations (simple and tiered)
- Output structures and file formats
- Development and demo workflows

## Contributing

When adding new utilities:
- Include comprehensive documentation
- Follow the established command-line interface patterns
- Ensure compatibility with both simple and tiered strategies
- Add appropriate error handling and user feedback
- Test from the project root directory 