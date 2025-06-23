#!/bin/bash

# Example script demonstrating how to use the tiered comparison tool

echo "==================================================================="
echo "Tiered vs Tiered Migration Simulation Comparison - Example Usage"
echo "==================================================================="

echo ""
echo "This script demonstrates different ways to use the tiered comparison tool."
echo ""

# Check if we have the required execution directories
if [ ! -d "../tiered/output/test_new_5" ] || [ ! -d "../tiered/output/test_new_6" ]; then
    echo "Error: Required tiered execution directories not found."
    echo "Expected: ../tiered/output/test_new_5 and ../tiered/output/test_new_6"
    echo "Please run tiered simulations first to generate the required data."
    exit 1
fi

echo "1. Basic console comparison (quickest way to see results):"
echo "   python tiered_comparison_tool.py --exec1 test_new_5 --exec2 test_new_6"
echo ""

echo "2. Organized output with CSV and text reports (recommended for analysis):"
echo "   python tiered_comparison_tool.py --exec1 test_new_5 --exec2 test_new_6 --comparison-exec-name my_analysis --output-csv"
echo ""

echo "3. Custom output location:"
echo "   python tiered_comparison_tool.py --exec1 test_new_5 --exec2 test_new_6 --output my_custom_results.csv"
echo ""

echo "4. Using full paths (advanced usage):"
echo "   python tiered_comparison_tool.py --exec1-path ../tiered/output/test_new_5 --exec2-path ../tiered/output/test_new_6"
echo ""

echo "Let's run example #1 - Basic console comparison:"
echo "==================================================================="
python tiered_comparison_tool.py --exec1 test_new_5 --exec2 test_new_6

echo ""
echo "==================================================================="
echo "Example completed!"
echo ""
echo "Key features of this tiered comparison tool:"
echo "- Compares two different tiered strategy executions"
echo "- Same core metrics as simple vs tiered tool (execution time, workers, CPUs, CPU time)"
echo "- Additional tier-by-tier breakdown (SMALL, MEDIUM, LARGE tiers)"
echo "- Organized output structure compatible with existing comparison workflow"
echo "- CSV and text report generation for further analysis"
echo ""
echo "For more details, see README_tiered_comparison.md"
echo "==================================================================" 