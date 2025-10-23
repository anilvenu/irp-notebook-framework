#!/bin/bash
# Run E2E integration test for IRP Integration module
#
# Usage:
#   ./run_e2e_test.sh          # Run with default settings
#   ./run_e2e_test.sh -v       # Run with verbose output
#   ./run_e2e_test.sh -vv      # Run with very verbose output

echo "Running IRP Integration E2E Test..."
echo "Base URL: $RISK_MODELER_BASE_URL"
echo ""

# Run the test
cd "$(dirname "$0")/../../.."
pytest workspace/tests/irp_integration/test_irp_integration_e2e.py -s "$@"
