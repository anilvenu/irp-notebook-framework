#!/bin/bash
# =============================================================================
# Jupyter Entrypoint Script for IRP Notebook Framework
# =============================================================================
#
# This script is the container entrypoint. It:
#   1. Initializes Kerberos credentials (if enabled)
#   2. Starts JupyterLab with no token/password authentication
#
# Usage:
#   This script is called automatically as the container entrypoint.
#   Additional arguments are passed to start-notebook.sh.
#
# Location: /usr/local/bin/jupyter-entrypoint.sh (consistent with kerberos-init.sh)
#
# =============================================================================

set -e

# Initialize Kerberos if enabled (ignore failures - notebook should still start)
/usr/local/bin/kerberos-init.sh || true

# Start Jupyter with no authentication and configured notebook directory
exec start-notebook.sh \
    --NotebookApp.token='' \
    --NotebookApp.password='' \
    --NotebookApp.notebook_dir="${NOTEBOOK_HOME_DIR:-/home/jovyan}" \
    "$@"
