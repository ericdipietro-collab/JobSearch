#!/usr/bin/env bash
set -e

echo "============================================================"
echo " Job Search Deep Search Add-on Installer (Mac/Linux)"
echo " Installs Playwright + Chromium for JavaScript-heavy sites"
echo "============================================================"
echo

# Find the project root (parent of this script's directory)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# Prefer venv Python
if [ -f "$PROJECT_DIR/.venv/bin/python" ]; then
    PYTHON="$PROJECT_DIR/.venv/bin/python"
elif [ -f "$PROJECT_DIR/venv/bin/python" ]; then
    PYTHON="$PROJECT_DIR/venv/bin/python"
elif command -v python3 &>/dev/null; then
    PYTHON=python3
elif command -v python &>/dev/null; then
    PYTHON=python
else
    echo "ERROR: Python not found. Run the main launcher first to set up the environment."
    exit 1
fi

echo "Using Python: $PYTHON"
echo

echo "Step 1 of 2: Installing playwright Python package..."
"$PYTHON" -m pip install "playwright>=1.40.0"

echo
echo "Step 2 of 2: Installing Chromium browser (~170MB download)..."
"$PYTHON" -m playwright install chromium

echo
echo "============================================================"
echo " Deep Search installation complete!"
echo
echo " To use it: open the dashboard, go to 'Run Job Search',"
echo " and enable the 'Deep Search' toggle before running."
echo
echo " Or via CLI: python run_job_search_v6.py --deep-search"
echo "============================================================"
echo
