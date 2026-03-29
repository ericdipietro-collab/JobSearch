#!/bin/bash
# Linux launcher — run with:  bash launch.sh
# Or make executable first:   chmod +x launch.sh && ./launch.sh

set -e
cd "$(dirname "$0")"

# ── Locate Python 3.9+ ───────────────────────────────────────────────────────
PYTHON=""
for cmd in python3 python; do
    if command -v "$cmd" &>/dev/null; then
        if "$cmd" -c "import sys; exit(0 if sys.version_info>=(3,9) else 1)" 2>/dev/null; then
            PYTHON="$cmd"
            break
        fi
    fi
done

if [ -z "$PYTHON" ]; then
    echo ""
    echo "  Python 3.9 or newer is required but was not found."
    echo ""
    echo "  Install it with your package manager, e.g.:"
    echo "    Ubuntu/Debian:  sudo apt install python3 python3-venv"
    echo "    Fedora/RHEL:    sudo dnf install python3"
    echo ""
    exit 1
fi

# ── Copy example config if missing ───────────────────────────────────────────
if [ ! -f "config/job_search_preferences.yaml" ] && [ -f "config/job_search_preferences.example.yaml" ]; then
    echo "Copying example preferences ..."
    cp "config/job_search_preferences.example.yaml" "config/job_search_preferences.yaml"
    echo "Done. Customise salary and location in Search Settings."
fi

# ── Create virtual environment on first run ───────────────────────────────────
if [ ! -f ".venv/bin/activate" ]; then
    echo ""
    echo "Setting up for the first time ..."
    $PYTHON -m venv .venv
fi

source .venv/bin/activate
echo "Checking dependencies..."
pip install -q --upgrade pip
pip install -q -r requirements.txt
mkdir -p results config

echo ""
echo "Starting Job Search Dashboard ..."
python -m streamlit run app.py --server.headless false --browser.gatherUsageStats false
