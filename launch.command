#!/bin/bash
# macOS launcher — double-click this file to start the app.
# If macOS blocks it: right-click → Open → Open (one time only).

set -e

# ── Move to the script's directory ───────────────────────────────────────────
cd "$(dirname "$0")"

# ── Locate Python 3.9+ ───────────────────────────────────────────────────────
PYTHON=""
for cmd in python3 python; do
    if command -v "$cmd" &>/dev/null; then
        ver=$("$cmd" -c "import sys; print(sys.version_info[:2])" 2>/dev/null)
        if "$cmd" -c "v=$ver; exit(0 if v>=(3,9) else 1)" 2>/dev/null; then
            PYTHON="$cmd"
            break
        fi
    fi
done

if [ -z "$PYTHON" ]; then
    osascript -e 'display dialog "Python 3.9 or newer is required.\n\nDownload it from python.org/downloads, then double-click launch.command again." buttons {"Open python.org"} default button 1'
    open "https://www.python.org/downloads/"
    exit 1
fi

# ── Copy example config if preferences.yaml is missing ───────────────────────
if [ ! -f "config/job_search_preferences.yaml" ] && [ -f "config/job_search_preferences.example.yaml" ]; then
    echo "Copying example preferences to config/job_search_preferences.yaml ..."
    cp "config/job_search_preferences.example.yaml" "config/job_search_preferences.yaml"
    echo "Done. Open Search Settings in the app to customise salary and location."
fi

# ── Create virtual environment on first run ───────────────────────────────────
if [ ! -f ".venv/bin/activate" ]; then
    echo ""
    echo "Setting up for the first time (this takes about a minute) ..."
    echo ""
    $PYTHON -m venv .venv
fi

# ── Activate and install deps ─────────────────────────────────────────────────
source .venv/bin/activate
echo "Checking dependencies..."
pip install -q --upgrade pip
pip install -q -r requirements.txt

# ── Ensure dirs exist ─────────────────────────────────────────────────────────
mkdir -p results config

# ── Launch ────────────────────────────────────────────────────────────────────
echo ""
echo "Starting Job Search Dashboard ..."
echo "It will open in your browser automatically."
echo "Close this window to stop the app."
echo ""
python -m streamlit run app.py --server.headless false --browser.gatherUsageStats false
