import streamlit as st
import sys
from pathlib import Path

if "jobsearch" not in sys.modules:
    sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))

try:
    from jobsearch import app_main
    app_main.main()
except ImportError as e:
    st.error(f"Failed to load application: {e}")
    import traceback
    st.code(traceback.format_exc())
