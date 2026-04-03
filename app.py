import streamlit as st

try:
    from jobsearch import app_main
    app_main.main()
except ImportError as e:
    st.error(f"Failed to load application: {e}")
    import traceback
    st.code(traceback.format_exc())
