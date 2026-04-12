import streamlit as st
import pandas as pd
from datetime import datetime
from jobsearch.services.ats_discovery_service import ATSDiscoveryService
from jobsearch.services.watch_service import WatchService
from jobsearch.scraper.engine import ScraperEngine
from jobsearch.config.settings import load_yaml, settings
from jobsearch import ats_db

def render_acquisition_ops(conn):
    st.header("⚙️ Acquisition Operations")
    st.write("Control surface for managing the search net and validating discovered sources.")

    t1, t2, t3, t4 = st.tabs(["ATS Candidates", "Priority Watch", "Search Recall", "Ops Summary"])

    with t1:
        _render_ats_candidates(conn)

    with t2:
        _render_priority_watch(conn)

    with t3:
        _render_search_recall(conn)

    with t4:
        _render_ops_summary(conn)


def _render_ats_candidates(conn):
    st.subheader("ATS Board Candidates")
    st.write("Review and validate newly discovered ATS roots before they enter the main registry.")

    discovery_service = ATSDiscoveryService(conn)
    candidates = discovery_service.get_pending_candidates()

    if not candidates:
        st.info("No pending ATS candidates.")
        return

    for c in candidates:
        with st.expander(f"[{c['confidence']:.2f}] {c['company_name']} ({c['ats_family_guess']})"):
            c1, c2 = st.columns([3, 1])
            c1.write(f"**URL:** {c['candidate_url']}")
            c1.write(f"**Rationale:** {c['rationale']}")
            c1.write(f"**Source:** {c['source']}")
            
            with c2:
                if st.button("Promote to Registry", key=f"prom_{c['id']}", type="primary"):
                    try:
                        # Add to main companies yaml
                        data = load_yaml(settings.companies_yaml)
                        companies = data.get("companies", [])
                        
                        # Check if already exists
                        if not any(cmp.get("name") == c['company_name'] for cmp in companies):
                            new_company = {
                                "name": c['company_name'],
                                "careers_url": c['candidate_url'],
                                "adapter": c['ats_family_guess'] or "generic",
                                "tier": 3,
                                "active": True
                            }
                            companies.append(new_company)
                            data["companies"] = companies
                            settings.companies_yaml.write_text(
                                __import__("yaml").safe_dump(data, sort_keys=False, allow_unicode=True),
                                encoding="utf-8"
                            )
                        ats_db.update_candidate_status(conn, c['id'], "promoted")
                        st.success(f"Promoted {c['company_name']} to Main Company List.")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Failed to promote: {e}")
                        
                if st.button("Approve (No Promotion)", key=f"app_{c['id']}"):
                    ats_db.update_candidate_status(conn, c['id'], "approved")
                    st.rerun()
                if st.button("Reject", key=f"rej_{c['id']}"):
                    ats_db.update_candidate_status(conn, c['id'], "rejected")
                    st.rerun()
                if st.button("Validate Now", key=f"val_{c['id']}"):
                    with st.spinner("Validating with Healer..."):
                        from jobsearch.services.healer_service import ATSHealer
                        healer = ATSHealer()
                        try:
                            res = healer.heal_company(c['company_name'])
                            if res and res.status != "NOT_FOUND":
                                st.success(f"✅ Validated: {res.adapter} at {res.careers_url}")
                                ats_db.update_candidate_status(conn, c['id'], "validated")
                            else:
                                st.error("❌ Validation failed or not found.")
                        except Exception as e:
                            st.error(f"Error during validation: {e}")

def _render_priority_watch(conn):
    st.subheader("Priority Watchlist")
    st.write("Manage high-frequency tracking for priority companies.")
    
    watch_service = WatchService(conn)
    watchlist = watch_service.get_watchlist()
    
    if st.button("🚀 Run Watch Now", type="primary"):
        with st.spinner("Polling watched companies..."):
            prefs_data = load_yaml(settings.prefs_yaml)
            comp_data = load_yaml(settings.companies_yaml).get("companies", [])
            res = watch_service.poll_watched(prefs_data, comp_data)
            st.success(f"Watch run complete: {res.get('polled_count', 0)} companies polled.")

    if not watchlist:
        st.info("No companies currently watched.")
    else:
        df = pd.DataFrame(watchlist)
        st.dataframe(
            df[["company", "adapter", "board_state", "last_success_at", "last_attempt_at"]],
            hide_index=True,
            use_container_width=True
        )

def _render_search_recall(conn):
    st.subheader("Search Recall")
    st.write("Run broad discovery queries. Results are ingested with low trust until validated.")

    query = st.text_input("Job Title Query", placeholder="e.g. Senior Python Developer")
    location = st.text_input("Location", placeholder="e.g. Remote")
    days = st.number_input("Max Age (Days)", min_value=1, max_value=30, value=3)

    if st.button("Run Recall Query"):
        if not query:
            st.warning("Please enter a query.")
        else:
            with st.spinner("Running broad discovery..."):
                prefs_data = load_yaml(settings.prefs_yaml)
                engine = ScraperEngine(prefs_data, [], full_refresh=False, db_conn=conn)
                results = engine.run_search_recall([query], location=location, days_old=days)
                st.success(f"Recall complete! Scored {results['scored']} jobs, discovered {results['candidates']} ATS candidates.")

def _render_ops_summary(conn):
    st.subheader("Operations Summary")
    
    c1, c2, c3 = st.columns(3)
    watch_service = WatchService(conn)
    c1.metric("Watched Companies", len(watch_service.get_watchlist()))
    
    discovery_service = ATSDiscoveryService(conn)
    c2.metric("Pending Candidates", len(discovery_service.get_pending_candidates()))
    
    recent_events = conn.execute("SELECT * FROM board_health_events ORDER BY timestamp DESC LIMIT 10").fetchall()
    c3.metric("Recent Health Events", len(recent_events))

    st.markdown("---")
    st.write("**Recent Health Transitions**")
    if recent_events:
        st.dataframe(pd.DataFrame([dict(r) for r in recent_events]), hide_index=True, use_container_width=True)
    else:
        st.write("No recent events.")
