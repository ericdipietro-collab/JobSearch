"""FastAPI Service for external job injection (Chrome Extension / Bookmarklet)."""

from __future__ import annotations

import logging
import os
import re
import threading
import uvicorn
import yaml
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, Dict, Any

from jobsearch.config.settings import settings
from jobsearch import ats_db
from jobsearch.scraper.models import Job
from jobsearch.scraper.scoring import Scorer
from jobsearch.services.opportunity_service import upsert_job
from jobsearch.services.enrichment_service import EnrichmentService

logger = logging.getLogger(__name__)

app = FastAPI(title="Job Search Injection API")

# Enable wide-open CORS for bookmarklets/extensions
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"],
)

class JobInjectionRequest(BaseModel):
    url: str
    title: Optional[str] = None
    company: Optional[str] = None
    description: Optional[str] = None
    html: Optional[str] = None
    text: Optional[str] = None

@app.get("/health")
async def health():
    return {"status": "ok"}

@app.get("/inject-job")
async def inject_job_get(url: str, title: Optional[str] = None, company: Optional[str] = None):
    """GET version of inject-job for CSP-safe bypass using window.open."""
    req = JobInjectionRequest(url=url, title=title, company=company)
    return await inject_job(req)

@app.post("/inject-job")
async def inject_job(req: JobInjectionRequest):
    """Inject a job from an external source."""
    logger.info(f"Injecting job from URL: {req.url}")
    
    # 1. Load preferences
    try:
        with settings.prefs_yaml.open("r", encoding="utf-8") as h:
            prefs = yaml.safe_load(h) or {}
    except Exception as e:
        logger.error(f"Failed to load preferences: {e}")
        raise HTTPException(status_code=500, detail="Configuration error")

    # 2. Extract details if missing
    title = req.title or "Unknown Position"
    company = req.company or "Unknown Company"
    description = req.description or req.text or req.html or ""
    
    # Heuristic: try to find company name in typical places in the URL or title
    # Normalize check to handle casing or slight variations
    if company.lower() in ("unknown company", "unknown", ""):
        # Breezy.hr often has company in the subdomain: company.breezy.hr
        if "breezy.hr" in req.url.lower():
            match = re.search(r"https?://([^.]+)\.breezy\.hr", req.url, re.I)
            if match:
                company = match.group(1).replace("-", " ").replace("_", " ").title()
        # Greenhouse often has company in path: boards.greenhouse.io/company
        elif "greenhouse.io" in req.url.lower():
            match = re.search(r"greenhouse\.io/([^/]+)", req.url, re.I)
            if match:
                company = match.group(1).replace("-", " ").replace("_", " ").title()
        # Lever often has company in path: jobs.lever.co/company
        elif "lever.co" in req.url.lower():
            match = re.search(r"lever\.co/([^/]+)", req.url, re.I)
            if match:
                company = match.group(1).replace("-", " ").replace("_", " ").title()
        # Fallback: Check if title contains company name (often "Job Title at Company")
        elif " at " in title:
            parts = title.split(" at ")
            if len(parts) > 1:
                company = parts[-1].strip()

    # Dedup check: If this exact URL already exists, ensure it has a WATCH tag and return.
    conn = ats_db.get_connection()
    try:
        existing = conn.execute(
            "SELECT id, scraper_key, company, role, score, fit_band FROM applications WHERE job_url = ? OR canonical_job_url = ?",
            (req.url, req.url)
        ).fetchone()
        if existing:
            if existing["scraper_key"]:
                ats_db.upsert_annotation(conn, existing["scraper_key"], note="Manual injection", tag="WATCH")
            return {
                "status": "success",
                "inserted": False,
                "id": existing["id"],
                "company": existing["company"],
                "title": existing["role"],
                "score": existing["score"],
                "fit_band": existing["fit_band"],
                "note": "Job already exists in database."
            }
    finally:
        conn.close()

    # Use LLM to refine details
    conn = ats_db.get_connection()
    try:
        # Load LLM settings from DB
        google_key = ats_db.get_setting(conn, "google_api_key", default=os.getenv("GOOGLE_API_KEY", ""))
        openai_key = ats_db.get_setting(conn, "openai_api_key", default=os.getenv("OPENAI_API_KEY", ""))
        llm_provider = ats_db.get_setting(conn, "llm_provider", default="gemini")
        ollama_url = ats_db.get_setting(conn, "ollama_base_url", default="http://localhost:11434")
        ollama_model = ats_db.get_setting(conn, "ollama_model", default="llama3.2")
        
        enrichment = EnrichmentService(
            db_conn=conn,
            google_api_key=google_key,
            openai_api_key=openai_key,
            preferred_provider=llm_provider,
            ollama_base_url=ollama_url,
            ollama_model=ollama_model
        )

        # Force LLM extraction if we still have "Unknown", very little data, or noisy source (LinkedIn)
        is_linkedin = "linkedin.com" in req.url.lower()
        if company == "Unknown Company" or title == "Unknown Position" or len(description) < 100 or is_linkedin:
            try:
                # Targeted prompt for extraction
                prompt = f"""Extract the hiring Company Name, Job Title, and a clean Job Description from this job posting data.
URL: {req.url}
Page Title: {req.title}
Content: {(req.text or "")[:3000]}

Return ONLY valid JSON:
{{
    "title": "string",
    "company": "string",
    "description": "string"
}}
"""
                text, _ = enrichment.llm_client.generate(prompt)
                import json
                data = json.loads(enrichment.llm_client.strip_json_response(text))
                
                # Update if LLM found better values
                if data.get("company") and data["company"] != "Unknown Company":
                    company = data["company"]
                if data.get("title") and data["title"] != "Unknown Position":
                    title = data["title"]
                if data.get("description"):
                    description = data["description"]
            except Exception as e:
                logger.warning(f"LLM extraction failed: {e}")

        # 3. Score the job — V1 for hard gates, V2 as primary
        scorer = Scorer(prefs)
        job_data = {
            "title": title,
            "description": description,
            "company": company,
            "url": req.url,
            "location": "",
            "tier": 4,
        }
        score_result = scorer.score_job(job_data)

        # Promote V2 score as primary (mirrors _rescore_saved_jobs logic)
        v1_disqualified = score_result.get("fit_band") in ("Disqualified", "Filtered Out")
        if not v1_disqualified:
            try:
                from jobsearch.scraper.scoring_v2 import build_v2_config_from_prefs, v2_title_pts, score_job_v2
                _v2_cfg, _v2_idx, _v2_pw, _v2_ftbase, _v2_ftmin = build_v2_config_from_prefs(prefs)
                _tpts = v2_title_pts(title, _v2_idx, _v2_cfg, _v2_pw, _v2_ftbase, _v2_ftmin)
                _v2r = score_job_v2(title, description, _tpts, _v2_cfg, _v2_idx)
                score_result["score"] = round(_v2r.final_score, 2)
                score_result["fit_band"] = _v2r.fit_band
                score_result["decision_reason"] = (
                    f"V2: {_v2r.fit_band} | title={_v2r.canonical_title or 'unresolved'}"
                    f" | seniority={_v2r.seniority.band}"
                    f" | anchor={round(_v2r.keyword.anchor_score, 1)}"
                    f" base={round(_v2r.keyword.baseline_score, 1)}"
                    f" neg={round(_v2r.keyword.negative_score, 1)}"
                )
            except Exception as e:
                logger.warning(f"V2 scoring failed, using V1: {e}")

        # Apply AI enrichment if possible
        try:
            enriched = enrichment.enrich_job(title, description)
            score_result = scorer.apply_enrichment_adjustments(score_result, enriched)
        except Exception as e:
            logger.warning(f"Post-injection enrichment failed: {e}")
    finally:
        conn.close()

    # 4. Create Job model
    try:
        import hashlib
        job_id = Job.make_id(company, title, req.url)
        
        job = Job(
            id=job_id,
            company=company,
            role_title_raw=title,
            role=title, # Legacy support
            url=req.url,
            location=score_result.get("location", ""),
            description_excerpt=description[:2000], # Store first 2k chars
            source="Manual Injection",
            source_lane="employer_ats",
            adapter="custom_manual",
            tier=4,
            score=score_result.get("score", 0.0),
            fit_band=score_result.get("fit_band", "Weak Match"),
            matched_keywords=score_result.get("matched_keywords", ""),
            penalized_keywords=score_result.get("penalized_keywords", ""),
            decision_reason=score_result.get("decision_reason", "Manual injection"),
            apply_now_eligible=score_result.get("apply_now_eligible", True),
        )
    except Exception as e:
        logger.error(f"Job model validation failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=422, detail=str(e))
    # Add extra fields used by upsert_job
    job.salary_text = score_result.get("salary_text", "")
    job.salary_min = score_result.get("salary_min")
    job.salary_max = score_result.get("salary_max")
    job.work_type = score_result.get("work_type", "fte")
    job.normalized_compensation_usd = score_result.get("normalized_compensation_usd")

    # 5. Persist to DB
    try:
        conn = ats_db.get_connection()
        inserted, app_id = upsert_job(conn, job)
        # Manually injected jobs always get at least a WATCH tag so they surface
        # in Job Matches regardless of their score (user chose to inject them).
        if inserted:
            ats_db.upsert_annotation(conn, job.id, note="Manual injection", tag="WATCH")
        conn.commit()
        conn.close()
        return {
            "status": "success",
            "inserted": inserted,
            "id": app_id,
            "company": company,
            "title": title,
            "score": job.score,
            "fit_band": job.fit_band
        }
    except Exception as e:
        logger.error(f"Failed to persist injected job: {e}")
        raise HTTPException(status_code=500, detail=str(e))

_api_thread: Optional[threading.Thread] = None

def start_api_server(port: int = 8505):
    """Start the FastAPI server in a background thread if not already running."""
    global _api_thread
    
    # Check if something is already listening on this port (likely a previous dashboard run)
    import socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        in_use = s.connect_ex(('127.0.0.1', port)) == 0
        if in_use:
            logger.info(f"API server already running on port {port} (or port busy). Skipping startup.")
            return

    if _api_thread and _api_thread.is_alive():
        return

    def _run():
        logger.info(f"Starting Injection API on port {port}")
        uvicorn.run(app, host="127.0.0.1", port=port, log_level="warning")

    _api_thread = threading.Thread(target=_run, daemon=True)
    _api_thread.start()

def stop_api_server():
    """Stopping uvicorn properly from a thread is hard, we rely on daemon=True."""
    pass
