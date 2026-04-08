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
    if company == "Unknown Company":
        # Breezy.hr often has company in the subdomain: company.breezy.hr
        if "breezy.hr" in req.url:
            match = re.search(r"https?://([^.]+)\.breezy\.hr", req.url)
            if match:
                company = match.group(1).replace("-", " ").title()
        # Greenhouse often has company in path: boards.greenhouse.io/company
        elif "greenhouse.io" in req.url:
            match = re.search(r"greenhouse\.io/([^/]+)", req.url)
            if match:
                company = match.group(1).replace("-", " ").title()
        # Lever often has company in path: jobs.lever.co/company
        elif "lever.co" in req.url:
            match = re.search(r"lever\.co/([^/]+)", req.url)
            if match:
                company = match.group(1).replace("-", " ").title()

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

        # Force LLM extraction if we still have "Unknown" or very little data
        if company == "Unknown Company" or title == "Unknown Position" or len(description) < 100:
            try:
                # Targeted prompt for extraction
                prompt = f"""Extract the hiring Company Name, Job Title, and a clean Job Description from this job posting data.
URL: {req.url}
Page Title: {req.title}
Content: {req.text or ""[:3000]}

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

        # 3. Score the job
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
    """Start the FastAPI server in a background thread."""
    global _api_thread
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
