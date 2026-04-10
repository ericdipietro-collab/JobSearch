"""Profile Service: LLM-driven resume analysis and preference generation."""

from __future__ import annotations
import json
import logging
import os
from typing import Any, Dict, List, Optional

from jobsearch.services.llm_client import LLMClient

logger = logging.getLogger(__name__)

class ProfileService:
    def __init__(
        self,
        api_key: Optional[str] = None,
        google_api_key: Optional[str] = None,
        openai_api_key: Optional[str] = None,
        preferred_provider: Optional[str] = None,
        ollama_base_url: Optional[str] = None,
        ollama_model: Optional[str] = None,
    ):
        # Support both legacy api_key and new multi-provider keys
        google_key = google_api_key or api_key or os.getenv("GOOGLE_API_KEY")
        openai_key = openai_api_key or os.getenv("OPENAI_API_KEY")
        self.llm_client = LLMClient(
            google_api_key=google_key,
            openai_api_key=openai_key,
            preferred_provider=preferred_provider,
            ollama_base_url=ollama_base_url,
            ollama_model=ollama_model,
        )

    def extract_preferences(self, resume_text: str) -> Dict[str, Any]:
        """
        Uses an LLM to extract job search preferences from resume text.
        Returns a dictionary compatible with job_search_preferences.yaml.
        """
        if not self.llm_client.get_active_provider() or self.llm_client.get_active_provider() == "none":
            logger.warning("No LLM provider configured. Cannot extract preferences.")
            return {}

        resume_limit = 2000 if self.llm_client.is_local() else 8000

        # Data first, schema last — improves local model accuracy via recency bias
        prompt = f"""You are a job search preferences extractor. Analyze the resume below and output structured preferences.

Resume Text:
{resume_text[:resume_limit]}

Output a JSON object with exactly this structure (use lowercase keys in all dicts):
{{
    "titles": {{
        "positive_keywords": ["Senior Solution Architect", "Technical Product Manager", "Integration Architect"],
        "positive_weights": {{"senior solution architect": 9, "technical product manager": 9, "integration architect": 8, "platform product manager": 7}},
        "negative_disqualifiers": ["software engineer", "data engineer", "devops engineer", "sales engineer"],
        "must_have_modifiers": []
    }},
    "keywords": {{
        "body_positive": {{"api integration": 9, "product roadmap": 8, "fintech": 7, "stakeholder management": 6}},
        "body_negative": {{"junior": 5, "entry level": 5, "consumer lending": 8}}
    }}
}}

Rules:
- positive_keywords: up to 15 full job title phrases the candidate should target (mixed case OK)
- positive_weights: dict of full title phrases (lowercase) to scores 6-10; 10=perfect fit, 6=relevant adjacent role
- negative_disqualifiers: full title strings that are clearly wrong roles for this candidate
- must_have_modifiers: leave as empty list [] unless resume shows a strong specialization filter
- body_positive: domain skills, tools, industry terms from the resume — scores 5-15 (higher = more rare/specific)
- body_negative: terms signaling unwanted domains or role types — scores 3-30 (higher = stronger exclusion)
- All dict keys must be lowercase strings"""

        try:
            text, _ = self.llm_client.generate(prompt)
            data = json.loads(LLMClient.strip_json_response(text))
            return data
        except Exception as exc:
            logger.error(f"Failed to extract preferences: {exc}")
            return {}

    def merge_preferences(self, current_prefs: Dict[str, Any], extracted: Dict[str, Any]) -> Dict[str, Any]:
        """Merges extracted preferences into current ones without overwriting entire blocks."""
        new_prefs = dict(current_prefs)

        if "titles" not in new_prefs:
            new_prefs["titles"] = {}
        if "keywords" not in new_prefs:
            new_prefs["keywords"] = {}

        ext_titles = extracted.get("titles", {}) or {}
        ext_keywords = extracted.get("keywords", {}) or {}

        # --- positive_keywords: deduped union (case-insensitive) ---
        current_pk = list(new_prefs["titles"].get("positive_keywords") or [])
        existing_lower = {s.lower() for s in current_pk}
        for item in (ext_titles.get("positive_keywords") or []):
            if item and item.lower() not in existing_lower:
                current_pk.append(item)
                existing_lower.add(item.lower())
        new_prefs["titles"]["positive_keywords"] = current_pk

        # --- positive_weights: dict merge, take max weight ---
        current_pw = new_prefs["titles"].get("positive_weights") or {}
        if isinstance(current_pw, list):
            current_pw = {k: v for k, v in current_pw if isinstance(k, str)}
        ext_pw = ext_titles.get("positive_weights") or {}
        if isinstance(ext_pw, list):
            ext_pw = {k: v for k, v in ext_pw if isinstance(k, str)}
        merged_pw = dict(current_pw)
        for k, v in (ext_pw.items() if isinstance(ext_pw, dict) else []):
            try:
                merged_pw[str(k).lower()] = max(merged_pw.get(str(k).lower(), 0), int(v))
            except (ValueError, TypeError):
                pass
        new_prefs["titles"]["positive_weights"] = merged_pw

        # --- negative_disqualifiers: deduped union ---
        current_nd = list(new_prefs["titles"].get("negative_disqualifiers") or [])
        existing_lower = {s.lower() for s in current_nd}
        for item in (ext_titles.get("negative_disqualifiers") or []):
            if item and item.lower() not in existing_lower:
                current_nd.append(item)
                existing_lower.add(item.lower())
        new_prefs["titles"]["negative_disqualifiers"] = current_nd

        # --- must_have_modifiers: deduped union ---
        current_mhm = list(new_prefs["titles"].get("must_have_modifiers") or [])
        existing_lower = {s.lower() for s in current_mhm}
        for item in (ext_titles.get("must_have_modifiers") or []):
            if item and item.lower() not in existing_lower:
                current_mhm.append(item)
                existing_lower.add(item.lower())
        new_prefs["titles"]["must_have_modifiers"] = current_mhm

        # --- body_positive: dict merge, take max weight ---
        current_bp = new_prefs["keywords"].get("body_positive") or {}
        if isinstance(current_bp, list):
            merged_bp = {k: v for k, v in current_bp if isinstance(k, str)}
        else:
            merged_bp = dict(current_bp)
        ext_bp = ext_keywords.get("body_positive") or {}
        if isinstance(ext_bp, list):
            ext_bp = {k: v for k, v in ext_bp if isinstance(k, str)}
        for k, v in (ext_bp.items() if isinstance(ext_bp, dict) else []):
            try:
                merged_bp[str(k).lower()] = max(merged_bp.get(str(k).lower(), 0), int(v))
            except (ValueError, TypeError):
                pass
        # Preserve original storage format
        if isinstance(new_prefs["keywords"].get("body_positive"), list):
            new_prefs["keywords"]["body_positive"] = [[k, v] for k, v in merged_bp.items()]
        else:
            new_prefs["keywords"]["body_positive"] = merged_bp

        # --- body_negative: dict merge, take max weight ---
        current_bn = new_prefs["keywords"].get("body_negative") or {}
        if isinstance(current_bn, list):
            merged_bn = {k: v for k, v in current_bn if isinstance(k, str)}
        else:
            merged_bn = dict(current_bn)
        ext_bn = ext_keywords.get("body_negative") or {}
        if isinstance(ext_bn, list):
            ext_bn = {k: v for k, v in ext_bn if isinstance(k, str)}
        for k, v in (ext_bn.items() if isinstance(ext_bn, dict) else []):
            try:
                merged_bn[str(k).lower()] = max(merged_bn.get(str(k).lower(), 0), int(v))
            except (ValueError, TypeError):
                pass
        if merged_bn:
            if isinstance(new_prefs["keywords"].get("body_negative"), list):
                new_prefs["keywords"]["body_negative"] = [[k, v] for k, v in merged_bn.items()]
            else:
                new_prefs["keywords"]["body_negative"] = merged_bn

        return new_prefs

    def extract_skills(self, resume_text: str) -> List[str]:
        """
        Extract technical skills and competencies from resume text.
        Returns a list of skills suitable for gap analysis.
        """
        if not self.llm_client.get_active_provider() or self.llm_client.get_active_provider() == "none":
            logger.warning("No LLM provider configured. Cannot extract skills.")
            return []

        resume_limit = 2000 if self.llm_client.is_local() else 8000

        # Data first, schema last
        prompt = f"""Extract technical skills from the resume below.

Resume Text:
{resume_text[:resume_limit]}

Return a JSON array of skill names (lowercase, singular). Include: tools, languages, frameworks, platforms, domain expertise.
Example: ["python", "kubernetes", "react", "aws", "sql", "api integration", "product management"]
Return ONLY the JSON array, nothing else."""

        try:
            text, _ = self.llm_client.generate(prompt)
            skills = json.loads(LLMClient.strip_json_response(text))
            if isinstance(skills, list):
                return [s.lower().strip() for s in skills if s]
            return []
        except Exception as exc:
            logger.error(f"Failed to extract skills: {exc}")
            return []

    def analyze_job_fit(self, job_title: str, job_description: str, resume_text: str) -> Dict[str, Any]:
        """
        Uses an LLM to analyze how well a job fits the user's resume.
        """
        if not self.llm_client.get_active_provider() or self.llm_client.get_active_provider() == "none":
            return {}

        jd_limit = 1500 if self.llm_client.is_local() else 4000
        resume_limit = 1500 if self.llm_client.is_local() else 5000

        # Data first, calibration + schema last — optimal for both cloud and local models
        prompt = f"""You are an expert resume strategist evaluating candidate fit for a specific job. Be specific and honest — do not be generous.

=== CANDIDATE CONTEXT ===
Positioning: Technical Product Manager / Product Architect with 18+ years in financial systems, data integration, and API platforms.
Domain depth: Wealthtech, capital markets, brokerage, investment operations, portfolio accounting, ETL/Data Vault, enterprise integrations.
Key proof points:
- Sole architect of NFS/Truist brokerage integration: $200B+ AUM, 20,000+ daily syncs, zero-to-certified production in 18 months
- Performance wins: 7min→12sec tax-lot processing; 3hr→60min IDW batch; SLA recovery 70%→95%
- Stakeholder reach: 8 teams / 40+ stakeholders, routine CIO-facing delivery at InvestCloud
- 13-year arc at Allstate Investments across Senior BSA, Product Lead, and Data Architect roles
Title note: Formal titles (Senior BSA, Product Lead) understated scope — candidate designed architecture, owned requirements end-to-end, and drove delivery without formal architecture titles.
Best-fit role archetypes: Technical Product Manager, Product Architect, Solution Architect, Senior BSA/Systems Analyst, API/Integrations Lead, Data Platform PM, Client Implementation Lead.
Weaker fits: roles over-indexing on direct Aladdin administration, Web3/crypto, heavy developer tooling, pure consumer growth, or large people-management org.

=== ROLE-FAMILY STORY ROUTING ===
positioning_angle MUST be exactly one of these six labels (not the job title):
1. Technical Product Manager
2. Product Architect / Solution Architect
3. API / Integrations
4. Data Platform / Architecture
5. Senior BSA / Client Implementation
6. AI-enabled workflow

Use this to choose interview_lead based on the positioning_angle you select:
- Technical Product Manager → lead with NFS/Truist product tradeoffs, roadmap influence, CIO-facing delivery, stakeholder coordination
- Product Architect / Solution Architect (use for: Solution Architect, Customer Solution Architect, Solutions Consultant, Implementation Architect, Technical Architect) → lead with NFS end-to-end platform design, API contract definition, rollback/data integrity, multi-system orchestration
- API / Integrations (use for: Integrations PM, API PM, Integration Engineer/Architect) → lead with NFS five-API orchestration, Plaid/DTCC/FIS/Morningstar integrations, delta sync, performance validation
- Data Platform / Architecture (use for: Data PM, Analytics PM, Data Architect) → lead with IDW modernization, Data Vault design, SLA recovery (70%→95%), lineage, reconciliation
- Senior BSA / Client Implementation (use for: Senior BSA, Business Analyst, Implementation Manager, Client Success) → lead with requirements authorship, client demos, ETL mapping, Truist launch dependency on delivery quality
- AI-enabled workflow (use for: AI PM, Automation PM, AI Product roles) → lead with job search platform as workflow-automation proof point (secondary signal, not core identity)

=== COMMON OBJECTIONS (use to write key_objection) ===
- "Titles don't match" → Title lagged scope; architecture and product ownership were the actual job at both InvestCloud and Allstate
- "Not a classical architect" → Framing is TPM/Product Architect, not pure enterprise architect; the work shows platform design fluency
- "No direct reports" → Led through technical authority, spec review, stakeholder alignment, and mentoring across 8 teams / 40+ people
- "Is the AI angle real?" → Strongest when framed as workflow acceleration and practical product iteration, not AI engineering

=== JOB POSTING ===
Title: {job_title}
{job_description[:jd_limit]}

=== CANDIDATE RESUME ===
{resume_text[:resume_limit]}

=== SCORING CALIBRATION ===
90-100: Near-perfect domain + seniority match, candidate exceeds most requirements
70-89: Strong fit, meets most requirements, only minor gaps
50-69: Partial fit, relevant background but notable skill or domain gaps
30-49: Weak fit, some transferable skills but significant misalignment
0-29: Wrong domain or seniority level

Return ONLY this JSON object:
{{
    "match_score": <int 0-100 per calibration>,
    "positioning_angle": "<which role archetype from the list above fits this JD best>",
    "summary": "<2 sentences: overall fit verdict and the single most important determining factor>",
    "pros": ["<specific candidate strength directly matching a JD requirement>", "<specific strength>", "<specific strength>"],
    "cons": ["<specific gap or mismatch>", "<specific gap>"],
    "missing_skills": ["<skill explicitly required by JD that is absent or weak in the resume>"],
    "interview_lead": "<which specific career story or proof point to open with for this role, per the story routing above>",
    "key_objection": "<the single objection from the list above MOST SPECIFIC to this JD — use 'No direct reports' if the JD explicitly requires managing or mentoring PMs; use 'Not a classical architect' if it requires formal architecture credentials or certifications; use 'Is the AI angle real?' if AI/LLM is core to the product (not just a tool); default to 'Titles don't match' only if none of the others apply — then one sentence on how to frame it>"
}}"""

        try:
            text, _ = self.llm_client.generate(prompt)
            return json.loads(LLMClient.strip_json_response(text))
        except Exception as exc:
            logger.error(f"Failed to analyze job fit: {exc}")
            return {}
