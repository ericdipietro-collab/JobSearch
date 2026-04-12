from __future__ import annotations
import logging
import json
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

from jobsearch import ats_db

logger = logging.getLogger(__name__)

@dataclass
class ContactStrategyItem:
    contact_name: str
    relationship: str
    last_contact: Optional[str]
    referral_suitability: str # 'high', 'medium', 'low'
    next_action: str
    rationale: str

@dataclass
class CompanyPlaybook:
    company: str
    tier: int
    active_apps: int
    total_apps: int
    conversion_metrics: Dict[str, float]
    top_role_families: List[str]
    underperforming_families: List[str]
    ats_family: str
    submission_friction: str
    recommended_strategy: str # 'apply_cold', 'network_first', 'ask_for_referral', 'watch', 'deprioritize'
    strategy_rationale: str
    contact_strategy: List[ContactStrategyItem] = field(default_factory=list)

class CompanyIntelligenceService:
    """Service for computing company-level summaries and approach strategies."""

    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def get_company_playbook(self, company_name: str) -> CompanyPlaybook:
        """Synthesize history and contacts into a strategic playbook for a company."""
        
        # 1. Basic Stats
        app_rows = self.conn.execute(
            "SELECT status, score, v2_canonical_title, extraction_method FROM applications WHERE company = ?",
            (company_name,)
        ).fetchall()
        
        apps = [dict(r) for r in app_rows]
        total_apps = len(apps)
        active_apps = len([a for a in apps if a['status'] in ('considering', 'prepared', 'screening', 'interviewing')])
        
        # 2. Conversion Metrics
        screens = len([a for a in apps if a['status'] not in ('considering', 'exploring', 'applied', 'prepared')])
        interviews = len([a for a in apps if a['status'] in ('interviewing', 'offer', 'accepted')])
        offers = len([a for a in apps if a['status'] in ('offer', 'accepted')])
        
        metrics = {
            "screen_rate": round(screens / total_apps * 100, 1) if total_apps > 0 else 0,
            "interview_rate": round(interviews / total_apps * 100, 1) if total_apps > 0 else 0,
            "offer_rate": round(offers / total_apps * 100, 1) if total_apps > 0 else 0,
        }

        # 3. Role Family Analysis
        families = {}
        for a in apps:
            f = a.get('v2_canonical_title') or 'Unknown'
            if f not in families: families[f] = {'total': 0, 'success': 0}
            families[f]['total'] += 1
            if a['status'] not in ('considering', 'exploring', 'applied', 'prepared', 'rejected'):
                families[f]['success'] += 1
        
        top_f = sorted([f for f, s in families.items() if s['success'] > 0], 
                       key=lambda f: families[f]['success']/families[f]['total'], reverse=True)
        under_f = sorted([f for f, s in families.items() if s['total'] >= 2 and s['success'] == 0], 
                         key=lambda f: families[f]['total'], reverse=True)

        # 4. ATS & Friction
        ats_family = 'unknown'
        if apps:
            ats_family = apps[0].get('extraction_method', 'unknown')
            
        friction_rows = self.conn.execute(
            """
            SELECT COUNT(*) FROM events e
            JOIN applications a ON e.application_id = a.id
            WHERE a.company = ? AND e.title LIKE 'Submission Blocked:%'
            """,
            (company_name,)
        ).fetchone()
        blocker_count = friction_rows[0]
        friction_label = "Low" if blocker_count == 0 else ("Medium" if blocker_count < 3 else "High")

        # 5. Contact Strategy
        contacts = ats_db.get_network_contacts_for_company(self.conn, company_name)
        contact_items = []
        for c in contacts:
            suitability = 'low'
            if c['relationship'] in ('former_colleague', 'mentor', 'referral'):
                suitability = 'high'
            elif c['relationship'] in ('recruiter', 'friend'):
                suitability = 'medium'
                
            next_action = "Reconnect"
            if suitability == 'high':
                next_action = "Ask for Referral"
            
            contact_items.append(ContactStrategyItem(
                contact_name=c['name'],
                relationship=c['relationship'] or 'Other',
                last_contact=c['last_contact_date'],
                referral_suitability=suitability,
                next_action=next_action,
                rationale=f"Warm relationship ({c['relationship']}) at target company."
            ))

        # 6. Strategy Rules
        strategy, rationale = self._derive_strategy(total_apps, metrics, contact_items, friction_label)

        return CompanyPlaybook(
            company=company_name,
            tier=4, # Placeholder
            active_apps=active_apps,
            total_apps=total_apps,
            conversion_metrics=metrics,
            top_role_families=top_f[:3],
            underperforming_families=under_f[:3],
            ats_family=ats_family,
            submission_friction=friction_label,
            recommended_strategy=strategy,
            strategy_rationale=rationale,
            contact_strategy=contact_items
        )

    def _derive_strategy(self, total_apps, metrics, contacts, friction) -> tuple[str, str]:
        has_warm = any(c.referral_suitability == 'high' for c in contacts)
        
        if has_warm:
            if metrics['screen_rate'] < 20 and total_apps >= 2:
                return 'ask_for_referral', "Cold applications aren't converting despite warm contacts available. Switch to referral-first."
            return 'ask_for_referral', "Warm contacts available. High-leverage path."
            
        if metrics['interview_rate'] > 30:
            return 'apply_cold', "Strong historical traction with direct applications. High confidence path."
            
        if friction == "High":
            return 'network_first', "High submission friction detected. Try to bypass the portal via networking."
            
        if total_apps >= 5 and metrics['screen_rate'] == 0:
            return 'deprioritize', "Repeated failure to convert (0/5 screens). Consider watching for better-fit roles or changing angle."
            
        if total_apps == 0:
            return 'apply_cold', "No history yet. Start with a high-quality tailored application."
            
        return 'watch', "Partial data. Continue monitoring or try networking to build leverage."
