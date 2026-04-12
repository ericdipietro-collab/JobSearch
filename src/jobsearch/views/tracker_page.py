"""
Application Tracker — mini CRM view.
Renders inside the main app.py navigation via render_tracker(conn).
"""
from __future__ import annotations

import csv
from datetime import date, datetime, timedelta
from typing import Optional

import pandas as pd
import streamlit as st
import yaml

from jobsearch import ats_db as db
from jobsearch.config.settings import settings
from jobsearch.scraper.scoring import normalize_compensation
from jobsearch.services.email_signal_service import (
    classify_email_signal,
    infer_interview_type,
    signal_resolution_for_existing_application,
)
from jobsearch.services.gmail_sync_service import sync_gmail_email_signals
from jobsearch.views.export_components import render_quick_export
from jobsearch.services.readiness_service import ReadinessService
from jobsearch.views.readiness_components import render_readiness_badge

FORMAL_TRACKER_EXCLUDED_STATUSES = {"considering"}
AUTO_FOLLOW_UP_DAYS = {
    "exploring": None,
    "considering": None,
    "applied": 7,
    "screening": 3,
    "interviewing": 2,
    "offer": 2,
    "accepted": None,
    "rejected": None,
    "withdrawn": None,
}

WORK_TYPE_LABELS = {
    "fte": "Full-time salary",
    "w2_contract": "W2 hourly",
    "1099_contract": "1099 hourly",
    "c2c_contract": "Corp-to-corp hourly",
    "part_time": "Part-time",
    "temporary": "Temporary",
    "internship": "Internship",
}

# ── Helpers ────────────────────────────────────────────────────────────────────

def _status_badge(status: str) -> str:
    color = db.STATUS_COLORS.get(status, "#6b7280")
    return (
        f'<span style="background:{color};color:#fff;padding:2px 10px;'
        f'border-radius:999px;font-size:0.75rem;font-weight:600;'
        f'text-transform:uppercase;letter-spacing:.04em">{status}</span>'
    )


def _stars(n: Optional[int]) -> str:
    if not n:
        return "—"
    return "⭐" * int(n)


def _fmt_dt(iso: Optional[str]) -> str:
    if not iso:
        return "—"
    try:
        dt = datetime.fromisoformat(iso)
        if "T" in iso:
            return dt.strftime("%m/%d/%y %I:%M %p").lstrip("0")
        return dt.strftime("%m/%d/%y").lstrip("0")
    except Exception:
        return iso[:10]


def _days_until(iso: Optional[str]) -> Optional[int]:
    if not iso:
        return None
    try:
        target = date.fromisoformat(iso[:10])
        return (target - date.today()).days
    except Exception:
        return None


def _event_day_from_signal(signal, fallback_day: str) -> str:
    scheduled = str(signal["interview_scheduled_at"] or "").strip()
    if len(scheduled) >= 10 and scheduled[:10].count("-") == 2:
        return scheduled[:10]
    return fallback_day


EVENT_ICONS = {
    "applied":             "📨",
    "conversation":        "💬",
    "networking_call":     "🤝",
    "recruiter_outreach":  "📞",
    "screening_scheduled": "📅",
    "screening_complete":  "✅",
    "interview_scheduled": "🗓️",
    "interview_complete":  "🎙️",
    "offer_received":      "🎉",
    "offer_negotiating":   "🤝",
    "offer_accepted":      "✔️",
    "offer_declined":      "❌",
    "rejected":            "👎",
    "withdrawn":           "🚪",
    "follow_up_sent":      "📧",
    "note":                "📝",
}


def _formal_tracker_rows(rows):
    return [
        row
        for row in rows
        if str(row["status"]).lower() not in FORMAL_TRACKER_EXCLUDED_STATUSES
    ]


def _summary_metrics_for_rows(rows):
    counts = {status: 0 for status in db.STATUSES}
    for row in rows:
        status = str(row["status"]).lower()
        counts[status] = counts.get(status, 0) + 1
    total = len(rows)
    active = sum(counts.get(status, 0) for status in ("applied", "screening", "interviewing", "offer"))
    return {
        "total": total,
        "active": active,
        "interviewing": counts.get("interviewing", 0),
        "offers": counts.get("offer", 0),
        "accepted": counts.get("accepted", 0),
        "rejected": counts.get("rejected", 0),
    }


def _default_follow_up_date(status: str, base_date: Optional[date] = None) -> Optional[date]:
    base = base_date or date.today()
    offset = AUTO_FOLLOW_UP_DAYS.get(str(status).lower())
    if offset is None:
        return None
    return base + timedelta(days=offset)


def _follow_up_template_note(status: str) -> str:
    status_l = str(status).lower()
    if status_l == "applied":
        return "Follow up on application status"
    if status_l == "screening":
        return "Confirm next steps after screening"
    if status_l == "interviewing":
        return "Send thank-you / check next interview step"
    if status_l == "offer":
        return "Follow up on offer details / timeline"
    return ""


def _load_search_preferences() -> dict:
    try:
        return yaml.safe_load(settings.prefs_yaml.read_text(encoding="utf-8")) or {}
    except Exception:
        return {}


def _load_base_resume_settings(conn) -> dict:
    return {
        "name": db.get_setting(conn, "base_resume_name", default="Master Resume"),
        "source_url": db.get_setting(conn, "base_resume_source_url", default=""),
        "text": db.get_setting(conn, "base_resume_text", default=""),
        "notes": db.get_setting(conn, "base_resume_notes", default=""),
        "focus": db.get_setting(conn, "base_resume_keyword_focus", default=""),
        "ignore": db.get_setting(conn, "base_resume_keyword_ignore", default=""),
    }


def _normalize_resume_text(value: str) -> str:
    return " " + "".join(ch.lower() if ch.isalnum() or ch in {" ", "+", "/", "-", "."} else " " for ch in str(value or "")) + " "


def _resume_contains_phrase(resume_text: str, phrase: str) -> bool:
    normalized_resume = _normalize_resume_text(resume_text)
    normalized_phrase = _normalize_resume_text(phrase).strip()
    if not normalized_phrase:
        return True
    return f" {normalized_phrase} " in normalized_resume


def _parse_multiline_keywords(value: str) -> list[str]:
    return [line.strip() for line in str(value or "").splitlines() if line.strip()]


def _record_get(record, key: str, default=None):
    if record is None:
        return default
    if isinstance(record, dict):
        return record.get(key, default)
    try:
        return record[key]
    except Exception:
        return default


def _tailor_resume_keywords(app: dict, base_resume: dict, limit: int = 8) -> list[str]:
    resume_text = str(base_resume.get("text") or "")
    ignored = {phrase.lower() for phrase in _parse_multiline_keywords(base_resume.get("ignore", ""))}
    preferred = _parse_multiline_keywords(base_resume.get("focus", ""))
    matched_keywords = [item.strip() for item in str(_record_get(app, "matched_keywords") or "").split(",") if item.strip()]

    candidates: list[str] = []
    for keyword in preferred + matched_keywords:
        if not keyword:
            continue
        lowered = keyword.lower()
        if lowered in ignored:
            continue
        if _resume_contains_phrase(resume_text, keyword):
            continue
        if keyword not in candidates:
            candidates.append(keyword)
        if len(candidates) >= limit:
            break
    return candidates


def _tailored_resume_summary(app: dict, base_resume: dict) -> str:
    company = _record_get(app, "company") or "the company"
    role = _record_get(app, "role") or "the role"
    gaps = _tailor_resume_keywords(app, base_resume, limit=5)
    bullets = [
        f"Target the {role} role at {company}.",
        "Lead with the strongest directly relevant product, architecture, integration, and domain achievements from the base resume.",
    ]
    if gaps:
        bullets.append("Explicitly reinforce these JD-aligned keywords: " + ", ".join(gaps) + ".")
    if _record_get(app, "jd_summary"):
        bullets.append("Use the JD summary to tighten the opening summary and top three bullets.")
    return "\n".join(f"- {bullet}" for bullet in bullets)


def _default_resume_variant_name(app) -> str:
    company = str(_record_get(app, "company") or "company").strip().replace(" ", "_")
    role = str(_record_get(app, "role") or "role").strip().replace(" ", "_")
    return f"{company}_{role}_tailored"


def _build_resume_variant_content(app, base_resume: dict, tailored_keywords: str, tailored_notes: str, tailored_summary: str) -> str:
    keywords = [kw.strip() for kw in str(tailored_keywords or "").splitlines() if kw.strip()]
    keyword_block = "\n".join(f"- {keyword}" for keyword in keywords) or "- None captured"
    notes_block = tailored_notes.strip() or "No additional tailoring notes yet."
    base_text = str(base_resume.get("text") or "").strip() or "[Paste your resume body here]"
    return (
        f"# Resume Variant: {_record_get(app, 'company') or 'Company'} — {_record_get(app, 'role') or 'Role'}\n\n"
        f"Source Resume: {base_resume.get('name') or 'Base Resume'}\n\n"
        f"## Tailoring Brief\n{tailored_summary.strip() or 'No tailoring brief yet.'}\n\n"
        f"## Keywords To Emphasize\n{keyword_block}\n\n"
        f"## Tailoring Notes\n{notes_block}\n\n"
        f"## Resume Draft\n{base_text}\n"
    )


def _offer_work_type_defaults(app) -> tuple[str, str, float, float]:
    work_type = str(app["work_type"] or "fte")
    unit = str(app["compensation_unit"] or ("hourly" if "contract" in work_type else "salary"))
    hours_per_week = float(app["hours_per_week"] or 40.0)
    if app["weeks_per_year"]:
        weeks_per_year = float(app["weeks_per_year"])
    elif work_type == "1099_contract":
        weeks_per_year = 46.0
    elif unit == "hourly":
        weeks_per_year = 50.0
    else:
        weeks_per_year = 52.0
    return work_type, unit, hours_per_week, weeks_per_year


def _offer_comparison_rows(offer_apps) -> list[dict]:
    prefs = _load_search_preferences()
    rows = []
    for a in offer_apps:
        work_type, unit, hours_per_week, weeks_per_year = _offer_work_type_defaults(a)
        base = float(a["offer_base"] or a["salary_low"] or a["salary_high"] or 0)
        hourly_rate = float(a["hourly_rate"] or 0)
        comp = normalize_compensation(
            prefs,
            {
                "work_type": work_type,
                "compensation_unit": unit,
                "salary_min": base if unit == "salary" and base else None,
                "salary_max": base if unit == "salary" and base else None,
                "hourly_rate": hourly_rate or None,
                "hours_per_week": hours_per_week if unit == "hourly" else None,
                "weeks_per_year": weeks_per_year if unit == "hourly" else None,
                "salary_text": "",
                "description": a["offer_notes"] or "",
            },
        )
        normalized = float(comp["normalized_compensation_usd"] or 0)
        bonus_pct = float(a["offer_bonus_pct"] or 0)
        bonus_cash = normalized * (bonus_pct / 100.0) if normalized and bonus_pct else 0.0
        signing = float(a["offer_signing"] or 0)
        first_year = normalized + bonus_cash + signing if normalized else signing
        rows.append(
            {
                "Company": a["company"],
                "Role": a["role"],
                "Work Type": WORK_TYPE_LABELS.get(work_type, work_type.replace("_", " ").title()),
                "Comp Unit": unit.title(),
                "Base/Rate": f"${base:,.0f}" if unit == "salary" and base else (f"${hourly_rate:,.2f}/hr" if hourly_rate else "—"),
                "Normalized Annual ($)": normalized,
                "Bonus Cash ($)": bonus_cash,
                "Signing ($)": signing,
                "First-Year Cash ($)": first_year,
                "PTO Days": float(a["offer_pto_days"] or 0),
                "401k": a["offer_k401_match"] or "—",
                "Equity": a["offer_equity"] or "—",
                "Remote Policy": a["offer_remote_policy"] or "—",
                "Start Date": a["offer_start_date"] or "—",
                "Expires": a["offer_expiry_date"] or "—",
                "Notes": a["offer_notes"] or "",
            }
        )
    return rows


def _offer_comparison_markdown(offer_apps) -> str:
    rows = _offer_comparison_rows(offer_apps)
    if not rows:
        return "No comparable offers available."
    lines = ["# Offer Comparison", ""]
    for row in rows:
        lines.extend(
            [
                f"## {row['Company']} — {row['Role']}",
                f"- Work type: {row['Work Type']}",
                f"- Base / rate: {row['Base/Rate']}",
                f"- Normalized annual: ${float(row['Normalized Annual ($)'] or 0):,.0f}",
                f"- First-year cash: ${float(row['First-Year Cash ($)'] or 0):,.0f}",
                f"- Remote policy: {row['Remote Policy']}",
                f"- Equity: {row['Equity']}",
                f"- PTO days: {int(float(row['PTO Days'] or 0)) if row['PTO Days'] else '—'}",
                f"- Notes: {row['Notes'] or '—'}",
                "",
            ]
        )
    return "\n".join(lines).strip()


def _negotiation_playbook_lines(app: dict, target_base: float, walkaway_base: float, market_low: float, market_high: float) -> list[str]:
    offer_base = float(_record_get(app, "offer_base") or 0)
    playbook: list[str] = []
    if offer_base and target_base:
        diff = target_base - offer_base
        pct = (diff / offer_base * 100) if offer_base else 0
        if diff > 0:
            playbook.append(f"Ask: `${target_base:,.0f}` which is `+${diff:,.0f}` (`+{pct:.1f}%`) over the current offer.")
        elif diff == 0:
            playbook.append("Your current target matches the offer. Focus on equity, sign-on, remote flexibility, or PTO.")
        else:
            playbook.append("Your target is below the current offer. Revisit the worksheet before negotiating.")
    if walkaway_base and offer_base and offer_base < walkaway_base:
        playbook.append("Current offer is below your walk-away floor. Prepare a firm decline or a very direct counter.")
    if _record_get(app, "offer_expiry_date"):
        playbook.append(f"Offer deadline: `{_record_get(app, 'offer_expiry_date')}`. Counter early and ask for written confirmation on any extension.")
    if _record_get(app, "offer_remote_policy"):
        playbook.append(f"Remote policy is `{_record_get(app, 'offer_remote_policy')}`. If flexibility matters, make it part of the trade package.")
    if _record_get(app, "offer_equity"):
        playbook.append("Equity is part of the package. Ask for grant size, vesting schedule, refresh policy, and strike/valuation context.")
    if _record_get(app, "offer_signing"):
        playbook.append("There is already a sign-on component. If base is sticky, use sign-on or guaranteed bonus as an alternate lever.")
    if market_low and market_high and offer_base:
        if offer_base < market_low:
            playbook.append("Offer is below your market range. Use market data as your lead justification.")
        elif offer_base > market_high:
            playbook.append("Offer is above your market range. Negotiate carefully and focus on non-cash terms only if needed.")
        else:
            playbook.append("Offer is inside your market range. Anchor on role fit, scope, and execution leverage rather than comp alone.")
    return playbook


def _negotiation_counter_draft(app: dict, target_base: float, market_low: float, market_high: float) -> str:
    company = _record_get(app, "company") or "the company"
    role = _record_get(app, "role") or "the role"
    offer_base = float(_record_get(app, "offer_base") or 0)
    remote_policy = _record_get(app, "offer_remote_policy") or ""
    reasons: list[str] = []
    if market_low and target_base and target_base >= market_low:
        reasons.append("the market range for comparable roles")
    if remote_policy:
        reasons.append(f"the `{remote_policy}` working model")
    if _record_get(app, "offer_equity"):
        reasons.append("the equity structure")
    if not reasons:
        reasons.append("the scope and impact of the role")
    reason_text = ", ".join(reasons[:-1]) + (f", and {reasons[-1]}" if len(reasons) > 1 else reasons[0])

    if offer_base and target_base and target_base > offer_base:
        ask_line = f"Based on the conversations so far, I’d like to explore a base salary of ${target_base:,.0f}."
    elif target_base:
        ask_line = f"I’d like to target a base salary around ${target_base:,.0f}."
    else:
        ask_line = "I’d like to discuss the compensation structure in a bit more detail."

    return (
        f"Hi [Recruiter Name],\n\n"
        f"Thank you again for the offer for the {role} role at {company}. I’m excited about the opportunity and the team.\n\n"
        f"{ask_line} After reviewing the package against {reason_text}, I believe that level would better reflect the opportunity.\n\n"
        f"If base is constrained, I’d also be open to discussing other levers such as sign-on, equity, or flexibility in the overall package.\n\n"
        f"Thanks again,\n"
        f"[Your Name]"
    )


def _snooze_follow_up(conn, app_id: int, days: int) -> None:
    app = db.get_application(conn, app_id)
    if not app:
        return
    current_due = app["follow_up_date"][:10] if app["follow_up_date"] else date.today().isoformat()
    try:
        start = date.fromisoformat(current_due)
    except Exception:
        start = date.today()
    new_due = start + timedelta(days=days)
    db.update_application(conn, app_id, follow_up_date=new_due.isoformat())


def _mark_follow_up_sent(conn, app_id: int) -> None:
    app = db.get_application(conn, app_id)
    if not app:
        return
    today_iso = date.today().isoformat()
    next_due = _default_follow_up_date(app["status"], date.today())
    db.add_event(conn, app_id, "follow_up_sent", today_iso, title=f"Followed up with {app['company']}")
    db.update_application(
        conn,
        app_id,
        follow_up_date=next_due.isoformat() if next_due else None,
        follow_up_notes=app["follow_up_notes"] or _follow_up_template_note(app["status"]),
    )


def _clear_jd_change_review(conn, app_id: int) -> None:
    db.update_application(
        conn,
        app_id,
        jd_needs_review=0,
        jd_change_summary=None,
    )


# ── LinkedIn CSV import ─────────────────────────────────────────────────────────

_LI_COLUMN_MAP = {
    # LinkedIn "Applied Jobs" export column names → our field names
    "Company Name":   "company",
    "Company":        "company",
    "Job Title":      "role",
    "Title":          "role",
    "Position":       "role",
    "Job URL":        "job_url",
    "URL":            "job_url",
    "Applied At":     "date_applied",
    "Date Applied":   "date_applied",
    "Application Date": "date_applied",
    "Status":         "li_status",
}


def _render_linkedin_import(conn) -> None:
    with st.expander("📥 Import from LinkedIn CSV", expanded=False):
        st.caption(
            "Export your Applied Jobs from LinkedIn: go to **My Jobs → Applied** → "
            "click the export/download icon. Upload the CSV here."
        )
        uploaded = st.file_uploader("Choose LinkedIn CSV", type="csv",
                                     key="li_import_uploader", label_visibility="collapsed")
        if not uploaded:
            return

        try:
            li_df = pd.read_csv(uploaded, dtype=str).fillna("")
        except Exception as e:
            st.error(f"Could not read CSV: {e}")
            return

        # Map column names
        col_rename = {}
        for orig, mapped in _LI_COLUMN_MAP.items():
            if orig in li_df.columns:
                col_rename[orig] = mapped
        li_df = li_df.rename(columns=col_rename)

        if "company" not in li_df.columns or "role" not in li_df.columns:
            st.error(
                "Could not find Company and Job Title columns. "
                f"Found columns: {list(li_df.columns)}"
            )
            return

        # Deduplicate against existing applications
        existing = {
            (r["company"].lower(), r["role"].lower())
            for r in conn.execute("SELECT company, role FROM applications").fetchall()
        }

        new_rows = []
        for _, row in li_df.iterrows():
            co = str(row.get("company") or "").strip()
            ro = str(row.get("role") or "").strip()
            if co and ro and (co.lower(), ro.lower()) not in existing:
                new_rows.append(row)

        if not new_rows:
            st.info("All entries in this CSV are already in your tracker.")
            return

        preview_df = pd.DataFrame(new_rows)[
            [c for c in ["company", "role", "date_applied", "job_url", "li_status"]
             if c in pd.DataFrame(new_rows).columns]
        ]
        st.markdown(f"**{len(new_rows)} new application(s) to import:**")
        st.dataframe(preview_df, hide_index=True, use_container_width=True)

        if st.button(f"Import {len(new_rows)} application(s)", type="primary",
                     key="li_import_confirm"):
            imported = 0
            for row in new_rows:
                co = str(row.get("company") or "").strip()
                ro = str(row.get("role") or "").strip()
                url = str(row.get("job_url") or "").strip() or None
                raw_date = str(row.get("date_applied") or "").strip()

                # Best-effort date parse — strip any time component first
                app_date = None
                from datetime import datetime as _dt
                _d = raw_date.split("T")[0].strip() if "T" in raw_date else raw_date.strip()
                for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%B %d, %Y", "%b %d, %Y"):
                    try:
                        app_date = _dt.strptime(_d, fmt).date().isoformat()
                        break
                    except Exception:
                        continue

                app_id = db.add_application(
                    conn,
                    company      = co,
                    role         = ro,
                    job_url      = url,
                    source       = "linkedin_import",
                    status       = "applied",
                    entry_type   = "application",
                    date_applied = app_date,
                )
                if app_date:
                    db.add_event(conn, app_id, "applied", app_date,
                                 title=f"Applied to {co} (LinkedIn import)")
                imported += 1

            st.success(f"Imported {imported} application(s) from LinkedIn.")
            st.rerun()


def _email_import_value(row: dict, keys: list[str]) -> str:
    for key in keys:
        value = row.get(key)
        if value:
            return str(value).strip()
    return ""


def _import_email_signals(conn, uploaded_file) -> int:
    uploaded_file.seek(0)
    raw_text = uploaded_file.read().decode("utf-8", errors="ignore")
    reader = csv.DictReader(raw_text.splitlines())
    companies = [str(r["company"]) for r in db.get_applications(conn)] + [str(r["name"]) for r in db.get_all_company_profiles(conn)]
    imported = 0
    for row in reader:
        signal = classify_email_signal(
            message_id=_email_import_value(row, ["message_id", "Message-ID", "Message Id", "id"]),
            thread_id=_email_import_value(row, ["thread_id", "Thread-ID", "threadId"]),
            sender=_email_import_value(row, ["from", "From", "sender", "Sender"]),
            subject=_email_import_value(row, ["subject", "Subject"]),
            body=_email_import_value(row, ["body", "Body", "snippet", "Snippet"]),
            received_at=_email_import_value(row, ["date", "Date", "received_at", "Received At"]),
            known_companies=companies,
        )
        if not signal:
            continue
        linked = db.find_best_application_match(conn, signal.get("company"), signal.get("role"))
        signal_status = "new"
        notes = signal.get("notes")
        if linked:
            if signal["signal_type"] == "interview_request":
                existing_interview = db.find_matching_interview(
                    conn,
                    linked["id"],
                    scheduled_at=signal.get("interview_scheduled_at") or None,
                    interviewer_names=signal.get("interviewer_names") or None,
                    location=signal.get("interview_location") or None,
                )
                if existing_interview:
                    signal_status = "resolved"
                    if signal.get("interview_change_type") == "cancelled":
                        notes = "Matched existing interview for cancellation review."
                    else:
                        notes = "Matched existing interview."
            else:
                signal_status, auto_note = signal_resolution_for_existing_application(signal["signal_type"], linked["status"])
                notes = auto_note or notes
        db.upsert_email_signal(
            conn,
            **signal,
            application_id=linked["id"] if linked else None,
            signal_status=signal_status,
            notes=notes,
        )
        imported += 1
    return imported


def _gmail_sync_config(conn) -> dict[str, str]:
    return {
        "address": db.get_setting(conn, "gmail_address", default=settings.gmail_address).strip(),
        "app_password": db.get_setting(conn, "gmail_app_password", default=settings.gmail_app_password).strip(),
        "imap_host": db.get_setting(conn, "gmail_imap_host", default=settings.gmail_imap_host).strip() or "imap.gmail.com",
    }


def _render_email_signal_import(conn) -> None:
    with st.expander("📬 Import Gmail email signals", expanded=False):
        gmail_cfg = _gmail_sync_config(conn)
        if gmail_cfg["address"] and gmail_cfg["app_password"]:
            st.caption(f"Live Gmail sync is enabled for `{gmail_cfg['address']}`.")
            c1, c2, c3 = st.columns(3)
            sync_days = c1.number_input("Recent days", min_value=1, max_value=90, value=14, step=1, key="gmail_sync_days")
            sync_limit = c2.number_input("Max messages", min_value=10, max_value=500, value=100, step=10, key="gmail_sync_limit")
            if c3.button("Sync Gmail Inbox", key="gmail_sync_btn", use_container_width=True):
                try:
                    stats = sync_gmail_email_signals(
                        conn,
                        days=int(sync_days),
                        max_messages=int(sync_limit),
                        address=gmail_cfg["address"],
                        app_password=gmail_cfg["app_password"],
                        imap_host=gmail_cfg["imap_host"],
                    )
                    st.success(
                        f"Scanned {stats['scanned']} message(s), classified {stats['classified']}, "
                        f"stored {stats['stored']} signal(s) | new {stats.get('new', 0)} | auto-resolved {stats.get('resolved', 0)}."
                    )
                    st.rerun()
                except Exception as exc:
                    st.error(f"Live Gmail sync failed: {exc}")
        else:
            st.caption(
                "Live Gmail sync is disabled. Add Gmail credentials in Search Settings → App Settings "
                "or set `JOBSEARCH_GMAIL_ADDRESS` and `JOBSEARCH_GMAIL_APP_PASSWORD` in the environment."
            )

        st.divider()
        st.caption(
            "Upload a CSV export of Gmail messages with columns like From, Subject, Date, Snippet/Body. "
            "The tracker will detect missed applications, rejection emails, and interview requests."
        )
        uploaded = st.file_uploader(
            "Choose Gmail message CSV",
            type="csv",
            key="gmail_signal_uploader",
            label_visibility="collapsed",
        )
        if not uploaded:
            return
        try:
            imported = _import_email_signals(conn, uploaded)
        except Exception as exc:
            st.error(f"Could not import Gmail signals: {exc}")
            return
        st.success(f"Imported {imported} email signal(s). Review them below.")
        st.rerun()

        st.divider()


def _apply_email_signal_action(conn, signal, action: str) -> None:
    signal_id = signal["id"]
    app = db.get_application(conn, signal["application_id"]) if signal["application_id"] else None
    received_day = (signal["received_at"] or date.today().isoformat())[:10]

    if action == "ignore":
        db.update_email_signal(conn, signal_id, signal_status="ignored")
        return
    if action == "resolve":
        db.update_email_signal(conn, signal_id, signal_status="resolved")
        return
    if action == "add_to_tracker":
        app_id = db.add_application(
            conn,
            company=signal["company"] or "Unknown Company",
            role=signal["role"] or signal["subject"],
            source="gmail_signal",
            status="applied",
            entry_type="application",
            date_applied=received_day,
            notes=signal["subject"],
        )
        db.add_event(conn, app_id, "applied", received_day, title=f"Detected from Gmail: {signal['subject']}")
        db.update_email_signal(conn, signal_id, signal_status="resolved", application_id=app_id)
        return
    if not app:
        db.update_email_signal(conn, signal_id, notes="No linked application found", signal_status="new")
        return
    if action == "mark_rejected":
        db.update_application(conn, app["id"], status="rejected", date_closed=received_day)
        db.add_event(conn, app["id"], "rejected", received_day, title="Gmail-detected rejection", notes=signal["subject"])
        db.update_email_signal(conn, signal_id, signal_status="resolved")
        return
    if action == "mark_interviewing":
        if app["status"] not in ("interviewing", "offer", "accepted", "rejected", "withdrawn"):
            db.update_application(conn, app["id"], status="interviewing")
        db.add_event(conn, app["id"], "conversation", received_day, title="Gmail-detected interview request", notes=signal["subject"])
        db.update_email_signal(conn, signal_id, signal_status="resolved")
        return
    if action == "cancel_interview":
        interview = db.find_matching_interview(
            conn,
            app["id"],
            scheduled_at=signal["interview_scheduled_at"] or None,
            interviewer_names=signal["interviewer_names"] or None,
            location=signal["interview_location"] or None,
        ) or db.find_reschedulable_interview(
            conn,
            app["id"],
            scheduled_at=signal["interview_scheduled_at"] or None,
            interviewer_names=signal["interviewer_names"] or None,
            location=signal["interview_location"] or None,
        )
        if not interview:
            db.update_email_signal(conn, signal_id, notes="No matching pending interview found", signal_status="new")
            return
        db.update_interview(conn, interview["id"], outcome="cancelled")
        db.add_event(
            conn,
            app["id"],
            "note",
            received_day,
            title="Gmail-detected interview cancellation",
            notes=signal["subject"],
        )
        db.update_email_signal(conn, signal_id, signal_status="resolved", notes="Cancelled matching interview.")
        return
    if action == "create_interview":
        if app["status"] not in ("interviewing", "offer", "accepted", "rejected", "withdrawn"):
            db.update_application(conn, app["id"], status="interviewing")
        existing = db.find_matching_interview(
            conn,
            app["id"],
            scheduled_at=signal["interview_scheduled_at"] or None,
            interviewer_names=signal["interviewer_names"] or None,
            location=signal["interview_location"] or None,
        )
        if existing:
            db.update_email_signal(conn, signal_id, signal_status="resolved", notes="Matched existing interview.")
            return
        rescheduled = db.find_reschedulable_interview(
            conn,
            app["id"],
            scheduled_at=signal["interview_scheduled_at"] or None,
            interviewer_names=signal["interviewer_names"] or None,
            location=signal["interview_location"] or None,
        )
        if rescheduled:
            event_day = _event_day_from_signal(signal, received_day)
            update_payload = {
                "scheduled_at": signal["interview_scheduled_at"] or rescheduled["scheduled_at"],
                "duration_mins": signal["interview_duration_mins"] or rescheduled["duration_mins"],
                "interviewer_names": signal["interviewer_names"] or rescheduled["interviewer_names"],
                "location": signal["interview_location"] or rescheduled["location"],
                "prep_notes": signal["subject"] or rescheduled["prep_notes"],
                "interview_type": infer_interview_type(
                    signal["subject"],
                    signal["raw_excerpt"],
                    signal["sender"],
                    signal["interview_location"],
                ),
                "outcome": "pending",
            }
            db.update_interview(conn, rescheduled["id"], **update_payload)
            db.add_event(
                conn,
                app["id"],
                "interview_scheduled",
                event_day,
                title="Gmail-detected interview rescheduled",
                notes=signal["subject"],
            )
            db.update_email_signal(conn, signal_id, signal_status="resolved", notes="Updated existing interview with rescheduled details.")
            return
        db.add_interview(
            conn,
            app["id"],
            round_number=max((iv["round_number"] or 0 for iv in db.get_interviews(conn, app["id"])), default=0) + 1,
            interview_type=infer_interview_type(
                signal["subject"],
                signal["raw_excerpt"],
                signal["sender"],
                signal["interview_location"],
            ),
            format="mixed",
            scheduled_at=signal["interview_scheduled_at"] or None,
            duration_mins=signal["interview_duration_mins"] or None,
            interviewer_names=signal["interviewer_names"] or None,
            location=signal["interview_location"] or None,
            prep_notes=signal["subject"],
        )
        event_day = _event_day_from_signal(signal, received_day)
        db.add_event(
            conn,
            app["id"],
            "interview_scheduled",
            event_day,
            title="Gmail-detected interview scheduled",
            notes=signal["subject"],
        )
        db.update_email_signal(conn, signal_id, signal_status="resolved")


def _auto_resolve_existing_email_signals(conn) -> None:
    dirty = False
    for signal in db.get_email_signals(conn, signal_status="new"):
        linked_status = signal["linked_status"]
        if not linked_status:
            continue
        if signal["signal_type"] == "interview_request" and signal["application_id"]:
            existing_interview = db.find_matching_interview(
                conn,
                signal["application_id"],
                scheduled_at=signal["interview_scheduled_at"] or None,
                interviewer_names=signal["interviewer_names"] or None,
                location=signal["interview_location"] or None,
            )
            if existing_interview:
                if str(signal["interview_change_type"] or "scheduled") == "cancelled" and str(existing_interview["outcome"] or "").lower() != "cancelled":
                    continue
                db.update_email_signal(
                    conn,
                    signal["id"],
                    signal_status="resolved",
                    notes="Matched existing interview.",
                )
                dirty = True
            continue
        new_status, note = signal_resolution_for_existing_application(signal["signal_type"], linked_status)
        if new_status != "new":
            db.update_email_signal(conn, signal["id"], signal_status=new_status, notes=note or signal["notes"])
            dirty = True
    return dirty


def _render_email_signal_banner(conn) -> None:
    _auto_resolve_existing_email_signals(conn)
    signals = db.get_email_signals(conn, signal_status="new")
    if signals:
        counts = {}
        for signal in signals:
            counts[signal["signal_type"]] = counts.get(signal["signal_type"], 0) + 1
        summary = ", ".join(f"{count} {kind.replace('_', ' ')}" for kind, count in counts.items())
        st.caption(f"**Inbox signals** — {summary}")
        for signal in signals[:10]:
            company = signal["company"] or signal["linked_company"] or "Unknown company"
            role = signal["role"] or signal["linked_role"] or "Unknown role"
            with st.container(border=True):
                st.markdown(
                    f"**{signal['signal_type'].replace('_', ' ').title()}** — **{company}** — {role}  \n"
                    f"{signal['subject']}  \n"
                    f"From: {signal['sender'] or 'Unknown'} | Received: {_fmt_dt(signal['received_at'])}"
                )
                if signal["signal_type"] == "interview_request":
                    details = []
                    change_type = str(signal["interview_change_type"] or "scheduled").title()
                    details.append(f"Change: {change_type}")
                    if signal["interview_scheduled_at"]:
                        details.append(f"When: {_fmt_dt(signal['interview_scheduled_at'])}")
                    if signal["interviewer_names"]:
                        details.append(f"With: {signal['interviewer_names']}")
                    if signal["interview_duration_mins"]:
                        details.append(f"Duration: {signal['interview_duration_mins']} mins")
                    if signal["interview_location"]:
                        details.append(f"Link/Location: {signal['interview_location']}")
                    if details:
                        st.caption(" | ".join(details))
                c1, c2, c3 = st.columns(3)
                if signal["signal_type"] == "new_application":
                    if c1.button("Add to tracker", key=f"email_add_{signal['id']}"):
                        _apply_email_signal_action(conn, signal, "add_to_tracker")
                        st.rerun()
                elif signal["signal_type"] == "rejection":
                    if c1.button("Mark rejected", key=f"email_reject_{signal['id']}"):
                        _apply_email_signal_action(conn, signal, "mark_rejected")
                        st.rerun()
                elif signal["signal_type"] == "interview_request":
                    change_type = str(signal["interview_change_type"] or "scheduled")
                    if change_type == "cancelled" and signal["application_id"]:
                        if c1.button("Cancel interview", key=f"email_interview_cancel_{signal['id']}"):
                            _apply_email_signal_action(conn, signal, "cancel_interview")
                            st.rerun()
                    elif signal["application_id"] and (
                        signal["interview_scheduled_at"] or signal["interviewer_names"] or signal["interview_location"]
                    ):
                        button_label = "Update interview" if change_type == "rescheduled" else "Create interview"
                        if c1.button(button_label, key=f"email_interview_create_{signal['id']}"):
                            _apply_email_signal_action(conn, signal, "create_interview")
                            st.rerun()
                    else:
                        if c1.button("Mark interviewing", key=f"email_interview_{signal['id']}"):
                            _apply_email_signal_action(conn, signal, "mark_interviewing")
                            st.rerun()
                if c2.button("Resolve", key=f"email_resolve_{signal['id']}"):
                    _apply_email_signal_action(conn, signal, "resolve")
                    st.rerun()
                if c3.button("Ignore", key=f"email_ignore_{signal['id']}"):
                    _apply_email_signal_action(conn, signal, "ignore")
                    st.rerun()
    else:
        st.caption("No pending Gmail inbox signals.")
    with st.expander("Recent Gmail Signal Activity", expanded=False):
        audit_rows = db.get_email_signals(conn)[:15]
        if not audit_rows:
            st.caption("No Gmail signal history yet.")
        else:
            audit_df = pd.DataFrame(
                [
                    {
                        "Received": _fmt_dt(row["received_at"]),
                        "Type": str(row["signal_type"]).replace("_", " ").title(),
                        "Company": row["company"] or row["linked_company"] or "Unknown",
                        "Role": row["role"] or row["linked_role"] or "Unknown",
                        "Status": row["signal_status"],
                        "Notes": row["notes"] or "",
                    }
                    for row in audit_rows
                ]
            )
            st.dataframe(audit_df, hide_index=True, use_container_width=True)


# ── Main entry point ───────────────────────────────────────────────────────────

def render_tracker(conn) -> None:
    db.init_db(conn)
    st.title("My Applications")

    # Seed from CSV on first load
    csv_path = db._BASE_DIR / "results" / "ApplicationTracker.csv"  # type: ignore[attr-defined]
    if csv_path.exists() and not st.session_state.get("tracker_csv_seeded"):
        n = db.migrate_from_csv(conn, csv_path)
        if n:
            st.toast(f"Imported {n} applications from ApplicationTracker.csv", icon="📂")
        st.session_state["tracker_csv_seeded"] = True

    st.title("Application Tracker")
    st.markdown("<p class='js-subtitle'>Manage and track your active job applications.</p>", unsafe_allow_html=True)
    st.markdown("<div style='margin-bottom: 2rem;'></div>", unsafe_allow_html=True)

    _render_followup_banner(conn)
    _render_summary_bar(conn)
    _render_linkedin_import(conn)
    _render_email_signal_import(conn)
    _render_jd_change_banner(conn)
    _render_email_signal_banner(conn)

    offer_apps = db.get_applications_with_offers(conn)
    if len(offer_apps) >= 2:
        with st.expander(f"⚖️ Compare {len(offer_apps)} Offers", expanded=False):
            _render_offer_comparison(offer_apps)

    st.divider()

    # ── Filters + Add button ──────────────────────────────────────────────────
    fc1, fc2, fc3, fc4, fc5, fc6 = st.columns([2, 2, 2, 2, 1, 1])
    with fc1:
        status_opts = ["All statuses"] + db.STATUSES
        sel_status  = st.selectbox("Status", status_opts, key="tracker_status_filter",
                                   label_visibility="collapsed")
    with fc2:
        type_opts  = ["All types", "Applications", "Opportunities", "Job Fairs"]
        sel_type   = st.selectbox("Type", type_opts, key="tracker_type_filter",
                                  label_visibility="collapsed")
    with fc3:
        sel_ready = st.multiselect("Readiness", ["READY", "DRAFT", "BLOCKED"], key="tracker_ready_filter", placeholder="Any readiness")
    with fc4:
        search = st.text_input("Search", placeholder="company or role…",
                               key="tracker_search", label_visibility="collapsed")
    with fc5:
        if st.button("➕ Add", key="tracker_add_btn", use_container_width=True):
            toggled = not st.session_state.get("tracker_show_add_form", False)
            st.session_state["tracker_show_add_form"] = toggled
            if toggled:
                st.session_state["tracker_selected_id"] = None
    with fc6:
        bulk_mode = st.toggle("Bulk", key="tracker_bulk_mode",
                               help="Switch to multi-select mode for bulk status changes or deletes")
        if not bulk_mode:
            st.session_state["tracker_bulk_selected_ids"] = []

    if st.session_state.get("tracker_show_add_form"):
        _render_add_form(conn)
        st.divider()

    # ── Load + filter ─────────────────────────────────────────────────────────
    _entry_filter = {
        "Applications": "application",
        "Opportunities": "opportunity",
        "Job Fairs": "job_fair",
    }.get(sel_type)
    apps = db.get_applications(
        conn,
        status=None if sel_status == "All statuses" else sel_status,
        entry_type=_entry_filter,
    )
    # Filter out scraper-only 'considering' matches from the formal tracker.
    apps = _formal_tracker_rows(apps)
    
    # ── Readiness Evaluation (Batch) ──────────────────────────────────────────
    readiness_service = ReadinessService(conn)
    app_ids = [a['id'] for a in apps]
    readiness_map = readiness_service.evaluate_batch(app_ids)

    # ── Readiness Summary Metrics ─────────────────────────────────────────────
    r_counts = {"ready": 0, "draft": 0, "blocked": 0}
    for r_state in readiness_map.values():
        r_counts[r_state.status] += 1
    
    rm1, rm2, rf3 = st.columns([1, 1, 4])
    rm1.metric("Ready Packages", r_counts["ready"])
    rm2.metric("Draft/Blocked", r_counts["draft"] + r_counts["blocked"])

    # Filter by readiness if selected
    if sel_ready:
        apps = [a for a in apps if readiness_map.get(a['id']).status.upper() in sel_ready]

    if search:
        q = search.lower()
        apps = [a for a in apps if q in a["company"].lower() or q in a["role"].lower()]

    if not apps:
        st.info("No applications match the current filter.")
        return

    # ── Build table DataFrame ─────────────────────────────────────────────────
    rows = []
    # Sort weight: Ready=0, Draft=1, Blocked=2
    status_weights = {'ready': 0, 'draft': 1, 'blocked': 2}
    
    # Sort apps by readiness weight first, then by original order (updated_at)
    apps_sorted = sorted(apps, key=lambda a: status_weights.get(readiness_map.get(a['id']).status, 3))

    for a in apps_sorted:
        fu_date   = a["follow_up_date"] or ""
        days_left = _days_until(fu_date) if fu_date else None
        if fu_date and days_left is not None and days_left < 0:
            fu_label = f"⚠ {fu_date}"
        else:
            fu_label = fu_date
        entry = a["entry_type"] if a["entry_type"] else "application"
        type_label = {"application": "📋 Application",
                      "opportunity": "🤝 Opportunity",
                      "job_fair":    "🎪 Job Fair"}.get(entry, "📋 Application")
        
        # Readiness Badge
        r_state = readiness_map.get(a['id'])
        ready_badge = render_readiness_badge(r_state)

        rows.append({
            "_id":       a["id"],
            "Ready":     ready_badge,
            "Type":      type_label,
            "Company":   a["company"],
            "Role":      a["role"],
            "Status":    a["status"].title(),
            "Applied":   a["date_applied"] or "",
            "Fit":       "⭐" * int(a["fit_stars"]) if a["fit_stars"] else "—",
            "JD Updated": "Changed" if a["jd_needs_review"] else "",
            "Follow-up": fu_label,
        })
    df_tbl = pd.DataFrame(rows)

    # Export CSV (always visible, reflects current filter)
    csv_bytes = df_tbl.drop(columns=["_id", "Ready"]).to_csv(index=False).encode("utf-8")
    st.download_button(
        label="⬇️ Export CSV",
        data=csv_bytes,
        file_name=f"applications_{date.today().isoformat()}.csv",
        mime="text/csv",
        key="export_csv_btn",
    )

    _col_config = {
        "Ready":     st.column_config.TextColumn("Ready",     width="small", help="Submission Readiness Status"),
        "Type":      st.column_config.TextColumn("Type",      width="small"),
        "Company":   st.column_config.TextColumn("Company",   width="medium"),
        "Role":      st.column_config.TextColumn("Role",      width="large"),
        "Status":    st.column_config.TextColumn("Status",    width="small"),
        "Applied":   st.column_config.TextColumn("Applied",   width="small"),
        "Fit":       st.column_config.TextColumn("Fit",       width="small"),
        "JD Updated": st.column_config.TextColumn("JD Updated", width="small"),
        "Follow-up": st.column_config.TextColumn("Follow-up", width="small"),
    }

    bulk_mode = st.session_state.get("tracker_bulk_mode", False)

    if bulk_mode:
        # ── Multi-row bulk select ─────────────────────────────────────────────
        event = st.dataframe(
            df_tbl.drop(columns=["_id"]),
            on_select="rerun",
            selection_mode="multi-row",
            hide_index=True,
            use_container_width=True,
            column_config=_col_config,
        )
        selected_ids = [int(df_tbl.iloc[i]["_id"]) for i in event.selection.rows]
        st.session_state["tracker_bulk_selected_ids"] = selected_ids

        if selected_ids:
            st.caption(f"{len(selected_ids)} row{'s' if len(selected_ids) != 1 else ''} selected")
            ba1, ba2, ba3, _ = st.columns([3, 2, 2, 3])
            new_status = ba1.selectbox("Change status to", db.STATUSES,
                                        key="bulk_status_pick", label_visibility="collapsed")
            if ba2.button(f"Update {len(selected_ids)}", type="primary", key="bulk_update_btn"):
                db.bulk_update_status(conn, selected_ids, new_status)
                st.toast(f"Updated {len(selected_ids)} application(s) to '{new_status}'")
                st.rerun()
            if ba3.button("🗑 Delete selected", key="bulk_delete_btn"):
                db.bulk_delete_applications(conn, selected_ids)
                st.session_state["tracker_bulk_selected_ids"] = []
                st.toast(f"Deleted {len(selected_ids)} application(s)", icon="🗑️")
                st.rerun()
    else:
        # ── Single-row click → detail ─────────────────────────────────────────
        event = st.dataframe(
            df_tbl.drop(columns=["_id"]),
            on_select="rerun",
            selection_mode="single-row",
            hide_index=True,
            use_container_width=True,
            column_config=_col_config,
        )
        if event.selection.rows:
            sel_id = int(df_tbl.iloc[event.selection.rows[0]]["_id"])
            st.session_state["tracker_selected_id"] = sel_id
            st.session_state["tracker_show_add_form"] = False

    # ── Inline detail panel (single-row mode only) ────────────────────────────
    sel_id = st.session_state.get("tracker_selected_id")
    if sel_id:
        st.divider()
        hdr1, hdr2 = st.columns([10, 1])
        hdr1.subheader("Application Detail", anchor=False)
        if hdr2.button("✕ Close", key="close_detail"):
            st.session_state["tracker_selected_id"] = None
            st.rerun()
        _render_detail(conn, sel_id)


# ── Follow-up banner ──────────────────────────────────────────────────────────

def _contact_action_links(app) -> str:
    """Return HTML links for email and LinkedIn from the first contact on an app row."""
    parts = []
    email = app["first_contact_email"] if "first_contact_email" in app.keys() else None
    linkedin = app["first_contact_linkedin"] if "first_contact_linkedin" in app.keys() else None
    if email:
        parts.append(f'<a href="mailto:{email}">✉️ Email</a>')
    if linkedin:
        parts.append(f'<a href="{linkedin}" target="_blank">🔗 LinkedIn</a>')
    return "  |  ".join(parts)


def _render_followup_banner(conn) -> None:
    overdue  = db.follow_up_due(conn)
    upcoming = db.follow_up_upcoming(conn, days=3)

    if not overdue and not upcoming:
        return

    if overdue:
        with st.container():
            st.error(f"**{len(overdue)} follow-up{'s' if len(overdue)>1 else ''} overdue**", icon="🔔")
            for app in overdue:
                days_ago = abs(_days_until(app["follow_up_date"]) or 0)
                label = f"**{app['company']}** — {app['role']}"
                note  = app["follow_up_notes"] or ""
                contacts = app["contact_summary"] or ""
                contact_links = _contact_action_links(app)
                col1, col2 = st.columns([5, 1])
                col1.markdown(
                    f"{label}  \n"
                    f"<span style='color:#f87171'>Due {app['follow_up_date']}"
                    f" ({days_ago} day{'s' if days_ago!=1 else ''} ago)</span>"
                    + (f"  \n👤 {contacts}" if contacts else "")
                    + (f"  \n{contact_links}" if contact_links else "")
                    + (f"  \n_{note}_" if note else ""),
                    unsafe_allow_html=True,
                )
                a1, a2, a3, a4 = col2.columns(4)
                if a1.button("View", key=f"fu_view_{app['id']}"):
                    st.session_state["tracker_selected_id"] = app["id"]
                    st.session_state["tracker_show_add_form"] = False
                    st.rerun()
                if a2.button("+3d", key=f"fu_snooze3_{app['id']}"):
                    _snooze_follow_up(conn, app["id"], 3)
                    st.rerun()
                if a3.button("+7d", key=f"fu_snooze7_{app['id']}"):
                    _snooze_follow_up(conn, app["id"], 7)
                    st.rerun()
                if a4.button("Sent", key=f"fu_sent_{app['id']}"):
                    _mark_follow_up_sent(conn, app["id"])
                    st.rerun()

    if upcoming:
        with st.expander(f"🗓 {len(upcoming)} follow-up{'s' if len(upcoming)>1 else ''} due in the next 3 days"):
            for app in upcoming:
                days_left = _days_until(app["follow_up_date"]) or 0
                contacts  = app["contact_summary"] or ""
                contact_links = _contact_action_links(app)
                c1, c2 = st.columns([5, 1])
                c1.markdown(
                    f"**{app['company']}** — {app['role']}  \n"
                    f"<span style='color:#fbbf24'>Due {app['follow_up_date']}"
                    f" (in {days_left} day{'s' if days_left!=1 else ''})</span>"
                    + (f"  \n👤 {contacts}" if contacts else "")
                    + (f"  \n{contact_links}" if contact_links else ""),
                    unsafe_allow_html=True,
                )
                u1, u2, u3 = c2.columns(3)
                if u1.button("View", key=f"fu_up_view_{app['id']}"):
                    st.session_state["tracker_selected_id"] = app["id"]
                    st.session_state["tracker_show_add_form"] = False
                    st.rerun()
                if u2.button("+3d", key=f"fu_up_snooze3_{app['id']}"):
                    _snooze_follow_up(conn, app["id"], 3)
                    st.rerun()
                if u3.button("Sent", key=f"fu_up_sent_{app['id']}"):
                    _mark_follow_up_sent(conn, app["id"])
                    st.rerun()


def _render_jd_change_banner(conn) -> None:
    changed_apps = db.get_jd_changed_applications(conn)
    if not changed_apps:
        return

    with st.expander(f"📝 {len(changed_apps)} active application(s) with JD changes", expanded=False):
        for app in changed_apps[:10]:
            c1, c2 = st.columns([5, 1])
            c1.markdown(
                f"**{app['company']}** — {app['role']}  \n"
                f"<span style='color:#f59e0b'>Changed {_fmt_dt(app['jd_last_changed_at'])}</span>"
                + (f"  \n_{app['jd_change_summary']}_" if app['jd_change_summary'] else ""),
                unsafe_allow_html=True,
            )
            b1, b2 = c2.columns(2)
            if b1.button("View", key=f"jdchg_view_{app['id']}"):
                st.session_state["tracker_selected_id"] = app["id"]
                st.session_state["tracker_show_add_form"] = False
                st.rerun()
            if b2.button("Reviewed", key=f"jdchg_done_{app['id']}"):
                _clear_jd_change_review(conn, app["id"])
                st.rerun()


# ── Summary bar ────────────────────────────────────────────────────────────────

def _render_summary_bar(conn) -> None:
    metrics = _summary_metrics_for_rows(_formal_tracker_rows(db.get_applications(conn)))

    with st.container(border=True):
        cols = st.columns(6)
        cards = [
            ("Total", metrics["total"], None),
            ("Active", metrics["active"], None),
            ("Interviewing", metrics["interviewing"], None),
            ("Offers", metrics["offers"], None),
            ("Accepted", metrics["accepted"], None),
            ("Rejected", metrics["rejected"], None),
        ]
        for col, (label, val, delta) in zip(cols, cards):
            col.metric(label, val, delta)

    # Upcoming interviews
    upcoming = db.upcoming_interviews(conn, limit=3)
    if upcoming:
        st.markdown("<div style='margin-top: 1rem;'></div>", unsafe_allow_html=True)
        st.subheader("Upcoming Interviews", anchor=False)
        for iv in upcoming:
            st.info(
                f"🗓 **{iv['company']}** — {iv['role']}  "
                f"| {iv['interview_type'] or 'interview'}  "
                f"| {_fmt_dt(iv['scheduled_at'])}"
            )




# ── Add application form ───────────────────────────────────────────────────────

def _render_add_form(conn) -> None:
    with st.form("add_app_form", clear_on_submit=True):
        # Entry type toggle
        ta1, ta2 = st.columns(2)
        entry_type = ta1.radio(
            "Type",
            ["Application", "Opportunity", "Job Fair"],
            horizontal=True,
            key="add_form_entry_type",
            help="Application = formal job posting.  "
                 "Opportunity = network contact / informal conversation.  "
                 "Job Fair = attended a job fair or recruiting event.",
        )
        is_opp      = entry_type == "Opportunity"
        is_job_fair = entry_type == "Job Fair"

        st.markdown(f"**New {entry_type}**")
        r1c1, r1c2 = st.columns(2)
        company  = r1c1.text_input("Company *")
        role     = r1c2.text_input(
            "Role / Description *",
            placeholder="e.g. Product Lead" if is_opp else "e.g. Senior Product Manager",
        )
        r2c1, r2c2, r2c3 = st.columns(3)
        # Status defaults: application→applied, opportunity/job_fair→exploring
        default_status_idx = (
            db.STATUSES.index("applied") if not is_opp and not is_job_fair
            else db.STATUSES.index("exploring")
        )
        status   = r2c1.selectbox(
            "Status", db.STATUSES, index=default_status_idx,
            help="Pipeline stage: exploring → applied → screening → interviewing → offer → accepted/rejected/withdrawn",
        )
        fit      = r2c2.selectbox(
            "Fit", ["—", "1", "2", "3", "4", "5"],
            help="Your personal excitement/fit score: 1 = low interest, 5 = dream role",
        )
        date_app = r2c3.date_input(
            "Event Date" if is_job_fair else ("Date of first contact" if is_opp else "Date Applied"),
            value=date.today(),
        )

        if not is_opp and not is_job_fair:
            job_url = st.text_input("Job URL")
        else:
            job_url = ""

        sal_low, sal_high = st.columns(2)
        s_low  = sal_low.number_input("Salary Low ($)", value=0, step=5000,
                                       help="Bottom of the posted salary range (0 = not posted)")
        s_high = sal_high.number_input("Salary High ($)", value=0, step=5000,
                                        help="Top of the posted salary range (0 = not posted)")
        referral   = st.text_input(
            "Referred by" if (is_opp or is_job_fair) else "Referral",
            placeholder="e.g. John Smith (former manager)" if is_opp else
                        "e.g. Hired by Design networking event" if is_job_fair else "",
        )
        jd_summary = st.text_area(
            "Event / Role Description" if is_job_fair else ("Notes on the role" if is_opp else "JD Summary"),
            height=80,
            placeholder="e.g. Denver Tech Job Fair — talked to Acme Corp, TechCo, StartupX" if is_job_fair else "",
        )
        notes = st.text_area("Notes", height=60)

        st.markdown("**Follow-up**")
        rf1, rf2 = st.columns(2)
        follow_up_date  = rf1.date_input("Follow-up date", value=None)
        follow_up_notes = rf2.text_input(
            "Who / what to say",
            placeholder="e.g. Follow up with John after call",
        )

        if not is_opp and not is_job_fair:
            st.markdown("**Documents**")
            resume_version     = st.text_input("Resume version", placeholder="e.g. PM resume v3 – fintech tailored")
            cover_letter_notes = st.text_input("Cover letter notes", placeholder="e.g. Tailored intro, emphasized API PM exp")
            da1, da2 = st.columns(2)
            resume_url       = da1.text_input("Resume URL", placeholder="e.g. link to Google Doc")
            cover_letter_url = da2.text_input("Cover Letter URL", placeholder="e.g. link to Google Doc")
        else:
            resume_version = cover_letter_notes = ""
            resume_url = cover_letter_url = ""

        if st.form_submit_button("Save", type="primary"):
            if not company.strip() or not role.strip():
                st.error("Company and Role are required.")
            else:
                auto_follow_up = follow_up_date or _default_follow_up_date(status, date_app)
                auto_follow_up_note = follow_up_notes.strip() or _follow_up_template_note(status)
                app_id = db.add_application(
                    conn,
                    company            = company.strip(),
                    role               = role.strip(),
                    job_url            = job_url.strip() or None,
                    source             = "manual",
                    entry_type         = "job_fair" if is_job_fair else ("opportunity" if is_opp else "application"),
                    status             = status,
                    fit_stars          = int(fit) if fit != "—" else None,
                    salary_low         = s_low  or None,
                    salary_high        = s_high or None,
                    salary_range       = f"${s_low:,}–${s_high:,}" if s_low and s_high else None,
                    referral           = referral.strip() or None,
                    jd_summary         = jd_summary.strip() or None,
                    notes              = notes.strip() or None,
                    date_applied       = date_app.isoformat(),
                    follow_up_date     = auto_follow_up.isoformat() if auto_follow_up else None,
                    follow_up_notes    = auto_follow_up_note or None,
                    resume_version     = resume_version.strip() or None,
                    cover_letter_notes = cover_letter_notes.strip() or None,
                    resume_url         = resume_url.strip() or None,
                    cover_letter_url   = cover_letter_url.strip() or None,
                )
                # First event based on type
                if is_job_fair:
                    first_event = "conversation"
                    first_title = f"Attended job fair — {company.strip()}"
                elif is_opp:
                    first_event = "conversation"
                    first_title = f"Initial conversation — {company.strip()}"
                else:
                    first_event = "applied"
                    first_title = f"Applied to {company.strip()}"
                db.add_event(conn, app_id, first_event, date_app.isoformat(), title=first_title)
                st.session_state["tracker_selected_id"]  = app_id
                st.session_state["tracker_show_add_form"] = False
                st.rerun()


# ── Application detail ─────────────────────────────────────────────────────────

def _render_detail(conn, app_id: int) -> None:
    app = db.get_application(conn, app_id)
    if not app:
        st.warning("Application not found.")
        st.session_state["tracker_selected_id"] = None
        return

    # Header
    color = db.STATUS_COLORS.get(app["status"], "#6b7280")
    st.markdown(
        f"### {app['company']}\n{app['role']}",
    )
    hc1, hc2, hc3, hc4 = st.columns(4)
    hc1.markdown(_status_badge(app["status"]), unsafe_allow_html=True)
    hc2.caption(f"Applied {app['date_applied'] or '—'}")
    hc3.caption(f"Fit {_stars(app['fit_stars'])}")
    hc4.caption(f"💰 {app['salary_range'] or '—'}")
    # Quick links row
    link_parts = []
    if app["job_url"]:
        link_parts.append(f"[Job Posting ↗]({app['job_url']})")
    if app["resume_url"]:
        link_parts.append(f"[Resume ↗]({app['resume_url']})")
    if app["cover_letter_url"]:
        link_parts.append(f"[Cover Letter ↗]({app['cover_letter_url']})")
    if link_parts:
        st.markdown("  |  ".join(link_parts))

    st.markdown("<div style='margin-bottom: 1rem;'></div>", unsafe_allow_html=True)
    render_quick_export(conn, app["id"])

    # Acquisition & Provenance
    with st.expander("🛠 Acquisition & Provenance", expanded=False):
        pc1, pc2 = st.columns(2)
        lane = app.get("source_lane") or "unknown"
        pc1.write(f"**Source Lane:** `{lane}`")
        pc1.write(f"**Source Origin:** `{app.get('source') or 'unknown'}`")
        
        gid = app.get("canonical_group_id")
        is_canon = app.get("is_canonical", 1)
        pc2.write(f"**Canonical:** {'✅ Yes' if is_canon else '🔗 Duplicate'}")
        if gid:
            pc2.write(f"**Group ID:** `{gid}`")
            
        if app.get("canonical_merge_rationale"):
            st.caption(f"Rationale: {app['canonical_merge_rationale']}")

    if app["job_description"]:
        with st.expander("📄 Full Job Description"):
            st.markdown(app["job_description"])
    if app["jd_needs_review"]:
        st.warning(
            f"JD changed on {_fmt_dt(app['jd_last_changed_at'])}. "
            + (app["jd_change_summary"] or ""),
            icon="📝",
        )
        if st.button("Mark JD change reviewed", key=f"jd_review_detail_{app['id']}"):
            _clear_jd_change_review(conn, app["id"])
            st.rerun()

    tab_tl, tab_iv, tab_co, tab_pr, tab_ng, tab_ed = st.tabs(
        ["Timeline", "Interviews", "Contacts", "Prep", "Negotiate", "Edit"]
    )

    with tab_tl:
        _render_timeline(conn, app)

    with tab_iv:
        _render_interviews(conn, app)

    with tab_co:
        _render_contacts(conn, app)

    with tab_pr:
        _render_prep_tab(conn, app)

    with tab_ng:
        _render_negotiate_tab(conn, app)

    with tab_ed:
        _render_edit_form(conn, app)

    # ── Company profile (below tabs) ──────────────────────────────────────────
    _render_inline_company_profile(conn, app["company"])


# ── Timeline tab ───────────────────────────────────────────────────────────────

def _render_timeline(conn, app) -> None:
    events = db.get_events(conn, app["id"])

    # Add event form
    with st.expander("➕ Add event", expanded=False):
        with st.form(f"add_event_{app['id']}"):
            ec1, ec2 = st.columns(2)
            etype    = ec1.selectbox("Type", db.EVENT_TYPES)
            edate    = ec2.date_input("Date", value=date.today())
            etitle   = st.text_input("Title (optional)")
            enotes   = st.text_area("Notes", height=70)
            if st.form_submit_button("Add", type="primary"):
                db.add_event(conn, app["id"], etype, edate.isoformat(),
                             title=etitle.strip() or None,
                             notes=enotes.strip() or None)
                # Auto-advance status
                _maybe_advance_status(conn, app, etype)
                st.rerun()

    st.divider()

    if not events:
        st.caption("No events yet.")
        return

    for ev in reversed(events):   # newest first
        icon  = EVENT_ICONS.get(ev["event_type"], "•")
        label = (ev["title"] or ev["event_type"].replace("_", " ").title())
        st.markdown(
            f'{icon} **{label}** <span style="color:#9ca3af;font-size:.8rem">'
            f'{_fmt_dt(ev["event_date"])}</span>',
            unsafe_allow_html=True,
        )
        if ev["notes"]:
            st.caption(ev["notes"])
        dc1, _ = st.columns([1, 8])
        if dc1.button("🗑", key=f"del_ev_{ev['id']}", help="Delete event"):
            db.delete_event(conn, ev["id"])
            st.rerun()
        st.markdown('<hr style="margin:4px 0;border-color:#374151">', unsafe_allow_html=True)


def _maybe_advance_status(conn, app, event_type: str) -> None:
    """Automatically advance application status based on the event logged."""
    mapping = {
        "applied":             "applied",
        "screening_scheduled": "screening",
        "screening_complete":  "screening",
        "interview_scheduled": "interviewing",
        "interview_complete":  "interviewing",
        "offer_received":      "offer",
        "offer_negotiating":   "offer",
        "offer_accepted":      "accepted",
        "offer_declined":      "withdrawn",
        "rejected":            "rejected",
        "withdrawn":           "withdrawn",
    }
    new_status = mapping.get(event_type)
    if new_status and new_status != app["status"]:
        db.update_application(conn, app["id"], status=new_status)


# ── Interviews tab ─────────────────────────────────────────────────────────────

def _render_interviews(conn, app) -> None:
    interviews = db.get_interviews(conn, app["id"])
    next_round = max((iv["round_number"] or 0 for iv in interviews), default=0) + 1

    with st.expander("➕ Schedule interview", expanded=False):
        with st.form(f"add_iv_{app['id']}"):
            ic1, ic2, ic3 = st.columns(3)
            round_n  = ic1.number_input("Round", value=next_round, min_value=1, step=1)
            iv_type  = ic2.selectbox("Type", db.INTERVIEW_TYPES)
            iv_fmt   = ic3.selectbox("Format", db.INTERVIEW_FORMATS)
            id1, id2 = st.columns(2)
            sched_d  = id1.date_input("Date", value=date.today())
            sched_t  = id2.time_input("Time")
            dur      = st.number_input("Duration (mins)", value=45, step=15)
            interviewers = st.text_input("Interviewers (comma-separated)")
            location = st.text_input("Location / Video link")
            prep     = st.text_area("Prep notes", height=70)
            if st.form_submit_button("Schedule", type="primary"):
                sched_dt = datetime.combine(sched_d, sched_t).isoformat()
                db.add_interview(
                    conn, app["id"],
                    round_number      = int(round_n),
                    interview_type    = iv_type,
                    format            = iv_fmt,
                    scheduled_at      = sched_dt,
                    duration_mins     = int(dur),
                    interviewer_names = interviewers.strip() or None,
                    location          = location.strip() or None,
                    prep_notes        = prep.strip() or None,
                )
                db.add_event(conn, app["id"], "interview_scheduled", sched_d.isoformat(),
                             title=f"Round {round_n} — {iv_type.replace('_',' ').title()} scheduled")
                if app["status"] not in ("interviewing", "offer", "accepted"):
                    db.update_application(conn, app["id"], status="interviewing")
                st.rerun()

    st.divider()

    if not interviews:
        st.caption("No interviews scheduled yet.")
        return

    for iv in interviews:
        outcome_color = {"pending": "#f59e0b", "passed": "#10b981", "failed": "#ef4444", "cancelled": "#6b7280"}.get(
            iv["outcome"] or "pending", "#6b7280"
        )
        st.markdown(
            f"**Round {iv['round_number'] or '?'}** — "
            f"{(iv['interview_type'] or '').replace('_',' ').title()}  "
            f"<span style='color:{outcome_color};font-weight:600'>{iv['outcome'] or 'pending'}</span>  "
            f"<span style='color:#9ca3af;font-size:.8rem'>{_fmt_dt(iv['scheduled_at'])}</span>",
            unsafe_allow_html=True,
        )
        if iv["interviewer_names"]:
            st.caption(f"With: {iv['interviewer_names']}")
        if iv["location"]:
            st.caption(f"📍 {iv['location']}")
        if iv["duration_mins"]:
            st.caption(f"Duration: {iv['duration_mins']} mins")

        # Quick outcome update
        oc1, oc2, _ = st.columns([2, 2, 4])
        new_outcome = oc1.selectbox(
            "Outcome", db.OUTCOME_OPTIONS,
            index=db.OUTCOME_OPTIONS.index(iv["outcome"] or "pending"),
            key=f"iv_out_{iv['id']}",
            label_visibility="collapsed",
        )
        if oc2.button("Update", key=f"iv_upd_{iv['id']}"):
            db.update_interview(conn, iv["id"], outcome=new_outcome)
            if new_outcome == "passed":
                db.add_event(conn, app["id"], "interview_complete",
                             date.today().isoformat(),
                             title=f"Round {iv['round_number']} passed")
            elif new_outcome == "failed":
                db.add_event(conn, app["id"], "interview_complete",
                             date.today().isoformat(),
                             title=f"Round {iv['round_number']} — did not advance")
            st.rerun()

        dc1, _ = st.columns([1, 8])
        if dc1.button("🗑", key=f"del_iv_{iv['id']}", help="Delete"):
            db.delete_interview(conn, iv["id"])
            st.rerun()

        with st.expander("Interview Debrief", expanded=False):
            with st.form(f"iv_debrief_{iv['id']}"):
                d1, d2, d3, d4 = st.columns(4)
                rapport = d1.slider("Rapport", 1, 5, int(iv["rapport_score"] or 3))
                clarity = d2.slider("Role Clarity", 1, 5, int(iv["role_clarity_score"] or 3))
                engaged = d3.slider("Interviewer Engaged", 1, 5, int(iv["interviewer_engaged_score"] or 3))
                confidence = d4.slider("Your Confidence", 1, 5, int(iv["confidence_score"] or 3))
                b1, b2, b3, b4 = st.columns(4)
                next_steps = b1.checkbox("Next steps discussed", value=bool(iv["next_steps_clear"]))
                timeline = b2.checkbox("Timeline mentioned", value=bool(iv["timeline_mentioned"]))
                comp = b3.checkbox("Comp discussed", value=bool(iv["compensation_discussed"]))
                availability = b4.checkbox("Availability discussed", value=bool(iv["availability_discussed"]))
                debrief_notes = st.text_area(
                    "Debrief Notes",
                    value=iv["debrief_notes"] or iv["outcome_notes"] or "",
                    height=90,
                    placeholder="Signals, concerns, what they emphasized, what to prepare next…",
                )
                if st.form_submit_button("Save Debrief", type="primary"):
                    db.update_interview(
                        conn,
                        iv["id"],
                        rapport_score=rapport,
                        role_clarity_score=clarity,
                        interviewer_engaged_score=engaged,
                        confidence_score=confidence,
                        next_steps_clear=1 if next_steps else 0,
                        timeline_mentioned=1 if timeline else 0,
                        compensation_discussed=1 if comp else 0,
                        availability_discussed=1 if availability else 0,
                        debrief_notes=debrief_notes.strip() or None,
                        outcome_notes=debrief_notes.strip() or None,
                    )
                    st.success("Debrief saved.")
                    st.rerun()
        st.markdown('<hr style="margin:6px 0;border-color:#374151">', unsafe_allow_html=True)


# ── Contacts tab ───────────────────────────────────────────────────────────────

def _render_contacts(conn, app) -> None:
    contacts = db.get_contacts(conn, app["id"])

    with st.expander("➕ Add contact", expanded=False):
        with st.form(f"add_co_{app['id']}"):
            cc1, cc2 = st.columns(2)
            cname    = cc1.text_input("Name *")
            ctitle   = cc2.text_input("Title")
            cc3, cc4 = st.columns(2)
            cemail   = cc3.text_input("Email")
            cphone   = cc4.text_input("Phone")
            cc5, cc6 = st.columns(2)
            crole    = cc5.selectbox("Role in Process", db.CONTACT_ROLES)
            clinkedin = cc6.text_input("LinkedIn URL")
            cnotes   = st.text_area("Notes", height=60)
            if st.form_submit_button("Add Contact", type="primary"):
                if not cname.strip():
                    st.error("Name is required.")
                else:
                    db.add_contact(
                        conn, app["id"],
                        name            = cname.strip(),
                        title           = ctitle.strip() or None,
                        email           = cemail.strip() or None,
                        phone           = cphone.strip() or None,
                        role_in_process = crole,
                        linkedin_url    = clinkedin.strip() or None,
                        notes           = cnotes.strip() or None,
                    )
                    st.rerun()

    st.divider()

    if not contacts:
        st.caption("No contacts yet.")
        return

    for co in contacts:
        role_color = {"recruiter": "#3b82f6", "hiring_manager": "#f59e0b",
                      "interviewer": "#8b5cf6", "referral": "#10b981"}.get(
            co["role_in_process"] or "", "#6b7280"
        )
        st.markdown(
            f"**{co['name']}**"
            + (f" — {co['title']}" if co["title"] else "")
            + f' <span style="color:{role_color};font-size:.8rem">{co["role_in_process"] or ""}</span>',
            unsafe_allow_html=True,
        )
        links = []
        if co["email"]:
            links.append(f"✉️ {co['email']}")
        if co["phone"]:
            links.append(f"📞 {co['phone']}")
        if co["linkedin_url"]:
            links.append(f"[LinkedIn ↗]({co['linkedin_url']})")
        if links:
            st.caption("  |  ".join(links))
        if co["notes"]:
            st.caption(co["notes"])
        dc1, _ = st.columns([1, 8])
        if dc1.button("🗑", key=f"del_co_{co['id']}", help="Delete"):
            db.delete_contact(conn, co["id"])
            st.rerun()
        st.markdown('<hr style="margin:4px 0;border-color:#374151">', unsafe_allow_html=True)


# ── Prep tab ───────────────────────────────────────────────────────────────────

def _render_prep_tab(conn, app) -> None:
    st.caption("Use this tab to prep for interviews. Notes are saved per application.")
    base_resume = _load_base_resume_settings(conn)
    linked_questions = db.get_questions(conn, company=app["company"], application_id=app["id"])
    if linked_questions:
        with st.expander(f"🎯 Related Question Bank ({len(linked_questions)})", expanded=False):
            for q in linked_questions:
                star_parts = [q["star_situation"], q["star_task"], q["star_action"], q["star_result"]]
                has_story = any(part for part in star_parts)
                tags = f"  ·  _{q['tags']}_" if q["tags"] else ""
                st.markdown(f"**{q['question']}**{tags}")
                if has_story:
                    if q["star_situation"]:
                        st.caption(f"Situation: {q['star_situation']}")
                    if q["star_task"]:
                        st.caption(f"Task: {q['star_task']}")
                    if q["star_action"]:
                        st.caption(f"Action: {q['star_action']}")
                    if q["star_result"]:
                        st.caption(f"Result: {q['star_result']}")
                else:
                    st.caption("No STAR story saved yet.")
                st.markdown("---")
    with st.expander("📄 Resume Tailoring", expanded=False):
        if not str(base_resume.get("text") or "").strip():
            st.info("Add your base resume in Search Settings → Base Resume to enable tailored resume guidance.")
        else:
            suggested_keywords = _tailor_resume_keywords(app, base_resume)
            suggested_summary = _tailored_resume_summary(app, base_resume)
            st.caption(f"Using base resume: {base_resume.get('name') or 'Stored resume'}")
            tc1, tc2 = st.columns(2)
            tailored_keywords = tc1.text_area(
                "Keywords To Add / Emphasize",
                value=app["resume_tailor_keywords"] or "\n".join(suggested_keywords),
                height=120,
                placeholder="One keyword or phrase per line.",
            )
            tailored_notes = tc2.text_area(
                "Tailoring Notes",
                value=app["resume_tailor_notes"] or "",
                height=120,
                placeholder="What to emphasize, compress, or reorder for this role.",
            )
            tailored_summary = st.text_area(
                "Tailored Resume Brief",
                value=app["resume_tailor_summary"] or suggested_summary,
                height=180,
                help="A copy-ready tailoring brief based on the base resume and this job's matched keywords.",
            )
            save_col, copy_col = st.columns([1, 1])
            if save_col.button("💾 Save Tailoring Notes", key=f"save_resume_tailor_{app['id']}"):
                db.update_application(
                    conn,
                    app["id"],
                    resume_tailor_keywords=tailored_keywords.strip() or None,
                    resume_tailor_notes=tailored_notes.strip() or None,
                    resume_tailor_summary=tailored_summary.strip() or None,
                )
                st.success("Resume tailoring notes saved.")
                st.rerun()
            copy_col.download_button(
                "⬇️ Export Tailoring Brief",
                data=(tailored_summary or "").encode("utf-8"),
                file_name=f"resume_tailor_{app['company']}_{app['id']}.md".replace(" ", "_"),
                mime="text/markdown",
                key=f"export_resume_tailor_{app['id']}",
                use_container_width=True,
            )
            st.markdown("**Saved Resume Variants**")
            existing_variants = db.get_resume_variants(conn, app["id"])
            variant_options = ["New variant"] + [
                f"{row['id']}: {row['variant_name']}" for row in existing_variants
            ]
            selected_variant_label = st.selectbox(
                "Choose Variant",
                variant_options,
                key=f"resume_variant_picker_{app['id']}",
            )
            selected_variant = None
            if selected_variant_label != "New variant":
                selected_id = int(selected_variant_label.split(":", 1)[0])
                selected_variant = db.get_resume_variant(conn, selected_id)

            default_variant_name = selected_variant["variant_name"] if selected_variant else _default_resume_variant_name(app)
            default_variant_notes = selected_variant["notes"] if selected_variant else tailored_notes
            default_variant_content = (
                selected_variant["content"]
                if selected_variant and selected_variant["content"]
                else _build_resume_variant_content(app, base_resume, tailored_keywords, tailored_notes, tailored_summary)
            )

            variant_name = st.text_input(
                "Variant Name",
                value=default_variant_name,
                key=f"resume_variant_name_{app['id']}",
            )
            variant_notes = st.text_area(
                "Variant Notes",
                value=default_variant_notes or "",
                height=80,
                key=f"resume_variant_notes_{app['id']}",
            )
            variant_content = st.text_area(
                "Variant Content",
                value=default_variant_content,
                height=280,
                key=f"resume_variant_content_{app['id']}",
                help="This is the editable saved resume variant for this application.",
            )
            rv1, rv2, rv3 = st.columns(3)
            if rv1.button("💾 Save Resume Variant", key=f"save_resume_variant_{app['id']}"):
                if selected_variant:
                    db.update_resume_variant(
                        conn,
                        selected_variant["id"],
                        variant_name=variant_name.strip() or _default_resume_variant_name(app),
                        source_resume_name=base_resume.get("name") or None,
                        notes=variant_notes.strip() or None,
                        content=variant_content.strip() or None,
                    )
                else:
                    db.add_resume_variant(
                        conn,
                        app["id"],
                        variant_name=variant_name.strip() or _default_resume_variant_name(app),
                        source_resume_name=base_resume.get("name") or None,
                        notes=variant_notes.strip() or None,
                        content=variant_content.strip() or None,
                    )
                st.success("Resume variant saved.")
                st.rerun()
            rv2.download_button(
                "⬇️ Export Resume Variant",
                data=(variant_content or "").encode("utf-8"),
                file_name=f"{(variant_name or _default_resume_variant_name(app)).replace(' ', '_')}.md",
                mime="text/markdown",
                key=f"export_resume_variant_{app['id']}",
                use_container_width=True,
            )
            if selected_variant and rv3.button("🗑 Delete Variant", key=f"delete_resume_variant_{app['id']}"):
                db.delete_resume_variant(conn, selected_variant["id"])
                st.success("Resume variant deleted.")
                st.rerun()
    with st.form(f"prep_form_{app['id']}"):
        prep_company = st.text_area(
            "Company Research",
            value=app["prep_company"] or "",
            height=100,
            placeholder="Products, business model, recent news, competitors, culture signals…",
        )
        prep_why = st.text_area(
            "Why This Role / Why This Company",
            value=app["prep_why"] or "",
            height=80,
            placeholder="What excites you about this role specifically?",
        )
        prep_tyabt = st.text_area(
            "Tell Me About Yourself (TMAY)",
            value=app["prep_tyabt"] or "",
            height=100,
            placeholder="Your tailored 90-second pitch for this role…",
        )
        prep_questions = st.text_area(
            "Questions to Ask",
            value=app["prep_questions"] or "",
            height=80,
            placeholder="What does success look like in 90 days?\nWhat's the biggest challenge the team is facing?",
        )
        prep_notes = st.text_area(
            "Other Prep Notes",
            value=app["prep_notes"] or "",
            height=80,
            placeholder="STAR stories, key talking points, things to avoid…",
        )
        if st.form_submit_button("💾 Save Prep Notes", type="primary"):
            db.update_application(
                conn, app["id"],
                prep_company   = prep_company.strip() or None,
                prep_why       = prep_why.strip() or None,
                prep_tyabt     = prep_tyabt.strip() or None,
                prep_questions = prep_questions.strip() or None,
                prep_notes     = prep_notes.strip() or None,
            )
            st.success("Prep notes saved.")
            st.rerun()


# ── Negotiate tab ──────────────────────────────────────────────────────────────

def _render_negotiate_tab(conn, app) -> None:
    offer_base = app["offer_base"] or 0
    st.caption("Use this worksheet to plan your counter-offer before negotiating.")

    with st.form(f"nego_form_{app['id']}"):
        nc1, nc2 = st.columns(2)
        target_base   = nc1.number_input("Your Target Base ($)", step=5000,
                                          value=int(app["nego_target_base"] or 0),
                                          help="The salary you'll ask for")
        walkaway_base = nc2.number_input("Walk-Away Base ($)", step=5000,
                                          value=int(app["nego_walkaway_base"] or 0),
                                          help="The lowest you'll accept")
        nm1, nm2 = st.columns(2)
        market_low  = nm1.number_input("Market Low ($)",  step=5000,
                                        value=int(app["nego_market_low"] or 0),
                                        help="From Levels.fyi, Glassdoor, LinkedIn Salary, etc.")
        market_high = nm2.number_input("Market High ($)", step=5000,
                                        value=int(app["nego_market_high"] or 0))
        nego_notes  = st.text_area("Talking Points / Notes",
                                    value=app["nego_notes"] or "", height=100,
                                    placeholder="BATNA, competing offers, reasons you deserve more…")
        if st.form_submit_button("💾 Save Worksheet", type="primary"):
            db.update_application(
                conn, app["id"],
                nego_target_base   = target_base or None,
                nego_walkaway_base = walkaway_base or None,
                nego_market_low    = market_low or None,
                nego_market_high   = market_high or None,
                nego_notes         = nego_notes.strip() or None,
            )
            st.success("Saved.")
            st.rerun()

    # ── Live calculation ──────────────────────────────────────────────────────
    if offer_base or target_base:
        st.divider()
        st.markdown("**Quick math**")
        rows = []
        if offer_base:
            rows.append(("Current offer", f"${offer_base:,}"))
        if target_base and offer_base:
            diff = target_base - offer_base
            pct  = diff / offer_base * 100
            rows.append(("Counter-offer ask", f"${target_base:,}  (+${diff:,} / +{pct:.1f}%)"))
        if walkaway_base and offer_base:
            diff = walkaway_base - offer_base
            pct  = diff / offer_base * 100
            sign = "+" if diff >= 0 else ""
            rows.append(("Walk-away floor", f"${walkaway_base:,}  ({sign}${diff:,} / {sign}{pct:.1f}%)"))
        if market_low and market_high:
            rows.append(("Market range", f"${market_low:,} – ${market_high:,}"))
        if offer_base and market_low and market_high:
            mid = (market_low + market_high) // 2
            vs_mid = offer_base - mid
            sign = "+" if vs_mid >= 0 else ""
            rows.append(("Offer vs market midpoint", f"{sign}${vs_mid:,}"))
        for label, val in rows:
            c1, c2 = st.columns([2, 3])
            c1.caption(label)
            c2.markdown(val)

    if offer_base or target_base or walkaway_base:
        st.divider()
        st.markdown("**Negotiation Playbook**")
        playbook = _negotiation_playbook_lines(app, target_base, walkaway_base, market_low, market_high)

        cadence = [
            "1. Thank them, restate enthusiasm, and ask for a short window to review.",
            "2. Counter once with a specific ask and 2–3 business reasons tied to scope, market, and impact.",
            "3. If base stalls, shift to sign-on, equity, remote flexibility, or start-date support.",
            "4. Get final terms in writing before you verbally accept.",
        ]

        for line in playbook:
            st.markdown(f"- {line}")
        st.caption("Suggested cadence")
        for line in cadence:
            st.markdown(f"- {line}")

        with st.expander("Suggested Counter Email", expanded=False):
            st.code(
                _negotiation_counter_draft(app, target_base, market_low, market_high),
                language="markdown",
            )


# ── Inline company profile ──────────────────────────────────────────────────────

def _render_inline_company_profile(conn, company_name: str) -> None:
    """Show a collapsible company profile panel below the application detail tabs."""
    profile = db.get_company_profile(conn, company_name)
    label   = f"🏢 {company_name} — Company Profile" + (" ✏️" if profile else " (no profile yet)")
    with st.expander(label, expanded=False):
        from jobsearch.views.company_profiles_page import _render_profile_form
        _render_profile_form(conn, profile=profile if profile else None)
        if not profile:
            st.caption(f"This will create a new profile for **{company_name}** shared across all applications to this company.")


# ── Edit form ──────────────────────────────────────────────────────────────────

def _render_edit_form(conn, app) -> None:
    # Parse stored follow_up_date ISO string → date object for date_input
    _fu_date = None
    if app["follow_up_date"]:
        try:
            _fu_date = date.fromisoformat(app["follow_up_date"][:10])
        except Exception:
            pass

    _current_entry_type = app["entry_type"] if app["entry_type"] else "application"

    with st.form(f"edit_app_{app['id']}"):
        et1, et2 = st.columns(2)
        _type_options = ["Application", "Opportunity", "Job Fair"]
        _type_idx     = {"application": 0, "opportunity": 1, "job_fair": 2}.get(_current_entry_type, 0)
        entry_type_label = et1.radio(
            "Type",
            _type_options,
            index=_type_idx,
            horizontal=True,
            key=f"edit_entry_type_{app['id']}",
        )

        ec1, ec2 = st.columns(2)
        company  = ec1.text_input("Company", value=app["company"] or "")
        role     = ec2.text_input("Role",    value=app["role"]    or "")

        ea1, ea2, ea3 = st.columns(3)
        status_idx = db.STATUSES.index(app["status"]) if app["status"] in db.STATUSES else 0
        status   = ea1.selectbox("Status", db.STATUSES, index=status_idx)
        fit_opts = ["—", "1", "2", "3", "4", "5"]
        fit_idx  = fit_opts.index(str(app["fit_stars"])) if str(app["fit_stars"]) in fit_opts else 0
        fit      = ea2.selectbox("Fit", fit_opts, index=fit_idx)
        job_url  = ea3.text_input("Job URL", value=app["job_url"] or "")

        eb1, eb2 = st.columns(2)
        sal_low  = eb1.number_input("Salary Low ($)",  value=int(app["salary_low"]  or 0), step=5000)
        sal_high = eb2.number_input("Salary High ($)", value=int(app["salary_high"] or 0), step=5000)

        referral   = st.text_input("Referral", value=app["referral"] or "")
        jd_summary = st.text_area("JD Summary", value=app["jd_summary"] or "", height=100)
        notes      = st.text_area("Notes", value=app["notes"] or "", height=80)

        st.markdown("**Follow-up**")
        rf1, rf2 = st.columns(2)
        follow_up_date  = rf1.date_input("Follow-up date", value=_fu_date)
        follow_up_notes = rf2.text_input("Who / what to say",
                                         value=app["follow_up_notes"] or "",
                                         placeholder="e.g. Email Sarah the recruiter")

        st.markdown("**Documents**")
        resume_version     = st.text_input("Resume version",
                                            value=app["resume_version"] or "",
                                            placeholder="e.g. PM resume v3 – fintech tailored")
        cover_letter_notes = st.text_input("Cover letter notes",
                                            value=app["cover_letter_notes"] or "",
                                            placeholder="e.g. Tailored intro, emphasized API PM exp")
        du1, du2 = st.columns(2)
        resume_url       = du1.text_input("Resume URL",
                                          value=app["resume_url"] or "",
                                          placeholder="e.g. link to Google Doc or Dropbox")
        cover_letter_url = du2.text_input("Cover Letter URL",
                                          value=app["cover_letter_url"] or "",
                                          placeholder="e.g. link to Google Doc")
        job_description  = st.text_area("Full Job Description",
                                         value=app["job_description"] or "",
                                         height=120,
                                         placeholder="Paste the full JD here for reference and future search")

        st.markdown("**Offer Details**")
        current_work_type, current_unit, current_hours, current_weeks = _offer_work_type_defaults(app)
        work_type_options = list(WORK_TYPE_LABELS.keys())
        work_type_idx = work_type_options.index(current_work_type) if current_work_type in work_type_options else 0
        of0, of1, of2, of3 = st.columns(4)
        offer_work_type = of0.selectbox(
            "Comp Model",
            work_type_options,
            index=work_type_idx,
            format_func=lambda value: WORK_TYPE_LABELS.get(value, value),
        )
        offer_unit = of1.selectbox(
            "Comp Unit",
            ["salary", "hourly"],
            index=0 if current_unit == "salary" else 1,
            format_func=lambda value: value.title(),
        )
        offer_base      = of2.number_input("Base Salary ($)", value=int(app["offer_base"] or 0), step=5000)
        hourly_rate     = of3.number_input("Hourly Rate ($/hr)", value=float(app["hourly_rate"] or 0.0), step=5.0)
        of4, of5, of6, of7 = st.columns(4)
        offer_bonus_pct = of4.number_input("Bonus (%)", value=int(app["offer_bonus_pct"] or 0), step=5)
        offer_signing   = of5.number_input("Signing Bonus ($)", value=int(app["offer_signing"] or 0), step=1000)
        hours_per_week  = of6.number_input("Hours / week", value=float(current_hours), step=1.0)
        weeks_per_year  = of7.number_input("Weeks / year", value=float(current_weeks), step=1.0)
        of8, of9, of10 = st.columns(3)
        offer_pto_days    = of8.number_input("PTO Days", value=int(app["offer_pto_days"] or 0), step=1)
        offer_k401_match  = of9.text_input("401k Match", value=app["offer_k401_match"] or "", placeholder="e.g. 4% match")
        offer_equity      = of10.text_input("Equity", value=app["offer_equity"] or "", placeholder="e.g. $50k RSU over 4yr")
        remote_opts = ["", "Remote", "Hybrid", "Onsite"]
        remote_idx  = remote_opts.index(app["offer_remote_policy"]) if app["offer_remote_policy"] in remote_opts else 0
        offer_remote_policy = st.selectbox("Remote Policy", remote_opts, index=remote_idx)

        _offer_start_date  = None
        _offer_expiry_date = None
        if app["offer_start_date"]:
            try:
                _offer_start_date = date.fromisoformat(app["offer_start_date"][:10])
            except Exception:
                pass
        if app["offer_expiry_date"]:
            try:
                _offer_expiry_date = date.fromisoformat(app["offer_expiry_date"][:10])
            except Exception:
                pass
        od1, od2 = st.columns(2)
        offer_start_date  = od1.date_input("Start Date",  value=_offer_start_date)
        offer_expiry_date = od2.date_input("Offer Expires", value=_offer_expiry_date)
        offer_notes = st.text_area("Offer Notes", value=app["offer_notes"] or "", height=60,
                                   placeholder="Benefits details, equity vesting schedule, negotiation notes…")
        offer_comp = normalize_compensation(
            _load_search_preferences(),
            {
                "work_type": offer_work_type,
                "compensation_unit": offer_unit,
                "salary_min": offer_base or None,
                "salary_max": offer_base or None,
                "hourly_rate": hourly_rate or None,
                "hours_per_week": hours_per_week if offer_unit == "hourly" else None,
                "weeks_per_year": weeks_per_year if offer_unit == "hourly" else None,
                "salary_text": "",
                "description": offer_notes or "",
            },
        )
        normalized_offer = offer_comp["normalized_compensation_usd"]
        if normalized_offer:
            bonus_cash = normalized_offer * ((offer_bonus_pct or 0) / 100.0)
            first_year = normalized_offer + bonus_cash + float(offer_signing or 0)
            st.caption(
                f"Normalized annual comp: ${normalized_offer:,.0f} | "
                f"First-year cash: ${first_year:,.0f}"
            )

        sc1, sc2 = st.columns([2, 1])
        saved   = sc1.form_submit_button("💾 Save", type="primary")
        deleted = sc2.form_submit_button("🗑 Delete Application")

    if saved:
        auto_follow_up = follow_up_date or _default_follow_up_date(status)
        auto_follow_up_note = follow_up_notes.strip() or _follow_up_template_note(status)
        db.update_application(
            conn, app["id"],
            entry_type         = {"Opportunity": "opportunity", "Job Fair": "job_fair"}.get(entry_type_label, "application"),
            company            = company.strip(),
            role               = role.strip(),
            status             = status,
            fit_stars          = int(fit) if fit != "—" else None,
            job_url            = job_url.strip() or None,
            salary_low         = sal_low  or None,
            salary_high        = sal_high or None,
            salary_range       = f"${sal_low:,}–${sal_high:,}" if sal_low and sal_high else None,
            referral           = referral.strip() or None,
            jd_summary         = jd_summary.strip() or None,
            notes              = notes.strip() or None,
            follow_up_date     = auto_follow_up.isoformat() if auto_follow_up else None,
            follow_up_notes    = auto_follow_up_note or None,
            resume_version     = resume_version.strip() or None,
            cover_letter_notes = cover_letter_notes.strip() or None,
            resume_url         = resume_url.strip() or None,
            cover_letter_url   = cover_letter_url.strip() or None,
            job_description    = job_description.strip() or None,
            work_type          = offer_work_type,
            compensation_unit  = offer_unit,
            hourly_rate        = hourly_rate or None,
            hours_per_week     = hours_per_week if offer_unit == "hourly" else None,
            weeks_per_year     = weeks_per_year if offer_unit == "hourly" else None,
            normalized_compensation_usd = normalized_offer or None,
            offer_base         = offer_base or None,
            offer_bonus_pct    = offer_bonus_pct or None,
            offer_signing      = offer_signing or None,
            offer_pto_days     = offer_pto_days or None,
            offer_k401_match   = offer_k401_match.strip() or None,
            offer_equity       = offer_equity.strip() or None,
            offer_remote_policy = offer_remote_policy or None,
            offer_start_date   = offer_start_date.isoformat() if offer_start_date else None,
            offer_expiry_date  = offer_expiry_date.isoformat() if offer_expiry_date else None,
            offer_notes        = offer_notes.strip() or None,
        )
        st.success("Saved.")
        st.rerun()

    if deleted:
        db.delete_application(conn, app["id"])
        st.session_state["tracker_selected_id"] = None
        st.rerun()


# ── Offer comparison ────────────────────────────────────────────────────────────

def _render_offer_comparison(offer_apps) -> None:
    """Side-by-side comparison of offers."""
    if len(offer_apps) < 2:
        return

    rows = _offer_comparison_rows(offer_apps)
    df = pd.DataFrame(rows)
    if df.empty:
        return

    best_norm = df["Normalized Annual ($)"].max()
    best_first_year = df["First-Year Cash ($)"].max()
    c1, c2, c3 = st.columns(3)
    c1.metric("Offers Compared", len(df))
    c2.metric("Best Normalized Annual", f"${best_norm:,.0f}" if best_norm else "—")
    c3.metric("Best First-Year Cash", f"${best_first_year:,.0f}" if best_first_year else "—")

    df["Vs Best Annual ($)"] = df["Normalized Annual ($)"].apply(
        lambda value: f"{value - best_norm:+,.0f}" if value else "—"
    )
    df["Vs Best First-Year ($)"] = df["First-Year Cash ($)"].apply(
        lambda value: f"{value - best_first_year:+,.0f}" if value else "—"
    )
    display_df = df.copy()
    for col in ["Normalized Annual ($)", "Bonus Cash ($)", "Signing ($)", "First-Year Cash ($)", "PTO Days"]:
        if col in display_df.columns:
            display_df[col] = display_df[col].apply(lambda value: "—" if not value else f"${value:,.0f}" if "($)" in col else f"{int(value)}")

    st.caption("Annual values are normalized using the same salary/W2/1099 assumptions as the search scorer.")
    st.dataframe(display_df.set_index("Company"), use_container_width=True)
    exp1, exp2 = st.columns(2)
    exp1.download_button(
        "⬇️ Export Comparison CSV",
        data=df.to_csv(index=False).encode("utf-8"),
        file_name=f"offer_comparison_{date.today().isoformat()}.csv",
        mime="text/csv",
        key="offer_comparison_csv",
        use_container_width=True,
    )
    exp2.download_button(
        "⬇️ Export Comparison Brief",
        data=_offer_comparison_markdown(offer_apps).encode("utf-8"),
        file_name=f"offer_comparison_{date.today().isoformat()}.md",
        mime="text/markdown",
        key="offer_comparison_md",
        use_container_width=True,
    )
    with st.expander("Copy-Ready Comparison Brief", expanded=False):
        st.code(_offer_comparison_markdown(offer_apps), language="markdown")
