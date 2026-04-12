"""Daily email digest — sends Apply Now / Review Today jobs via Gmail SMTP."""

from __future__ import annotations

import logging
import smtplib
import sqlite3
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import List, Optional

logger = logging.getLogger(__name__)

_GMAIL_SMTP_HOST = "smtp.gmail.com"
_GMAIL_SMTP_PORT = 465


def send_digest(
    conn: sqlite3.Connection,
    new_jobs: List[sqlite3.Row],
    recipient_email: str,
    sender_email: str,
    sender_app_password: str,
) -> bool:
    """
    Send an HTML email digest of new high-score jobs.

    Args:
        conn: Database connection (unused here but kept for future token checks)
        new_jobs: Rows from applications table — must have company, role_title_raw,
                  score, fit_band, url, date_discovered columns.
        recipient_email: Destination address.
        sender_email: Gmail address used to send (must match app password).
        sender_app_password: Gmail app password (not account password).

    Returns:
        True on success, False on failure.
    """
    if not new_jobs:
        return False
    if not recipient_email or not sender_email or not sender_app_password:
        logger.warning("Email digest skipped — missing credentials or recipient")
        return False

    try:
        html = _build_html(new_jobs)
        msg = MIMEMultipart("alternative")
        msg["Subject"] = f"Job Search Digest — {len(new_jobs)} new match{'es' if len(new_jobs) != 1 else ''}"
        msg["From"] = sender_email
        msg["To"] = recipient_email
        msg.attach(MIMEText(html, "html"))

        with smtplib.SMTP_SSL(_GMAIL_SMTP_HOST, _GMAIL_SMTP_PORT) as server:
            server.login(sender_email, sender_app_password)
            server.sendmail(sender_email, recipient_email, msg.as_string())

        logger.info("Email digest sent to %s (%d jobs)", recipient_email, len(new_jobs))
        return True
    except Exception as exc:
        logger.error("Email digest failed: %s", exc, exc_info=True)
        return False


def _build_html(jobs: List[sqlite3.Row]) -> str:
    """Build the HTML body for the digest email."""
    # Split by bucket
    apply_now = [j for j in jobs if (j["fit_band"] or "") == "Strong Match"]
    review_today = [j for j in jobs if (j["fit_band"] or "") == "Good Match"]

    sections = ""
    if apply_now:
        sections += _section("Apply Now", "#10b981", apply_now)
    if review_today:
        sections += _section("Review Today", "#f59e0b", review_today)
    # Anything else
    other = [j for j in jobs if j not in apply_now and j not in review_today]
    if other:
        sections += _section("Other Matches", "#6b7280", other)

    return f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"></head>
<body style="font-family:Arial,sans-serif;max-width:680px;margin:0 auto;padding:20px;color:#1f2937">
  <h2 style="color:#1f2937;border-bottom:2px solid #e5e7eb;padding-bottom:8px">
    Job Search Digest
  </h2>
  <p style="color:#6b7280;font-size:0.9rem">{len(jobs)} new match{'es' if len(jobs) != 1 else ''} since last check</p>
  {sections}
  <hr style="border:none;border-top:1px solid #e5e7eb;margin-top:24px">
  <p style="color:#9ca3af;font-size:0.75rem">Sent by Job Search Pipeline &nbsp;|&nbsp; Open the dashboard to manage applications</p>
</body>
</html>"""


def _section(title: str, color: str, jobs: List[sqlite3.Row]) -> str:
    rows = ""
    for j in jobs:
        company = j["company"] or ""
        role = j["role_title_raw"] or ""
        score = j["score"] or 0
        url = j.get("url") or j.get("careers_url") or ""
        link = f'<a href="{url}" style="color:#2563eb">{company} — {role}</a>' if url else f"{company} — {role}"
        rows += f"""
        <tr>
          <td style="padding:8px 4px;border-bottom:1px solid #f3f4f6">{link}</td>
          <td style="padding:8px 4px;border-bottom:1px solid #f3f4f6;text-align:right;color:#6b7280;font-size:0.85rem">{score:.0f}</td>
        </tr>"""

    return f"""
  <h3 style="color:{color};margin-top:20px">{title} ({len(jobs)})</h3>
  <table style="width:100%;border-collapse:collapse">
    <thead>
      <tr>
        <th style="text-align:left;padding:4px;color:#6b7280;font-size:0.8rem;border-bottom:2px solid #e5e7eb">Position</th>
        <th style="text-align:right;padding:4px;color:#6b7280;font-size:0.8rem;border-bottom:2px solid #e5e7eb">Score</th>
      </tr>
    </thead>
    <tbody>{rows}</tbody>
  </table>"""
