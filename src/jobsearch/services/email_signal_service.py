from __future__ import annotations

import hashlib
import re
from datetime import datetime
from email.utils import parsedate_to_datetime
from typing import Iterable, Optional


NEW_APPLICATION_PATTERNS = [
    "application received",
    "received your application",
    "thank you for applying",
    "thanks for applying",
    "application submitted",
]

REJECTION_PATTERNS = [
    "not moving forward",
    "move forward with other candidates",
    "unfortunately",
    "we regret to inform",
    "position has been filled",
    "no longer under consideration",
]

INTERVIEW_PATTERNS = [
    "schedule an interview",
    "schedule a time",
    "share your availability",
    "phone screen",
    "interview request",
    "next round",
    "next interview",
]

MONTH_PATTERN = r"(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)"
TIME_PATTERN = r"(?:[0-1]?\d(?::[0-5]\d)?\s?(?:am|pm))"


def _normalize_text(*parts: Optional[str]) -> str:
    return " ".join(str(part or "") for part in parts).lower()


def _extract_role(text: str) -> Optional[str]:
    patterns = [
        r"for (?:the )?(?P<role>.+?) role",
        r"for (?:the )?position of (?P<role>.+?)(?:\.|,| at |$)",
        r"application for (?P<role>.+?)(?:\.|,| at |$)",
        r"interview for (?P<role>.+?)(?:\.|,| at |$)",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            role = match.group("role").strip(" -:.,")
            role = re.sub(
                r"^(?:the )?(?:next interview for |interview for |application for )",
                "",
                role,
                flags=re.IGNORECASE,
            ).strip(" -:.,")
            role = re.sub(r"^(?:the )", "", role, flags=re.IGNORECASE).strip(" -:.,")
            if role and len(role) <= 120:
                return role.title()
    return None


def _extract_company(known_companies: Iterable[str], text: str, sender: str) -> Optional[str]:
    haystack = f"{text} {sender}".lower()
    candidates = sorted({c for c in known_companies if c}, key=len, reverse=True)
    for company in candidates:
        if company.lower() in haystack:
            return company
    return None


def _extract_interview_datetime(text: str, received_at: Optional[str]) -> Optional[str]:
    patterns = [
        rf"on\s+(?P<month>{MONTH_PATTERN})\s+(?P<day>\d{{1,2}})(?:,\s*(?P<year>\d{{4}}))?(?:\s+at\s+(?P<time>{TIME_PATTERN}))?",
        rf"(?P<month>{MONTH_PATTERN})\s+(?P<day>\d{{1,2}})(?:,\s*(?P<year>\d{{4}}))?(?:\s+at\s+(?P<time>{TIME_PATTERN}))?",
    ]
    base_year = datetime.now().year
    if received_at:
        try:
            base_year = parsedate_to_datetime(received_at).year
        except Exception:
            pass
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if not match:
            continue
        month = match.group("month")
        day = int(match.group("day"))
        year = int(match.group("year")) if match.groupdict().get("year") else base_year
        time_part = (match.groupdict().get("time") or "9:00 am").replace("  ", " ").strip().upper()
        for fmt in ("%B %d %Y %I:%M %p", "%b %d %Y %I:%M %p", "%B %d %Y %I %p", "%b %d %Y %I %p"):
            try:
                return datetime.strptime(f"{month} {day} {year} {time_part}", fmt).isoformat()
            except Exception:
                continue
    return None


def _extract_interviewer_names(text: str, sender: Optional[str]) -> Optional[str]:
    patterns = [
        r"with\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,2})",
        r"interview(?:ing)?\s+with\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,2})",
    ]
    raw_text = text or ""
    for pattern in patterns:
        match = re.search(pattern, raw_text)
        if match:
            return match.group(1).strip()
    sender_text = str(sender or "")
    name_part = sender_text.split("<", 1)[0].strip().strip('"')
    if "@" not in name_part and name_part and len(name_part.split()) <= 4:
        return name_part
    return None


def _extract_meeting_location(text: str) -> Optional[str]:
    url_match = re.search(r"https?://[^\s)>\]]+", text or "", flags=re.IGNORECASE)
    if url_match:
        return url_match.group(0).rstrip(".,")
    return None


def _extract_duration_mins(text: str) -> Optional[int]:
    match = re.search(r"(\d{2,3})\s*(?:minutes|mins|min)\b", text or "", flags=re.IGNORECASE)
    if match:
        return int(match.group(1))
    return None


def signal_resolution_for_existing_application(signal_type: str, linked_status: Optional[str]) -> tuple[str, Optional[str]]:
    status = str(linked_status or "").lower()
    if not status:
        return "new", None
    if signal_type == "new_application":
        return "resolved", f"Matched existing application already tracked as {status}."
    if signal_type == "rejection" and status == "rejected":
        return "resolved", "Matched application already marked rejected."
    if signal_type == "interview_request" and status in {"interviewing", "offer", "accepted"}:
        return "resolved", f"Matched application already in {status} stage."
    return "new", None


def classify_email_signal(
    *,
    message_id: Optional[str],
    thread_id: Optional[str],
    sender: Optional[str],
    subject: str,
    body: Optional[str],
    received_at: Optional[str],
    known_companies: Iterable[str],
) -> Optional[dict]:
    text = _normalize_text(subject, body)
    signal_type = None
    if any(pattern in text for pattern in REJECTION_PATTERNS):
        signal_type = "rejection"
    elif any(pattern in text for pattern in INTERVIEW_PATTERNS):
        signal_type = "interview_request"
    elif any(pattern in text for pattern in NEW_APPLICATION_PATTERNS):
        signal_type = "new_application"
    if not signal_type:
        return None

    company = _extract_company(known_companies, text, str(sender or ""))
    role = _extract_role(text)
    interview_scheduled_at = None
    interviewer_names = None
    interview_location = None
    interview_duration_mins = None
    if signal_type == "interview_request":
        source_text = " ".join(part for part in [subject, body] if part)
        interview_scheduled_at = _extract_interview_datetime(source_text, received_at)
        interviewer_names = _extract_interviewer_names(body or subject, sender)
        interview_location = _extract_meeting_location(body or "")
        interview_duration_mins = _extract_duration_mins(body or "")
    stable_id = message_id or hashlib.md5(
        f"{sender}|{subject}|{received_at}|{thread_id}".encode("utf-8")
    ).hexdigest()
    excerpt_source = (body or subject or "").strip()
    return {
        "message_id": stable_id,
        "thread_id": thread_id,
        "sender": sender,
        "subject": subject,
        "received_at": received_at,
        "signal_type": signal_type,
        "company": company,
        "role": role,
        "raw_excerpt": excerpt_source[:500],
        "interview_scheduled_at": interview_scheduled_at,
        "interviewer_names": interviewer_names,
        "interview_location": interview_location,
        "interview_duration_mins": interview_duration_mins,
    }
