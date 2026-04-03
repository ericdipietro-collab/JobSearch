from __future__ import annotations

import hashlib
import re
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
    }
