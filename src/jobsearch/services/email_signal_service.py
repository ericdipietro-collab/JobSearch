from __future__ import annotations

import hashlib
import re
from datetime import datetime, timedelta
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
    "interview confirmation",
    "interview scheduled",
    "next round",
    "next interview",
    "share availability",
    "calendar invite",
    "screening call",
    "intro call",
]

INTERVIEW_RESCHEDULE_PATTERNS = [
    "rescheduled",
    "reschedule",
    "updated invitation",
    "updated invite",
    "new time",
    "new interview time",
    "moved to",
]

INTERVIEW_CANCELLATION_PATTERNS = [
    "cancelled",
    "canceled",
    "interview has been canceled",
    "interview has been cancelled",
    "cancel this interview",
    "cancellation",
]

MARKETING_PATTERNS = [
    "unsubscribe",
    "subscribe",
    "sale",
    "steal",
    "discount",
    "approved sale",
    "something you need to see",
    "curated",
    "shop",
    "buy now",
]

MONTH_PATTERN = r"(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)"
TIME_PATTERN = r"(?:[0-1]?\d(?::[0-5]\d)?\s?(?:am|pm))"
WEEKDAY_PATTERN = r"(?:monday|tuesday|wednesday|thursday|friday|saturday|sunday)"


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
    ics_match = re.search(
        r"DTSTART(?:;TZID=(?P<tzid>[^:;\r\n]+))?:(?P<value>\d{8}T\d{6}Z?)",
        text or "",
        flags=re.IGNORECASE,
    )
    if ics_match:
        value = ics_match.group("value")
        try:
            if value.endswith("Z"):
                return datetime.strptime(value, "%Y%m%dT%H%M%SZ").isoformat()
            return datetime.strptime(value, "%Y%m%dT%H%M%S").isoformat()
        except Exception:
            pass

    patterns = [
        rf"on\s+(?P<month>{MONTH_PATTERN})\s+(?P<day>\d{{1,2}})(?:,\s*(?P<year>\d{{4}}))?(?:\s+at\s+(?P<time>{TIME_PATTERN}))?",
        rf"(?P<month>{MONTH_PATTERN})\s+(?P<day>\d{{1,2}})(?:,\s*(?P<year>\d{{4}}))?(?:\s+at\s+(?P<time>{TIME_PATTERN}))?",
    ]
    base_dt = datetime.now()
    if received_at:
        try:
            base_dt = parsedate_to_datetime(received_at).replace(tzinfo=None)
        except Exception:
            pass
    base_year = base_dt.year
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
    weekday_matches = list(
        re.finditer(
            rf"(?:on\s+)?(?P<weekday>{WEEKDAY_PATTERN})(?:\s+at\s+(?P<time>{TIME_PATTERN}))?",
            text or "",
            flags=re.IGNORECASE,
        )
    )
    weekday_match = next((m for m in weekday_matches if m.groupdict().get("time")), None)
    if weekday_match is None and weekday_matches:
        weekday_match = weekday_matches[0]
    if weekday_match:
        weekday_name = weekday_match.group("weekday").lower()
        weekday_index = [
            "monday",
            "tuesday",
            "wednesday",
            "thursday",
            "friday",
            "saturday",
            "sunday",
        ].index(weekday_name)
        delta_days = (weekday_index - base_dt.weekday()) % 7
        if delta_days == 0:
            delta_days = 7
        target_date = base_dt + timedelta(days=delta_days)
        time_part = (weekday_match.groupdict().get("time") or "9:00 am").replace("  ", " ").strip().upper()
        for fmt in ("%Y-%m-%d %I:%M %p", "%Y-%m-%d %I %p"):
            try:
                return datetime.strptime(f"{target_date.date().isoformat()} {time_part}", fmt).isoformat()
            except Exception:
                continue
    return None


def _extract_interviewer_names(text: str, sender: Optional[str]) -> Optional[str]:
    schedule_line = re.search(
        rf"{TIME_PATTERN}\s*[-–]\s*{TIME_PATTERN}(?:\s+[A-Z]{{2,4}})?\s*:\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)",
        text or "",
        flags=re.IGNORECASE,
    )
    if schedule_line:
        return schedule_line.group(1).strip()
    ics_match = re.search(r"(?:ORGANIZER|ATTENDEE);CN=([^:;\r\n]+)", text or "", flags=re.IGNORECASE)
    if ics_match:
        return ics_match.group(1).strip()
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
    ics_location = re.search(r"LOCATION:([^\r\n]+)", text or "", flags=re.IGNORECASE)
    if ics_location:
        location = ics_location.group(1).strip()
        if re.fullmatch(r"[A-Za-z_]+/[A-Za-z_]+", location):
            location = ""
        if location:
            return location
    url_match = re.search(r"https?://[^\s)>\]]+", text or "", flags=re.IGNORECASE)
    if url_match:
        return url_match.group(0).rstrip(".,")
    meet_match = re.search(r"\b((?:meet|teams)\.[a-z0-9.-]+/[^\s)>\]]+)", text or "", flags=re.IGNORECASE)
    if meet_match:
        return f"https://{meet_match.group(1).rstrip('.,')}"
    return None


def _extract_duration_mins(text: str) -> Optional[int]:
    match = re.search(r"(\d{2,3})\s*(?:minutes|mins|min)\b", text or "", flags=re.IGNORECASE)
    if match:
        return int(match.group(1))
    range_match = re.search(
        rf"(?P<start>{TIME_PATTERN})\s*[-–]\s*(?P<end>{TIME_PATTERN})",
        text or "",
        flags=re.IGNORECASE,
    )
    if range_match:
        for fmt in ("%I:%M %p", "%I %p"):
            try:
                start = datetime.strptime(range_match.group("start").replace("  ", " ").strip().upper(), fmt)
                end = datetime.strptime(range_match.group("end").replace("  ", " ").strip().upper(), fmt)
                delta = int((end - start).total_seconds() // 60)
                if delta > 0:
                    return delta
            except Exception:
                continue
    return None


def infer_interview_type(subject: Optional[str], body: Optional[str], sender: Optional[str], location: Optional[str]) -> str:
    text = _normalize_text(subject, body, sender, location)
    if "final" in text:
        return "final"
    if "panel" in text:
        return "panel"
    if any(token in text for token in ("onsite", "on-site", "in person")):
        return "onsite"
    if any(token in text for token in ("take home", "take-home", "assignment")):
        return "take_home"
    if any(token in text for token in ("zoom", "google meet", "microsoft teams", "video")):
        return "video"
    if any(token in text for token in ("meet.google.com", "teams.microsoft.com")):
        return "video"
    if any(token in text for token in ("phone screen", "screening call", "intro call", "recruiter")):
        return "phone_screen"
    return "mixed"


def infer_interview_change_type(subject: Optional[str], body: Optional[str]) -> str:
    text = _normalize_text(subject, body)
    if any(pattern in text for pattern in INTERVIEW_CANCELLATION_PATTERNS):
        return "cancelled"
    if any(pattern in text for pattern in INTERVIEW_RESCHEDULE_PATTERNS):
        return "rescheduled"
    return "scheduled"


def signal_resolution_for_existing_application(signal_type: str, linked_status: Optional[str]) -> tuple[str, Optional[str]]:
    status = str(linked_status or "").lower()
    if not status:
        return "new", None
    if signal_type == "new_application":
        return "resolved", f"Matched existing application already tracked as {status}."
    if signal_type == "rejection" and status == "rejected":
        return "resolved", "Matched application already marked rejected."
    return "new", None


def _looks_like_interview_request(text: str, company: Optional[str]) -> bool:
    if any(pattern in text for pattern in INTERVIEW_CANCELLATION_PATTERNS + INTERVIEW_RESCHEDULE_PATTERNS):
        return True
    if any(pattern in text for pattern in INTERVIEW_PATTERNS):
        return True
    if any(pattern in text for pattern in MARKETING_PATTERNS):
        return False
    scheduling_cues = [
        "availability",
        "available to meet",
        "available for a call",
        "available for a conversation",
        "would like to meet",
        "would like to connect",
        "let's connect",
        "please join",
        "please confirm",
    ]
    meeting_cues = [
        "zoom",
        "google meet",
        "microsoft teams",
        "teams meeting",
        "phone call",
        "video call",
        "calendar",
        "monday",
        "tuesday",
        "wednesday",
        "thursday",
        "friday",
    ]
    return bool(company) and any(cue in text for cue in scheduling_cues) and any(cue in text for cue in meeting_cues)


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
    company = _extract_company(known_companies, text, str(sender or ""))
    signal_type = None
    if any(pattern in text for pattern in REJECTION_PATTERNS):
        signal_type = "rejection"
    elif _looks_like_interview_request(text, company):
        signal_type = "interview_request"
    elif any(pattern in text for pattern in NEW_APPLICATION_PATTERNS):
        signal_type = "new_application"
    if not signal_type:
        return None

    role = _extract_role(text)
    interview_scheduled_at = None
    interview_change_type = None
    interviewer_names = None
    interview_location = None
    interview_duration_mins = None
    if signal_type == "interview_request":
        source_text = " ".join(part for part in [subject, body] if part)
        interview_scheduled_at = _extract_interview_datetime(source_text, received_at)
        interview_change_type = infer_interview_change_type(subject, body)
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
        "interview_change_type": interview_change_type,
        "interviewer_names": interviewer_names,
        "interview_location": interview_location,
        "interview_duration_mins": interview_duration_mins,
    }
