from __future__ import annotations

import email
import imaplib
import re
from datetime import date, timedelta
from email.header import decode_header, make_header
from email.message import Message
from typing import Optional

from jobsearch import ats_db as db
from jobsearch.config.settings import settings
from jobsearch.services.email_signal_service import classify_email_signal, signal_resolution_for_existing_application


def _decode_header_value(value: Optional[str]) -> str:
    if not value:
        return ""
    try:
        return str(make_header(decode_header(value)))
    except Exception:
        return str(value)


def _html_to_text(html: str) -> str:
    text = re.sub(r"<[^>]+>", " ", html or "")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _message_text(msg: Message) -> str:
    if msg.is_multipart():
        for part in msg.walk():
            content_type = part.get_content_type()
            disposition = str(part.get("Content-Disposition") or "")
            if "attachment" in disposition.lower():
                continue
            try:
                payload = part.get_payload(decode=True) or b""
                charset = part.get_content_charset() or "utf-8"
                text = payload.decode(charset, errors="ignore")
            except Exception:
                continue
            if content_type == "text/plain" and text.strip():
                return text.strip()
            if content_type == "text/html" and text.strip():
                return _html_to_text(text)
    else:
        try:
            payload = msg.get_payload(decode=True) or b""
            charset = msg.get_content_charset() or "utf-8"
            text = payload.decode(charset, errors="ignore")
        except Exception:
            text = ""
        if msg.get_content_type() == "text/html":
            return _html_to_text(text)
        return text.strip()
    return ""


def _parse_imap_message(raw_message: bytes) -> dict:
    msg = email.message_from_bytes(raw_message)
    return {
        "message_id": _decode_header_value(msg.get("Message-ID")).strip("<>"),
        "thread_id": None,
        "sender": _decode_header_value(msg.get("From")),
        "subject": _decode_header_value(msg.get("Subject")),
        "received_at": _decode_header_value(msg.get("Date")),
        "body": _message_text(msg),
    }


def sync_gmail_email_signals(
    conn,
    days: int = 14,
    max_messages: int = 100,
    *,
    address: Optional[str] = None,
    app_password: Optional[str] = None,
    imap_host: Optional[str] = None,
) -> dict[str, int]:
    address = (address or settings.gmail_address).strip()
    app_password = (app_password or settings.gmail_app_password).strip()
    imap_host = (imap_host or settings.gmail_imap_host).strip() or "imap.gmail.com"
    if not (address and app_password):
        raise RuntimeError("Gmail sync credentials are not configured")

    known_companies = [str(r["company"]) for r in db.get_applications(conn)] + [str(r["name"]) for r in db.get_all_company_profiles(conn)]
    cutoff = (date.today() - timedelta(days=days)).strftime("%d-%b-%Y")
    stats = {"scanned": 0, "classified": 0, "stored": 0}

    mailbox = imaplib.IMAP4_SSL(imap_host)
    try:
        mailbox.login(address, app_password)
        mailbox.select("INBOX")
        status, data = mailbox.search(None, "SINCE", cutoff)
        if status != "OK":
            return stats
        message_ids = [mid for mid in data[0].split() if mid][-max_messages:]
        for message_id in reversed(message_ids):
            status, payload = mailbox.fetch(message_id, "(RFC822)")
            if status != "OK" or not payload:
                continue
            raw_message = b""
            for part in payload:
                if isinstance(part, tuple) and len(part) > 1:
                    raw_message = part[1]
                    break
            if not raw_message:
                continue
            parsed = _parse_imap_message(raw_message)
            stats["scanned"] += 1
            signal = classify_email_signal(
                message_id=parsed["message_id"] or None,
                thread_id=parsed["thread_id"],
                sender=parsed["sender"],
                subject=parsed["subject"],
                body=parsed["body"],
                received_at=parsed["received_at"],
                known_companies=known_companies,
            )
            if not signal:
                continue
            stats["classified"] += 1
            linked = db.find_best_application_match(conn, signal.get("company"), signal.get("role"))
            signal_status = "new"
            notes = signal.get("notes")
            if linked:
                signal_status, auto_note = signal_resolution_for_existing_application(signal["signal_type"], linked["status"])
                notes = auto_note or notes
            db.upsert_email_signal(
                conn,
                **signal,
                application_id=linked["id"] if linked else None,
                signal_status=signal_status,
                notes=notes,
            )
            stats["stored"] += 1
        return stats
    finally:
        try:
            mailbox.logout()
        except Exception:
            pass
