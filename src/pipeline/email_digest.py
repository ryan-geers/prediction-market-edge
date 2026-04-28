"""SMTP helper for weekly digest; supports dry-run (no socket)."""
from __future__ import annotations

import os
import smtplib
from email.mime.text import MIMEText

from src.core.config import Settings


def send_weekly_digest_email(
    body_text: str,
    settings: Settings,
    *,
    subject: str = "Prediction Market Edge — weekly digest",
) -> None:
    """
    Send plain-text digest. If ``settings.email_dry_run`` is True, prints a preview and returns.
    """
    if settings.email_dry_run:
        preview = body_text if len(body_text) <= 4000 else body_text[:4000] + "\n… [truncated for dry-run preview]"
        print("[EMAIL_DRY_RUN] Skipping SMTP. Subject:", subject)
        print(preview)
        return

    host = settings.smtp_host or os.environ.get("SMTP_HOST")
    port = int(settings.smtp_port or os.environ.get("SMTP_PORT") or "587")
    user = settings.smtp_user or os.environ.get("SMTP_USER")
    password = settings.smtp_pass or os.environ.get("SMTP_PASS")
    email_from = settings.email_from or os.environ.get("EMAIL_FROM")
    email_to = settings.email_to or os.environ.get("EMAIL_TO")

    missing = [n for n, v in [
        ("SMTP_HOST", host),
        ("SMTP_USER", user),
        ("SMTP_PASS", password),
        ("EMAIL_FROM", email_from),
        ("EMAIL_TO", email_to),
    ] if not v]
    if missing:
        raise ValueError(f"Missing email/SMTP config: {', '.join(missing)} (env or Settings)")

    msg = MIMEText(body_text, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"] = email_from  # type: ignore[arg-type]
    msg["To"] = email_to  # type: ignore[arg-type]

    with smtplib.SMTP(host, port) as server:  # type: ignore[arg-type]
        server.starttls()
        server.login(user, password)  # type: ignore[arg-type]
        server.send_message(msg)
