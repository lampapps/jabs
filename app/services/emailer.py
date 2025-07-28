"""Email utility functions for sending notifications."""

import os
import smtplib
import threading
import socket
from datetime import datetime
from email.mime.text import MIMEText
from dotenv import load_dotenv
from collections import Counter

from app.settings import EMAIL_CONFIG, ENV_PATH, ENV_MODE
from app.utils.logger import setup_logger
from app.models.email_digests import queue_email_digest, get_email_digest_queue, clear_email_digest_queue

# Set up a dedicated logger for email notifications
email_logger = setup_logger("email", log_file="email.log")

# Use the ENV_PATH from settings
load_dotenv(ENV_PATH)

EMAIL_DIGEST_LOCK = threading.Lock()

def _get_smtp_credentials():
    """Fetch SMTP username and password from environment variables."""
    username = os.environ.get("JABS_SMTP_USERNAME")
    password = os.environ.get("JABS_SMTP_PASSWORD")
    return username, password

def _send_email(subject, body, html=False):
    """
    Send an email with the given subject and body.
    If html is True, send as HTML email.
    """
    username, password = _get_smtp_credentials()
    env_mode = ENV_MODE
    if env_mode == "development":
        subject = f"[DEV] {subject}"
    if not username:
        email_logger.error("No SMTP username found in environment variable JABS_SMTP_USERNAME.")
        return False
    if not password:
        email_logger.error("No SMTP password found in environment variable JABS_SMTP_PASSWORD.")
        return False

    msg_type = "html" if html else "plain"
    msg = MIMEText(body, msg_type)
    msg['Subject'] = subject
    msg['From'] = username
    msg['To'] = ", ".join(EMAIL_CONFIG['to_addrs'])

    try:
        with smtplib.SMTP(EMAIL_CONFIG['smtp_server'], EMAIL_CONFIG['smtp_port']) as server:
            if EMAIL_CONFIG.get('use_tls'):
                server.starttls()
                server.login(username, password)
                server.sendmail(
                    username,
                    EMAIL_CONFIG['to_addrs'],
                    msg.as_string()
                )
        email_logger.info(f"Email sent: '{subject}' to {EMAIL_CONFIG['to_addrs']}")
        return True
    except (smtplib.SMTPException, OSError) as exc:
        email_logger.error(f"Failed to send email '{subject}' to {EMAIL_CONFIG['to_addrs']}: {exc}")
        return False

def _queue_email(subject, body, html=False, event_type=None):
    """Queue an email for later digest sending using the database."""
    # No need for a lock when using the database
    queue_email_digest(subject, body, html, event_type)

def send_email_digest():
    """Send all queued emails as a single digest and clear the queue."""
    with EMAIL_DIGEST_LOCK:
        queue = get_email_digest_queue()
        if not queue:
            email_logger.info("No queued emails to send in digest.")
            return False

        # --- Build summary of event types ---
        event_types = [item.get("event_type") for item in queue]
        event_types = [et for et in event_types if et]  # Filter out None values
        counts = Counter(event_types)
        summary_lines = [
            "Digest Summary:",
            f"Host: {socket.gethostname()}",
        ]
        for event_type, count in counts.items():
            summary_lines.append(f"  {event_type}: {count}")

        # --- Calculate time frame ---
        from dateutil.parser import parse as parse_dt
        timestamps = [parse_dt(item["timestamp"]) for item in queue if "timestamp" in item]
        if timestamps:
            start_time = min(timestamps)
            end_time = max(timestamps)
            time_frame = (
                "Digest covers:\n"
                f"  Start: {start_time.strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"  End:   {end_time.strftime('%Y-%m-%d %H:%M:%S')}\n"
            )
        else:
            time_frame = "Digest covers: (time frame unavailable)\n"

        summary = "\n".join(summary_lines) + "\n" + time_frame + "\n"

        digest_subject = f"JABS Daily Digest from {socket.gethostname()} ({datetime.now().strftime('%Y-%m-%d')})"
        digest_body = summary
        for item in queue:
            digest_body += (
                f"\n---\n"
                f"{item.get('body', '').strip()}\n"
            )
        success = _send_email(digest_subject, digest_body, html=False)
        if success:
            # Clear the queue in the database
            clear_email_digest_queue()
        return success

def process_email_event(event_type, subject, body, html=False):
    """
    Send an email notification if the event_type is enabled in config.
    Uses per-event mode: immediate or digest.
    If mode is immediate, send and also queue for digest.
    """
    notify_on = EMAIL_CONFIG.get('notify_on', {})
    event_cfg = notify_on.get(event_type, {})
    enabled = False
    mode = "immediate"
    if isinstance(event_cfg, dict):
        enabled = event_cfg.get("enabled", False)
        mode = event_cfg.get("mode", "immediate")
    elif isinstance(event_cfg, bool):
        enabled = event_cfg
        mode = "immediate"
    if not enabled:
        email_logger.info(f"Notification for event '{event_type}' is disabled in config.")
        return False
    if mode == "digest":
        _queue_email(subject, body, html, event_type=event_type)
        email_logger.debug(f"Queued email for digest: '{subject}' (type: {event_type})")
        return True
    # If mode is immediate, send and also queue
    sent = _send_email(subject, body, html)
    _queue_email(subject, body, html, event_type=event_type)
    email_logger.info(f"Sent and queued immediate email: '{subject}' (type: {event_type})")
    return sent
