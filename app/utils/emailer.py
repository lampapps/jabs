"""Email utility functions for sending notifications."""

import os
import smtplib
import json
import threading
from datetime import datetime
from email.mime.text import MIMEText
from dotenv import load_dotenv
from app.settings import EMAIL_CONFIG, DATA_DIR
from app.utils.logger import setup_logger

# Set up a dedicated logger for email notifications
email_logger = setup_logger("email", log_file="email.log")

load_dotenv()

EMAIL_DIGEST_FILE = os.path.join(DATA_DIR, "email_digest_queue.json")  # Store digest in data folder
EMAIL_DIGEST_LOCK = threading.Lock()

def send_email(subject, body, html=False):
    """
    Send an email with the given subject and body.
    If html is True, send as HTML email.
    """
    password = os.environ.get("JABS_EMAIL_PASSWORD")
    app_env = os.environ.get("APP_ENV", "").lower()
    if app_env == "development":
        subject = f"[DEV] {subject}"
    if not password:
        email_logger.error("No email password found in environment variable JABS_EMAIL_PASSWORD.")
        return False

    msg_type = "html" if html else "plain"
    msg = MIMEText(body, msg_type)
    msg['Subject'] = subject
    msg['From'] = EMAIL_CONFIG['from_addr']
    msg['To'] = ", ".join(EMAIL_CONFIG['to_addrs'])

    try:
        with smtplib.SMTP(EMAIL_CONFIG['smtp_server'], EMAIL_CONFIG['smtp_port']) as server:
            if EMAIL_CONFIG.get('use_tls'):
                server.starttls()
            server.login(EMAIL_CONFIG['username'], password)
            server.sendmail(
                EMAIL_CONFIG['from_addr'],
                EMAIL_CONFIG['to_addrs'],
                msg.as_string()
            )
        email_logger.info(f"Email sent: '{subject}' to {EMAIL_CONFIG['to_addrs']}")
        return True
    except (smtplib.SMTPException, OSError) as exc:
        email_logger.error(f"Failed to send email '{subject}' to {EMAIL_CONFIG['to_addrs']}: {exc}")
        return False

def queue_email(subject, body, html=False, event_type=None):
    """Queue an email for later digest sending."""
    with EMAIL_DIGEST_LOCK:
        if os.path.exists(EMAIL_DIGEST_FILE):
            with open(EMAIL_DIGEST_FILE, "r", encoding="utf-8") as f:
                queue = json.load(f)
        else:
            queue = []
        queue.append({
            "timestamp": datetime.now().isoformat(),
            "subject": subject,
            "body": body,
            "html": html,
            "event_type": event_type,
        })
        with open(EMAIL_DIGEST_FILE, "w", encoding="utf-8") as f:
            json.dump(queue, f)

def send_email_digest():
    """Send all queued emails as a single digest and clear the queue."""
    with EMAIL_DIGEST_LOCK:
        if not os.path.exists(EMAIL_DIGEST_FILE):
            email_logger.info("No queued emails to send in digest.")
            return False
        with open(EMAIL_DIGEST_FILE, "r", encoding="utf-8") as f:
            queue = json.load(f)
        if not queue:
            return False
        digest_subject = f"JABS Daily Digest ({datetime.now().strftime('%Y-%m-%d')})"
        digest_body = ""
        for item in queue:
            digest_body += (
                f"\n---\nTime: {item['timestamp']}\n"
                f"Type: {item.get('event_type','')}\n"
                f"Subject: {item['subject']}\n{item['body']}\n"
            )
        success = send_email(digest_subject, digest_body, html=False)
        if success:
            os.remove(EMAIL_DIGEST_FILE)
        return success

def email_event(event_type, subject, body, html=False):
    """
    Send an email notification if the event_type is enabled in config.
    Uses per-event mode: immediate or digest.
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
        queue_email(subject, body, html, event_type=event_type)
        email_logger.info(f"Queued email for digest: '{subject}' (type: {event_type})")
        return True
    return send_email(subject, body, html)
