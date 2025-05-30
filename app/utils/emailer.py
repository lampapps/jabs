import os
import smtplib
from email.mime.text import MIMEText
from dotenv import load_dotenv 
from app.settings import EMAIL_CONFIG
from app.utils.logger import setup_logger

# Set up a dedicated logger for email notifications
email_logger = setup_logger("email", log_file="email.log")

load_dotenv()

def send_email(subject, body, html=False):
    password = os.environ.get("JABS_EMAIL_PASSWORD")
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
    except Exception as e:
        email_logger.error(f"Failed to send email '{subject}' to {EMAIL_CONFIG['to_addrs']}: {e}")
        return False

def email_event(event_type, subject, body, html=False):
    """
    Send an email notification if the event_type is enabled in config.
    event_type: string, e.g. 'error', 'job_complete', etc.
    """
    notify_on = EMAIL_CONFIG.get('notify_on', {})
    if notify_on.get(event_type, False):
        return send_email(subject, body, html=html)
    else:
        email_logger.info(f"Notification for event '{event_type}' is disabled in config.")
        return False