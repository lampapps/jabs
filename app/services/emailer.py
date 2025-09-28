"""Email utility functions for sending notifications."""

import os
import smtplib
import threading
import socket
from datetime import datetime
from collections import Counter
from email.mime.text import MIMEText
from dotenv import load_dotenv
from dateutil.parser import parse as parse_dt

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

    # Log email size for debugging
    body_size = len(body.encode('utf-8'))
    email_logger.info(f"Preparing to send email: '{subject}' (size: {body_size} bytes)")
    
    # Log current thread info for debugging
    import threading
    current_thread = threading.current_thread()
    email_logger.debug(f"Sending email from thread: {current_thread.name} (ID: {current_thread.ident})")
    
    # Debug: Check for problematic characters
    try:
        # Test subject encoding
        subject_encoded = subject.encode('utf-8')
        email_logger.debug(f"Subject encoding test passed: {len(subject_encoded)} bytes")
        
        # Test body encoding
        body_encoded = body.encode('utf-8')
        email_logger.debug(f"Body encoding test passed: {len(body_encoded)} bytes")
        
        # Check for common problematic characters
        problematic_chars = ['\x00', '\r\n\r\n', '\n.\n']
        for char in problematic_chars:
            if char in subject or char in body:
                email_logger.warning(f"Found potentially problematic character in email content: {repr(char)}")
                
    except UnicodeEncodeError as e:
        email_logger.error(f"Unicode encoding error in email content: {e}")
        return False

    msg_type = "html" if html else "plain"
    
    try:
        msg = MIMEText(body, msg_type)
        email_logger.debug(f"MIMEText object created successfully")
        
        msg['Subject'] = subject
        msg['From'] = username
        msg['To'] = ", ".join(EMAIL_CONFIG['to_addrs'])
        email_logger.debug(f"Email headers set successfully")
        
        # Test message string conversion
        msg_string = msg.as_string()
        email_logger.debug(f"Email message converted to string: {len(msg_string)} bytes")
        
    except Exception as e:
        email_logger.error(f"Error creating email message: {e}")
        return False

    # Try sending with retry logic for TLS issues
    max_retries = 2
    for attempt in range(max_retries):
        server = None
        try:
            # Set socket timeout for SMTP operations
            original_timeout = socket.getdefaulttimeout()
            socket.setdefaulttimeout(5)  # Very short timeout to force quick failure
            email_logger.debug(f"Socket timeout set to 5 seconds (attempt {attempt + 1})")
            
            try:
                email_logger.debug(f"Creating SMTP connection to {EMAIL_CONFIG['smtp_server']}:{EMAIL_CONFIG['smtp_port']} (attempt {attempt + 1})")
                
                # Use a fresh connection for each attempt
                server = smtplib.SMTP(EMAIL_CONFIG['smtp_server'], EMAIL_CONFIG['smtp_port'])
                email_logger.debug(f"SMTP connection established (attempt {attempt + 1})")
                
                # Set individual timeout for the server socket
                server.sock.settimeout(5)
                email_logger.debug(f"Server socket timeout set to 5 seconds (attempt {attempt + 1})")
                
                server.set_debuglevel(0)  # Disable SMTP debug output
                
                if EMAIL_CONFIG.get('use_tls'):
                    email_logger.debug(f"Starting TLS... (attempt {attempt + 1})")
                    
                    # Create SSL context with minimal settings
                    import ssl
                    ssl_context = ssl.create_default_context()
                    ssl_context.check_hostname = False
                    ssl_context.verify_mode = ssl.CERT_NONE
                    
                    # Use thread-local TLS without signals
                    server.starttls(context=ssl_context)
                    email_logger.debug(f"TLS established successfully (attempt {attempt + 1})")
                    
                    email_logger.debug(f"Logging in... (attempt {attempt + 1})")
                    server.login(username, password)
                    email_logger.debug(f"SMTP login successful (attempt {attempt + 1})")
                else:
                    email_logger.debug(f"No TLS configured (attempt {attempt + 1})")
                
                email_logger.debug(f"Sending email via SMTP... (attempt {attempt + 1})")
                server.sendmail(
                    username,
                    EMAIL_CONFIG['to_addrs'],
                    msg_string
                )
                email_logger.debug(f"SMTP sendmail completed successfully (attempt {attempt + 1})")
                        
            finally:
                # Always close the connection cleanly
                if server:
                    try:
                        server.quit()
                        email_logger.debug(f"SMTP connection closed cleanly (attempt {attempt + 1})")
                    except:
                        try:
                            server.close()
                            email_logger.debug(f"SMTP connection force closed (attempt {attempt + 1})")
                        except:
                            pass
                            
                # Restore original timeout
                socket.setdefaulttimeout(original_timeout)
                email_logger.debug(f"Socket timeout restored (attempt {attempt + 1})")
                
            # If we get here, the email was sent successfully
            email_logger.info(f"Email sent: '{subject}' to {EMAIL_CONFIG['to_addrs']} (size: {body_size} bytes)")
            return True
            
        except (socket.timeout, TimeoutError):
            email_logger.warning(f"Timeout on attempt {attempt + 1} sending email '{subject}' (size: {body_size} bytes)")
            if attempt == max_retries - 1:
                email_logger.error(f"All {max_retries} attempts timed out sending email '{subject}'")
                return False
            else:
                email_logger.info(f"Retrying email send in 3 seconds...")
                import time
                time.sleep(3)  # Longer delay before retry
                
        except smtplib.SMTPException as smtp_exc:
            email_logger.warning(f"SMTP error on attempt {attempt + 1} sending email '{subject}': {smtp_exc}")
            if attempt == max_retries - 1:
                email_logger.error(f"All {max_retries} attempts failed with SMTP errors sending email '{subject}'")
                return False
            else:
                email_logger.info(f"Retrying email send in 3 seconds...")
                import time
                time.sleep(3)
                
        except OSError as os_exc:
            email_logger.warning(f"Network error on attempt {attempt + 1} sending email '{subject}': {os_exc}")
            if attempt == max_retries - 1:
                email_logger.error(f"All {max_retries} attempts failed with network errors sending email '{subject}'")
                return False
            else:
                email_logger.info(f"Retrying email send in 3 seconds...")
                import time
                time.sleep(3)
                
        except Exception as exc:
            email_logger.error(f"Unexpected error on attempt {attempt + 1} sending email '{subject}': {exc}")
            import traceback
            email_logger.error(f"Full traceback: {traceback.format_exc()}")
            return False  # Don't retry unexpected errors
    
    return False  # Should never reach here

def _queue_email(subject, body, html=False, event_type=None):
    """Queue an email for later digest sending using the database."""
    # No need for a lock when using the database
    queue_email_digest(subject, body, html, event_type)

def send_email_digest():
    """Send all queued emails as a single digest and clear the queue."""
    with EMAIL_DIGEST_LOCK:
        email_logger.info("Starting digest email processing")
        
        queue = get_email_digest_queue()
        email_logger.info(f"Found {len(queue)} queued emails for digest")
        
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
            
        # Log digest size for debugging
        digest_size = len(digest_body.encode('utf-8'))
        email_logger.info(f"Attempting to send digest email with subject: {digest_subject}")
        email_logger.info(f"Digest email size: {digest_size} bytes ({len(queue)} emails)")
        email_logger.debug(f"Digest body preview: {digest_body[:200]}...")
        
        # Add size protection (limit to ~1MB)
        max_size = 1024 * 1024  # 1MB
        if digest_size > max_size:
            email_logger.warning(f"Digest email size ({digest_size} bytes) exceeds limit ({max_size} bytes), truncating...")
            digest_body = digest_body[:max_size - 1000] + "\n\n[EMAIL TRUNCATED DUE TO SIZE LIMIT]"
        
        # TEST: Try using the exact same call pattern as immediate emails
        email_logger.info("Testing immediate email pattern from digest thread...")
        
        # First try: Use the exact same call signature as immediate emails
        html = False  # Explicit variable like in process_email_event
        success = _send_email(digest_subject, digest_body, html)
        
        if not success:
            email_logger.warning("First attempt failed, trying alternative approach...")
            # Second try: Use process_email_event pattern (but this would queue it again)
            # Let's try a simple test email instead
            test_subject = "JABS Test from Digest Thread"
            test_body = "This is a test email sent from the digest thread to verify threading works."
            success = _send_email(test_subject, test_body, False)
            
            if success:
                email_logger.info("Test email from digest thread succeeded! Issue is with digest content.")
                # Try the original digest again
                success = _send_email(digest_subject, digest_body, False)
            else:
                email_logger.error("Test email from digest thread also failed! Issue is with threading.")
            
        if success:
            email_logger.info("Digest email sent successfully, clearing queue")
            # Clear the queue in the database
            clear_email_digest_queue()
        else:
            email_logger.error("Failed to send digest email, keeping queue")
            
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
