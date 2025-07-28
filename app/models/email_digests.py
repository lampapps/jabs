"""Email digest database model for JABS."""

import json
from datetime import datetime
from typing import List, Dict, Optional, Any
from app.models.db_core import get_db_connection

def init_email_digests_table(cursor):
    """Create the email_digests table."""
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS email_digests (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT NOT NULL,          -- ISO format timestamp
        subject TEXT NOT NULL,
        body TEXT NOT NULL,
        html BOOLEAN DEFAULT 0,
        event_type TEXT
    )
    """)

def queue_email_digest(subject: str, body: str, html: bool = False, event_type: Optional[str] = None) -> int:
    """Add an email to the digest queue in the database."""
    with get_db_connection() as conn:
        c = conn.cursor()
        timestamp = datetime.now().isoformat()
        c.execute("""
            INSERT INTO email_digests (
                timestamp, subject, body, html, event_type
            )
            VALUES (?, ?, ?, ?, ?)
        """, (timestamp, subject, body, html, event_type))
        conn.commit()
        return c.lastrowid

def get_email_digest_queue() -> List[Dict[str, Any]]:
    """Get all email digests in the queue."""
    with get_db_connection() as conn:
        c = conn.cursor()
        c.execute("SELECT * FROM email_digests ORDER BY timestamp ASC")
        return [dict(row) for row in c.fetchall()]

def clear_email_digest_queue() -> None:
    """Clear all email digests from the queue."""
    with get_db_connection() as conn:
        c = conn.cursor()
        c.execute("DELETE FROM email_digests")
        conn.commit()

def import_from_json(json_path: str) -> int:
    """Import email digests from a JSON file."""
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            digests = json.load(f)
        
        with get_db_connection() as conn:
            c = conn.cursor()
            count = 0
            for digest in digests:
                c.execute("""
                    INSERT INTO email_digests (
                        timestamp, subject, body, html, event_type
                    )
                    VALUES (?, ?, ?, ?, ?)
                """, (
                    digest.get('timestamp', datetime.now().isoformat()),
                    digest.get('subject', ''),
                    digest.get('body', ''),
                    digest.get('html', False),
                    digest.get('event_type', None)
                ))
                count += 1
            conn.commit()
        return count
    except (json.JSONDecodeError, FileNotFoundError):
        return 0