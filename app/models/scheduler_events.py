"""Scheduler event database utilities for JABS.

Provides functions to append, retrieve, and trim scheduler event records.
"""

from app.models.db_core import get_db_connection
from app.settings import MAX_SCHEDULER_EVENTS

def append_scheduler_event(datetime, job_name, backup_type, status):
    """Insert a new scheduler event record into the database."""
    with get_db_connection() as conn:
        c = conn.cursor()
        c.execute("""
            INSERT INTO scheduler_events (datetime, job_name, backup_type, status)
            VALUES (?, ?, ?, ?)
        """, (datetime, job_name, backup_type, status))
        conn.commit()

def get_scheduler_events(limit=MAX_SCHEDULER_EVENTS):
    """Retrieve the most recent scheduler event records, up to the specified limit."""
    with get_db_connection() as conn:
        c = conn.cursor()
        c.execute("""
            SELECT datetime, job_name, backup_type, status
            FROM scheduler_events
            ORDER BY id DESC
            LIMIT ?
        """, (limit,))
        return [dict(row) for row in c.fetchall()]

def trim_scheduler_events(max_events=MAX_SCHEDULER_EVENTS):
    """
    Trim the scheduler_events table to keep only the newest max_events records.
    """
    with get_db_connection() as conn:
        c = conn.cursor()
        # Find the id threshold: the id of the Nth newest event
        c.execute("""
            SELECT id FROM scheduler_events
            ORDER BY id DESC
            LIMIT 1 OFFSET ?
        """, (max_events - 1,))
        row = c.fetchone()
        if row:
            threshold_id = row["id"]
            # Delete all events with id less than the threshold
            c.execute("""
                DELETE FROM scheduler_events
                WHERE id < ?
            """, (threshold_id,))
            conn.commit()
