import socket
import time
import sqlite3
from typing import List, Dict, Optional, Any
from app.models.db_core import get_db_connection

def get_or_create_backup_set(job_name: str, set_name: str, config_settings: Optional[str] = None) -> int:
    """Get existing backup set or create new one if it doesn't exist."""
    with get_db_connection() as conn:
        c = conn.cursor()
        
        # Try to get existing backup set
        c.execute("SELECT id FROM backup_sets WHERE job_name = ? AND set_name = ?", (job_name, set_name))
        row = c.fetchone()
        
        if row:
            # Update the updated_at timestamp
            c.execute("UPDATE backup_sets SET updated_at = ? WHERE id = ?", (time.time(), row['id']))
            conn.commit()
            return row['id']
        else:
            # Create new backup set
            current_time = time.time()
            hostname = socket.gethostname()
            c.execute("""
                INSERT INTO backup_sets (job_name, set_name, created_at, updated_at, config_snapshot, hostname)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (job_name, set_name, current_time, current_time, config_settings, hostname))
            conn.commit()
            return c.lastrowid

def get_backup_set_by_job_and_set(job_name: str, set_name: str) -> Optional[sqlite3.Row]:
    """Get a backup set by job_name and set_name."""
    with get_db_connection() as conn:
        c = conn.cursor()
        c.execute("SELECT * FROM backup_sets WHERE job_name = ? AND set_name = ?", (job_name, set_name))
        return c.fetchone()

def get_backup_set(set_id: int) -> Optional[sqlite3.Row]:
    """Get a backup set by numeric ID."""
    with get_db_connection() as conn:
        c = conn.cursor()
        c.execute("SELECT * FROM backup_sets WHERE id = ?", (set_id,))
        return c.fetchone()

def list_backup_sets(job_name: Optional[str] = None, limit: int = 20) -> List[sqlite3.Row]:
    """List backup sets, optionally filtered by job_name."""
    with get_db_connection() as conn:
        c = conn.cursor()
        if job_name:
            c.execute("""
                SELECT * FROM backup_sets 
                WHERE job_name = ?
                ORDER BY created_at DESC LIMIT ?
            """, (job_name, limit))
        else:
            c.execute("""
                SELECT * FROM backup_sets 
                ORDER BY created_at DESC LIMIT ?
            """, (limit,))
        return c.fetchall()

def delete_backup_set(set_id: int):
    """Delete a backup set and all its jobs and files (cascading)."""
    with get_db_connection() as conn:
        c = conn.cursor()
        c.execute("DELETE FROM backup_sets WHERE id = ?", (set_id,))
        conn.commit()