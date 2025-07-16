import socket
import sqlite3
import time
from datetime import datetime
from typing import Dict, List, Optional, Any, Union, Tuple
from app.models.db_core import get_db_connection

def create_events_view(conn=None):
    """Create a view for events based on backup_jobs table"""
    close_conn = False
    if conn is None:
        conn = get_db_connection()
        close_conn = True
    
    try:
        c = conn.cursor()
        
        # First ensure the hostname column exists in backup_sets
        c.execute("PRAGMA table_info(backup_sets)")
        columns = [column[1] for column in c.fetchall()]
        
        if 'hostname' not in columns:
            # Add hostname column if it doesn't exist
            c.execute("ALTER TABLE backup_sets ADD COLUMN hostname TEXT")
            
            # Set default hostname for existing records
            hostname = socket.gethostname()
            c.execute("UPDATE backup_sets SET hostname = ? WHERE hostname IS NULL", (hostname,))
            conn.commit()
        
        # Create the events view - drop it first if it exists to ensure we have the latest definition
        c.execute("DROP VIEW IF EXISTS events")
        
        c.execute("""
        CREATE VIEW events AS
        SELECT 
            bj.id,
            bs.job_name,
            datetime(bj.started_at, 'unixepoch', 'localtime') as starttimestamp,
            bs.hostname,
            bj.backup_type,
            bj.encrypted as encrypt,
            bj.synced as sync,
            bj.status,
            CASE
                WHEN bj.completed_at IS NULL THEN '<i class="fas fa-spinner fa-spin"></i>'
                WHEN bj.runtime_seconds IS NOT NULL THEN 
                    printf('%02d:%02d:%02d', 
                        bj.runtime_seconds / 3600,
                        (bj.runtime_seconds % 3600) / 60, 
                        bj.runtime_seconds % 60)
                ELSE NULL
            END as runtime,
            bj.event_message as event,
            bj.error_message,
            bs.id as backup_set_id,
            bs.set_name,
            bj.started_at as start_time_float
        FROM 
            backup_jobs bj
        JOIN 
            backup_sets bs ON bj.backup_set_id = bs.id
        ORDER BY 
            bj.started_at DESC
        """)
        conn.commit()
    finally:
        if close_conn:
            conn.close()

def get_all_events() -> Dict[str, List[Dict[str, Any]]]:
    """Get all events from the database."""
    with get_db_connection() as conn:
        c = conn.cursor()
        c.execute("SELECT * FROM events")
        rows = c.fetchall()
        events = [dict(row) for row in rows]
        return {"data": events}

def get_event_by_id(event_id: int) -> Optional[Dict[str, Any]]:
    """Get a specific event by ID."""
    with get_db_connection() as conn:
        c = conn.cursor()
        c.execute("SELECT * FROM events WHERE id = ?", (event_id,))
        row = c.fetchone()
        return dict(row) if row else None

def get_event_by_job_name(job_name: str) -> Optional[Dict[str, Any]]:
    """Get the most recent event for a job."""
    with get_db_connection() as conn:
        c = conn.cursor()
        c.execute("""
            SELECT * FROM events 
            WHERE job_name = ? 
            ORDER BY start_time_float DESC 
            LIMIT 1
        """, (job_name,))
        row = c.fetchone()
        return dict(row) if row else None

def get_events_for_job(job_name: str) -> List[Dict[str, Any]]:
    """Get all events for a specific job."""
    with get_db_connection() as conn:
        c = conn.cursor()
        c.execute("""
            SELECT * FROM events 
            WHERE job_name = ? 
            ORDER BY start_time_float DESC
        """, (job_name,))
        return [dict(row) for row in c.fetchall()]

def get_event_status(event_id: int) -> Optional[str]:
    """Get the status of an event."""
    with get_db_connection() as conn:
        c = conn.cursor()
        c.execute("SELECT status FROM events WHERE id = ?", (event_id,))
        row = c.fetchone()
        return row[0] if row else None

def event_exists(event_id: int) -> bool:
    """Check if an event exists."""
    with get_db_connection() as conn:
        c = conn.cursor()
        c.execute("SELECT 1 FROM events WHERE id = ? LIMIT 1", (event_id,))
        return c.fetchone() is not None

# CONSOLIDATED EVENT MANAGEMENT FUNCTIONS

def create_event(job_name: str, 
                 event_message: str,
                 backup_type: str,
                 encrypt: bool = False, 
                 sync: bool = False,
                 config: Dict = None) -> int:
    """
    Create a new event in the database by inserting a backup job.
    
    This replaces initialize_event from event_logger.py with a more direct database approach.
    
    Args:
        job_name: The name of the backup job
        event_message: The initial event message
        backup_type: The type of backup (full, incremental, differential, dryrun, restore)
        encrypt: Whether encryption is enabled
        sync: Whether sync is enabled
        config: Optional configuration settings
        
    Returns:
        The ID of the newly created event/job
    """
    from app.models.backup_sets import get_or_create_backup_set
    from app.models.backup_jobs import insert_backup_job, get_last_full_backup_job
    import json
    
    backup_set_id = None
    
    # For incremental/differential backups, reuse the backup set from the last full backup
    if backup_type.lower() in ['incremental', 'differential', 'diff']:
        # Get the last full backup job for this job name
        last_full_job = get_last_full_backup_job(job_name)
        if last_full_job:
            backup_set_id = last_full_job['backup_set_id']
    
    # If no existing backup set or this is a full/dryrun backup, create a new one
    if not backup_set_id:
        # Generate a set name based on timestamp
        set_name = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Get or create a backup set
        backup_set_id = get_or_create_backup_set(
            job_name=job_name, 
            set_name=set_name,
            config_settings=json.dumps(config) if config else None
        )
    
    # Insert a new backup job
    job_id = insert_backup_job(
        backup_set_id=backup_set_id,
        backup_type=backup_type,
        encrypted=encrypt,
        synced=sync,
        event_message=event_message
    )
    
    return job_id

def update_event(event_id: int, 
                 event_message: Optional[str] = None,
                 backup_type: Optional[str] = None,
                 status: Optional[str] = None) -> bool:
    """
    Update an existing event in the database.
    
    Args:
        event_id: The ID of the event to update
        event_message: New event message (if provided)
        backup_type: New backup type (if provided)
        status: New status (if provided)
        
    Returns:
        True if the update was successful, False otherwise
    """
    if not event_exists(event_id):
        return False
        
    with get_db_connection() as conn:
        c = conn.cursor()
        updates = []
        params = []
        
        if event_message is not None:
            updates.append("event_message = ?")
            params.append(event_message)
            
        if backup_type is not None:
            updates.append("backup_type = ?")
            params.append(backup_type)
            
        if status is not None:
            updates.append("status = ?")
            params.append(status)
            
        if not updates:
            return True  # Nothing to update
            
        query = f"UPDATE backup_jobs SET {', '.join(updates)} WHERE id = ?"
        params.append(event_id)
        c.execute(query, params)
        conn.commit()
        return c.rowcount > 0

def finalize_event(event_id: int, 
                  status: str,
                  event_message: str,
                  runtime: Optional[Union[int, str]] = None,
                  backup_set_id: Optional[str] = None,
                  error_message: Optional[str] = None,
                  total_files: int = 0,
                  total_size_bytes: int = 0) -> bool:
    """
    Finalize an event by updating its status and completion details.
    
    Args:
        event_id: The ID of the event to finalize
        status: The final status (success, error, skipped)
        event_message: The final event message
        runtime: Runtime in seconds or as a string in HH:MM:SS format
        backup_set_id: Optional backup set ID (used in notification)
        error_message: Optional error message if status is error
        total_files: Number of files processed
        total_size_bytes: Total size of files processed in bytes
        
    Returns:
        True if the update was successful, False otherwise
    """
    if not event_exists(event_id):
        return False
        
    # Map status to database status
    job_status = {
        "error": "failed",
        "success": "completed",
        "skipped": "skipped"
    }.get(status, status)
    
    # Calculate runtime in seconds if provided as a string
    runtime_seconds = None
    if runtime is not None:
        if isinstance(runtime, int):
            runtime_seconds = runtime
        elif isinstance(runtime, str) and ":" in runtime:
            try:
                hours, minutes, seconds = runtime.split(":")
                runtime_seconds = int(hours) * 3600 + int(minutes) * 60 + int(seconds)
            except (ValueError, TypeError):
                pass
    
    # If no error message provided but status is error, use event message
    if error_message is None and job_status == "failed":
        error_message = event_message
    
    # Use finalize_backup_job to update the database record
    from app.models.backup_jobs import finalize_backup_job
    
    # Let finalize_backup_job handle the database update
    finalize_backup_job(
        job_id=event_id,
        status=job_status,
        event_message=event_message,
        error_message=error_message,
        total_files=total_files,
        total_size_bytes=total_size_bytes
    )
    
    # Send notifications if needed
    send_event_notification(event_id, status, backup_set_id)
    
    return True

def send_event_notification(event_id: int, status: str, backup_set_id: Optional[str] = None) -> None:
    """
    Send notifications for event completion based on global settings.
    
    Args:
        event_id: The ID of the event
        status: The status of the event (success, error, skipped)
        backup_set_id: Optional backup set ID
    """
    import yaml
    from app.settings import GLOBAL_CONFIG_PATH
    
    # Load the event data
    event_data = get_event_by_id(event_id)
    if not event_data:
        return
    
    # Determine event type for notifications
    event_type = None
    if status == "error":
        event_type = "error"
    elif status == "success":
        backup_type = event_data.get("backup_type", "").lower()
        if backup_type == "restore":
            event_type = "restore_complete"
        else:
            event_type = "backup_complete"
    elif status == "skipped":
        event_type = "backup_complete"
    
    # If we couldn't determine the event type, don't send notification
    if not event_type:
        return
    
    # Check if notifications are enabled for this event type
    try:
        with open(GLOBAL_CONFIG_PATH, "r", encoding="utf-8") as f:
            global_config = yaml.safe_load(f)
        notify_on = global_config.get("email", {}).get("notify_on", {})
    except (OSError, yaml.YAMLError):
        notify_on = {}
    
    # Send email notification if configured
    notify_cfg = notify_on.get(event_type, {})
    if notify_cfg.get("enabled", False):
        from app.services.emailer import process_email_event
        
        hostname = event_data.get('hostname', socket.gethostname())
        job_name = event_data.get('job_name', 'Unknown Job')
        
        subject = f"JABS Notification from {hostname}: {event_type.replace('_', ' ').title()}"
        body = (
            f"Job: {job_name}\n"
            f"Type: {event_data.get('backup_type', 'N/A')}\n"
            f"Status: {event_data.get('status', 'N/A')}\n"
            f"Time: {event_data.get('starttimestamp', 'N/A')}\n"
            f"Runtime: {event_data.get('runtime', 'N/A')}\n"
            f"Message: {event_data.get('event', 'N/A')}\n"
        )
        
        # Add error details if available
        if event_data.get('error_message'):
            body += f"\nError details: {event_data.get('error_message')}\n"
        
        # Call process_email_event with the correct positional arguments
        process_email_event(
            event_type,  # First positional argument: event_type
            subject,     # Second positional argument: subject
            body,        # Third positional argument: body
            False        # Fourth positional argument: html=False
        )
        
    # Record scheduler event if needed
    if event_type in ["backup_complete", "error"]:
        from app.utils.scheduler_events import append_scheduler_event
        # Fix: update parameters to match what append_scheduler_event expects
        current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        append_scheduler_event(
            datetime=current_datetime,
            job_name=event_data.get('job_name', 'Unknown Job'),
            backup_type=event_data.get('backup_type', 'N/A'),
            status=status
        )

def remove_events_by_backup_set_id(backup_set_id: int) -> bool:
    """
    Remove events by deleting their underlying backup jobs.
    Returns True if any events were removed.
    """
    with get_db_connection() as conn:
        c = conn.cursor()
        c.execute("DELETE FROM backup_jobs WHERE backup_set_id = ?", (backup_set_id,))
        conn.commit()
        return c.rowcount > 0

def delete_event(event_id: int) -> bool:
    """
    Delete an event from the database.
    
    Args:
        event_id: The ID of the event to delete
        
    Returns:
        True if the deletion was successful, False otherwise
    """
    with get_db_connection() as conn:
        c = conn.cursor()
        c.execute("DELETE FROM backup_jobs WHERE id = ?", (event_id,))
        conn.commit()
        return c.rowcount > 0

def delete_events(event_ids: List[int]) -> int:
    """
    Delete multiple events from the database.
    
    Args:
        event_ids: A list of event IDs to delete
        
    Returns:
        Number of events deleted
    """
    if not event_ids:
        return 0
        
    with get_db_connection() as conn:
        c = conn.cursor()
        placeholders = ','.join('?' for _ in event_ids)
        c.execute(f"DELETE FROM backup_jobs WHERE id IN ({placeholders})", event_ids)
        conn.commit()
        return c.rowcount

def get_event_count_by_status(status: str) -> int:
    """
    Get the count of events with a specific status.
    
    Args:
        status: The status to count (error, completed, skipped, etc.)
        
    Returns:
        Number of events with the specified status
    """
    with get_db_connection() as conn:
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM events WHERE status = ?", (status,))
        return c.fetchone()[0]

def count_error_events() -> int:
    """
    Count the number of events with status 'error' in the database.
    
    Returns:
        Number of events with error status
    """
    return get_event_count_by_status('error')