"""Event logging utilities for JABS."""

import socket
import time
from datetime import datetime
import yaml
from app.settings import GLOBAL_CONFIG_PATH
from app.utils.scheduler_events import append_scheduler_event
from app.utils.emailer import process_email_event
from app.models.manifest_db import (
    get_all_events, get_event_by_id, get_event_by_job_name, 
    get_events_for_job, get_event_status, event_exists,
    remove_events_by_backup_set_id
)

def generate_event_id(job_name):
    """
    Generate a placeholder event ID for compatibility with existing code.
    This will be replaced by the actual database ID.
    """
    return f"{job_name}_{datetime.now().strftime('%Y%m%d%H%M%S')}"

def initialize_event(job_name, event, backup_type, encrypt=False, sync=False):
    """Initialize a new event by creating a backup job."""
    from app.models.manifest_db import (
        get_or_create_backup_set, insert_backup_job, get_last_full_backup_job
    )
    
    # Only create a new backup set for full backups or dryruns
    # For incremental or differential, reuse the existing backup set from the last full backup
    backup_set_id = None
    
    if backup_type.lower() in ['incremental', 'differential', 'diff']:
        # Get the last full backup job for this job name
        last_full_job = get_last_full_backup_job(job_name)
        
        if last_full_job:
            # Use the existing backup set ID
            backup_set_id = last_full_job['backup_set_id']
            
            # If we found a backup set, no need to create a new one
            if backup_set_id:
                print(f"Using existing backup set {backup_set_id} for {backup_type} backup")
    
    # If no existing backup set was found or this is a full/dryrun backup, create a new one
    if not backup_set_id:
        # Generate a set name based on timestamp
        set_name = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Get or create a backup set
        backup_set_id = get_or_create_backup_set(job_name, set_name)
    
    # Insert a new backup job
    job_id = insert_backup_job(
        backup_set_id=backup_set_id,
        backup_type=backup_type,
        encrypted=encrypt,
        synced=sync,
        event_message=event
    )
    
    # Return the job ID as the event ID
    return job_id

def update_event(event_id, backup_type=None, event=None, status=None, url=None):
    """Update an event by updating the backup job."""
    from app.models.manifest_db import get_db_connection
    
    with get_db_connection() as conn:
        c = conn.cursor()
        updates = []
        params = []
        
        if event:
            updates.append("event_message = ?")
            params.append(event)
            
        if backup_type:
            updates.append("backup_type = ?")
            params.append(backup_type)
            
        if status:
            updates.append("status = ?")
            params.append(status)
            
        if updates:
            query = f"UPDATE backup_jobs SET {', '.join(updates)} WHERE id = ?"
            params.append(event_id)
            c.execute(query, params)
            conn.commit()

def finalize_event(event_id, status, event, runtime=None, backup_set_id=None, event_type=None):
    """Finalize an event by updating the backup job."""
    from app.models.manifest_db import finalize_backup_job, get_backup_job
    
    # Calculate runtime in seconds if provided as a string
    runtime_seconds = None
    if runtime and isinstance(runtime, str) and ":" in runtime:
        try:
            hours, minutes, seconds = runtime.split(":")
            runtime_seconds = int(hours) * 3600 + int(minutes) * 60 + int(seconds)
        except (ValueError, TypeError):
            pass
    
    # Map status to backup job status
    job_status = {
        "error": "failed",
        "success": "completed",
        "skipped": "completed"
    }.get(status, status)
    
    # Update the backup job
    finalize_backup_job(
        job_id=event_id,
        status=job_status,
        event_message=event,
        error_message=event if job_status == "failed" else None,
        completed_at=time.time()
    )
    
    # Load the updated event data
    event_data = get_event_by_id(event_id)
    if not event_data:
        return
    
    # Determine event type for notifications if not provided
    if not event_type:
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

    # --- Email notification logic based on notify_on settings ---
    try:
        with open(GLOBAL_CONFIG_PATH, "r", encoding="utf-8") as f:
            global_config = yaml.safe_load(f)
        notify_on = global_config.get("email", {}).get("notify_on", {})
    except (OSError, yaml.YAMLError):
        notify_on = {}
    
    # Send email notification if configured
    notify_cfg = notify_on.get(event_type, {})
    if notify_cfg.get("enabled", False):
        subject = f"JABS Notification from {event_data.get('hostname', 'N/A')}: {event_type.replace('_', ' ').title()}"
        body = (
            f"Start Time: {event_data.get('starttimestamp', 'N/A')}\n"
            f"Job Name: {event_data.get('job_name', 'N/A')}\n"
            f"Type: {event_data.get('backup_type', 'N/A')}\n"
            f"Event: {event}\n"
            f"Status: {status}\n"
            f"Runtime: {event_data.get('runtime', 'N/A')}\n"
        )
        process_email_event(event_type, subject, body)

    # --- Scheduler mini chart logging ---
    append_scheduler_event(
        datetime=event_data.get("starttimestamp", datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
        job_name=event_data.get("job_name"),
        backup_type=event_data.get("backup_type"),
        status=status
    )

def remove_event_by_backup_set_id(backup_set_id, logger):
    """Remove events by deleting their underlying backup jobs."""
    success = remove_events_by_backup_set_id(backup_set_id)
    if success:
        logger.info(f"Removed events for backup set ID: {backup_set_id}")
    else:
        logger.warning(f"No events found with backup_set_id: {backup_set_id}")

def load_events():
    """Get all events from the database."""
    return get_all_events()

