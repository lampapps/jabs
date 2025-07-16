"""Backup dispatcher module for JABS."""

import os
from .full import run_full_backup
from .diff import run_diff_backup
from .incremental import run_incremental_backup
from .dryrun import run_dryrun_backup
from .common import acquire_lock, release_lock
from app.settings import LOCK_DIR
from app.models.events import finalize_event, update_event

def run_backup(config, backup_type, **kwargs):
    """Dispatches backup jobs to the appropriate backup type handler, with job-level locking."""
    job_name = config.get("job_name", "unknown_job")
    event_id = kwargs.get("event_id")
    lock_path = os.path.join(LOCK_DIR, f"{job_name}.lock")
    lock_file = None
    
    # Update the event to show we're acquiring a lock
    if event_id:
        update_event(event_id, event_message=f"Acquiring lock for {job_name}")
    
    try:
        lock_file = acquire_lock(lock_path)
    except RuntimeError as e:
        # Only finalize the event in case of lock errors
        if event_id:
            finalize_event(
                event_id=event_id,
                status="skipped",
                event_message=f"Backup job '{job_name}' is already running or locked.",
                backup_set_id=None,
                runtime="00:00:00"
            )
        raise RuntimeError(f"Backup job '{job_name}' is already running or locked. {e}")

    try:
        # Pass the event_id to the appropriate backup module
        # Each module will UPDATE the event but NOT finalize it (except for errors)
        if backup_type == "full":
            return run_full_backup(config, **kwargs)
        if backup_type in ("diff", "differential"):
            return run_diff_backup(config, **kwargs)
        if backup_type == "incremental":
            return run_incremental_backup(config, **kwargs)
        if backup_type in ("dry_run", "dryrun"):
            return run_dryrun_backup(config, **kwargs)
        raise ValueError(f"Unsupported backup type: {backup_type}")
    finally:
        if lock_file:
            release_lock(lock_file)
