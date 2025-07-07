"""Backup dispatcher module for JABS."""

from .full import run_full_backup
from .diff import run_diff_backup
from .incremental import run_incremental_backup
from .dryrun import run_dryrun_backup
from .common import acquire_lock, release_lock
import os
from app.settings import LOCK_DIR

def run_backup(config, backup_type, **kwargs):
    """Dispatches backup jobs to the appropriate backup type handler, with job-level locking."""
    job_name = config.get("job_name", "unknown_job")
    event_id = kwargs.get("event_id")
    lock_path = os.path.join(LOCK_DIR, f"{job_name}.lock")
    lock_file = None
    try:
        lock_file = acquire_lock(lock_path)
    except RuntimeError as e:
        # Finalize the event as skipped or error if event_id is present
        from app.utils.event_logger import finalize_event
        if event_id:
            finalize_event(
                event_id=event_id,
                status="skipped",
                event=f"Backup job '{job_name}' is already running or locked.",
                backup_set_id=None,
                runtime="00:00:00"
            )
        raise RuntimeError(f"Backup job '{job_name}' is already running or locked. {e}")

    try:
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
