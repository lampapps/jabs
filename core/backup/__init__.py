"""Backup dispatcher module for JABS."""

from .full import run_full_backup
from .diff import run_diff_backup
from .incremental import run_incremental_backup
from .dryrun import run_dryrun_backup

def run_backup(config, backup_type, **kwargs):
    """Dispatches backup jobs to the appropriate backup type handler."""
    event_id = kwargs.get("event_id")
    
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
    except Exception as e:
        # In case of unexpected errors, update the event if one exists
        if event_id:
            from app.models.events import update_event, finalize_event
            update_event(event_id, event_message=f"Error: {str(e)}")
            finalize_event(
                event_id=event_id,
                status="error",
                event_message=f"Backup failed: {str(e)}",
                backup_set_id=None
            )
        raise
