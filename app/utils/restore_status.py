"""Helpers for tracking restore job status using status files."""

import os
from app.settings import RESTORE_STATUS_DIR

def set_restore_status(job_name, backup_set_id, running=True):
    """Set or clear the restore status for a given job and backup set."""
    os.makedirs(RESTORE_STATUS_DIR, exist_ok=True)
    status_file = os.path.join(RESTORE_STATUS_DIR, f"{job_name}_{backup_set_id}.status")
    if running:
        with open(status_file, "w", encoding="utf-8") as f:
            f.write("running")
    else:
        if os.path.exists(status_file):
            os.remove(status_file)

def check_restore_status(job_name, backup_set_id):
    """Check if the restore status file exists for a given job and backup set."""
    status_file = os.path.join(RESTORE_STATUS_DIR, f"{job_name}_{backup_set_id}.status")
    return os.path.exists(status_file)
