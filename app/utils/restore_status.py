import os
from app.settings import RESTORE_STATUS_DIR

def set_restore_status(job_name, backup_set_id, running=True):
    os.makedirs(RESTORE_STATUS_DIR, exist_ok=True)
    status_file = os.path.join(RESTORE_STATUS_DIR, f"{job_name}_{backup_set_id}.status")
    if running:
        with open(status_file, "w") as f:
            f.write("running")
    else:
        if os.path.exists(status_file):
            os.remove(status_file)

def check_restore_status(job_name, backup_set_id):
    status_file = os.path.join(RESTORE_STATUS_DIR, f"{job_name}_{backup_set_id}.status")
    return os.path.exists(status_file)