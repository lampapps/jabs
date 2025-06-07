import os
import json
import socket

def count_errors_in_log(log_path):
    try:
        with open(log_path, "r", encoding="utf-8") as f:
            return sum(1 for line in f if "ERROR" in line)
    except Exception:
        return 0

def write_monitor_status(shared_monitor_dir, version, last_run, log_dir):
    if not shared_monitor_dir:
        return
    os.makedirs(shared_monitor_dir, exist_ok=True)
    monitor_dir = os.path.join(shared_monitor_dir, "monitor")
    os.makedirs(monitor_dir, exist_ok=True)
    machine_name = socket.gethostname()
    status = {
        "machinename": machine_name,
        "version": version,
        "last_run": last_run,
        "backup_log_errors": count_errors_in_log(os.path.join(log_dir, "backup.log")),
        "email_log_errors": count_errors_in_log(os.path.join(log_dir, "email.log")),
        "scheduler_log_errors": count_errors_in_log(os.path.join(log_dir, "scheduler.log")),
    }
    status_path = os.path.join(monitor_dir, f"{machine_name}.json")
    with open(status_path, "w", encoding="utf-8") as f:
        json.dump(status, f, indent=2)