import os
import json
import socket
from datetime import datetime

from app.settings import EVENTS_FILE, VERSION

def count_error_events(events_path):
    try:
        with open(events_path, "r", encoding="utf-8") as f:
            events_data = json.load(f)
            if isinstance(events_data, dict) and "data" in events_data:
                events = events_data["data"]
            else:
                events = events_data
            return sum(1 for e in events if e.get("status") == "error")
    except Exception:
        return 0

def write_monitor_status(shared_monitor_dir, version, last_run, log_dir):
    if not shared_monitor_dir:
        return
    os.makedirs(shared_monitor_dir, exist_ok=True)
    monitor_dir = os.path.join(shared_monitor_dir, "monitor")
    os.makedirs(monitor_dir, exist_ok=True)
    machine_name = socket.gethostname()

    # Gather heartbeat data
    last_run_ts = None
    last_run_str = None
    try:
        last_run_ts = float(last_run)
        last_run_str = datetime.fromtimestamp(last_run_ts).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        last_run_str = last_run

    error_event_count = count_error_events(EVENTS_FILE)

    status = {
        "hostname": machine_name,
        "version": VERSION,
        "status": "ok",
        "last_scheduler_run": last_run_ts,
        "last_scheduler_run_str": last_run_str,
        "error_event_count": error_event_count
    }
    status_path = os.path.join(monitor_dir, f"{machine_name}.json")
    with open(status_path, "w", encoding="utf-8") as f:
        json.dump(status, f, indent=2)