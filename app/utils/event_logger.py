"""Event logging utilities for JABS."""

import json
import os
import socket
import time
from datetime import datetime
import portalocker
import yaml
from app.settings import EVENTS_FILE, GLOBAL_CONFIG_PATH
from app.utils.scheduler_events import append_scheduler_event
from app.utils.emailer import process_email_event

def update_events_json_atomic(modifier_func):
    """
    Atomically load, modify, and save events.json using an exclusive lock.
    modifier_func(events_dict) should modify the dict in place.
    """
    os.makedirs(os.path.dirname(EVENTS_FILE), exist_ok=True)
    # Ensure the file exists and is valid JSON
    if not os.path.exists(EVENTS_FILE):
        with open(EVENTS_FILE, "w", encoding="utf-8") as f:
            json.dump({"data": []}, f)
    with open(EVENTS_FILE, "r+", encoding="utf-8") as f:
        portalocker.lock(f, portalocker.LOCK_EX)
        f.seek(0)
        try:
            events = json.load(f)
        except json.JSONDecodeError:
            events = {"data": []}
        modifier_func(events)
        f.seek(0)
        json.dump(events, f, indent=4)
        f.truncate()
        portalocker.unlock(f)

def load_events_locked():
    """Load events.json with a shared lock (cross-platform)."""
    if os.path.exists(EVENTS_FILE):
        try:
            with open(EVENTS_FILE, "r", encoding="utf-8") as f:
                portalocker.lock(f, portalocker.LOCK_SH)
                try:
                    events = json.load(f)
                except json.JSONDecodeError:
                    print("Warning: events.json is empty or corrupted.")
                    events = {"data": []}
                portalocker.unlock(f)
                return events
        except OSError as e:
            print(f"Warning: Failed to load events.json: {e}")
            return {"data": []}
    return {"data": []}

def save_events_locked(events):
    """Save events.json with an exclusive lock (cross-platform)."""
    os.makedirs(os.path.dirname(EVENTS_FILE), exist_ok=True)
    with open(EVENTS_FILE, "w", encoding="utf-8") as f:
        portalocker.lock(f, portalocker.LOCK_EX)
        json.dump(events, f, indent=4)
        portalocker.unlock(f)

def generate_event_id(hostname, job_name, starttimestamp):
    """Generate a unique ID for the event."""
    return f"{hostname}/{job_name}/{starttimestamp}"

def initialize_event(job_name, event, backup_type, encrypt=False, sync=False):
    """Initialize a new event in the events.json file."""
    starttimestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    event_id = generate_event_id(socket.gethostname(), job_name, starttimestamp)
    event_data = {
        "id": event_id,
        "starttimestamp": starttimestamp,
        "hostname": socket.gethostname(),
        "job_name": job_name,
        "event": event,
        "backup_type": backup_type,
        "encrypt": str(encrypt).lower(),
        "sync": str(sync).lower(),
        "url": None,
        "status": "running",
        "runtime": "<i class='fas fa-spinner fa-spin'></i>",
        "start_time_float": time.time()
    }
    def add_event(events):
        events["data"].append(event_data)
    update_events_json_atomic(add_event)
    return event_id

def update_event(event_id, backup_type=None, event=None, status=None, url=None):
    """
    Update an existing event in the events.json file.
    """
    def modify(events):
        for e in events["data"]:
            if e["id"] == event_id:
                if event:
                    e["event"] = event
                if backup_type:
                    e["backup_type"] = backup_type
                if status:
                    e["status"] = status
                if url:
                    e["url"] = url
                break
        else:
            raise ValueError(f"Event with ID {event_id} not found")
    update_events_json_atomic(modify)

def finalize_event(event_id, status, event, runtime=None, backup_set_id=None, event_type=None):
    """
    Finalize an event in the events.json file by updating its status, event description,
    runtime, URL, and backup_set_id.
    """
    event_data_holder = {}

    def modify(events):
        found = False
        for item in events["data"]:
            if item["id"] == event_id:
                item["status"] = status
                item["event"] = event
                # Calculate runtime if not provided
                if runtime is None and "start_time_float" in item:
                    end_time = time.time()
                    duration_seconds = end_time - item["start_time_float"]
                    hours, remainder = divmod(duration_seconds, 3600)
                    minutes, seconds = divmod(remainder, 60)
                    item["runtime"] = f"{int(hours):02}:{int(minutes):02}:{int(seconds):02}"
                elif runtime:
                    item["runtime"] = runtime

                if "url" in item:
                    del item["url"]

                if backup_set_id:
                    item["backup_set_id"] = backup_set_id
                else:
                    item["backup_set_id"] = None

                if "start_time_float" in item:
                    del item["start_time_float"]

                event_data_holder["event_data"] = item.copy()
                found = True
                break
        if not found:
            raise ValueError(f"Event with ID {event_id} not found")

    update_events_json_atomic(modify)
    event_data = event_data_holder.get("event_data")
    if not event_data:
        return

    # --- Email notification logic based on notify_on settings ---
    try:
        with open(GLOBAL_CONFIG_PATH, "r", encoding="utf-8") as f:
            global_config = yaml.safe_load(f)
        notify_on = global_config.get("email", {}).get("notify_on", {})
    except (OSError, yaml.YAMLError):
        notify_on = {}

    # Map status and backup_type to event_type for email notifications
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

    # Email 
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
    """
    Remove an event from events.json based on the backup set ID.
    :param backup_set_id: The ID of the backup set to remove.
    :param logger: Logger instance for logging messages.
    """
    event_removed = {"removed": False}
    def modify(events):
        new_data = []
        for event in events["data"]:
            event_backup_set_id = event.get("backup_set_id")
            if event_backup_set_id == backup_set_id:
                logger.info(f"Removed event for backup set ID: {backup_set_id} (Event ID: {event.get('id')})")
                event_removed["removed"] = True
            else:
                new_data.append(event)
        events["data"] = new_data
    update_events_json_atomic(modify)
    if not event_removed["removed"]:
        logger.warning(f"No event found with backup_set_id: {backup_set_id}")

def get_event_status(event_id):
    """
    Returns the status of the event with the given event_id, or None if not found.
    """
    if not os.path.exists(EVENTS_FILE):
        return None
    with open(EVENTS_FILE, "r", encoding="utf-8") as f:
        try:
            events_json = json.load(f)
        except json.JSONDecodeError:
            return None
    events = events_json.get("data", [])
    for event in events:
        if isinstance(event, dict) and event.get("id") == event_id:
            return event.get("status")
    return None

def event_exists(event_id):
    """
    Returns True if an event with the given event_id exists, else False.
    """
    events = load_events_locked()
    return any(e.get("id") == event_id for e in events["data"])

