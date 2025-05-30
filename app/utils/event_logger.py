"""Event logging utilities for JABS."""

import json
import os
import socket
import time
from datetime import datetime
import portalocker

from app.settings import EVENTS_FILE

def load_events_locked():
    """Load events.json with a shared lock (cross-platform)."""
    if os.path.exists(EVENTS_FILE):
        try:
            with open(EVENTS_FILE, "r", encoding="utf-8") as f:  # Specify encoding
                portalocker.lock(f, portalocker.LOCK_SH)
                try:
                    events = json.load(f)
                except json.JSONDecodeError:
                    print("Warning: events.json is empty or corrupted.")
                    events = {"data": []}
                portalocker.unlock(f)
                return events
        except OSError as e:  # More specific exception
            print(f"Warning: Failed to load events.json: {e}")
            return {"data": []}
    return {"data": []}

def save_events_locked(events):
    """Save events.json with an exclusive lock (cross-platform)."""
    os.makedirs(os.path.dirname(EVENTS_FILE), exist_ok=True)
    with open(EVENTS_FILE, "w", encoding="utf-8") as f:  # Specify encoding
        portalocker.lock(f, portalocker.LOCK_EX)
        json.dump(events, f, indent=4)
        portalocker.unlock(f)

def generate_event_id(hostname, job_name, starttimestamp):
    """Generate a unique ID for the event."""
    return f"{hostname}/{job_name}/{starttimestamp}"

def initialize_event(job_name, event, backup_type, encrypt=False, sync=False):
    """Initialize a new event in the events.json file."""
    events = load_events_locked()

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

    events["data"].append(event_data)
    save_events_locked(events)

    return event_id

def update_event(event_id, backup_type=None, event=None, status=None, url=None):
    """
    Update an existing event in the events.json file.
    """
    events = load_events_locked()

    # Locate the event by its ID and update it
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

    save_events_locked(events)

def finalize_event(event_id, status, event, runtime=None, backup_set_id=None):
    """
    Finalize an event in the events.json file by updating its status, event description,
    runtime, URL, and backup_set_id.
    """
    events = load_events_locked()
    found = False
    event_data = None

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

            found = True
            event_data = item  # Save for email
            break

    if found:
        save_events_locked(events)
        # --- Email notification logic based on notify_on settings ---
        # Import inside function to avoid circular import
        from app.utils.emailer import email_event

        # Map status and backup_type to event_type for email notifications
        event_type = None
        if status == "error":
            event_type = "error"
        elif status == "success":
            backup_type = event_data.get("backup_type", "").lower()
            if backup_type == "restore":
                event_type = "restore_complete"
            elif backup_type == "sync":
                event_type = "sync_complete"
            else:
                event_type = "job_complete"

        if event_type:
            subject = f"JABS Notification: {event_type.replace('_', ' ').title()}"
            # Build detailed email body
            body = (
                f"Machine: {event_data.get('hostname', 'N/A')}\n"
                f"Job Name: {event_data.get('job_name', 'N/A')}\n"
                f"Type: {event_data.get('backup_type', 'N/A')}\n"
                f"Event: {event}\n"
                f"Status: {status}\n"
                f"Start Time: {event_data.get('starttimestamp', 'N/A')}\n"
                f"Runtime: {event_data.get('runtime', 'N/A')}\n"
            )
            email_event(event_type, subject, body)

def remove_event_by_backup_set_id(backup_set_id, logger):
    """
    Remove an event from events.json based on the backup set ID.
    :param backup_set_id: The ID of the backup set to remove.
    :param logger: Logger instance for logging messages.
    """
    events = load_events_locked()
    updated_events = {"data": []}
    event_removed = False

    logger.info(f"Attempting to remove event for backup set ID: {backup_set_id}")

    # Filter out the event corresponding to the backup set ID
    for event in events["data"]:
        # Check the new 'backup_set_id' field directly
        event_backup_set_id = event.get("backup_set_id")  # Use .get() for safety

        if event_backup_set_id == backup_set_id:
            logger.info(f"Removed event for backup set ID: {backup_set_id} (Event ID: {event.get('id')})")
            event_removed = True
        else:
            updated_events["data"].append(event)

    # Save the updated events back to events.json
    save_events_locked(updated_events)

    if not event_removed:
        logger.warning(f"No event found with backup_set_id: {backup_set_id}")

def get_event_status(event_id):
    """
    Returns the status of the event with the given event_id, or None if not found.
    """
    if not os.path.exists(EVENTS_FILE):
        return None
    with open(EVENTS_FILE, "r", encoding="utf-8") as f:  # Specify encoding
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

