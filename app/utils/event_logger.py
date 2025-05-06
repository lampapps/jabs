#/utils/event_logger.py
import json
import os
import socket
import time
from datetime import datetime
from app.settings import BASE_DIR, EVENTS_FILE

def load_events():
    """Load the events from the JSON file."""
    if os.path.exists(EVENTS_FILE):
        try:
            with open(EVENTS_FILE, "r") as f:
                events = json.load(f)
                return events
        except json.JSONDecodeError:
            print("Warning: events.json is empty or corrupted.")
            return {"data": []}
    print("Warning: events.json does not exist.")
    return {"data": []}

def save_events(events):
    """Save the events to the JSON file."""
    os.makedirs(os.path.dirname(EVENTS_FILE), exist_ok=True)
    with open(EVENTS_FILE, "w") as f:
        json.dump(events, f, indent=4)

def generate_event_id(hostname, job_name, starttimestamp):
    """Generate a unique ID for the event."""
    return f"{hostname}/{job_name}/{starttimestamp}"

def initialize_event(job_name, event, backup_type, encrypt=False, sync=False):
    """Initialize a new event in the events.json file."""
    events = load_events()

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
    save_events(events)

    return event_id

def update_event(event_id, backup_type=None, event=None, status=None, url=None):
    """
    Update an existing event in the events.json file.
    """
    events = load_events()

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

    save_events(events)

def finalize_event(event_id, status, event, runtime=None, url=None, backup_set_id=None):
    """
    Finalize an event in the events.json file by updating its status, event description,
    runtime, URL, and backup_set_id.
    :param event_id: The unique ID of the event to finalize.
    :param status: The final status ('success', 'error', 'warning').
    :param event: The final event description string.
    :param runtime: Optional final runtime string.
    :param url: Optional URL (consider removing if backup_set_id is used for links).
    :param backup_set_id: Optional backup set ID string associated with the event.
    """
    events = load_events()
    found = False

    for item in events["data"]:
        if item["id"] == event_id:
            item["status"] = status
            item["event"] = event
            # Calculate runtime if not provided
            if runtime is None and "start_time_float" in item:
                end_time = time.time()
                duration_seconds = end_time - item["start_time_float"]
                # Format duration as HH:MM:SS
                hours, remainder = divmod(duration_seconds, 3600)
                minutes, seconds = divmod(remainder, 60)
                item["runtime"] = f"{int(hours):02}:{int(minutes):02}:{int(seconds):02}"
            elif runtime:
                item["runtime"] = runtime

            # Remove the old URL field if it exists
            if "url" in item:
                del item["url"]

            # Add the backup_set_id if provided
            if backup_set_id:
                item["backup_set_id"] = backup_set_id
            else:
                # Ensure the key exists but is null if no ID provided for this finalization
                item["backup_set_id"] = None

            # Remove temporary start time float
            if "start_time_float" in item:
                del item["start_time_float"]

            found = True
            break

    if found:
        save_events(events)
    # else: Consider logging a warning if event_id wasn't found

def remove_event_by_backup_set_id(backup_set_id, logger):
    """
    Remove an event from events.json based on the backup set ID.
    :param backup_set_id: The ID of the backup set to remove.
    :param logger: Logger instance for logging messages.
    """
    events = load_events()
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
    save_events(updated_events)

    if not event_removed:
        logger.warning(f"No event found with backup_set_id: {backup_set_id}")

def get_event_status(event_id):
    """
    Returns the status of the event with the given event_id, or None if not found.
    """
    if not os.path.exists(EVENTS_FILE):
        return None
    with open(EVENTS_FILE, "r") as f:
        try:
            events_json = json.load(f)
        except Exception:
            return None
    events = events_json.get("data", [])
    for event in events:
        if isinstance(event, dict) and event.get("id") == event_id:
            return event.get("status")
    return None

