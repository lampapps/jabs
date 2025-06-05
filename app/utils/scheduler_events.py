"""Scheduler event logging utilities for JABS."""

import os
import json
from app.settings import SCHEDULER_EVENTS_PATH, MAX_SCHEDULER_EVENTS


def append_scheduler_event(datetime, job_name, backup_type, status):
    """
    Append a scheduler event to the scheduler events JSON file,
    trimming to the maximum allowed number of events.
    """
    os.makedirs(os.path.dirname(SCHEDULER_EVENTS_PATH), exist_ok=True)
    if os.path.exists(SCHEDULER_EVENTS_PATH):
        with open(SCHEDULER_EVENTS_PATH, "r", encoding="utf-8") as f:
            try:
                events = json.load(f)
            except json.JSONDecodeError:
                events = []
    else:
        events = []
    events.append({
        "datetime": datetime,
        "job_name": job_name,
        "backup_type": backup_type,
        "status": status
    })
    if len(events) > MAX_SCHEDULER_EVENTS:
        events = events[-MAX_SCHEDULER_EVENTS:]
    with open(SCHEDULER_EVENTS_PATH, "w", encoding="utf-8") as f:
        json.dump(events, f, indent=2)

