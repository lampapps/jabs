"""Application-wide settings and configuration constants."""

import os
import sys
import yaml

from datetime import timedelta

VERSION = "v0.7.3"

# --- Application Configuration ---
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
TEMPLATE_DIR = os.path.join(BASE_DIR, 'app', 'templates')
STATIC_DIR = os.path.join(BASE_DIR, 'app', 'static')
LOCK_DIR = os.path.join(BASE_DIR, 'locks')
CLI_SCRIPT = os.path.join(BASE_DIR, 'cli.py')
RESTORE_STATUS_DIR = os.path.join(BASE_DIR, "data", "restore_status")
PYTHON_EXECUTABLE = sys.executable or "python3"

# --- Jobs Configuration ---
CONFIG_DIR = os.path.join(BASE_DIR, 'config')
JOBS_DIR = os.path.join(CONFIG_DIR, 'jobs')
GLOBAL_CONFIG_PATH = os.path.join(CONFIG_DIR, "global.yaml")

# --- Data Configuration ---
DATA_DIR = os.path.join(BASE_DIR, 'data')
MANIFEST_BASE = os.path.join(DATA_DIR, 'manifests')
EVENTS_FILE = os.path.join(DATA_DIR, 'dashboard', 'events.json')
SCHEDULER_EVENTS_PATH = os.path.join(DATA_DIR, "dashboard", "scheduler_events.json")
DB_PATH = os.path.join(DATA_DIR, "jabs.sqlite")

# --- Logging Configuration ---
LOG_DIR = os.path.join(BASE_DIR, 'logs')
MAX_LOG_LINES = 5000

# --- Restore Configuration ---
HOME_DIR = os.path.expanduser("~") # user's home path to restrict custom restore location
RESTORE_SCRIPT_SRC = os.path.join(BASE_DIR, 'restore.py') # script that is copied to archive destination directory

# --- Load GLOBAL_CONFIG ---
with open(GLOBAL_CONFIG_PATH, "r", encoding="utf-8") as f:
    GLOBAL_CONFIG = yaml.safe_load(f)

# --- SMTP Configuration ---
EMAIL_CONFIG = GLOBAL_CONFIG.get("email", {})
EMAIL_DIGEST_FILE = os.path.join(DATA_DIR, "email_digest_queue.json") 

#--- Scheduler Configuration ---
MAX_SCHEDULER_EVENTS = 300      # How many event bars show in the dashboard
SCHEDULE_TOLERANCE = timedelta(seconds=15)      # buffer for cron job execution
SCHEDULER_STATUS_FILE = os.path.join(LOG_DIR, "scheduler.status")