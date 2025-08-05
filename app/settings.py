"""Application-wide settings and configuration constants."""

import os
import sys
from datetime import timedelta
import yaml



VERSION = "v0.8.0 beta"

# --- Environment Configuration ---
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

# Path to the .env file for environment credentials (Encryption, AWS, and SMTP)
# This file should not be committed to version control
ENV_PATH = os.path.join(BASE_DIR, ".env")

# Uncomment the following line if codebase is in development mode
# This will display a banner, create a seperate monitor json file, and enable debug logging
ENV_MODE='development'

# --- Application Configuration ---
TEMPLATE_DIR = os.path.join(BASE_DIR, 'app', 'templates')
STATIC_DIR = os.path.join(BASE_DIR, 'app', 'static')
LOCK_DIR = os.path.join(BASE_DIR, 'locks')
CLI_SCRIPT = os.path.join(BASE_DIR, 'cli.py')
RESTORE_STATUS_DIR = os.path.join(BASE_DIR, 'locks', "restore_status")
PYTHON_EXECUTABLE = sys.executable or "python3"

# --- CONFIG Configuration ---
CONFIG_DIR = os.path.join(BASE_DIR, 'config')
JOBS_DIR = os.path.join(CONFIG_DIR, 'jobs')
GLOBAL_CONFIG_PATH = os.path.join(CONFIG_DIR, "global.yaml")
MONITOR_CONFIG_PATH = os.path.join(CONFIG_DIR, "monitor.yaml")

# --- Data Configuration ---
DATA_DIR = os.path.join(BASE_DIR, 'data')
DB_PATH = os.path.join(DATA_DIR, "jabs.sqlite")

# --- Logging Configuration ---
LOG_DIR = os.path.join(BASE_DIR, 'logs')
MAX_LOG_LINES = 10000

# --- Restore Configuration ---
HOME_DIR = os.path.expanduser("~") # user's home path to restrict custom restore location
RESTORE_SCRIPT_SRC = os.path.join(BASE_DIR, 'restore.py') # script that is copied to repositories with archives

#--- Scheduler Configuration ---
MAX_SCHEDULER_EVENTS = 300      # How many event bars show in the dashboard
SCHEDULE_TOLERANCE = timedelta(seconds=15)      # buffer for cron job execution
SCHEDULER_STATUS_FILE = os.path.join(LOG_DIR, "scheduler.status")

# --- SMTP Configuration ---
with open(GLOBAL_CONFIG_PATH, "r", encoding="utf-8") as f:
    GLOBAL_CONFIG = yaml.safe_load(f)

EMAIL_CONFIG = GLOBAL_CONFIG.get("email", {})
