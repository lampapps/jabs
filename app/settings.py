import os
import sys

VERSION = "1.0.0"

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

# --- Logging Configuration ---
LOG_DIR = os.path.join(BASE_DIR, 'logs')
MAX_LOG_LINES = 1000

# --- Restore Base Configuration ---
HOME_DIR = os.path.expanduser("~")
RESTORE_SCRIPT_SRC = os.path.join(BASE_DIR, 'restore.py')