import os
import sys

# --- Application Configuration ---
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
CONFIG_DIR = os.path.join(BASE_DIR, 'config')
JOBS_DIR = os.path.join(CONFIG_DIR, 'jobs')  # Add this line for job configs
LOG_DIR = os.path.join(BASE_DIR, 'logs')
TEMPLATE_DIR = os.path.join(BASE_DIR, 'app', 'templates')
STATIC_DIR = os.path.join(BASE_DIR, 'app', 'static')
LOCK_DIR = os.path.join(BASE_DIR, 'locks')
CLI_SCRIPT = os.path.join(BASE_DIR, 'cli.py')

DATA_DIR = os.path.join(BASE_DIR, 'data')
MANIFEST_BASE = os.path.join(DATA_DIR, 'manifests')
EVENTS_FILE = os.path.join(DATA_DIR, 'dashboard', 'events.json')

PYTHON_EXECUTABLE = sys.executable or "python3"

