"""Flask routes for the JABS monitor page: displays status of monitored targets."""

import os
import socket
from datetime import datetime, timezone

import yaml
from flask import Blueprint, render_template

from app.settings import CONFIG_DIR, ENV_MODE
from app.utils.poll_targets import poll_targets


monitor_bp = Blueprint('monitor', __name__)

@monitor_bp.route("/monitor")
def monitor():
    """Render the monitor page with status of all monitored targets."""
    monitor_yaml_path = os.path.join(CONFIG_DIR, "monitor.yaml")
    with open(monitor_yaml_path, "r", encoding="utf-8") as f:
        monitor_cfg = yaml.safe_load(f)
    shared_monitor_dir = monitor_cfg.get("shared_monitor_dir")
    targets = monitor_cfg.get("monitored_targets", [])
    monitors = poll_targets(targets)  # This just checks network connectivity, not HTTP APIs

    return render_template(
        "monitor.html",
        monitors=monitors,
        monitor_statuses={},  # Empty - will be populated by client-side JS
        api_statuses={},  # Empty - will be populated by client-side JS
        expected_paths={},  # Empty - will be populated by client-side JS
        targets=targets,
        problems={},  # Empty - will be populated by client-side JS
        hostname=socket.gethostname(),
        monitor_yaml_path=monitor_yaml_path,
        env_mode=ENV_MODE,
        now=datetime.now(timezone.utc).timestamp(),
    )
