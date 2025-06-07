import os
import json
from flask import Blueprint, render_template, current_app
from app.utils.poll_targets import poll_targets
import yaml
import socket
from app.settings import CONFIG_DIR

monitor_bp = Blueprint('monitor', __name__)

@monitor_bp.route("/monitor")
def monitor():
    monitor_yaml_path = os.path.join(CONFIG_DIR, "monitor.yaml")
    with open(monitor_yaml_path, "r", encoding="utf-8") as f:
        monitor_cfg = yaml.safe_load(f)
    shared_monitor_dir = monitor_cfg.get("shared_monitor_dir")
    targets = monitor_cfg.get("monitored_targets", [])
    monitors = poll_targets(targets)

    # Read status JSON for each target
    monitor_statuses = {}
    if shared_monitor_dir:
        monitor_dir = os.path.join(shared_monitor_dir, "monitor")
        for target in targets:
            # Use the expected hostname for the JSON file
            hostname = target.get("hostname")  # Optionally add this to your yaml
            if not hostname:
                # fallback: try to use the name as the hostname
                hostname = target["name"]
            json_path = os.path.join(monitor_dir, f"{hostname}.json")
            if os.path.exists(json_path):
                with open(json_path, "r", encoding="utf-8") as f:
                    monitor_statuses[hostname] = json.load(f)
            else:
                monitor_statuses[hostname] = None

    return render_template(
        "monitor.html",
        monitors=monitors,
        monitor_statuses=monitor_statuses,
        targets=targets,
        hostname=socket.gethostname()
    )
