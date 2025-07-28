"""Flask routes for the JABS monitor page: displays status of monitored targets."""

import os
import json
import socket
from datetime import datetime, timezone

import requests
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
    monitors = poll_targets(targets)

    monitor_statuses = {}
    expected_paths = {}
    api_statuses = {}
    problems = {}

    now = datetime.now(timezone.utc)
    for target in targets:
        host_keys = []
        if target.get("hostname"):
            host_keys.append(target["hostname"])
        if target.get("name"):
            host_keys.append(target["name"])
        status = None
        api_status = None
        api_available = False
        api_url = target.get("url")
        # Try API first
        if api_url:
            try:
                resp = requests.get(f"{api_url}/api/heartbeat", timeout=2)
                if resp.ok:
                    api_status = resp.json()
                    api_available = True
            except requests.RequestException:
                api_status = None
                api_available = False
        # If API not available, try local file
        if not api_available and shared_monitor_dir:
            monitor_dir = os.path.join(shared_monitor_dir, "monitor")
            for host_key in host_keys:
                json_path = os.path.join(monitor_dir, f"{host_key}.json")
                if os.path.exists(json_path):
                    with open(json_path, "r", encoding="utf-8") as f:
                        status = json.load(f)
                    break
        key = target.get("hostname") or target.get("name") or "UNKNOWN"
        monitor_statuses[key] = status
        api_statuses[key] = api_status
        expected_paths[key] = api_url if api_available else (os.path.join(shared_monitor_dir, "monitor", f"{host_keys[0]}.json") if host_keys else None)

        # Determine if there is a problem
        s = api_status or status or {}
        error_count = s.get("error_event_count", 0)
        last_run_ts = s.get("last_scheduler_run")
        grace_period = target.get("grace_period", 60)
        too_old = False
        if last_run_ts:
            try:
                last_run_dt = datetime.fromtimestamp(float(last_run_ts), tz=timezone.utc)
                minutes_since = (now - last_run_dt).total_seconds() / 60
                too_old = minutes_since > grace_period
            except (ValueError, TypeError, OSError, IOError):
                too_old = True  # If can't parse, flag as problem
        else:
            too_old = True  # No last_run_ts, flag as problem

        problems[key] = (error_count > 0) or too_old

    return render_template(
        "monitor.html",
        monitors=monitors,
        monitor_statuses=monitor_statuses,
        api_statuses=api_statuses,
        expected_paths=expected_paths,
        targets=targets,
        problems=problems,
        hostname=socket.gethostname(),
        monitor_yaml_path=monitor_yaml_path,
        env_mode=ENV_MODE,
        now=datetime.now(timezone.utc).timestamp(),
    )
