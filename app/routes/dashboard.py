"""Flask routes for the JABS dashboard web interface, including job status, documentation, and storage tree views."""

import os
import json
import socket
from datetime import datetime, timezone

import requests
import yaml
from cron_descriptor import get_description
from flask import Blueprint, render_template, current_app
from markupsafe import Markup
import mistune

from app.settings import BASE_DIR, CONFIG_DIR, ENV_MODE
from app.utils.dashboard_helpers import ensure_minimum_scheduler_events

dashboard_bp = Blueprint('dashboard', 'dashboard')

def load_storage_config(config_path):
    """Load storage configuration from a YAML file."""
    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    drives = config.get("drives", [])
    s3_buckets = config.get("s3_buckets", [])
    return drives, s3_buckets

@dashboard_bp.route("/")
def dashboard():
    """Render the dashboard with scheduled jobs and their statuses."""
    ensure_minimum_scheduler_events()

    jobs_dir = os.path.join(BASE_DIR, "config", "jobs")
    job_paths = [
        os.path.join(jobs_dir, fname)
        for fname in os.listdir(jobs_dir)
        if fname.endswith(".yaml")
    ]

    with open('config/global.yaml', encoding="utf-8") as f:
        global_config = yaml.safe_load(f)

    # --- Monitor badge logic ---
    monitor_yaml_path = os.path.join(CONFIG_DIR, "monitor.yaml")
    targets = []
    problems = {}
    api_statuses = {}
    try:
        with open(monitor_yaml_path, "r", encoding="utf-8") as f:
            monitor_cfg = yaml.safe_load(f)
        targets = monitor_cfg.get("monitored_targets", [])
        shared_monitor_dir = monitor_cfg.get("shared_monitor_dir")
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
                except Exception:
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
            api_statuses[key] = api_status
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
                    too_old = True
            else:
                too_old = True
            problems[key] = (error_count > 0) or too_old
    except (OSError, IOError, yaml.YAMLError) as e:
        current_app.logger.error(f"Error loading monitor.yaml: {e}")
        # Optionally: set targets = [] or handle gracefully
    # --- End monitor badge logic ---

    scheduled_jobs = []
    for job_path in job_paths:
        try:
            with open(job_path, encoding="utf-8") as f:
                job_config = yaml.safe_load(f)
        except (OSError, IOError, yaml.YAMLError) as e:
            current_app.logger.error(f"Error loading job config {job_path}: {e}")
            continue

        aws_enabled = job_config.get("aws", {}).get("enabled")
        if aws_enabled is None:
            aws_enabled = global_config.get("aws", {}).get("enabled", False)

        encrypt_enabled = job_config.get("encryption", {}).get("enabled")
        if encrypt_enabled is None:
            encrypt_enabled = global_config.get("encryption", {}).get("enabled", False)

        enabled_schedules = []
        for s in job_config.get("schedules", []):
            if s.get("enabled"):
                cron_expr = s.get("cron", "")
                try:
                    s["cron_human"] = get_description(cron_expr)
                except (ValueError, TypeError):
                    s["cron_human"] = cron_expr
                enabled_schedules.append(s)

        if enabled_schedules:
            scheduled_jobs.append({
                "job_name": job_config.get("job_name", os.path.basename(job_path)),
                "schedules": enabled_schedules,
                "sync": aws_enabled,
                "encrypt": encrypt_enabled,
            })

    return render_template(
        "index.html",
        scheduled_jobs=scheduled_jobs,
        hostname=socket.gethostname(),
        targets=targets,
        problems=problems,
        api_statuses=api_statuses,
        env_mode=ENV_MODE
    )

@dashboard_bp.route("/documentation")
def documentation():
    """Render the documentation page from README.md."""
    readme_path = os.path.join(BASE_DIR, "README.md")
    if not os.path.exists(readme_path):
        content = "<p>README.md not found.</p>"
    else:
        with open(readme_path, "r", encoding="utf-8") as f:
            md_content = f.read()
        markdown_renderer = mistune.create_markdown(renderer=mistune.HTMLRenderer())
        content = Markup(markdown_renderer(md_content))
    return render_template("documentation.html", content=content, env_mode=ENV_MODE,hostname=socket.gethostname())

@dashboard_bp.route("/change_log")
def change_log():
    """Render the documentation page from CHANGELOG.md."""
    changelog_path = os.path.join(BASE_DIR, "CHANGELOG.md")
    if not os.path.exists(changelog_path):
        content = "<CHANGELOG.md not found.</p>"
    else:
        with open(changelog_path, "r", encoding="utf-8") as f:
            md_content = f.read()
        markdown_renderer = mistune.create_markdown(renderer=mistune.HTMLRenderer())
        content = Markup(markdown_renderer(md_content))
    return render_template("change_log.html", content=content, env_mode=ENV_MODE, hostname=socket.gethostname())

@dashboard_bp.route("/license")
def license_page():
    """Render the documentation page from LICENSE.md."""
    license_path = os.path.join(BASE_DIR, "LICENSE.md")
    if not os.path.exists(license_path):
        content = "<LICENSE.md not found.</p>"
    else:
        with open(license_path, "r", encoding="utf-8") as f:
            md_content = f.read()
        markdown_renderer = mistune.create_markdown(renderer=mistune.HTMLRenderer())
        content = Markup(markdown_renderer(md_content))
    return render_template("license.html", content=content, env_mode=ENV_MODE, hostname=socket.gethostname())

@dashboard_bp.route("/scheduler")
def scheduler():
    """Render the scheduler page."""
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
    venv_python = os.path.join(base_dir, 'venv', 'bin', 'python3')
    scheduler_py = os.path.join(base_dir, 'scheduler.py')

    return render_template(
        "scheduler.html",
        venv_python=venv_python,
        scheduler_py=scheduler_py,
        env_mode=ENV_MODE,
        hostname=socket.gethostname()
    )
