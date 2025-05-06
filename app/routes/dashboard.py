from flask import Blueprint, render_template, abort
from markupsafe import Markup
import os
import json
import markdown
from datetime import datetime
from app.settings import BASE_DIR, MANIFEST_BASE
from app.utils.manifest import get_cleaned_yaml_config, get_tarball_summary
from app.utils.dashboard_helpers import find_config_path_by_job_name, load_config
import socket

dashboard_bp = Blueprint('dashboard', __name__)

@dashboard_bp.route("/")
def dashboard():
    return render_template("index.html")

@dashboard_bp.route("/help")
def help():
    readme_path = os.path.join(BASE_DIR, "README.md")
    if not os.path.exists(readme_path):
        content = "<p>README.md not found.</p>"
    else:
        with open(readme_path, "r") as f:
            md_content = f.read()
        content = Markup(markdown.markdown(md_content, extensions=["fenced_code", "tables"]))
    return render_template("help.html", content=content)

@dashboard_bp.route('/manifest/<string:job_name>/<string:backup_set_id>')
def view_manifest(job_name, backup_set_id):
    sanitized_job = "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in job_name)
    abs_json_path = os.path.join(BASE_DIR, MANIFEST_BASE, sanitized_job, f"{backup_set_id}.json")
    if not os.path.exists(abs_json_path):
        abort(404, description="Manifest file not found (os.path.exists failed).")
    with open(abs_json_path, "r") as f:
        manifest_data = json.load(f)
    job_config_path = find_config_path_by_job_name(job_name)
    tarball_summary_list = []
    if job_config_path:
        job_config = load_config(job_config_path)
        if job_config and 'destination' in job_config:
            backup_set_path_on_dst = os.path.join(
                job_config['destination'],
                socket.gethostname(),
                sanitized_job,
                f"backup_set_{backup_set_id}"
            )
            tarball_summary_list = get_tarball_summary(backup_set_path_on_dst)
    cleaned_config = get_cleaned_yaml_config(job_config_path) if job_config_path else "Config file not found."
    manifest_timestamp = manifest_data.get("timestamp", "N/A")
    if manifest_timestamp != "N/A":
        try:
            dt_object = datetime.fromisoformat(manifest_timestamp)
            manifest_timestamp = dt_object.strftime("%A, %B %d, %Y at %I:%M %p")
        except Exception:
            pass
    return render_template(
        'manifest.html',
        job_name=manifest_data.get("job_name", job_name),
        backup_set_id=manifest_data.get("backup_set_id", backup_set_id),
        manifest_timestamp=manifest_timestamp,
        config_content=cleaned_config,
        all_files=manifest_data.get("files", []),
        tarball_summary=tarball_summary_list
    )