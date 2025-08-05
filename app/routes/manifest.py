"""Flask routes for the manifest web interface."""
import os
import json
import socket
from datetime import datetime

import yaml
from flask import Blueprint, render_template, abort, current_app

from app.settings import GLOBAL_CONFIG_PATH, HOME_DIR, ENV_MODE
from app.services.manifest import get_tarball_summary, get_merged_cleaned_yaml_config
from app.utils.dashboard_helpers import find_config_path_by_job_name, load_config
from app.services.manifest import get_manifest_with_files, calculate_total_size

manifest_bp = Blueprint('manifest', '__name__')

@manifest_bp.route('/manifest/<string:job_name>/<string:backup_set_id>')
def view_manifest(job_name, backup_set_id):
    """Render the manifest view for a specific job and backup set (from SQLite)."""
    # Get the original job name (with spaces) from the URL parameter
    original_job_name = job_name

    # Sanitize the job name for filesystem paths only
    sanitized_job = "".join(
        c if c.isalnum() or c in ("-", "_") else "_" for c in job_name
    )

    # Use original job name for database lookup
    manifest_data = get_manifest_with_files(original_job_name, backup_set_id)
    if not manifest_data:
        abort(404, description=f"Manifest not found in database for job '{original_job_name}' and backup set '{backup_set_id}'.")

    # Use original job name for config lookup
    job_config_path = find_config_path_by_job_name(original_job_name)
    tarball_summary_list = []
    total_size_bytes = 0
    total_size_human = "0 B"

    with open(GLOBAL_CONFIG_PATH, encoding="utf-8") as f:
        global_config = yaml.safe_load(f)

    destination = None
    if job_config_path:
        job_config = load_config(job_config_path)
        destination = job_config.get('destination') or global_config.get('destination')
        if destination:
            # Use sanitized_job for filesystem paths
            backup_set_path_on_dst = os.path.join(
                destination,
                socket.gethostname(),
                sanitized_job,  # Use sanitized version for file paths
                f"backup_set_{backup_set_id}"
            )
            # Get the tarball summary with actual file sizes from disk
            tarball_summary_list = get_tarball_summary(backup_set_path_on_dst)

            # Replace the manual calculation with the shared function
            totals = calculate_total_size(tarball_summary_list)
            total_size_bytes = totals["total_size_bytes"]
            total_size_human = totals["total_size_human"]

    cleaned_config = (
        get_merged_cleaned_yaml_config(job_config_path)
        if job_config_path else "Config file not found."
    )

    # Format timestamp for display
    manifest_timestamp = manifest_data.get("timestamp", "N/A")
    if manifest_timestamp != "N/A":
        try:
            dt_object = datetime.fromisoformat(manifest_timestamp)
            manifest_timestamp = dt_object.strftime("%A, %B %d, %Y at %I:%M %p")
        except ValueError:
            pass

    # Extract data from the new schema
    all_files = manifest_data.get("files", [])

    used_config = {}
    if manifest_data.get("config_snapshot"):
        try:
            used_config = json.loads(manifest_data["config_snapshot"])
            current_app.logger.info(f"Successfully loaded config snapshot from database for {original_job_name}/{backup_set_id}")
        except (json.JSONDecodeError, TypeError) as e:
            current_app.logger.error(f"Error parsing config snapshot from database: {e}")
            used_config = {"error": "Could not parse config snapshot from database"}
            
    return render_template(
        'manifest.html',
        job_name=manifest_data.get("job_name", job_name),
        set_name=manifest_data.get("set_name", backup_set_id),
        backup_set_id=backup_set_id,  # For compatibility
        backup_type=manifest_data.get("backup_type", "unknown"),
        status=manifest_data.get("status", "unknown"),
        event_message=manifest_data.get("event", ""),
        manifest_timestamp=manifest_timestamp,
        started_at=manifest_data.get("started_at"),
        completed_at=manifest_data.get("completed_at"),
        config_content=cleaned_config,
        all_files=all_files,
        tarball_summary=tarball_summary_list,
        total_size_bytes=total_size_bytes,
        total_size_human=total_size_human,
        used_config=used_config,
        HOME_DIR=HOME_DIR,
        env_mode=ENV_MODE,
        hostname=socket.gethostname()
    )
