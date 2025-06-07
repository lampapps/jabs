"""Routes for configuration management in JABS."""

import os
import yaml
import socket
from flask import Blueprint, render_template, request, redirect, url_for, abort, flash
from dotenv import load_dotenv
from cron_descriptor import get_description
from app.settings import JOBS_DIR, GLOBAL_CONFIG_PATH

config_bp = Blueprint('config', __name__)

@config_bp.route("/config", endpoint="config")
def show_global_config():
    """Display the global configuration."""
    with open(GLOBAL_CONFIG_PATH, encoding="utf-8") as f:
        global_config = yaml.safe_load(f)
    env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), ".env")
    load_dotenv(env_path)
    current_passphrase = bool(os.environ.get("JABS_ENCRYPT_PASSPHRASE"))

    digest_cron = global_config.get("email", {}).get("digest_email_schedule")
    digest_cron_human = ""
    if digest_cron:
        try:
            digest_cron_human = get_description(digest_cron)
        except Exception:  
            digest_cron_human = "Invalid cron expression"

    # --- Add this block for common_exclude.yaml ---
    common_exclude_path = os.path.join(os.path.dirname(GLOBAL_CONFIG_PATH), "common_exclude.yaml")
    try:
        with open(common_exclude_path, encoding="utf-8") as f:
            common_exclude_raw = f.read()
        common_exclude_error = None
    except OSError as e:
        common_exclude_raw = ""
        common_exclude_error = str(e)

    return render_template(
        "globalconfig.html",
        global_config=global_config,
        current_passphrase=current_passphrase,
        digest_cron_human=digest_cron_human,
        common_exclude_raw=common_exclude_raw,
        common_exclude_error=common_exclude_error,
        hostname=socket.gethostname()
    )

@config_bp.route("/config/save_global", methods=["POST"])
def save_global():
    """Save the global configuration file."""
    new_content = request.form.get("content", "")
    try:
        yaml.safe_load(new_content)
    except yaml.YAMLError as e:
        return render_template("globalconfig.html", raw_data=new_content, error=str(e))  # changed
    with open(GLOBAL_CONFIG_PATH, "w", encoding="utf-8") as f:
        f.write(new_content)
    flash("Global configuration saved.", "success")
    return redirect(url_for("config.config"))

# Example Flask route
@config_bp.route('/edit/<filename>', methods=['GET', 'POST'])
def edit_config(filename):
    """Edit a configuration file."""
    error_message = None
    if filename == "global.yaml":
        file_path = GLOBAL_CONFIG_PATH
        cancel_url = url_for("config.config")
    elif filename == "common_exclude.yaml":
        file_path = os.path.join(os.path.dirname(GLOBAL_CONFIG_PATH), "common_exclude.yaml")
        cancel_url = url_for("config.config")
    elif filename == "monitor.yaml":
        file_path = os.path.join(os.path.dirname(GLOBAL_CONFIG_PATH), "monitor.yaml")
        cancel_url = url_for("monitor.monitor")
    else:
        file_path = os.path.join(JOBS_DIR, filename)
        cancel_url = url_for("jobs.jobs_view")
    if not os.path.exists(file_path):
        loaded_yaml = "# File not found."
    else:
        with open(file_path, "r", encoding="utf-8") as f:
            loaded_yaml = f.read()
    return render_template(
        "edit_config.html",
        file_name=filename,
        raw_data=loaded_yaml,
        cancel_url=cancel_url,
        error=error_message,
        hostname=socket.gethostname()
    )

@config_bp.route("/config/save/<filename>", methods=["POST"])
def save_config(filename):
    """Save a configuration file."""
    if not filename.endswith(".yaml") or "/" in filename or ".." in filename:
        abort(400, "Invalid filename")
    if filename == "global.yaml":
        file_path = GLOBAL_CONFIG_PATH
    elif filename == "common_exclude.yaml":
        file_path = os.path.join(os.path.dirname(GLOBAL_CONFIG_PATH), "common_exclude.yaml")
    elif filename == "monitor.yaml":
        file_path = os.path.join(os.path.dirname(GLOBAL_CONFIG_PATH), "monitor.yaml")
    else:
        file_path = os.path.join(JOBS_DIR, filename)
    new_content = request.form.get("content", "")
    next_url = request.form.get("next") or url_for("config.config")
    try:
        yaml.safe_load(new_content)
    except yaml.YAMLError as e:
        return render_template(
            "edit_config.html",
            file_name=filename,
            raw_data=new_content,
            error=str(e),
            cancel_url=next_url
        )
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(new_content)
    flash("Configuration saved.", "success")
    return redirect(next_url)

