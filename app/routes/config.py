from flask import Blueprint, render_template, request, redirect, url_for, abort, flash
import os
import yaml
from app.settings import JOBS_DIR, GLOBAL_CONFIG_PATH
from dotenv import set_key, load_dotenv
from werkzeug.utils import secure_filename
import re

config_bp = Blueprint('config', __name__)


@config_bp.route("/config.html", endpoint="config")
def show_global_config():
    with open(GLOBAL_CONFIG_PATH) as f:
        global_config = yaml.safe_load(f)
    # Load current passphrase status
    from dotenv import load_dotenv
    env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), ".env")
    load_dotenv(env_path)
    import os as _os
    current_passphrase = bool(_os.environ.get("JABS_ENCRYPT_PASSPHRASE"))
    return render_template(
        "globalconfig.html",  # changed from "config.html"
        global_config=global_config,
        current_passphrase=current_passphrase
    )

@config_bp.route("/config/save_global", methods=["POST"])
def save_global():
    new_content = request.form.get("content", "")
    try:
        yaml.safe_load(new_content)
    except yaml.YAMLError as e:
        return render_template("globalconfig.html", raw_data=new_content, error=str(e))  # changed
    with open(GLOBAL_CONFIG_PATH, "w") as f:
        f.write(new_content)
    flash("Global configuration saved.", "success")
    return redirect(url_for("config.config"))

# Example Flask route
@config_bp.route('/edit/<filename>', methods=['GET', 'POST'])
def edit_config(filename):
    error_message = None
    file_path = GLOBAL_CONFIG_PATH if filename == "global.yaml" else os.path.join(JOBS_DIR, filename)
    if not os.path.exists(file_path):
        loaded_yaml = "# File not found."
    else:
        with open(file_path, "r") as f:
            loaded_yaml = f.read()
    # Set cancel_url based on file type
    if filename == "global.yaml":
        cancel_url = url_for("config.config")
    else:
        cancel_url = url_for("jobs.jobs")
    return render_template(
        "edit_config.html",
        file_name=filename,
        raw_data=loaded_yaml,
        cancel_url=cancel_url,
        error=error_message
    )

@config_bp.route("/config/save/<filename>", methods=["POST"])
def save_config(filename):
    if not filename.endswith(".yaml") or "/" in filename or ".." in filename:
        abort(400, "Invalid filename")
    # Save to the correct path
    if filename == "global.yaml":
        file_path = GLOBAL_CONFIG_PATH
    else:
        file_path = os.path.join(JOBS_DIR, filename)
    new_content = request.form.get("content", "")
    try:
        yaml.safe_load(new_content)
    except yaml.YAMLError as e:
        next_url = request.form.get("next") or url_for("config.config")
        return render_template(
            "edit_config.html",
            file_name=filename,
            raw_data=new_content,
            error=str(e),
            cancel_url=next_url
        )
    with open(file_path, "w") as f:
        f.write(new_content)
    if filename == "global.yaml":
        return redirect(url_for("config.config"))
    else:
        return redirect(url_for("jobs.jobs"))

@config_bp.route("/config/set_passphrase", methods=["POST"])
def set_passphrase():
    passphrase = request.form.get("passphrase", "").strip()
    if not passphrase:
        flash("Passphrase cannot be empty.", "danger")
        return redirect(url_for("config.config"))
    env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), ".env")
    load_dotenv(env_path)
    set_key(env_path, "JABS_ENCRYPT_PASSPHRASE", passphrase)
    flash("Encryption passphrase updated.", "success")
    return redirect(url_for("config.config"))
