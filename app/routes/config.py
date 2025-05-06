from flask import Blueprint, render_template, request, redirect, url_for, abort, flash
import os
import yaml
from app.settings import JOBS_DIR, GLOBAL_CONFIG_PATH

config_bp = Blueprint('config', __name__)


@config_bp.route("/config.html")
def config():
    if not os.path.exists(GLOBAL_CONFIG_PATH):
        raw_data = "# global.yaml not found."
    else:
        with open(GLOBAL_CONFIG_PATH, "r") as f:
            raw_data = f.read()
    return render_template("config.html", raw_data=raw_data)

@config_bp.route("/config/save_global", methods=["POST"])
def save_global():
    new_content = request.form.get("content", "")
    try:
        yaml.safe_load(new_content)
    except yaml.YAMLError as e:
        return render_template("config.html", raw_data=new_content, error=str(e))
    with open(GLOBAL_CONFIG_PATH, "w") as f:
        f.write(new_content)
    flash("Global configuration saved.", "success")
    return redirect(url_for("config.config"))

@config_bp.route("/config/edit/<filename>", methods=["GET"])
def edit_config(filename):
    if not filename.endswith(".yaml") or "/" in filename or ".." in filename:
        abort(400, "Invalid filename")
    file_path = os.path.join(JOBS_DIR, filename)
    if not os.path.exists(file_path):
        abort(404, "Config file not found")
    with open(file_path) as f:
        content = f.read()
    next_url = request.args.get("next", url_for("config.config"))
    return render_template("edit_config.html", filename=filename, content=content, next_url=next_url)

@config_bp.route("/config/save/<filename>", methods=["POST"])
def save_config(filename):
    if not filename.endswith(".yaml") or "/" in filename or ".." in filename:
        abort(400, "Invalid filename")
    file_path = os.path.join(JOBS_DIR, filename)
    new_content = request.form.get("content", "")
    try:
        yaml.safe_load(new_content)
    except yaml.YAMLError as e:
        next_url = request.form.get("next") or url_for("config.config")
        return render_template("edit_config.html", filename=filename, content=new_content, error=str(e), next_url=next_url)
    with open(file_path, "w") as f:
        f.write(new_content)
    next_url = request.form.get("next") or url_for("config.config")
    return redirect(next_url)

@config_bp.route("/config/copy", methods=["POST"])
def copy_config():
    source = request.form.get("copy_source")
    new_filename = request.form.get("new_filename")
    next_url = request.form.get("next") or url_for("jobs.jobs")
    if not source or not new_filename or "/" in new_filename or ".." in new_filename or not new_filename.endswith(".yaml"):
        flash("Invalid filename.", "danger")
        return redirect(url_for("jobs.jobs"))

    # Determine source path (template or job)
    if source.startswith("templates/"):
        src_path = os.path.join(JOBS_DIR, source)
    else:
        src_path = os.path.join(JOBS_DIR, source)
    dest_path = os.path.join(JOBS_DIR, new_filename)

    if not os.path.exists(src_path):
        flash("Source file does not exist.", "danger")
        return redirect(url_for("jobs.jobs"))
    if os.path.exists(dest_path):
        flash("A file with that name already exists.", "danger")
        return redirect(url_for("jobs.jobs"))
    with open(src_path, "r") as src, open(dest_path, "w") as dst:
        dst.write(src.read())
    flash(f"Copied {source} to {new_filename}.", "success")
    # Pass next as a query param to edit_config
    return redirect(url_for("config.edit_config", filename=new_filename, next=next_url))

@config_bp.route("/config/rename/<filename>", methods=["POST"])
def rename_config(filename):
    if not filename.endswith(".yaml") or "/" in filename or ".." in filename:
        flash("Invalid original filename.", "danger")
        return redirect(url_for("jobs.jobs"))
    new_filename = request.form.get("new_filename")
    if not new_filename or "/" in new_filename or ".." in new_filename or not new_filename.endswith(".yaml"):
        flash("Invalid new filename.", "danger")
        return redirect(url_for("jobs.jobs"))
    src_path = os.path.join(JOBS_DIR, filename)
    dest_path = os.path.join(JOBS_DIR, new_filename)
    if not os.path.exists(src_path):
        flash("Original file does not exist.", "danger")
        return redirect(url_for("jobs.jobs"))
    if os.path.exists(dest_path):
        flash("A file with that name already exists.", "danger")
        return redirect(url_for("jobs.jobs"))
    os.rename(src_path, dest_path)
    flash(f"Renamed {filename} to {new_filename}.", "success")
    return redirect(url_for("jobs.jobs"))

@config_bp.route("/config/delete/<filename>", methods=["POST"])
def delete_config(filename):
    if filename in ("drives.yaml", "example.yaml") or "/" in filename or ".." in filename or not filename.endswith(".yaml"):
        flash("This file cannot be deleted.", "danger")
        return redirect(url_for("jobs.jobs"))
    file_path = os.path.join(JOBS_DIR, filename)
    if not os.path.exists(file_path):
        flash("File does not exist.", "danger")
        return redirect(url_for("jobs.jobs"))
    os.remove(file_path)
    flash(f"Deleted {filename}.", "success")
    return redirect(url_for("jobs.jobs"))
