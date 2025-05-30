"""Flask routes for managing backup jobs and job configuration in JABS."""

import os
import re
import subprocess
import sys
import tempfile
import pathlib
import fcntl
import yaml
from flask import Blueprint, render_template, request, redirect, url_for, flash
from werkzeug.utils import secure_filename
from cron_descriptor import get_description
from app.settings import LOCK_DIR, BASE_DIR, JOBS_DIR, GLOBAL_CONFIG_PATH

jobs_bp = Blueprint('jobs', __name__)

def is_job_locked(lock_path):
    """Check if a job lock file is currently locked."""
    try:
        with open(lock_path, 'a+', encoding="utf-8") as lock_file:
            try:
                fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
                fcntl.flock(lock_file, fcntl.LOCK_UN)
                return False
            except BlockingIOError:
                return True
    except OSError:
        return True

@jobs_bp.route("/jobs")
def jobs_view():
    """Display all jobs and templates."""
    with open(GLOBAL_CONFIG_PATH, encoding="utf-8") as f:
        global_config = yaml.safe_load(f)

    jobs = []
    for fname in os.listdir(JOBS_DIR):
        if fname.endswith(".yaml"):
            fpath = os.path.join(JOBS_DIR, fname)
            with open(fpath, encoding="utf-8") as f:
                raw_data = f.read()
            try:
                data = yaml.safe_load(raw_data)
                schedules = data.get("schedules", [])
                for sched in schedules:
                    cron_expr = sched.get("cron", "")
                    try:
                        sched["cron_human"] = get_description(cron_expr)
                    except (ValueError, TypeError):
                        sched["cron_human"] = cron_expr
                job_name = data.get("job_name", fname.replace(".yaml", ""))
                source = data.get("source", "")
                destination = data.get("destination") or global_config.get("destination")
                aws = data.get("aws") or global_config.get("aws")
                aws_enabled = None
                if data.get("aws") and "enabled" in data["aws"]:
                    aws_enabled = data["aws"]["enabled"]
                elif global_config.get("aws") and "enabled" in global_config["aws"]:
                    aws_enabled = global_config["aws"]["enabled"]
                else:
                    aws_enabled = False
            except yaml.YAMLError:
                job_name = fname.replace(".yaml", "")
                source = ""
                destination = global_config.get("destination")
                aws = global_config.get("aws")
                aws_enabled = False
                data = {}
            jobs.append({
                "file_name": fname,
                "job_name": job_name,
                "source": source,
                "destination": destination,
                "aws": aws,
                "aws_enabled": aws_enabled,
                "data": data,
                "raw_data": raw_data,
            })

    templates_dir = os.path.join(JOBS_DIR, "templates")
    templates = []
    if os.path.isdir(templates_dir):
        for tname in os.listdir(templates_dir):
            if tname.endswith(".yaml"):
                templates.append(tname)

    with open(GLOBAL_CONFIG_PATH, encoding="utf-8") as f:
        global_config = yaml.safe_load(f)

    return render_template(
        "jobs.html",
        configs=jobs,
        templates=templates,
        global_config=global_config
    )

@jobs_bp.route("/jobs/run/<filename>", methods=["POST"])
def run_job(filename):
    """Run a backup job."""
    if (
        not filename.endswith(".yaml")
        or "/" in filename
        or ".." in filename
    ):
        flash("Invalid job file.", "danger")
        return redirect(url_for("jobs.jobs_view"))

    config_path = os.path.join(JOBS_DIR, filename)
    if not os.path.exists(config_path):
        flash("Config file does not exist.", "danger")
        return redirect(url_for("jobs.jobs_view"))

    with open(config_path, encoding="utf-8") as f:
        config = yaml.safe_load(f)
    job_name = config.get("job_name", filename.replace(".yaml", ""))

    aws_enabled = None
    if config.get("aws") and "enabled" in config["aws"]:
        aws_enabled = config["aws"]["enabled"]
    else:
        with open(GLOBAL_CONFIG_PATH, encoding="utf-8") as gf:
            global_config = yaml.safe_load(gf)
        aws_enabled = global_config.get("aws", {}).get("enabled", False)

    lock_path = os.path.join(LOCK_DIR, f"{job_name}.lock")
    if os.path.exists(lock_path) and is_job_locked(lock_path):
        flash(f"Backup already running for job '{job_name}'.", "warning")
        return redirect(url_for("jobs.jobs_view"))

    backup_type = request.form.get("backup_type", "full").lower()
    if backup_type not in ("full", "diff"):
        flash("Invalid backup type.", "danger")
        return redirect(url_for("jobs.jobs_view"))

    sync = request.form.get("sync", "0")
    cli_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../cli.py'))
    args = [sys.executable, cli_path, "--config", config_path]
    if backup_type == "full":
        args.append("--full")
    else:
        args.append("--diff")
    if sync == "1" and aws_enabled:
        args.append("--sync")

    try:
        temp_dir = tempfile.gettempdir()
        stdout_log = os.path.join(temp_dir, "backup_stdout.log")
        stderr_log = os.path.join(temp_dir, "backup_stderr.log")
        with open(stdout_log, 'w', encoding="utf-8") as out, open(stderr_log, 'w', encoding="utf-8") as err:
            subprocess.Popen(
                args,
                stdout=out,
                stderr=err,
                start_new_session=True,
                cwd=BASE_DIR
            )
        flash(f"{backup_type.capitalize()} backup for {job_name} has been started.", "success")
    except (OSError, subprocess.SubprocessError) as e:
        flash(f"Failed to start backup: {e}", "danger")

    return redirect(url_for("jobs.jobs_view"))

@jobs_bp.route("/config/copy", methods=["POST"])
def copy_config():
    """Copy a job or template configuration file."""
    source = request.form.get("copy_source")
    new_job_name = request.form.get("new_job_name", "").strip()
    next_url = request.form.get("next") or url_for("jobs.jobs_view")

    if not source or not new_job_name or not all(c.isalnum() or c in " _-" for c in new_job_name):
        flash("Invalid job name.", "danger")
        return redirect(url_for("jobs.jobs_view"))

    base_name = secure_filename(new_job_name.replace(" ", "_"))
    if not base_name:
        flash("Invalid job name.", "danger")
        return redirect(url_for("jobs.jobs_view"))
    new_filename = f"{base_name}.yaml"

    src_path = os.path.join(JOBS_DIR, source)
    dest_path = os.path.join(JOBS_DIR, new_filename)

    if not os.path.exists(src_path):
        flash("Source file does not exist.", "danger")
        return redirect(url_for("jobs.jobs_view"))
    if os.path.exists(dest_path):
        flash("A file with that job name already exists.", "danger")
        return redirect(url_for("jobs.jobs_view"))

    with open(src_path, "r", encoding="utf-8") as src:
        content = src.read()
    content_new = re.sub(
        r'^(job_name\s*:\s*)(["\']?.*?["\']?)\s*$',
        r'\1"' + new_job_name + r'"',
        content,
        count=1,
        flags=re.MULTILINE
    )
    with open(dest_path, "w", encoding="utf-8") as dst:
        dst.write(content_new)

    flash(f"Copied {source} to {new_filename}.", "success")
    return redirect(url_for("config.edit_config", filename=new_filename, next=next_url))

@jobs_bp.route("/config/rename/<filename>", methods=["POST"])
def rename_config(filename):
    """Rename a configuration file."""

    if (
        not filename.endswith(".yaml")
        or os.path.sep in filename
        or (os.path.altsep and os.path.altsep in filename)
        or ".." in filename
    ):
        flash("Invalid original filename.", "danger")
        return redirect(url_for("jobs.jobs_view"))

    new_filename = request.form.get("new_filename")
    if (
        not new_filename
        or os.path.sep in new_filename
        or (os.path.altsep and os.path.altsep in new_filename)
        or ".." in new_filename
        or not new_filename.endswith(".yaml")
    ):
        flash("Invalid new filename.", "danger")
        return redirect(url_for("jobs.jobs_view"))

    src_path = os.path.join(JOBS_DIR, filename)
    dest_path = os.path.join(JOBS_DIR, new_filename)

    jobs_dir_path = pathlib.Path(JOBS_DIR).resolve()
    if (
        pathlib.Path(src_path).resolve().parent != jobs_dir_path or
        pathlib.Path(dest_path).resolve().parent != jobs_dir_path
    ):
        flash("Invalid file path.", "danger")
        return redirect(url_for("jobs.jobs_view"))

    if not os.path.exists(src_path):
        flash("Original file does not exist.", "danger")
        return redirect(url_for("jobs.jobs_view"))
    if os.path.exists(dest_path):
        flash("A file with that name already exists.", "danger")
        return redirect(url_for("jobs.jobs_view"))

    os.rename(src_path, dest_path)
    flash(f"Renamed {filename} to {new_filename}.", "success")
    return redirect(url_for("jobs.jobs_view"))

@jobs_bp.route("/config/delete/<filename>", methods=["POST"])
def delete_config(filename):
    """Delete a configuration file."""
    if filename in ("drives.yaml", "example.yaml") or "/" in filename or ".." in filename or not filename.endswith(".yaml"):
        flash("This file cannot be deleted.", "danger")
        return redirect(url_for("jobs.jobs_view"))
    file_path = os.path.join(JOBS_DIR, filename)
    if not os.path.exists(file_path):
        flash("File does not exist.", "danger")
        return redirect(url_for("jobs.jobs_view"))
    os.remove(file_path)
    flash(f"Deleted {filename}.", "success")
    return redirect(url_for("jobs.jobs_view"))
