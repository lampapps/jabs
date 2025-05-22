from flask import Blueprint, render_template, request, redirect, url_for, flash, abort
import os
import yaml
import subprocess
import sys
from werkzeug.utils import secure_filename
import re
from app.settings import LOCK_DIR, BASE_DIR, JOBS_DIR, GLOBAL_CONFIG_PATH
from cron_descriptor import get_description

jobs_bp = Blueprint('jobs', __name__)

def is_job_locked(lock_path):
    import fcntl
    try:
        lock_file = open(lock_path, 'a+')
        try:
            fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
            fcntl.flock(lock_file, fcntl.LOCK_UN)
            lock_file.close()
            return False
        except BlockingIOError:
            lock_file.close()
            return True
    except Exception:
        return True

@jobs_bp.route("/jobs")
def jobs():
    # Load global config once
    with open(GLOBAL_CONFIG_PATH) as f:
        global_config = yaml.safe_load(f)

    jobs = []
    for fname in os.listdir(JOBS_DIR):
        if fname.endswith(".yaml"):
            fpath = os.path.join(JOBS_DIR, fname)
            with open(fpath) as f:
                raw_data = f.read()
            try:
                data = yaml.safe_load(raw_data)
                # Add cron_human to each schedule if present
                schedules = data.get("schedules", [])
                for sched in schedules:
                    cron_expr = sched.get("cron", "")
                    try:
                        sched["cron_human"] = get_description(cron_expr)
                    except Exception:
                        sched["cron_human"] = cron_expr
                # Use job value if present, else global
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
            except Exception:
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

    # List templates
    templates_dir = os.path.join(JOBS_DIR, "templates")
    templates = []
    if os.path.isdir(templates_dir):
        for tname in os.listdir(templates_dir):
            if tname.endswith(".yaml"):
                templates.append(tname)

    with open(GLOBAL_CONFIG_PATH) as f:
        global_config = yaml.safe_load(f)

    return render_template(
        "jobs.html",
        configs=jobs,
        templates=templates,
        global_config=global_config
    )

@jobs_bp.route("/jobs/run/<filename>", methods=["POST"])
def run_job(filename):
    if (
        not filename.endswith(".yaml")
        or "/" in filename
        or ".." in filename
    ):
        flash("Invalid job file.", "danger")
        return redirect(url_for("jobs.jobs"))

    config_path = os.path.join(JOBS_DIR, filename)
    if not os.path.exists(config_path):
        flash("Config file does not exist.", "danger")
        return redirect(url_for("jobs.jobs"))

    with open(config_path) as f:
        config = yaml.safe_load(f)
    job_name = config.get("job_name", filename.replace(".yaml", ""))

    # Determine if S3 sync is enabled (job overrides global)
    aws_enabled = None
    if config.get("aws") and "enabled" in config["aws"]:
        aws_enabled = config["aws"]["enabled"]
    else:
        # Load global config if not already loaded
        with open(GLOBAL_CONFIG_PATH) as gf:
            global_config = yaml.safe_load(gf)
        aws_enabled = global_config.get("aws", {}).get("enabled", False)

    lock_path = os.path.join(LOCK_DIR, f"{job_name}.lock")
    if os.path.exists(lock_path) and is_job_locked(lock_path):
        flash(f"Backup already running for job '{job_name}'.", "warning")
        return redirect(url_for("jobs.jobs"))

    backup_type = request.form.get("backup_type", "full").lower()
    if backup_type not in ("full", "diff"):
        flash("Invalid backup type.", "danger")
        return redirect(url_for("jobs.jobs"))

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
        with open('/tmp/backup_stdout.log', 'w') as out, open('/tmp/backup_stderr.log', 'w') as err:
            subprocess.Popen(
                args,
                stdout=out,
                stderr=err,
                start_new_session=True,
                cwd=BASE_DIR
            )
        flash(f"{backup_type.capitalize()} backup for {job_name} has been started.", "success")
    except Exception as e:
        flash(f"Failed to start backup: {e}", "danger")

    return redirect(url_for("jobs.jobs"))

@jobs_bp.route("/config/copy", methods=["POST"])
def copy_config():
    source = request.form.get("copy_source")
    new_job_name = request.form.get("new_job_name", "").strip()
    next_url = request.form.get("next") or url_for("jobs.jobs")

    # Validate job name (only allow letters, numbers, spaces, dashes, underscores)
    if not source or not new_job_name or not all(c.isalnum() or c in " _-" for c in new_job_name):
        flash("Invalid job name.", "danger")
        return redirect(url_for("jobs.jobs"))

    # Generate a safe file name
    base_name = secure_filename(new_job_name.replace(" ", "_"))
    if not base_name:
        flash("Invalid job name.", "danger")
        return redirect(url_for("jobs.jobs"))
    new_filename = f"{base_name}.yaml"

    # Determine source path (template or job)
    src_path = os.path.join(JOBS_DIR, source)
    dest_path = os.path.join(JOBS_DIR, new_filename)

    if not os.path.exists(src_path):
        flash("Source file does not exist.", "danger")
        return redirect(url_for("jobs.jobs"))
    if os.path.exists(dest_path):
        flash("A file with that job name already exists.", "danger")
        return redirect(url_for("jobs.jobs"))

    # Read as text, replace job_name, and write out (preserve comments/formatting)
    with open(src_path, "r") as src:
        content = src.read()
    # Replace the first occurrence of job_name: ... (with or without quotes)
    content_new = re.sub(
        r'^(job_name\s*:\s*)(["\']?.*?["\']?)\s*$', 
        r'\1"' + new_job_name + r'"', 
        content, 
        count=1, 
        flags=re.MULTILINE
    )
    with open(dest_path, "w") as dst:
        dst.write(content_new)

    flash(f"Copied {source} to {new_filename}.", "success")
    return redirect(url_for("config.edit_config", filename=new_filename, next=next_url))

@jobs_bp.route("/config/rename/<filename>", methods=["POST"])
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

@jobs_bp.route("/config/delete/<filename>", methods=["POST"])
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