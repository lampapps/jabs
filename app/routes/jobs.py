from flask import Blueprint, render_template, request, redirect, url_for, flash
import os
import yaml
import subprocess
import sys
from app.settings import LOCK_DIR, BASE_DIR, JOBS_DIR, GLOBAL_CONFIG_PATH

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
                # Use job value if present, else global
                job_name = data.get("job_name", fname.replace(".yaml", ""))
                source = data.get("source", "")
                destination = data.get("destination") or global_config.get("destination")
                aws = data.get("aws") or global_config.get("aws")
            except Exception:
                job_name = fname.replace(".yaml", "")
                source = ""
                destination = global_config.get("destination")
                aws = global_config.get("aws")
                data = {}
            jobs.append({
                "file_name": fname,
                "job_name": job_name,
                "source": source,
                "destination": destination,
                "aws": aws,
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

    return render_template("jobs.html", configs=jobs, templates=templates)

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
    lock_path = os.path.join(LOCK_DIR, f"{job_name}.lock")
    if os.path.exists(lock_path) and is_job_locked(lock_path):
        flash(f"Backup already running for job '{job_name}'.", "warning")
        return redirect(url_for("jobs.jobs"))

    backup_type = request.form.get("backup_type", "full").lower()
    if backup_type not in ("full", "diff"):
        flash("Invalid backup type.", "danger")
        return redirect(url_for("jobs.jobs"))

    sync = request.form.get("sync", "0")
    cli_path = os.path.join(BASE_DIR, "cli.py")
    args = [sys.executable, cli_path, config_path]
    if backup_type == "full":
        args.append("--full")
    else:
        args.append("--diff")
    if sync == "1":
        args.append("--sync")

    try:
        subprocess.Popen(
            args,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
            cwd=BASE_DIR
        )
        flash(f"{backup_type.capitalize()} backup for {job_name} has been started.", "success")
    except Exception as e:
        flash(f"Failed to start backup: {e}", "danger")

    return redirect(url_for("jobs.jobs"))