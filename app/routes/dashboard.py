"""Flask routes for the JABS dashboard web interface, including job status, documentation, and storage tree views."""
import os
import json
import socket
from datetime import datetime

import markdown
import yaml
from cron_descriptor import get_description
import boto3

from flask import Blueprint, render_template, abort, current_app
from markupsafe import Markup
from app.settings import BASE_DIR, MANIFEST_BASE, GLOBAL_CONFIG_PATH, HOME_DIR
from app.utils.manifest import get_tarball_summary, get_merged_cleaned_yaml_config
from app.utils.dashboard_helpers import find_config_path_by_job_name, load_config

dashboard_bp = Blueprint('dashboard', 'dashboard')

def load_storage_config(config_path):
    """Load storage configuration from a YAML file."""
    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    drives = config.get("drives", [])
    s3_buckets = config.get("s3_buckets", [])
    return drives, s3_buckets

def build_local_tree(path):
    """Recursively build a tree structure for local directories and files."""
    node = {"name": os.path.basename(path) or path, "type": "directory", "children": []}
    try:
        for entry in os.scandir(path):
            if entry.is_dir(follow_symlinks=False):
                node["children"].append(build_local_tree(entry.path))
            else:
                node["children"].append({"name": entry.name, "type": "file"})
    except Exception:
        pass  # Permission errors, etc.
    return node

def build_s3_tree(bucket_name, prefix="", s3_client=None):
    """Recursively build a tree structure for an S3 bucket."""
    if s3_client is None:
        s3_client = boto3.client("s3")
    node = {"name": bucket_name if not prefix else prefix.rstrip('/'), "type": "folder", "children": []}
    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix, Delimiter='/'):
        for cp in page.get('CommonPrefixes', []):
            node["children"].append(build_s3_tree(bucket_name, cp['Prefix'], s3_client))
        for obj in page.get('Contents', []):
            if obj['Key'] != prefix:
                node["children"].append({"name": os.path.basename(obj['Key']), "type": "file"})
    return node

@dashboard_bp.route("/")
def dashboard():
    """Render the dashboard with scheduled jobs and their statuses."""
    jobs_dir = os.path.join(BASE_DIR, "config", "jobs")
    job_paths = [os.path.join(jobs_dir, fname) for fname in os.listdir(jobs_dir) if fname.endswith(".yaml")]

    with open('config/global.yaml', encoding="utf-8") as f:
        global_config = yaml.safe_load(f)

    scheduled_jobs = []
    for job_path in job_paths:
        with open(job_path, encoding="utf-8") as f:
            job_config = yaml.safe_load(f)

        # Effective AWS sync
        aws_enabled = job_config.get("aws", {}).get("enabled")
        if aws_enabled is None:
            aws_enabled = global_config.get("aws", {}).get("enabled", False)

        # Effective encryption
        encrypt_enabled = job_config.get("encryption", {}).get("enabled")
        if encrypt_enabled is None:
            encrypt_enabled = global_config.get("encryption", {}).get("enabled", False)

        enabled_schedules = []
        for s in job_config.get("schedules", []):
            if s.get("enabled"):
                cron_expr = s.get("cron", "")
                try:
                    s["cron_human"] = get_description(cron_expr)
                except Exception:
                    s["cron_human"] = cron_expr
                enabled_schedules.append(s)

        if enabled_schedules:
            scheduled_jobs.append({
                "job_name": job_config.get("job_name", os.path.basename(job_path)),
                "schedules": enabled_schedules,
                "sync": aws_enabled,
                "encrypt": encrypt_enabled,
            })

    return render_template("index.html", scheduled_jobs=scheduled_jobs, hostname=socket.gethostname())

@dashboard_bp.route("/documentation")
def documentation():
    """Render the documentation page from README.md."""
    readme_path = os.path.join(BASE_DIR, "README.md")
    if not os.path.exists(readme_path):
        content = "<p>README.md not found.</p>"
    else:
        with open(readme_path, "r", encoding="utf-8") as f:
            md_content = f.read()
        content = Markup(markdown.markdown(md_content, extensions=["fenced_code", "tables"]))
    return render_template("documentation.html", content=content)

@dashboard_bp.route('/manifest/<string:job_name>/<string:backup_set_id>')
def view_manifest(job_name, backup_set_id):
    """Render the manifest view for a specific job and backup set."""
    sanitized_job = "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in job_name)
    abs_json_path = os.path.join(BASE_DIR, MANIFEST_BASE, sanitized_job, f"{backup_set_id}.json")
    if not os.path.exists(abs_json_path):
        abort(404, description="Manifest file not found (os.path.exists failed).")
    with open(abs_json_path, "r", encoding="utf-8") as f:
        manifest_data = json.load(f)
    job_config_path = find_config_path_by_job_name(job_name)
    tarball_summary_list = []
    # Load global config for fallback
    with open(GLOBAL_CONFIG_PATH, encoding="utf-8") as f:
        global_config = yaml.safe_load(f)
    global_encryption = global_config.get("encryption", {})
    job_encryption = {}
    destination = None
    if job_config_path:
        job_config = load_config(job_config_path)
        # Use job destination if present, else global
        destination = job_config.get('destination') or global_config.get('destination')
        job_encryption = job_config.get("encryption", {})
        if destination:
            backup_set_path_on_dst = os.path.join(
                destination,
                socket.gethostname(),
                sanitized_job,
                f"backup_set_{backup_set_id}"
            )
            tarball_summary_list = get_tarball_summary(backup_set_path_on_dst)
    cleaned_config = get_merged_cleaned_yaml_config(job_config_path) if job_config_path else "Config file not found."
    manifest_timestamp = manifest_data.get("timestamp", "N/A")
    if manifest_timestamp != "N/A":
        try:
            dt_object = datetime.fromisoformat(manifest_timestamp)
            manifest_timestamp = dt_object.strftime("%A, %B %d, %Y at %I:%M %p")
        except Exception:
            pass
    used_config = manifest_data.get("config", {})
    return render_template(
        'manifest.html',
        job_name=manifest_data.get("job_name", job_name),
        backup_set_id=manifest_data.get("backup_set_id", backup_set_id),
        manifest_timestamp=manifest_timestamp,
        config_content=cleaned_config,
        all_files=manifest_data.get("files", []),
        tarball_summary=tarball_summary_list,
        used_config=used_config,
        HOME_DIR=HOME_DIR,
    )

@dashboard_bp.route('/storage-tree')
def storage_tree_view():
    """Render the storage tree view for local and S3 storage."""
    config_path = os.path.join(os.path.dirname(current_app.root_path), 'config', 'global.yaml')
    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    # Get the single local destination and AWS bucket
    destination = config.get("destination")
    aws_cfg = config.get("aws", {})
    bucket = aws_cfg.get("bucket")
    local_trees = []
    if destination:
        local_trees.append({
            "label": destination,
            "tree": build_local_tree(destination)
        })
    s3_trees = []
    if bucket:
        s3_client = boto3.client("s3")
        s3_trees.append({
            "label": bucket,
            "tree": build_s3_tree(bucket, s3_client=s3_client)
        })
    return render_template(
        "storage_tree_view.html",
        local_tree_json=json.dumps(local_trees),
        s3_tree_json=json.dumps(s3_trees)
    )
