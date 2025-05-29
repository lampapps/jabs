"""API routes for JABS: provides endpoints for restore, events, disk/S3 usage, logs, and manifest management."""

import os
import re
import glob
import json
import shutil
import time
import math

from flask import Blueprint, jsonify, send_from_directory, request, render_template, flash, url_for
import yaml
import boto3

from app.settings import BASE_DIR, LOG_DIR, EVENTS_FILE, GLOBAL_CONFIG_PATH, HOME_DIR, MAX_LOG_LINES
from core import restore
from app.utils.restore_status import check_restore_status
from app.utils.event_logger import load_events_locked, save_events_locked

api_bp = Blueprint('api', __name__)

def is_valid_path(path):
    """Check if a given path is valid and within HOME_DIR."""
    # Disallow empty, parent traversal, or illegal chars
    if not path or not isinstance(path, str):
        return False
    if any(c in path for c in '<>:"|?*'):
        return False
    # Make sure the resolved path is inside HOME_DIR
    abs_path = os.path.abspath(os.path.join(HOME_DIR, path))
    if not abs_path.startswith(HOME_DIR):
        return False
    return True

@api_bp.route('/api/restore/status/<job_name>/<backup_set_id>')
def restore_status(job_name, backup_set_id):
    """Return the running status of a restore job."""
    running = check_restore_status(job_name, backup_set_id)
    return jsonify({"running": running})

@api_bp.route("/api/events")
def get_events():
    """Return all events from the events file."""
    events = load_events_locked()
    return jsonify(events)

@api_bp.route('/data/dashboard/events.json')
def serve_events():
    """Serve the events.json file from the events directory."""
    events_dir = os.path.dirname(EVENTS_FILE)
    return send_from_directory(events_dir, "events.json")

@api_bp.route('/api/disk_usage')
def get_disk_usage():
    """Return disk usage statistics for configured drives."""
    # Use global.yaml instead of drives.yaml
    try:
        with open(GLOBAL_CONFIG_PATH, "r", encoding="utf-8") as f:
            global_config = yaml.safe_load(f)
            drives = global_config.get("drives", [])
            drive_labels = {d['path']: d.get('label', d['path']) for d in global_config.get('drives', [])}
    except FileNotFoundError:
        return jsonify({"error": f"Configuration file {GLOBAL_CONFIG_PATH} not found."}), 404
    except yaml.YAMLError as e:
        return jsonify({"error": f"Error parsing {GLOBAL_CONFIG_PATH}: {str(e)}"}), 500
    disk_usage = []
    for drive in drives:
        label = drive_labels.get(drive['path'], drive['path'])
        try:
            total, used, free = shutil.disk_usage(drive['path'])
            disk_usage.append({
                "drive": label,
                "total_gib": round(total / (1024 ** 3), 2),
                "used_gib": round(used / (1024 ** 3), 2),
                "free_gib": round(free / (1024 ** 3), 2),
                "percent_used": round((used / total) * 100, 2)
            })
        except FileNotFoundError:
            disk_usage.append({
                "drive": label,
                "error": "Drive not found or inaccessible"
            })
    return jsonify(disk_usage)

@api_bp.route('/api/s3_usage')
def get_s3_usage():
    """Return S3 bucket usage statistics for configured buckets."""
    # Use global.yaml instead of drives.yaml 
    try:
        with open(GLOBAL_CONFIG_PATH, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)
            s3_buckets = config.get("s3_buckets", [])
            bucket_labels = {}
            for b in s3_buckets:
                if isinstance(b, dict):
                    bucket_labels[b.get('bucket')] = b.get('label', b.get('bucket'))
                else:
                    bucket_labels[b] = b  # fallback for old format
    except FileNotFoundError:
        return jsonify({"error": f"Configuration file {GLOBAL_CONFIG_PATH} not found."}), 404
    except yaml.YAMLError as e:
        return jsonify({"error": f"Error parsing {GLOBAL_CONFIG_PATH}: {str(e)}"}), 500
    s3 = boto3.client("s3")
    s3_usage = []
    for bucket in s3_buckets:
        if isinstance(bucket, dict):
            bucket_name = bucket.get('bucket')
        else:
            bucket_name = bucket
        label = bucket_labels.get(bucket_name, bucket_name)
        bucket_data = {"bucket": label, "prefixes": []}
        try:
            paginator = s3.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=bucket_name, Delimiter="/"):
                if "CommonPrefixes" in page:
                    for prefix in page["CommonPrefixes"]:
                        prefix_name = prefix["Prefix"]
                        total_size = 0
                        sub_prefixes = []
                        for sub_page in paginator.paginate(Bucket=bucket_name, Prefix=prefix_name, Delimiter="/"):
                            if "CommonPrefixes" in sub_page:
                                for sub_prefix in sub_page["CommonPrefixes"]:
                                    sub_prefix_name = sub_prefix["Prefix"]
                                    sub_total_size = 0
                                    for obj_page in paginator.paginate(Bucket=bucket_name, Prefix=sub_prefix_name):
                                        if "Contents" in obj_page:
                                            sub_total_size += sum(obj["Size"] for obj in obj_page["Contents"])
                                    sub_prefixes.append({
                                        "prefix": sub_prefix_name.rstrip("/"),
                                        "size_gib": round(sub_total_size / (1024 ** 3), 2)
                                    })
                            if "Contents" in sub_page:
                                total_size += sum(obj["Size"] for obj in sub_page["Contents"])
                        bucket_data["prefixes"].append({
                            "prefix": prefix_name.rstrip("/"),
                            "size_gib": round(total_size / (1024 ** 3), 2),
                            "sub_prefixes": sub_prefixes
                        })
        except Exception as e:
            bucket_data["error"] = str(e)
        s3_usage.append(bucket_data)
    return jsonify(s3_usage)

@api_bp.route('/api/trim_logs', methods=['POST'])
def trim_logs():
    """Trim log files in the log directory to a maximum number of lines."""
    log_dir = LOG_DIR
    max_lines = MAX_LOG_LINES
    if not os.path.exists(log_dir):
        return jsonify({"error": "Log directory does not exist"}), 404
    trimmed_logs = []
    log_files = glob.glob(f"{log_dir}/*.log")
    if not log_files:
        return jsonify({"error": "No log files found in the logs directory"}), 404
    for log_file in log_files:
        try:
            with open(log_file, "r", encoding="utf-8") as f:
                lines = f.readlines()
            if len(lines) > max_lines:
                with open(log_file, "w", encoding="utf-8") as f:
                    f.writelines(lines[-max_lines:])
                trimmed_logs.append({"file": log_file, "status": "trimmed"})
            else:
                trimmed_logs.append({"file": log_file, "status": "not trimmed (already small)"})
        except Exception as e:
            trimmed_logs.append({"file": log_file, "status": f"error: {str(e)}"})
    return jsonify({"trimmed_logs": trimmed_logs})

@api_bp.route('/api/manifest/<string:job_name>/<string:backup_set_id>/json')
def api_manifest_json(job_name, backup_set_id):
    """Return the manifest JSON for a specific job and backup set."""
    sanitized_job = "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in job_name)
    json_path = os.path.join(BASE_DIR, "data", "manifests", sanitized_job, f"{backup_set_id}.json")
    if not os.path.exists(json_path):
        return jsonify({"error": "Manifest not found"}), 404
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": "Failed to read manifest file"}), 500

@api_bp.route('/api/scheduler_status')
def get_scheduler_status():
    """Return the status and last run time of the scheduler."""
    status_file = os.path.join(LOG_DIR, "scheduler.status")
    stale_threshold_seconds = 3600 + 300
    status = "unknown"
    last_run_timestamp = None
    age_seconds = None
    message = "Scheduler status file not found or unreadable."
    if os.path.exists(status_file):
        try:
            with open(status_file, 'r', encoding="utf-8") as f:
                last_run_timestamp_str = f.read().strip()
            last_run_timestamp = float(last_run_timestamp_str)
            age_seconds = time.time() - last_run_timestamp
            age_minutes = age_seconds / 60.0
            if age_minutes < 1:
                time_ago_str = f"{int(age_seconds)} seconds ago"
            elif age_minutes < 2:
                time_ago_str = "about 1 minute ago"
            else:
                time_ago_str = f"about {int(math.floor(age_minutes))} minutes ago"
            if age_seconds < stale_threshold_seconds:
                status = "ok"
                message = f"Scheduler last run {time_ago_str}."
            else:
                status = "stale"
                threshold_minutes = int(math.ceil(stale_threshold_seconds / 60.0))
                message = f"Scheduler last run {time_ago_str} (older than threshold: ~{threshold_minutes} min)."
        except ValueError:
            status = "error"
            message = "Scheduler status file contains invalid data."
        except Exception as e:
            status = "error"
            message = f"Error reading scheduler status file: {e}"
    else:
        status = "error"
    return jsonify({
        "status": status,
        "last_run_timestamp": last_run_timestamp,
        "age_seconds": age_seconds,
        "message": message,
        "threshold_seconds": stale_threshold_seconds
    })

@api_bp.route('/api/purge_log/<log_name>', methods=['POST'])
def purge_log(log_name):
    """Purge the contents of a log file, only allowing .log files."""
    # Only allow .log files, no path traversal
    if not re.match(r'^[\w\-.]+\.log$', log_name):
        return jsonify({"success": False, "error": "Invalid log name"}), 400
    log_path = os.path.join(LOG_DIR, log_name)
    if not os.path.exists(log_path):
        return jsonify({"success": False, "error": "Log not found"}), 404
    try:
        with open(log_path, "w", encoding="utf-8") as f:
            f.truncate(0)
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@api_bp.route('/api/restore/full', methods=['POST'])
def restore_full():
    """Perform a full restore for a given job and backup set."""
    data = request.json
    job_name = data['job_name']
    backup_set_id = data['backup_set_id']
    restore_location = data.get('restore_location', 'original')
    custom_path = data.get('custom_path', None)

    # Only set dest for custom restore
    if restore_location == "custom":
        if not is_valid_path(custom_path):
            return jsonify({"error": "Invalid custom path."}), 400
        dest = os.path.abspath(os.path.join(HOME_DIR, custom_path))
    else:
        dest = None

    result = restore.restore_full(job_name, backup_set_id, dest=dest, base_dir=BASE_DIR)
    if result.get("overwrite_warnings"):
        return jsonify({
            "error": "Some files will be overwritten.",
            "files": result["overwrite_warnings"]
        }), 409
    if result["errors"]:
        first = result["errors"][0]
        flash(f"{first['error']}", "danger")
    else:
        flash("Full restore completed.", "success")
    return jsonify({"redirect": url_for('dashboard.view_manifest', job_name=job_name, backup_set_id=backup_set_id)})

@api_bp.route('/api/restore/files', methods=['POST'])
def restore_files():
    """Restore selected files for a given job and backup set."""
    data = request.json
    job_name = data['job_name']
    backup_set_id = data['backup_set_id']
    files = data.get('files', [])
    restore_location = data.get('restore_location', 'original')
    custom_path = data.get('custom_path', None)

    # Only set dest for custom restore
    if restore_location == "custom":
        if not is_valid_path(custom_path):
            return jsonify({"error": "Invalid custom path."}), 400
        dest = os.path.abspath(os.path.join(HOME_DIR, custom_path))
    else:
        dest = None

    if not files or not isinstance(files, list):
        return jsonify({"error": "No files selected for restore."}), 400

    result = restore.restore_files(job_name, backup_set_id, files, dest=dest, base_dir=BASE_DIR)
    if result.get("overwrite_warnings"):
        return jsonify({
            "error": "Some files will be overwritten.",
            "files": result["overwrite_warnings"]
        }), 409
    if result["errors"]:
        first = result["errors"][0]
        flash(f"{first['error']}", "danger")
    else:
        flash("Selected files restored.", "success")
    return jsonify({"redirect": url_for('dashboard.view_manifest', job_name=job_name, backup_set_id=backup_set_id)})

@api_bp.route('/manifest/<string:job_name>/<string:backup_set_id>')
def manifest_page(job_name, backup_set_id):
    """Render the manifest page for a specific job and backup set."""
    return render_template(
        "manifest.html",
        job_name=job_name,
        backup_set_id=backup_set_id,
        HOME_DIR=HOME_DIR
    )

@api_bp.route('/api/events/delete', methods=['POST'])
def delete_events():
    """Delete events by ID and remove corresponding manifest files if they exist."""

    data = request.get_json()
    ids = data.get('ids', [])
    if not ids:
        return jsonify({"message": "No IDs provided."}), 400

    events_json = load_events_locked()
    events = events_json.get("data", [])

    # Find events to delete (so we can remove their manifests if they exist)
    events_to_delete = [e for e in events if str(e.get('id') or e.get('starttimestamp')) in ids]

    # Remove events from the list
    events = [e for e in events if str(e.get('id') or e.get('starttimestamp')) not in ids]
    events_json["data"] = events

    # Save updated events file atomically
    save_events_locked(events_json)

    # Remove corresponding manifest files if they exist
    for event in events_to_delete:
        job_name = event.get('job_name')
        backup_set_id = event.get('backup_set_id')
        if job_name and backup_set_id:
            sanitized_job = "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in job_name)
            manifest_dir = os.path.join(BASE_DIR, "data", "manifests", sanitized_job)
            json_path = os.path.join(manifest_dir, f"{backup_set_id}.json")
            html_path = os.path.join(manifest_dir, f"{backup_set_id}.html")
            for path in [json_path, html_path]:
                try:
                    if os.path.exists(path):
                        os.remove(path)
                except Exception:
                    pass  # Ignore errors if file does not exist or cannot be deleted

    return jsonify({"message": f"Deleted {len(ids)} event(s) and any corresponding manifests. Remove any remaining backup sets manually."})
