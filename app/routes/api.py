"""API routes for JABS: provides endpoints for restore, events, disk/S3 usage, logs, and manifest management."""
 
import os
import re
import glob
import json
import shutil
import time
import math
import yaml
import boto3
import socket
from datetime import datetime

from flask import (
    Blueprint, jsonify, send_from_directory, request, render_template, flash, url_for
)

from app.settings import (
    BASE_DIR, LOG_DIR, GLOBAL_CONFIG_PATH, HOME_DIR, MAX_LOG_LINES, SCHEDULER_EVENTS_PATH, VERSION, SCHEDULER_STATUS_FILE
)
from core import restore
from app.utils.restore_status import check_restore_status
from app.models.events import get_all_events, count_error_events
from app.utils.poll_targets import poll_targets

from app.models.backup_sets import get_backup_set_by_job_and_set, delete_backup_set
from app.services.manifest import get_manifest_with_files
from app.models.db_core import get_db_connection

api_bp = Blueprint('api', __name__)

def is_valid_path(path):
    """Check if a given path is valid and within HOME_DIR."""
    if not path or not isinstance(path, str):
        return False
    if any(c in path for c in '<>:"|?*'):
        return False
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
    """Return all events from the database."""
    events = get_all_events()
    return jsonify(events)

@api_bp.route('/data/dashboard/events.json')
def serve_events():
    """Serve the events from the database in JSON format."""
    return jsonify(get_all_events())

@api_bp.route('/api/disk_usage')
def get_disk_usage():
    """Return disk usage statistics for configured drives."""
    try:
        with open(GLOBAL_CONFIG_PATH, "r", encoding="utf-8") as f:
            global_config = yaml.safe_load(f)
            drives = global_config.get("drives", [])
            drive_labels = {
                d['path']: d.get('label', d['path'])
                for d in global_config.get('drives', [])
            }
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
    # Check for AWS credentials before proceeding
    session = boto3.Session()
    credentials = session.get_credentials()
    if credentials is None or not credentials.access_key or not credentials.secret_key:
        return jsonify({"error": "AWS credentials not found."}), 403

    try:
        with open(GLOBAL_CONFIG_PATH, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)
            s3_buckets = config.get("s3_buckets", [])
            bucket_labels = {}
            for b in s3_buckets:
                if isinstance(b, dict):
                    bucket_labels[b.get('bucket')] = b.get('label', b.get('bucket'))
                else:
                    bucket_labels[b] = b
    except FileNotFoundError:
        return jsonify({"error": f"Configuration file {GLOBAL_CONFIG_PATH} not found."}), 404
    except yaml.YAMLError as e:
        return jsonify({"error": f"Error parsing {GLOBAL_CONFIG_PATH}: {str(e)}"}), 500

    s3 = session.client("s3")
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
                        for sub_page in paginator.paginate(
                            Bucket=bucket_name, Prefix=prefix_name, Delimiter="/"
                        ):
                            if "CommonPrefixes" in sub_page:
                                for sub_prefix in sub_page["CommonPrefixes"]:
                                    sub_prefix_name = sub_prefix["Prefix"]
                                    sub_total_size = 0
                                    for obj_page in paginator.paginate(
                                        Bucket=bucket_name, Prefix=sub_prefix_name
                                    ):
                                        if "Contents" in obj_page:
                                            sub_total_size += sum(
                                                obj["Size"] for obj in obj_page["Contents"]
                                            )
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
        except boto3.exceptions.Boto3Error as e:
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
        except OSError as e:
            trimmed_logs.append({"file": log_file, "status": f"error: {str(e)}"})
    return jsonify({"trimmed_logs": trimmed_logs})

@api_bp.route('/api/manifest/<string:job_name>/<string:backup_set_id>/json')
def api_manifest_json(job_name, backup_set_id):
    """Return the manifest JSON for a specific job and backup set from SQLite database."""
    
    # Keep the original job name for database lookup
    original_job_name = job_name
    
    # Sanitize the job name for any file path operations (if needed)
    sanitized_job = "".join(
        c if c.isalnum() or c in ("-", "_") else "_" for c in job_name
    )
    
    # Get manifest data from database using the original job name
    manifest_data = get_manifest_with_files(original_job_name, backup_set_id)
    if not manifest_data:
        return jsonify({"error": f"Manifest not found for job '{original_job_name}' and backup set '{backup_set_id}'"}), 404
    
    try:
        # Format the data for the JavaScript DataTable
        files_for_table = []
        for file_data in manifest_data.get('files', []):
            # Format timestamp for display
            modified_display = "N/A"
            if file_data.get('mtime'):
                try:
                    dt = datetime.fromtimestamp(file_data['mtime'])
                    modified_display = dt.strftime("%Y-%m-%d %H:%M:%S")
                except (ValueError, TypeError):
                    modified_display = "N/A"
            
            # Format size for display
            size_display = file_data.get('size', 0)
            if isinstance(size_display, (int, float)):
                # Convert bytes to human readable format
                def sizeof_fmt(num, suffix='B'):
                    for unit in ['','K','M','G','T','P','E','Z']:
                        if abs(num) < 1024.0:
                            return f"{num:3.1f}{unit}{suffix}"
                        num /= 1024.0
                    return f"{num:.1f}Y{suffix}"
                size_display = sizeof_fmt(size_display)
            
            files_for_table.append({
                'tarball': file_data.get('tarball', 'unknown'),
                'tarball_path': file_data.get('tarball', 'unknown'),  # For checkbox data attribute
                'path': file_data.get('path', ''),
                'size': size_display,
                'modified': modified_display
            })
        
        return jsonify({
            'job_name': manifest_data.get('job_name'),
            'set_name': manifest_data.get('set_name'),  # Separate set_name from new schema
            'backup_set_id': backup_set_id,  # For compatibility
            'backup_type': manifest_data.get('backup_type'),
            'status': manifest_data.get('status'),
            'event': manifest_data.get('event'),
            'timestamp': manifest_data.get('timestamp'),
            'started_at': manifest_data.get('started_at'),
            'completed_at': manifest_data.get('completed_at'),
            'files': files_for_table
        })
        
    except Exception as e:
        return jsonify({"error": f"Failed to process manifest data: {str(e)}"}), 500

@api_bp.route('/api/scheduler_status')
def get_scheduler_status():
    """Return the status and last run time of the scheduler."""
    status_file = SCHEDULER_STATUS_FILE
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
                message = (
                    f"Scheduler last run {time_ago_str} "
                    f"(older than threshold: ~{threshold_minutes} min)."
                )
        except ValueError:
            status = "error"
            message = "Scheduler status file contains invalid data."
        except OSError as e:
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
    if not re.match(r'^[\w\-.]+\.log$', log_name):
        return jsonify({"success": False, "error": "Invalid log name"}), 400
    log_path = os.path.join(LOG_DIR, log_name)
    if not os.path.exists(log_path):
        return jsonify({"success": False, "error": "Log not found"}), 404
    try:
        with open(log_path, "w", encoding="utf-8") as f:
            f.truncate(0)
        return jsonify({"success": True})
    except OSError as e:
        return jsonify({"success": False, "error": str(e)}), 500

@api_bp.route('/api/restore/full', methods=['POST'])
def restore_full():
    """Perform a full restore for a given job and backup set."""
    data = request.json
    job_name = data['job_name']
    backup_set_id = data['backup_set_id']
    restore_location = data.get('restore_location', 'original')
    custom_path = data.get('custom_path', None)

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
    return jsonify({
        "redirect": url_for('dashboard.view_manifest', job_name=job_name, backup_set_id=backup_set_id)
    })

@api_bp.route('/api/restore/files', methods=['POST'])
def restore_files():
    """Restore selected files for a given job and backup set."""
    data = request.json
    job_name = data['job_name']
    backup_set_id = data['backup_set_id']
    files = data.get('files', [])
    restore_location = data.get('restore_location', 'original')
    custom_path = data.get('custom_path', None)

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
    return jsonify({
        "redirect": url_for('dashboard.view_manifest', job_name=job_name, backup_set_id=backup_set_id)
    })

@api_bp.route('/manifest/<string:job_name>/<string:backup_set_id>')
def manifest_page(job_name, backup_set_id):
    """Render the manifest page for a specific job and backup set."""
    return render_template(
        "manifest.html",
        job_name=job_name,
        backup_set_id=backup_set_id,
        HOME_DIR=HOME_DIR
    )

@api_bp.route("/api/events/delete", methods=["POST"])
def delete_events():
    """Delete events by ID directly from the database."""
    data = request.get_json()
    ids = data.get('ids', [])
    if not ids:
        return jsonify({"message": "No IDs provided."}), 400

    deleted_count = 0
    deleted_backup_sets = set()
    
    # Get the events data for reference before deletion
    events_data = []
    with get_db_connection() as conn:
        c = conn.cursor()
        placeholders = ",".join("?" for _ in ids)
        c.execute(f"SELECT id, job_name, backup_set_id FROM events WHERE id IN ({placeholders})", ids)
        events_data = [dict(row) for row in c.fetchall()]
        
    # Delete each event by removing the corresponding backup job
    for event_data in events_data:
        event_id = event_data.get("id")
        job_name = event_data.get("job_name")
        backup_set_id = event_data.get("backup_set_id")
        
        try:
            with get_db_connection() as conn:
                c = conn.cursor()
                # Delete the backup job
                c.execute("DELETE FROM backup_jobs WHERE id = ?", (event_id,))
                conn.commit()
                
                if c.rowcount > 0:
                    deleted_count += 1
                    
                    # Check if we should also delete the backup set (if no more jobs)
                    if backup_set_id:
                        c.execute("SELECT COUNT(*) FROM backup_jobs WHERE backup_set_id = ?", (backup_set_id,))
                        if c.fetchone()[0] == 0:
                            # No more jobs for this set, delete it
                            deleted_backup_sets.add((backup_set_id, job_name))
        except Exception as e:
            print(f"Error deleting event {event_id}: {e}")
    
    # Delete any backup sets that have no more jobs
    for backup_set_id, job_name in deleted_backup_sets:
        try:
            # Remove manifest files (legacy)
            sanitized_job = "".join(
                c if c.isalnum() or c in ("-", "_") else "_" for c in job_name
            )
            manifest_dir = os.path.join(BASE_DIR, "data", "manifests", sanitized_job)
            
            # Find all manifest files for this backup set
            if os.path.exists(manifest_dir):
                for filename in os.listdir(manifest_dir):
                    if filename.startswith(f"{backup_set_id}."):
                        os.remove(os.path.join(manifest_dir, filename))
                        
            # Delete the backup set from the database
            delete_backup_set(backup_set_id)
        except Exception as e:
            print(f"Error cleaning up after event deletion: {e}")
    
    return jsonify({
        "success": True, 
        "deleted": deleted_count,
        "message": f"Successfully deleted {deleted_count} event(s)."
    })

@api_bp.route("/data/dashboard/scheduler_events.json")
def get_scheduler_events():
    """Return the scheduler events as JSON for the dashboard mini chart."""
    path = SCHEDULER_EVENTS_PATH
    if not os.path.exists(path):
        return jsonify([])
    with open(path, "r", encoding="utf-8") as f:
        try:
            return jsonify(json.load(f))
        except json.JSONDecodeError:
            return jsonify([])

@api_bp.route("/api/monitor_status")
def monitor_status():
    """Return the status of monitored targets."""
    # Load targets from config
    with open("config/global.yaml") as f:
        config = yaml.safe_load(f)
    targets = config.get("monitored_targets", [])
    return jsonify(poll_targets(targets))


@api_bp.route("/api/heartbeat")
def heartbeat():
    """Return basic health/status info for this JABS instance."""
    status_file = SCHEDULER_STATUS_FILE
    last_run = None
    last_run_str = None
    if os.path.exists(status_file):
        try:
            with open(status_file, "r", encoding="utf-8") as f:
                ts = float(f.read().strip())
                last_run = ts
                last_run_str = datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            last_run = None
            last_run_str = None

    # Count events with status == "error" from database
    error_event_count = count_error_events()

    return jsonify({
        "hostname": socket.gethostname(),
        "version": VERSION,
        "status": "ok",
        "last_scheduler_run": last_run,
        "last_scheduler_run_str": last_run_str,
        "error_event_count": error_event_count
    })