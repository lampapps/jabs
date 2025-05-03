from flask import Blueprint, render_template, jsonify, request, send_from_directory, abort
import os
import glob
import json
import yaml
import subprocess
import shutil
import boto3
from datetime import datetime
from app.utils.manifest import MANIFEST_BASE, get_cleaned_yaml_config, get_tarball_summary, sizeof_fmt
import traceback
import re
import time
import math
from app.config import BASE_DIR, CONFIG_DIR, LOG_DIR, MANIFEST_BASE, EVENTS_FILE

dashboard_bp = Blueprint('dashboard', __name__)

# --- Helper Functions for Config Loading ---
def find_config_path_by_job_name(target_job_name):
    if not os.path.isdir(CONFIG_DIR):
        print(f"Error: Configuration directory not found at {CONFIG_DIR}")
        return None
    for filename in os.listdir(CONFIG_DIR):
        if filename.endswith((".yaml", ".yml")):
            file_path = os.path.join(CONFIG_DIR, filename)
            try:
                with open(file_path, 'r') as f:
                    config_data = yaml.safe_load(f)
                    if isinstance(config_data, dict) and config_data.get('job_name') == target_job_name:
                        return file_path
            except yaml.YAMLError:
                print(f"Warning: Could not parse YAML file {filename}")
                continue
            except Exception as e:
                print(f"Warning: Error reading file {filename}: {e}")
                continue
    return None

def load_config(config_path):
    if not config_path or not os.path.exists(config_path):
        return None
    try:
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    except Exception as e:
        print(f"Error loading config file {config_path}: {e}")
        return None

# --- Routes ---
@dashboard_bp.route("/")
def dashboard():
    return render_template("index.html")

@dashboard_bp.route("/logs")
def logs():
    log_dir = os.path.join(BASE_DIR, "logs")
    logs = []
    for log_file in sorted(glob.glob(f"{log_dir}/*.log")):
        with open(log_file) as f:
            content = f.read()
        logs.append((os.path.basename(log_file), content))
    return render_template("logs.html", logs=logs)

@dashboard_bp.route("/config.html")
def config():
    configs = []
    print("CONFIG_DIR is:", CONFIG_DIR)
    for yaml_file in sorted(os.listdir(CONFIG_DIR)):
        if yaml_file.endswith(".yaml"):
            file_path = os.path.join(CONFIG_DIR, yaml_file)
            with open(file_path, "r") as f:
                raw_data = f.read()
                config_data = yaml.safe_load(raw_data)
                configs.append({"file_name": yaml_file, "data": config_data, "raw_data": raw_data})
    return render_template("config.html", configs=configs)

@dashboard_bp.route("/api/events")
def get_events():
    events_file = os.path.join(BASE_DIR, "data", "dashboard", "events.json")
    if os.path.exists(events_file):
        with open(events_file) as f:
            events = json.load(f)
    else:
        events = {"data": []}
    return jsonify(events)

@dashboard_bp.route("/cronstatus.html")
def cronstatus():
    return render_template("cronstatus.html")

@dashboard_bp.route('/data/dashboard/events.json')
def serve_events():
    events_dir = os.path.join(BASE_DIR, "data", "dashboard")
    return send_from_directory(events_dir, "events.json")

@dashboard_bp.route('/api/disk_usage')
def get_disk_usage():
    drives_config_path = os.path.join(CONFIG_DIR, "drives.yaml")
    try:
        with open(drives_config_path, "r") as f:
            drives_config = yaml.safe_load(f)
            drives = drives_config.get("drives", [])
    except FileNotFoundError:
        return jsonify({"error": f"Configuration file {drives_config_path} not found."}), 404
    except yaml.YAMLError as e:
        return jsonify({"error": f"Error parsing {drives_config_path}: {str(e)}"}), 500
    disk_usage = []
    for drive in drives:
        try:
            total, used, free = shutil.disk_usage(drive)
            disk_usage.append({
                "drive": drive,
                "total_gib": round(total / (1024 ** 3), 2),
                "used_gib": round(used / (1024 ** 3), 2),
                "free_gib": round(free / (1024 ** 3), 2),
                "percent_used": round((used / total) * 100, 2)
            })
        except FileNotFoundError:
            disk_usage.append({
                "drive": drive,
                "error": "Drive not found or inaccessible"
            })
    return jsonify(disk_usage)

@dashboard_bp.route('/api/s3_usage')
def get_s3_usage():
    s3_config_path = os.path.join(CONFIG_DIR, "drives.yaml")
    try:
        with open(s3_config_path, "r") as f:
            config = yaml.safe_load(f)
            s3_buckets = config.get("s3_buckets", [])
    except FileNotFoundError:
        return jsonify({"error": f"Configuration file {s3_config_path} not found."}), 404
    except yaml.YAMLError as e:
        return jsonify({"error": f"Error parsing {s3_config_path}: {str(e)}"}), 500
    s3 = boto3.client("s3")
    s3_usage = []
    for bucket_name in s3_buckets:
        bucket_data = {"bucket": bucket_name, "prefixes": []}
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

@dashboard_bp.route('/api/trim_logs', methods=['POST'])
def trim_logs():
    log_dir = os.path.join(BASE_DIR, "logs")
    max_lines = 1000
    if not os.path.exists(log_dir):
        return jsonify({"error": "Log directory does not exist"}), 404
    trimmed_logs = []
    log_files = glob.glob(f"{log_dir}/*.log")
    if not log_files:
        return jsonify({"error": "No log files found in the logs directory"}), 404
    for log_file in log_files:
        try:
            with open(log_file, "r") as f:
                lines = f.readlines()
            if len(lines) > max_lines:
                with open(log_file, "w") as f:
                    f.writelines(lines[-max_lines:])
                trimmed_logs.append({"file": log_file, "status": "trimmed"})
            else:
                trimmed_logs.append({"file": log_file, "status": "not trimmed (already small)"})
        except Exception as e:
            trimmed_logs.append({"file": log_file, "status": f"error: {str(e)}"})
    return jsonify({"trimmed_logs": trimmed_logs})

@dashboard_bp.route('/manifest/<string:job_name>/<string:backup_set_id>')
def view_manifest(job_name, backup_set_id):
    sanitized_job = "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in job_name)
    abs_json_path = os.path.join(BASE_DIR, MANIFEST_BASE, sanitized_job, f"{backup_set_id}.json")
    if not os.path.exists(abs_json_path):
         abort(404, description="Manifest file not found (os.path.exists failed).")
    try:
        with open(abs_json_path, "r") as f:
            manifest_data = json.load(f)
    except Exception as e:
        abort(500, description="Error reading manifest file.")
    job_config_path = find_config_path_by_job_name(job_name)
    tarball_summary_list = []
    job_config = None
    if job_config_path:
        job_config = load_config(job_config_path)
        if job_config and 'destination' in job_config:
            backup_set_path_on_dst = os.path.join(
                job_config['destination'],
                sanitized_job,
                f"backup_set_{backup_set_id}"
            )
            if os.path.isdir(backup_set_path_on_dst):
                tarball_files = glob.glob(os.path.join(backup_set_path_on_dst, '*.tar.gz')) + \
                glob.glob(os.path.join(backup_set_path_on_dst, '*.tar.gz.gpg'))
                summary_data_for_sorting = []
                timestamp_pattern = re.compile(r'_(\d{8}_\d{6})\.tar\.gz$')
                for tar_path in tarball_files:
                    basename = os.path.basename(tar_path)
                    timestamp_str = '00000000_000000'
                    match = timestamp_pattern.search(basename)
                    if match:
                        timestamp_str = match.group(1)
                    is_encrypted = basename.endswith('.gpg')
                    try:
                        size_bytes = os.path.getsize(tar_path)
                        summary_data_for_sorting.append({
                            "name": basename,
                            "size": sizeof_fmt(size_bytes),
                            "timestamp_str": timestamp_str,
                            "encrypted": is_encrypted
                        })
                    except Exception as e:
                        summary_data_for_sorting.append({
                            "name": basename,
                            "size": "Error",
                            "timestamp_str": timestamp_str,
                            "encrypted": is_encrypted
                        })
                tarball_summary_list = sorted(summary_data_for_sorting, key=lambda item: item['timestamp_str'], reverse=True)
    cleaned_config = get_cleaned_yaml_config(job_config_path) if job_config_path else "Config file not found."
    manifest_timestamp = manifest_data.get("timestamp", "N/A")
    if manifest_timestamp != "N/A":
        try:
            dt_object = datetime.fromisoformat(manifest_timestamp)
            manifest_timestamp = dt_object.strftime("%A, %B %d, %Y at %I:%M %p")
        except Exception:
            pass
    return render_template(
        'manifest.html',
        job_name=manifest_data.get("job_name", job_name),
        backup_set_id=manifest_data.get("backup_set_id", backup_set_id),
        manifest_timestamp=manifest_timestamp,
        config_content=cleaned_config,
        all_files=manifest_data.get("files", []),
        tarball_summary=tarball_summary_list
    )

@dashboard_bp.route('/api/manifest/<string:job_name>/<string:backup_set_id>')
def api_manifest(job_name, backup_set_id):
    sanitized_job = "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in job_name)
    json_path = os.path.join(BASE_DIR, MANIFEST_BASE, sanitized_job, f"{backup_set_id}.json")
    if not os.path.exists(json_path):
        return jsonify({"error": "Manifest not found"}), 404
    try:
        with open(json_path, "r") as f:
            data = json.load(f)
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": "Failed to read manifest file"}), 500

@dashboard_bp.route('/api/scheduler_status')
def get_scheduler_status():
    status_file = os.path.join(LOG_DIR, "scheduler.status")
    stale_threshold_seconds = 3600 + 300  # Default threshold
    status = "unknown"
    last_run_timestamp = None
    age_seconds = None
    message = "Scheduler status file not found or unreadable."
    if os.path.exists(status_file):
        try:
            with open(status_file, 'r') as f:
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