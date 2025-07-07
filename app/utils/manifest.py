"""Utilities for handling backup manifest files, summaries, and YAML config merging."""

import os
import re
import glob
import json
import tarfile
import copy
import time
from datetime import datetime
from collections import defaultdict

import yaml
from jinja2 import Environment, FileSystemLoader, TemplateError

from app.utils.logger import ensure_dir, sizeof_fmt
from app.settings import MANIFEST_BASE, GLOBAL_CONFIG_PATH


def merge_configs(global_config, job_config):
    """
    Recursively merge two configuration dictionaries, with job_config taking precedence.
    """
    merged = copy.deepcopy(global_config)
    for key, value in job_config.items():
        if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
            merged[key] = merge_configs(merged[key], value)
        else:
            merged[key] = value
    return merged

def extract_tar_info(tar_path, encryption_enabled=False):
    """
    Extract file info from a tar archive for manifest purposes.
    Returns a list of file info dictionaries.
    """
    files_info = []
    base = os.path.basename(tar_path)
    recorded_base = base + '.gpg' if encryption_enabled and not base.endswith('.gpg') else base
    recorded_tar_path = tar_path + '.gpg' if encryption_enabled and not tar_path.endswith('.gpg') else tar_path
    try:
        with tarfile.open(tar_path, "r:*") as tar:
            for member in tar.getmembers():
                if member.isfile():
                    files_info.append({
                        "tarball": recorded_base,
                        "tarball_path": recorded_tar_path,
                        "path": member.name,
                        "size": member.size,  # Store numeric bytes (not formatted string)
                        "mtime": member.mtime
                    })
    except (tarfile.TarError, OSError) as e:
        print(f"Error reading tar file {tar_path}: {e}")
    return files_info

def build_tarball_summary_from_manifest(files_list):
    """
    Build a summary of tarballs from a list of file info dictionaries (from manifest).
    Sums sizes and extracts timestamps for each tarball.
    :param files_list: List of file info dicts (from manifest).
    :return: List of dicts summarizing each tarball.
    """
    tarballs = defaultdict(lambda: {"size_bytes": 0, "timestamp_str": "00000000_000000"})
    timestamp_pattern = re.compile(r'_(\d{8}_\d{6})\.tar\.gz')
    for f in files_list:
        tarball_name = f.get("tarball") or f.get("name")
        if not tarball_name:
            continue
        match = timestamp_pattern.search(tarball_name)
        if match:
            tarballs[tarball_name]["timestamp_str"] = match.group(1)
        
        # Handle size - now expecting numeric bytes from database
        size_val = f.get("size", 0)
        if isinstance(size_val, (int, float)):
            # Numeric bytes - use directly
            tarballs[tarball_name]["size_bytes"] += size_val
        elif isinstance(size_val, str):
            # String size - parse it (backward compatibility if needed)
            tarballs[tarball_name]["size_bytes"] += parse_size_to_bytes(size_val)
    
    summary = []
    for name, info in tarballs.items():
        summary.append({
            "name": name,
            "size": sizeof_fmt(info["size_bytes"]),
            "size_bytes": info["size_bytes"],
            "timestamp_str": info["timestamp_str"],
        })
    return sorted(summary, key=lambda item: item['timestamp_str'], reverse=True)

def parse_size_to_bytes(size_str):
    """
    Convert a human-readable size string (e.g., '1.2 MB') to bytes as an integer.
    :param size_str: Size string.
    :return: Size in bytes (int).
    """
    size_str = size_str.strip()
    units = {
        "B": 1, "KB": 1024, "KIB": 1024, "MB": 1024**2, "MIB": 1024**2,
        "GB": 1024**3, "GIB": 1024**3, "TB": 1024**4, "TIB": 1024**4
    }
    match = re.match(r"([\d.]+)\s*([KMGT]?i?B)", size_str, re.I)
    if not match:
        try:
            return int(size_str)
        except ValueError:
            return 0
    value, unit = match.groups()
    return int(float(value) * units[unit.upper()])

def write_manifest_files(
    job_config_path, job_name, backup_set_id, backup_set_path, new_tar_info, mode="full"
):
    """
    Write manifest files (HTML only) for a backup set, using SQLite for manifest storage.
    This function is now mainly for HTML generation since job/file management is handled
    in the backup logic itself.
    
    :param job_config_path: Path to the job config YAML.
    :param job_name: Name of the backup job.
    :param backup_set_id: Backup set identifier (set_name).
    :param backup_set_path: Path to the backup set directory.
    :param new_tar_info: List of file info dicts for new tarballs (not used in new schema).
    :param mode: "full", "incremental", or "differential" (for compatibility).
    :return: Tuple (None, html_path) for compatibility.
    """
    # Load and merge configs for display
    try:
        with open(job_config_path, 'r', encoding='utf-8') as f:
            job_config_dict = yaml.safe_load(f)
        with open(GLOBAL_CONFIG_PATH, 'r', encoding='utf-8') as f:
            global_config = yaml.safe_load(f)
        merged_config = merge_configs(global_config, job_config_dict)
    except (OSError, yaml.YAMLError) as e:
        merged_config = {"error": f"Could not load config: {e}"}

    # Get backup set data from the new schema
    from app.models.manifest_db import get_backup_set_by_job_and_set, get_files_for_backup_set
    
    backup_set_row = get_backup_set_by_job_and_set(job_name, backup_set_id)
    if not backup_set_row:
        # This shouldn't happen in the new flow, but handle gracefully
        print(f"Warning: Backup set not found for {job_name}/{backup_set_id}")
        return None, None

    # Retrieve all files for this backup set from DB for HTML generation
    # This gets files from ALL jobs in the backup set (full + incrementals)
    raw_files = get_files_for_backup_set(backup_set_row['id'])

    # Format the files data for HTML template display
    all_files = []
    for f in raw_files:
        # Format size to human readable
        size_display = sizeof_fmt(f.get("size", 0)) if isinstance(f.get("size", 0), (int, float)) else "N/A"
        
        # Format timestamp to readable date
        modified_display = "N/A"
        if f.get("mtime"):
            try:
                dt = datetime.fromtimestamp(f["mtime"])
                modified_display = dt.strftime("%Y-%m-%d %H:%M:%S")
            except (ValueError, TypeError):
                modified_display = "N/A"
        
        # Create formatted file entry for template
        formatted_file = {
            "tarball": f.get("tarball", "unknown"),
            "tarball_path": f.get("tarball", "unknown"),  # For compatibility
            "path": f.get("path", ""),
            "size": size_display,  # Human readable size
            "size_bytes": f.get("size", 0),  # Keep numeric for calculations
            "modified": modified_display,  # Formatted timestamp
            "mtime": f.get("mtime", 0),  # Keep raw timestamp
            "backup_type": f.get("backup_type", "unknown"),  # Job type that added this file
            "job_started_at": f.get("job_started_at", 0)  # When the job ran
        }
        all_files.append(formatted_file)

    # Build tarball summary for HTML (using raw data for calculations)
    tarball_summary = build_tarball_summary_from_manifest(raw_files)

    last_file_modified = None
    if raw_files:
        try:
            last_file_modified = max(
                datetime.fromtimestamp(f["mtime"])
                for f in raw_files if "mtime" in f and f["mtime"] is not None
            ).strftime("%Y-%m-%d %H:%M:%S")
        except (ValueError, KeyError, TypeError):
            last_file_modified = None

    html_path = os.path.join(backup_set_path, f"manifest_{backup_set_id}.html")
    with open(html_path, "w", encoding='utf-8') as f:
        f.write(render_html_manifest(
            job_name=job_name,
            backup_set_id=backup_set_id,
            job_config_path=job_config_path,
            all_files=all_files,  # Now properly formatted with job info
            timestamp=last_file_modified,
            tarball_summary=tarball_summary,
            used_config=merged_config
        ))

    # Return None for json_path (no longer written), html_path for compatibility
    return None, html_path

def render_html_manifest(
    job_name,
    backup_set_id,
    job_config_path,
    all_files,
    timestamp,
    tarball_summary,
    used_config=None
):
    """
    Render the HTML manifest for a backup set using Jinja2 templates.
    :param job_name: Name of the backup job.
    :param backup_set_id: Backup set identifier.
    :param job_config_path: Path to the job config YAML.
    :param all_files: List of file info dicts.
    :param timestamp: Last file modification timestamp.
    :param tarball_summary: Tarball summary list.
    :param used_config: The merged config dict.
    :return: Rendered HTML string.
    """
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    template_path = os.path.join(base_dir, "templates", "manifest_archived.html")

    global_config = {}
    job_config = {}
    if job_config_path and os.path.exists(job_config_path):
        try:
            with open(job_config_path, 'r', encoding='utf-8') as f:
                job_config = yaml.safe_load(f) or {}
            with open(GLOBAL_CONFIG_PATH, 'r', encoding='utf-8') as f:
                global_config = yaml.safe_load(f) or {}
        except (OSError, yaml.YAMLError):
            job_config = {}
            global_config = {}

    global_encryption = global_config.get("encryption", {}) if global_config else {}
    job_encryption = job_config.get("encryption", {}) if job_config else {}

    if not job_config_path or not os.path.exists(job_config_path):
        config_yaml_no_comments = (
            f"# Error: Config file path invalid or not found: {job_config_path}"
        )
    else:
        try:
            config_yaml_no_comments = get_merged_cleaned_yaml_config(job_config_path)
        except (OSError, yaml.YAMLError) as e:
            config_yaml_no_comments = f"# Error reading config file: {e}"

    try:
        dt_object = datetime.fromisoformat(timestamp)
        formatted_timestamp = dt_object.strftime("%A, %B %d, %Y at %I:%M %p")
    except (TypeError, ValueError):
        formatted_timestamp = timestamp

    try:
        templates_dir = os.path.dirname(template_path)
        env = Environment(loader=FileSystemLoader(templates_dir))
        template = env.get_template(os.path.basename(template_path))
        return template.render(
            job_name=job_name,
            backup_set_id=backup_set_id,
            config_yaml=config_yaml_no_comments,
            tarballs=all_files,
            manifest_timestamp=formatted_timestamp,
            tarball_summary=tarball_summary,
            global_config=global_config,
            job_config=job_config,
            global_encryption=global_encryption,
            job_encryption=job_encryption,
            used_config=used_config
        )
    except TemplateError as e:
        return f"<html><body>Error rendering manifest: {e}</body></html>"

def _remove_yaml_comments(yaml_string):
    """
    Remove comments from a YAML string.
    :param yaml_string: The raw YAML string.
    :return: YAML string with comments removed.
    """
    lines = yaml_string.splitlines()
    cleaned_lines = []
    for line in lines:
        stripped_line = line.split('#', 1)[0].rstrip()
        if stripped_line:
            cleaned_lines.append(line.split('#', 1)[0])
        elif line.strip() == '':
            cleaned_lines.append('')
    result = "\n".join(cleaned_lines)
    if yaml_string.endswith('\n'):
        result += '\n'
    return result.rstrip() + '\n' if result.strip() else ''

def get_merged_cleaned_yaml_config(job_config_path):
    """
    Load, clean, and merge the job and global YAML configs for display in the manifest.
    Removes comments and merges in global defaults for destination and aws if missing.
    :param job_config_path: Path to the job config YAML.
    :return: Cleaned and merged YAML string.
    """
    if not job_config_path or not os.path.exists(job_config_path):
        return f"# Error: Config file path invalid or not found: {job_config_path}"
    try:
        with open(job_config_path, 'r', encoding='utf-8') as f:
            raw_yaml = f.read()
        cleaned_yaml_str = _remove_yaml_comments(raw_yaml)
        job_config = yaml.safe_load(cleaned_yaml_str)
        with open(GLOBAL_CONFIG_PATH, 'r', encoding='utf-8') as f:
            global_config = yaml.safe_load(f)
        if "destination" not in job_config or not job_config.get("destination"):
            job_config["destination"] = global_config.get("destination")
        if "aws" not in job_config or not job_config.get("aws"):
            job_config["aws"] = global_config.get("aws")
        merged = {}
        for key in ("destination", "aws"):
            if key in job_config:
                merged[key] = job_config[key]
        for key, value in job_config.items():
            if key not in merged:
                merged[key] = value
        merged_yaml = yaml.safe_dump(
            merged,
            sort_keys=False,
            default_flow_style=False,
            indent=2
        )
        return merged_yaml
    except (OSError, yaml.YAMLError) as e:
        return f"# Error reading config file {job_config_path}: {e}"

def get_tarball_summary(backup_set_path, *, show_full_name=True):
    """
    Build a summary of all tarball files in a backup set directory.
    Includes size, and timestamp for each tarball.
    :param backup_set_path: Path to the backup set directory.
    :param show_full_name: Whether to show the full tarball filename.
    :return: List of dicts summarizing each tarball.
    """

    # Find all tarballs (both encrypted and unencrypted) in the backup set directory
    tarball_files = glob.glob(os.path.join(backup_set_path, '*.tar.gz')) + \
                    glob.glob(os.path.join(backup_set_path, '*.tar.gz.gpg'))

    # Regex to extract timestamp from tarball filename
    timestamp_pattern = re.compile(r'_(\d{8}_\d{6})\.tar\.gz')
    summary = []

    for tar_path in tarball_files:
        base = os.path.basename(tar_path)
        # Determine the display name for the tarball
        tarball_name = base if show_full_name else base.rsplit('.', 2)[0]
        # Default timestamp string if not found in filename
        timestamp_str = '00000000_000000'
        # Try to extract timestamp from filename
        match = timestamp_pattern.search(base)
        if match:
            timestamp_str = match.group(1)
        try:
            # Get the file size in human-readable format and bytes
            size_bytes = os.path.getsize(tar_path)
            summary.append({
                "name": tarball_name,
                "size": sizeof_fmt(size_bytes),
                "size_bytes": size_bytes,
                "timestamp_str": timestamp_str,
            })
        except OSError:
            # If file size can't be determined, mark as error
            summary.append({
                "name": tarball_name,
                "size": "Error",
                "size_bytes": 0,
                "timestamp_str": timestamp_str,
            })
    # Sort tarballs by timestamp (newest first)
    return sorted(summary, key=lambda item: item['timestamp_str'], reverse=True)
