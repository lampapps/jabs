# /utils/manifest.py
import os
import json
import tarfile
import yaml
import re
import glob
from jinja2 import Template
from datetime import datetime
from app.utils.logger import ensure_dir, setup_logger
from app.settings import BASE_DIR, CONFIG_DIR, LOG_DIR, MANIFEST_BASE, EVENTS_FILE


def extract_tar_info(tar_path):
    """Extracts file information from a tarball."""
    files_info = []
    # Remove .tar.gz or .tar.gz.gpg extension for display in the DataTable
    base = os.path.basename(tar_path)
    if base.endswith('.tar.gz.gpg'):
        tarball_name = base[:-11]  # Remove 11 chars: '.tar.gz.gpg'
    elif base.endswith('.tar.gz'):
        tarball_name = base[:-7]   # Remove 7 chars: '.tar.gz'
    else:
        tarball_name = base
    try:
        with tarfile.open(tar_path, "r:*") as tar:
            for member in tar.getmembers():
                if member.isfile():
                    files_info.append({
                        "tarball": tarball_name,
                        "path": member.name,
                        "size": sizeof_fmt(member.size),
                        "modified": datetime.fromtimestamp(member.mtime).strftime('%Y-%m-%d %H:%M:%S')
                    })
    except tarfile.ReadError as e:
        print(f"Error reading tar file {tar_path}: {e}")  # Or use logger
    except FileNotFoundError:
        print(f"Tar file not found: {tar_path}")  # Or use logger
    return files_info

def sizeof_fmt(num, suffix="B"):
    """Formats file sizes."""
    for unit in ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"]:
        if abs(num) < 1024.0:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f}Yi{suffix}"

def _remove_yaml_comments(yaml_string):
    """Removes full-line and end-of-line comments from a YAML string."""
    lines = yaml_string.splitlines()
    cleaned_lines = []
    for line in lines:
        stripped_line = line.split('#', 1)[0].rstrip()
        if stripped_line:  # Keep lines that are not empty after removing comments
            cleaned_lines.append(line.split('#', 1)[0])  # Keep original indentation
        elif line.strip() == '':  # Keep empty lines for structure
            cleaned_lines.append('')
    # Join lines, ensuring final newline if original had one
    result = "\n".join(cleaned_lines)
    if yaml_string.endswith('\n'):
        result += '\n'
    # Remove trailing whitespace from the final result
    return result.rstrip() + '\n' if result.strip() else ''

def build_tarball_summary(backup_set_path, *, show_full_name=True):
    """
    Returns a sorted list of tarball dicts for manifest summary.
    :param backup_set_path: Directory to search for tarballs.
    :param show_full_name: If True, use full filename (with extension). If False, strip extension.
    """
    tarball_files = glob.glob(os.path.join(backup_set_path, '*.tar.gz')) + \
                    glob.glob(os.path.join(backup_set_path, '*.tar.gz.gpg'))
    timestamp_pattern = re.compile(r'_(\d{8}_\d{6})\.tar\.gz')
    summary = []
    for tar_path in tarball_files:
        base = os.path.basename(tar_path)
        if show_full_name:
            tarball_name = base
        else:
            if base.endswith('.tar.gz.gpg'):
                tarball_name = base[:-11]
            elif base.endswith('.tar.gz'):
                tarball_name = base[:-7]
            else:
                tarball_name = base
        is_encrypted = base.endswith('.gpg')
        timestamp_str = '00000000_000000'
        match = timestamp_pattern.search(base)
        if match:
            timestamp_str = match.group(1)
        try:
            size_bytes = os.path.getsize(tar_path)
            summary.append({
                "name": tarball_name,
                "size": sizeof_fmt(size_bytes),
                "timestamp_str": timestamp_str,
                "encrypted": is_encrypted
            })
        except Exception:
            summary.append({
                "name": tarball_name,
                "size": "Error",
                "timestamp_str": timestamp_str,
                "encrypted": is_encrypted
            })
    return sorted(summary, key=lambda item: item['timestamp_str'], reverse=True)

# This function writes the manifest files (JSON and HTML) for the backup set into Flask
# The JSON file is used for the API, while the HTML file is used for the web interface
def write_manifest_files(file_list, job_config_path, job_name, backup_set_id, backup_set_path, new_tar_info):
    try:
        with open(job_config_path, 'r') as f:
            job_config_dict = yaml.safe_load(f)
    except Exception as e:
        job_config_dict = {"error": f"Could not load config: {e}"}

    # Prepare local JSON path
    sanitized_job = "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in job_name)
    json_dir = os.path.join(MANIFEST_BASE, sanitized_job)
    ensure_dir(json_dir)
    json_path = os.path.join(json_dir, f"{backup_set_id}.json")

    # Load existing manifest data or initialize
    if os.path.exists(json_path):
        try:
            with open(json_path, "r") as f:
                manifest_data = json.load(f)
        except json.JSONDecodeError:
            manifest_data = {
                "job_name": job_name,
                "backup_set_id": backup_set_id,
                "timestamp": datetime.now().isoformat(),
                "config": job_config_dict,
                "files": []
            }
    else:
        manifest_data = {
            "job_name": job_name,
            "backup_set_id": backup_set_id,
            "timestamp": datetime.now().isoformat(),
            "config": job_config_dict,
            "files": []
        }

    manifest_data.setdefault("job_name", job_name)
    manifest_data.setdefault("backup_set_id", backup_set_id)
    manifest_data.setdefault("config", job_config_dict)
    manifest_data.setdefault("files", [])

    # Use the pre-extracted tar info for the manifest's file list
    manifest_data["files"].extend(new_tar_info)
    # Always update timestamp
    manifest_data["timestamp"] = datetime.now().isoformat()

    # Write updated JSON manifest
    with open(json_path, "w") as f:
        json.dump(manifest_data, f, indent=2)

    # --- Prepare data for HTML manifest ---
    tarball_summary = build_tarball_summary(backup_set_path, show_full_name=True)

    last_file_modified = None
    if manifest_data["files"]:
        try:
            last_file_modified = max(
                datetime.strptime(f["modified"], "%Y-%m-%d %H:%M:%S")
                for f in manifest_data["files"] if "modified" in f
            ).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            last_file_modified = None

    # Write updated HTML manifest
    html_path = os.path.join(backup_set_path, f"manifest_{backup_set_id}.html")
    with open(html_path, "w") as f:
        f.write(render_html_manifest(
            job_name=job_name,
            backup_set_id=backup_set_id,
            job_config_path=job_config_path,
            all_files=manifest_data["files"],
            manifest_timestamp=last_file_modified,
            tarball_summary=tarball_summary
        ))

    return json_path, html_path

# This function renders the Manifest_archived.html manifest using Jinja2
def render_html_manifest(job_name, backup_set_id, job_config_path, all_files, manifest_timestamp, tarball_summary):
    # Determine the base directory of the project
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    template_path = os.path.join(base_dir, "templates", "manifest_archived.html")

    # Read and clean YAML config
    try:
        with open(job_config_path, 'r') as f:
            raw_yaml_content = f.read()
        config_yaml_no_comments = _remove_yaml_comments(raw_yaml_content)
    except Exception as e:
        config_yaml_no_comments = f"# Error reading config file: {e}"

    # Format timestamp
    try:
        dt_object = datetime.fromisoformat(manifest_timestamp)
        formatted_timestamp = dt_object.strftime("%Y-%m-%d %H:%M:%S")
    except ValueError:
        formatted_timestamp = manifest_timestamp  # Fallback
    
    # Render the template
    try:
        with open(template_path) as template_file:
            template = Template(template_file.read())

            print("DEBUG tarball_summary:", tarball_summary)

        return template.render(
            job_name=job_name,
            backup_set_id=backup_set_id,
            config_yaml=config_yaml_no_comments,
            tarballs=all_files,  # For the main table
            timestamp=formatted_timestamp,
            tarball_summary=tarball_summary  # For the accordion summary
        )
    except Exception as e:
        return f"<html><body>Error rendering manifest: {e}</body></html>"

def get_cleaned_yaml_config(job_config_path):
    print(f"DEBUG (get_cleaned_yaml_config): Received path: {job_config_path}") # Add debug
    if not job_config_path or not os.path.exists(job_config_path):
        print(f"DEBUG (get_cleaned_yaml_config): Path invalid or does not exist.") # Add debug
        return f"# Error: Config file path invalid or not found: {job_config_path}"
    try:
        with open(job_config_path, 'r') as f:
            raw_yaml_content = f.read()
        print(f"DEBUG (get_cleaned_yaml_config): Read raw content successfully.") # Add debug
        cleaned_content = _remove_yaml_comments(raw_yaml_content)
        print(f"DEBUG (get_cleaned_yaml_config): Cleaned content length: {len(cleaned_content)}") # Add debug
        return cleaned_content
    except Exception as e:
        print(f"DEBUG (get_cleaned_yaml_config): Exception reading/cleaning: {e}") # Add debug
        return f"# Error reading config file {job_config_path}: {e}"

def get_tarball_summary(backup_set_path):
    """
    Calculates the total size and count of tarballs in a backup set directory.
    :param backup_set_path: The full path to the backup_set_* directory.
    :return: A dictionary {'count': int, 'total_size': int, 'total_size_fmt': str}
    """
    count = 0
    total_size = 0
    tarball_pattern = os.path.join(backup_set_path, '*.tar.gz')
    tarball_files = glob.glob(tarball_pattern)

    count = len(tarball_files)
    for tarball in tarball_files:
        try:
            total_size += os.path.getsize(tarball)
        except OSError as e:
            print(f"Warning: Could not get size of {tarball}: {e}") # Log a warning

    return {
        "count": count,
        "total_size": total_size,
        "total_size_fmt": sizeof_fmt(total_size)
    }


