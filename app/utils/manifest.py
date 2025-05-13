# /utils/manifest.py
import os
import json
import tarfile
import yaml
import re
import glob
from jinja2 import Template, Environment, FileSystemLoader
from datetime import datetime
from app.utils.logger import ensure_dir, setup_logger, sizeof_fmt
from app.settings import BASE_DIR, CONFIG_DIR, LOG_DIR, MANIFEST_BASE, EVENTS_FILE, GLOBAL_CONFIG_PATH


def extract_tar_info(tar_path):
    """Extracts file information from a tarball."""
    files_info = []
    base = os.path.basename(tar_path)
    if base.endswith('.tar.gz.gpg'):
        tarball_name = base[:-11]
    elif base.endswith('.tar.gz'):
        tarball_name = base[:-7]
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
        print(f"Error reading tar file {tar_path}: {e}")
    except FileNotFoundError:
        print(f"Tar file not found: {tar_path}")
    return files_info

def _remove_yaml_comments(yaml_string):
    """Removes full-line and end-of-line comments from a YAML string."""
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

def build_tarball_summary(backup_set_path, *, show_full_name=True):
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

def write_manifest_files(file_list, job_config_path, job_name, backup_set_id, backup_set_path, new_tar_info):
    # Load job config and merge global defaults
    try:
        with open(job_config_path, 'r') as f:
            job_config_dict = yaml.safe_load(f)
        with open(GLOBAL_CONFIG_PATH, 'r') as f:
            global_config = yaml.safe_load(f)
        if "destination" not in job_config_dict or not job_config_dict.get("destination"):
            job_config_dict["destination"] = global_config.get("destination")
        if "aws" not in job_config_dict or not job_config_dict.get("aws"):
            job_config_dict["aws"] = global_config.get("aws")
        global_encryption = global_config.get("encryption", {})
        job_encryption = job_config_dict.get("encryption", {})
    except Exception as e:
        job_config_dict = {"error": f"Could not load config: {e}"}
        global_encryption = {}
        job_encryption = {}

    sanitized_job = "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in job_name)
    json_dir = os.path.join(MANIFEST_BASE, sanitized_job)
    ensure_dir(json_dir)
    json_path = os.path.join(json_dir, f"{backup_set_id}.json")

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

    manifest_data["files"].extend(new_tar_info)
    manifest_data["timestamp"] = datetime.now().isoformat()

    with open(json_path, "w") as f:
        json.dump(manifest_data, f, indent=2)

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

    html_path = os.path.join(backup_set_path, f"manifest_{backup_set_id}.html")
    with open(html_path, "w") as f:
        f.write(render_html_manifest(
            job_name=job_name,
            backup_set_id=backup_set_id,
            job_config_path=job_config_path,
            all_files=manifest_data["files"],
            timestamp=last_file_modified, 
            tarball_summary=tarball_summary,
            global_encryption=global_encryption,
            job_encryption=job_encryption
        ))

    return json_path, html_path

def render_html_manifest(
    job_name,
    backup_set_id,
    job_config_path,
    all_files,
    timestamp,  # <-- Use 'timestamp'
    tarball_summary,
    global_encryption=None,
    job_encryption=None
):
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    template_path = os.path.join(base_dir, "templates", "manifest_archived.html")

    # Load configs for settings display
    global_config = {}
    job_config = {}
    if job_config_path and os.path.exists(job_config_path):
        try:
            with open(job_config_path, 'r') as f:
                job_config = yaml.safe_load(f) or {}
            from app.settings import GLOBAL_CONFIG_PATH
            with open(GLOBAL_CONFIG_PATH, 'r') as f:
                global_config = yaml.safe_load(f) or {}
        except Exception:
            job_config = {}
            global_config = {}

    if not job_config_path or not os.path.exists(job_config_path):
        config_yaml_no_comments = f"# Error: Config file path invalid or not found: {job_config_path}"
    else:
        try:
            config_yaml_no_comments = get_merged_cleaned_yaml_config(job_config_path)
        except Exception as e:
            config_yaml_no_comments = f"# Error reading config file: {e}"

    try:
        dt_object = datetime.fromisoformat(timestamp)
        formatted_timestamp = dt_object.strftime("%A, %B %d, %Y at %I:%M %p")
    except Exception:
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
            global_encryption=global_encryption or {},
            job_encryption=job_encryption or {},
            global_config=global_config,
            job_config=job_config
        )
    except Exception as e:
        return f"<html><body>Error rendering manifest: {e}</body></html>"

def get_merged_cleaned_yaml_config(job_config_path):
    import yaml
    if not job_config_path or not os.path.exists(job_config_path):
        return f"# Error: Config file path invalid or not found: {job_config_path}"
    try:
        with open(job_config_path, 'r') as f:
            raw_yaml = f.read()
        cleaned_yaml_str = _remove_yaml_comments(raw_yaml)
        job_config = yaml.safe_load(cleaned_yaml_str)
        from app.settings import GLOBAL_CONFIG_PATH
        with open(GLOBAL_CONFIG_PATH, 'r') as f:
            global_config = yaml.safe_load(f)
        # Merge global values if missing
        if "destination" not in job_config or not job_config.get("destination"):
            job_config["destination"] = global_config.get("destination")
        if "aws" not in job_config or not job_config.get("aws"):
            job_config["aws"] = global_config.get("aws")
        # Build dict with globals first
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
    except Exception as e:
        return f"# Error reading config file {job_config_path}: {e}"

def get_tarball_summary(backup_set_path):
    tarball_files = []
    if not os.path.isdir(backup_set_path):
        return []
    tarball_files = [
        os.path.join(backup_set_path, f)
        for f in os.listdir(backup_set_path)
        if f.endswith('.tar.gz') or f.endswith('.tar.gz.gpg')
    ]
    summary_data_for_sorting = []
    timestamp_pattern = re.compile(r'_(\d{8}_\d{6})\.tar\.gz')
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
        except Exception:
            summary_data_for_sorting.append({
                "name": basename,
                "size": "Error",
                "timestamp_str": timestamp_str,
                "encrypted": is_encrypted
            })
    return sorted(summary_data_for_sorting, key=lambda item: item['timestamp_str'], reverse=True)


