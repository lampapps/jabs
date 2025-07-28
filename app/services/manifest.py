"""
Service for handling backup manifest data and generation.

This module provides functions for:
1. Querying manifest data from the database for Flask routes
2. Generating archived (standalone HTML) manifests for backup repositories
3. Building tarball summaries with accurate file sizes
"""

from datetime import datetime
import os
import re
import glob
import tarfile
import copy
import yaml
from jinja2 import Environment, FileSystemLoader, TemplateError
from typing import Dict, List, Optional, Any
from collections import defaultdict
import logging
import json

from app.utils.logger import sizeof_fmt
from app.settings import GLOBAL_CONFIG_PATH

from app.models.backup_sets import get_backup_set_by_job_and_set
from app.models.backup_jobs import get_jobs_for_backup_set
from app.models.backup_files import get_files_for_backup_set

# Set up logging
logger = logging.getLogger(__name__)

def get_manifest_with_files(job_name: str, backup_set_id: str) -> Optional[Dict[str, Any]]:
    """
    Get manifest data with files for a backup set (used by Flask routes).
    
    Args:
        job_name: Name of the backup job
        backup_set_id: Set ID/name of the backup set
        
    Returns:
        Dictionary with manifest data or None if backup set not found
    """
    # The backup_set_id here is actually the set_name from the URL
    backup_set = get_backup_set_by_job_and_set(job_name, backup_set_id)
    if not backup_set:
        return None
    
    # Get the most recent completed job for this backup set
    jobs = get_jobs_for_backup_set(backup_set['id'])
    completed_jobs = [j for j in jobs if j['status'] == 'completed']
    if not completed_jobs:
        return None
    
    # Get the most recent completed job
    latest_job = max(completed_jobs, key=lambda j: j['started_at'])
    
    # Get all files for the backup set
    files = get_files_for_backup_set(backup_set['id'])
    
    # Format timestamps
    def format_timestamp(timestamp):
        if timestamp:
            try:
                dt = datetime.fromtimestamp(timestamp)
                return dt.isoformat()
            except (ValueError, TypeError):
                return None
        return None
    
    # Extract config_snapshot from backup_set using subscript notation
    config_snapshot = None
    try:
        if 'config_snapshot' in backup_set.keys():
            config_snapshot = backup_set['config_snapshot']
    except Exception as e:
        logger.error(f"Error accessing config_snapshot: {e}")
    
    return {
        'job_name': backup_set['job_name'],
        'set_name': backup_set['set_name'],
        'backup_type': latest_job['backup_type'],
        'status': latest_job['status'],
        'event': latest_job['event_message'] if latest_job['event_message'] else '',
        'timestamp': format_timestamp(latest_job['completed_at']),
        'started_at': format_timestamp(latest_job['started_at']),
        'completed_at': format_timestamp(latest_job['completed_at']),
        'files': files,
        'config_snapshot': config_snapshot  # Changed key name to match what routes/manifest.py expects
    }


def merge_configs(global_config: Dict, job_config: Dict) -> Dict:
    """
    Recursively merge two configuration dictionaries, with job_config taking precedence.
    
    Args:
        global_config: Global configuration dictionary
        job_config: Job-specific configuration dictionary
        
    Returns:
        Merged configuration dictionary
    """
    merged = copy.deepcopy(global_config)
    for key, value in job_config.items():
        if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
            merged[key] = merge_configs(merged[key], value)
        else:
            merged[key] = value
    return merged


def extract_tar_info(tar_path: str, encryption_enabled: bool = False) -> List[Dict]:
    """
    Extract file info from a tar archive for manifest purposes.
    
    Args:
        tar_path: Path to the tar archive
        encryption_enabled: Whether encryption is enabled
        
    Returns:
        List of file info dictionaries
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
                        "size": member.size,
                        "mtime": member.mtime
                    })
    except (tarfile.TarError, OSError) as e:
        logger.error(f"Error reading tar file {tar_path}: {e}")
    return files_info


def parse_size_to_bytes(size_str: str) -> int:
    """
    Convert a human-readable size string (e.g., '1.2 MB') to bytes as an integer.
    
    Args:
        size_str: Human-readable size string
        
    Returns:
        Size in bytes as an integer
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


def build_tarball_summary_from_manifest(files_list: List[Dict]) -> List[Dict]:
    """
    Build a summary of tarballs from a list of file info dictionaries from manifest.
    
    Args:
        files_list: List of file info dictionaries
        
    Returns:
        List of tarball summary dictionaries
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
        
        # Handle size - expecting numeric bytes from database
        size_val = f.get("size", 0)
        if isinstance(size_val, (int, float)):
            tarballs[tarball_name]["size_bytes"] += size_val
        elif isinstance(size_val, str):
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


def get_tarball_summary(backup_set_path: str, *, show_full_name: bool = True) -> List[Dict]:
    """
    Build a summary of all tarball files in a backup set directory.
    
    Args:
        backup_set_path: Path to the backup set directory
        show_full_name: Whether to show the full tarball name
        
    Returns:
        List of tarball summary dictionaries
    """
    # Check if directory exists
    if not os.path.exists(backup_set_path):
        logger.warning(f"Backup set path does not exist: {backup_set_path}")
        return []
    
    # Find all tarballs (both encrypted and unencrypted)
    tarball_pattern1 = os.path.join(backup_set_path, '*.tar.gz')
    tarball_pattern2 = os.path.join(backup_set_path, '*.tar.gz.gpg')
    
    tarball_files = glob.glob(tarball_pattern1) + glob.glob(tarball_pattern2)
    logger.debug(f"Found {len(tarball_files)} tarball files in {backup_set_path}")
    
    if not tarball_files:
        return []

    # Extract timestamp from tarball filename
    timestamp_pattern = re.compile(r'_(\d{8}_\d{6})\.tar\.gz')
    summary = []

    for tar_path in tarball_files:
        base = os.path.basename(tar_path)
        tarball_name = base if show_full_name else base.rsplit('.', 2)[0]
        timestamp_str = '00000000_000000'
        
        match = timestamp_pattern.search(base)
        if match:
            timestamp_str = match.group(1)
            
        try:
            size_bytes = os.path.getsize(tar_path)
            summary.append({
                "name": tarball_name,
                "size": sizeof_fmt(size_bytes),
                "size_bytes": size_bytes,
                "timestamp_str": timestamp_str,
            })
        except OSError as e:
            logger.error(f"Error getting size for {tar_path}: {e}")
            summary.append({
                "name": tarball_name,
                "size": "Error",
                "size_bytes": 0,
                "timestamp_str": timestamp_str,
            })
            
    # Sort tarballs by timestamp (newest first)
    return sorted(summary, key=lambda item: item['timestamp_str'], reverse=True)


def format_files_for_archived_manifest(raw_files: List[Dict]) -> List[Dict]:
    """
    Format raw file data from the database for use in archived manifest template.
    
    Args:
        raw_files: List of raw file dictionaries from the database
        
    Returns:
        List of formatted file dictionaries
    """
    formatted_files = []
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
        
        formatted_files.append({
            "tarball": f.get("tarball", "unknown"),
            "path": f.get("path", ""),
            "size": size_display,
            "modified": modified_display,
        })
    
    return formatted_files


def calculate_last_modified(raw_files: List[Dict]) -> Optional[str]:
    """
    Calculate the last modified timestamp from a list of files.
    
    Args:
        raw_files: List of file dictionaries
        
    Returns:
        Formatted timestamp string or None
    """
    if not raw_files:
        return None
        
    try:
        # Find the most recent modification time
        last_file_modified = max(
            datetime.fromtimestamp(f["mtime"])
            for f in raw_files if "mtime" in f and f["mtime"] is not None
        )
        return last_file_modified.strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, KeyError, TypeError):
        return None


def generate_archived_manifest(
    job_config_path: str, 
    job_name: str, 
    backup_set_id: str,
    backup_set_path: str, 
    backup_type: str,
    **kwargs  # Keep this to ignore any other parameters
) -> Optional[str]:
    """
    Generate an archived (standalone HTML) manifest for a backup set.
    
    Args:
        job_config_path: Path to the job configuration file
        job_name: Name of the backup job
        backup_set_id: ID of the backup set
        backup_set_path: Path to the backup set directory
        backup_type: Type of backup (full, incremental, differential, dryrun)
        
    Returns:
        Path to the generated HTML manifest or None if generation failed
    """
    # Handle 'mode' parameter for compatibility with existing calls
    if 'mode' in kwargs and not backup_type:
        backup_type = kwargs['mode']
        
    # Skip manifest generation for dryrun backups
    if backup_type == "dryrun":
        logger.info("Skipping archived manifest generation for dryrun backup")
        return None
        
    # Load and merge configs
    try:
        with open(job_config_path, 'r', encoding='utf-8') as f:
            job_config_dict = yaml.safe_load(f)
        with open(GLOBAL_CONFIG_PATH, 'r', encoding='utf-8') as f:
            global_config = yaml.safe_load(f)
        merged_config = merge_configs(global_config, job_config_dict)
    except (OSError, yaml.YAMLError) as e:
        logger.error(f"Could not load config: {e}")
        merged_config = {"error": f"Could not load config: {e}"}
    
    # Get backup set from database
    backup_set_row = get_backup_set_by_job_and_set(job_name, backup_set_id)
    if not backup_set_row:
        logger.warning(f"Backup set not found for {job_name}/{backup_set_id}")
        return None

    # Access config_snapshot using subscript notation and handle potential errors
    try:
        config_snapshot = backup_set_row['config_snapshot']
        if config_snapshot:
            try:
                merged_config = json.loads(config_snapshot)
                logger.info(f"Using config snapshot from database for {job_name}/{backup_set_id}")
            except (json.JSONDecodeError, TypeError) as e:
                logger.error(f"Could not parse config snapshot from database: {e}")
    except (KeyError, IndexError) as e:
        logger.warning(f"No config_snapshot column in backup_set: {e}")

    # Get files for this backup set
    raw_files = get_files_for_backup_set(backup_set_row['id'])
    if not raw_files:
        logger.warning(f"No files found for backup set {job_name}/{backup_set_id}")
        return None

    # Format files data and get tarball summary
    formatted_files = format_files_for_archived_manifest(raw_files)
    tarball_summary = get_tarball_summary(backup_set_path)
    
    # Fall back to estimated sizes if no tarballs found
    if not tarball_summary:
        logger.warning(f"No tarballs found in {backup_set_path}, using estimated sizes")
        tarball_summary = build_tarball_summary_from_manifest(raw_files)

    # Calculate last modification time
    last_modified = calculate_last_modified(raw_files)

    # Ensure output directory exists
    try:
        os.makedirs(backup_set_path, exist_ok=True)
    except Exception as e:
        logger.error(f"Could not ensure directory exists {backup_set_path}: {e}")
        return None

    # Generate the HTML manifest
    html_path = os.path.join(backup_set_path, f"manifest_{backup_set_id}.html")
    try:
        html_content = render_archived_manifest(
            job_name=job_name,
            backup_set_id=backup_set_id,
            job_config_path=job_config_path,
            files=formatted_files,
            last_modified=last_modified,
            tarball_summary=tarball_summary,
            used_config=merged_config
        )
        
        with open(html_path, "w", encoding='utf-8') as f:
            f.write(html_content)
            
        logger.info(f"Successfully wrote archived manifest to {html_path}")
        return html_path
    except Exception as e:
        logger.error(f"Error writing archived manifest to {html_path}: {e}")
        return None


def calculate_total_size(tarball_summary: List[Dict]) -> Dict:
    """
    Calculate the total size from a tarball summary list.
    
    Args:
        tarball_summary: List of tarball summary dictionaries
        
    Returns:
        Dictionary with total_size_bytes and total_size_human
    """
    total_size_bytes = sum(tarball.get("size_bytes", 0) for tarball in tarball_summary)
    total_size_human = sizeof_fmt(total_size_bytes) if total_size_bytes > 0 else "N/A"
    
    return {
        "total_size_bytes": total_size_bytes,
        "total_size_human": total_size_human
    }


def render_archived_manifest(
    job_name: str,
    backup_set_id: str,
    job_config_path: str,
    files: List[Dict],
    last_modified: Optional[str],
    tarball_summary: List[Dict],
    used_config: Optional[Dict] = None
) -> str:
    """
    Render the archived HTML manifest for a backup set using Jinja2 templates.
    
    Args:
        job_name: Name of the backup job
        backup_set_id: ID of the backup set
        job_config_path: Path to the job configuration file
        files: List of formatted file dictionaries
        last_modified: Last modified timestamp string
        tarball_summary: List of tarball summary dictionaries
        used_config: Merged configuration dictionary
        
    Returns:
        HTML content as a string
    """
    # Get the path to the manifest_archived.html template
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    template_path = os.path.join(base_dir, "templates", "manifest_archived.html")

    # Get job and global configs for display
    global_config = {}
    job_config = {}
    if job_config_path and os.path.exists(job_config_path):
        try:
            with open(job_config_path, 'r', encoding='utf-8') as f:
                job_config = yaml.safe_load(f) or {}
            with open(GLOBAL_CONFIG_PATH, 'r', encoding='utf-8') as f:
                global_config = yaml.safe_load(f) or {}
        except (OSError, yaml.YAMLError):
            pass

    # Extract encryption settings for display
    global_encryption = global_config.get("encryption", {}) if global_config else {}
    job_encryption = job_config.get("encryption", {}) if job_config else {}

    # Get cleaned YAML config for display
    try:
        config_yaml_no_comments = get_merged_cleaned_yaml_config(job_config_path) if job_config_path else ""
    except Exception as e:
        config_yaml_no_comments = f"# Error reading config file: {e}"

    # Calculate total size using shared function
    totals = calculate_total_size(tarball_summary)
    total_size_human = totals["total_size_human"]
    total_size_bytes = totals["total_size_bytes"]  # Also store bytes for consistency

    # Format the timestamp for display
    try:
        if isinstance(last_modified, str) and ":" in last_modified:
            # Already formatted
            formatted_timestamp = last_modified
        else:
            dt_object = datetime.fromisoformat(last_modified) if last_modified else datetime.now()
            formatted_timestamp = dt_object.strftime("%A, %B %d, %Y at %I:%M %p")
    except (TypeError, ValueError):
        formatted_timestamp = last_modified or "Unknown"

    # Render the template
    try:
        templates_dir = os.path.dirname(template_path)
        env = Environment(loader=FileSystemLoader(templates_dir))
        template = env.get_template(os.path.basename(template_path))
        return template.render(
            job_name=job_name,
            backup_set_id=backup_set_id,
            config_yaml=config_yaml_no_comments,
            tarballs=files,
            manifest_timestamp=formatted_timestamp,
            tarball_summary=tarball_summary,
            global_config=global_config,
            job_config=job_config,
            global_encryption=global_encryption,
            job_encryption=job_encryption,
            used_config=used_config,
            total_size_human=total_size_human,
            total_size_bytes=total_size_bytes
        )
    except TemplateError as e:
        logger.error(f"Template error rendering manifest: {e}")
        return f"""
        <!DOCTYPE html>
        <html lang="en" data-bs-theme="dark">
        <head>
            <meta charset="utf-8">
            <title>Manifest Error</title>
            <meta name="viewport" content="width=device-width, initial-scale=1">
            <link href="https://cdnjs.cloudflare.com/ajax/libs/twitter-bootstrap/5.3.0/css/bootstrap.min.css" 
                  rel="stylesheet">
        </head>
        <body>
            <div class="container my-5">
                <div class="alert alert-danger">
                    <h4>Error rendering manifest</h4>
                    <p>{e}</p>
                </div>
            </div>
        </body>
        </html>
        """


def _remove_yaml_comments(yaml_string: str) -> str:
    """
    Remove comments from a YAML string.
    
    Args:
        yaml_string: YAML content as a string
        
    Returns:
        YAML content with comments removed
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


def get_merged_cleaned_yaml_config(job_config_path: str) -> str:
    """
    Load, clean, and merge the job and global YAML configs for display.
    
    Args:
        job_config_path: Path to the job configuration file
        
    Returns:
        Merged YAML configuration as a string
    """
    if not os.path.exists(job_config_path):
        return f"# Error: Config file not found: {job_config_path}"
    
    try:
        with open(job_config_path, 'r', encoding='utf-8') as f:
            raw_yaml = f.read()
        cleaned_yaml_str = _remove_yaml_comments(raw_yaml)
        job_config = yaml.safe_load(cleaned_yaml_str)
        
        with open(GLOBAL_CONFIG_PATH, 'r', encoding='utf-8') as f:
            global_config = yaml.safe_load(f)
            
        # Add defaults from global config if missing
        if "destination" not in job_config or not job_config.get("destination"):
            job_config["destination"] = global_config.get("destination")
        if "aws" not in job_config or not job_config.get("aws"):
            job_config["aws"] = global_config.get("aws")
            
        # Build merged config keeping order
        merged = {}
        for key in ("destination", "aws"):
            if key in job_config:
                merged[key] = job_config[key]
        for key, value in job_config.items():
            if key not in merged:
                merged[key] = value
                
        # Convert to YAML string
        return yaml.safe_dump(
            merged,
            sort_keys=False,
            default_flow_style=False,
            indent=2
        )
    except Exception as e:
        return f"# Error reading config file {job_config_path}: {e}"
