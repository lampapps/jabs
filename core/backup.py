"""Core backup logic for JABS: handles backup creation, rotation, encryption, and events."""

import os
import time
import fnmatch
import tarfile
import glob
import shutil
import socket
from datetime import datetime

import yaml
import portalocker

from app.utils.logger import setup_logger, timestamp, ensure_dir
from app.utils.manifest import write_manifest_files, extract_tar_info, merge_configs
from app.utils.event_logger import (
    remove_event_by_backup_set_id,
    update_event,
    finalize_event,
    event_exists,
)
from app.settings import LOCK_DIR, RESTORE_SCRIPT_SRC, GLOBAL_CONFIG_PATH
from core.encrypt import encrypt_tarballs

def load_job_config(path):
    """Load a YAML job configuration file from the given path."""
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)
    
def load_exclude_patterns(job_config_path, global_config_path, common_exclude_path):
    """Load and merge exclude patterns for a backup job."""
    with open(job_config_path, encoding="utf-8") as f:
        job_config = yaml.safe_load(f)
    with open(global_config_path, encoding="utf-8") as f:
        global_config = yaml.safe_load(f)
    # Load common excludes from a separate file
    if os.path.exists(common_exclude_path):
        with open(common_exclude_path, encoding="utf-8") as f:
            data = yaml.safe_load(f)
            if isinstance(data, dict):
                common_exclude = data.get("exclude", [])
            elif isinstance(data, list):
                common_exclude = data
            else:
                common_exclude = []
    else:
        common_exclude = []

    # --- NEW LOGIC: allow per-job override, fallback to global, default True ---
    if "use_common_exclude" in job_config:
        use_common = job_config["use_common_exclude"]
    elif "use_common_exclude" in global_config:
        use_common = global_config["use_common_exclude"]
    else:
        use_common = True

    job_exclude = job_config.get("exclude", [])
    # Ensure job_exclude is always a list
    if job_exclude is None:
        job_exclude = []
    if use_common:
        exclude_patterns = common_exclude + job_exclude
    else:
        exclude_patterns = job_exclude
    return exclude_patterns

def is_excluded(path, exclude_patterns, src_base):
    rel_path = os.path.relpath(path, src_base)
    rel_path = rel_path.replace(os.sep, "/")
    is_dir = os.path.isdir(path)
    if rel_path in [".", ".."]:
        return False
    for pattern in exclude_patterns:
        normalized_pattern = pattern.replace(os.sep, "/").rstrip("/")
        # If pattern ends with '/', only match directories
        if pattern.endswith("/"):
            if is_dir and fnmatch.fnmatch(rel_path, normalized_pattern):
                return True
            if is_dir and fnmatch.fnmatch(rel_path + "/", normalized_pattern + "/"):
                return True
        else:
            if fnmatch.fnmatch(rel_path, normalized_pattern):
                return True
            if fnmatch.fnmatch(rel_path + "/", normalized_pattern + "/"):
                return True
        if "**" in normalized_pattern:
            if fnmatch.fnmatch(rel_path, normalized_pattern):
                return True
        if normalized_pattern == ".*":
            basename = os.path.basename(rel_path)
            if basename.startswith(".") and basename not in [".", ".."]:
                return True
    return False

def acquire_lock(lock_path):
    """
    Acquire a file lock to prevent concurrent backups for the same job.
    Returns the lock file handle if successful, or None if already locked.
    """
    lock_file = open(lock_path, 'w', encoding="utf-8")
    try:
        portalocker.lock(lock_file, portalocker.LOCK_EX | portalocker.LOCK_NB)
        return lock_file
    except portalocker.exceptions.LockException:
        lock_file.close()
        return None

def release_lock(lock_file):
    """Release a previously acquired file lock."""
    try:
        portalocker.unlock(lock_file)
        lock_file.close()
    except portalocker.exceptions.PortalockerError:
        pass

def create_tar_archives(src, dest_tar_dir, exclude_patterns, max_tarball_size_mb, logger, backup_type, config):
    """
    Create multiple tar archives from the source directory or a list of files, 
    each up to max_tarball_size_mb (in MB).
    Returns a list of tarball paths and their contents.
    """
    max_tarball_size = max_tarball_size_mb * 1024 * 1024
    tarball_index = 1
    current_tar_size = 0
    tarball_paths = []
    tarball_contents = []
    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    current_tar_path = os.path.join(
        dest_tar_dir, f"{backup_type}_part_{tarball_index}_{timestamp_str}.tar.gz"
    )
    tar = tarfile.open(current_tar_path, "w:gz")
    tarball_paths.append(current_tar_path)
    tarball_contents.append([])

    if isinstance(src, list):
        files = src
        source_base = config["source"]
    else:
        files = []
        source_base = src
        for dirpath, dirnames, filenames in os.walk(src):
            if is_excluded(dirpath, exclude_patterns, src):
                dirnames[:] = []
                continue
            for name in filenames:
                full_path = os.path.join(dirpath, name)
                if not is_excluded(full_path, exclude_patterns, src):
                    files.append(full_path)

    for full_path in files:
        arcname = os.path.relpath(full_path, source_base)
        try:
            # Skip broken symlinks
            if os.path.islink(full_path):
                target = os.readlink(full_path)
                if not os.path.exists(os.path.join(os.path.dirname(full_path), target)):
                    logger.warning(f"Skipping broken symlink: {full_path} -> {target}")
                    continue
            file_size = os.path.getsize(full_path)
        except (FileNotFoundError, OSError) as e:
            logger.warning(f"Skipping file (not found or inaccessible): {full_path} ({e})")
            continue
        if current_tar_size + file_size > max_tarball_size:
            tar.close()
            logger.info(f"Tarball created: {current_tar_path} (size: {current_tar_size} bytes)")
            tarball_index += 1
            current_tar_path = os.path.join(
                dest_tar_dir, f"{backup_type}_part_{tarball_index}_{timestamp_str}.tar.gz"
            )
            tar = tarfile.open(current_tar_path, "w:gz")
            tarball_paths.append(current_tar_path)
            tarball_contents.append([])
            current_tar_size = 0
        tar.add(full_path, arcname=arcname)
        tarball_contents[-1].append(arcname)
        current_tar_size += file_size

    tar.close()
    logger.info(f"Tarball created: {current_tar_path} (size: {current_tar_size} bytes)")
    return tarball_paths, tarball_contents

def get_modified_files(src, last_full_time, exclude_patterns=None):
    """
    Identify files modified after the last full backup timestamp.
    Returns a list of modified file paths.
    """
    modified = []
    exclude_patterns = exclude_patterns or []
    for dirpath, dirnames, filenames in os.walk(src):
        if is_excluded(dirpath, exclude_patterns, src):
            dirnames[:] = []
            continue
        dirnames[:] = [
            d for d in dirnames
            if not is_excluded(os.path.join(dirpath, d), exclude_patterns, src)
        ]
        for name in filenames:
            full_path = os.path.join(dirpath, name)
            if is_excluded(full_path, exclude_patterns, src):
                continue
            try:
                if os.path.getmtime(full_path) > last_full_time:
                    modified.append(full_path)
            except (OSError, IOError) as e:
                print(f"Error checking file {full_path}: {e}")
                continue
    return modified

def rotate_backups(job_dst, keep_sets, logger, config=None):
    """
    Rotate backup sets to keep only the latest 'keep_sets' sets and clean up corresponding JSON files, events, and S3 folders.
    """
    import subprocess
    backup_sets = sorted(glob.glob(os.path.join(job_dst, "backup_set_*")), reverse=True)
    if len(backup_sets) > keep_sets:
        to_delete = backup_sets[keep_sets:]
        for old_set in to_delete:
            try:
                shutil.rmtree(old_set)
                logger.info(f"Deleted old backup set: {old_set}")
                backup_set_id = os.path.basename(old_set).replace("backup_set_", "")
                job_name = os.path.basename(job_dst)
                manifest_dir = os.path.join("data", "manifests", job_name)
                manifest_file = os.path.join(manifest_dir, f"{backup_set_id}.json")
                if os.path.exists(manifest_file):
                    os.remove(manifest_file)
                    logger.info(f"Deleted JSON file: {manifest_file}")
                else:
                    logger.warning(f"JSON file not found for backup set: {backup_set_id}")
                remove_event_by_backup_set_id(backup_set_id, logger)
                if config:
                    aws_config = config.get("aws", {})
                    bucket = aws_config.get("bucket")
                    profile = aws_config.get("profile", "default")
                    region = aws_config.get("region")
                    machine_name = socket.gethostname()
                    sanitized_job_name = "".join(
                        c if c.isalnum() or c in ("-", "_") else "_" for c in job_name
                    )
                    prefix = machine_name
                    s3_path = f"s3://{bucket}/{prefix}/{sanitized_job_name}/{os.path.basename(old_set)}"
                    cmd = [
                        "aws", "s3", "rm", s3_path, "--recursive", "--profile", profile
                    ]
                    if region:
                        cmd.extend(["--region", region])
                    logger.info(f"Deleting S3 backup set: {s3_path}")
                    subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                    logger.info(f"Deleted S3 backup set: {s3_path}")
            except (OSError, shutil.Error, subprocess.SubprocessError) as e:
                logger.error(f"Error deleting backup set {old_set} or its manifest/S3 folder: {e}")

def find_latest_backup_set(job_dst):
    """
    Find the most recent backup_set folder based on timestamp.
    Returns the path to the latest backup set, or None if none exist.
    """
    pattern = os.path.join(job_dst, "backup_set_*")
    sets = sorted(glob.glob(pattern), reverse=True)
    return sets[0] if sets else None

def run_backup(config, backup_type, encrypt=False, sync=False, event_id=None, job_config_path=None):
    """
    Run the backup process for a given job configuration.
    Handles full and differential backups, encryption, manifest writing, and backup rotation.
    """
    job_name = config.get("job_name", "unknown_job")
    logger = setup_logger(job_name)
    logger.info(f"Starting backup job '{job_name}' with provided config.")
    logger.info(f"Backup type: {backup_type}, Encrypt: {encrypt}, Sync: {sync}")

    if event_id and not event_exists(event_id):
        logger.error(f"Event with ID {event_id} not found at backup start. Aborting backup.")
        return None, event_id, None

    src = config.get("source")
    if not src or not os.path.exists(src):
        error_msg = f"Source path does not exist: {src}"
        logger.error(error_msg)
        if event_id and event_exists(event_id):
            finalize_event(
                event_id=event_id,
                status="error",
                event=error_msg,
                backup_set_id=None,
                runtime="00:00:00"
            )
        return None, event_id, None

    encryption_cfg = config.get("encryption", {})
    if encryption_cfg.get("enabled", False) or encrypt:
        passphrase_env = encryption_cfg.get("passphrase_env", "JABS_ENCRYPT_PASSPHRASE")
        if not os.environ.get(passphrase_env):
            error_msg = (
                f"Passphrase environment variable "
                f"'{passphrase_env}' is not set. Backup aborted."
            )
            logger.error(error_msg)
            if event_id and event_exists(event_id):
                finalize_event(
                    event_id=event_id,
                    status="error",
                    event=error_msg,
                    backup_set_id=None,
                    runtime="00:00:00"
                )
            return None, event_id, None

    os.makedirs(LOCK_DIR, exist_ok=True)
    lock_path = os.path.join(LOCK_DIR, f"{job_name}.lock")
    lock_file = acquire_lock(lock_path)
    if not lock_file:
        logger.error(f"Backup already running for job '{job_name}'. Exiting.")
        if event_id and event_exists(event_id):
            finalize_event(
                event_id=event_id,
                status="skipped",
                event=f"Backup already running for job '{job_name}'.",
                backup_set_id=None
            )
        return None, event_id, None

    backup_set_id_string = None
    latest_backup_set_path = None

    try:
        with open(GLOBAL_CONFIG_PATH, encoding="utf-8") as f:
            global_config = yaml.safe_load(f)
        with open(job_config_path, encoding="utf-8") as f:
            job_config = yaml.safe_load(f)
        config = merge_configs(global_config, job_config)

        # --- PATCH: Use common_exclude.yaml if present ---
        common_exclude_path = os.path.join(os.path.dirname(GLOBAL_CONFIG_PATH), "common_exclude.yaml")
        exclude_patterns = load_exclude_patterns(job_config_path, GLOBAL_CONFIG_PATH, common_exclude_path)

        max_tarball_size_mb = config.get("max_tarball_size", 1024)
        src = config["source"]
        raw_dst = config["destination"]
        job_name = config.get("job_name") or "unknown_job"
        keep_sets = config.get("keep_sets", 5)

        if not os.path.isabs(raw_dst):
            error_msg = f"Destination path is not absolute: {raw_dst}"
            logger.error(error_msg)
            finalize_event(
                event_id=event_id,
                status="error",
                event=error_msg,
                backup_set_id=None,
                runtime="00:00:00"
            )
            return None, event_id, None
        parent_dir = os.path.dirname(raw_dst)
        if not os.path.exists(parent_dir) or not os.access(parent_dir, os.W_OK):
            error_msg = f"Destination parent directory does not exist or is not writable: {parent_dir}"
            logger.error(error_msg)
            finalize_event(
                event_id=event_id,
                status="error",
                event=error_msg,
                backup_set_id=None,
                runtime="00:00:00"
            )
            return None, event_id, None

        machine_name = socket.gethostname()
        sanitized_job_name = "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in job_name)
        sanitized_machine_name = "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in machine_name)
        job_dst = os.path.join(raw_dst, sanitized_machine_name, sanitized_job_name)

        ensure_dir(job_dst)
        logger.info(f"Starting {backup_type.upper()} backup: {src} -> {job_dst}")
        now = timestamp()

        last_full_file = os.path.join(job_dst, "last_full.txt")
        if backup_type in ["diff", "differential"]:
            if not os.path.exists(last_full_file):
                logger.info("No full backup found. Performing full backup instead of diff.")
                update_event(
                    event_id=event_id,
                    backup_type="full",
                    event="No full backup found. Performing full backup instead of diff.",
                )
                backup_type = "full"

        if backup_type == "full":
            backup_set_id_string = now
            backup_set_dir = os.path.join(job_dst, f"backup_set_{backup_set_id_string}")
            ensure_dir(backup_set_dir)
            tarball_paths, _ = create_tar_archives(
                src, backup_set_dir, exclude_patterns, max_tarball_size_mb, logger, backup_type="full", config=config
            )
            encryption_enabled = config.get("encryption", {}).get("enabled", False) or encrypt
            new_tar_info = []
            for tar_path in tarball_paths:
                new_tar_info.extend(extract_tar_info(tar_path, encryption_enabled=encryption_enabled))
            if tarball_paths:
                logger.info("Writing manifest files...")
                try:
                    json_manifest_path, html_manifest_path = write_manifest_files(
                        job_config_path=job_config_path,
                        job_name=job_name,
                        backup_set_id=backup_set_id_string,
                        backup_set_path=backup_set_dir,
                        new_tar_info=new_tar_info,
                        mode="full"
                    )
                    logger.info(f"JSON Manifest written to: {json_manifest_path}")
                    logger.info(f"HTML Manifest written to: {html_manifest_path}")
                except Exception as e:
                    logger.error(f"Failed to write manifest files: {e}", exc_info=True)
                    finalize_event(
                        event_id=event_id,
                        status="error",
                        event=f"Failed to write manifest files: {e}",
                        backup_set_id=backup_set_id_string,
                        runtime="00:00:00"
                    )
                    return None, event_id, None
            else:
                logger.warning("No tarballs created, skipping manifest generation.")
            if encrypt and tarball_paths:
                tarball_paths = encrypt_tarballs(tarball_paths, config, logger)
            with open(os.path.join(job_dst, "last_full.txt"), "w", encoding="utf-8") as f:
                f.write(backup_set_id_string)
            logger.info(f"Full backup SUCCESS: {len(tarball_paths)} tarballs created.")
            latest_backup_set_path = backup_set_dir

        elif backup_type in ["diff", "differential"]:
            with open(last_full_file, encoding="utf-8") as f:
                last_full_timestamp = f.read().strip()
            backup_set_id_string = last_full_timestamp
            backup_set_dir = os.path.join(job_dst, f"backup_set_{backup_set_id_string}")
            if not os.path.exists(backup_set_dir):
                raise RuntimeError(f"Expected backup set folder not found: {backup_set_dir}")
            last_full_time = float(time.mktime(time.strptime(last_full_timestamp, "%Y%m%d_%H%M%S")))
            modified_files = get_modified_files(src, last_full_time, exclude_patterns)
            if not modified_files:
                logger.info("No modified files since last full backup.")
                return None, event_id, None
            tarball_paths, _ = create_tar_archives(
                modified_files,
                backup_set_dir,
                exclude_patterns,
                max_tarball_size_mb,
                logger,
                backup_type="diff",
                config=config
            )
            new_tar_info = []
            for tar_path in tarball_paths:
                new_tar_info.extend(extract_tar_info(tar_path, encryption_enabled=encrypt))
            if tarball_paths:
                logger.info("Writing manifest files...")
                try:
                    json_manifest_path, html_manifest_path = write_manifest_files(
                        job_config_path=job_config_path,
                        job_name=job_name,
                        backup_set_id=backup_set_id_string,
                        backup_set_path=backup_set_dir,
                        new_tar_info=new_tar_info,
                        mode="diff"
                    )
                    logger.info(f"JSON Manifest written to: {json_manifest_path}")
                    logger.info(f"HTML Manifest written to: {html_manifest_path}")
                except Exception as e:
                    logger.error(f"Failed to write manifest files: {e}", exc_info=True)
                    finalize_event(
                        event_id=event_id,
                        status="error",
                        event=f"Failed to write manifest files: {e}",
                        backup_set_id=backup_set_id_string,
                        runtime="00:00:00"
                    )
                    return None, event_id, None
            else:
                logger.warning("No tarballs created, skipping manifest generation.")
            if encrypt and tarball_paths:
                tarball_paths = encrypt_tarballs(tarball_paths, config, logger)
            logger.info(f"Differential backup SUCCESS: {len(modified_files)} files in {len(tarball_paths)} tarballs.")
            latest_backup_set_path = backup_set_dir

        else:
            raise ValueError(f"Unsupported backup type: {backup_type}")

        rotate_backups(job_dst, keep_sets, logger, config)
        try:
            shutil.copy2(RESTORE_SCRIPT_SRC, backup_set_dir)
        except (OSError, shutil.Error) as e:
            logger.warning(f"Could not copy restore.py to backup set: {e}")

        return latest_backup_set_path, event_id, backup_set_id_string

    except Exception as e:
        logger.error(f"An error occurred during the backup process: {e}", exc_info=True)
        if event_id and event_exists(event_id):
            finalize_event(
                event_id=event_id,
                status="error",
                event=f"Backup failed: {e}",
                backup_set_id=None
            )
        raise
    finally:
        release_lock(lock_file)
