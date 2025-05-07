# /core/backup.py
import os
import time
import fnmatch
import tarfile
import yaml
import glob
import shutil
import json
import fcntl
import socket
from datetime import datetime, timedelta
from app.utils.logger import setup_logger, timestamp, ensure_dir
from app.utils.manifest import write_manifest_files, MANIFEST_BASE, extract_tar_info
from app.utils.event_logger import remove_event_by_backup_set_id, update_event, finalize_event
from app.utils.encrypt import encrypt_file_gpg
from app.settings import LOCK_DIR

def load_job_config(path):
    with open(path) as f:
        return yaml.safe_load(f)

def is_excluded(path, exclude_patterns, src_base):
    """
    Check if a given path should be excluded based on the exclusion patterns.
    :param path: The full path to the file or directory.
    :param exclude_patterns: A list of exclusion patterns.
    :param src_base: The base source directory for the backup.
    :return: True if the path should be excluded, False otherwise.
    """
    # Convert the path to a relative path for matching
    rel_path = os.path.relpath(path, src_base).replace(os.sep, "/")

    # Skip the current directory (.) and parent directory (..)
    if rel_path in [".", ".."]:
        return False

    for pattern in exclude_patterns:
        # Normalize the pattern to remove trailing slashes
        normalized_pattern = pattern.rstrip("/")

        # Match exact patterns
        if fnmatch.fnmatch(rel_path, normalized_pattern):
            return True

        # Match directories and their contents
        if normalized_pattern.endswith("/"):
            if rel_path.startswith(normalized_pattern):
                return True

        # Match recursive patterns (e.g., **/__pycache__/**)
        if "**" in normalized_pattern:
            if fnmatch.fnmatch(rel_path, normalized_pattern):
                return True

        # Special case: Exclude hidden files and directories
        if normalized_pattern == ".*":
            basename = os.path.basename(rel_path)
            if basename.startswith(".") and basename not in [".", ".."]:
                return True

    return False

def acquire_lock(lock_path):
    lock_file = open(lock_path, 'w')
    try:
        fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return lock_file
    except BlockingIOError:
        lock_file.close()
        return None

def release_lock(lock_file):
    try:
        fcntl.flock(lock_file, fcntl.LOCK_UN)
        lock_file.close()
    except Exception:
        pass

def create_tar_archives(src, dest_tar_dir, exclude_patterns, max_tarball_size_mb, logger, backup_type, config):
    """
    Create multiple tar archives from the source directory or a list of files, 
    each up to max_tarball_size_mb (in MB).
    """
    max_tarball_size = max_tarball_size_mb * 1024 * 1024  # Convert MB to bytes
    tarball_index = 1
    current_tar_size = 0
    tarball_paths = []
    tarball_contents = []  # Initialize the list to track contents of each tarball

    # Generate a timestamp for the tarball filenames
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Create the first tarball
    current_tar_path = os.path.join(
        dest_tar_dir, 
        f"{backup_type}_part_{tarball_index}_{timestamp}.tar.gz"  # Add timestamp to the filename
    )
    tar = tarfile.open(current_tar_path, "w:gz")
    tarball_paths.append(current_tar_path)
    tarball_contents.append([])  # Add an empty list for the first tarball

    # Handle a list of files (for differential backups)
    if isinstance(src, list):
        files = src
        source_base = config["source"]  # Use the source directory from the configuration
    else:
        # Handle a directory (for full backups)
        files = []
        source_base = src
        for dirpath, dirnames, filenames in os.walk(src):
            if is_excluded(dirpath, exclude_patterns, src):
                dirnames[:] = []  # Skip excluded dirs
                continue

            for name in filenames:
                full_path = os.path.join(dirpath, name)
                if not is_excluded(full_path, exclude_patterns, src):
                    files.append(full_path)

    # Add files to tarballs
    for full_path in files:
        # Calculate the relative path (arcname) based on the source directory
        arcname = os.path.relpath(full_path, source_base)
        file_size = os.path.getsize(full_path)

        # Check if adding this file would exceed the max tarball size
        if current_tar_size + file_size > max_tarball_size:
            # Close the current tarball and start a new one
            tar.close()
            logger.info(f"Tarball created: {current_tar_path} (size: {current_tar_size} bytes)")

            tarball_index += 1
            current_tar_path = os.path.join(
                dest_tar_dir, 
                f"{backup_type}_part_{tarball_index}_{timestamp}.tar.gz"  # Add timestamp to the filename
            )
            tar = tarfile.open(current_tar_path, "w:gz")
            tarball_paths.append(current_tar_path)
            tarball_contents.append([])  # Add a new list for the next tarball
            current_tar_size = 0

        # Add the file to the current tarball
        tar.add(full_path, arcname=arcname)
        tarball_contents[-1].append(arcname)
        current_tar_size += file_size

    # Close the last tarball
    tar.close()
    logger.info(f"Tarball created: {current_tar_path} (size: {current_tar_size} bytes)")

    return tarball_paths, tarball_contents

def get_modified_files(src, last_full_time, exclude_patterns=None):
    """
    Identify files modified after the last full backup timestamp.
    """
    modified = []
    exclude_patterns = exclude_patterns or []

    for dirpath, dirnames, filenames in os.walk(src):
        if is_excluded(dirpath, exclude_patterns, src):
            dirnames[:] = []  # Skip excluded directories
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
                # Check if the file was modified after the last full backup
                if os.path.getmtime(full_path) > last_full_time:
                    modified.append(full_path)
            except Exception as e:
                print(f"Error checking file {full_path}: {e}")
                continue

    return modified

def rotate_backups(job_dst, keep_sets, logger):
    """
    Rotate backup sets to keep only the latest 'keep_sets' sets and clean up corresponding JSON files and events.
    :param job_dst: The directory where backups are stored.
    :param keep_sets: The number of backup sets to retain.
    :param logger: Logger instance for logging messages.
    """
    # Find all backup set directories sorted by timestamp
    backup_sets = sorted(glob.glob(os.path.join(job_dst, "backup_set_*")), reverse=True)

    if len(backup_sets) > keep_sets:
        # Delete the oldest backup sets beyond the limit
        to_delete = backup_sets[keep_sets:]
        for old_set in to_delete:
            try:
                # Remove the old backup set folder
                shutil.rmtree(old_set)
                logger.info(f"Deleted old backup set: {old_set}")

                # Remove the corresponding JSON manifest file
                backup_set_id = os.path.basename(old_set).replace("backup_set_", "")
                job_name = os.path.basename(job_dst)
                manifest_dir = os.path.join("data", "manifests", job_name)
                manifest_file = os.path.join(manifest_dir, f"{backup_set_id}.json")
                if os.path.exists(manifest_file):
                    os.remove(manifest_file)
                    logger.info(f"Deleted JSON file: {manifest_file}")
                else:
                    logger.warning(f"JSON file not found for backup set: {backup_set_id}")

                # Remove the corresponding event from events.json
                remove_event_by_backup_set_id(backup_set_id, logger)

            except Exception as e:
                logger.error(f"Error deleting backup set {old_set} or its manifest: {e}")

def find_latest_backup_set(job_dst):
    """Find the most recent backup_set folder based on timestamp."""
    pattern = os.path.join(job_dst, "backup_set_*")
    sets = sorted(glob.glob(pattern), reverse=True)
    return sets[0] if sets else None

def encrypt_tarballs(tarball_paths, config, logger):
    """
    Encrypts each tarball in tarball_paths using GPG and removes the original.
    Returns a list of encrypted tarball paths.
    """
    from app.utils.encrypt import encrypt_file_gpg

    passphrase_env = (
        config.get("encryption", {}).get("passphrase_env")
        or "JABS_ENCRYPT_PASSPHRASE"
    )
    encrypted_paths = []
    for tarball_path in tarball_paths:
        encrypted_path = tarball_path + ".gpg"
        try:
            encrypt_file_gpg(tarball_path, encrypted_path, passphrase_env)
            os.remove(tarball_path)
            logger.info(f"Encrypted and removed: {tarball_path}")
            encrypted_paths.append(encrypted_path)
        except Exception as e:
            logger.error(f"Failed to encrypt {tarball_path}: {e}", exc_info=True)
            # Optionally: raise or continue
    return encrypted_paths


def run_backup(config, backup_type, encrypt=False, sync=False, event_id=None, job_config_path=None):
    # config is already a dict! Do NOT open or load YAML here.
    """
    Run the backup process.
    :param config: Configuration dictionary.
    :param backup_type: Type of backup ("full" or "diff").
    :param encrypt: Whether to encrypt the backup (not yet implemented).
    :param sync: Whether to sync the backup to the cloud.
    :param event_id: Unique ID of the event to update.
    :param job_config_path: Path to the job configuration file.
    :return: Tuple containing (path to the latest backup set, event_id, backup_set_id string or None).
    """
    # Set up the logger
    job_name = config.get("job_name", "unknown_job")
    logger = setup_logger(job_name)
    logger.info(f"Starting backup job '{job_name}' with provided config.")
    logger.info(f"Backup type: {backup_type}, Encrypt: {encrypt}, Sync: {sync}")

    # --- ENCRYPTION PASSPHRASE CHECK ---
    encryption_cfg = config.get("encryption", {})
    if encryption_cfg.get("enabled", False) or encrypt:
        passphrase_env = encryption_cfg.get("passphrase_env", "JABS_ENCRYPT_PASSPHRASE")
        if not os.environ.get(passphrase_env):
            error_msg = (
                f"Passphrase environment variable "
                f"'{passphrase_env}' is not set. Backup aborted."
            )
            logger.error(error_msg)
            finalize_event(
                event_id=event_id,
                status="error",
                event=error_msg,
                backup_set_id=None,
                runtime="00:00:00"
            )
            return None, event_id, None

    # --- ACQUIRE LOCK ---
    os.makedirs(LOCK_DIR, exist_ok=True)
    lock_path = os.path.join(LOCK_DIR, f"{job_name}.lock")
    lock_file = acquire_lock(lock_path)
    if not lock_file:
        logger.error(f"Backup already running for job '{job_name}'. Exiting.")
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
        exclude_patterns = config.get("exclude", [])
        max_tarball_size_mb = config.get("max_tarball_size", 1024)

        src = config["source"]
        raw_dst = config["destination"]
        job_name = config.get("job_name") or "unknown_job"
        keep_sets = config.get("keep_sets", 5)

        # --- NEW STRUCTURE: (destination_path)/(machine_name)/(job_name)/ ---
        machine_name = socket.gethostname()
        sanitized_job_name = "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in job_name)
        sanitized_machine_name = "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in machine_name)
        job_dst = os.path.join(raw_dst, sanitized_machine_name, sanitized_job_name)

        ensure_dir(job_dst)
        logger.info(f"Starting {backup_type.upper()} backup: {src} -> {job_dst}")
        now = timestamp()

        if backup_type == "full":
            backup_set_id_string = now
            backup_set_dir = os.path.join(job_dst, f"backup_set_{backup_set_id_string}")
            ensure_dir(backup_set_dir)

            tarball_paths, tarball_contents = create_tar_archives(
                src, backup_set_dir, exclude_patterns, max_tarball_size_mb, logger, backup_type="full", config=config
            )

            new_tar_info = []
            for tar_path in tarball_paths:
                new_tar_info.extend(extract_tar_info(tar_path))

            if encrypt and tarball_paths:
                tarball_paths = encrypt_tarballs(tarball_paths, config, logger)

            if tarball_paths:
                logger.info("Writing manifest files...")
                try:
                    json_manifest_path, html_manifest_path = write_manifest_files(
                        file_list=tarball_paths,
                        job_config_path=job_config_path,
                        job_name=job_name,
                        backup_set_id=backup_set_id_string,
                        backup_set_path=backup_set_dir,
                        new_tar_info=new_tar_info
                    )
                    logger.info(f"JSON Manifest written to: {json_manifest_path}")
                    logger.info(f"HTML Manifest written to: {html_manifest_path}")
                except Exception as e:
                    logger.error(f"Failed to write manifest files: {e}", exc_info=True)
            else:
                logger.warning("No tarballs created, skipping manifest generation.")

            with open(os.path.join(job_dst, "last_full.txt"), "w") as f:
                f.write(backup_set_id_string)

            logger.info(f"Full backup SUCCESS: {len(tarball_paths)} tarballs created.")

            latest_backup_set_path = backup_set_dir

        elif backup_type in ["diff", "differential"]:
            last_full_file = os.path.join(job_dst, "last_full.txt")
            if not os.path.exists(last_full_file):
                logger.info("No full backup found. Running full backup now")
                update_event(
                    event_id=event_id,
                    backup_type="full",
                    event="No full backup found. Running full backup now",
                )
                return run_backup(
                    config,
                    backup_type="full",
                    encrypt=encrypt,
                    sync=sync,
                    event_id=event_id
                )

            with open(last_full_file) as f:
                last_full_timestamp = f.read().strip()

            backup_set_id_string = last_full_timestamp

            backup_set_dir = os.path.join(job_dst, f"backup_set_{backup_set_id_string}")
            if not os.path.exists(backup_set_dir):
                raise Exception(f"Expected backup set folder not found: {backup_set_dir}")

            last_full_time = float(time.mktime(time.strptime(last_full_timestamp, "%Y%m%d_%H%M%S")))
            modified_files = get_modified_files(src, last_full_time, exclude_patterns)
            if not modified_files:
                logger.info("No modified files since last full backup.")
                return None, event_id, None

            tarball_paths, tarball_contents = create_tar_archives(
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
                new_tar_info.extend(extract_tar_info(tar_path))

            if encrypt and tarball_paths:
                tarball_paths = encrypt_tarballs(tarball_paths, config, logger)

            if tarball_paths:
                logger.info("Writing manifest files...")
                try:
                    json_manifest_path, html_manifest_path = write_manifest_files(
                        file_list=tarball_paths,
                        job_config_path=job_config_path,
                        job_name=job_name,
                        backup_set_id=backup_set_id_string,
                        backup_set_path=backup_set_dir,
                        new_tar_info=new_tar_info
                    )
                    logger.info(f"JSON Manifest written to: {json_manifest_path}")
                    logger.info(f"HTML Manifest written to: {html_manifest_path}")
                except Exception as e:
                    logger.error(f"Failed to write manifest files: {e}", exc_info=True)
            else:
                logger.warning("No tarballs created, skipping manifest generation.")

            logger.info(f"Differential backup SUCCESS: {len(modified_files)} files in {len(tarball_paths)} tarballs.")

            latest_backup_set_path = backup_set_dir

        else:
            raise ValueError(f"Unsupported backup type: {backup_type}")

        rotate_backups(job_dst, keep_sets, logger)

        return latest_backup_set_path, event_id, backup_set_id_string

    except Exception as e:
        logger.error(f"An error occurred during the backup process: {e}", exc_info=True)
        finalize_event(
            event_id=event_id,
            status="error",
            event=f"Backup failed: {e}",
            backup_set_id=None
        )
        raise
    finally:
        release_lock(lock_file)

