import os
import shutil
import socket
import json
from app.utils.logger import setup_logger, timestamp, ensure_dir
from app.services.manifest import generate_archived_manifest, extract_tar_info
from app.models.events import update_event, finalize_event, event_exists
from app.settings import RESTORE_SCRIPT_SRC
from .utils import get_all_files, create_tar_archives, get_merged_exclude_patterns

from app.models.db_core import get_db_connection
from app.models.backup_sets import get_or_create_backup_set
from app.models.backup_jobs import insert_backup_job
from app.models.backup_files import insert_files

def run_full_backup(config, encrypt=False, sync=False, event_id=None, job_config_path=None, global_config=None):
    job_name = config.get("job_name", "unknown_job")
    logger = setup_logger(job_name)
    logger.debug(f"Starting FULL backup job '{job_name}' with provided config.")

    # Update the event if we have one
    if event_id and event_exists(event_id):
        update_event(event_id, event_message=f"Initializing full backup for {job_name}")

    # Get merged exclude patterns using the utility function
    exclude_patterns = get_merged_exclude_patterns(config, global_config, job_config_path, logger)

    src = config.get("source")
    dest = config.get("destination")
    if not src or not os.path.exists(src):
        error_msg = f"Source path does not exist: {src}"
        logger.error(error_msg)
        if event_id and event_exists(event_id):
            finalize_event(
                event_id=event_id,
                status="error",
                event_message=error_msg,
                backup_set_id=None,
                runtime="00:00:00"
            )
        return None, event_id, None, None

    # Path setup
    machine_name = socket.gethostname()
    sanitized_job_name = "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in job_name)
    sanitized_machine_name = "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in machine_name)
    job_dst = os.path.join(dest, sanitized_machine_name, sanitized_job_name)
    ensure_dir(job_dst)
    now = timestamp()
    backup_set_id_string = now
    backup_set_dir = os.path.join(job_dst, f"backup_set_{backup_set_id_string}")
    ensure_dir(backup_set_dir)

    max_tarball_size_mb = config.get("max_tarball_size", 1024)

    # For full backups, we ALWAYS create a new backup set with the config snapshot
    try:
        #Debug the config object
        logger.debug(f"config type: {type(config)}")
        logger.debug(f"config empty? {not bool(config)}")
        logger.debug(f"config keys: {list(config.keys()) if config else 'None'}")
        
        # Create a JSON string of the config for the config_snapshot field
        config_snapshot = json.dumps(config) if config else None
        
        # Debug the config_snapshot
        logger.debug(f"config_snapshot type: {type(config_snapshot)}")
        logger.debug(f"config_snapshot is None? {config_snapshot is None}")
        if config_snapshot:
            logger.debug(f"config_snapshot length: {len(config_snapshot)}")
            logger.debug(f"config_snapshot preview: {config_snapshot[:100]}...")

        # Create a new backup set entry in the database
        backup_set_id = get_or_create_backup_set(
            job_name=job_name,  # Use the original job name for database consistency
            set_name=backup_set_id_string,  # Use the timestamp as the set name
            config_settings=None  # Do not store the config snapshot
        )
        logger.debug(f"Created backup set with ID {backup_set_id}")

        # Use the event_id as the backup_job_id if provided, otherwise create a new job
        backup_job_id = event_id
        if not backup_job_id:
            backup_job_id = insert_backup_job(
                backup_set_id=backup_set_id,
                backup_type="full",
                encrypted=encrypt,
                synced=sync,
                event_message="Starting full backup"
            )
            logger.info(f"Created backup job with ID {backup_job_id}")
        else:
            # If we already have a backup_job_id (from the event), update it to point to our new backup set
            with get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "UPDATE backup_jobs SET backup_set_id = ? WHERE id = ?",
                    (backup_set_id, backup_job_id)
                )
                conn.commit()
                logger.debug(f"Updated backup job {backup_job_id} to use backup set {backup_set_id}")

    except Exception as e:
        error_msg = f"Failed to create database entries: {e}"
        logger.error(error_msg)
        if event_id and event_exists(event_id):
            finalize_event(
                event_id=event_id,
                status="error",
                event_message=error_msg,
                backup_set_id=None,
                runtime="00:00:00"
            )
        return None, event_id, None, None

    try:
        # Update the event to show we're collecting files
        if event_id and event_exists(event_id):
            update_event(event_id, event_message=f"Scanning source directory with {len(exclude_patterns)} exclude patterns")
        
        logger.debug(f"Collecting files with {len(exclude_patterns)} exclude patterns")
        
        # Let the should_exclude function handle all exclusions
        files = get_all_files(src, exclude_patterns)
        logger.debug(f"Collected {len(files)} files after applying exclusion patterns")
        
        # Update the event with progress
        if event_id and event_exists(event_id):
            update_event(event_id, event_message=f"Creating tar archives for {len(files)} files")
        
        tarball_paths = create_tar_archives(
            files, backup_set_dir, max_tarball_size_mb, logger, "full", config
        )
        
        encryption_enabled = config.get("encryption", {}).get("enabled", False) or encrypt
        new_tar_info = []
        total_files = 0
        total_size_bytes = 0
        
        # Update the event with progress
        if event_id and event_exists(event_id):
            update_event(event_id, event_message="Extracting file information from tarballs")
        
        # Extract file info from all tarballs
        for tar_path in tarball_paths:
            tar_info = extract_tar_info(tar_path, encryption_enabled=encryption_enabled)
            new_tar_info.extend(tar_info)
            total_files += len(tar_info)
            total_size_bytes += sum(f.get('size', 0) for f in tar_info)

        # Insert file records into the database
        if new_tar_info:
            if event_id and event_exists(event_id):
                update_event(event_id, event_message=f"Updating database with {len(new_tar_info)} files")
            logger.debug(f"Inserting {len(new_tar_info)} files into database...")
            insert_files(backup_job_id, new_tar_info)
            logger.info(f"Database updated with {total_files} files, {total_size_bytes} bytes")

        # Generate manifest HTML (now reads from database)
        if tarball_paths:
            logger.debug("Writing manifest files...")
            if event_id and event_exists(event_id):
                update_event(event_id, event_message="Generating manifest files")
                
            html_manifest_path = generate_archived_manifest(
                job_config_path=job_config_path,
                job_name=job_name,
                backup_set_id=backup_set_id_string,
                backup_set_path=backup_set_dir,
                backup_type="full"
            )
            logger.info(f"HTML Manifest written to: {html_manifest_path}")
        else:
            logger.warning("No tarballs created, skipping manifest generation.")
            total_files = 0
            total_size_bytes = 0

        # Copy restore.py
        try:
            shutil.copy2(RESTORE_SCRIPT_SRC, backup_set_dir)
        except (OSError, shutil.Error) as e:
            logger.warning(f"Could not copy restore.py to backup set: {e}")

        logger.debug(f"FULL backup completed for {src}")
        # Return the actual backup_set_dir instead of job_dst for consistency with incremental/differential
        return backup_set_dir, event_id, backup_set_id_string, tarball_paths

    except Exception as e:
        logger.error(f"An error occurred during the full backup process: {e}", exc_info=True)
        
        # Finalize the event ONLY for errors
        if event_id and event_exists(event_id):
            finalize_event(
                event_id=event_id,
                status="error",
                event_message=f"Backup failed: {e}",
                backup_set_id=None
            )
        raise
