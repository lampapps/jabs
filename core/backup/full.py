import os
import shutil
import socket
import time
from datetime import datetime
from app.utils.logger import setup_logger, timestamp, ensure_dir
from app.utils.manifest import write_manifest_files, extract_tar_info
from app.utils.event_logger import update_event, finalize_event, event_exists
from app.settings import LOCK_DIR, RESTORE_SCRIPT_SRC
from core.encrypt import encrypt_tarballs
from .utils import get_all_files, create_tar_archives
from .common import rotate_backups

# Import database functions
from app.models.manifest_db import (
    get_or_create_backup_set,
    insert_backup_job,
    finalize_backup_job,
    insert_files
)

def run_full_backup(config, encrypt=False, sync=False, event_id=None, job_config_path=None, global_config=None):
    job_name = config.get("job_name", "unknown_job")
    logger = setup_logger(job_name)
    logger.info(f"Starting FULL backup job '{job_name}' with provided config.")

    src = config.get("source")
    dest = config.get("destination")
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

    # Exclude patterns
    exclude_patterns = config.get("exclude_patterns", [])
    max_tarball_size_mb = config.get("max_tarball_size", 1024)

    # Create database entries for the new backup
    try:
        # Step 1: Create or get the backup set
        config_snapshot = str(config) if config else None
        backup_set_id = get_or_create_backup_set(
            job_name=sanitized_job_name,
            set_name=backup_set_id_string,
            config_settings=config_snapshot
        )
        
        # Step 2: Create a new backup job
        job_id = insert_backup_job(
            backup_set_id=backup_set_id,
            backup_type="full",
            encrypted=encrypt,
            synced=sync,
            event_message="Starting full backup"
        )
        
        logger.info(f"Created database entries: backup_set_id={backup_set_id}, job_id={job_id}")

    except Exception as e:
        error_msg = f"Failed to create database entries: {e}"
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

    try:
        files = get_all_files(src, exclude_patterns)
        tarball_paths = create_tar_archives(
            files, backup_set_dir, max_tarball_size_mb, logger, "full", config
        )
        
        encryption_enabled = config.get("encryption", {}).get("enabled", False) or encrypt
        new_tar_info = []
        total_files = 0
        total_size_bytes = 0
        
        # Extract file info from all tarballs
        for tar_path in tarball_paths:
            tar_info = extract_tar_info(tar_path, encryption_enabled=encryption_enabled)
            new_tar_info.extend(tar_info)
            total_files += len(tar_info)
            total_size_bytes += sum(f.get('size', 0) for f in tar_info)

        # Step 3: Insert file records into the database
        if new_tar_info:
            logger.info(f"Inserting {len(new_tar_info)} files into database...")
            insert_files(job_id, new_tar_info)
            logger.info(f"Database updated with {total_files} files, {total_size_bytes} bytes")

        # Step 4: Generate manifest HTML (now reads from database)
        if tarball_paths:
            logger.info("Writing manifest files...")
            json_manifest_path, html_manifest_path = write_manifest_files(
                job_config_path=job_config_path,
                job_name=sanitized_job_name,
                backup_set_id=backup_set_id_string,
                backup_set_path=backup_set_dir,
                new_tar_info=new_tar_info,  # Still passed for compatibility
                mode="full"
            )
            logger.info(f"JSON Manifest written to: {json_manifest_path}")
            logger.info(f"HTML Manifest written to: {html_manifest_path}")
        else:
            logger.warning("No tarballs created, skipping manifest generation.")
            total_files = 0
            total_size_bytes = 0

        if encrypt and tarball_paths:
            tarball_paths = encrypt_tarballs(tarball_paths, config, logger)

        if sync:
            from core.sync_s3 import sync_to_s3
            sync_to_s3(backup_set_dir, config, event_id)

        # Step 5: Finalize the backup job in the database
        finalize_backup_job(
            job_id=job_id,
            status="completed",
            event_message="Full backup completed successfully",
            total_files=total_files,
            total_size_bytes=total_size_bytes
        )
        
        logger.info(f"Backup job finalized in database with {total_files} files")

        # Write last_full.txt
        with open(os.path.join(job_dst, "last_full.txt"), "w", encoding="utf-8") as f:
            f.write(backup_set_id_string)

        # --- RESOLVE keep_sets ---
        keep_sets = config.get("keep_sets", None)
        if keep_sets is None and global_config is not None:
            keep_sets = global_config.get("keep_sets", None)
        if keep_sets is None:
            keep_sets = 5  # fallback default
        keep_sets = int(keep_sets)

        rotate_backups(job_dst, keep_sets, logger, config)

        # Copy restore.py
        try:
            shutil.copy2(RESTORE_SCRIPT_SRC, backup_set_dir)
        except (OSError, shutil.Error) as e:
            logger.warning(f"Could not copy restore.py to backup set: {e}")

        logger.info(f"FULL backup completed for {src}")
        return backup_set_dir, event_id, backup_set_id_string

    except Exception as e:
        logger.error(f"An error occurred during the full backup process: {e}", exc_info=True)
        
        # Mark the job as failed in the database
        try:
            finalize_backup_job(
                job_id=job_id,
                status="failed",
                error_message=str(e),
                event_message=f"Backup failed: {e}"
            )
        except Exception as db_e:
            logger.error(f"Failed to update database with error status: {db_e}")
        
        if event_id and event_exists(event_id):
            finalize_event(
                event_id=event_id,
                status="error",
                event=f"Backup failed: {e}",
                backup_set_id=None
            )
        raise
