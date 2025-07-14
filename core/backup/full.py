import os
import shutil
import socket
import time
import yaml
from datetime import datetime
from app.utils.logger import setup_logger, timestamp, ensure_dir
from app.utils.manifest import write_manifest_files, extract_tar_info
from app.utils.event_logger import update_event, finalize_event, event_exists
from app.settings import LOCK_DIR, RESTORE_SCRIPT_SRC
from core.encrypt import encrypt_tarballs
from .utils import get_all_files, create_tar_archives, should_exclude
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

    # Debug common exclude settings
    use_common_exclude = config.get("use_common_exclude", False)
    logger.info(f"use_common_exclude setting: {use_common_exclude}")
    if global_config and "use_common_exclude" in global_config:
        logger.info(f"Global use_common_exclude setting: {global_config.get('use_common_exclude')}")

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

    # Exclude patterns: merge job-specific and common excludes if enabled
    exclude_patterns = []
    
    # First check if use_common_exclude is set in job config or inherited from global config
    use_common = config.get("use_common_exclude", False)
    if global_config:
        use_common = config.get("use_common_exclude", global_config.get("use_common_exclude", False))
    
    if use_common:
        # Load common_exclude.yaml
        common_exclude_path = os.path.join(os.path.dirname(job_config_path or ""), "..", "common_exclude.yaml")
        logger.info(f"Loading common exclude patterns from: {common_exclude_path}")
        try:
            with open(common_exclude_path, "r", encoding="utf-8") as f:
                common_excludes = yaml.safe_load(f)
            if isinstance(common_excludes, dict):
                exclude_patterns.extend(common_excludes.get("exclude", []))
            elif isinstance(common_excludes, list):
                exclude_patterns.extend(common_excludes)
            logger.info(f"Loaded {len(exclude_patterns)} common exclude patterns")
        except Exception as e:
            logger.warning(f"Could not load common_exclude.yaml: {e}")

    # Add job-specific excludes
    job_excludes = config.get("exclude", [])
    exclude_patterns.extend(job_excludes)
    logger.info(f"Added {len(job_excludes)} job-specific exclude patterns")
    
    # Also add any legacy 'exclude_patterns' key
    legacy_excludes = config.get("exclude_patterns", [])
    exclude_patterns.extend(legacy_excludes)
    if legacy_excludes:
        logger.info(f"Added {len(legacy_excludes)} legacy exclude patterns")
        
    # Log all patterns for debugging
    logger.info(f"Total exclude patterns: {len(exclude_patterns)}")
    for i, pattern in enumerate(exclude_patterns):
        logger.info(f"  Pattern {i+1}: '{pattern}'")

    # Special directory exclusion check
    logger.info("Checking for Pictures/ and venv/ directories in exclusion patterns:")
    pictures_pattern = "Pictures/"
    venv_pattern = "venv/"
    
    if pictures_pattern in exclude_patterns:
        logger.info(f"Found '{pictures_pattern}' in exclusion patterns")
    else:
        logger.warning(f"'{pictures_pattern}' NOT found in exclusion patterns")
        
    if venv_pattern in exclude_patterns:
        logger.info(f"Found '{venv_pattern}' in exclusion patterns")
    else:
        logger.warning(f"'{venv_pattern}' NOT found in exclusion patterns")

    max_tarball_size_mb = config.get("max_tarball_size", 1024)

    # Check if we already have a job_id from the event_id
    backup_set_id = None
    job_id = None
    
    if event_id:
        job_id = event_id
        # Update the event with progress
        update_event(event_id, event="Processing files for full backup")
        
        # Get the backup set ID associated with this job
        from app.models.manifest_db import get_backup_job
        job = get_backup_job(job_id)
        if job:
            backup_set_id = job['backup_set_id']
            logger.info(f"Using existing job_id={job_id} with backup_set_id={backup_set_id}")
    
    # Only create database entries if we don't have a job_id yet
    if not job_id:
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
    else:
        logger.info(f"Using existing job_id={job_id} from event_id")

    try:
        logger.info(f"Collecting files with {len(exclude_patterns)} exclude patterns")
        files = get_all_files(src, exclude_patterns)
        logger.info(f"Collected {len(files)} files after applying exclusion patterns")
        
        # Update the event with progress
        update_event(job_id, event=f"Creating tar archives for {len(files)} files")
        
        tarball_paths = create_tar_archives(
            files, backup_set_dir, max_tarball_size_mb, logger, "full", config
        )
        
        encryption_enabled = config.get("encryption", {}).get("enabled", False) or encrypt
        new_tar_info = []
        total_files = 0
        total_size_bytes = 0
        
        # Update the event with progress
        update_event(job_id, event="Extracting file information from tarballs")
        
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
            logger.info(f"HTML Manifest written to: {html_manifest_path}")
        else:
            logger.warning("No tarballs created, skipping manifest generation.")
            total_files = 0
            total_size_bytes = 0

        if encrypt and tarball_paths:
            tarball_paths = encrypt_tarballs(tarball_paths, config, logger)

        if sync:
            update_event(job_id, event="Starting sync to S3")
            from core.sync_s3 import sync_to_s3
            sync_to_s3(backup_set_dir, config, job_id)  # Use job_id here instead of event_id

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
        return backup_set_dir, job_id, backup_set_id_string

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
