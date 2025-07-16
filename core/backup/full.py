import os
import shutil
import socket
import time
import yaml
from datetime import datetime
from app.utils.logger import setup_logger, timestamp, ensure_dir
from app.services.manifest import write_manifest_files, extract_tar_info
from app.models.events import create_event, update_event, finalize_event, delete_event, event_exists
from app.settings import LOCK_DIR, RESTORE_SCRIPT_SRC
from core.encrypt import encrypt_tarballs
from .utils import get_all_files, create_tar_archives, should_exclude
from .common import rotate_backups

from app.models.db_core import get_db_connection
from app.models.backup_sets import get_or_create_backup_set, get_backup_set_by_job_and_set
from app.models.backup_jobs import insert_backup_job, finalize_backup_job, get_backup_job, get_last_backup_job, get_last_full_backup_job
from app.models.backup_files import insert_files, get_files_for_backup_set, get_files_for_last_full_backup

def run_full_backup(config, encrypt=False, sync=False, event_id=None, job_config_path=None, global_config=None):
    job_name = config.get("job_name", "unknown_job")
    logger = setup_logger(job_name)
    logger.info(f"Starting FULL backup job '{job_name}' with provided config.")

    # Update the event if we have one
    if event_id and event_exists(event_id):
        update_event(event_id, event_message=f"Initializing full backup for {job_name}")

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
                event_message=error_msg,
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
        
    # Log all patterns for debug
    logger.info(f"Total exclude patterns: {len(exclude_patterns)}")
    for i, pattern in enumerate(exclude_patterns):
        logger.info(f"  Pattern {i+1}: '{pattern}'")

    max_tarball_size_mb = config.get("max_tarball_size", 1024)

    # Get the backup job ID from the event
    # In our schema, the event ID IS the backup job ID
    backup_job_id = event_id
    backup_set_id = None
    
    if backup_job_id:
        logger.info(f"Using event_id as backup_job_id: {backup_job_id}")
        # Get the backup set ID associated with this job
        with get_db_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute('SELECT backup_set_id FROM backup_jobs WHERE id = ?', (backup_job_id,))
                job_result = cursor.fetchone()
                if job_result and job_result['backup_set_id']:
                    backup_set_id = job_result['backup_set_id']
                    logger.info(f"Using backup set ID {backup_set_id} from job {backup_job_id}")
            except Exception as e:
                logger.warning(f"Could not get backup set ID from backup job: {e}")

    # If we don't have a backup job ID, check if we need to create database entries
    # (This is a fallback and should not be needed if CLI creates them properly)
    if not backup_job_id:
        try:
            # Step 1: Create or get the backup set
            config_snapshot = str(config) if config else None
            backup_set_id = get_or_create_backup_set(
                job_name=sanitized_job_name,
                set_name=backup_set_id_string,
                config_settings=config_snapshot
            )
            
            # Step 2: Create a new backup job
            backup_job_id = insert_backup_job(
                backup_set_id=backup_set_id,
                backup_type="full",
                encrypted=encrypt,
                synced=sync,
                event_message="Starting full backup"
            )
            
            logger.info(f"Created database entries: backup_set_id={backup_set_id}, backup_job_id={backup_job_id}")

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
            return None, event_id, None

    try:
        # Update the event to show we're collecting files
        if event_id and event_exists(event_id):
            update_event(event_id, event_message=f"Scanning source directory with {len(exclude_patterns)} exclude patterns")
        
        logger.info(f"Collecting files with {len(exclude_patterns)} exclude patterns")
        
        # Make sure we're not expecting special case handling for directories
        # Let the should_exclude function handle all exclusions
        files = get_all_files(src, exclude_patterns)
        logger.info(f"Collected {len(files)} files after applying exclusion patterns")
        
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
            logger.info(f"Inserting {len(new_tar_info)} files into database...")
            insert_files(backup_job_id, new_tar_info)
            logger.info(f"Database updated with {total_files} files, {total_size_bytes} bytes")

        # Generate manifest HTML (now reads from database)
        if tarball_paths:
            logger.info("Writing manifest files...")
            if event_id and event_exists(event_id):
                update_event(event_id, event_message="Generating manifest files")
                
            html_manifest_path = write_manifest_files(
                job_config_path=job_config_path,
                job_name=sanitized_job_name,
                backup_set_id=backup_set_id_string,
                backup_set_path=backup_set_dir,
                new_tar_info=new_tar_info,
                mode="full"
            )
            logger.info(f"HTML Manifest written to: {html_manifest_path}")
        else:
            logger.warning("No tarballs created, skipping manifest generation.")
            total_files = 0
            total_size_bytes = 0

        # Handle encryption if enabled
        if encrypt and tarball_paths:
            if event_id and event_exists(event_id):
                update_event(event_id, event_message="Encrypting backup files")
            tarball_paths = encrypt_tarballs(tarball_paths, config, logger)
            if event_id and event_exists(event_id):
                update_event(event_id, event_message="Encryption completed")

        # Important: Don't finalize the event here!
        # Just update it with progress information
        if event_id and event_exists(event_id):
            update_event(
                event_id=event_id,
                event_message="Full backup completed successfully",
                status="running"  # Keep it running so CLI can finalize it
            )

        # Write last_full.txt debug
        #with open(os.path.join(job_dst, "last_full.txt"), "w", encoding="utf-8") as f:
        #    f.write(backup_set_id_string)

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
        
        # Finalize the event ONLY for errors
        if event_id and event_exists(event_id):
            finalize_event(
                event_id=event_id,
                status="error",
                event_message=f"Backup failed: {e}",
                backup_set_id=None
            )
        raise
