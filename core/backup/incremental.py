from datetime import datetime
import os
import json
import socket
import re
import fnmatch
from app.utils.logger import setup_logger, ensure_dir
from app.services.manifest import write_manifest_files, extract_tar_info
from app.models.events import create_event, update_event, finalize_event, delete_event, event_exists
from core.encrypt import encrypt_tarballs
from .utils import create_tar_archives, should_exclude

from app.models.db_core import get_db_connection
from app.models.backup_sets import get_or_create_backup_set, get_backup_set_by_job_and_set
from app.models.backup_jobs import insert_backup_job, finalize_backup_job, get_backup_job, get_last_backup_job, get_last_full_backup_job
from app.models.backup_files import insert_files, get_files_for_backup_set, get_files_for_last_full_backup

def run_incremental_backup(config, encrypt=False, sync=False, event_id=None, job_config_path=None, global_config=None):
    job_name = config.get("job_name", "unknown_job")
    logger = setup_logger(job_name)
    logger.info(f"Starting INCREMENTAL backup job '{job_name}' with provided config.")

    # Update event with our current status
    if event_id and event_exists(event_id):
        update_event(event_id, event_message=f"Initializing incremental backup for {job_name}")
    
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
        # Finalize the event ONLY for errors
        if event_id and event_exists(event_id):
            finalize_event(
                event_id=event_id,  # Use the passed event_id
                status="error",
                event_message=error_msg,
                backup_set_id=None,
                runtime="00:00:00"
            )
        return None, event_id, None

    # Update event for next stage
    if event_id and event_exists(event_id):
        update_event(event_id, event_message=f"Setting up backup paths and loading exclude patterns")
    
    # Path setup 
    machine_name = socket.gethostname()
    sanitized_job_name = "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in job_name)
    sanitized_machine_name = "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in machine_name)
    job_dst = os.path.join(dest, sanitized_machine_name, sanitized_job_name)
    ensure_dir(job_dst)

    # Exclude patterns: merge job-specific and common excludes if enabled
    exclude_patterns = []
    
    # First check if use_common_exclude is set in job config or inherited from global config
    use_common = config.get("use_common_exclude", False)
    if global_config:
        use_common = config.get("use_common_exclude", global_config.get("use_common_exclude", False))
    
    if use_common:
        # Load common_exclude.yaml
        import yaml
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

    # Get the LAST backup job (could be full or incremental) from the database
    # Use sanitized job name for consistency with database entries
    last_backup_job = get_last_backup_job(job_name=sanitized_job_name, completed_only=True)
    if not last_backup_job:
        logger.warning("No previous backup job found in database, running full backup instead.")
        # Update the event to show we're changing to full backup
        if event_id and event_exists(event_id):
            update_event(
                event_id=event_id,
                event_message="No previous backup job found; running full backup instead of incremental.",
                backup_type="full"
            )
        from .full import run_full_backup
        return run_full_backup(config, encrypt=encrypt, sync=sync, event_id=event_id, job_config_path=job_config_path, global_config=global_config)

    # Get the backup set that this job belongs to
    backup_set_name = last_backup_job['set_name']
    logger.info(f"Found last backup job in set: {backup_set_name}, type: {last_backup_job['backup_type']}")

    # For incremental backup, we need to find the correct backup set to add to:
    # - If last job was full, use that backup set
    # - If last job was incremental, use the same backup set
    target_backup_set_name = backup_set_name
    target_backup_set_dir = os.path.join(job_dst, f"backup_set_{target_backup_set_name}")
    
    # Ensure the target backup set directory exists
    if not os.path.exists(target_backup_set_dir):
        logger.error(f"Target backup set directory not found: {target_backup_set_dir}")
        return None, event_id, None

    # Get the backup set ID for comparison
    backup_set_id = get_or_create_backup_set(
        job_name=sanitized_job_name,
        set_name=target_backup_set_name,
        config_settings=json.dumps(config) if config else None
    )

    # CRITICAL FIX: Get ALL files from the backup set (not just the last job)
    # This includes files from the full backup AND all previous incremental backups
    logger.info(f"Checking for changes against ALL files in backup set: {target_backup_set_name}")
    all_backup_set_files = get_files_for_backup_set(backup_set_id)
    
    # Convert database files to the format expected by comparison function
    manifest_data = {
        "files": []
    }
    
    for db_file in all_backup_set_files:
        manifest_data["files"].append({
            "path": db_file["path"],
            "mtime": db_file["mtime"],
            "size": db_file["size"]
        })

    logger.info(f"Comparing against {len(manifest_data['files'])} files from backup set")

    # Detect new or modified files by comparing with ALL files in the backup set
    files = get_new_or_modified_files_from_data(src, manifest_data, exclude_patterns=exclude_patterns)
    if not files:
        logger.info("No files changed or added since last backup. Skipping incremental backup.")
        # We don't finalize here - let the CLI handle it
        return "skipped", event_id, None

    # Update event for starting the backup
    if event_id and event_exists(event_id):
        update_event(event_id, event_message=f"Found {len(files)} new or modified files, creating tarballs")

    # Use the event_id passed from CLI instead of creating a new backup job
    backup_job_id = event_id

    try:
        # Create tarballs for the new/changed files in the existing backup set directory
        tarball_paths = create_tar_archives(
            files, 
            target_backup_set_dir, 
            max_tarball_size_mb, 
            logger, 
            "incremental", 
            config
        )
        
        encryption_enabled = config.get("encryption", {}).get("enabled", False) or encrypt
        new_tar_info = []
        for tar_path in tarball_paths:
            new_tar_info.extend(extract_tar_info(tar_path, encryption_enabled=encryption_enabled))

        # Mark files as new or modified for tracking
        for file_info in new_tar_info:
            file_info['is_new'] = True  # For incremental, consider all as new additions
            file_info['is_modified'] = False

        # Insert the files into the database for this backup job
        if tarball_paths:
            # Calculate totals first
            total_files = len(new_tar_info)
            total_size_bytes = sum(f.get('size', 0) for f in new_tar_info)
            
            logger.info(f"Inserting {total_files} files into database for backup job {backup_job_id}")
            insert_files(backup_job_id, new_tar_info)
            
            # Update event with progress
            if event_id and event_exists(event_id):
                update_event(
                    event_id=event_id,
                    event_message=f"Backed up {total_files} files ({total_size_bytes} bytes)"
                )
            
            logger.info(f"Incremental backup job completed: {total_files} files, {total_size_bytes} bytes")
            
            # Generate updated HTML manifest for the backup set
            if event_id and event_exists(event_id):
                update_event(event_id, event_message="Generating manifest files")
                
            html_manifest_path = write_manifest_files(
                job_config_path=job_config_path,
                job_name=sanitized_job_name,
                backup_set_id=target_backup_set_name,
                backup_set_path=target_backup_set_dir,
                new_tar_info=new_tar_info,  # Still passed for compatibility
                mode="incremental"
            )
            logger.info(f"HTML Manifest written to: {html_manifest_path}")
            
        else:
            logger.warning("No tarballs created")
            if event_id and event_exists(event_id):
                update_event(event_id, event_message="No files to backup")

        # Important: Don't finalize the event here!
        # Just update it with progress information
        if event_id and event_exists(event_id):
            update_event(
                event_id=event_id,
                event_message="Incremental backup completed successfully",
                status="running"  # Keep it running so CLI can finalize it
            )

        # Handle encryption if enabled
        if encrypt and tarball_paths:
            if event_id and event_exists(event_id):
                update_event(event_id, event_message="Encrypting backup files")
            tarball_paths = encrypt_tarballs(tarball_paths, config, logger)
            if event_id and event_exists(event_id):
                update_event(event_id, event_message="Encryption completed")

        logger.info(f"INCREMENTAL backup completed for {src}")
        # Return the event_id that was passed to us
        return target_backup_set_dir, event_id, target_backup_set_name

    except Exception as e:
        logger.error(f"Error during incremental backup: {e}", exc_info=True)
        
        # Finalize the event for errors ONLY
        if event_id and event_exists(event_id):
            finalize_event(
                event_id=event_id,
                status="error",
                event_message=f"Incremental backup failed: {e}",
                backup_set_id=target_backup_set_name,
                runtime="00:00:00"
            )
        
        raise


def get_new_or_modified_files_from_data(src, manifest_data, exclude_patterns=None):
    """
    Compare filesystem with manifest data to find new or modified files.
    This replaces the JSON file-based comparison with in-memory data comparison.
    """
    if exclude_patterns is None:
        exclude_patterns = []
        
    # Debug - print the patterns to help diagnose issues
    logger = setup_logger("backup")
    logger.info(f"Incremental backup scanning with {len(exclude_patterns)} exclusion patterns")

    manifest_files = {}
    for file_info in manifest_data.get("files", []):
        path = file_info.get("path", "")
        manifest_files[path] = {
            "mtime": file_info.get("mtime", 0),
            "size": file_info.get("size", 0)
        }

    new_or_modified = []
    dirs_excluded = 0
    files_excluded = 0
        
    for root, dirs, files in os.walk(src):
        # Process directories to exclude before walking them
        i = 0
        while i < len(dirs):
            dir_path = os.path.join(root, dirs[i])
            rel_dir = os.path.relpath(dir_path, src)
            dir_name = os.path.basename(dir_path)
            
            if should_exclude(dir_path, exclude_patterns, src):
                logger.info(f"EXCLUDING directory: '{rel_dir}/' (matched exclude pattern)")
                dirs.pop(i)
                dirs_excluded += 1
            else:
                logger.debug(f"Including directory: {rel_dir}/")
                i += 1

        # Process files
        for file in files:
            file_path = os.path.join(root, file)
            rel_path = os.path.relpath(file_path, src)

            # Check if file should be excluded
            if should_exclude(file_path, exclude_patterns, src):
                logger.info(f"EXCLUDING file: {rel_path}")
                files_excluded += 1
                continue

            try:
                stat_info = os.stat(file_path)
                current_mtime = stat_info.st_mtime
                current_size = stat_info.st_size

                # Check if file is new or modified
                if rel_path not in manifest_files:
                    new_or_modified.append(file_path)
                else:
                    manifest_file = manifest_files[rel_path]
                    if (abs(current_mtime - manifest_file["mtime"]) > 1 or
                        current_size != manifest_file["size"]):
                        new_or_modified.append(file_path)

            except OSError as e:
                logger.warning(f"Could not access file {file_path}: {e}")
                continue

    logger.info(f"SUMMARY: Excluded {dirs_excluded} directories and {files_excluded} files based on patterns")
    logger.info(f"Found {len(new_or_modified)} new or modified files to back up")
    return new_or_modified