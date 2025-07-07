from datetime import datetime
import os
import json
import socket
import re
from app.utils.logger import setup_logger, ensure_dir, timestamp
from app.utils.manifest import write_manifest_files, extract_tar_info
from app.utils.event_logger import update_event, finalize_event, event_exists
from core.encrypt import encrypt_tarballs
from .utils import create_tar_archives
from app.models.manifest_db import (
    get_backup_set_by_job_and_set,  # Use this instead of get_or_create
    get_last_full_backup_job,
    get_files_for_last_full_backup,
    insert_backup_job,
    finalize_backup_job,
    insert_files
)

def run_diff_backup(config, encrypt=False, sync=False, event_id=None, job_config_path=None, global_config=None):
    job_name = config.get("job_name", "unknown_job")
    logger = setup_logger(job_name)
    logger.info(f"Starting DIFFERENTIAL backup job '{job_name}' with provided config.")

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

    # Exclude patterns
    exclude_patterns = config.get("exclude_patterns", [])
    max_tarball_size_mb = config.get("max_tarball_size", 1024)

    # Get the LAST FULL backup job from the database
    # Differential backups compare against the last full backup only (not incrementals)
    last_full_backup_job = get_last_full_backup_job(job_name=sanitized_job_name)
    if not last_full_backup_job:
        logger.warning("No previous full backup job found in database, running full backup instead.")
        from .full import run_full_backup
        return run_full_backup(config, encrypt=encrypt, sync=sync, event_id=event_id, job_config_path=job_config_path, global_config=global_config)

    # Get the backup set that contains the last full backup
    backup_set_name = last_full_backup_job['set_name']
    logger.info(f"Found last full backup job in set: {backup_set_name}")

    # For differential backup, we ADD to the SAME backup set as the full backup
    # (this is different from incremental which continues from the last job)
    target_backup_set_name = backup_set_name
    target_backup_set_dir = os.path.join(job_dst, f"backup_set_{target_backup_set_name}")
    
    # Ensure the target backup set directory exists
    if not os.path.exists(target_backup_set_dir):
        logger.error(f"Target backup set directory not found: {target_backup_set_dir}")
        return None, event_id, None

    # Get files from the last FULL backup for comparison
    logger.info(f"Comparing against last full backup in set: {backup_set_name}")
    full_backup_files = get_files_for_last_full_backup(sanitized_job_name)
    
    # Convert database files to the format expected by comparison function
    manifest_data = {
        "files": []
    }
    
    for db_file in full_backup_files:
        manifest_data["files"].append({
            "path": db_file["path"],
            "mtime": db_file["mtime"],
            "size": db_file["size"]
        })

    logger.info(f"Comparing against {len(manifest_data['files'])} files from last full backup")

    # Detect new or modified files by comparing with the last full backup
    files = get_new_or_modified_files_from_data(src, manifest_data, exclude_patterns=exclude_patterns)
    if not files:
        logger.info("No files changed or added since last full backup. Skipping differential backup.")
        return "skipped", event_id, None

    logger.info(f"Found {len(files)} new or modified files since last full backup. Adding differential job to backup set: {target_backup_set_name}")

    # Get the existing backup set (DO NOT create a new one)
    backup_set_row = get_backup_set_by_job_and_set(sanitized_job_name, target_backup_set_name)
    if not backup_set_row:
        logger.error(f"Backup set not found: {sanitized_job_name}/{target_backup_set_name}")
        return None, event_id, None
    
    backup_set_id = backup_set_row['id']
    logger.info(f"Using existing backup set ID: {backup_set_id}")

    # Create a new differential backup job within the existing backup set
    backup_job_id = insert_backup_job(
        backup_set_id=backup_set_id,
        backup_type="differential",
        encrypted=encrypt,
        synced=sync,
        event_message="Differential backup started"
    )

    try:
        # Create tarballs for the new/changed files in the existing backup set directory
        tarball_paths = create_tar_archives(
            files, 
            target_backup_set_dir, 
            max_tarball_size_mb, 
            logger, 
            "differential", 
            config
        )
        
        encryption_enabled = config.get("encryption", {}).get("enabled", False) or encrypt
        new_tar_info = []
        for tar_path in tarball_paths:
            new_tar_info.extend(extract_tar_info(tar_path, encryption_enabled=encryption_enabled))

        # Mark files as modified for tracking (differential = files that changed since full)
        for file_info in new_tar_info:
            file_info['is_new'] = False
            file_info['is_modified'] = True  # Differential files are considered "modified" since full

        # Insert the files into the database for this backup job
        if tarball_paths:
            logger.info(f"Inserting {len(new_tar_info)} files into database for backup job {backup_job_id}")
            insert_files(backup_job_id, new_tar_info)
            
            # Calculate totals for job completion
            total_files = len(new_tar_info)
            total_size_bytes = sum(f.get('size', 0) for f in new_tar_info)
            
            # Mark the backup job as completed
            finalize_backup_job(
                job_id=backup_job_id,
                status="completed",
                event_message="Differential backup completed successfully",
                total_files=total_files,
                total_size_bytes=total_size_bytes
            )
            
            logger.info(f"Differential backup job completed: {total_files} files, {total_size_bytes} bytes")
            
            # Generate updated HTML manifest for the backup set (includes all jobs)
            logger.info("Updating manifest files...")
            json_manifest_path, html_manifest_path = write_manifest_files(
                job_config_path=job_config_path,
                job_name=sanitized_job_name,
                backup_set_id=target_backup_set_name,
                backup_set_path=target_backup_set_dir,
                new_tar_info=new_tar_info,  # Still passed for compatibility
                mode="differential"
            )
            logger.info(f"JSON Manifest written to: {json_manifest_path}")
            logger.info(f"HTML Manifest written to: {html_manifest_path}")
            
        else:
            logger.warning("No tarballs created, marking job as completed with no files.")
            finalize_backup_job(
                job_id=backup_job_id,
                status="completed",
                event_message="No files to backup"
            )

        if encrypt and tarball_paths:
            tarball_paths = encrypt_tarballs(tarball_paths, config, logger)

        if sync:
            from core.sync_s3 import sync_to_s3
            sync_to_s3(target_backup_set_dir, config, event_id)
            
            # Update the job to mark it as synced
            if tarball_paths:
                import time
                from app.models.manifest_db import get_db_connection
                with get_db_connection() as conn:
                    c = conn.cursor()
                    c.execute("UPDATE backup_jobs SET synced = 1 WHERE id = ?", (backup_job_id,))
                    conn.commit()

        logger.info("DIFFERENTIAL backup completed for %s", src)
        return target_backup_set_dir, event_id, target_backup_set_name

    except Exception as e:
        logger.error(f"Error during differential backup: {e}", exc_info=True)
        
        # Mark the backup job as failed
        finalize_backup_job(
            job_id=backup_job_id,
            status="failed",
            event_message="Differential backup failed",
            error_message=str(e)
        )
        
        if event_id and event_exists(event_id):
            finalize_event(
                event_id=event_id,
                status="error",
                event=f"Differential backup failed: {e}",
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
    
    # Build a lookup dict from manifest data for quick comparison
    manifest_files = {}
    for file_info in manifest_data.get("files", []):
        path = file_info.get("path", "")
        manifest_files[path] = {
            "mtime": file_info.get("mtime", 0),
            "size": file_info.get("size", 0)
        }
    
    new_or_modified = []
    
    # Walk the source directory
    for root, dirs, files in os.walk(src):
        # Apply exclusion patterns to directories
        dirs[:] = [d for d in dirs if not any(
            re.match(pattern, os.path.join(root, d)) or 
            re.match(pattern, d) for pattern in exclude_patterns
        )]
        
        for file in files:
            file_path = os.path.join(root, file)
            
            # Skip excluded files
            if any(re.match(pattern, file_path) or re.match(pattern, file) for pattern in exclude_patterns):
                continue
                
            try:
                stat_info = os.stat(file_path)
                current_mtime = stat_info.st_mtime
                current_size = stat_info.st_size
                
                # Make path relative to source directory
                rel_path = os.path.relpath(file_path, src)
                
                # Check if file is new or modified
                if rel_path not in manifest_files:
                    # New file
                    new_or_modified.append(file_path)
                else:
                    # Check if modified (compare mtime and size)
                    manifest_file = manifest_files[rel_path]
                    if (abs(current_mtime - manifest_file["mtime"]) > 1 or  # 1 second tolerance for mtime
                        current_size != manifest_file["size"]):
                        # Modified file
                        new_or_modified.append(file_path)
                        
            except OSError as e:
                # Skip files that can't be accessed
                logger = setup_logger("backup")
                logger.warning(f"Could not access file {file_path}: {e}")
                continue
    
    return new_or_modified