"""Restore logic for JABS: handles file and full restores from backup sets."""

import os
import tarfile
import json
import subprocess
import time
from typing import List, Dict, Any, Optional
from app.utils.logger import setup_logger
from app.utils.restore_status import set_restore_status
from app.utils.event_logger import initialize_event, update_event, finalize_event, event_exists
from app.models.manifest_db import (
    get_backup_set_by_job_and_set, 
    get_files_for_backup_set,
    get_jobs_for_backup_set,
    list_backup_sets
)

def get_passphrase():
    """
    Retrieve the GPG passphrase from the environment variable.
    Returns None if not set.
    """
    return os.getenv("JABS_ENCRYPT_PASSPHRASE")

def get_manifest_from_db(job_name: str, backup_set_id: str, logger) -> Dict[str, Any]:
    """
    Load the manifest data from the database for a given job and backup set.
    :param job_name: Name of the backup job.
    :param backup_set_id: Set name (timestamp string) identifying the backup set.
    :param logger: Logger instance for logging.
    :return: Manifest dictionary with config and files.
    """
    logger.info(f"Loading manifest from DB for job '{job_name}', set '{backup_set_id}'")
    
    # Debug: List all backup sets to see what's available
    all_sets = list_backup_sets(job_name=None, limit=50)
    logger.info(f"Debug: Found {len(all_sets)} backup sets in database:")
    for backup_set in all_sets:
        logger.info(f"  - Job: '{backup_set['job_name']}', Set: '{backup_set['set_name']}'")
    
    # Try to find backup set
    backup_set = get_backup_set_by_job_and_set(job_name, backup_set_id)
    if not backup_set:
        # Try with sanitized job name (how backup jobs typically store names)
        sanitized_job = "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in job_name)
        logger.info(f"Backup set not found with original job name '{job_name}', trying sanitized: '{sanitized_job}'")
        backup_set = get_backup_set_by_job_and_set(sanitized_job, backup_set_id)
        
        if not backup_set:
            # List all sets for this job name to help debug
            job_sets = list_backup_sets(job_name=job_name, limit=10)
            sanitized_sets = list_backup_sets(job_name=sanitized_job, limit=10)
            
            logger.error(f"No backup sets found for job '{job_name}' ({len(job_sets)} sets)")
            logger.error(f"No backup sets found for sanitized job '{sanitized_job}' ({len(sanitized_sets)} sets)")
            
            raise FileNotFoundError(f"Backup set '{backup_set_id}' not found for job '{job_name}' or '{sanitized_job}'")
    
    # Get all files for this backup set
    files = get_files_for_backup_set(backup_set['id'])
    logger.info(f"Loaded manifest with {len(files)} files from database.")
    
    # Parse config from backup set
    config = {}
    if backup_set.get('config_snapshot'):
        try:
            # Try JSON first (newer format)
            config = json.loads(backup_set['config_snapshot'])
        except json.JSONDecodeError:
            # Fall back to string representation (older format)
            logger.warning("Config snapshot is not valid JSON, using string representation")
            config = {"source": "/unknown"}  # Provide a default
    
    # Ensure config has required fields
    if 'source' not in config:
        logger.warning("No source directory found in config, using default")
        config['source'] = "/unknown"
    
    return {
        'job_name': backup_set['job_name'],
        'set_name': backup_set['set_name'],
        'config': config,
        'files': files
    }

def reconstruct_tarball_path(backup_set_name: str, tarball_basename: str, job_name: str, config: Dict[str, Any]) -> str:
    """
    Reconstruct the full path to a tarball file from the backup set name and tarball basename.
    :param backup_set_name: The backup set timestamp (e.g., "20250706_151807")
    :param tarball_basename: The tarball filename (e.g., "full_part_1_20250706_151807.tar.gz.gpg")
    :param job_name: The job name for path construction
    :param config: Config dict containing destination info
    :return: Full path to the tarball file
    """
    import socket
    
    # Get destination from config
    destination = config.get('destination', '/tmp/backup')
    
    # Sanitize names for filesystem paths
    machine_name = socket.gethostname()
    sanitized_job_name = "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in job_name)
    sanitized_machine_name = "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in machine_name)
    
    # Construct the full path: destination/machine/job/backup_set_timestamp/tarball_file
    full_path = os.path.join(
        destination,
        sanitized_machine_name,
        sanitized_job_name,
        f"backup_set_{backup_set_name}",
        tarball_basename
    )
    
    return full_path

def extract_file_from_tarball(tarball_path: str, member_path: str, target_path: str, logger) -> tuple[bool, Optional[str]]:
    """
    Extract a single file from a tarball (optionally GPG-encrypted) to the target path.
    :param tarball_path: Path to the (possibly encrypted) tarball.
    :param member_path: Path of the file inside the tarball to extract.
    :param target_path: Destination path for the extracted file.
    :param logger: Logger instance for logging.
    :return: (success: bool, error_message: str or None)
    """
    logger.info(f"Extracting '{member_path}' from '{tarball_path}' to '{target_path}'")
    
    # Check if tarball file exists
    if not os.path.exists(tarball_path):
        error_msg = f"Tarball file does not exist: {tarball_path}"
        logger.error(error_msg)
        return False, error_msg
    
    try:
        if tarball_path.endswith('.gpg'):
            passphrase = get_passphrase()
            if not passphrase:
                logger.error("GPG passphrase not set in environment or .env file.")
                return False, "GPG passphrase not set. Cannot decrypt archive."
            gpg_cmd = [
                "gpg", "--batch", "--yes", "--passphrase", passphrase,
                "-d", tarball_path
            ]
            proc = subprocess.Popen(gpg_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            try:
                with tarfile.open(fileobj=proc.stdout, mode='r|*') as tar:
                    for member in tar:
                        if member.name == member_path:
                            os.makedirs(os.path.dirname(target_path), exist_ok=True)
                            with open(target_path, "wb") as out_f, tar.extractfile(member) as in_f:
                                out_f.write(in_f.read())
                            # Set mtime from tar member
                            if hasattr(member, "mtime"):
                                os.utime(target_path, (member.mtime, member.mtime))
                            logger.info(f"Successfully restored '{member_path}' to '{target_path}'")
                            return True, None
                    logger.error(f"{member_path} not found in {tarball_path}")
                    return False, f"{member_path} not found in {tarball_path}"
            except tarfile.ReadError:
                gpg_err = proc.stderr.read().decode()
                logger.error(f"GPG decryption or tar extraction failed: {gpg_err}")
                user_msg = (
                    f"Cannot restore '{member_path}' from '{os.path.basename(tarball_path)}':\n"
                    "GPG decryption failed or output is not a valid tar archive.\n"
                    f"GPG error: {gpg_err.strip()}"
                )
                return False, user_msg
            finally:
                proc.stdout.close()
                proc.stderr.close()
                proc.wait()
        else:
            with tarfile.open(tarball_path, 'r:*') as tar:
                member = tar.getmember(member_path)
                os.makedirs(os.path.dirname(target_path), exist_ok=True)
                with open(target_path, "wb") as out_f, tar.extractfile(member) as in_f:
                    out_f.write(in_f.read())
                # Set mtime from tar member
                if hasattr(member, "mtime"):
                    os.utime(target_path, (member.mtime, member.mtime))
            logger.info(f"Successfully restored '{member_path}' to '{target_path}'")
            return True, None
    except KeyError:
        logger.error(f"{member_path} not found in {tarball_path}")
        return False, f"{member_path} not found in {tarball_path}"
    except (OSError, tarfile.TarError, subprocess.SubprocessError) as e:
        logger.error(f"Error extracting '{member_path}' from '{tarball_path}': {e}")
        return False, str(e)

def restore_files(
    job_name: str, 
    backup_set_id: str, 
    files: List[Dict[str, str]], 
    dest: Optional[str] = None, 
    base_dir: Optional[str] = None,
    event_id: Optional[str] = None, 
    restore_option: str = "selected", 
    logger = None
) -> Dict[str, List]:
    """
    Restore a list of files from a backup set using direct tarball extraction.
    """
    if logger is None:
        logger = setup_logger(job_name, log_file="restore.log")
    
    logger.info(f"PASSPHRASE loaded: {'YES' if get_passphrase() else 'NO'}")
    logger.info(f"Starting simplified restore for job '{job_name}', backup_set_id '{backup_set_id}'")
    logger.info(f"Files requested for restore: {files}")
    
    # Track both original and effective job names for cleanup
    original_job_name = job_name
    effective_job_name = job_name
    
    set_restore_status(original_job_name, backup_set_id, running=True)
    
    restored = []
    errors = []

    try:
        # Get basic config info (we need the actual destination path for tarball reconstruction)
        backup_set = get_backup_set_by_job_and_set(job_name, backup_set_id)
        if not backup_set:
            # Try with sanitized job name
            sanitized_job = "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in job_name)
            backup_set = get_backup_set_by_job_and_set(sanitized_job, backup_set_id)
            effective_job_name = sanitized_job  # Use the sanitized name for path construction
        
        if not backup_set:
            error_msg = f"Backup set '{backup_set_id}' not found for job '{job_name}'"
            logger.error(error_msg)
            return {"restored": [], "errors": [{"file": "backup_set", "error": error_msg}]}
        
        # Load config from actual config files instead of database snapshot
        import yaml
        from app.settings import GLOBAL_CONFIG_PATH
        
        config = {}
        
        # Load global config first
        try:
            with open(GLOBAL_CONFIG_PATH, 'r', encoding='utf-8') as f:
                global_config = yaml.safe_load(f)
                if 'destination' in global_config:
                    config['destination'] = global_config['destination']
                if 'source' in global_config:
                    config['source'] = global_config['source']
        except Exception as e:
            logger.warning(f"Could not load global config: {e}")
        
        # Load job-specific config to override global settings
        # Use the original job name for config file lookup
        job_config_path = f"/home/jim/jabs_dev/config/jobs/{original_job_name}.yaml"
        try:
            with open(job_config_path, 'r', encoding='utf-8') as f:
                job_config = yaml.safe_load(f)
                # Job config overrides global config
                if 'destination' in job_config:
                    config['destination'] = job_config['destination']
                if 'source' in job_config:
                    config['source'] = job_config['source']
        except Exception as e:
            logger.warning(f"Could not load job config from {job_config_path}: {e}")
        
        # If still no destination, try the database config snapshot as fallback
        if 'destination' not in config:
            config_snapshot = backup_set['config_snapshot'] if backup_set['config_snapshot'] else None
            if config_snapshot:
                try:
                    db_config = json.loads(config_snapshot)
                    if 'destination' in db_config:
                        config['destination'] = db_config['destination']
                    if 'source' in db_config and 'source' not in config:
                        config['source'] = db_config['source']
                except json.JSONDecodeError:
                    logger.warning("Config snapshot is not valid JSON")
        
        # Final fallback
        if 'destination' not in config:
            config['destination'] = '/tmp/backup'
        if 'source' not in config:
            config['source'] = '/tmp/jabs_restore'
        
        logger.info(f"Using config - destination: {config['destination']}, source: {config.get('source', 'not set')}")
        
        # Set default restore destination if not provided
        if not dest and 'source' in config:
            dest = config['source']
        elif not dest:
            dest = "/tmp/jabs_restore"  # fallback

        # Initialize event if needed
        if not event_id:
            event_desc = f"Restoring {len(files)} files to: {dest}"
            event_id = initialize_event(
                job_name=effective_job_name,
                event=event_desc,
                backup_type="restore",
                encrypt=False,
                sync=False
            )
            update_event(event_id, event=event_desc, status="running")

        for i, file_info in enumerate(files):
            file_path = file_info["path"]
            tarball_name = file_info.get("tarball_path", file_info.get("tarball"))
            
            logger.info(f"Restoring file {i+1}/{len(files)}: {file_path} from {tarball_name}")
            
            # Construct full tarball path using effective job name
            tarball_path = reconstruct_tarball_path(
                backup_set_name=backup_set_id,
                tarball_basename=tarball_name,
                job_name=effective_job_name,
                config=config
            )
            
            logger.info(f"Tarball path: {tarball_path}")
            
            # Check if tarball exists
            if not os.path.exists(tarball_path):
                error_msg = f"Tarball not found: {tarball_path}"
                logger.error(error_msg)
                errors.append({"file": file_path, "error": error_msg})
                continue
            
            # Determine target path
            target_path = os.path.join(dest, file_path)
            
            # Extract the file directly
            success, error = extract_file_from_tarball(tarball_path, file_path, target_path, logger)
            
            if success:
                restored.append(target_path)
                logger.info(f"Successfully restored: {target_path}")
            else:
                errors.append({"file": file_path, "error": error})
                logger.error(f"Failed to restore {file_path}: {error}")
                
        logger.info(f"Restore complete. Restored: {len(restored)}, Errors: {len(errors)}")
        
        # Update event
        if event_id:
            if errors:
                status = "error"
                event_msg = f"Restore completed with errors: {len(restored)} restored, {len(errors)} failed"
            else:
                status = "completed"
                event_msg = f"Restore completed successfully: {len(restored)} files"
            finalize_event(event_id, event=event_msg, runtime="-", status=status)

    except Exception as e:
        logger.error(f"Failed to get backup set info: {e}")
        errors.append({"file": "config", "error": str(e)})
        
        if event_id:
            update_event(event_id, event=f"Restore failed: {str(e)}", status="error")
    finally:
        # Always cleanup restore status using the original job name
        logger.info(f"Cleaning up restore status for job '{original_job_name}', backup_set '{backup_set_id}'")
        try:
            set_restore_status(original_job_name, backup_set_id, running=False)
            logger.info("Successfully cleaned up restore status")
        except Exception as cleanup_error:
            logger.error(f"Failed to cleanup restore status: {cleanup_error}")

    logger.info("Restore function complete")
    return {"restored": restored, "errors": errors}

def restore_full(job_name: str, backup_set_id: str, dest: Optional[str] = None, base_dir: Optional[str] = None, event_id: Optional[str] = None) -> Dict[str, List]:
    """
    Restore all files from a backup set using the simplified direct extraction approach.
    For sets with multiple jobs (full + incremental/differential), restores files
    from the latest version of each file across all jobs in the set.
    """
    logger = setup_logger(job_name, log_file="restore.log")
    logger.info(f"Starting full restore for job '{job_name}', backup_set_id '{backup_set_id}'")
    
    # Track both original and effective job names for cleanup
    original_job_name = job_name
    effective_job_name = job_name
    
    set_restore_status(original_job_name, backup_set_id, running=True)
    
    try:
        # Get backup set info without loading all files
        backup_set = get_backup_set_by_job_and_set(job_name, backup_set_id)
        if not backup_set:
            # Try with sanitized job name
            sanitized_job = "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in job_name)
            backup_set = get_backup_set_by_job_and_set(sanitized_job, backup_set_id)
            effective_job_name = sanitized_job
        
        if not backup_set:
            error_msg = f"Backup set '{backup_set_id}' not found for job '{job_name}'"
            logger.error(error_msg)
            return {"restored": [], "errors": [{"file": "backup_set", "error": error_msg}]}
        
        # Get all files for this backup set (we need them to determine latest versions)
        logger.info("Loading file list from database for version resolution...")
        all_files = get_files_for_backup_set(backup_set['id'])
        logger.info(f"Loaded {len(all_files)} files from database")
        
        # Get jobs in the backup set
        jobs = get_jobs_for_backup_set(backup_set['id'])
        completed_jobs = [j for j in jobs if j['status'] == 'completed']
        
        if not completed_jobs:
            error_msg = "No completed jobs found in backup set"
            logger.error(error_msg)
            return {"restored": [], "errors": [{"file": "jobs", "error": error_msg}]}
        
        # Group files by path and get the latest version of each
        logger.info("Resolving latest version of each file...")
        file_versions = {}
        for file_entry in all_files:
            path = file_entry['path']
            job_started_at = file_entry.get('job_started_at', 0)
            
            # Keep the file from the most recent job
            if path not in file_versions or job_started_at > file_versions[path].get('job_started_at', 0):
                file_versions[path] = file_entry
        
        # Convert to list format expected by restore_files
        files_to_restore = [
            {"path": file_entry["path"], "tarball": file_entry["tarball"]}
            for file_entry in file_versions.values()
        ]
        
        logger.info(f"Restoring {len(files_to_restore)} files (latest version of each)")
        
        # Use the simplified restore_files function
        result = restore_files(
            original_job_name, backup_set_id, files_to_restore, dest=dest, base_dir=base_dir,
            event_id=event_id, restore_option="full", logger=logger
        )
        
        return result
        
    except Exception as e:
        logger.error(f"Full restore failed: {e}", exc_info=True)
        return {"restored": [], "errors": [{"file": "full_restore", "error": str(e)}]}
    finally:
        # Always cleanup restore status using the original job name
        logger.info(f"Cleaning up restore status for job '{original_job_name}', backup_set '{backup_set_id}'")
        try:
            set_restore_status(original_job_name, backup_set_id, running=False)
            logger.info("Successfully cleaned up restore status")
        except Exception as cleanup_error:
            logger.error(f"Failed to cleanup restore status: {cleanup_error}")
        
    logger.info("Full restore function complete")