import os
import socket
import time
import json
import yaml
from app.utils.logger import setup_logger, timestamp
from app.models.events import update_event, finalize_event, event_exists
from .utils import get_all_files
import boto3
from botocore.exceptions import ClientError

from app.models.backup_sets import get_or_create_backup_set
from app.models.backup_jobs import insert_backup_job, finalize_backup_job
from app.models.backup_files import insert_files
from app.models.db_core import get_db_connection

def check_s3_accessible(config, logger):
    """Check if S3 bucket is accessible and writable."""
    aws = config.get("aws", {})
    if not aws.get("enabled"):
        logger.info("S3 sync not enabled, skipping S3 check.")
        return True
    
    bucket = aws.get("bucket")
    region = aws.get("region")
    profile = aws.get("profile", "default")
    
    if not bucket:
        logger.error("S3 enabled but no bucket specified in config.")
        return False
    
    try:
        session = boto3.Session(profile_name=profile, region_name=region)
        s3 = session.resource('s3')
        
        # Try to access the bucket (this checks if it exists and we have permissions)
        s3.meta.client.head_bucket(Bucket=bucket)
        logger.info(f"S3 bucket '{bucket}' is accessible.")
        
        # Test write permissions with a small test object
        test_key = f"jabs_dryrun_test_{int(time.time())}.txt"
        s3.Bucket(bucket).put_object(Key=test_key, Body=b"dryrun test")
        s3.Object(bucket, test_key).delete()
        logger.info(f"S3 bucket '{bucket}' is writable.")
        return True
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == '404':
            logger.error(f"S3 bucket '{bucket}' does not exist.")
        elif error_code == 'AccessDenied':
            logger.error(f"Access denied to S3 bucket '{bucket}'. Check AWS credentials and permissions.")
        else:
            logger.error(f"S3 bucket '{bucket}' is not accessible: {e}")
        return False
    except Exception as e:
        logger.error(f"Error checking S3 bucket '{bucket}': {e}")
        return False

def run_dryrun_backup(config, encrypt=False, sync=False, event_id=None, job_config_path=None, global_config=None):
    """
    Perform a dryrun backup that mimics a full backup but only writes to the database.
    - Checks source and destination folder access
    - Creates database entries (backup set, job, and files)
    - Does NOT create archive files, directories, or HTML manifest
    - If S3 sync enabled, only checks bucket accessibility
    """
    job_name = config.get("job_name", "unknown_job")
    logger = setup_logger(job_name)
    logger.info(f"Starting DRYRUN backup job '{job_name}' with provided config.")
    
    # Update event with our current status
    if event_id and event_exists(event_id):
        update_event(event_id, event_message=f"Initializing dryrun backup for {job_name}")

    # Debug common exclude settings
    use_common_exclude = config.get("use_common_exclude", False)
    logger.info(f"use_common_exclude setting: {use_common_exclude}")
    if global_config and "use_common_exclude" in global_config:
        logger.info(f"Global use_common_exclude setting: {global_config.get('use_common_exclude')}")

    src = config.get("source")
    dest = config.get("destination")
    
    # Update event for path validation
    if event_id and event_exists(event_id):
        update_event(event_id, event_message="Validating source and destination paths")
    
    # Test source folder
    if not src or not os.path.exists(src):
        error_msg = f"Source path does not exist: {src}"
        logger.error(error_msg)
        # Finalize the event ONLY for errors
        if event_id and event_exists(event_id):
            finalize_event(
                event_id=event_id,
                status="error",
                event_message=error_msg,
                backup_set_id=None,
                runtime="00:00:00"
            )
        return None, event_id, None
    
    if not os.access(src, os.R_OK):
        error_msg = f"Source path is not readable: {src}"
        logger.error(error_msg)
        # Finalize the event ONLY for errors
        if event_id and event_exists(event_id):
            finalize_event(
                event_id=event_id,
                status="error",
                event_message=error_msg,
                backup_set_id=None,
                runtime="00:00:00"
            )
        return None, event_id, None

    # Test destination folder
    if not dest or not os.path.exists(dest):
        error_msg = f"Destination path does not exist: {dest}"
        logger.error(error_msg)
        # Finalize the event ONLY for errors
        if event_id and event_exists(event_id):
            finalize_event(
                event_id=event_id,
                status="error",
                event_message=error_msg,
                backup_set_id=None,
                runtime="00:00:00"
            )
        return None, event_id, None
    
    if not os.access(dest, os.W_OK):
        error_msg = f"Destination path is not writable: {dest}"
        logger.error(error_msg)
        # Finalize the event ONLY for errors
        if event_id and event_exists(event_id):
            finalize_event(
                event_id=event_id,
                status="error",
                event_message=error_msg,
                backup_set_id=None,
                runtime="00:00:00"
            )
        return None, event_id, None

    # Test S3 if sync is enabled
    if sync:
        if event_id and event_exists(event_id):
            update_event(event_id, event_message="Checking S3 bucket access")
            
        if not check_s3_accessible(config, logger):
            error_msg = "S3 bucket is not accessible or writable."
            logger.error(error_msg)
            # Finalize the event ONLY for errors
            if event_id and event_exists(event_id):
                finalize_event(
                    event_id=event_id,
                    status="error",
                    event_message=error_msg,
                    backup_set_id=None,
                    runtime="00:00:00"
                )
            return None, event_id, None

    # Path setup (for validation only - no directories created)
    machine_name = socket.gethostname()
    sanitized_job_name = "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in job_name)
    sanitized_machine_name = "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in machine_name)
    job_dst = os.path.join(dest, sanitized_machine_name, sanitized_job_name)
    
    now = timestamp()
    backup_set_id_string = now
    backup_set_dir = os.path.join(job_dst, f"backup_set_{backup_set_id_string}")

    logger.info(f"DRYRUN: Would create backup in: {backup_set_dir}")
    
    # Update event for exclude patterns
    if event_id and event_exists(event_id):
        update_event(event_id, event_message="Loading exclude patterns")

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

    # Update event for scanning files
    if event_id and event_exists(event_id):
        update_event(event_id, event_message="Scanning for files to backup (dry run)")
    
    # Get files that would be backed up
    files = get_all_files(src, exclude_patterns)
    logger.info(f"DRYRUN: Found {len(files)} files that would be archived.")

    if not files:
        logger.warning("DRYRUN: No files found to backup.")
        # Don't finalize here - let the CLI handle it
        if event_id and event_exists(event_id):
            update_event(
                event_id=event_id,
                event_message="No files found for dryrun backup",
                status="running"  # Keep it running so CLI can finalize it
            )
        return "skipped", event_id, backup_set_id_string

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

    # Create database entries for the dryrun if we don't have them already
    try:
        # Update event for database creation
        if event_id and event_exists(event_id):
            update_event(event_id, event_message="Creating database entries (dry run)")
        
        # If we don't have a backup_job_id or backup_set_id from the event,
        # we need to create them (should not happen with proper CLI event creation)
        if not backup_job_id or not backup_set_id:
            # Step 1: Create backup set in database
            config_snapshot = json.dumps(config) if config else None
            backup_set_id = get_or_create_backup_set(
                job_name=sanitized_job_name,
                set_name=backup_set_id_string,
                config_settings=config_snapshot
            )
            
            # Step 2: Create backup job in database
            backup_job_id = insert_backup_job(
                backup_set_id=backup_set_id,
                backup_type="dryrun",
                encrypted=encrypt,
                synced=sync,
                event_message="Dryrun backup started"
            )
            
            logger.info(f"DRYRUN: Created database entries - backup_set_id={backup_set_id}, job_id={backup_job_id}")
        else:
            logger.info(f"DRYRUN: Using existing database entries - backup_set_id={backup_set_id}, job_id={backup_job_id}")

        # Step 3: Create file records for database
        total_size_bytes = 0
        file_records = []
        
        if event_id and event_exists(event_id):
            update_event(event_id, event_message="Processing file information (dry run)")
            
        for file_path in files:
            try:
                stat_info = os.stat(file_path)
                rel_path = os.path.relpath(file_path, src)
                file_size = stat_info.st_size
                total_size_bytes += file_size
                
                file_records.append({
                    "tarball": f"dryrun_{backup_set_id_string}.tar.gz",  # Simulated tarball name
                    "path": rel_path,
                    "mtime": stat_info.st_mtime,
                    "size": file_size,
                    "is_new": True,
                    "is_modified": False
                })
                
            except OSError as e:
                logger.warning(f"DRYRUN: Could not stat file {file_path}: {e}")
                continue

        # Step 4: Insert file records into database
        if file_records:
            logger.info(f"DRYRUN: Inserting {len(file_records)} file records into database...")
            
            if event_id and event_exists(event_id):
                update_event(event_id, event_message=f"Adding {len(file_records)} file records to database (dry run)")
                
            insert_files(backup_job_id, file_records)
            
            # Mark the backup job as completed in database
            finalize_backup_job(
                job_id=backup_job_id,
                status="completed",
                event_message="Dryrun backup completed successfully",
                total_files=len(file_records),
                total_size_bytes=total_size_bytes
            )
            
            logger.info(f"DRYRUN: Backup job completed with {len(file_records)} files, {total_size_bytes} bytes")
        else:
            # No valid files
            finalize_backup_job(
                job_id=backup_job_id,
                status="completed",
                event_message="Dryrun completed with no valid files"
            )
            logger.info("DRYRUN: No valid files to record")

        # Log what would happen in a real backup
        logger.info(f"DRYRUN: Would create directory: {backup_set_dir}")
        logger.info(f"DRYRUN: Would create {len(file_records)} archive files")
        logger.info(f"DRYRUN: Would generate HTML manifest")
        if encrypt:
            logger.info("DRYRUN: Would encrypt archive files")
        if sync:
            logger.info("DRYRUN: Would sync to S3")

        # Important: Don't finalize the event here!
        # Just update it with progress information
        if event_id and event_exists(event_id):
            # Set a descriptive message that will be properly hyperlinked 
            # The CLI will finalize this with the backup_set_id
            update_event(
                event_id=event_id,
                event_message=f"Dryrun Manifest ({len(file_records)} files)",
                status="running"  # Keep it running so CLI can finalize it
            )

        logger.info(f"DRYRUN backup completed for {src}")
        return backup_set_dir, event_id, backup_set_id_string

    except Exception as e:
        logger.error(f"Error during dryrun backup: {e}", exc_info=True)
        
        # Try to mark job as failed if we got far enough to create it
        try:
            if 'backup_job_id' in locals():
                finalize_backup_job(
                    job_id=backup_job_id,
                    status="failed",
                    error_message=str(e),
                    event_message=f"Dryrun backup failed: {e}"
                )
        except Exception as db_e:
            logger.error(f"Failed to update database with error status: {db_e}")
        
        # Finalize the event for errors ONLY
        if event_id and event_exists(event_id):
            finalize_event(
                event_id=event_id,
                status="error",
                event_message=f"Dryrun backup failed: {e}",
                backup_set_id=backup_set_id_string if 'backup_set_id_string' in locals() else None
            )
        
        raise