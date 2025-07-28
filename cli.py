#!/venv/bin/python3
"""JABS CLI: Run backup jobs with options for encryption and cloud sync."""

import argparse
import os
import sys
from dotenv import load_dotenv
import yaml
import socket

from app.models.events import (
    create_event, update_event, finalize_event, get_event_status, event_exists
)
from app.utils.logger import setup_logger
from app.settings import GLOBAL_CONFIG_PATH, LOCK_DIR, CONFIG_DIR, ENV_PATH
from core.sync_s3 import sync_to_s3
from core.encrypt import encrypt_tarballs
from core.backup import run_backup
from core.backup.common import acquire_lock, release_lock, rotate_backups

# Set the working directory to the project root
os.chdir(os.path.dirname(os.path.abspath(__file__)))

# Create a module-level logger
cli_logger = setup_logger("cli")

# Load .env file from the path defined in settings
load_dotenv(ENV_PATH)

# Get the passphrase
PASSPHRASE = os.getenv("JABS_ENCRYPT_PASSPHRASE")

# Get AWS profile from environment
AWS_PROFILE = os.getenv("AWS_PROFILE")
if AWS_PROFILE:
    os.environ["AWS_PROFILE"] = AWS_PROFILE

try:
    def merge_dicts(global_dict, job_dict):
        """Merge two dicts, with job_dict taking precedence."""
        merged = (global_dict or {}).copy()
        merged.update(job_dict or {})
        return merged

    def run_job(config_path, backup_type, encrypt=False, sync=False):
        """Run a backup job with the given configuration."""
        lock_file = None
        try:
            # Load job configuration
            with open(config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
            
            # Load global configuration and merge with job config
            global_config = {}
            try:
                with open(GLOBAL_CONFIG_PATH, encoding='utf-8') as f:
                    global_config = yaml.safe_load(f)
            except (OSError, yaml.YAMLError) as e:
                cli_logger.warning(f"Could not load global config: {e}")
                
            # Merge nested dicts for aws and encryption
            config["aws"] = merge_dicts(global_config.get("aws"), config.get("aws"))
            config["encryption"] = merge_dicts(global_config.get("encryption"), config.get("encryption"))

            # Merge all missing flat values from global config (including destination)
            for key, value in global_config.items():
                # Skip keys that are already processed as nested dicts (aws, encryption)
                if key in ["aws", "encryption"]:
                    continue
                    
                # Apply global config values if the key is missing or None in job config
                if key not in config or config[key] is None:
                    config[key] = value

            # Determine if encryption and sync should be enabled
            # Either from command line or from config
            encrypt_effective = encrypt or config.get("encryption", {}).get("enabled", False)
            sync_effective = sync or config.get("aws", {}).get("enabled", False)

            # Get the job name from the config
            job_name = config.get("job_name", "unknown")

            # Set up the logger with the job name
            logger = setup_logger(job_name)
            logger.info(f"###### Starting {backup_type.upper()} backup ######")

            # ACQUIRE LOCK FIRST before doing anything ELSE
            os.makedirs(LOCK_DIR, exist_ok=True)
            job_name_sanitized = "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in job_name)
            lock_file_path = os.path.join(LOCK_DIR, f"{job_name_sanitized}.lock")
            
            # Try to acquire the lock, catching any lock exceptions
            try:
                lock_file = acquire_lock(lock_file_path)
            except RuntimeError as e:
                # This catches the lock acquisition error from common.py
                lock_error_msg = f"ERROR: {str(e)}"
                
                # Log using error level for file logs, but with cleaner output for terminal
                logger.error(lock_error_msg)
                
                # Exit directly from CLI
                if __name__ == "__main__":
                    print(lock_error_msg)
                    sys.exit(2)  # Exit with error code 2 for lock errors
                    
                return "locked"  # Simple return for programmatic use
    
            # Debug the config object
            logger.debug(f"DEBUG: create_event: config type: {type(config)}")
            logger.debug(f"DEBUG: create_event: config empty? {not bool(config)}")
            logger.debug(f"DEBUG: create_event: config keys: {list(config.keys()) if isinstance(config, dict) and config else 'None'}")

            # --- Initialize the event and capture the event_id ---
            event_id = create_event(
                job_name=job_name,
                event_message=f"Starting {backup_type} backup",
                backup_type=backup_type,
                encrypt=encrypt_effective,
                sync=sync_effective,
                config=config  # Pass the config here
            )
    
            # --- Run the backup operation ---
            backup_result = run_backup(
                config,
                backup_type,
                encrypt=False,  # Don't encrypt in backup modules
                sync=False,     # Don't sync in backup modules
                event_id=event_id,  # Pass our event_id to be updated, not finalized
                job_config_path=config_path,
                global_config=global_config
            )
            
            # Unpack the backup result
            # The result could be:
            # - For successful backups: (backup_set_dir, event_id, backup_set_id_str, tarball_paths)
            # - For skipped backups: ("skipped", event_id, None, None)
            # - For failed backups: (None, event_id, None, None)
            
            if isinstance(backup_result, tuple) and len(backup_result) >= 4:
                latest_backup_set, event_id, backup_set_id_str, tarball_paths = backup_result
            else:
                # Handle old-style return values for backward compatibility
                latest_backup_set, event_id, backup_set_id_str = backup_result
                tarball_paths = []

            # --- Check for skipped diff or incremental backup ---
            if backup_type in ["diff", "differential", "incremental"] and latest_backup_set == "skipped":
                logger.info("No files modified. Backup skipped.")
                finalize_event(
                    event_id=event_id,  # Always use our original event_id
                    status="skipped",
                    event_message="No files modified. Backup skipped.",
                    backup_set_id=None,
                    runtime="00:00:00"
                )
                return
                
            # --- Encryption if requested ---
            if encrypt_effective and tarball_paths:
                logger.debug("Starting encryption of backup files")
                update_event(
                    event_id=event_id,
                    event_message="Encrypting backup files",
                    status="running"
                )
                try:
                    tarball_paths = encrypt_tarballs(tarball_paths, config, logger)
                    update_event(
                        event_id=event_id,
                        event_message="Encryption completed",
                        status="running"
                    )
                except Exception as e:
                    logger.error(f"Encryption failed: {e}", exc_info=True)
                    update_event(
                        event_id=event_id,
                        event_message=f"Encryption failed: {e}",
                        status="running"
                    )
                    # Continue with the backup process, don't fail the entire job

            # --- S3 sync if requested - KEEP LOCK DURING SYNC ---
            if sync_effective and latest_backup_set:
                logger.debug("Starting sync to S3")
                update_event(
                    event_id=event_id,  # Always use our original event_id
                    event_message="Sync to S3 started",
                    status="running"
                )
                sync_result = sync_to_s3(latest_backup_set, config, event_id)  # Pass our event_id
                
                if not sync_result:
                    logger.warning("S3 sync was skipped or failed but continuing with backup process")
                    # The sync_to_s3 function will have updated the event with the reason

            # --- Rotate Backups ---
            # RESOLVE keep_sets
            keep_sets = config.get("keep_sets", None)
            if keep_sets is None and global_config is not None:
                keep_sets = global_config.get("keep_sets", None)
            if keep_sets is None:
                keep_sets = 5  # fallback default
            keep_sets = int(keep_sets)
 
            # RECREATE job_dst path
            machine_name = socket.gethostname()
            sanitized_job_name = "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in job_name)
            sanitized_machine_name = "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in machine_name)
            
            # Get destination from config
            dest = config.get("destination")
            if not dest:
                logger.error("No destination specified in config")
                return False
                
            job_dst = os.path.join(dest, sanitized_machine_name, sanitized_job_name)
            
            # call rotate_backups
            rotate_backups(job_dst, keep_sets, logger, config)

            # --- Finalize the event as successful ---
            if backup_set_id_str is not None and event_exists(event_id):
                current_status = get_event_status(event_id)
                if current_status not in ("error", "skipped", "success"):
                    logger.debug("Backup job completed successfully")
                    final_message = f"Backup Set ID: {backup_set_id_str}"
                    finalize_event(
                        event_id=event_id,  # Always use our original event_id
                        status="success",
                        event_message=final_message,
                        backup_set_id=backup_set_id_str
                    )

            logger.info("Backup operation completed successfully")
            return True  # Successful completion

        except Exception as e:
            logger = setup_logger("cli_error", log_file="cli_error.log")
            logger.error(f"Fatal error during execution: {e}", exc_info=True)
            
            # Print a simpler error message to the terminal
            error_msg = str(e)
            if __name__ == "__main__":
                print(f"ERROR: {error_msg}")
            
            return None  # Indicate failure
        finally:
            # Always release the lock file if we acquired it
            if lock_file:
                try:
                    logger.debug("Released lock file for job")
                    release_lock(lock_file)
                except Exception as e:  # pylint: disable=broad-except
                    logger.error(f"Error releasing lock file: {e}")
                    # Even if we fail to release the lock, don't raise an exception
                    # as this would obscure the original error

    if __name__ == "__main__":
        parser = argparse.ArgumentParser(description="JABS CLI")
        parser.add_argument("--job", required=True, help="Path to job config file or job name")
        parser.add_argument("--type", choices=["full", "diff", "differential", "incremental", "dry_run", "dryrun"],
                            default="incremental", help="Type of backup to perform")
        parser.add_argument("--encrypt", action="store_true", help="Encrypt the backup")
        parser.add_argument("--sync", action="store_true", help="Sync to S3 after backup")

        args = parser.parse_args()

        # If the job argument is not a path to a config file, assume it's a job name
        job_config_path = args.job
        if not job_config_path.endswith((".yaml", ".yml")) or not os.path.exists(job_config_path):
            # Try to find the config file in the jobs directory
            job_name = args.job
            job_config_path = os.path.join(CONFIG_DIR, "jobs", f"{job_name}.yaml")
            if not os.path.exists(job_config_path):
                cli_logger.error(f"Config file not found: {job_config_path}")
                print(f"ERROR: Config file not found: {job_config_path}")
                sys.exit(1)

        backup_type = args.type
        if backup_type in ["diff"]:
            backup_type = "differential"
        if backup_type in ["dry_run"]:
            backup_type = "dryrun"

        run_job(job_config_path, backup_type, args.encrypt, args.sync)
except Exception as e:
    cli_logger.error(f"Fatal error: {e}", exc_info=True)
    print(f"ERROR: {e}")
    sys.exit(1)


