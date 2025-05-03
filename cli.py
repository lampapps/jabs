#!/venv/bin/python3
# /cli.py
import argparse
import logging
import yaml
import os
from app.utils.event_logger import initialize_event, update_event, finalize_event, get_event_status
from datetime import datetime, timedelta

# Set the working directory to the project root
os.chdir(os.path.dirname(os.path.abspath(__file__)))

try:
    from jobs.backup import run_backup
    # Removed sync_s3 import from here, it's imported conditionally later

    def setup_logger(job_name):
        """Set up a logger with the job name included in every message."""
        log_file = "logs/backup.log"  # Default log file
        logger = logging.getLogger("cli")
        # Prevent duplicate handlers if called multiple times
        if not logger.handlers:
            logger.setLevel(logging.INFO)

            # Create handlers
            file_handler = logging.FileHandler(log_file)
            stream_handler = logging.StreamHandler()

            # Create formatter and add it to the handlers
            formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
            file_handler.setFormatter(formatter)
            stream_handler.setFormatter(formatter)

            # Add handlers to the logger
            logger.addHandler(file_handler)
            logger.addHandler(stream_handler)

        # Use a LoggerAdapter to inject the job name
        return logging.LoggerAdapter(logger, {"job_name": job_name})

    def run_job(config_path, backup_type, encrypt=False, sync=False):
        """
        Run a backup job programmatically.
        :param config_path: Path to the YAML configuration file.
        :param backup_type: Type of backup ("full" or "diff").
        :param encrypt: Whether to encrypt the backup.
        :param sync: Whether to sync the backup to the cloud.
        """
        # Load the YAML configuration
        with open(config_path) as f:
            config = yaml.safe_load(f)

        # If encrypt is not set by CLI, check the config file
        if not encrypt:
            encrypt = config.get("encryption", {}).get("enabled", False)

        # Get the job name from the config
        job_name = config.get("job_name", "unknown")

        # Set up the logger with the job name
        logger = setup_logger(job_name)

        # Initialize the event and capture the event_id
        event_id = initialize_event(
            job_name=job_name,
            event="Starting backup",
            backup_type=backup_type,
            encrypt=encrypt,
            sync=sync
        )

        try:
            # Run the backup - unpack the new third return value
            logger.info(f"Starting {backup_type.upper()} backup")
            latest_backup_set, returned_event_id, backup_set_id_str = run_backup(
                config_path,
                backup_type,
                encrypt=encrypt,
                sync=sync, # Pass sync flag if run_backup needs it internally
                event_id=event_id
            )

            # Handle the case where no backup set was created (e.g., diff with no changes)
            # backup_set_id_str will be None in this case based on run_backup logic
            if not latest_backup_set and backup_set_id_str is None:
                status = get_event_status(returned_event_id)
                if status != "error":
                    logger.info("No files modified or backup skipped. Finalizing event.")
                    finalize_event(
                        event_id=returned_event_id,
                        status="skipped",
                        event="No files modified. Backup skipped.",
                        backup_set_id=None
                    )
                return

            # If we reach here, backup was attempted and likely succeeded in creating/updating a set
            # backup_set_id_str now holds the correct ID string

            # Sync to S3 if requested
            if sync and latest_backup_set: # Ensure there's a path to sync
                logger.info("Starting sync to S3")
                update_event(
                    event_id=returned_event_id,
                    event="Sync to S3 started",
                    status="running"
                )
                # Import sync_s3 here to avoid circular dependency issues if sync_s3 imports cli elements
                from jobs.sync_s3 import sync_to_s3
                # Pass config and event_id to sync_s3.py for potential error finalization within sync
                sync_to_s3(latest_backup_set, config, returned_event_id)

            # Log final success message
            logger.info("Completed backup successfully")

            # Finalize the event with success status
            # Use the backup_set_id_str returned by run_backup
            final_event = f"Backup Set ID: {backup_set_id_str}"
            finalize_event(
                event_id=returned_event_id,
                status="success",
                event=final_event,
                backup_set_id=backup_set_id_str # Pass the correct ID here
                # Remove url parameter
                # Runtime will be calculated by finalize_event
            )

        except Exception as e:
            # Log final failure message
            logger.error(f"Backup failed: {e}", exc_info=True)

            # Finalize the event with error status
            # Use the original event_id as returned_event_id might not be set if error happened early
            finalize_event(
                event_id=event_id,
                status="error",
                event=f"Backup failed: {e}",
                backup_set_id=None # Pass None for backup_set_id on failure
                # Runtime will be calculated by finalize_event
            )
            raise # Re-raise the exception so the calling process knows it failed

    def main():
        parser = argparse.ArgumentParser(description="Run a backup job.")
        parser.add_argument("config", help="Path to the YAML config file")

        group = parser.add_mutually_exclusive_group(required=True)
        group.add_argument("-f", "--full", action="store_true", help="Run full backup")
        group.add_argument("-d", "--diff", action="store_true", help="Run differential backup")

        parser.add_argument("--encrypt", action="store_true", help="Encrypt backup archives (tarballs) after creation")
        parser.add_argument("--sync", action="store_true", help="Sync to cloud (e.g., AWS S3)")

        args = parser.parse_args()

        # Determine backup type
        backup_type = "full" if args.full else "diff"

        # Run the job
        run_job(args.config, backup_type, encrypt=args.encrypt, sync=args.sync)

    if __name__ == "__main__":
        main()

except Exception as e:
    # Log errors that occur during imports or initialization
    # Ensure basic logging is configured even if setup_logger fails
    logging.basicConfig(level=logging.ERROR, format="%(asctime)s - %(levelname)s - %(message)s", filename="logs/cli_error.log")
    logging.error(f"Fatal error during initialization or execution: {e}", exc_info=True)
    print(f"Fatal error during initialization or execution: {e}. Check logs/backup.log or logs/cli_error.log for details.")

