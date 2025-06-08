#!/venv/bin/python3
# /cli.py
"""JABS CLI: Run backup jobs with options for encryption and cloud sync."""

import argparse
import logging
import os
import yaml
from dotenv import load_dotenv
from app.utils.event_logger import (
    initialize_event, update_event, finalize_event, get_event_status, event_exists
)
from app.utils.logger import setup_logger
from app.settings import GLOBAL_CONFIG_PATH
from core.sync_s3 import sync_to_s3

# Set the working directory to the project root
os.chdir(os.path.dirname(os.path.abspath(__file__)))

# Load .env file (by default, looks in current directory)
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

# Get the passphrase
PASSPHRASE = os.getenv("JABS_ENCRYPT_PASSPHRASE")

try:
    from core.backup import run_backup

    def merge_dicts(global_dict, job_dict):
        """Merge two dicts, with job_dict taking precedence."""
        merged = (global_dict or {}).copy()
        merged.update(job_dict or {})
        return merged

    def run_job(config_path, backup_type, encrypt=False, sync=False):
        """
        Run a backup job programmatically.
        :param config_path: Path to the YAML configuration file.
        :param backup_type: Type of backup ("full" or "diff").
        :param encrypt: Whether to encrypt the backup.
        :param sync: Whether to sync the backup to the cloud.
        """
        # Load the YAML configuration
        with open(config_path, encoding="utf-8") as f:
            config = yaml.safe_load(f)

        # --- Merge global config defaults ---
        with open(GLOBAL_CONFIG_PATH, encoding="utf-8") as f:
            global_config = yaml.safe_load(f)

        # Merge nested dicts for aws and encryption
        config["aws"] = merge_dicts(global_config.get("aws"), config.get("aws"))
        config["encryption"] = merge_dicts(global_config.get("encryption"), config.get("encryption"))

        # Fallback assignment for flat values
        if "destination" not in config or not config.get("destination"):
            config["destination"] = global_config.get("destination")

        # Determine effective encrypt value: CLI overrides config
        encrypt_effective = encrypt or config.get("encryption", {}).get("enabled", False)
        sync_effective = sync or config.get("aws", {}).get("enabled", False)

        # Get the job name from the config
        job_name = config.get("job_name", "unknown")

        # Set up the logger with the job name
        logger = setup_logger(job_name)

        # Initialize the event and capture the event_id
        event_id = initialize_event(
            job_name=job_name,
            event="Backup job started",
            backup_type=backup_type,
            encrypt=encrypt_effective,
            sync=sync_effective
        )

        try:
            logger.info("Starting %s backup", backup_type.upper())
            latest_backup_set, returned_event_id, backup_set_id_str = run_backup(
                config,
                backup_type,
                encrypt=encrypt_effective,
                sync=sync_effective,
                event_id=event_id,
                job_config_path=config_path
            )

            # Check for skipped diff backup
            if backup_type in ["diff", "differential"] and latest_backup_set is None and backup_set_id_str is None:
                logger.info("No files modified. Differential backup skipped.")
                finalize_event(
                    event_id=returned_event_id,
                    status="skipped",
                    event="No files modified. Backup skipped.",
                    backup_set_id=None,
                    runtime="00:00:00"
                )
                return

            if sync_effective and latest_backup_set:
                logger.info("Starting sync to S3")
                update_event(
                    event_id=returned_event_id,
                    event="Sync to S3 started",
                    status="running"
                )
                sync_to_s3(latest_backup_set, config, returned_event_id)

            logger.info("Completed backup successfully")
            final_event = f"Backup Set ID: {backup_set_id_str}"
            finalize_event(
                event_id=returned_event_id,
                status="success",
                event=final_event,
                backup_set_id=backup_set_id_str
            )

        except (OSError, yaml.YAMLError, ValueError) as e:
            logger.error("Backup failed: %s", e, exc_info=True)
            # Only finalize if event still exists
            if event_exists(event_id):
                finalize_event(
                    event_id=event_id,
                    status="error",
                    event=f"Backup failed: {e}",
                    backup_set_id=None
                )
            raise

    if __name__ == "__main__":
        try:
            parser = argparse.ArgumentParser(description="Run a backup job.")
            parser.add_argument("--config", required=True, help="Path to the YAML configuration file")
            parser.add_argument("--full", action="store_true", help="Perform a full backup")
            parser.add_argument("--diff", action="store_true", help="Perform a differential backup")
            parser.add_argument("--encrypt", action="store_true", help="Encrypt backup archives (tarballs) after creation")
            parser.add_argument("--sync", action="store_true", help="Sync to cloud (e.g., AWS S3)")
            args = parser.parse_args()

            backup_type = "full" if args.full else "diff"
            run_job(args.config, backup_type, encrypt=args.encrypt, sync=args.sync)
        except (OSError, yaml.YAMLError, ValueError) as e:
            logging.basicConfig(
                level=logging.ERROR,
                format="%(asctime)s - %(levelname)s - %(message)s",
                filename="logs/cli_error.log"
            )
            logging.error("Fatal error during initialization or execution: %s", e, exc_info=True)
            print(
                f"Fatal error during initialization or execution: {e}. "
                "Check logs/backup.log or logs/cli_error.log for details."
            )

except (OSError, yaml.YAMLError, ValueError) as e:
    # Log errors that occur during imports or initialization
    logging.basicConfig(
        level=logging.ERROR,
        format="%(asctime)s - %(levelname)s - %(message)s",
        filename="logs/cli_error.log"
    )
    logging.error("Fatal error during initialization or execution: %s", e, exc_info=True)
    print(
        f"Fatal error during initialization or execution: {e}. "
        "Check logs/backup.log or logs/cli_error.log for details."
    )

