"""Sync a backup set directory to AWS S3 using the AWS CLI."""

import os
import subprocess
import socket

from app.utils.event_logger import finalize_event, event_exists
from app.utils.logger import setup_logger

def sync_to_s3(backup_set_path, config, event_id=None):
    # pylint: disable=too-many-locals
    """
    Sync only the current backup set directory to AWS S3.
    :param backup_set_path: Path to the latest backup set directory.
    :param config: Parsed YAML configuration with AWS details.
    :param event_id: The event ID to update.
    """
    job_name = config.get("job_name", "unknown")
    logger = setup_logger(job_name)

    aws_config = config.get("aws", {})
    profile = aws_config.get("profile", "default")
    region = aws_config.get("region", None)
    bucket = aws_config.get("bucket")
    storage_class = aws_config.get("storage_class", "STANDARD")
    machine_name = socket.gethostname()
    prefix = machine_name

    if not bucket:
        raise ValueError("AWS S3 bucket name is not specified in the configuration.")

    sanitized_job_name = job_name.replace(" ", "_")
    backup_set_name = os.path.basename(backup_set_path.rstrip("/\\"))
    s3_path = (
        f"s3://{bucket}/{prefix}/{sanitized_job_name}/{backup_set_name}"
    )

    logger.info(
        f"Syncing backup set: {backup_set_path} to S3 bucket: {bucket} under "
        f"prefix: {prefix}/{sanitized_job_name}/{backup_set_name}"
    )

    if not os.path.isdir(backup_set_path):
        logger.error(f"Backup set directory does not exist: {backup_set_path}")
        return

    logger.info(f"Checking if bucket '{bucket}' exists...")
    cmd = [
        "aws", "s3api", "head-bucket", "--bucket", bucket, "--profile", profile
    ]
    if region:
        cmd.extend(["--region", region])
    try:
        subprocess.run(
            cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )
        logger.info(f"Bucket '{bucket}' exists.")
    except subprocess.CalledProcessError:
        logger.warning(f"Bucket '{bucket}' does not exist. Attempting to create it...")
        create_cmd = [
            "aws", "s3api", "create-bucket", "--bucket", bucket, "--profile", profile
        ]
        if region and region != "us-east-1":
            create_cmd.extend([
                "--region", region,
                "--create-bucket-configuration", f"LocationConstraint={region}"
            ])
        elif region:
            create_cmd.extend(["--region", region])
        subprocess.run(
            create_cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )
        logger.info(f"Bucket '{bucket}' created.")

    try:
        cmd = [
            "aws", "s3", "sync", backup_set_path, s3_path,
            "--profile", profile,
            "--storage-class", storage_class
        ]
        if region:
            cmd.extend(["--region", region])

        logger.info(
            f"Syncing {backup_set_path} to {s3_path} with storage class {storage_class}..."
        )
        subprocess.run(
            cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )
        logger.info(
            f"Sync to AWS S3 completed successfully for backup set: {backup_set_path}"
        )
        logger.info("Sync and cleanup completed successfully.")

    except Exception as e:
        logger.error(f"An error occurred during the sync process: {e}")
        if event_id and event_exists(event_id):
            finalize_event(
                event_id=event_id,
                status="error",
                event="Sync to S3 failed"
            )
        raise RuntimeError(f"Sync failed for job '{job_name}': {e}") from e
