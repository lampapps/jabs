#/jobs/sync_s3.py
from app.utils.event_logger import finalize_event
import os
import subprocess
from app.utils.logger import setup_logger
import socket

def sync_to_s3(backup_set_path, config, event_id=None):
    """
    Sync the entire job directory (all backup sets for this job) to AWS S3, mirroring the local destination structure.
    Only the current job's directory is synced; other jobs are not affected.
    :param backup_set_path: Path to the latest backup set directory (used to determine job directory and for event logging).
    :param config: Parsed YAML configuration with AWS details.
    :param event_id: The event ID to update.
    """
    job_name = config.get("job_name", "unknown")
    logger = setup_logger(job_name)

    # Extract AWS and S3 configuration from the job config
    aws_config = config.get("aws", {})
    profile = aws_config.get("profile", "default")
    region = aws_config.get("region", None)
    bucket = aws_config.get("bucket")
    storage_class = aws_config.get("storage_class", "STANDARD")
    machine_name = socket.gethostname()
    prefix = machine_name

    if not bucket:
        raise ValueError("AWS S3 bucket name is not specified in the configuration.")

    # Determine the job directory (parent of the backup set)
    sanitized_job_name = job_name.replace(" ", "_")
    job_dir = os.path.dirname(backup_set_path.rstrip("/"))
    s3_path = f"s3://{bucket}/{prefix}/{sanitized_job_name}"

    logger.info(f"Syncing entire job directory: {job_dir} to S3 bucket: {bucket} under prefix: {prefix}/{sanitized_job_name}")

    # Check if the local job directory exists before attempting sync
    if not os.path.isdir(job_dir):
        logger.error(f"Job directory does not exist: {job_dir}")
        return

    # Ensure the S3 bucket exists before syncing
    logger.info(f"Checking if bucket '{bucket}' exists...")
    cmd = ["aws", "s3api", "head-bucket", "--bucket", bucket, "--profile", profile]
    if region:
        cmd.extend(["--region", region])
    subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    logger.info(f"Bucket '{bucket}' exists.")

    try:
        # Build the AWS CLI command for syncing the entire job directory
        cmd = [
            "aws", "s3", "sync", job_dir, s3_path,
            "--profile", profile, "--delete",
            "--storage-class", storage_class
        ]
        if region:
            cmd.extend(["--region", region])

        logger.info(f"Syncing {job_dir} to {s3_path} with storage class {storage_class}...")
        subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        logger.info(f"Sync to AWS S3 completed successfully for job: {job_name}")

        logger.info("Sync and cleanup completed successfully.")

    except Exception as e:
        # Log any errors and update the event status if event_id is provided
        logger.error(f"An error occurred during the sync process: {e}")
        if event_id:
            finalize_event(
                event_id=event_id,
                status="error",
                event="Sync to S3 failed"
            )
        raise RuntimeError(f"Sync failed for job '{job_name}': {e}")
