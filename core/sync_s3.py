#/jobs/sync_s3.py
from app.utils.event_logger import finalize_event
import os
import subprocess
from app.utils.logger import setup_logger
import socket

def sync_to_s3(backup_set_path, config, event_id=None):
    """
    Sync the entire job directory (all backup sets) to AWS S3, mirroring the local destination structure.
    :param backup_set_path: Path to the latest backup set directory (not used for sync root, but for event logging).
    :param config: Parsed YAML configuration with AWS details.
    :param event_id: The event ID to update.
    """
    import shutil  # Used for potential local file operations

    # Retrieve job name and set up logger
    job_name = config.get("job_name", "unknown")
    logger = setup_logger(job_name)

    # Extract AWS and S3 configuration from the job config
    aws_config = config.get("aws", {})
    profile = aws_config.get("profile", "default")
    region = aws_config.get("region", None)
    bucket = aws_config.get("bucket")
    machine_name = socket.gethostname()
    prefix = machine_name  # Used as S3 prefix

    if not bucket:
        raise ValueError("AWS S3 bucket name is not specified in the configuration.")

    # Build the local job directory path (where all backup sets are stored)
    destination_base = config["destination"]
    sanitized_job_name = job_name.replace(" ", "_")
    job_dir = os.path.join(destination_base, machine_name, sanitized_job_name)

    # Log all relevant path components for debugging
    logger.info(f"destination_base: {destination_base}")
    logger.info(f"machine_name: {machine_name}")
    logger.info(f"job_name: {job_name}")
    logger.info(f"sanitized_job_name: {sanitized_job_name}")
    logger.info(f"job_dir: {job_dir}")

    # Check if the local job directory exists before attempting sync
    if not os.path.isdir(job_dir):
        logger.error(f"Job directory does not exist: {job_dir}")
        return

    logger.info(f"Syncing entire job directory: {job_dir} to S3 bucket: {bucket} under prefix: {prefix}/{sanitized_job_name}")

    try:
        # Ensure the S3 bucket exists before syncing
        logger.info(f"Checking if bucket '{bucket}' exists...")
        cmd = ["aws", "s3api", "head-bucket", "--bucket", bucket, "--profile", profile]
        if region:
            cmd.extend(["--region", region])
        subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        logger.info(f"Bucket '{bucket}' exists.")
    except subprocess.CalledProcessError:
        # If the bucket does not exist, attempt to create it
        logger.info(f"Bucket '{bucket}' does not exist. Creating it...")
        create_cmd = ["aws", "s3api", "create-bucket", "--bucket", bucket, "--profile", profile]
        if region:
            create_cmd.extend(["--region", region])
            if region != "us-east-1":
                # For regions other than us-east-1, specify location constraint
                create_cmd.extend(["--create-bucket-configuration", f"LocationConstraint={region}"])
        try:
            subprocess.run(create_cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            logger.info(f"Bucket '{bucket}' created successfully.")
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to create bucket '{bucket}': {e.stderr}")
            raise RuntimeError(f"Failed to create bucket '{bucket}': {e.stderr}")

    try:
        # Construct the S3 destination path for the job directory
        s3_path = f"s3://{bucket}/{prefix}/{sanitized_job_name}".rstrip("/")

        # Build the AWS CLI command for syncing the entire job directory
        # --delete ensures S3 matches local (removes remote files not present locally)
        cmd = ["aws", "s3", "sync", job_dir, s3_path, "--profile", profile, "--delete"]
        if region:
            cmd.extend(["--region", region])

        logger.info(f"Syncing {job_dir} to {s3_path}...")
        # Run the sync command and capture output for logging
        subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        logger.info(f"Sync to AWS S3 completed successfully for job directory: {job_dir}")

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
