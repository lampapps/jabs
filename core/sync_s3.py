#/jobs/sync_s3.py
from app.utils.event_logger import finalize_event
import os
import subprocess
import json
from app.utils.logger import setup_logger
import socket

def sync_to_s3(backup_set_path, config, event_id=None):
    """
    Sync the entire job directory (all backup sets) to AWS S3, mirroring the local destination structure.
    :param backup_set_path: Path to the latest backup set directory (not used for sync root, but for event logging).
    :param config: Parsed YAML configuration with AWS details.
    :param event_id: The event ID to update.
    """
    import shutil

    job_name = config.get("job_name", "unknown")
    logger = setup_logger(job_name)

    aws_config = config.get("aws", {})
    profile = aws_config.get("profile", "default")
    region = aws_config.get("region", None)
    bucket = aws_config.get("bucket")
    machine_name = socket.gethostname()
    prefix = machine_name

    if not bucket:
        raise ValueError("AWS S3 bucket name is not specified in the configuration.")

    # The local job directory (parent of all backup sets for this job)
    destination_base = config["destination"]
    sanitized_job_name = job_name.replace(" ", "_")
    job_dir = os.path.join(destination_base, machine_name, sanitized_job_name)

    logger.info(f"destination_base: {destination_base}")
    logger.info(f"machine_name: {machine_name}")
    logger.info(f"job_name: {job_name}")
    logger.info(f"sanitized_job_name: {sanitized_job_name}")
    logger.info(f"job_dir: {job_dir}")

    if not os.path.isdir(job_dir):
        logger.error(f"Job directory does not exist: {job_dir}")
        return

    logger.info(f"Syncing entire job directory: {job_dir} to S3 bucket: {bucket} under prefix: {prefix}/{job_name}")

    try:
        # Ensure the bucket exists (same as before)
        logger.info(f"Checking if bucket '{bucket}' exists...")
        cmd = ["aws", "s3api", "head-bucket", "--bucket", bucket, "--profile", profile]
        if region:
            cmd.extend(["--region", region])
        subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        logger.info(f"Bucket '{bucket}' exists.")
    except subprocess.CalledProcessError:
        logger.info(f"Bucket '{bucket}' does not exist. Creating it...")
        create_cmd = ["aws", "s3api", "create-bucket", "--bucket", bucket, "--profile", profile]
        if region:
            create_cmd.extend(["--region", region])
            if region != "us-east-1":
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
        cmd = ["aws", "s3", "sync", job_dir, s3_path, "--profile", profile, "--delete"]
        if region:
            cmd.extend(["--region", region])

        logger.info(f"Syncing {job_dir} to {s3_path}...")
        subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        logger.info(f"Sync to AWS S3 completed successfully for job directory: {job_dir}")

        # After successful sync, mark the latest backup set as synced
        if backup_set_path:
            sync_backup_set_to_s3(backup_set_path)

        logger.info("Sync and cleanup completed successfully.")

    except Exception as e:
        logger.error(f"An error occurred during the sync process: {e}")
        if event_id:
            finalize_event(
                event_id=event_id,
                status="error",
                event="Sync to S3 failed"
            )
        raise RuntimeError(f"Sync failed for job '{job_name}': {e}")

def sync_backup_set_to_s3(backup_set_path):
    """
    Create a .synced marker file after successful S3 sync.
    :param backup_set_path: Path to the backup set directory.
    """
    try:
        with open(os.path.join(backup_set_path, ".synced"), "w") as f:
            f.write("synced")
    except Exception as e:
        raise RuntimeError(f"Failed to create .synced marker file: {e}")
