#/jobs/sync_s3.py
from app.utils.event_logger import finalize_event
import os
import subprocess
import json
from app.utils.logger import setup_logger
import socket

def sync_to_s3(backup_set_path, config, event_id=None):
    """
    Sync the latest backup set to AWS S3 and remove older backup sets.
    :param backup_set_path: Path to the latest backup set directory.
    :param config: Parsed YAML configuration with AWS details.
    :param event_id: The event ID to update.
    """
    # Get the job name from the config
    job_name = config.get("job_name", "unknown")

    # Set up the logger with the job name
    logger = setup_logger(job_name)

    # Handle the case where backup_set_path is None
    if not backup_set_path:
        logger.info("No backup set to sync. Exiting sync process.")
        return

    aws_config = config.get("aws", {})
    profile = aws_config.get("profile", "default")
    region = aws_config.get("region", None)
    bucket = aws_config.get("bucket")
    # Always use the machine name as the prefix
    machine_name = socket.gethostname()
    prefix = machine_name 

    if not bucket:
        raise ValueError("AWS S3 bucket name is not specified in the configuration.")

    # Log the start of the sync process
    logger.info(f"Backup set path: {backup_set_path}")
    logger.info(f"Bucket: {bucket}")
    logger.info(f"Prefix: {prefix}")

    try:
        # Ensure the bucket exists
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
            # Add region-specific flag for non-default regions
            if region != "us-east-1":
                create_cmd.extend(["--create-bucket-configuration", f"LocationConstraint={region}"])
        try:
            subprocess.run(create_cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            logger.info(f"Bucket '{bucket}' created successfully.")
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to create bucket '{bucket}': {e.stderr}")
            raise RuntimeError(f"Failed to create bucket '{bucket}': {e.stderr}")

    try:
        # Construct the S3 destination path
        destination_base = config["destination"]
        relative_path = os.path.relpath(backup_set_path, destination_base)  # Get relative path from destination
        # Remove the first part (machine_name) from relative_path, since prefix is now machine_name
        remainder_path = "/".join(relative_path.split("/")[1:])  # Remove machine_name
        s3_path = f"s3://{bucket}/{prefix}/{remainder_path}".rstrip("/")  # Use prefix explicitly

        # Build the AWS CLI command for syncing
        cmd = ["aws", "s3", "sync", backup_set_path, s3_path, "--profile", profile]
        if region:
            cmd.extend(["--region", region])

        # Execute the sync command
        logger.info(f"Syncing {backup_set_path} to {s3_path}...")
        subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        logger.info(f"Sync to AWS S3 completed successfully for backup set: {backup_set_path}")

        # Remove older backup sets from S3 within the machine_name directory, but keep latest for each job
        logger.info(f"Removing older backup sets from S3 under prefix '{prefix}' (keeping latest for each job)...")
        list_cmd = [
            "aws", "s3api", "list-objects-v2",
            "--bucket", bucket,
            "--prefix", f"{prefix}/",
            "--profile", profile
        ]
        if region:
            list_cmd.extend(["--region", region])
        result = subprocess.run(list_cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        objects = result.stdout

        # Parse the list of objects
        objects_data = json.loads(objects)
        job_latest = {}  # {job_name: latest_backup_set_id}
        job_backup_sets = {}  # {job_name: set([backup_set_id, ...])}
        if "Contents" in objects_data:
            for obj in objects_data["Contents"]:
                key = obj["Key"]
                # Example key: jim-imac/PI_Test/backup_set_20250506_100504/manifest_20250506_100504.html
                parts = key.split("/")
                if len(parts) >= 3 and parts[2].startswith("backup_set_"):
                    job_name = parts[1]
                    backup_set_id = parts[2][len("backup_set_"):]
                    if job_name not in job_backup_sets:
                        job_backup_sets[job_name] = set()
                    job_backup_sets[job_name].add(backup_set_id)
                    # Track latest backup_set_id for each job
                    if (job_name not in job_latest) or (backup_set_id > job_latest[job_name]):
                        job_latest[job_name] = backup_set_id

            # Now, delete all backup sets except the latest for the current job only
            for obj in objects_data["Contents"]:
                key = obj["Key"]
                parts = key.split("/")
                if len(parts) >= 3 and parts[2].startswith("backup_set_"):
                    job_name_in_key = parts[1]
                    backup_set_id = parts[2][len("backup_set_"):]
                    # Only delete if this is the current job and not the latest backup set
                    if job_name_in_key == job_name and backup_set_id != job_latest.get(job_name):
                        delete_cmd = ["aws", "s3", "rm", f"s3://{bucket}/{key}", "--profile", profile]
                        if region:
                            delete_cmd.extend(["--region", region])
                        subprocess.run(delete_cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                        logger.info(f"Deleted old backup set: {key}")
        else:
            logger.info("No older backup sets found to delete.")

        logger.info("Sync and cleanup completed successfully.")

    except Exception as e:
        logger.error(f"An error occurred during the sync or cleanup process: {e}")
        if event_id:
            finalize_event(
                event_id=event_id,
                status="error",
                event="Sync to S3 failed"
            )
        raise RuntimeError(f"Sync and cleanup failed for job '{job_name}': {e}")
