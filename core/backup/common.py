"""Common backup utilities for locking and backup rotation."""

import os
import fcntl
import shutil
import glob
import socket
import subprocess

from app.models.events import remove_events_by_backup_set_id

def acquire_lock(lock_path):
    """Acquire an exclusive lock on the given file path."""
    lock_file = open(lock_path, "w", encoding="utf-8")
    try:
        fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return lock_file
    except Exception as exc:
        lock_file.close()
        raise RuntimeError(f"Could not acquire lock: {lock_path}") from exc

def release_lock(lock_file):
    """Release the lock and close the file."""
    fcntl.flock(lock_file, fcntl.LOCK_UN)
    lock_file.close()


def rotate_backups(job_dst, keep_sets, logger, config=None):
    """
    Rotate backup sets to keep only the latest 'keep_sets' sets and clean up corresponding JSON files, events, and S3 folders.
    """
    backup_sets = sorted(glob.glob(os.path.join(job_dst, "backup_set_*")), reverse=True)
    if len(backup_sets) > keep_sets:
        to_delete = backup_sets[keep_sets:]
        for old_set in to_delete:
            try:
                shutil.rmtree(old_set)
                logger.info(f"Deleted old backup set: {old_set}")
                backup_set_id = os.path.basename(old_set).replace("backup_set_", "")
                job_name = os.path.basename(job_dst)
                manifest_dir = os.path.join("data", "manifests", job_name)
                manifest_file = os.path.join(manifest_dir, f"{backup_set_id}.json")
                if os.path.exists(manifest_file):
                    os.remove(manifest_file)
                    logger.info(f"Deleted JSON file: {manifest_file}")
                else:
                    logger.warning(f"JSON file not found for backup set: {backup_set_id}")
                remove_events_by_backup_set_id(backup_set_id)
                if config:
                    aws_config = config.get("aws", {})
                    bucket = aws_config.get("bucket")
                    profile = aws_config.get("profile", "default")
                    region = aws_config.get("region")
                    machine_name = socket.gethostname()
                    sanitized_job_name = "".join(
                        c if c.isalnum() or c in ("-", "_") else "_" for c in job_name
                    )
                    prefix = machine_name
                    s3_path = f"s3://{bucket}/{prefix}/{sanitized_job_name}/{os.path.basename(old_set)}"
                    cmd = [
                        "aws", "s3", "rm", s3_path, "--recursive", "--profile", profile
                    ]
                    if region:
                        cmd.extend(["--region", region])
                    logger.info(f"Deleting S3 backup set: {s3_path}")
                    subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                    logger.info(f"Deleted S3 backup set: {s3_path}")
            except (OSError, shutil.Error, subprocess.SubprocessError) as e:
                logger.error(f"Error deleting backup set {old_set} or its manifest/S3 folder: {e}")