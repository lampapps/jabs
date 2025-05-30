import os
import tarfile
import json
import subprocess
import time
from app.utils.logger import setup_logger
from app.utils.restore_status import set_restore_status
from app.utils.event_logger import initialize_event, update_event, finalize_event, event_exists

def get_passphrase():
    """
    Retrieve the GPG passphrase from the environment variable.
    Returns None if not set.
    """
    return os.getenv("JABS_ENCRYPT_PASSPHRASE")

def get_manifest(job_name, backup_set_id, base_dir, logger):
    """
    Load the manifest JSON for a given job and backup set.
    :param job_name: Name of the backup job.
    :param backup_set_id: Timestamp string identifying the backup set.
    :param base_dir: Base directory of the project.
    :param logger: Logger instance for logging.
    :return: Manifest dictionary.
    """
    # Sanitize job name for filesystem safety
    sanitized_job = "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in job_name)
    manifest_path = os.path.join(base_dir, "data", "manifests", sanitized_job, f"{backup_set_id}.json")
    logger.info(f"Loading manifest: {manifest_path}")
    with open(manifest_path, "r") as f:
        manifest = json.load(f)
    logger.info(f"Loaded manifest with {len(manifest.get('files', []))} files.")
    return manifest

def extract_file_from_tarball(tarball_path, member_path, target_path, logger):
    """
    Extract a single file from a tarball (optionally GPG-encrypted) to the target path.
    :param tarball_path: Path to the (possibly encrypted) tarball.
    :param member_path: Path of the file inside the tarball to extract.
    :param target_path: Destination path for the extracted file.
    :param logger: Logger instance for logging.
    :return: (success: bool, error_message: str or None)
    """
    logger.info(f"Extracting '{member_path}' from '{tarball_path}' to '{target_path}'")
    try:
        if tarball_path.endswith('.gpg'):
            # Handle GPG-encrypted tarballs
            passphrase = get_passphrase()
            if not passphrase:
                logger.error("GPG passphrase not set in environment or .env file.")
                return False, "GPG passphrase not set. Cannot decrypt archive."
            # Build GPG command to decrypt to stdout
            gpg_cmd = [
                "gpg", "--batch", "--yes", "--passphrase", passphrase,
                "-d", tarball_path
            ]
            # Start GPG process, pipe output to tarfile
            proc = subprocess.Popen(gpg_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            try:
                # Open the decrypted tar stream
                with tarfile.open(fileobj=proc.stdout, mode='r|*') as tar:
                    for member in tar:
                        if member.name == member_path:
                            # Ensure target directory exists
                            os.makedirs(os.path.dirname(target_path), exist_ok=True)
                            # Extract the file
                            with open(target_path, "wb") as out_f, tar.extractfile(member) as in_f:
                                out_f.write(in_f.read())
                            logger.info(f"Successfully restored '{member_path}' to '{target_path}'")
                            return True, None
                    logger.error(f"{member_path} not found in {tarball_path}")
                    return False, f"{member_path} not found in {tarball_path}"
            except tarfile.ReadError as e:
                # If tar extraction fails, read GPG stderr for error details
                gpg_err = proc.stderr.read().decode()
                logger.error(f"GPG decryption or tar extraction failed: {gpg_err}")
                user_msg = (
                    f"Cannot restore '{member_path}' from '{os.path.basename(tarball_path)}':\n"
                    "GPG decryption failed or output is not a valid tar archive.\n"
                    f"GPG error: {gpg_err.strip()}"
                )
                return False, user_msg
            finally:
                # Clean up process pipes
                proc.stdout.close()
                proc.stderr.close()
                proc.wait()
        else:
            # Handle unencrypted tarballs
            with tarfile.open(tarball_path, 'r:*') as tar:
                member = tar.getmember(member_path)
                os.makedirs(os.path.dirname(target_path), exist_ok=True)
                with open(target_path, "wb") as out_f, tar.extractfile(member) as in_f:
                    out_f.write(in_f.read())
            logger.info(f"Successfully restored '{member_path}' to '{target_path}'")
            return True, None
    except KeyError:
        logger.error(f"{member_path} not found in {tarball_path}")
        return False, f"{member_path} not found in {tarball_path}"
    except Exception as e:
        logger.error(f"Error extracting '{member_path}' from '{tarball_path}': {e}")
        return False, str(e)

def restore_files(job_name, backup_set_id, files, dest=None, base_dir=None, event_id=None, restore_option="selected", logger=None):
    """
    Restore a list of files from a backup set.
    :param job_name: Name of the backup job.
    :param backup_set_id: Timestamp string identifying the backup set.
    :param files: List of file paths (relative to source) to restore.
    :param dest: Optional destination directory for restore.
    :param base_dir: Base directory of the project.
    :param event_id: Optional event ID for logging.
    :param restore_option: "full" or "selected"
    :param logger: Logger instance for logging.
    :return: Dict with lists of restored files and errors.
    """
    if logger is None:
        logger = setup_logger(job_name, log_file="restore.log")
    logger.info(f"PASSPHRASE loaded: {'YES' if get_passphrase() else 'NO'}")
    logger.info(f"Starting restore_files for job '{job_name}', backup_set_id '{backup_set_id}'")
    set_restore_status(job_name, backup_set_id, running=True)
    manifest = get_manifest(job_name, backup_set_id, base_dir, logger)
    restored = []
    errors = []
    start_time = time.time()

    restore_path = dest if dest else manifest["config"]["source"]
    event_type = "restore"
    # PATCH: Use descriptive event for full or selected restore
    if restore_option == "full":
        event_desc = f"Restoring all files to: {restore_path}"
    else:
        event_desc = f"Restoring selected files to: {restore_path}"

    # Initialize event if not provided
    if not event_id:
        event_id = initialize_event(
            job_name=job_name,
            event=event_desc,
            backup_type=event_type,
            encrypt=False,
            sync=False
        )
        # PATCH: Show option in event
        update_event(event_id, event=f"{event_desc}", status="running")

    try:
        for f in manifest.get("files", []):
            if f["path"] in files:
                tarball_path = f["tarball_path"]
                member_path = f["path"]
                # Determine restore target location
                if dest:
                    target = os.path.join(dest, member_path)
                else:
                    source_base = manifest["config"]["source"]
                    target = os.path.join(source_base, member_path)
                # Attempt extraction
                ok, err = extract_file_from_tarball(tarball_path, member_path, target, logger)
                if ok:
                    restored.append(target)
                else:
                    errors.append({"file": member_path, "error": err})
                    logger.error(f"Restore failed for '{member_path}': {err}")
                    # Exit early on first error
                    break
        logger.info(f"Restore complete. Restored: {len(restored)}, Errors: {len(errors)}")
        if errors:
            logger.error(f"Restore errors: {errors}")
            status = "error"
            if restore_option == "full":
                event_msg = f"Full restore failed to {restore_path}: {errors[0]['error'] if errors else 'Unknown error'}"
            else:
                event_msg = f"Partial restore failed to {restore_path} with selected files: {errors[0]['error'] if errors else 'Unknown error'}"
        else:
            status = "success"
            if restore_option == "full":
                event_msg = f"Full restore complete to {restore_path}"
            else:
                event_msg = f"Partial restore complete to {restore_path} with selected files"
        runtime = int(time.time() - start_time)
        runtime_str = f"{runtime//3600:02}:{(runtime%3600)//60:02}:{runtime%60:02}"
        if event_exists(event_id):
            finalize_event(
                event_id=event_id,
                status=status,
                event=event_msg,
                runtime=runtime_str
            )
    finally:
        set_restore_status(job_name, backup_set_id, running=False)
    return {"restored": restored, "errors": errors}

def restore_full(job_name, backup_set_id, dest=None, base_dir=None, event_id=None):
    """
    Restore all files from a backup set.
    :param job_name: Name of the backup job.
    :param backup_set_id: Timestamp string identifying the backup set.
    :param dest: Optional destination directory for restore.
    :param base_dir: Base directory of the project.
    :param event_id: Optional event ID for logging.
    :return: Dict with lists of restored files and errors.
    """
    logger = setup_logger(job_name, log_file="restore.log")
    logger.info(f"Starting full restore for job '{job_name}', backup_set_id '{backup_set_id}'")
    set_restore_status(job_name, backup_set_id, running=True)
    manifest = get_manifest(job_name, backup_set_id, base_dir, logger)
    files = [f["path"] for f in manifest.get("files", [])]
    result = restore_files(job_name, backup_set_id, files, dest=dest, base_dir=base_dir, event_id=event_id, restore_option="full", logger=logger)
    set_restore_status(job_name, backup_set_id, running=False)
    return result