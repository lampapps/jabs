import os
import tarfile
import json
import subprocess
from app.utils.logger import setup_logger
from app.utils.restore_status import set_restore_status

def get_passphrase():
    return os.getenv("JABS_ENCRYPT_PASSPHRASE")

def get_manifest(job_name, backup_set_id, base_dir):
    logger = setup_logger(job_name, log_file="logs/restore.log")
    sanitized_job = "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in job_name)
    manifest_path = os.path.join(base_dir, "data", "manifests", sanitized_job, f"{backup_set_id}.json")
    logger.info(f"Loading manifest: {manifest_path}")
    with open(manifest_path, "r") as f:
        manifest = json.load(f)
    logger.info(f"Loaded manifest with {len(manifest.get('files', []))} files.")
    return manifest

def extract_file_from_tarball(tarball_path, member_path, target_path, logger):
    logger.info(f"Extracting '{member_path}' from '{tarball_path}' to '{target_path}'")
    try:
        if tarball_path.endswith('.gpg'):
            passphrase = get_passphrase()
            if not passphrase:
                logger.error("GPG passphrase not set in environment or .env file.")
                return False, "GPG passphrase not set. Cannot decrypt archive."
            gpg_cmd = [
                "gpg", "--batch", "--yes", "--passphrase", passphrase,
                "-d", tarball_path
            ]
            proc = subprocess.Popen(gpg_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            try:
                with tarfile.open(fileobj=proc.stdout, mode='r|*') as tar:
                    for member in tar:
                        if member.name == member_path:
                            os.makedirs(os.path.dirname(target_path), exist_ok=True)
                            with open(target_path, "wb") as out_f, tar.extractfile(member) as in_f:
                                out_f.write(in_f.read())
                            logger.info(f"Successfully restored '{member_path}' to '{target_path}'")
                            return True, None
                    logger.error(f"{member_path} not found in {tarball_path}")
                    return False, f"{member_path} not found in {tarball_path}"
            except tarfile.ReadError as e:
                # Read GPG stderr for more info
                gpg_err = proc.stderr.read().decode()
                logger.error(f"GPG decryption or tar extraction failed: {gpg_err}")
                user_msg = (
                    f"Cannot restore '{member_path}' from '{os.path.basename(tarball_path)}':\n"
                    "GPG decryption failed or output is not a valid tar archive.\n"
                    f"GPG error: {gpg_err.strip()}"
                )
                return False, user_msg
            finally:
                proc.stdout.close()
                proc.stderr.close()
                proc.wait()
        else:
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

def restore_files(job_name, backup_set_id, files, dest=None, base_dir=None):
    logger = setup_logger(job_name, log_file="logs/restore.log")
    logger.info(f"PASSPHRASE loaded: {'YES' if get_passphrase() else 'NO'}")
    logger.info(f"Starting restore_files for job '{job_name}', backup_set_id '{backup_set_id}'")
    set_restore_status(job_name, backup_set_id, running=True)
    manifest = get_manifest(job_name, backup_set_id, base_dir)
    restored = []
    errors = []
    for f in manifest.get("files", []):
        if f["path"] in files:
            tarball_path = f["tarball_path"]
            member_path = f["path"]
            if dest:
                target = os.path.join(dest, member_path)
            else:
                source_base = manifest["config"]["source"]
                target = os.path.join(source_base, member_path)
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
    set_restore_status(job_name, backup_set_id, running=False)
    return {"restored": restored, "errors": errors}

def restore_full(job_name, backup_set_id, dest=None, base_dir=None):
    logger = setup_logger(job_name, log_file="logs/restore.log")
    logger.info(f"Starting full restore for job '{job_name}', backup_set_id '{backup_set_id}'")
    # Before starting restore
    set_restore_status(job_name, backup_set_id, running=True)
    manifest = get_manifest(job_name, backup_set_id, base_dir)
    files = [f["path"] for f in manifest.get("files", [])]
    result = restore_files(job_name, backup_set_id, files, dest=dest, base_dir=base_dir)
    # After restore completes (success or fail)
    set_restore_status(job_name, backup_set_id, running=False)
    return result