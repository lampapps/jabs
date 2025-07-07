import os
import tarfile
import json
from datetime import datetime

def get_all_files(src, exclude_patterns):
    """
    Recursively get all files in src, excluding any that match exclude_patterns.
    """
    file_list = []
    for root, dirs, files in os.walk(src):
        for file in files:
            full_path = os.path.join(root, file)
            if not any(pattern in full_path for pattern in exclude_patterns):
                file_list.append(full_path)
    return file_list

def get_new_or_modified_files(src, manifest_path, exclude_patterns=None):
    """
    Return a list of files that are either new (not present in previous manifest)
    or have a newer mtime than recorded in the manifest.
    """
    exclude_patterns = exclude_patterns or []
    # Load previous manifest file paths and mtimes
    prev_files = {}
    if os.path.exists(manifest_path):
        with open(manifest_path, "r", encoding="utf-8") as f:
            manifest = json.load(f)
        for entry in manifest.get("files", []):
            # entry should have at least "path" and "mtime"
            prev_files[entry["path"]] = entry.get("mtime", 0)
    # Walk current source tree
    changed_files = []
    for root, dirs, filenames in os.walk(src):
        for filename in filenames:
            full_path = os.path.join(root, filename)
            rel_path = os.path.relpath(full_path, src)
            if any(pattern in full_path for pattern in exclude_patterns):
                continue
            try:
                mtime = os.path.getmtime(full_path)
            except (FileNotFoundError, OSError):
                continue
            prev_mtime = prev_files.get(rel_path)
            if prev_mtime is None:
                # New file (not in manifest)
                changed_files.append(full_path)
            elif mtime > prev_mtime:
                # Modified file
                changed_files.append(full_path)
    return changed_files

def create_tar_archives(files, dest_tar_dir, max_tarball_size_mb, logger, backup_type, config):
    """
    Create multiple tar archives from the list of files, each up to max_tarball_size_mb (in MB).
    Returns a list of tarball paths.
    """
    max_tarball_size = max_tarball_size_mb * 1024 * 1024
    tarball_index = 1
    current_tar_size = 0
    tarball_paths = []
    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    current_tar_path = os.path.join(
        dest_tar_dir, f"{backup_type}_part_{tarball_index}_{timestamp_str}.tar.gz"
    )
    tar = tarfile.open(current_tar_path, "w:gz")
    tarball_paths.append(current_tar_path)

    # Use the source base for relative paths in the archive
    source_base = config.get("source", "")

    for full_path in files:
        arcname = os.path.relpath(full_path, source_base)
        try:
            # Skip broken symlinks
            if os.path.islink(full_path):
                target = os.readlink(full_path)
                if not os.path.exists(os.path.join(os.path.dirname(full_path), target)):
                    logger.warning(f"Skipping broken symlink: {full_path} -> {target}")
                    continue
            file_size = os.path.getsize(full_path)
        except (FileNotFoundError, OSError) as e:
            logger.warning(f"Skipping file (not found or inaccessible): {full_path} ({e})")
            continue
        if current_tar_size + file_size > max_tarball_size and current_tar_size > 0:
            tar.close()
            logger.info(f"Tarball created: {current_tar_path} (size: {current_tar_size} bytes)")
            tarball_index += 1
            current_tar_path = os.path.join(
                dest_tar_dir, f"{backup_type}_part_{tarball_index}_{timestamp_str}.tar.gz"
            )
            tar = tarfile.open(current_tar_path, "w:gz")
            tarball_paths.append(current_tar_path)
            current_tar_size = 0
        tar.add(full_path, arcname=arcname)
        current_tar_size += file_size

    tar.close()
    logger.info(f"Tarball created: {current_tar_path} (size: {current_tar_size} bytes)")
    return tarball_paths

def find_latest_backup_set(job_dst):
    # Find the latest backup set directory by timestamp or naming convention
    sets = sorted([
        d for d in os.listdir(job_dst)
        if os.path.isdir(os.path.join(job_dst, d))
    ], reverse=True)
    if sets:
        return os.path.join(job_dst, sets[0])
    return None