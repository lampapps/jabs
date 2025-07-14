import re
import fnmatch
import os
import tarfile
import json
from datetime import datetime

from app.utils.logger import setup_logger

def get_all_files(src, exclude_patterns):
    """
    Recursively get all files in src, excluding any that match exclude_patterns.
    Uses the improved should_exclude function to properly handle directory patterns.
    """
    file_list = []
    logger = setup_logger("backup")
    logger.info(f"Starting file collection with {len(exclude_patterns)} exclusion patterns")
    
    # Collect all excluded directories for debugging
    excluded_dirs = []
    excluded_files = []
    
    for root, dirs, files in os.walk(src):
        # Use explicit indexes for modification during iteration
        i = 0
        while i < len(dirs):
            dir_path = os.path.join(root, dirs[i])
            # Critical: Force directory path to end with slash for matching
            dir_path_slash = dir_path
            
            # Print all directory paths for debugging
            rel_dir = os.path.relpath(dir_path, src)
            logger.debug(f"Checking directory: {rel_dir}")
            
            # Explicit check for Pictures and venv
            dir_name = os.path.basename(dir_path)
            if dir_name == "Pictures" or dir_name == "venv":
                logger.info(f"EXCLUDING directory by name: {rel_dir} (special case)")
                dirs.pop(i)
                excluded_dirs.append(rel_dir)
                continue
            
            # Check if directory should be excluded
            if should_exclude(dir_path, exclude_patterns, src):
                logger.info(f"EXCLUDING directory: {rel_dir}")
                dirs.pop(i)  # Remove from dirs to prevent traversal
                excluded_dirs.append(rel_dir)
            else:
                i += 1
        
        # Process files
        for file in files:
            file_path = os.path.join(root, file)
            rel_file = os.path.relpath(file_path, src)
            
            # Check if file should be excluded
            if should_exclude(file_path, exclude_patterns, src):
                logger.info(f"EXCLUDING file: {rel_file}")
                excluded_files.append(rel_file)
            else:
                file_list.append(file_path)
    
    logger.info(f"Excluded directories: {len(excluded_dirs)}")
    logger.info(f"Excluded files: {len(excluded_files)}")
    logger.info(f"Including files: {len(file_list)}")
    
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

def should_exclude(path, exclude_patterns, src=None):
    """
    Returns True if the given path should be excluded based on the patterns.
    Correctly handles directory patterns with trailing slashes.
    """
    if not exclude_patterns:
        return False
    
    logger = setup_logger("backup")
    
    # Get relative path
    rel_path = os.path.relpath(path, src) if src else path
    is_dir = os.path.isdir(path)
    
    # Normalize path for consistent matching across platforms
    rel_path_norm = rel_path.replace(os.sep, '/')
    if is_dir and not rel_path_norm.endswith('/'):
        rel_path_norm += '/'
    
    # Exact matching and simple pattern tests
    for pattern in exclude_patterns:
        orig_pattern = pattern
        
        # Handle directory patterns (with trailing slashes)
        is_dir_pattern = pattern.endswith('/')
        pattern = pattern.rstrip('/')
        
        # Skip directory patterns for files
        if is_dir_pattern and not is_dir:
            continue
            
        # Check if this path exactly matches the pattern
        if rel_path_norm.rstrip('/') == pattern:
            logger.info(f"EXCLUDED: '{rel_path}' exactly matches pattern '{orig_pattern}'")
            return True
            
        # Check if this path starts with pattern/ (directory prefix match)
        if is_dir_pattern and rel_path_norm.startswith(f"{pattern}/"):
            logger.info(f"EXCLUDED: '{rel_path}' is in directory '{orig_pattern}'")
            return True
            
        # Check for basename matches (filename only)
        if fnmatch.fnmatch(os.path.basename(path), pattern):
            logger.info(f"EXCLUDED: '{rel_path}' basename matches '{orig_pattern}'")
            return True
            
        # Check for direct glob matches on the whole path
        if fnmatch.fnmatch(rel_path_norm, pattern):
            logger.info(f"EXCLUDED: '{rel_path}' glob matches '{orig_pattern}'")
            return True
            
        # Handle ** wildcard patterns for matching any directory level
        if '**' in pattern:
            # Convert ** pattern to a regex
            regex_pattern = pattern.replace('**/', '(.*?/)?')  # Match any directory level
            regex_pattern = regex_pattern.replace('**', '.*?')  # Match any content
            regex_pattern = regex_pattern.replace('*', '[^/]*?')  # Regular glob
            regex_pattern = regex_pattern.replace('?', '.')  # Single character
            regex_pattern = f"^{regex_pattern}$"
            
            if re.match(regex_pattern, rel_path_norm):
                logger.info(f"EXCLUDED: '{rel_path}' matches wildcard pattern '{orig_pattern}'")
                return True
                
        # For directory exclusion patterns, see if any component of the path matches
        if is_dir_pattern:
            path_parts = rel_path_norm.split('/')
            for i, part in enumerate(path_parts):
                # Check if any directory component matches the pattern exactly
                if part == pattern:
                    parent_path = '/'.join(path_parts[:i+1])
                    logger.info(f"EXCLUDED: '{rel_path}' contains directory component '{pattern}/' at '{parent_path}'")
                    return True
    
    # Special case checks for common directory names
    # This is a safeguard for specific directories we know should be excluded
    if is_dir:
        dir_name = os.path.basename(path.rstrip('/'))
        if dir_name == "Pictures" or dir_name == "venv":
            logger.info(f"EXCLUDED: '{rel_path}' is a specifically excluded directory name (Pictures or venv)")
            return True
    
    return False