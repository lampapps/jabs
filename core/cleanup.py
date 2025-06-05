import os
import fcntl

def is_lock_stale(lock_path):
    """Return True if the lock file is not locked by any process."""
    try:
        with open(lock_path, 'a+') as lock_file:
            try:
                fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
                # Lock acquired, so it's not held by anyone else
                fcntl.flock(lock_file, fcntl.LOCK_UN)
                return True
            except BlockingIOError:
                # Lock is held by another process
                return False
    except Exception:
        # If the file can't be opened, treat as not stale
        return False

def cleanup_stale_locks(lock_dir):
    """Remove all stale lock files in the given directory."""
    removed = []
    for fname in os.listdir(lock_dir):
        if fname.endswith('.lock'):
            path = os.path.join(lock_dir, fname)
            if is_lock_stale(path):
                try:
                    os.remove(path)
                    removed.append(fname)
                except Exception:
                    pass
    return removed