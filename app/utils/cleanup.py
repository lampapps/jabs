"""Utility functions for cleaning up stale lock files in JABS."""

import os
import fcntl
from app.settings import LOCK_DIR

def is_lock_stale(lock_path):
    """Return True if the lock file is not locked by any process."""
    try:
        with open(lock_path, 'a+', encoding='utf-8') as lock_file:
            try:
                fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
                # Lock acquired, so it's not held by anyone else
                fcntl.flock(lock_file, fcntl.LOCK_UN)
                return True
            except BlockingIOError:
                # Lock is held by another process
                return False
    except OSError:
        # If the file can't be opened, treat as not stale
        return False

def cleanup_stale_locks():
    """Remove all stale lock files in the LOCK_DIR."""
    removed = []
    for fname in os.listdir(LOCK_DIR):
        if fname.endswith('.lock'):
            path = os.path.join(LOCK_DIR, fname)
            if is_lock_stale(path):
                try:
                    os.remove(path)
                    removed.append(fname)
                except OSError:
                    pass
    return removed
