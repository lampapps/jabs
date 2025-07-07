"""Utility functions for cleaning up lock files in JABS."""

import os
import time
from app.settings import LOCK_DIR

def cleanup_all_locks():
    """Remove all lock files in the LOCK_DIR."""
    removed = []
    if not os.path.exists(LOCK_DIR):
        return removed
        
    for fname in os.listdir(LOCK_DIR):
        if fname.endswith('.lock'):
            path = os.path.join(LOCK_DIR, fname)
            try:
                os.remove(path)
                removed.append(fname)
            except OSError:
                pass
    return removed

def cleanup_stale_locks():
    """
    Remove stale lock files using multiple heuristics:
    1. Check if the PID in the lock file is still running
    2. Fall back to very conservative time-based cleanup (7 days)
    """
    removed = []
    if not os.path.exists(LOCK_DIR):
        return removed
    
    # Very conservative threshold - 7 days
    # If a backup job runs longer than 7 days, it's likely stuck
    stale_threshold = time.time() - (7 * 24 * 60 * 60)
    
    for fname in os.listdir(LOCK_DIR):
        if fname.endswith('.lock'):
            lock_path = os.path.join(LOCK_DIR, fname)
            try:
                # First, try to check if the process is still running
                is_stale = False
                
                # Read the PID from the lock file
                try:
                    with open(lock_path, 'r', encoding='utf-8') as f:
                        content = f.read().strip()
                    
                    try:
                        pid = int(content)
                        # Check if PID exists by trying to send signal 0 (doesn't actually send a signal)
                        os.kill(pid, 0)
                        # If we get here, process exists - check if it's really old
                        mtime = os.path.getmtime(lock_path)
                        if mtime < stale_threshold:
                            # Process exists but lock is very old, likely stuck
                            is_stale = True
                    except (ValueError, ProcessLookupError):
                        # Invalid PID format or process doesn't exist
                        is_stale = True
                    except PermissionError:
                        # Process exists but we can't signal it (different user)
                        # Use time-based check only
                        mtime = os.path.getmtime(lock_path)
                        if mtime < stale_threshold:
                            is_stale = True
                        
                except (OSError, IOError):
                    # Can't read lock file, use time-based check
                    mtime = os.path.getmtime(lock_path)
                    if mtime < stale_threshold:
                        is_stale = True
                
                if is_stale:
                    os.remove(lock_path)
                    removed.append(fname)
                    
            except OSError:
                # Can't access the file, skip it
                pass
                
    return removed

def cleanup_stale_locks_advanced():
    """
    Advanced cleanup using psutil for better process detection.
    Falls back to basic cleanup if psutil is not available.
    """
    try:
        import psutil
    except ImportError:
        # Fall back to basic cleanup if psutil is not available
        return cleanup_stale_locks()
    
    removed = []
    if not os.path.exists(LOCK_DIR):
        return removed
    
    # Even more conservative with psutil - 14 days for truly stuck processes
    very_stale_threshold = time.time() - (14 * 24 * 60 * 60)
        
    for fname in os.listdir(LOCK_DIR):
        if fname.endswith('.lock'):
            lock_path = os.path.join(LOCK_DIR, fname)
            try:
                # Read the PID from the lock file
                with open(lock_path, 'r', encoding='utf-8') as f:
                    content = f.read().strip()
                
                try:
                    pid = int(content)
                except ValueError:
                    # Invalid lock file format, consider it stale
                    os.remove(lock_path)
                    removed.append(fname)
                    continue
                
                is_stale = False
                
                # Check if the process is still running
                if not psutil.pid_exists(pid):
                    # Process doesn't exist, lock is definitely stale
                    is_stale = True
                else:
                    try:
                        proc = psutil.Process(pid)
                        cmdline = ' '.join(proc.cmdline())
                        
                        # Check if it's actually a JABS process
                        if 'cli.py' not in cmdline and 'jabs' not in cmdline.lower() and 'backup' not in cmdline.lower():
                            # PID was reused by a different process
                            is_stale = True
                        else:
                            # It's a JABS process, but check if it's been running too long
                            mtime = os.path.getmtime(lock_path)
                            if mtime < very_stale_threshold:
                                # Even JABS processes shouldn't run for 2 weeks
                                is_stale = True
                                
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        # Process disappeared or we can't access it
                        is_stale = True
                
                if is_stale:
                    os.remove(lock_path)
                    removed.append(fname)
                        
            except (OSError, IOError):
                # Can't read the lock file, might be corrupted or in use
                pass
                
    return removed

