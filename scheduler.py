import os
import glob
import yaml
import subprocess
import fcntl  # For file locking (Unix-specific)
import sys
import errno
from datetime import datetime, timedelta
from croniter import croniter
from utils.logger import setup_logger
import time  # Add time import

# --- Constants ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_DIR = os.path.join(BASE_DIR, "config")
LOCK_DIR = os.path.join(BASE_DIR, "locks")
LOG_DIR = os.path.join(BASE_DIR, "logs")
CLI_SCRIPT = os.path.join(BASE_DIR, "cli.py")
PYTHON_EXECUTABLE = sys.executable or "python3" # Use the same python or default to python3
# How far back to check for missed schedules (e.g., if cron runs every minute)
SCHEDULE_TOLERANCE = timedelta(seconds=15)
# Maximum number of lines to keep in the scheduler log file
MAX_LOG_LINES = 1000 # Adjust this value as needed
SCHEDULER_LOG_FILE = os.path.join(LOG_DIR, "scheduler.log")
SCHEDULER_STATUS_FILE = os.path.join(LOG_DIR, "scheduler.status") # Path for the status file

# --- Logging Setup ---
os.makedirs(LOG_DIR, exist_ok=True)
logger = setup_logger("scheduler", log_file=SCHEDULER_LOG_FILE)

# --- Lock File Handling ---
# Dictionary to keep track of open lock file handles
# Keys are lock file paths, values are file objects
_lock_files = {}

def acquire_lock(lock_file_path):
    """
    Attempts to acquire an exclusive, non-blocking lock on a file.
    Returns the file handle if successful, None otherwise.
    """
    try:
        # Open the file, creating it if it doesn't exist
        f = open(lock_file_path, 'w')
        # Attempt to acquire an exclusive lock without blocking
        fcntl.lockf(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        # Store the file handle
        _lock_files[lock_file_path] = f
        logger.debug(f"Acquired lock: {lock_file_path}")
        return f # Return the handle
    except IOError as e:
        # If the error is EACCES or EAGAIN, it means the lock is held
        if e.errno == errno.EACCES or e.errno == errno.EAGAIN:
            logger.debug(f"Lock already held for: {lock_file_path}")
            if f: # Close the file handle if we opened it but failed to lock
                f.close()
            return None
        # Re-raise other IOErrors
        logger.error(f"Unexpected IOError acquiring lock for {lock_file_path}: {e}")
        if f:
            f.close()
        raise
    except Exception as e:
        logger.error(f"Unexpected error acquiring lock for {lock_file_path}: {e}")
        if lock_file_path in _lock_files and _lock_files[lock_file_path]:
             _lock_files[lock_file_path].close()
             del _lock_files[lock_file_path]
        return None # Indicate failure

def release_lock(lock_file_path):
    """
    Releases the lock and closes the lock file.
    """
    if lock_file_path in _lock_files:
        f = _lock_files.pop(lock_file_path)
        if f:
            try:
                fcntl.lockf(f, fcntl.LOCK_UN)
                f.close()
                # Optionally remove the lock file after releasing
                # os.remove(lock_file_path)
                logger.debug(f"Released lock: {lock_file_path}")
            except Exception as e:
                logger.error(f"Error releasing lock for {lock_file_path}: {e}")
        else:
             logger.warning(f"Attempted to release lock for {lock_file_path}, but file handle was None.")
    else:
        logger.warning(f"Attempted to release lock for {lock_file_path}, but it was not found in tracked locks.")

# --- Log Trimming ---
def trim_log_file(log_path, max_lines):
    """
    Trims the log file to a maximum number of lines, keeping the most recent ones.
    """
    try:
        if not os.path.exists(log_path):
            logger.debug(f"Log file {log_path} not found for trimming.")
            return

        with open(log_path, 'r') as f:
            lines = f.readlines()

        if len(lines) > max_lines:
            lines_to_keep = lines[-max_lines:]
            with open(log_path, 'w') as f:
                f.writelines(lines_to_keep)
            logger.info(f"Trimmed log file {log_path}. Removed {len(lines) - max_lines} lines.")
        else:
            logger.debug(f"Log file {log_path} is within the line limit ({len(lines)}/{max_lines}). No trimming needed.")

    except IOError as e:
        logger.error(f"Error reading/writing log file {log_path} during trimming: {e}")
    except Exception as e:
        logger.error(f"Unexpected error trimming log file {log_path}: {e}")


# --- Main Logic ---
def main():
    """Checks configurations, schedules, and triggers jobs if needed."""
    logger.info("--- Scheduler Check Started ---")
    os.makedirs(LOCK_DIR, exist_ok=True)
    now = datetime.now()

    config_files = glob.glob(os.path.join(CONFIG_DIR, "*.yaml"))
    if not config_files:
        logger.info("No configuration files found in %s", CONFIG_DIR)
        logger.info("--- Scheduler Check Finished ---")
        return

    triggered_jobs_count = 0

    try:  # Add a try block around the main loop for status update
        for config_path in config_files:
            job_name_from_file = os.path.splitext(os.path.basename(config_path))[0]
            lock_file_path = os.path.join(LOCK_DIR, f"{job_name_from_file}.lock")

            lock_handle = acquire_lock(lock_file_path)
            if not lock_handle:
                logger.info(f"Job '{job_name_from_file}' is already running or locked. Skipping.")
                continue # Skip this config file if locked

            try:
                # Load configuration
                try:
                    with open(config_path, 'r') as f:
                        config = yaml.safe_load(f)
                    if not config:
                        logger.warning(f"Config file is empty or invalid: {config_path}")
                        continue
                    job_name = config.get("job_name", job_name_from_file) # Use job_name from config if available
                except FileNotFoundError:
                    logger.error(f"Config file not found during check: {config_path}")
                    continue
                except yaml.YAMLError as e:
                    logger.error(f"Error parsing YAML file {config_path}: {e}")
                    continue
                except Exception as e:
                     logger.error(f"Error loading config {config_path}: {e}")
                     continue

                schedules = config.get('schedules', [])
                if not schedules:
                    logger.debug(f"No schedules defined in {config_path}")
                    continue

                job_triggered_for_this_config = False
                for schedule in schedules:
                    cron_expr = schedule.get('cron')
                    enabled = schedule.get('enabled', False)
                    backup_type = schedule.get('type', 'full') # Default to full
                    sync_s3 = schedule.get('sync', False)

                    if not enabled:
                        logger.debug(f"Schedule '{cron_expr}' in {config_path} is disabled.")
                        continue

                    if not cron_expr:
                        logger.warning(f"Schedule in {config_path} is missing 'cron' expression.")
                        continue

                    try:
                        # Check if the schedule should have run recently
                        cron = croniter(cron_expr, now)
                        # Get the most recent scheduled time *before or at* the current time
                        prev_run_time = cron.get_prev(datetime)

                        # Check if the previous run time is within our tolerance window
                        if (now - prev_run_time) < SCHEDULE_TOLERANCE:
                            logger.info(f"MATCH FOUND for job '{job_name}' (config: {os.path.basename(config_path)}): Schedule '{cron_expr}'")

                            # Construct command to run cli.py
                            command = [PYTHON_EXECUTABLE, CLI_SCRIPT, config_path]
                            if backup_type.lower() in ["diff", "differential"]:
                                command.append("--diff")
                            else:
                                command.append("--full") # Default to full if type is invalid or missing

                            if sync_s3:
                                command.append("--sync")

                            logger.info(f"Executing command: {' '.join(command)}")
                            try:
                                # Use Popen to run asynchronously
                                subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                                triggered_jobs_count += 1
                                job_triggered_for_this_config = True
                                # Break after triggering one schedule for this config file
                                break
                            except FileNotFoundError:
                                logger.error(f"Error: The script '{CLI_SCRIPT}' or python executable '{PYTHON_EXECUTABLE}' was not found.")
                            except Exception as e:
                                logger.error(f"Failed to launch subprocess for job '{job_name}': {e}")
                        else:
                            logger.debug(f"Schedule '{cron_expr}' in {config_path} did not match within tolerance. Last scheduled: {prev_run_time}")

                    except ValueError as e:
                        logger.error(f"Invalid cron expression '{cron_expr}' in {config_path}: {e}")
                    except Exception as e:
                        logger.error(f"Error processing schedule '{cron_expr}' in {config_path}: {e}")

                # If no job was triggered for this config, we don't need the lock anymore for this run
                # The lock is released in the finally block anyway

            finally:
                # Always release the lock after checking/triggering the job for this config file
                release_lock(lock_file_path)

        logger.info(f"Triggered {triggered_jobs_count} job(s) during this check.")
        logger.info("--- Scheduler Check Finished ---")

        # --- Add Status File Update ---
        try:
            with open(SCHEDULER_STATUS_FILE, 'w') as f:
                f.write(str(time.time())) # Write current Unix timestamp
            logger.debug(f"Updated status file: {SCHEDULER_STATUS_FILE}")
        except Exception as e:
            logger.error(f"Failed to update status file {SCHEDULER_STATUS_FILE}: {e}")
        # --- End Status File Update ---

    except Exception as e:
         logger.error(f"An unexpected error occurred in the main scheduler loop: {e}", exc_info=True)
         # Optionally write an error state to the status file or handle differently
    finally:
        # Trim the log file after the check is complete, regardless of success/failure in loop
        trim_log_file(SCHEDULER_LOG_FILE, MAX_LOG_LINES)

if __name__ == "__main__":
    main()
