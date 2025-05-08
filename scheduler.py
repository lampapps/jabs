import os
import glob
import yaml
import subprocess
import fcntl  # For file locking (Unix-specific)
import errno
from datetime import datetime, timedelta
from croniter import croniter
from app.utils.logger import setup_logger, trim_all_logs
import time
from app.settings import BASE_DIR, CONFIG_DIR, LOCK_DIR, LOG_DIR, CLI_SCRIPT, PYTHON_EXECUTABLE

# --- Constants ---
SCHEDULE_TOLERANCE = timedelta(seconds=15)
MAX_LOG_LINES = 1000
SCHEDULER_LOG_FILE = os.path.join(LOG_DIR, "scheduler.log")
SCHEDULER_STATUS_FILE = os.path.join(LOG_DIR, "scheduler.status")

# --- Logging Setup ---
os.makedirs(LOG_DIR, exist_ok=True)
logger = setup_logger("scheduler", log_file=SCHEDULER_LOG_FILE)

# --- Lock File Handling ---
_lock_files = {}

def acquire_lock(lock_file_path):
    try:
        f = open(lock_file_path, 'w')
        fcntl.lockf(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        _lock_files[lock_file_path] = f
        logger.debug(f"Acquired lock: {lock_file_path}")
        return f
    except IOError as e:
        if e.errno == errno.EACCES or e.errno == errno.EAGAIN:
            logger.debug(f"Lock already held for: {lock_file_path}")
            if 'f' in locals():
                f.close()
            return None
        logger.error(f"Unexpected IOError acquiring lock for {lock_file_path}: {e}")
        if 'f' in locals():
            f.close()
        raise
    except Exception as e:
        logger.error(f"Unexpected error acquiring lock for {lock_file_path}: {e}")
        if lock_file_path in _lock_files and _lock_files[lock_file_path]:
            _lock_files[lock_file_path].close()
            del _lock_files[lock_file_path]
        return None

def release_lock(lock_file_path):
    if lock_file_path in _lock_files:
        f = _lock_files.pop(lock_file_path)
        if f:
            try:
                fcntl.lockf(f, fcntl.LOCK_UN)
                f.close()
                logger.debug(f"Released lock: {lock_file_path}")
            except Exception as e:
                logger.error(f"Error releasing lock for {lock_file_path}: {e}")
        else:
            logger.warning(f"Attempted to release lock for {lock_file_path}, but file handle was None.")
    else:
        logger.warning(f"Attempted to release lock for {lock_file_path}, but it was not found in tracked locks.")

def main():
    """Checks configurations, schedules, and triggers jobs if needed."""
    logger.info("--- Scheduler Check Started ---")
    os.makedirs(LOCK_DIR, exist_ok=True)
    now = datetime.now()

    # FIX: Look for job configs in config/jobs/
    JOBS_DIR = os.path.join(CONFIG_DIR, "jobs")
    config_files = glob.glob(os.path.join(JOBS_DIR, "*.yaml"))
    if not config_files:
        logger.info("No configuration files found in %s", JOBS_DIR)
        logger.info("--- Scheduler Check Finished ---")
        return

    triggered_jobs_count = 0

    try:
        for config_path in config_files:
            job_name_from_file = os.path.splitext(os.path.basename(config_path))[0]
            lock_file_path = os.path.join(LOCK_DIR, f"{job_name_from_file}.lock")

            lock_handle = acquire_lock(lock_file_path)
            if not lock_handle:
                logger.info(f"Job '{job_name_from_file}' is already running or locked. Skipping.")
                continue

            try:
                try:
                    with open(config_path, 'r') as f:
                        config = yaml.safe_load(f)
                    if not config:
                        logger.warning(f"Config file is empty or invalid: {config_path}")
                        continue
                    job_name = config.get("job_name", job_name_from_file)
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
                    backup_type = schedule.get('type', 'full')
                    sync_s3 = schedule.get('sync', False)

                    if not enabled:
                        logger.debug(f"Schedule '{cron_expr}' in {config_path} is disabled.")
                        continue

                    if not cron_expr:
                        logger.warning(f"Schedule in {config_path} is missing 'cron' expression.")
                        continue

                    try:
                        cron = croniter(cron_expr, now)
                        prev_run_time = cron.get_prev(datetime)
                        if (now - prev_run_time) < SCHEDULE_TOLERANCE:
                            logger.info(f"MATCH FOUND for job '{job_name}' (config: {os.path.basename(config_path)}): Schedule '{cron_expr}'")
                            command = [PYTHON_EXECUTABLE, CLI_SCRIPT, config_path]
                            if backup_type.lower() in ["diff", "differential"]:
                                command.append("--diff")
                            else:
                                command.append("--full")
                            if sync_s3:
                                command.append("--sync")
                            logger.info(f"Executing command: {' '.join(command)}")
                            try:
                                subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                                triggered_jobs_count += 1
                                job_triggered_for_this_config = True
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
            finally:
                release_lock(lock_file_path)

        logger.info(f"Triggered {triggered_jobs_count} job(s) during this check.")
        logger.info("--- Scheduler Check Finished ---")

        try:
            with open(SCHEDULER_STATUS_FILE, 'w') as f:
                f.write(str(time.time()))
            logger.debug(f"Updated status file: {SCHEDULER_STATUS_FILE}")
        except Exception as e:
            logger.error(f"Failed to update status file {SCHEDULER_STATUS_FILE}: {e}")

    except Exception as e:
        logger.error(f"An unexpected error occurred in the main scheduler loop: {e}", exc_info=True)
    finally:
        trim_all_logs()

if __name__ == "__main__":
    main()
