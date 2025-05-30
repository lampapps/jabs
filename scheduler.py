"""Scheduler for running backup jobs based on cron schedules."""

import os
import glob
import subprocess
import fcntl
import errno
import time
from datetime import datetime, timedelta

import yaml
from croniter import croniter

from app.utils.logger import setup_logger, trim_all_logs
from app.settings import CONFIG_DIR, LOCK_DIR, LOG_DIR, CLI_SCRIPT, PYTHON_EXECUTABLE

# --- Constants ---
SCHEDULE_TOLERANCE = timedelta(seconds=15)
SCHEDULER_STATUS_FILE = os.path.join(LOG_DIR, "scheduler.status")

# --- Logging Setup ---
os.makedirs(LOG_DIR, exist_ok=True)
logger = setup_logger("scheduler", log_file="scheduler.log")

# --- Lock File Handling ---
_lock_files = {}

def acquire_lock(lock_file_path):
    """Acquire a non-blocking exclusive lock on the given file path."""
    try:
        f = open(lock_file_path, 'w')
        fcntl.lockf(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        _lock_files[lock_file_path] = f
        logger.debug(f"Acquired lock: {lock_file_path}")
        return f
    except IOError as e:
        if e.errno in (errno.EACCES, errno.EAGAIN):
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
    """Release the lock for the given file path."""
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
            logger.warning(
                f"Attempted to release lock for {lock_file_path}, but file handle was None."
            )
    else:
        logger.warning(
            f"Attempted to release lock for {lock_file_path}, but it was not found in tracked locks."
        )

def load_yaml_config(path):
    """Load a YAML configuration file and return its contents."""
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        logger.error(f"Config file not found: {path}")
        return None
    except yaml.YAMLError as e:
        logger.error(f"Error parsing YAML file {path}: {e}")
        return None
    except Exception as e:
        logger.error(f"Error loading config {path}: {e}")
        return None

def get_job_configs():
    """Return a list of job config file paths."""
    jobs_dir = os.path.join(CONFIG_DIR, "jobs")
    config_files = glob.glob(os.path.join(jobs_dir, "*.yaml"))
    return config_files

def should_trigger(cron_expr, now):
    """Return True if the cron expression matches the current time within tolerance."""
    try:
        cron = croniter(cron_expr, now)
        prev_run_time = cron.get_prev(datetime)
        return (now - prev_run_time) < SCHEDULE_TOLERANCE, prev_run_time
    except Exception as e:
        logger.error(f"Invalid cron expression '{cron_expr}': {e}")
        return False, None

def build_command(config_path, backup_type, config, global_config):
    """Build the command to execute for the job."""
    command = [PYTHON_EXECUTABLE, CLI_SCRIPT, "--config", config_path]
    if backup_type.lower() in ["diff", "differential"]:
        command.append("--diff")
    else:
        command.append("--full")

    aws_enabled = config.get("aws", {}).get("enabled")
    if aws_enabled is None:
        aws_enabled = global_config.get("aws", {}).get("enabled", False)
    if aws_enabled:
        command.append("--sync")

    encrypt_enabled = config.get("encryption", {}).get("enabled")
    if encrypt_enabled is None:
        encrypt_enabled = global_config.get("encryption", {}).get("enabled", False)
    if encrypt_enabled:
        command.append("--encrypt")

    return command

def process_job_config(config_path, global_config, now):
    """Process a single job config file and trigger jobs if needed."""
    job_name_from_file = os.path.splitext(os.path.basename(config_path))[0]
    lock_file_path = os.path.join(LOCK_DIR, f"{job_name_from_file}.lock")
    lock_handle = acquire_lock(lock_file_path)
    if not lock_handle:
        logger.info(f"Job '{job_name_from_file}' is already running or locked. Skipping.")
        return 0

    triggered = 0
    try:
        config = load_yaml_config(config_path)
        if not config:
            logger.warning(f"Config file is empty or invalid: {config_path}")
            return 0
        job_name = config.get("job_name", job_name_from_file)
        schedules = config.get('schedules', [])
        if not schedules:
            logger.debug(f"No schedules defined in {config_path}")
            return 0

        for schedule in schedules:
            cron_expr = schedule.get('cron')
            enabled = schedule.get('enabled', False)
            backup_type = schedule.get('type', 'full')

            if not enabled:
                logger.debug(f"Schedule '{cron_expr}' in {config_path} is disabled.")
                continue

            if not cron_expr:
                logger.warning(f"Schedule in {config_path} is missing 'cron' expression.")
                continue

            matched, prev_run_time = should_trigger(cron_expr, now)
            if matched:
                logger.info(
                    f"MATCH FOUND for job '{job_name}' (config: {os.path.basename(config_path)}): "
                    f"Schedule '{cron_expr}'"
                )
                command = build_command(config_path, backup_type, config, global_config)
                logger.info(f"Executing command: {' '.join(command)}")
                try:
                    subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                    triggered += 1
                    break  # Only trigger once per config per run
                except FileNotFoundError:
                    logger.error(
                        f"Error: The script '{CLI_SCRIPT}' or python executable '{PYTHON_EXECUTABLE}' was not found."
                    )
                except Exception as e:
                    logger.error(f"Failed to launch subprocess for job '{job_name}': {e}")
            else:
                logger.debug(
                    f"Schedule '{cron_expr}' in {config_path} did not match within tolerance. "
                    f"Last scheduled: {prev_run_time}"
                )
    finally:
        release_lock(lock_file_path)
    return triggered

def update_status_file():
    """Update the scheduler status file with the current timestamp."""
    try:
        with open(SCHEDULER_STATUS_FILE, 'w', encoding='utf-8') as f:
            f.write(str(time.time()))
        logger.debug(f"Updated status file: {SCHEDULER_STATUS_FILE}")
    except Exception as e:
        logger.error(f"Failed to update status file {SCHEDULER_STATUS_FILE}: {e}")

def main():
    """Checks configurations, schedules, and triggers jobs if needed."""
    logger.info("--- Scheduler Check Started ---")
    os.makedirs(LOCK_DIR, exist_ok=True)
    now = datetime.now()

    config_files = get_job_configs()
    if not config_files:
        logger.info("No configuration files found in %s", os.path.join(CONFIG_DIR, "jobs"))
        logger.info("--- Scheduler Check Finished ---")
        return

    global_config_path = os.path.join(CONFIG_DIR, "global.yaml")
    global_config = load_yaml_config(global_config_path) or {}

    triggered_jobs_count = 0

    try:
        for config_path in config_files:
            triggered_jobs_count += process_job_config(config_path, global_config, now)

        logger.info(f"Triggered {triggered_jobs_count} job(s) during this check.")
        logger.info("--- Scheduler Check Finished ---")
        update_status_file()
    except Exception as e:
        logger.error(f"An unexpected error occurred in the main scheduler loop: {e}", exc_info=True)
    finally:
        trim_all_logs()

if __name__ == "__main__":
    main()
