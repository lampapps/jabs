"""Scheduler for running backup jobs based on cron schedules."""

import os
import glob
import subprocess
import time
from datetime import datetime

import yaml
from croniter import croniter

from app.utils.logger import setup_logger, trim_all_logs
from app.services.emailer import send_email_digest
from app.utils.monitor_status import write_monitor_status
from app.settings import CONFIG_DIR, LOCK_DIR, LOG_DIR, CLI_SCRIPT, PYTHON_EXECUTABLE, SCHEDULER_STATUS_FILE, SCHEDULE_TOLERANCE, VERSION
from app.utils.scheduler_events import append_scheduler_event
from core.backup.common import acquire_lock, release_lock


# --- Logging Setup ---
os.makedirs(LOG_DIR, exist_ok=True)
logger = setup_logger("scheduler", log_file="scheduler.log")

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
    except OSError as e:
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
    except (ValueError, TypeError) as e:
        logger.error(f"Invalid cron expression '{cron_expr}': {e}")
        return False, None

def build_command(config_path, backup_type, config, global_config):
    """Build the command to execute for the job."""
    command = [PYTHON_EXECUTABLE, CLI_SCRIPT, "--config", config_path]
    
    # Handle different backup types
    backup_type_lower = backup_type.lower()
    if backup_type_lower in ["diff", "differential"]:
        command.append("--diff")
    elif backup_type_lower in ["inc", "incremental"]:
        command.append("--incremental")
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
    lock_handle = None
    try:
        lock_handle = acquire_lock(lock_file_path)
        if not lock_handle:
            logger.info(f"Job '{job_name_from_file}' is already running or locked. Skipping.")
            return 0

        triggered = 0
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
                except subprocess.SubprocessError as e:
                    logger.error(f"Failed to launch subprocess for job '{job_name}': {e}")
                except Exception as e:  # If you must, add a comment
                    logger.error(f"Unexpected error launching subprocess for job '{job_name}': {e}")
            else:
                logger.debug(
                    f"Schedule '{cron_expr}' in {config_path} did not match within tolerance. "
                    f"Last scheduled: {prev_run_time}"
                )
        return triggered
    finally:
        if lock_handle:
            release_lock(lock_handle)

def update_status_file():
    """Update the scheduler status file with the current timestamp."""
    try:
        with open(SCHEDULER_STATUS_FILE, 'w', encoding='utf-8') as f:
            f.write(str(time.time()))
        logger.debug(f"Updated status file: {SCHEDULER_STATUS_FILE}")
    except OSError as e:
        logger.error(f"Failed to update status file {SCHEDULER_STATUS_FILE}: {e}")

def send_digest_email(global_config, now):
    """Return True if it's time to send the digest email based on cron syntax in config."""
    email_cfg = global_config.get("email", {})
    cron_expr = email_cfg.get("digest_email_schedule")
    if not cron_expr:
        return False
    try:
        cron = croniter(cron_expr, now)
        prev_run_time = cron.get_prev(datetime)
        return (now - prev_run_time) < SCHEDULE_TOLERANCE
    except (ValueError, TypeError) as e:
        logger.error(f"Invalid digest_email_schedule cron '{cron_expr}': {e}")
        return False

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
    triggered_jobs_info = []  # List to keep track of jobs that actually ran
    any_job_errored = False

    try:
        for config_path in config_files:
            job_name_from_file = os.path.splitext(os.path.basename(config_path))[0]
            config = load_yaml_config(config_path)
            job_name = config.get("job_name", job_name_from_file) if config else job_name_from_file
            schedules = config.get('schedules', []) if config else []
            backup_type = None

            for schedule in schedules:
                cron_expr = schedule.get('cron')
                enabled = schedule.get('enabled', False)
                backup_type = schedule.get('type', 'full')
                if not enabled or not cron_expr:
                    continue
                matched, _ = should_trigger(cron_expr, now)
                if matched:
                    # Actually trigger the job
                    command = build_command(config_path, backup_type, config, global_config)
                    try:
                        subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                        triggered_jobs_count += 1
                        triggered_jobs_info.append({
                            "name": job_name,
                            "backup_type": backup_type,
                            "error": any_job_errored
                        })
                        break  # Only trigger once per config per run
                    except FileNotFoundError:
                        logger.error(
                            f"Error: The script '{CLI_SCRIPT}' or python executable '{PYTHON_EXECUTABLE}' was not found."
                        )
                    except subprocess.SubprocessError as e:
                        logger.error(f"Failed to launch subprocess for job '{job_name}': {e}")
                    except Exception as e:  # pylint: disable=broad-except
                        logger.error(f"Unexpected error launching subprocess for job '{job_name}': {e}")
                        triggered_jobs_info.append({
                            "name": job_name,
                            "backup_type": backup_type,
                            "error": True
                        })
                        any_job_errored = True
                        break
        # --- Digest email schedule check ---
        if send_digest_email(global_config, now):
            logger.info("Digest email schedule matched. Sending digest email.")
            send_email_digest()

        logger.info(f"Triggered {triggered_jobs_count} job(s) during this check.")
        logger.info("--- Scheduler Check Finished ---")
        update_status_file()
    except Exception as e:  # pylint: disable=broad-except
        logger.error(f"An unexpected error occurred in the main scheduler loop: {e}", exc_info=True)
        any_job_errored = True
    finally:
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if triggered_jobs_count == 0:
            append_scheduler_event(
                datetime=now_str,
                job_name="No jobs",
                backup_type=None,
                status="none"
            )
        trim_all_logs()
        # Monitor status reporting
        try:
            with open(os.path.join(CONFIG_DIR, "monitor.yaml"), encoding="utf-8") as f:
                monitor_cfg = yaml.safe_load(f)
            if monitor_cfg.get("enable_monitoring"):
                shared_dir = monitor_cfg.get("shared_monitor_dir")
                write_monitor_status(
                    shared_monitor_dir=shared_dir,
                    version=VERSION,
                    last_run=datetime.now().isoformat(),
                    log_dir=LOG_DIR
                )
        except (OSError, IOError, yaml.YAMLError) as e:
            logger.error(f"Failed to write monitor status: {e}")

if __name__ == "__main__":
    main()
    # Clean up stale lock files after log trimming 
    try:
        from app.utils.cleanup import cleanup_stale_locks
        removed = cleanup_stale_locks()
        if removed:
            logger.info(f"Cleaned up stale lock files: {removed}")
    except Exception as e:  # pylint: disable=broad-except
        logger.error(f"Error during lock file cleanup: {e}")
