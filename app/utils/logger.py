# /utils/logger.py
import logging
import os
import glob
from datetime import datetime
from app.settings import LOG_DIR, MAX_LOG_LINES

class JobNameFormatter(logging.Formatter):
    """Custom formatter to include the job name in every log message."""
    def format(self, record):
        if hasattr(record, "job_name"):
            record.msg = f"{record.job_name} - {record.msg}"
        return super().format(record)

def setup_logger(job_name, log_file="backup.log"):
    """
    Set up a logger with the job name included in every message.
    :param job_name: Name of the job to include in log messages.
    :param log_file: Name or path to the log file.
    :return: A logger instance.
    """
    # If log_file is not an absolute path, join with LOG_DIR
    if not os.path.isabs(log_file):
        log_file = os.path.join(LOG_DIR, log_file)

    # Ensure the log directory exists
    log_dir = os.path.dirname(log_file)
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir, exist_ok=True)

    logger = logging.getLogger(job_name)
    logger.setLevel(logging.INFO)

    # Avoid adding duplicate handlers
    if not logger.handlers:
        file_handler = logging.FileHandler(log_file)
        stream_handler = logging.StreamHandler()

        formatter = JobNameFormatter("%(asctime)s - %(levelname)s - %(message)s")
        file_handler.setFormatter(formatter)
        stream_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        logger.addHandler(stream_handler)

    # Use a LoggerAdapter to inject the job name
    return logging.LoggerAdapter(logger, {"job_name": job_name})

def timestamp():
    return datetime.now().strftime('%Y%m%d_%H%M%S')

def ensure_dir(path):
    os.makedirs(path, exist_ok=True)

def sizeof_fmt(num, suffix="B"):
    """Formats file sizes."""
    for unit in ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"]:
        if abs(num) < 1024.0:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f}Yi{suffix}"

def trim_log_file(log_path, max_lines):
    try:
        if not os.path.exists(log_path):
            return
        with open(log_path, 'r') as f:
            lines = f.readlines()
        if len(lines) > max_lines:
            lines_to_keep = lines[-max_lines:]
            with open(log_path, 'w') as f:
                f.writelines(lines_to_keep)
    except Exception as e:
        print(f"Error trimming log file {log_path}: {e}")

def trim_all_logs():
    for log_file in glob.glob(os.path.join(LOG_DIR, "*.log")):
        trim_log_file(log_file, MAX_LOG_LINES)
