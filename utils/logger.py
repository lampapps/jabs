# /utils/logger.py
import logging
import os
from datetime import datetime

class JobNameFormatter(logging.Formatter):
    """Custom formatter to include the job name in every log message."""
    def format(self, record):
        if hasattr(record, "job_name"):
            record.msg = f"{record.job_name} - {record.msg}"
        return super().format(record)

def setup_logger(job_name, log_file="logs/backup.log"):
    """
    Set up a logger with the job name included in every message.
    :param job_name: Name of the job to include in log messages.
    :param log_file: Path to the log file.
    :return: A logger instance.
    """
    logger = logging.getLogger(job_name)
    logger.setLevel(logging.INFO)

    # Avoid adding duplicate handlers
    if not logger.handlers:
        # Create handlers
        file_handler = logging.FileHandler(log_file)
        stream_handler = logging.StreamHandler()

        # Create formatter and add it to the handlers
        formatter = JobNameFormatter("%(asctime)s - %(levelname)s - %(message)s")
        file_handler.setFormatter(formatter)
        stream_handler.setFormatter(formatter)

        # Add handlers to the logger
        logger.addHandler(file_handler)
        logger.addHandler(stream_handler)

    # Use a LoggerAdapter to inject the job name
    return logging.LoggerAdapter(logger, {"job_name": job_name})

def timestamp():
    return datetime.now().strftime('%Y%m%d_%H%M%S')

def ensure_dir(path):
    os.makedirs(path, exist_ok=True)