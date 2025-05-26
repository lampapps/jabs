"""Helpers for loading and finding job config files for the dashboard."""

import os
import yaml
from app.settings import JOBS_DIR

def find_config_path_by_job_name(target_job_name):
    """Find the path to a job config file by its job_name."""
    if not os.path.isdir(JOBS_DIR):
        print(f"Error: Jobs directory not found at {JOBS_DIR}")
        return None
    for filename in os.listdir(JOBS_DIR):
        if filename.endswith((".yaml", ".yml")):
            file_path = os.path.join(JOBS_DIR, filename)
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    config_data = yaml.safe_load(f)
                    if isinstance(config_data, dict) and config_data.get('job_name') == target_job_name:
                        return file_path
            except yaml.YAMLError:
                print(f"Warning: Could not parse YAML file {filename}")
                continue
            except Exception as e:  # pylint: disable=broad-except
                print(f"Warning: Error reading file {filename}: {e}")
                continue
    return None

def load_config(config_path):
    """Load a YAML config file from the given path."""
    if not config_path or not os.path.exists(config_path):
        return None
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    except Exception as e:  # pylint: disable=broad-except
        print(f"Error loading config file {config_path}: {e}")
        return None