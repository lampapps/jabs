import os
import shutil
import glob
import subprocess
import yaml

def remove_pycache_dirs(root_dir):
    for dirpath, dirnames, filenames in os.walk(root_dir):
        if "__pycache__" in dirnames:
            pycache_path = os.path.join(dirpath, "__pycache__")
            print(f"Removing: {pycache_path}")
            shutil.rmtree(pycache_path, ignore_errors=True)

def remove_files_by_pattern(pattern):
    for file_path in glob.glob(pattern):
        if os.path.isfile(file_path):
            print(f"Removing: {file_path}")
            os.remove(file_path)

def remove_dir_contents(directory):
    if os.path.isdir(directory):
        for entry in os.listdir(directory):
            entry_path = os.path.join(directory, entry)
            if os.path.isfile(entry_path) or os.path.islink(entry_path):
                print(f"Removing: {entry_path}")
                os.remove(entry_path)
            elif os.path.isdir(entry_path):
                print(f"Removing: {entry_path}")
                shutil.rmtree(entry_path, ignore_errors=True)

def remove_backups_from_destinations_and_s3(script_dir):
    config_dir = os.path.join(script_dir, "config")
    yaml_files = glob.glob(os.path.join(config_dir, "*.yaml"))
    s3_buckets = set()
    s3_prefixes = dict()
    for yml in yaml_files:
        with open(yml, "r") as f:
            try:
                config = yaml.safe_load(f)
            except Exception:
                continue
        # Remove local destination backups
        dest = config.get("destination")
        if dest and os.path.isdir(dest):
            print(f"Removing all backups from local destination: {dest}")
            remove_dir_contents(dest)
        # Remove S3 backups if bucket is specified
        aws = config.get("aws", {})
        bucket = aws.get("bucket") or config.get("bucket")
        prefix = aws.get("prefix") or config.get("prefix", "")
        if bucket:
            s3_buckets.add(bucket)
            s3_prefixes[bucket] = prefix

    # Remove from S3 using awscli
    for bucket in s3_buckets:
        prefix = s3_prefixes.get(bucket, "")
        s3_path = f"s3://{bucket}/{prefix}".rstrip("/")
        print(f"Removing all backups from S3: {s3_path}")
        try:
            subprocess.run(["aws", "s3", "rm", s3_path, "--recursive"], check=True)
        except Exception as e:
            print(f"Failed to remove S3 backups: {e}")

def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Remove all __pycache__ directories in this folder only
    remove_pycache_dirs(script_dir)

    # Remove all files in logs/ (only if logs/ is in this folder)
    remove_dir_contents(os.path.join(script_dir, "logs"))

    # Remove all files in locks/ (only if locks/ is in this folder)
    remove_dir_contents(os.path.join(script_dir, "locks"))

    # Remove all manifest files in data/manifests/ (only if data/manifests/ is in this folder)
    remove_files_by_pattern(os.path.join(script_dir, "data", "manifests", "*.*"))

    # Remove dashboard events.json (only if data/dashboard/ is in this folder)
    events_json = os.path.join(script_dir, "data", "dashboard", "events.json")
    if os.path.exists(events_json):
        print(f"Removing: {events_json}")
        os.remove(events_json)

    # Remove all backups from the destination folders and S3 buckets listed in config/*.yaml
    remove_backups_from_destinations_and_s3(script_dir)

    print("JABS app has been fully reset (in this folder, all configured destinations, and remote S3 buckets).")

if __name__ == "__main__":
    main()