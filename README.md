# JABS - Just Another Backup Script

JABS is a Python-based backup utility designed for creating local and cloud (AWS S3) backups with scheduling capabilities and a web-based monitoring dashboard.

## Features

*   **YAML Configuration:** Define backup jobs, sources, destinations, exclusions, and schedules using simple YAML files.
*   **Full & Differential Backups:** Supports both full backups and differential backups (based on modification time since the last full backup).
*   **Independent Tarballs:** Automatically starts a new archive (tarball) when the max size limit is reached, creating independent tar archives for easier partial restores.
*   **Local & S3 Storage:** Backs up to a local destination and optionally syncs to AWS S3.
*   **Backup Rotation:** Automatically rotates local backup sets, keeping a specified number of recent sets.
*   **Scheduling:** Includes a scheduler script (`scheduler.py`) designed to be run via cron to trigger backups based on cron expressions defined in the config files.
*   **Web Dashboard (Flask):**
    *   View recent backup events (status, runtime, type).
    *   Monitor local disk usage and S3 bucket/prefix usage (requires `config/drives.yaml`).
    *   View backup job configurations.
    *   View application logs (`scheduler.log`, `backup.log`).
    *   Monitor scheduler status (heartbeat).
*   **Manifest Files:** Exports an independent HTML manifest for each backup set listing all files in all archives. The manifest also provides the backup job's configuration settings and a list of all archives in the set. This manifest is stored with the backup set and synced to AWS.
*   **Encryption:** Optionally encrypts each tarball using GPG with a passphrase. Encrypted archives have a `.gpg` extension and can only be restored with the correct passphrase.

## Installation

1.  **Prerequisites:**
    *   Python 3.7+
    *   pip (Python package installer)
    *   `awscli` (AWS Command Line Interface) configured, if using S3 sync.
    *   `gpg` (GNU Privacy Guard) for encryption/decryption (optional, but required for encrypted backups).

2.  **Clone the Repository:**
    ```bash
    git clone <your-repository-url> jabs4
    cd jabs4
    ```

3.  **Set up Virtual Environment (Recommended):**
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    ```

4.  **Install Dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

5.  **AWS Configuration (Optional):**
    If you plan to use S3 sync, ensure your AWS CLI is configured with the necessary credentials and default region, or specify a profile in your job config files.
    ```bash
    aws configure
    ```

## Configuration

1.  **Job Configuration (`config/*.yaml`):**
    *   Create one `.yaml` file in the `config/` directory for each backup job.
    *   Copy and modify [`config/backup_example.yaml`](config/backup_example.yaml) as a template.
    *   Key fields:
        *   `job_name`: Unique name for the job.
        *   `source`: Absolute path to the directory to back up.
        *   `destination`: Absolute path to the *parent* directory for local backups (job-specific folder will be created inside).
        *   `keep_sets`: Number of backup sets to retain locally.
        *   `max_tarball_size`: Max size (MB) for individual tar archives.
        *   `exclude`: List of glob patterns for files/directories to exclude.
        *   `aws`: S3 configuration (`profile`, `region`, `bucket`, `prefix`).
        *   `schedules`: List of schedules with `cron` expression, `type` (full/diff), `sync` (true/false), and `enabled` (true/false).
        *   `encryption`: (Optional) Enable encryption for tarballs. Example:
            ```yaml
            encryption:
              enabled: true
              method: gpg
              passphrase_env: JABS_ENCRYPT_PASSPHRASE
            ```
            - Set `enabled: true` to turn on encryption.
            - `method: gpg` uses GPG symmetric encryption.
            - `passphrase_env` is the name of the environment variable holding your GPG passphrase.

2.  **Drive/S3 Monitoring (`config/drives.yaml`):**
    *   (Optional) Create `config/drives.yaml` to configure which local drives and S3 buckets are monitored on the dashboard.
    *   Example structure:
        ```yaml
        drives:
          - /
          - /media/backupdrive
        s3_buckets:
          - my-jabs-bucket-1
          - another-s3-bucket
        ```

## Running JABS

1.  **Web Interface:**
    *   Start the Flask development server:
        ```bash
        python run.py
        ```
    *   Access the dashboard in your browser (usually `http://localhost:5000` or `http://<your-server-ip>:5000`).
    *   For production, use a proper WSGI server like Gunicorn or Waitress.

2.  **Manual Backups (CLI):**
    *   Run a specific job using `cli.py`:
        ```bash
        # Full backup
        python cli.py config/my_job.yaml --full

        # Differential backup with S3 sync
        python cli.py config/my_job.yaml --diff --sync

        # Encrypted backup (if enabled in config)
        JABS_ENCRYPT_PASSPHRASE='yourpassphrase' python cli.py config/my_job.yaml --full
        ```
        * If encryption is enabled, set the passphrase environment variable before running the backup.

3.  **Scheduled Backups (Cron):**
    *   Add the `scheduler.py` script to your system's crontab.
    *   Edit the crontab: `crontab -e`
    *   Add a line similar to this (adjust paths for your setup), assuming the scheduler should check every minute:
        ```crontab
        * * * * * /path/to/your/jabs4/venv/bin/python /path/to/your/jabs4/scheduler.py >> /path/to/your/jabs4/logs/cron.log 2>&1
        ```
        *   `* * * * *`: Run every minute. Adjust as needed (e.g., `0 * * * *` for hourly).
        *   `/path/to/your/jabs4/venv/bin/python`: Absolute path to the Python interpreter in your virtual environment.
        *   `/path/to/your/jabs4/scheduler.py`: Absolute path to the scheduler script.
        *   `>> /path/to/your/jabs4/logs/cron.log 2>&1`: (Optional) Redirect cron output to a log file.

## Restoring Backups

JABS creates standard `.tar.gz` archives, optionally encrypted as `.tar.gz.gpg`, allowing you to restore files using common operating system tools like `tar` and `gpg`. The process depends on whether you are restoring from a full backup or applying differentials.

**Using the Manifest:**

*   Each backup set includes an HTML manifest. This manifest is also synced to AWS. A local copy of the manifest is also linked from the dashboard.
*   These manifests list every file included in the backup set and which specific `.tar.gz` archive contains that file.
*   Use the manifest to identify which tarball(s) you need if you only want to restore specific files or directories.

**1. Restoring a Full Backup Set:**

*   Identify the directory of the full backup set you want to restore (e.g., `/path/to/storage/my-job-name/backup_set_YYYYMMDD_HHMMSS`).
*   Navigate to a *temporary* or *target* directory where you want to restore the files.
*   Extract all `.tar.gz` files from the backup set directory into your current location. The order usually doesn't matter for a full backup.

    ```bash
    # Example: Restore all tarballs from a specific full backup set
    cd /path/to/restore/location/
    find /path/to/jabs4/my-job-name/backup_set_YYYYMMDD_HHMMSS -name '*.tar.gz' -exec tar -xzvf {} \;
    ```

**2. Restoring from a Differential Backup:**

*   **IMPORTANT:** You MUST first restore the corresponding **Full Backup** set that the differential is based on (see step 1).
*   Identify the directory of the differential backup set you want to apply (e.g., `/path/to/my-job-name/backup_set_YYYYMMDD_HHMMSS_diff`).
*   Ensure you are in the *same target directory* where you restored the full backup.
*   Extract all `.tar.gz` files from the differential backup set directory. This will overwrite any files that were modified since the full backup.

    ```bash
    # Example: Apply a differential backup AFTER restoring the full backup
    cd /path/to/restore/location/
    find /media/backupdrive/jabs4/my-job-name/backup_set_YYYYMMDD_HHMMSS_diff -name '*.tar.gz' -exec tar -xzvf {} \;
    ```

    *   If you need to apply multiple differentials, apply them in chronological order after restoring the base full backup.

**3. Restoring Encrypted Archives:**

*   If your backup set contains `.tar.gz.gpg` files, you must decrypt them before extracting.
*   You can use the provided `restore.sh` script or do it manually:

    ```bash
    # Decrypt an archive (you will be prompted for the passphrase)
    gpg --output archive.tar.gz --decrypt archive.tar.gz.gpg

    # Then extract as usual
    tar -xzvf archive.tar.gz
    ```

*   To restore all encrypted and unencrypted archives in a folder, use the provided script:

    ```bash
    chmod +x restore.sh
    ./restore.sh
    ```

    The script will list all archives, let you choose to restore or decrypt, and prompt for the passphrase if needed.

**4. Restoring Specific Files or Directories:**

*   Use the manifest (HTML or JSON) for the desired backup set (full or differential) to find the exact `.tar.gz` file(s) containing the specific file(s) or directory you need.
*   Locate the required tarball(s) in the backup set directory.
*   If encrypted, decrypt first as above.
*   Use the `tar` command, specifying the archive and the exact path(s) of the file(s)/directory(ies) inside the archive you want to extract.

    ```bash
    # Example: Restore a specific file from a single tarball
    cd /path/to/restore/location/
    tar -xzvf /media/backupdrive/jabs4/my-job-name/backup_set_YYYYMMDD_HHMMSS/001_archive.tar.gz path/inside/archive/to/your/file.txt

    # Example: Restore a specific directory from a single tarball
    tar -xzvf /media/backupdrive/jabs4/my-job-name/backup_set_YYYYMMDD_HHMMSS/002_archive.tar.gz path/inside/archive/to/your/directory/
    ```

    *   If restoring a specific file from a *differential* backup, it represents the state of the file *at the time of that differential backup*.

## Directory Structure

```
jabs4/
├── app/
│   ├── __init__.py
│   ├── routes/
│   │   └── dashboard.py
│   ├── settings.py
│   ├── static/
│   │   ├── css/
│   │   │   └── custom.css
│   │   └── js/
│   │       ├── dashboard.js
│   │       └── global.js
│   └── templates/
│       ├── base.html
│       ├── index.html
│       ├── config.html
│       ├── jobs.html
│       ├── logs.html
│       ├── help.html
│       ├── manifest.html
│       ├── manifest_archived.html
│       └── edit_config.html
├── config/
│   ├── default.yaml
│   ├── drives.yaml
│   └── (your job .yaml files)
├── core/
│   ├── backup.py
│   └── sync_s3.py
├── data/
│   ├── dashboard/
│   │   └── events.json
│   └── manifests/
│       └── (job_name/backup_set_xxx.json)
├── logs/
│   ├── backup.log
│   ├── scheduler.log
│   └── scheduler.status
├── utils/
│   ├── event_logger.py
│   ├── logger.py
│   └── manifest.py
├── venv/
├── cli.py
├── requirements.txt
├── restore.sh
├── run.py
├── scheduler.py


# Structure of a local destination folder (for each backup job)
(destination_path)/
└── (job_name)/
    ├── backup_set_YYYYMMDD_HHMMSS/
    │   ├── full_part_1_YYYYMMDD_HHMMSS.tar.gz
    │   ├── full_part_2_YYYYMMDD_HHMMSS.tar.gz
    │   ├── ... (other tarballs)
    │   └── manifest_YYYYMMDD_HHMMSS.html
    ├── last_full.txt
    └── ... (other backup_set_*/ folders)


```




## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.


