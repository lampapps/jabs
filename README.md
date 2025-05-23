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
    *   Monitor local disk usage and S3 bucket/prefix usage (requires `drives` and `s3_buckets` to be set in `config/global.yaml`).
    *   View backup job configurations.
    *   View application logs (`scheduler.log`, `backup.log`).
    *   Monitor scheduler status (heartbeat).
    *   **Restore files or entire backup sets directly from the web dashboard.**
*   **Manifest Files:** Exports an independent HTML manifest for each backup set listing all files in all archives. The manifest also provides the backup job's configuration settings and a list of all archives in the set. This manifest is stored with the backup set and synced to AWS.
*   **Encryption:** Optionally encrypts each tarball using GPG with a passphrase. Encrypted archives have a `.gpg` extension and can only be restored with the correct passphrase.
*   **Restore Utility:** Each backup set includes a `restore.py` script that can be used to restore files or directories from the backup set, including handling encrypted archives.

## Installation

1.  **Prerequisites:**
    *   Python 3.7+
    *   pip (Python package installer)
    *   `awscli` (AWS Command Line Interface) configured, if using S3 sync.
    *   `gpg` (GNU Privacy Guard) for encryption/decryption (optional, but required for encrypted backups).

2.  **Clone the Repository:**
    ```bash
    git clone <your-repository-url> jabs
    cd jabs
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

- Launch the Flask dashboard (`python3 run.py`).
- Navigate to the **"Jobs"**  section in the dashboard.
- You can:
    - Create new backup jobs using provided templates.
    - Edit existing job configurations (sources, destinations, schedules, encryption, etc.).
    - Set up S3 sync, backup rotation, and encryption options.
    - Manage global settings such as monitored drives and S3 buckets.

#### **Drive/S3 Monitoring (Optional):**
- Add `drives` and `s3_buckets` to your `config/global.yaml` to configure which local drives and S3 buckets are monitored on the dashboard.

## Encryption Passphrase Setup

If you enable encryption for your backups, JABS uses a passphrase to encrypt and decrypt your tarballs using GPG.  

### **How to Set or Update the Encryption Passphrase**

- Go to the **Config** page in the web dashboard.
- Use the **"Set encryption passphrase"** form to set or update the passphrase.
- This will securely update the `JABS_ENCRYPT_PASSPHRASE` value in your `.env` file automatically.
- The dashboard will indicate whether a passphrase is currently set.

**Note:**  
- Never commit your `.env` file with sensitive passphrases to version control.
- The passphrase is required to restore encrypted backups.
- If you change the passphrase, you will need the old passphrase to restore backups made with it.

## Restoring Backups

JABS provides **two main ways to restore files or directories** from your backups:

### **1. Restore Using the Web Dashboard**

- The web dashboard allows you to restore individual files, directories, or entire backup sets directly from your browser.
- The dashboard uses the manifest to let you browse and select files to restore.
- Handles both encrypted and unencrypted archives.

### **2. Restore Using the Included `restore.py` Script**

- Each backup set includes a `restore.py` script.
- You can run this script from the command line to restore specific files, directories, or the entire backup set.
- The script automatically detects encrypted archives and prompts for the passphrase if needed.
- Example usage:
    ```bash
    python3 restore.py
    ```

## Directory Structure

```
jabs/
├── app/
│   ├── __init__.py
│   ├── routes/
|   ├── utils/
│   ├── settings.py
│   ├── static/
│   │   ├── css/
│   │   └── js/
│   └── templates/
├── config/
│   ├── global.yaml
│   └── jobs/
|       ├── templates/
│       └── (your job .yaml files)
├── core/
│   ├── backup.py
│   ├── sync_s3.py
|   └── encrypt.py
├── data/
│   ├── dashboard/
│   │   └── events.json
│   └── manifests/
│       └── (job_name/backup_set_xxx.json)
├── logs/
├── venv/
├── cli.py
├── requirements.txt
├── restore.sh
├── run.py
└── scheduler.py

# Structure of a local destination folder and AWS Bucket
(destination_path)/ | s3://<bucket>/
    ├──(machine_name)/
    |   ├── (job_name)/
    |   |    ├── backup_set_YYYYMMDD_HHMMSS/
    |   |    │   ├── full_part_1_YYYYMMDD_HHMMSS.tar.gz
    |   |    │   ├── full_part_2_YYYYMMDD_HHMMSS.tar.gz
    |   |    │   ├── ... (other tarballs)
    |   |    │   ├── restore.py    
    |   |    │   └── manifest_YYYYMMDD_HHMMSS.html
    |   |    ├── ... (other backup_sets)
    |   |    └── last_full.txt
    |   └── ... (other jobs on same machine_name)
    └── ...
```
## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.


