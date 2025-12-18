# JABS - Just Another Backup Script

A Python-based backup utility for creating local and cloud backups with a web dashboard, scheduler, or command line interface.

## Features

* **Multiple Backup Types:** Full, incremental, differential and dryrun backups
* **Local & S3 Storage:** Backs up locally with optional S3 sync
* **Backup Rotation:** Automatically manages backup sets retention
* **Email notifications:** Receive email of job errors and summary of jobs run
* **Web Dashboard:**
  * Auto-discover and monitor multiple JABS instances on your network
  * Real-time status monitoring with error detection and color-coded badges
  * View backup events and status with advanced filtering
  * Monitor disk usage and S3 storage with interactive charts
  * Schedule backup jobs with cron-style scheduling
  * Run manual backups and restore operations
  * Access logs and scheduler status
* **Disater Recover:** Each backup set has a manifest and restore script
* **Encryption:** Optional GPG encryption for archives
* **YAML Configuration:** Simple setup for jobs, sources, and schedules

## Requirements

* Python 3.12+
* pip (Python package installer)
* `python3.12-venv` module
* `awscli` (optional, for S3 sync)
* `gpg` (optional, for encryption)

## Quick Start

**Setup and Launch:**

```bash
# Clone repository
git clone https://github.com/lampapps/jabs.git jabs
cd jabs

# Setup and start dashboard (includes validation and dependency checks)
chmod +x jabs.sh
bash jabs.sh start
```

**jabs.sh Script Commands:**

```bash
# Start the web dashboard
bash jabs.sh start

# Stop the dashboard
bash jabs.sh stop

# Restart the dashboard
bash jabs.sh restart

# Check dashboard status
bash jabs.sh status

# View dashboard logs
bash jabs.sh logs

# Validate setup (check dependencies, permissions, config)
bash jabs.sh validate

# Update JABS from GitHub (with confirmation)
bash jabs.sh update

# Force update JABS from GitHub (automatic conflict resolution)
bash jabs.sh force-update

# Fix update conflicts manually
bash jabs.sh fix-update

# Show script help
bash jabs.sh help
```

The `jabs.sh` script provides convenient commands to manage the web dashboard service. The dashboard will be available at `http://localhost:5000` (production) or `http://localhost:5001` (development) and includes:

* **Monitor Page:** Auto-discover and monitor multiple JABS instances across your network
* **Real-time Status:** View CRON (CLI) and Web interface status with error detection
* **Smart Badges:** Color-coded status indicators with detailed tooltips
* **Instance Management:** Grace period editing and remote access to web interfaces

**Configuration:**

* Configure global settings in `config/global.yaml`
* Create job configurations in `config/jobs/` (see templates)
* Set encryption passphrase
* Set smtp credentials (email and password)

**Manully Run Backups via CLI:**

```bash
# Start a backup job
python3 cli.py --job myjob --type full

# Options: full, incremental, differential
# Add --encrypt or --sync as needed
```

**Schedule Backups:**

```bash
# Add to crontab
crontab -e

# this example will run scheduler every hour
0 * * * * /path/to/jabs/venv/bin/python3 /path/to/jabs/scheduler.py
```

## Storage Structure

```text
(destination_path/ | s3://<bucket>/)
└── (machine_name)/
    └── (job_name)/
        └── backup_set_YYYYMMDD_HHMMSS/
            ├── full_part_1_YYYYMMDD_HHMMSS.tar.gz[.gpg]
            ├── ... (other tarballs)
            ├── restore.py
            └── manifest_YYYYMMDD_HHMMSS.html
```

## License

This project is licensed under the MIT License - see the [LICENSE](license) file for details.

## Credits

This project uses the following open source libraries:

* [Chart.js](https://www.chartjs.org/) - MIT License
* [chartjs-plugin-datalabels](https://chartjs-plugin-datalabels.netlify.app/) - MIT License
* [DataTables.js](https://datatables.net/) - MIT License
* [Bootstrap](https://getbootstrap.com/) - MIT License
* [Font Awesome](https://fontawesome.com/) - CC BY 4.0 License
* [SortableJS](https://sortablejs.github.io/Sortable/) - MIT License
* [Shields.io](https://shields.io/) - CC0 License

See each library's website for full license details.
