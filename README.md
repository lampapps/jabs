# JABS - Just Another Backup Script

A Python-based backup utility for creating local and cloud backups with a web dashboard, scheduler, or command line interface.

## Features

* **Multiple Backup Types:** Full, incremental, differential and dryrun backups
* **Local & S3 Storage:** Backs up locally with optional S3 sync
* **Backup Rotation:** Automatically manages backup sets retention
* **Email notifications:** Receive email of job errors and summary of jobs run
* **Web Dashboard:**
  * View backup events and status
  * Monitor disk usage and S3 storage
  * Schedule backup jobs
  * Run manual backups
  * Restore files or entire backup sets
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

**Setup:**

```bash
# Clone repository
git clone https://github.com/lampapps/jabs.git jabs
cd jabs

# Run setup script
chmod +x setup.sh
./setup.sh
```

**Launch Dashboard:**

```bash
python3 run.py
```

**Configuration:**

* Configure global settings in `config/global.yaml`
* Create job configurations in `config/jobs/` (see templates)
* Set encryption passphrase
* Set smtp credentials (email and password)

**Run Backups:**

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
