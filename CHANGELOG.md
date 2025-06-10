
## To do
- improve setup.sh: needs to activate venv after install and then run.py
- fix scheduler file not found in new installs
- Periodic refresh badges and monitor.html
- Edit external badges (cronitor) from inside flask

## v0.7.2.beta2

### Fixed
- Error if AWS credentials are not set. Now gracefully returns prompt.

## v0.7.1

### Added
- Monitoring other JABS apps on the same network. 

### Fixed
- error on fresh install, check for existence of email_digest_queue.json before writing
- Title tag includes machine name
- Double email notifications for diff backup when no modified files found

## v0.6.0

### Added
- Security page to handle encryption passphrase and credentials for smtp
- Schedule heartbeat chart on index.html, removed the chart from Schedule page and renamed all references.

### Fixed
- Full restore logic fixed so only latest diff backups is restore after the full backup
- Partial restore logic fixed so only the file from the requested archive is restored. Not the file from all archives in the set
- Improved emails notification logic and formatting
- Cleanup of lock files

## v0.5.1

### Added
- Summary in Digest email
- Manifest_archived loading spinner

### Fixed
- edit_config save button not returning to correct page
- Manifest configuration view not showing correct setting for use_common_exclude
- Misc style and formatting for exclude pattern handling
- Reduced width of Documentation and Change Log pages to match other pages

## v0.5.0

### Added
- Enhanced exclude file/folder pattern handling

### Fixed
- backup error due to broken links and unaccessible files


## v0.4.2 - 2025-05-30

### Added
- Change log
- Digest mode for email notifications

### Fixed
- Removed duplicate logging function resulting in inconsistent logs and errors
- Cleaned up code using pylint

## v0.1.0 - 2025-04-01

### Added
- Full & differential backups
- gzip compression for the tar archives
- Configurable archive size limits
- Local & S3 archive storage
- Unix shell-style glob patterns for excluding files and directories from backup
- GPG Encryption
- Email notifications
- Backup rotation
- Scheduling for unattended backups and email notifications
- Web based dashboard for monitoring, manual backups, restore, and configuration
- Web based full and partial restore to original or other directory
- Command line interface for backups
- Python restore script saved with offsite backups
- HTML searchable backup manifest saved with offsite backups
