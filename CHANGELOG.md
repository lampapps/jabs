
## [0.5.0]

### Added
- Enhanced exclude file/folder pattern handling

### Fixed
- backup error due to broken links and unaccessible files


## [0.4.2] - 2025-05-30

### Added
- Change log
- Digest mode for email notifications

### Fixed
- Removed duplicate logging function resulting in inconsistent logs and errors
- Cleaned up code using pylint

## [0.4.0] - 2025-05-01

### Added
- Full & differential backups
- gzip compression for the tar archives
- Configurable archive size limits
- Local & S3 archive storage
- Unix shell-style glob patterns for excluding files from backup
- Encryption (GPG)
- Email notifications
- Backup rotation
- Scheduling for unattended backups
- Web based dashboard for monitoring, manual backups, restore, and configuration
- Full or partial restore to original or other directory
- Command line interface for backups
- Python restore script saved with offsite backups
- HTML manifest saved with offsite backups
