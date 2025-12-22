<!-- markdownlint-disable MD024 MD041 MD007 -->
## To do

- reformat emails
- improve dryrun logging, check but skip sync, skip rotate, and disable restore in manifest
- add example backup.job config file
- change "error loading S3 Usage data" to Credentials not found (better error handling)
- Update/improve scheduler config page
- Update/improve AWS credential setup on Security page, remove aws config option
- Fix errors in terminal when AWS credetials are missing or incorrect.
- Backup Archives in set on the manifest page lists only first 90 parts.


## v0.9.3 - 2025-12-22

### Fixed
 
 - fixed scheduler.py to restore broken functionality to the shared monitor .json file

## v0.9.2 - 2025-12-22

### Fixed
 
 - fixed scheduler.py to handle monitor correctly

## v0.9.1 - 2025-12-21

### Fixed
 
 - Monitoring logic fixes, moved all montoring settings to global.yaml

## v0.9.0 - 2025-12-18

### Added

 - Refactored the monitor page to auto detect JABS instances on the configured network
 - Updated badges on the dashboard to work with refactored monitoing

### Fixed

 - Fixed duplicate server log created by jabs.sh

## v0.8.5 - 25-12-02

### Added

- Refactor setup process: remove old setup.sh, add jabs.sh with enhanced management routines and logging

## v0.8.4 - 25-11-05

### Fixed

- Moved development mode flag back to .env

## v0.8.3 - 25-09-29

### Fixed

- Monitor status not properly rendering if target instance flask app was not running.

## v0.8.2 - 25-09-29

### Fixed

- Daily digest email was not firing as scheduled.

## v0.8.1

### Added

- Adjust setup script to require Python 3.11

## v0.8.0

### Added

- Changed from json data files to SQLite
- Allowed AWS credentials to be added to enviremental variables so app can be run under root user

### Fixed

- Refactured logic flow to improve code readabilty
- Many fixes due to refacturing for upgrade to SQLite.

## v0.7.3

### Fixed

- Removed Auto theme setting
- Fixed badges alignment and switched to shields.io for badge generation.

## v0.7.2

### Fixed

- Error if AWS credentials are not set. Now gracefully returns prompt.
- setup.sh to handle renaming example configuration files if needed

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
