"""Database operations for backup set records in JABS.

Provides functions to create, retrieve, update, delete, and rotate backup sets and their associated jobs and files.
"""
import socket
import time
import sqlite3
import logging
from typing import List, Dict, Optional
from app.models.db_core import get_db_connection

def get_or_create_backup_set(job_name: str, set_name: str, config_settings: Optional[str] = None, source_path: Optional[str] = None) -> int:
    """Get existing backup set or create new one if it doesn't exist."""
    with get_db_connection() as conn:
        c = conn.cursor()

        # Try to get existing backup set
        c.execute("SELECT id FROM backup_sets WHERE job_name = ? AND set_name = ?", (job_name, set_name))
        row = c.fetchone()

        if row:
            # Update the updated_at timestamp
            c.execute("UPDATE backup_sets SET updated_at = ? WHERE id = ?", (time.time(), row['id']))
            conn.commit()
            return row['id']
        else:
            # Create new backup set
            current_time = time.time()
            hostname = socket.gethostname()
            c.execute("""
                INSERT INTO backup_sets (job_name, set_name, created_at, updated_at, config_snapshot, source_path, hostname)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (job_name, set_name, current_time, current_time, config_settings, source_path, hostname))
            conn.commit()
            return c.lastrowid

def get_backup_set_by_job_and_set(job_name: str, set_name: str) -> Optional[sqlite3.Row]:
    """Get a backup set by job_name and set_name."""
    with get_db_connection() as conn:
        c = conn.cursor()
        c.execute("SELECT * FROM backup_sets WHERE job_name = ? AND set_name = ?", (job_name, set_name))
        return c.fetchone()

def get_backup_set(set_id: int) -> Optional[sqlite3.Row]:
    """Get a backup set by numeric ID."""
    with get_db_connection() as conn:
        c = conn.cursor()
        c.execute("SELECT * FROM backup_sets WHERE id = ?", (set_id,))
        return c.fetchone()

def list_backup_sets(job_name: Optional[str] = None, limit: int = 20) -> List[sqlite3.Row]:
    """List backup sets, optionally filtered by job_name."""
    with get_db_connection() as conn:
        c = conn.cursor()
        if job_name:
            c.execute("""
                SELECT * FROM backup_sets 
                WHERE job_name = ?
                ORDER BY created_at DESC LIMIT ?
            """, (job_name, limit))
        else:
            c.execute("""
                SELECT * FROM backup_sets 
                ORDER BY created_at DESC LIMIT ?
            """, (limit,))
        return c.fetchall()

def delete_backup_set(set_id: int) -> bool:
    """
    Delete a backup set and all its associated data.
    
    This deletes:
    1. All backup files associated with jobs in this set
    2. All backup jobs in this set
    3. The backup set itself
    
    Returns:
        True if deletion was successful, False otherwise
    """
    logger = logging.getLogger("app")

    logger.info(f"Deleting backup set with ID {set_id} and all related records")

    try:
        with get_db_connection() as conn:
            c = conn.cursor()

            # First, check that the backup set exists
            c.execute("SELECT id, job_name, set_name FROM backup_sets WHERE id = ?", (set_id,))
            backup_set = c.fetchone()
            if not backup_set:
                logger.error(f"Backup set with ID {set_id} not found in database")
                return False

            logger.debug(f"Found backup set: {dict(backup_set)}")

            # Count how many backup jobs are associated with this set
            c.execute("SELECT COUNT(*) FROM backup_jobs WHERE backup_set_id = ?", (set_id,))
            job_count = c.fetchone()[0]

            # Count how many backup files are associated with this set
            c.execute("""
                SELECT COUNT(*) FROM backup_files 
                WHERE backup_job_id IN (
                    SELECT id FROM backup_jobs WHERE backup_set_id = ?
                )
            """, (set_id,))
            file_count = c.fetchone()[0]

            logger.info(f"About to delete {job_count} job(s) and {file_count} file record(s) for backup set {set_id}")

            # Delete all related backup files
            c.execute("""
                DELETE FROM backup_files 
                WHERE backup_job_id IN (
                    SELECT id FROM backup_jobs WHERE backup_set_id = ?
                )
            """, (set_id,))
            files_deleted = c.rowcount

            # Then, delete all backup jobs
            c.execute("DELETE FROM backup_jobs WHERE backup_set_id = ?", (set_id,))
            jobs_deleted = c.rowcount

            # Finally, delete the backup set
            c.execute("DELETE FROM backup_sets WHERE id = ?", (set_id,))
            sets_deleted = c.rowcount

            conn.commit()

            logger.info(f"Successfully deleted backup set {set_id}: {sets_deleted} set(s), {jobs_deleted} job(s), {files_deleted} file record(s)")
            return True
    except Exception as e:
        logger.error(f"Failed to delete backup set {set_id}: {e}", exc_info=True)
        return False

def set_backup_set_config(backup_set_id: int, config_settings: str) -> bool:
    """Set the config snapshot for an existing backup set."""
    logger = logging.getLogger("app")

    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            c.execute(
                "UPDATE backup_sets SET config_snapshot = ? WHERE id = ?",
                (config_settings, backup_set_id)
            )
            conn.commit()
            return True
    except Exception as e:
        logger.error(f"Failed to set config snapshot: {e}")
        return False

def rotate_backup_sets_in_db(job_name: str, keep_sets: int) -> Dict[str, int]:
    """
    Rotate backup sets in the database, keeping only the latest N sets for a job.
    
    Args:
        job_name: Name of the job to rotate backup sets for
        keep_sets: Number of backup sets to keep
        
    Returns:
        Dictionary with counts of deleted records:
        {
            'sets_deleted': number of backup sets deleted,
            'jobs_deleted': number of backup jobs deleted,
            'files_deleted': number of backup files deleted
        }
    """

    logger = logging.getLogger("app")

    logger.info(f"Rotating backup sets in database for job '{job_name}', keeping {keep_sets} sets")
    result = {'sets_deleted': 0, 'jobs_deleted': 0, 'files_deleted': 0}

    try:
        with get_db_connection() as conn:
            c = conn.cursor()

            # Get all backup sets for this job, sorted by creation time (newest first)
            c.execute("""
                SELECT id, set_name, created_at
                FROM backup_sets
                WHERE job_name = ?
                ORDER BY created_at DESC
            """, (job_name,))
            all_sets = c.fetchall()

            if not all_sets:
                logger.info(f"No backup sets found in database for job '{job_name}'")
                return result

            logger.info(f"Found {len(all_sets)} backup sets in database for job '{job_name}'")

            # If we have more sets than we want to keep, delete the oldest
            if len(all_sets) > keep_sets:
                # These are the sets to delete (all except the newest keep_sets)
                sets_to_delete = all_sets[keep_sets:]
                set_ids_to_delete = [s['id'] for s in sets_to_delete]

                logger.info(f"Will delete {len(set_ids_to_delete)} oldest backup sets from database for job '{job_name}'")
                
                for set_id in set_ids_to_delete:
                    # Count files before deletion
                    c.execute("""
                        SELECT COUNT(*) FROM backup_files
                        WHERE backup_job_id IN (
                            SELECT id FROM backup_jobs WHERE backup_set_id = ?
                        )
                    """, (set_id,))
                    file_count = c.fetchone()[0]

                    # Count jobs before deletion
                    c.execute("SELECT COUNT(*) FROM backup_jobs WHERE backup_set_id = ?", (set_id,))
                    job_count = c.fetchone()[0]

                    # Delete files
                    c.execute("""
                        DELETE FROM backup_files
                        WHERE backup_job_id IN (
                            SELECT id FROM backup_jobs WHERE backup_set_id = ?
                        )
                    """, (set_id,))
                    result['files_deleted'] += c.rowcount

                    # Delete jobs
                    c.execute("DELETE FROM backup_jobs WHERE backup_set_id = ?", (set_id,))
                    result['jobs_deleted'] += c.rowcount
                    
                    # Delete the set
                    c.execute("DELETE FROM backup_sets WHERE id = ?", (set_id,))
                    if c.rowcount > 0:
                        result['sets_deleted'] += 1
                        logger.info(f"Deleted backup set ID {set_id} from database with {job_count} jobs and {file_count} files")
                    else:
                        logger.warning(f"Failed to delete backup set ID {set_id} from database")
                
                conn.commit()
                
                logger.info(f"Database rotation completed: deleted {result['sets_deleted']} sets, {result['jobs_deleted']} jobs, {result['files_deleted']} file records")
            else:
                logger.info(f"No need to rotate database records: {len(all_sets)} sets found, keeping {keep_sets}")
                
            return result
            
    except Exception as e:
        logger.error(f"Error during database backup set rotation for job '{job_name}': {e}", exc_info=True)
        return result
