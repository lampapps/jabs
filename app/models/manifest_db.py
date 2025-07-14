import sqlite3
import time
from typing import List, Dict, Optional, Any, Tuple
from app.settings import DB_PATH
from contextlib import contextmanager

@contextmanager
def get_db_connection(db_path: str = DB_PATH):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA foreign_keys = ON")  # <-- Ensure foreign keys are always enabled
        yield conn
    finally:
        conn.close()

def init_db(db_path: str = DB_PATH):
    # Ensure the parent directory exists
    import os
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    with get_db_connection(db_path) as conn:
        c = conn.cursor()
        
        # Enable foreign key constraints
        c.execute("PRAGMA foreign_keys = ON")
        
        # Table 1: Backup Sets (top level container)
        c.execute("""
        CREATE TABLE IF NOT EXISTS backup_sets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_name TEXT NOT NULL,           -- e.g., "test2", "jabs"
            set_name TEXT NOT NULL,           -- e.g., "20250706_130851" (from full backup)
            created_at REAL NOT NULL,         -- When the full backup was first run
            updated_at REAL NOT NULL,         -- Last activity in this set
            description TEXT,
            is_active BOOLEAN DEFAULT 1,      -- Can mark old sets as inactive
            config_snapshot TEXT,             -- Config used when set was created
            UNIQUE(job_name, set_name)
        );
        """)
        
        # Table 2: Backup Jobs (individual backup runs within a set)
        c.execute("""
        CREATE TABLE IF NOT EXISTS backup_jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            backup_set_id INTEGER NOT NULL,
            backup_type TEXT NOT NULL,        -- 'full', 'differential', 'incremental', 'dryrun'
            started_at REAL NOT NULL,
            completed_at REAL,
            status TEXT NOT NULL DEFAULT 'running', -- 'running', 'completed', 'failed', 'cancelled'
            encrypted BOOLEAN DEFAULT 0,
            synced BOOLEAN DEFAULT 0,
            runtime_seconds INTEGER,
            total_files INTEGER DEFAULT 0,
            total_size_bytes INTEGER DEFAULT 0,
            event_message TEXT,
            error_message TEXT,               -- For failed jobs
            FOREIGN KEY (backup_set_id) REFERENCES backup_sets(id) ON DELETE CASCADE
        );
        """)
        
        # Table 3: Backup Files (files within each job)
        c.execute("""
        CREATE TABLE IF NOT EXISTS backup_files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            backup_job_id INTEGER NOT NULL,
            tarball TEXT NOT NULL,
            path TEXT NOT NULL,
            mtime REAL NOT NULL,
            size_bytes INTEGER NOT NULL,
            checksum TEXT,                    -- Optional integrity checking
            is_new BOOLEAN DEFAULT 0,         -- True for new files (incremental/diff)
            is_modified BOOLEAN DEFAULT 0,    -- True for modified files (incremental/diff)
            FOREIGN KEY (backup_job_id) REFERENCES backup_jobs(id) ON DELETE CASCADE
        );
        """)
        
        # Create indexes for better performance
        c.execute("CREATE INDEX IF NOT EXISTS idx_backup_sets_job_name ON backup_sets(job_name)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_backup_jobs_set_id ON backup_jobs(backup_set_id)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_backup_jobs_type ON backup_jobs(backup_type)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_backup_jobs_started_at ON backup_jobs(started_at)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_backup_files_job_id ON backup_files(backup_job_id)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_backup_files_path ON backup_files(path)")
        
        conn.commit()


# =============================================================================
# BACKUP SETS FUNCTIONS
# =============================================================================

def get_or_create_backup_set(job_name: str, set_name: str, config_settings: Optional[str] = None) -> int:
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
            c.execute("""
                INSERT INTO backup_sets (job_name, set_name, created_at, updated_at, config_snapshot)
                VALUES (?, ?, ?, ?, ?)
            """, (job_name, set_name, current_time, current_time, config_settings))
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


def delete_backup_set(set_id: int):
    """Delete a backup set and all its jobs and files (cascading)."""
    with get_db_connection() as conn:
        c = conn.cursor()
        c.execute("DELETE FROM backup_sets WHERE id = ?", (set_id,))
        conn.commit()


# =============================================================================
# BACKUP JOBS FUNCTIONS
# =============================================================================

def insert_backup_job(
    backup_set_id: int,
    backup_type: str,
    encrypted: bool = False,
    synced: bool = False,
    started_at: float = None,
    event_message: str = None
) -> int:
    """Insert a new backup job."""
    if started_at is None:
        started_at = time.time()
        
    with get_db_connection() as conn:
        c = conn.cursor()
        c.execute("""
            INSERT INTO backup_jobs (
                backup_set_id, backup_type, started_at, encrypted, synced, event_message
            )
            VALUES (?, ?, ?, ?, ?, ?)
        """, (backup_set_id, backup_type, started_at, encrypted, synced, event_message))
        conn.commit()
        return c.lastrowid


def finalize_backup_job(
    job_id: int, 
    completed_at: float = None, 
    status: str = "completed", 
    event_message: str = None,
    error_message: str = None,
    total_files: int = 0,
    total_size_bytes: int = 0
):
    """Update backup job when completed."""
    if completed_at is None:
        completed_at = time.time()
        
    # Calculate runtime
    with get_db_connection() as conn:
        c = conn.cursor()
        c.execute("SELECT started_at FROM backup_jobs WHERE id = ?", (job_id,))
        row = c.fetchone()
        runtime_seconds = int(completed_at - row['started_at']) if row else 0
        
        c.execute("""
            UPDATE backup_jobs 
            SET completed_at = ?, status = ?, event_message = ?, error_message = ?, 
                runtime_seconds = ?, total_files = ?, total_size_bytes = ?
            WHERE id = ?
        """, (completed_at, status, event_message, error_message, runtime_seconds, 
              total_files, total_size_bytes, job_id))
        conn.commit()


def get_backup_job(job_id: int) -> Optional[sqlite3.Row]:
    """Get a backup job by ID."""
    with get_db_connection() as conn:
        c = conn.cursor()
        c.execute("SELECT * FROM backup_jobs WHERE id = ?", (job_id,))
        return c.fetchone()


def get_jobs_for_backup_set(backup_set_id: int) -> List[sqlite3.Row]:
    """Get all jobs for a backup set."""
    with get_db_connection() as conn:
        c = conn.cursor()
        c.execute("""
            SELECT * FROM backup_jobs 
            WHERE backup_set_id = ? 
            ORDER BY started_at ASC
        """, (backup_set_id,))
        return c.fetchall()


def get_last_backup_job(
    job_name: str,
    backup_type: Optional[str] = None,
    completed_only: bool = True
) -> Optional[sqlite3.Row]:
    """Get the most recent backup job for a job name."""
    with get_db_connection() as conn:
        c = conn.cursor()
        query = """
            SELECT bj.*, bs.job_name, bs.set_name 
            FROM backup_jobs bj
            JOIN backup_sets bs ON bj.backup_set_id = bs.id
            WHERE bs.job_name = ?
        """
        params = [job_name]
        
        if backup_type:
            query += " AND bj.backup_type = ?"
            params.append(backup_type)
            
        if completed_only:
            query += " AND bj.status = 'completed'"
            
        query += " ORDER BY bj.started_at DESC LIMIT 1"
        c.execute(query, params)
        return c.fetchone()


def get_last_full_backup_job(job_name: str) -> Optional[sqlite3.Row]:
    """Get the most recent completed full backup job for a job name."""
    return get_last_backup_job(job_name, backup_type="full", completed_only=True)


# =============================================================================
# BACKUP FILES FUNCTIONS
# =============================================================================

def insert_files(backup_job_id: int, files: List[Dict[str, Any]]):
    """Insert backup files for a backup job."""
    with get_db_connection() as conn:
        c = conn.cursor()
        c.executemany("""
            INSERT INTO backup_files (backup_job_id, tarball, path, mtime, size_bytes, is_new, is_modified)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, [
            (
                backup_job_id,
                f["tarball"],
                f["path"],
                f["mtime"],
                f.get("size", 0),  # Handle both 'size' and 'size_bytes'
                f.get("is_new", False),
                f.get("is_modified", False)
            )
            for f in files
        ])
        conn.commit()


def get_files_for_backup_job(backup_job_id: int) -> List[Dict[str, Any]]:
    """Get all files for a backup job."""
    with get_db_connection() as conn:
        c = conn.cursor()
        c.execute("""
            SELECT tarball, path, mtime, size_bytes as size, is_new, is_modified
            FROM backup_files
            WHERE backup_job_id = ?
            ORDER BY path
        """, (backup_job_id,))
        return [dict(row) for row in c.fetchall()]


def get_files_for_backup_set(backup_set_id: int) -> List[Dict[str, Any]]:
    """Get all files across all jobs in a backup set."""
    with get_db_connection() as conn:
        c = conn.cursor()
        c.execute("""
            SELECT bf.tarball, bf.path, bf.mtime, bf.size_bytes as size, bf.is_new, bf.is_modified,
                   bj.backup_type, bj.started_at as job_started_at
            FROM backup_files bf
            JOIN backup_jobs bj ON bf.backup_job_id = bj.id
            WHERE bj.backup_set_id = ?
            ORDER BY bf.path
        """, (backup_set_id,))
        return [dict(row) for row in c.fetchall()]


def get_files_for_last_full_backup(job_name: str) -> List[Dict[str, Any]]:
    """Get files from the last completed full backup for differential comparison."""
    with get_db_connection() as conn:
        c = conn.cursor()
        c.execute("""
            SELECT bf.tarball, bf.path, bf.mtime, bf.size_bytes as size
            FROM backup_files bf
            JOIN backup_jobs bj ON bf.backup_job_id = bj.id
            JOIN backup_sets bs ON bj.backup_set_id = bs.id
            WHERE bs.job_name = ? AND bj.backup_type = 'full' AND bj.status = 'completed'
            ORDER BY bj.started_at DESC, bf.path
        """, (job_name,))
        return [dict(row) for row in c.fetchall()]


# =============================================================================
# COMPATIBILITY FUNCTIONS FOR ROUTES
# =============================================================================

def get_manifest_with_files(job_name: str, backup_set_id: str) -> Optional[Dict[str, Any]]:
    """Get manifest data with files for a backup set (compatibility function for routes)."""
    # The backup_set_id here is actually the set_name from the URL
    backup_set = get_backup_set_by_job_and_set(job_name, backup_set_id)
    if not backup_set:
        return None
    
    # Get the most recent completed job for this backup set
    jobs = get_jobs_for_backup_set(backup_set['id'])
    completed_jobs = [j for j in jobs if j['status'] == 'completed']
    if not completed_jobs:
        return None
    
    # Get the most recent completed job
    latest_job = max(completed_jobs, key=lambda j: j['started_at'])
    
    # Get all files for the backup set
    files = get_files_for_backup_set(backup_set['id'])
    
    # Format timestamps
    from datetime import datetime
    
    def format_timestamp(timestamp):
        if timestamp:
            try:
                dt = datetime.fromtimestamp(timestamp)
                return dt.isoformat()
            except (ValueError, TypeError):
                return None
        return None
    
    return {
        'job_name': backup_set['job_name'],
        'set_name': backup_set['set_name'],
        'backup_type': latest_job['backup_type'],
        'status': latest_job['status'],
        'event': latest_job['event_message'] if latest_job['event_message'] else '',
        'timestamp': format_timestamp(latest_job['completed_at']),
        'started_at': format_timestamp(latest_job['started_at']),
        'completed_at': format_timestamp(latest_job['completed_at']),
        'files': files
    }


# =============================================================================
# DASHBOARD AND STATS FUNCTIONS
# =============================================================================

def get_backup_set_with_jobs(job_name: str, set_name: str) -> Optional[Dict[str, Any]]:
    """Get backup set with all its jobs and summary stats."""
    backup_set = get_backup_set_by_job_and_set(job_name, set_name)
    if not backup_set:
        return None
        
    jobs = get_jobs_for_backup_set(backup_set['id'])
    files = get_files_for_backup_set(backup_set['id'])
    
    # Calculate summary stats
    total_files = len(files)
    total_size = sum(f.get('size', 0) for f in files)
    completed_jobs = [j for j in jobs if j['status'] == 'completed']
    
    from datetime import datetime
    
    # Format timestamps
    created_timestamp = None
    updated_timestamp = None
    
    try:
        if backup_set.get('created_at'):
            dt = datetime.fromtimestamp(backup_set['created_at'])
            created_timestamp = dt.isoformat()
    except (ValueError, TypeError):
        created_timestamp = None
        
    try:
        if backup_set.get('updated_at'):
            dt = datetime.fromtimestamp(backup_set['updated_at'])
            updated_timestamp = dt.isoformat()
    except (ValueError, TypeError):
        updated_timestamp = None
    
    return {
        'backup_set': dict(backup_set),
        'jobs': [dict(job) for job in jobs],
        'files': files,
        'stats': {
            'total_jobs': len(jobs),
            'completed_jobs': len(completed_jobs),
            'total_files': total_files,
            'total_size_bytes': total_size,
            'created_at': created_timestamp,
            'updated_at': updated_timestamp
        }
    }


def get_dashboard_summary(job_name: Optional[str] = None) -> List[Dict[str, Any]]:
    """Get summary data for dashboard."""
    with get_db_connection() as conn:
        c = conn.cursor()
        
        if job_name:
            query = """
                SELECT 
                    bs.job_name,
                    bs.set_name,
                    bs.created_at,
                    bs.updated_at,
                    COUNT(bj.id) as total_jobs,
                    COUNT(CASE WHEN bj.status = 'completed' THEN 1 END) as completed_jobs,
                    SUM(bj.total_files) as total_files,
                    SUM(bj.total_size_bytes) as total_size_bytes,
                    MAX(bj.completed_at) as last_completed
                FROM backup_sets bs
                LEFT JOIN backup_jobs bj ON bs.id = bj.backup_set_id
                WHERE bs.job_name = ?
                GROUP BY bs.id
                ORDER BY bs.updated_at DESC
            """
            c.execute(query, (job_name,))
        else:
            query = """
                SELECT 
                    bs.job_name,
                    bs.set_name,
                    bs.created_at,
                    bs.updated_at,
                    COUNT(bj.id) as total_jobs,
                    COUNT(CASE WHEN bj.status = 'completed' THEN 1 END) as completed_jobs,
                    SUM(bj.total_files) as total_files,
                    SUM(bj.total_size_bytes) as total_size_bytes,
                    MAX(bj.completed_at) as last_completed
                FROM backup_sets bs
                LEFT JOIN backup_jobs bj ON bs.id = bj.backup_set_id
                GROUP BY bs.id
                ORDER BY bs.updated_at DESC
            """
            c.execute(query)
            
        return [dict(row) for row in c.fetchall()]


# =============================================================================
# SEARCH AND UTILITY FUNCTIONS
# =============================================================================

def search_files(query: str, job_name: str = None) -> List[sqlite3.Row]:
    """Search for files by path pattern."""
    with get_db_connection() as conn:
        c = conn.cursor()
        
        if job_name:
            c.execute("""
                SELECT 
                    bf.path,
                    bf.mtime,
                    bf.size_bytes as size,
                    bf.tarball,
                    bs.job_name,
                    bs.set_name,
                    bj.backup_type
                FROM backup_files bf
                JOIN backup_jobs bj ON bf.backup_job_id = bj.id
                JOIN backup_sets bs ON bj.backup_set_id = bs.id
                WHERE bs.job_name = ? AND bf.path LIKE ?
                ORDER BY bs.updated_at DESC, bf.path
            """, (job_name, f"%{query}%"))
        else:
            c.execute("""
                SELECT 
                    bf.path,
                    bf.mtime,
                    bf.size_bytes as size,
                    bf.tarball,
                    bs.job_name,
                    bs.set_name,
                    bj.backup_type
                FROM backup_files bf
                JOIN backup_jobs bj ON bf.backup_job_id = bj.id
                JOIN backup_sets bs ON bj.backup_set_id = bs.id
                WHERE bf.path LIKE ?
                ORDER BY bs.job_name, bs.updated_at DESC, bf.path
            """, (f"%{query}%",))
        
        return c.fetchall()