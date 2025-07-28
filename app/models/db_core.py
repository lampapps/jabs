import sqlite3
import os
from app.settings import DB_PATH
from contextlib import contextmanager

@contextmanager
def get_db_connection(db_path: str = DB_PATH):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA foreign_keys = ON")
        yield conn
    finally:
        conn.close()

def init_db(db_path: str = DB_PATH):
    """Initialize the database schema"""
    # Ensure the parent directory exists
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    with get_db_connection(db_path) as conn:
        c = conn.cursor()
        
        # Enable foreign key constraints
        c.execute("PRAGMA foreign_keys = ON")
        
        # Create tables
        _create_backup_sets_table(c)
        _create_backup_jobs_table(c)
        _create_backup_files_table(c)
        _create_scheduler_events_table(c)
        _create_email_digests_table(c)
        _create_indexes(c)
        
        # Create the events view
        from app.models.events import create_events_view
        create_events_view(conn)
        
        conn.commit()

def _create_backup_sets_table(cursor):
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS backup_sets (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_name TEXT NOT NULL,           -- e.g., "test2", "jabs"
        set_name TEXT NOT NULL,           -- e.g., "20250706_130851" (from full backup)
        created_at REAL NOT NULL,         -- When the full backup was first run
        updated_at REAL NOT NULL,         -- Last activity in this set
        description TEXT,
        is_active BOOLEAN DEFAULT 1,      -- Can mark old sets as inactive
        config_snapshot TEXT,             -- Config used when set was created
        hostname TEXT,                    -- Added for events view
        UNIQUE(job_name, set_name)
    );
    """)

def _create_backup_jobs_table(cursor):
    cursor.execute("""
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

def _create_backup_files_table(cursor):
    cursor.execute("""
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

def _create_scheduler_events_table(cursor):
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS scheduler_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        datetime TEXT NOT NULL,
        job_name TEXT NOT NULL,
        backup_type TEXT,
        status TEXT NOT NULL
    )
    """)

def _create_email_digests_table(cursor):
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS email_digests (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT NOT NULL,          -- ISO format timestamp
        subject TEXT NOT NULL,
        body TEXT NOT NULL,
        html BOOLEAN DEFAULT 0,
        event_type TEXT
    )
    """)

def _create_indexes(cursor):
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_backup_sets_job_name ON backup_sets(job_name)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_backup_jobs_set_id ON backup_jobs(backup_set_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_backup_jobs_type ON backup_jobs(backup_type)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_backup_jobs_started_at ON backup_jobs(started_at)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_backup_files_job_id ON backup_files(backup_job_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_backup_files_path ON backup_files(path)")