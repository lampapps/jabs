from datetime import datetime
from typing import Dict, List, Optional, Any
from app.models.db_core import get_db_connection
from app.models.backup_sets import get_backup_set_by_job_and_set
from app.models.backup_jobs import get_jobs_for_backup_set
from app.models.backup_files import get_files_for_backup_set

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