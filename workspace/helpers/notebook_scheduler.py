"""
Utilities for working with JupyterLab's built-in job scheduler.
The JupyterLab scheduler stores jobs in a SQLite database at:
    ~/.local/share/jupyter/scheduler.sqlite

Downloaded job outputs are stored in:
    /jovyan/home/
"""

import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, Optional, List

from jupyter_core.paths import jupyter_data_dir
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker


# Default database URL for JupyterLab scheduler
def get_default_db_url() -> str:
    """Get the default JupyterLab scheduler database URL."""
    return f"sqlite:///{jupyter_data_dir()}/scheduler.sqlite"


def get_db_session(db_url: Optional[str] = None):
    """
    Create a database session for the scheduler database.

    Args:
        db_url: Database URL. If None, uses default JupyterLab location.

    Returns:
        SQLAlchemy session factory
    """
    if db_url is None:
        db_url = get_default_db_url()

    engine = create_engine(db_url, echo=False)
    Session = sessionmaker(bind=engine)
    return Session



def list_jobs(
    status: Optional[str] = None,
    limit: int = 100,
    db_url: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    List jobs from the scheduler database.

    Args:
        status: Filter by status (IN_PROGRESS, COMPLETED, FAILED, etc.)
        limit: Maximum number of jobs to return
        db_url: Database URL. If None, uses default.

    Returns:
        List of job dictionaries
    """
    if db_url is None:
        db_url = get_default_db_url()

    Session = get_db_session(db_url)

    with Session() as session:
        if status:
            query = text("""
                SELECT job_id, name, status, create_time, start_time, end_time, input_filename, parameters
                FROM jobs
                WHERE status = :status
                ORDER BY create_time DESC
                LIMIT :limit
            """)
            result = session.execute(query, {"status": status, "limit": limit})
        else:
            query = text("""
                SELECT job_id, name, status, create_time, start_time, end_time, input_filename, parameters
                FROM jobs
                ORDER BY create_time DESC
                LIMIT :limit
            """)
            result = session.execute(query, {"limit": limit})

        jobs = []
        for row in result.fetchall():
            create_time = datetime.fromtimestamp(row[3] / 1000) if row[3] else None
            start_time = datetime.fromtimestamp(row[4] / 1000) if row[4] else None
            end_time = datetime.fromtimestamp(row[5] / 1000) if row[5] else None

            jobs.append({
                'job_id': row[0],
                'name': row[1],
                'status': row[2],
                'create_time': create_time.isoformat() if create_time else None,
                'start_time': start_time.isoformat() if start_time else None,
                'end_time': end_time.isoformat() if end_time else None,
                'input_filename': row[6],
                'parameters': row[7]
            })

        return jobs


def get_job_info(job_id: str, db_url: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """
    Get detailed information about a specific job.

    Args:
        job_id: The job ID to look up
        db_url: Database URL. If None, uses default.

    Returns:
        Job information dictionary or None if not found
    """
    if db_url is None:
        db_url = get_default_db_url()

    Session = get_db_session(db_url)

    with Session() as session:
        query = text("""
            SELECT job_id, name, status, create_time, start_time, end_time,
                   input_filename, parameters, status_message
            FROM jobs
            WHERE job_id = :job_id
        """)
        result = session.execute(query, {"job_id": job_id})
        row = result.fetchone()

        if not row:
            return None

        create_time = datetime.fromtimestamp(row[3] / 1000) if row[3] else None
        start_time = datetime.fromtimestamp(row[4] / 1000) if row[4] else None
        end_time = datetime.fromtimestamp(row[5] / 1000) if row[5] else None

        return {
            'job_id': row[0],
            'name': row[1],
            'status': row[2],
            'create_time': create_time.isoformat() if create_time else None,
            'start_time': start_time.isoformat() if start_time else None,
            'end_time': end_time.isoformat() if end_time else None,
            'input_filename': row[6],
            'parameters': row[7],
            'status_message': row[8]
        }
    
def cleanup_old_jobs(
    days_threshold: int = 30,
    dry_run: bool = True,
    db_url: Optional[str] = None,
    delete_staging_files: bool = True
) -> Dict[str, Any]:
    """
    Clean up JupyterLab scheduler jobs older than specified days.

    This function deletes jobs from the scheduler database and optionally
    removes their staging files from disk.

    Args:
        days_threshold: Number of days. Jobs older than this will be deleted.
                       Default is 30 days.
        dry_run: If True, returns what would be deleted without actually deleting.
                If False, performs the actual deletion.
        db_url: Database URL. If None, uses default JupyterLab location.
        delete_staging_files: If True, also delete staging area files for jobs.

    Returns:
        Dictionary with cleanup results:
        - jobs_count: Number of jobs deleted (or would be deleted)
        - job_ids: List of job IDs affected
        - oldest_job_date: Date of oldest job found for cleanup
        - newest_job_date: Date of newest job found for cleanup
        - staging_files_deleted: Number of staging directories deleted
        - dry_run: Whether this was a dry run
        - message: Summary message

    Example:
        >>> # Preview what would be deleted
        >>> result = cleanup_old_jobs(days_threshold=30, dry_run=True)
        >>> print(f"Would delete {result['jobs_count']} jobs")

        >>> # Actually delete
        >>> result = cleanup_old_jobs(days_threshold=30, dry_run=False)
        >>> print(f"Deleted {result['jobs_count']} jobs")
    """
    import shutil

    if db_url is None:
        db_url = get_default_db_url()

    # Calculate threshold timestamp (Unix timestamp in milliseconds)
    threshold_date = datetime.utcnow() - timedelta(days=days_threshold)
    threshold_ts = int(threshold_date.timestamp() * 1000)  # Scheduler uses ms

    Session = get_db_session(db_url)

    with Session() as session:
        # Query jobs older than threshold
        query = text("""
            SELECT job_id, name, status, create_time, input_filename
            FROM jobs
            WHERE create_time < :threshold
            ORDER BY create_time ASC
        """)

        result = session.execute(query, {"threshold": threshold_ts})
        old_jobs = result.fetchall()

        if not old_jobs:
            return {
                'jobs_count': 0,
                'job_ids': [],
                'oldest_job_date': None,
                'newest_job_date': None,
                'staging_files_deleted': 0,
                'dry_run': dry_run,
                'days_threshold': days_threshold,
                'message': f'No jobs older than {days_threshold} days found'
            }

        # Collect job information
        job_ids = [row[0] for row in old_jobs]
        job_names = [row[1] for row in old_jobs]

        # Convert timestamps to dates
        oldest_ts = old_jobs[0][3]
        newest_ts = old_jobs[-1][3]
        oldest_date = datetime.fromtimestamp(oldest_ts / 1000).isoformat()
        newest_date = datetime.fromtimestamp(newest_ts / 1000).isoformat()

        result_info = {
            'jobs_count': len(job_ids),
            'job_ids': job_ids,
            'job_names': job_names,
            'oldest_job_date': oldest_date,
            'newest_job_date': newest_date,
            'staging_files_deleted': 0,
            'dry_run': dry_run,
            'days_threshold': days_threshold
        }

        if dry_run:
            result_info['message'] = f'Dry run. Detected {len(job_ids)} jobs for deletion.'
            return result_info

        # Delete staging files if requested
        staging_deleted = 0
        if delete_staging_files:
            staging_path = os.path.join(jupyter_data_dir(), "scheduler_staging_area")
            for job_id in job_ids:
                job_staging_dir = os.path.join(staging_path, job_id)
                if os.path.exists(job_staging_dir):
                    try:
                        shutil.rmtree(job_staging_dir)
                        staging_deleted += 1
                    except Exception as e:
                        print(f"Warning: Could not delete staging dir for job {job_id}: {e}")

        # Delete jobs from database
        delete_query = text("""
            DELETE FROM jobs
            WHERE create_time < :threshold
        """)
        session.execute(delete_query, {"threshold": threshold_ts})
        session.commit()

        result_info['staging_files_deleted'] = staging_deleted
        result_info['message'] = f'Successfully deleted {len(job_ids)} jobs and {staging_deleted} staging directories'

        return result_info
    

def get_downloaded_output_directory():
    """
    Get information about downloaded job output directories."""
    jupyter_home = Path.home()  # /home/jovyan in container
    jobs_output_dir = jupyter_home / "jobs"

    if jobs_output_dir.exists():
        # List all downloaded job output directories
        output_dirs = sorted(jobs_output_dir.iterdir(), key=lambda x: x.stat().st_mtime, reverse=True)

        directories = []
        total_size = 0
        for d in output_dirs:
            if d.is_dir():
                # Calculate directory size
                dir_size = sum(f.stat().st_size for f in d.rglob('*') if f.is_file())
                total_size += dir_size

                # Get modification time
                mtime = datetime.fromtimestamp(d.stat().st_mtime).strftime('%Y-%m-%d %H:%M')

                # Count files
                file_count = len(list(d.rglob('*')))

                directories.append({
                    'directory': str(d),
                    'size': f"{dir_size / 1024:.1f} KB",
                    'file_count': file_count,
                    'modified': mtime
                })

        print(f"\nTotal size: {total_size / 1024 / 1024:.2f} MB")

        return {
            'jobs_output_directory': str(jobs_output_dir),
            'directories': directories,
            'total_size': f"{total_size / 1024 / 1024:.2f} MB"
        }
    else:
        raise FileNotFoundError(f"Jobs output directory does not exist: {jobs_output_dir}")


def find_orphaned_outputs(jobs_output_dir: Path) -> list:
    """
    Find downloaded output directories for jobs that no longer exist in the database.
    
    Returns:
        List of (directory_path, job_id) tuples for orphaned directories
    """
    if not jobs_output_dir.exists():
        return []
    
    # Get all job IDs from database
    all_jobs = list_jobs(limit=1000000)  # Get all jobs
    existing_job_ids = {job['job_id'] for job in all_jobs}
    
    orphaned = []
    for d in jobs_output_dir.iterdir():
        if d.is_dir():
            # Extract job_id from directory name (format: notebook_name-job_id)
            # The job_id is a UUID like "abc12345-def6-7890-ghij-klmnopqrstuv"
            name_parts = d.name.rsplit('-', 5)  # UUID has 5 parts separated by -
            if len(name_parts) >= 5:
                # Reconstruct the job_id (last 5 parts)
                job_id = '-'.join(name_parts[-5:])
                if job_id not in existing_job_ids:
                    orphaned.append((d, job_id))
    
    return orphaned