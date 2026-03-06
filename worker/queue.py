"""
SQLite-based job queue manager.

This module provides the JobQueue class for managing document processing jobs
using SQLite as the backing store. Jobs are stored persistently and can survive
worker crashes, UI restarts, and system failures.
"""
# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License

import sqlite3
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, List


class JobQueue:
    """SQLite-based job queue manager for document processing tasks."""

    def __init__(self, db_path: str):
        """
        Initialize job queue with database path.

        Args:
            db_path: Path to SQLite database file (will be created if doesn't exist)
        """
        self.db_path = db_path
        self._init_database()

    def _init_database(self):
        """Initialize database schema if it doesn't exist."""
        # Read schema from schema.sql file
        schema_path = Path(__file__).parent / 'schema.sql'
        with open(schema_path, 'r') as f:
            schema_sql = f.read()

        # Execute schema
        with sqlite3.connect(self.db_path) as conn:
            conn.executescript(schema_sql)
            conn.commit()
            
        # Migrate existing databases (add missing columns)
        self._migrate_database()
    
    def _migrate_database(self):
        """Migrate existing database schema to add missing columns."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Check if parser_type column exists in jobs table
            cursor.execute("PRAGMA table_info(jobs)")
            columns = [row[1] for row in cursor.fetchall()]
            
            if 'parser_type' not in columns:
                # Add parser_type column
                conn.execute("ALTER TABLE jobs ADD COLUMN parser_type TEXT")
                conn.commit()

    def _get_connection(self) -> sqlite3.Connection:
        """Get database connection with row factory."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def enqueue(self, job_type: str, file_path: str, params: Optional[Dict[str, Any]] = None,
                parser_type: Optional[str] = None, max_retries: int = 0) -> int:
        """
        Add job to queue.

        Args:
            job_type: Type of job ('stage_2', 'stage_3', 'parse', 'question')
            file_path: Path to file to process
            params: Optional parameters dict (will be serialized to JSON)
            parser_type: Parser type identifier (required for 'parse' jobs: 'uslm', 'formex', 'ca_html')
            max_retries: Maximum number of retry attempts on failure

        Returns:
            job_id: ID of newly created job
        """
        params_json = json.dumps(params) if params else None

        with self._get_connection() as conn:
            cursor = conn.execute(
                """
                INSERT INTO jobs (status, job_type, parser_type, file_path, params, max_retries)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                ('queued', job_type, parser_type, file_path, params_json, max_retries)
            )
            job_id = cursor.lastrowid
            conn.commit()

        return job_id

    def get_next_job(self, worker_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Get next queued job and mark it as running.

        This is an atomic operation - the job status is updated to 'running'
        within the same transaction that retrieves it.

        Args:
            worker_id: Optional worker identifier (e.g., process ID)

        Returns:
            Job dict with keys: id, job_type, file_path, params, etc.
            Returns None if no jobs are queued.
        """
        if worker_id is None:
            worker_id = str(os.getpid())

        with self._get_connection() as conn:
            # Begin exclusive transaction
            conn.execute("BEGIN IMMEDIATE")

            try:
                # Get oldest queued job
                cursor = conn.execute(
                    """
                    SELECT id, job_type, parser_type, file_path, params, max_retries, retry_count
                    FROM jobs
                    WHERE status = 'queued'
                    ORDER BY created_at ASC
                    LIMIT 1
                    """
                )
                row = cursor.fetchone()

                if row is None:
                    conn.rollback()
                    return None

                job_id = row['id']

                # Mark as running
                conn.execute(
                    """
                    UPDATE jobs
                    SET status = 'running',
                        started_at = CURRENT_TIMESTAMP,
                        worker_id = ?
                    WHERE id = ?
                    """,
                    (worker_id, job_id)
                )
                conn.commit()

                # Return job details
                return {
                    'id': job_id,
                    'job_type': row['job_type'],
                    'parser_type': row['parser_type'],
                    'file_path': row['file_path'],
                    'params': json.loads(row['params']) if row['params'] else {},
                    'max_retries': row['max_retries'],
                    'retry_count': row['retry_count']
                }

            except Exception as e:
                conn.rollback()
                raise

    def update_progress(self, job_id: int, phase: str, current_item: str,
                       items_done: int, total_items: int):
        """
        Update job progress.

        This should be called frequently during job execution (every 5-10 seconds)
        to provide real-time progress updates to the UI.

        Args:
            job_id: Job ID
            phase: Current processing phase (e.g., 'level_1_summaries')
            current_item: Current item being processed (e.g., 'section_143c')
            items_done: Number of items completed
            total_items: Total number of items to process
        """
        with self._get_connection() as conn:
            conn.execute(
                """
                INSERT INTO job_progress (job_id, phase, current_item, items_done, total_items)
                VALUES (?, ?, ?, ?, ?)
                """,
                (job_id, phase, current_item, items_done, total_items)
            )
            conn.commit()

    def complete_job(self, job_id: int, result: Optional[Dict[str, Any]] = None):
        """
        Mark job as completed.

        Args:
            job_id: Job ID
            result: Optional result dict (will be serialized to JSON)
        """
        result_json = json.dumps(result) if result else None

        with self._get_connection() as conn:
            conn.execute(
                """
                UPDATE jobs
                SET status = 'completed',
                    completed_at = CURRENT_TIMESTAMP,
                    result = ?
                WHERE id = ?
                """,
                (result_json, job_id)
            )
            conn.commit()

    def fail_job(self, job_id: int, error_message: str):
        """
        Mark job as failed and handle retry logic.

        If the job has retry attempts remaining, it will be re-queued.
        Otherwise, it will be marked as failed.

        Args:
            job_id: Job ID
            error_message: Error message describing the failure
        """
        with self._get_connection() as conn:
            # Get current retry info
            cursor = conn.execute(
                "SELECT retry_count, max_retries FROM jobs WHERE id = ?",
                (job_id,)
            )
            row = cursor.fetchone()

            if row is None:
                raise ValueError(f"Job {job_id} not found")

            retry_count = row['retry_count']
            max_retries = row['max_retries']

            if retry_count < max_retries:
                # Re-queue for retry
                conn.execute(
                    """
                    UPDATE jobs
                    SET status = 'queued',
                        retry_count = retry_count + 1,
                        error_message = ?,
                        started_at = NULL,
                        worker_id = NULL
                    WHERE id = ?
                    """,
                    (error_message, job_id)
                )
            else:
                # Mark as failed
                conn.execute(
                    """
                    UPDATE jobs
                    SET status = 'failed',
                        completed_at = CURRENT_TIMESTAMP,
                        error_message = ?
                    WHERE id = ?
                    """,
                    (error_message, job_id)
                )

            conn.commit()

    def cancel_job(self, job_id: int):
        """
        Mark job as cancelled.

        Note: This only sets the status in the database. The worker is responsible
        for checking this status and stopping execution.

        Args:
            job_id: Job ID
        """
        with self._get_connection() as conn:
            conn.execute(
                """
                UPDATE jobs
                SET status = 'cancelled',
                    completed_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (job_id,)
            )
            conn.commit()

    def get_job_status(self, job_id: int) -> Dict[str, Any]:
        """
        Get current job status and latest progress.

        Args:
            job_id: Job ID

        Returns:
            Dict with keys:
                - id, status, job_type, file_path
                - created_at, started_at, completed_at
                - error_message, result
                - progress: Dict with latest progress info (if any)
        """
        with self._get_connection() as conn:
            # Get job info
            cursor = conn.execute(
                """
                SELECT id, status, job_type, file_path, created_at, started_at,
                       completed_at, error_message, result, worker_id
                FROM jobs
                WHERE id = ?
                """,
                (job_id,)
            )
            row = cursor.fetchone()

            if row is None:
                raise ValueError(f"Job {job_id} not found")

            # Get latest progress
            cursor = conn.execute(
                """
                SELECT phase, current_item, items_done, total_items, timestamp
                FROM job_progress
                WHERE job_id = ?
                ORDER BY timestamp DESC
                LIMIT 1
                """,
                (job_id,)
            )
            progress_row = cursor.fetchone()

            job_status = {
                'id': row['id'],
                'status': row['status'],
                'job_type': row['job_type'],
                'file_path': row['file_path'],
                'created_at': row['created_at'],
                'started_at': row['started_at'],
                'completed_at': row['completed_at'],
                'error_message': row['error_message'],
                'result': json.loads(row['result']) if row['result'] else None,
                'worker_id': row['worker_id'],
                'progress': None
            }

            if progress_row:
                job_status['progress'] = {
                    'phase': progress_row['phase'],
                    'current_item': progress_row['current_item'],
                    'items_done': progress_row['items_done'],
                    'total_items': progress_row['total_items'],
                    'timestamp': progress_row['timestamp']
                }

            return job_status

    def get_recent_jobs(self, limit: int = 50, status: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get recent jobs for UI display.

        Args:
            limit: Maximum number of jobs to return
            status: Optional filter by status ('queued', 'running', 'completed', 'failed', 'cancelled')

        Returns:
            List of job dicts with basic info (no progress details)
        """
        with self._get_connection() as conn:
            if status:
                cursor = conn.execute(
                    """
                    SELECT id, status, job_type, file_path, created_at, started_at,
                           completed_at, error_message
                    FROM jobs
                    WHERE status = ?
                    ORDER BY created_at DESC
                    LIMIT ?
                    """,
                    (status, limit)
                )
            else:
                cursor = conn.execute(
                    """
                    SELECT id, status, job_type, file_path, created_at, started_at,
                           completed_at, error_message
                    FROM jobs
                    ORDER BY created_at DESC
                    LIMIT ?
                    """,
                    (limit,)
                )

            jobs = []
            for row in cursor:
                jobs.append({
                    'id': row['id'],
                    'status': row['status'],
                    'job_type': row['job_type'],
                    'file_path': row['file_path'],
                    'created_at': row['created_at'],
                    'started_at': row['started_at'],
                    'completed_at': row['completed_at'],
                    'error_message': row['error_message']
                })

            return jobs

    def mark_stale_jobs_cancelled(self):
        """
        Mark all 'running' jobs as 'cancelled'.
        
        This should be called when the worker starts, since any job marked as 'running'
        from a previous session is stale (the worker was restarted, so those jobs can't
        actually be running).
        
        Returns:
            Number of jobs marked as cancelled
        """
        with self._get_connection() as conn:
            cursor = conn.execute(
                """
                UPDATE jobs
                SET status = 'cancelled',
                    completed_at = CURRENT_TIMESTAMP,
                    error_message = 'Job cancelled due to worker restart (stale running job)'
                WHERE status = 'running'
                """
            )
            count = cursor.rowcount
            conn.commit()
            return count

    def cleanup_old_jobs(self, days: int = 30):
        """
        Remove old completed/failed jobs from the database.

        This helps keep the database size manageable. Only removes jobs
        that are completed or failed and older than the specified number of days.

        Args:
            days: Remove jobs older than this many days
        """
        with self._get_connection() as conn:
            conn.execute(
                """
                DELETE FROM jobs
                WHERE status IN ('completed', 'failed')
                AND completed_at < datetime('now', '-' || ? || ' days')
                """,
                (days,)
            )
            conn.commit()

    def get_queue_stats(self) -> Dict[str, int]:
        """
        Get queue statistics.

        Returns:
            Dict with counts by status:
                {'queued': 5, 'running': 2, 'completed': 100, 'failed': 3, 'cancelled': 1}
        """
        with self._get_connection() as conn:
            cursor = conn.execute(
                """
                SELECT status, COUNT(*) as count
                FROM jobs
                GROUP BY status
                """
            )

            stats = {
                'queued': 0,
                'running': 0,
                'completed': 0,
                'failed': 0,
                'cancelled': 0
            }

            for row in cursor:
                stats[row['status']] = row['count']

            return stats

    def log_message(self, job_id: int, level: str, message: str):
        """
        Log a message for a job.

        Args:
            job_id: Job ID
            level: Log level ('DEBUG', 'INFO', 'WARNING', 'ERROR')
            message: Log message
        """
        with self._get_connection() as conn:
            conn.execute(
                """
                INSERT INTO job_logs (job_id, level, message)
                VALUES (?, ?, ?)
                """,
                (job_id, level, message)
            )
            conn.commit()

    def get_job_logs(self, job_id: int, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get logs for a job.

        Args:
            job_id: Job ID
            limit: Maximum number of log entries to return

        Returns:
            List of log dicts with timestamp, level, and message
        """
        with self._get_connection() as conn:
            cursor = conn.execute(
                """
                SELECT timestamp, level, message
                FROM job_logs
                WHERE job_id = ?
                ORDER BY timestamp DESC
                LIMIT ?
                """,
                (job_id, limit)
            )

            logs = []
            for row in cursor:
                logs.append({
                    'timestamp': row['timestamp'],
                    'level': row['level'],
                    'message': row['message']
                })

            return logs

    def _normalize_path_for_comparison(self, file_path: str) -> str:
        """
        Normalize a file path for comparison purposes.
        
        Uses resolve() if the file exists (resolves symlinks and gets true absolute path),
        otherwise uses absolute() to get an absolute path for comparison.
        
        Args:
            file_path: Path to normalize
            
        Returns:
            Normalized absolute path string
        """
        if not file_path:
            return ""
            
        try:
            path_obj = Path(file_path)
            # Try to resolve if file exists (resolves symlinks, gets canonical path)
            if path_obj.exists():
                normalized = str(path_obj.resolve())
            else:
                # File doesn't exist, use absolute() and normalize separators
                normalized = str(path_obj.absolute())
            # Normalize path separators (handles Windows backslashes)
            return os.path.normpath(normalized)
        except (OSError, RuntimeError, ValueError):
            # Fallback: just normalize the string
            return os.path.normpath(file_path)

    def has_active_job(self, file_path: str, job_type: str) -> bool:
        """
        Check if there is an active (queued or running) job for a specific file and job type.

        Args:
            file_path: File path to check (will be normalized for comparison)
            job_type: Job type to check ('stage_2', 'stage_3', 'parse', 'question')

        Returns:
            True if there is a queued or running job for the file and job type, False otherwise
        """
        if not file_path:
            return False
        
        # Normalize input path for comparison
        normalized_input = self._normalize_path_for_comparison(file_path)
        
        with self._get_connection() as conn:
            # Get all active jobs for this job type and compare normalized paths
            cursor = conn.execute(
                """
                SELECT id, file_path
                FROM jobs
                WHERE job_type = ?
                  AND status IN ('queued', 'running')
                """,
                (job_type,)
            )
            
            rows = cursor.fetchall()
            if not rows:
                # No active jobs at all - definitely no match
                return False
            
            for row in rows:
                stored_path = row['file_path']
                if not stored_path:
                    continue
                    
                # Normalize stored path the same way
                normalized_stored = self._normalize_path_for_comparison(stored_path)
                
                # Case-insensitive comparison on Windows, case-sensitive on Unix
                if os.name == 'nt':  # Windows
                    if normalized_stored.lower() == normalized_input.lower():
                        return True
                else:  # Unix-like
                    if normalized_stored == normalized_input:
                        return True
            
            return False