"""
Worker package for SQLite-based job queue system.

This package provides a robust job queue system for managing long-running
document processing tasks. The system uses SQLite for job state management
and progress tracking, replacing the fragile subprocess-based approach.

Key components:
- queue.py: JobQueue class for job management
- jobs.py: Job execution functions (refactored from Process_Stage_2/3.py)
- run_worker.py: Worker process launcher
- schema.sql: Database schema definition
"""
# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License

from .queue import JobQueue

__all__ = ['JobQueue']
