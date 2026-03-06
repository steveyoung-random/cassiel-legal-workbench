"""
Worker process launcher for document processing jobs.

This script runs a worker process that continuously polls the job queue
for new jobs, executes them, and updates their status.

Usage:
    python worker/run_worker.py [--db PATH] [--poll-interval SECONDS]

The worker will run until interrupted (Ctrl+C) or killed.
"""
# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License

import argparse
import os
import sys
import time
import traceback
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from worker.queue import JobQueue
from worker.jobs import process_parse_job, process_stage_2_job, process_stage_3_job, process_question_job


def run_worker(db_path: str, poll_interval: float = 2.0):
    """
    Worker main loop.

    Args:
        db_path: Path to SQLite database file
        poll_interval: Seconds to wait between polling for jobs (default: 2.0)
    """
    print(f'Worker starting...')
    print(f'  Database: {db_path}')
    print(f'  Poll interval: {poll_interval} seconds')
    print(f'  Process ID: {os.getpid()}')
    print()

    # Initialize job queue
    queue = JobQueue(db_path)

    # Mark any stale 'running' jobs as cancelled (from previous worker sessions)
    stale_count = queue.mark_stale_jobs_cancelled()
    if stale_count > 0:
        print(f'Marked {stale_count} stale job(s) as cancelled')

    # Display initial queue stats
    stats = queue.get_queue_stats()
    print(f'Queue stats:')
    print(f'  Queued: {stats["queued"]}')
    print(f'  Running: {stats["running"]}')
    print(f'  Completed: {stats["completed"]}')
    print(f'  Failed: {stats["failed"]}')
    print()

    print('Worker ready. Polling for jobs...')
    print('Press Ctrl+C to stop')
    print()

    try:
        while True:
            # Get next job
            job = queue.get_next_job(worker_id=str(os.getpid()))

            if job is None:
                # No jobs available, wait and try again
                time.sleep(poll_interval)
                continue

            # Execute job based on type
            job_id = job['id']
            job_type = job['job_type']
            file_path = job['file_path']

            print(f'[Job {job_id}] Starting {job_type} processing')
            print(f'[Job {job_id}]   File: {os.path.basename(file_path)}')

            try:
                if job_type == 'parse':
                    result = process_parse_job(queue, job)
                elif job_type == 'stage_2':
                    result = process_stage_2_job(queue, job)
                elif job_type == 'stage_3':
                    result = process_stage_3_job(queue, job)
                elif job_type == 'question':
                    result = process_question_job(queue, job)
                else:
                    raise ValueError(f'Unknown job type: {job_type}')

                # Check if job is incomplete (hit max_items limit)
                if result.get('incomplete'):
                    reason = result.get('reason', 'Unknown reason')
                    print(f'[Job {job_id}] Incomplete: {reason}')
                    queue.log_message(job_id, 'WARNING', f'Job incomplete: {reason}')

                    # For incomplete jobs, mark as completed but with a note
                    # User can re-submit to continue processing
                    result['note'] = 'Job hit processing limit. Re-submit to continue.'

                # Mark job as completed
                queue.complete_job(job_id, result)
                print(f'[Job {job_id}] Completed successfully')

                # Display results based on job type
                if job_type == 'parse':
                    if 'files_created' in result:
                        print(f'[Job {job_id}]   Files created: {len(result["files_created"])}')
                    if 'parser_type' in result:
                        print(f'[Job {job_id}]   Parser: {result["parser_type"]}')

                elif job_type == 'stage_2':
                    if 'total_items' in result:
                        print(f'[Job {job_id}]   Total items: {result["total_items"]}')
                    if 'definitions_found' in result:
                        print(f'[Job {job_id}]   Definitions found: {result["definitions_found"]}')

                elif job_type == 'stage_3':
                    if 'operational_items' in result:
                        print(f'[Job {job_id}]   Operational items: {result["operational_items"]}')
                    if 'organizational_units' in result:
                        print(f'[Job {job_id}]   Organizational units: {result["organizational_units"]}')
                    if 'phases_completed' in result:
                        print(f'[Job {job_id}]   Phases: {", ".join(result["phases_completed"])}')

                elif job_type == 'question':
                    if 'question_text' in result:
                        q_text = result['question_text']
                        q_preview = q_text[:80] + '...' if len(q_text) > 80 else q_text
                        print(f'[Job {job_id}]   Question: {q_preview}')
                    if 'relevance_scores' in result:
                        print(f'[Job {job_id}]   Units scored: {result["relevance_scores"]}')
                    if 'answer_text' in result:
                        ans_len = len(result['answer_text'])
                        print(f'[Job {job_id}]   Answer length: {ans_len} chars')

                print()

            except Exception as e:
                # Log error details
                error_msg = str(e)
                tb = traceback.format_exc()

                print(f'[Job {job_id}] FAILED: {error_msg}')
                print(f'[Job {job_id}] Traceback:')
                print(tb)
                print()

                # Mark job as failed (will handle retry logic if max_retries > 0)
                queue.fail_job(job_id, error_msg)
                queue.log_message(job_id, 'ERROR', f'Job failed: {error_msg}')
                queue.log_message(job_id, 'ERROR', f'Traceback:\n{tb}')

    except KeyboardInterrupt:
        print()
        print('Worker interrupted by user. Shutting down...')
        return

    except Exception as e:
        print(f'Worker crashed: {e}')
        traceback.print_exc()
        return


def main():
    """Parse arguments and run worker."""
    parser = argparse.ArgumentParser(
        description='Run worker process for document processing job queue'
    )
    parser.add_argument(
        '--db',
        default='jobs.db',
        help='Path to SQLite database file (default: jobs.db in current directory)'
    )
    parser.add_argument(
        '--poll-interval',
        type=float,
        default=2.0,
        help='Seconds to wait between polling for jobs (default: 2.0)'
    )

    args = parser.parse_args()

    # Resolve database path (convert to absolute if relative)
    db_path = os.path.abspath(args.db)

    # Run worker
    run_worker(db_path, args.poll_interval)


if __name__ == '__main__':
    main()
