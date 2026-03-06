# Worker System Documentation

The worker system provides a robust SQLite-based job queue for managing long-running document processing tasks.

## Architecture

```
┌─────────────────┐         ┌──────────────┐         ┌────────────────┐
│  Streamlit UI   │ ──────> │  jobs.db     │ <────── │ Worker Process │
│  or CLI         │ <────── │  (SQLite)    │         │                │
└─────────────────┘         └──────────────┘         └────────────────┘
     Submit job              Job queue                 Execute jobs
     Check status            Progress tracking         Update progress
```

## Components

### 1. JobQueue (`worker/queue.py`)

SQLite-based job queue manager. Provides methods for:
- `enqueue()` - Submit new jobs
- `get_next_job()` - Worker retrieves next queued job
- `update_progress()` - Update job progress during execution
- `complete_job()` - Mark job as completed
- `fail_job()` - Mark job as failed (with retry logic)
- `get_job_status()` - Query job status and progress
- `get_recent_jobs()` - List recent jobs
- `cleanup_old_jobs()` - Remove old completed/failed jobs

### 2. Job Execution Functions (`worker/jobs.py`)

Processing logic refactored from `Process_Stage_2.py` and `Process_Stage_3.py`:
- `process_stage_3_job()` - Execute Stage 3 processing
- `process_stage_2_job()` - Execute Stage 2 processing (planned)

These functions:
- Take a JobQueue instance and job dict as parameters
- Call `queue.update_progress()` during execution
- Return result dicts
- Log errors and progress via JobQueue

### 3. Worker Process (`worker/run_worker.py`)

Standalone process that:
- Polls job queue every 2 seconds
- Executes jobs and updates progress
- Can be restarted without losing jobs
- Handles errors and retries

### 4. Database Schema (`worker/schema.sql`)

Three tables:
- **jobs** - Job metadata and status
- **job_progress** - Real-time progress updates
- **job_logs** - Optional structured logging

## Usage

### Starting the Worker

```bash
# Start worker in foreground
python worker/run_worker.py

# With custom database path
python worker/run_worker.py --db /path/to/jobs.db

# With custom poll interval
python worker/run_worker.py --poll-interval 5
```

The worker will run until interrupted (Ctrl+C).

### Submitting Jobs

#### Using Python API:

```python
from worker.queue import JobQueue

# Initialize queue
queue = JobQueue('jobs.db')

# Submit a Stage 3 job
job_id = queue.enqueue(
    job_type='stage_3',
    file_path='/path/to/file_processed.json',
    params={
        'checkpoint_threshold': 30,
        'max_items': 100,
        'config': 'config.json'
    },
    max_retries=0
)

print(f'Job submitted: {job_id}')
```

#### Using Test Script:

```bash
# Submit a job
python testing/test_job_queue.py submit /path/to/file_processed.json 100

# Monitor job progress
python testing/test_job_queue.py monitor <job_id>

# Check job status
python testing/test_job_queue.py status <job_id>

# List recent jobs
python testing/test_job_queue.py list

# Show queue stats
python testing/test_job_queue.py stats
```

### Checking Status

```python
from worker.queue import JobQueue

queue = JobQueue('jobs.db')
status = queue.get_job_status(job_id)

print(f"Status: {status['status']}")  # queued, running, completed, failed
if status['progress']:
    prog = status['progress']
    print(f"Progress: {prog['items_done']}/{prog['total_items']}")
    print(f"Phase: {prog['phase']}")
    print(f"Current: {prog['current_item']}")
```

### Job Lifecycle

1. **queued** - Job submitted, waiting for worker
2. **running** - Worker is processing the job
3. **completed** - Job finished successfully
4. **failed** - Job failed (after retries exhausted)
5. **cancelled** - Job was cancelled

## Benefits Over Subprocess Approach

### 1. Fault Tolerance
- Worker crashes → job stays in queue, can be restarted
- UI crashes → worker continues processing
- Database is ACID-compliant (no data loss)

### 2. Observability
- Clear job lifecycle states
- Progress updates queryable via SQL
- Job history for debugging
- Structured logging

### 3. Windows-Friendly
- SQLite handles file locking correctly
- No subprocess management issues
- No external dependencies (sqlite3 built into Python)

### 4. Scalability
- Can run multiple workers (future)
- Can add job prioritization (future)
- Can add job dependencies (future)

## Error Handling

### Automatic Retries

If a job fails and `max_retries > 0`, it will automatically be re-queued:

```python
job_id = queue.enqueue(
    job_type='stage_3',
    file_path='/path/to/file.json',
    params={...},
    max_retries=2  # Retry up to 2 times
)
```

### Manual Retry

Re-submit a failed job:

```python
# Get failed job details
status = queue.get_job_status(failed_job_id)

# Submit new job with same parameters
new_job_id = queue.enqueue(
    status['job_type'],
    status['file_path'],
    params={...}
)
```

## Progress Updates

Jobs update progress every 5-10 seconds during execution:

```python
# In job execution function
queue.update_progress(
    job_id=job_id,
    phase='level_2_summaries',
    current_item='section_143c',
    items_done=42,
    total_items=150
)
```

The UI polls this progress and displays real-time updates.

## Database Maintenance

Clean up old completed/failed jobs:

```python
# Remove jobs older than 30 days
queue.cleanup_old_jobs(days=30)
```

## Troubleshooting

### Worker Not Processing Jobs

1. Check if worker is running:
   ```bash
   ps aux | grep run_worker  # Linux/Mac
   tasklist | findstr python  # Windows
   ```

2. Check database for errors:
   ```bash
   python testing/test_job_queue.py list
   ```

3. Check job logs:
   ```python
   logs = queue.get_job_logs(job_id)
   for log in logs:
       print(f"{log['timestamp']} [{log['level']}] {log['message']}")
   ```

### Jobs Stuck in "running" State

This can happen if worker crashed. To reset:

```python
# Manually re-queue stuck job
import sqlite3
conn = sqlite3.connect('jobs.db')
conn.execute("UPDATE jobs SET status = 'queued' WHERE id = ?", (job_id,))
conn.commit()
```

### Database Locked Errors

SQLite uses file-level locking. If you see "database locked" errors:
- Reduce poll interval in worker
- Ensure only one worker is running
- Check for long-running transactions

## Next Steps

1. **Test the system** - Use `testing/test_job_queue.py` to submit test jobs
2. **Run a worker** - Start `python worker/run_worker.py` in a terminal
3. **Monitor progress** - Use `testing/test_job_queue.py monitor <job_id>`
4. **Integrate with UI** - Update `ui/app.py` to use job queue

## Future Enhancements

- [ ] Job priorities (high/normal/low)
- [ ] Job dependencies (Job B waits for Job A)
- [ ] Multiple workers (parallel processing)
- [ ] Web dashboard for job monitoring
- [ ] Email notifications on job completion/failure
- [ ] Job scheduling (cron-like)
