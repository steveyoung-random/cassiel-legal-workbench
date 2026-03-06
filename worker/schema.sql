-- SQLite Job Queue Database Schema
-- This schema defines the tables for managing document processing jobs,
-- progress tracking, and optional logging.

-- Jobs table: Core job tracking
CREATE TABLE IF NOT EXISTS jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    status TEXT NOT NULL CHECK(status IN ('queued', 'running', 'completed', 'failed', 'cancelled')),
    job_type TEXT NOT NULL CHECK(job_type IN ('stage_2', 'stage_3', 'parse', 'question')),
    parser_type TEXT,  -- Required for parse jobs: 'uslm', 'formex', 'ca_html', etc.
    file_path TEXT NOT NULL,
    params TEXT,  -- JSON blob: {checkpoint_threshold, max_items, parse_mode, specific_units, etc.}
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    worker_id TEXT,  -- Process ID of worker executing job
    error_message TEXT,
    result TEXT,  -- JSON blob: {items_processed, errors, files_created, etc.}
    retry_count INTEGER DEFAULT 0,
    max_retries INTEGER DEFAULT 0
);

-- Progress table: Real-time progress updates
CREATE TABLE IF NOT EXISTS job_progress (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER NOT NULL,
    phase TEXT NOT NULL,  -- 'level_1_summaries', 'level_2_summaries', 'definition_extraction', etc.
    current_item TEXT,  -- 'section_143c', 'article_5', etc.
    items_done INTEGER NOT NULL,
    total_items INTEGER NOT NULL,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE
);

-- Logs table (optional): Structured job logging
CREATE TABLE IF NOT EXISTS job_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER NOT NULL,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    level TEXT NOT NULL CHECK(level IN ('DEBUG', 'INFO', 'WARNING', 'ERROR')),
    message TEXT NOT NULL,
    FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
CREATE INDEX IF NOT EXISTS idx_jobs_created ON jobs(created_at);
CREATE INDEX IF NOT EXISTS idx_jobs_type ON jobs(job_type);
CREATE INDEX IF NOT EXISTS idx_progress_job_id ON job_progress(job_id);
CREATE INDEX IF NOT EXISTS idx_progress_timestamp ON job_progress(timestamp);
CREATE INDEX IF NOT EXISTS idx_logs_job_id ON job_logs(job_id);
