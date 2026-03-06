"""
Job History View Component.

PHASE 3 IMPLEMENTATION - Full job history interface.

Displays complete job history with filtering, management, and log viewing.
"""
# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License

import streamlit as st
import time
from typing import Dict, List, Optional, Any
from datetime import datetime
from pathlib import Path

from worker.queue import JobQueue
from utils.config import get_job_queue_database
from ui.utils import show_error, show_error_with_action, show_error_with_retry


def render(config: Dict, output_dir: str):
    """
    Render job history view.

    Main entry point called from column_layout.py render_job_history_mode().

    Args:
        config: Application configuration
        output_dir: Output directory path
    """
    st.header("📋 Job History")

    # Initialize session state for job history
    # Note: Session state is initialized globally in ui/utils.py
    # We don't need _init_job_history_state() since defaults are already set

    # Get queue instance
    db_path = get_job_queue_database(config)
    queue = JobQueue(db_path)

    # Render queue statistics panel
    render_queue_stats(queue)

    st.divider()

    # Render job filtering controls
    render_job_filters()

    st.divider()

    # Render job list with pagination
    render_job_list(queue, output_dir)

    # Auto-refresh logic (check if any jobs are running)
    _handle_auto_refresh(queue)


def render_queue_stats(queue: JobQueue):
    """
    Render queue statistics panel at top.

    Shows counts by status in color-coded metric cards.

    Args:
        queue: JobQueue instance
    """
    st.subheader("Queue Statistics")

    stats = queue.get_queue_stats()

    col1, col2, col3, col4, col5 = st.columns(5)

    with col1:
        st.metric("Queued", stats.get('queued', 0))

    with col2:
        running = stats.get('running', 0)
        st.metric("Running", running)
        if running > 0:
            st.caption("🟢 Active")

    with col3:
        st.metric("Completed", stats.get('completed', 0))

    with col4:
        failed = stats.get('failed', 0)
        st.metric("Failed", failed)
        if failed > 0:
            st.caption("🔴 Attention needed")

    with col5:
        st.metric("Cancelled", stats.get('cancelled', 0))


def render_job_filters():
    """Render job filtering controls."""
    st.subheader("Filter Jobs")

    col1, col2 = st.columns([3, 1])

    with col1:
        filter_options = ['all', 'queued', 'running', 'completed', 'failed', 'cancelled']
        current_filter = st.session_state.job_history_filter_status

        # Ensure current filter is valid
        if current_filter not in filter_options:
            current_filter = 'all'
            st.session_state.job_history_filter_status = 'all'

        filter_status = st.selectbox(
            "Status Filter",
            filter_options,
            index=filter_options.index(current_filter),
            key='job_filter_selectbox'
        )

        if filter_status != st.session_state.job_history_filter_status:
            st.session_state.job_history_filter_status = filter_status
            st.session_state.job_history_page = 0  # Reset to first page
            st.rerun()

    with col2:
        if st.button("🔄 Refresh", use_container_width=True):
            with st.spinner("Refreshing..."):
                st.cache_data.clear()
            st.rerun()


def render_job_list(queue: JobQueue, output_dir: str):
    """
    Render paginated job list with expandable details.

    Args:
        queue: JobQueue instance
        output_dir: Output directory path
    """
    st.subheader("Jobs")

    # Get jobs with current filter
    filter_status = st.session_state.job_history_filter_status
    status_filter = None if filter_status == 'all' else filter_status

    # Pagination
    page_size = 50

    # Get jobs (API returns recent jobs, we'll implement client-side pagination)
    try:
        all_jobs = queue.get_recent_jobs(limit=1000, status=status_filter)
    except Exception as e:
        def retry_load():
            st.rerun()
        show_error_with_retry(st, "Failed to fetch jobs from database", retry_load, "Reload Jobs", exception=e)
        return

    total_jobs = len(all_jobs)
    total_pages = (total_jobs + page_size - 1) // page_size if total_jobs > 0 else 1

    # Ensure current page is valid
    if st.session_state.job_history_page >= total_pages:
        st.session_state.job_history_page = max(0, total_pages - 1)

    # Paginated slice
    offset = st.session_state.job_history_page * page_size
    start_idx = offset
    end_idx = min(start_idx + page_size, total_jobs)
    page_jobs = all_jobs[start_idx:end_idx]

    if not page_jobs:
        st.info("No jobs found matching current filter.")
        return

    # Pagination controls (top)
    _render_pagination_controls(total_pages, total_jobs, start_idx, end_idx, position="top")

    st.divider()

    # Render each job as expandable card
    for job in page_jobs:
        render_job_card(job, queue, output_dir)

    st.divider()

    # Pagination controls (bottom)
    _render_pagination_controls(total_pages, total_jobs, start_idx, end_idx, position="bottom")


def _render_pagination_controls(total_pages: int, total_jobs: int, start_idx: int, end_idx: int, position: str = "top"):
    """
    Render pagination controls.

    Args:
        total_pages: Total number of pages
        total_jobs: Total number of jobs
        start_idx: Start index of current page
        end_idx: End index of current page
    """
    if total_pages <= 1:
        return

    col1, col2, col3 = st.columns([1, 2, 1])

    with col1:
        if st.session_state.job_history_page > 0:
            if st.button("← Previous", use_container_width=True, key=f"prev_{position}_{start_idx}"):
                st.session_state.job_history_page -= 1
                st.rerun()

    with col2:
        st.caption(
            f"Showing {start_idx + 1}-{end_idx} of {total_jobs} jobs | "
            f"Page {st.session_state.job_history_page + 1} of {total_pages}"
        )

    with col3:
        if st.session_state.job_history_page < total_pages - 1:
            if st.button("Next →", use_container_width=True, key=f"next_{position}_{start_idx}"):
                st.session_state.job_history_page += 1
                st.rerun()


def render_job_card(job: Dict[str, Any], queue: JobQueue, output_dir: str):
    """
    Render expandable job card with details.

    Args:
        job: Job dictionary from get_recent_jobs()
        queue: JobQueue instance
        output_dir: Output directory path
    """
    job_id = job['id']
    status = job['status']
    job_type = job.get('job_type', 'unknown')
    file_path = job.get('file_path', '')

    # Status icon
    status_icons = {
        'queued': '⏳',
        'running': '🔄',
        'completed': '✅',
        'failed': '❌',
        'cancelled': '⚠️'
    }
    icon = status_icons.get(status, '❓')

    # Job type display
    job_type_display = {
        'stage_2': 'Stage 2',
        'stage_3': 'Stage 3',
        'parse': 'Parse',
        'question': 'Question'
    }.get(job_type, job_type)

    # File name
    file_name = Path(file_path).name if file_path else 'N/A'

    # Expandable container
    with st.expander(
        f"{icon} **Job #{job_id}** - {job_type_display} - {file_name} - {status.upper()}",
        expanded=(st.session_state.selected_job_id == job_id)
    ):
        # When expanded, get full job details
        try:
            job_status = queue.get_job_status(job_id)
        except ValueError:
            st.error(f"Job {job_id} not found")
            return
        except Exception as e:
            st.error(f"Failed to get job status: {e}")
            return

        # Render job details
        render_job_details(job_status, queue, output_dir)


def render_job_details(job_status: Dict[str, Any], queue: JobQueue, output_dir: str):
    """
    Render detailed job information inside expanded card.

    Args:
        job_status: Full job status from get_job_status()
        queue: JobQueue instance
        output_dir: Output directory path
    """
    job_id = job_status['id']
    status = job_status['status']

    # Basic info section
    st.caption("**Job Information**")

    col1, col2 = st.columns(2)

    with col1:
        st.caption(f"**Job ID:** {job_id}")
        st.caption(f"**Type:** {job_status.get('job_type', 'N/A')}")
        st.caption(f"**Status:** {status}")

    with col2:
        st.caption(f"**Created:** {_format_timestamp(job_status.get('created_at'))}")
        if job_status.get('started_at'):
            st.caption(f"**Started:** {_format_timestamp(job_status.get('started_at'))}")
        if job_status.get('completed_at'):
            st.caption(f"**Completed:** {_format_timestamp(job_status.get('completed_at'))}")

    # File path
    st.caption("**File Path:**")
    st.code(job_status.get('file_path', 'N/A'), language='text')

    st.divider()

    # Status-specific sections
    if status == 'running':
        render_job_progress(job_status)

    elif status == 'completed':
        render_job_result(job_status, output_dir)

    elif status == 'failed':
        render_job_error(job_status)

    st.divider()

    # Action buttons
    render_job_actions(job_id, status, queue)


def render_job_progress(job_status: Dict[str, Any]):
    """
    Render progress information for running job.

    Args:
        job_status: Job status dictionary
    """
    progress = job_status.get('progress')

    if progress:
        st.caption("**Progress**")

        items_done = progress.get('items_done', 0)
        total_items = progress.get('total_items', 1)
        phase = progress.get('phase', 'processing')
        current_item = progress.get('current_item', '')

        # Progress bar
        if total_items > 0:
            progress_pct = items_done / total_items
            st.progress(progress_pct, text=f"{items_done}/{total_items} items")
        else:
            st.progress(0.0, text="Processing...")

        st.caption(f"**Phase:** {phase}")
        if current_item:
            st.caption(f"**Current Item:** {current_item}")

        if progress.get('timestamp'):
            st.caption(f"**Last Update:** {_format_timestamp(progress['timestamp'])}")
    else:
        st.info("Waiting for progress update...")


def render_job_result(job_status: Dict[str, Any], output_dir: str):
    """
    Render result summary for completed job.

    Args:
        job_status: Job status dictionary
        output_dir: Output directory path
    """
    st.caption("**Result**")

    result = job_status.get('result')

    if result and isinstance(result, dict):
        # Display key metrics
        if 'total_items' in result:
            st.caption(f"✅ Items processed: {result.get('total_items', 'N/A')}")
        if 'definitions_found' in result:
            st.caption(f"📖 Definitions found: {result.get('definitions_found', 'N/A')}")
        if 'summaries_generated' in result:
            st.caption(f"📝 Summaries generated: {result.get('summaries_generated', 'N/A')}")
        if 'question_file' in result:
            st.caption(f"❓ Question file: {Path(result['question_file']).name}")

        # Full result in expander
        with st.expander("Full Result Details", expanded=False):
            st.json(result)
    else:
        st.info("No detailed result available")


def render_job_error(job_status: Dict[str, Any]):
    """
    Render error information for failed job.

    Args:
        job_status: Job status dictionary
    """
    st.caption("**Error**")

    error_msg = job_status.get('error_message')

    if error_msg:
        st.error(error_msg)
    else:
        st.error("Job failed with unknown error")


def render_job_actions(job_id: int, status: str, queue: JobQueue):
    """
    Render action buttons for job.

    Args:
        job_id: Job ID
        status: Job status
        queue: JobQueue instance
    """
    col1, col2 = st.columns(2)

    with col1:
        # Cancel button (only for queued/running)
        if status in ['queued', 'running']:
            if st.button("❌ Cancel Job", key=f"cancel_{job_id}", use_container_width=True):
                with st.spinner("Cancelling job..."):
                    try:
                        queue.cancel_job(job_id)
                        st.success("✅ Job cancelled successfully")
                        st.rerun()
                    except Exception as e:
                        show_error(st, "Failed to cancel job", exception=e)

    with col2:
        # View logs button
        if st.button("📄 View Logs", key=f"logs_{job_id}", use_container_width=True):
            st.session_state.show_logs_job_id = job_id
            st.rerun()

    # Log viewer (modal-like behavior using expander)
    if st.session_state.show_logs_job_id == job_id:
        render_log_viewer(job_id, queue)


def render_log_viewer(job_id: int, queue: JobQueue):
    """
    Render log viewer for job.

    Uses st.expander to simulate modal behavior.

    Args:
        job_id: Job ID
        queue: JobQueue instance
    """
    st.divider()
    st.caption("**Job Logs**")

    try:
        logs = queue.get_job_logs(job_id, limit=100)

        if logs:
            # Reverse to show oldest first
            logs.reverse()

            # Display in code block
            log_text = "\n".join([
                f"[{log['timestamp']}] {log['level']}: {log['message']}"
                for log in logs
            ])

            st.code(log_text, language='log')
        else:
            st.info("No logs available for this job")

    except Exception as e:
        show_error(st, "Failed to load job logs", exception=e)

    # Close button
    if st.button("Close Logs", key=f"close_logs_{job_id}"):
        st.session_state.show_logs_job_id = None
        st.rerun()


def _handle_auto_refresh(queue: JobQueue):
    """
    Handle auto-refresh when jobs are running.

    Checks if any jobs are in 'running' or 'queued' status.
    If yes, shows countdown and triggers rerun.

    Args:
        queue: JobQueue instance
    """
    try:
        stats = queue.get_queue_stats()

        has_active_jobs = (
            stats.get('running', 0) > 0 or
            stats.get('queued', 0) > 0
        )

        if has_active_jobs:
            # Show auto-refresh with countdown
            st.divider()
            countdown_placeholder = st.empty()

            # Countdown from 5 seconds
            for remaining in range(5, 0, -1):
                countdown_placeholder.caption(f"🔄 Auto-refreshing in {remaining} second{'s' if remaining > 1 else ''}...")
                time.sleep(1)

            countdown_placeholder.caption("🔄 Refreshing now...")
            st.rerun()
    except Exception:
        # If stats fetch fails, don't auto-refresh
        pass


def _format_timestamp(timestamp_str: Optional[str]) -> str:
    """
    Format timestamp string for display.

    Args:
        timestamp_str: ISO format timestamp string

    Returns:
        Formatted timestamp string
    """
    if not timestamp_str:
        return "N/A"

    try:
        # Parse ISO format timestamp
        dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
        return dt.strftime('%Y-%m-%d %H:%M:%S')
    except Exception:
        return timestamp_str
