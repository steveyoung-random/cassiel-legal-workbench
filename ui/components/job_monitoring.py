"""
Job Monitoring Widget Component.

PHASE 2 IMPLEMENTATION - Reusable job monitoring component.

Provides sticky job monitoring that persists across navigation.
"""
# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License

import streamlit as st
import time
from typing import Dict, Optional
from datetime import datetime

from worker.queue import JobQueue
from utils.config import get_job_queue_database


def render(job_id: int, config: Dict, current_files: Optional[Dict] = None):
    """
    Render job monitoring widget with auto-refresh.

    This widget displays job status, progress, and handles auto-refresh
    for running jobs. It should be called on every render to ensure
    real-time updates.

    Args:
        job_id: Job ID to monitor
        config: Application configuration
        current_files: Optional dict with 'parse_file' and 'processed_file' keys
    """
    db_path = get_job_queue_database(config)
    queue = JobQueue(db_path)

    try:
        job_status = queue.get_job_status(job_id)
    except ValueError:
        # Job not found - could be a race condition if just submitted
        # Don't clear monitoring state immediately; wait for next refresh
        st.warning(f"⏳ Job {job_id} queued - waiting for worker to pick it up...")

        # Auto-refresh to check again
        time.sleep(2)
        st.rerun()
        return

    status = job_status['status']
    job_type = job_status.get('job_type', 'unknown')
    progress = job_status.get('progress')
    job_file_path = job_status.get('file_path')

    # Header with job info
    st.subheader("📊 Job Monitoring")

    # Job ID and type
    job_type_display = {
        'stage_2': 'Stage 2 (Definitions)',
        'stage_3': 'Stage 3 (Summaries)',
        'parse': 'Parse',
        'question': 'Question'
    }.get(job_type, job_type)

    st.caption(f"Job #{job_id} - {job_type_display}")

    # Check if viewing different document than being processed
    if current_files and job_file_path:
        from pathlib import Path
        job_file = Path(job_file_path).resolve()

        # Check against both parse_file and processed_file
        parse_file = current_files.get('parse_file')
        processed_file = current_files.get('processed_file')

        is_viewing_job_document = False
        current_file_name = None

        if parse_file:
            current_parse = Path(parse_file).resolve()
            if current_parse == job_file:
                is_viewing_job_document = True
                current_file_name = Path(parse_file).name

        if not is_viewing_job_document and processed_file:
            current_processed = Path(processed_file).resolve()
            if current_processed == job_file:
                is_viewing_job_document = True
                current_file_name = Path(processed_file).name

        if not is_viewing_job_document:
            st.warning(
                f"⚠️ You are viewing a different document. "
                f"This job is processing: `{Path(job_file_path).name}`"
            )
        else:
            # Show current file name
            st.caption(f"📄 {current_file_name}")
    elif job_file_path:
        # Show job file path even if not viewing it
        from pathlib import Path
        st.caption(f"📄 Processing: {Path(job_file_path).name}")

    st.divider()

    # Status display based on job state
    if status == 'queued':
        st.info("⏳ Job queued - waiting for worker")
        st.caption(f"Created: {_format_timestamp(job_status.get('created_at'))}")

        # Auto-refresh with countdown
        st.divider()
        countdown_placeholder = st.empty()

        # Countdown from 5 seconds
        for remaining in range(5, 0, -1):
            countdown_placeholder.caption(f"🔄 Checking status in {remaining} second{'s' if remaining > 1 else ''}...")
            time.sleep(1)

        countdown_placeholder.caption("🔄 Refreshing now...")
        st.rerun()

    elif status == 'running':
        # Show progress if available
        if progress:
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

            # Phase and current item
            st.caption(f"**Phase:** {phase}")
            if current_item:
                st.caption(f"**Current:** {current_item}")

            # Timestamp
            if progress.get('timestamp'):
                st.caption(f"Last update: {_format_timestamp(progress['timestamp'])}")
        else:
            st.info("🔄 Job running - waiting for progress update...")
            st.caption(f"Started: {_format_timestamp(job_status.get('started_at'))}")

        # Auto-refresh with countdown
        st.divider()
        countdown_placeholder = st.empty()

        # Countdown from 5 seconds
        for remaining in range(5, 0, -1):
            countdown_placeholder.caption(f"🔄 Auto-refreshing in {remaining} second{'s' if remaining > 1 else ''}...")
            time.sleep(1)

        countdown_placeholder.caption("🔄 Refreshing now...")
        st.rerun()

    elif status == 'completed':
        st.success("✅ Job completed successfully!")
        
        # Show completion time
        if job_status.get('completed_at'):
            st.caption(f"Completed: {_format_timestamp(job_status['completed_at'])}")

        # Show result summary if available
        result = job_status.get('result')
        if result:
            if isinstance(result, dict):
                # Display key result metrics
                if 'total_items' in result:
                    st.caption(f"Items processed: {result.get('total_items', 'N/A')}")
                if 'definitions_found' in result:
                    st.caption(f"Definitions found: {result.get('definitions_found', 'N/A')}")
                if 'summaries_generated' in result:
                    st.caption(f"Summaries generated: {result.get('summaries_generated', 'N/A')}")

        # Clear monitoring state and refresh UI
        if st.session_state.get('monitoring_job_id') == job_id:
            st.session_state.monitoring_job_id = None
            st.session_state.processing_active = False

            # Clear cache to refresh document list
            st.cache_data.clear()

            # Refresh selected document and chapter to pick up new files
            _refresh_selected_document_data(config)

            # Force rerun to update UI with new file paths and status
            st.rerun()

    elif status == 'failed':
        st.error("❌ Job failed")
        
        # Show error message
        error_msg = job_status.get('error_message')
        if error_msg:
            with st.expander("Error Details", expanded=True):
                st.code(error_msg, language='text')
        
        # Show completion time
        if job_status.get('completed_at'):
            st.caption(f"Failed: {_format_timestamp(job_status['completed_at'])}")

        # Clear monitoring state and refresh UI
        if st.session_state.get('monitoring_job_id') == job_id:
            st.session_state.monitoring_job_id = None
            st.session_state.processing_active = False

            # Refresh document data in case partial progress was saved
            st.cache_data.clear()
            _refresh_selected_document_data(config)

            # Force rerun to update UI
            st.rerun()

    elif status == 'cancelled':
        st.warning("⚠️ Job cancelled")

        # Clear monitoring state and refresh UI
        if st.session_state.get('monitoring_job_id') == job_id:
            st.session_state.monitoring_job_id = None
            st.session_state.processing_active = False

            # Refresh document data in case partial progress was saved
            st.cache_data.clear()
            _refresh_selected_document_data(config)

            # Force rerun to update UI
            st.rerun()

    # Action buttons
    st.divider()
    
    col1, col2 = st.columns(2)
    
    with col1:
        if status in ['queued', 'running']:
            if st.button("Cancel Job", use_container_width=True):
                with st.spinner("Cancelling job..."):
                    try:
                        queue.cancel_job(job_id)
                        st.success("✅ Job cancelled successfully")
                        st.session_state.monitoring_job_id = None
                        st.session_state.processing_active = False
                        st.rerun()
                    except Exception as e:
                        st.error(f"❌ Failed to cancel job: {e}")

    with col2:
        if st.button("View Logs", use_container_width=True):
            # Switch to Job History view to see logs
            st.session_state.view_mode = 'Job History'
            st.session_state.selected_job_id = job_id
            st.rerun()


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


def _refresh_selected_document_data(config: Dict):
    """
    Refresh selected document and chapter data from disk.

    After a job completes, the processed file may have been created or updated.
    This function reloads the document data to pick up these changes.

    Args:
        config: Application configuration
    """
    # Only refresh if we have a selected document and chapter
    selected_doc = st.session_state.get('selected_doc')
    selected_chapter = st.session_state.get('selected_chapter')

    if not selected_doc or not selected_chapter:
        return

    try:
        from utils.config import get_output_directory
        from ui.app import scan_documents

        # Get fresh document list (cache was just cleared)
        output_dir = get_output_directory(config)
        fresh_docs = scan_documents(output_dir)

        # Find the matching document by manifest_path
        manifest_path = selected_doc.get('manifest_path')
        if not manifest_path:
            return

        matching_doc = None
        for doc in fresh_docs:
            if doc.get('manifest_path') == manifest_path:
                matching_doc = doc
                break

        if not matching_doc:
            # Document no longer exists, clear selection
            st.session_state.selected_doc = None
            st.session_state.selected_chapter = None
            return

        # Update selected_doc with fresh data
        st.session_state.selected_doc = matching_doc

        # Find the matching chapter by parse_file path
        parse_file = selected_chapter.get('parse_file')
        if not parse_file:
            return

        matching_chapter = None
        for pf in matching_doc.get('parsed_files', []):
            if pf.get('parse_file') == parse_file:
                matching_chapter = pf
                break

        if matching_chapter:
            # Update selected_chapter with fresh data (includes new processed_file)
            st.session_state.selected_chapter = matching_chapter

    except Exception:
        # If refresh fails, don't crash - user can manually refresh
        pass
