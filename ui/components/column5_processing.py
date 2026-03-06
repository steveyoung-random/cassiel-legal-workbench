"""
Column 5: Processing Control Component.

PHASE 2 IMPLEMENTATION - Full processing controls and job monitoring.

This column shows processing controls, stage progression, and job monitoring.
"""
# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License

import streamlit as st
from typing import Dict, Optional
from pathlib import Path

from worker.queue import JobQueue
from utils.config import get_job_queue_database, get_checkpoint_threshold
from ui.components.job_monitoring import render as render_job_monitoring
from ui.app import get_document_status
from ui.utils import show_error, show_error_with_action


def render(config: Dict, output_dir: str):
    """
    Main render function for Column 5 (Processing Control).

    Shows:
    - Selected document/chapter information
    - Stage progression tracker (Parse → Stage 2 → Stage 3)
    - Processing parameter controls
    - Action buttons (Start Stage 2, Start Stage 3)
    - Quick question interface (for completed documents)
    - Sticky job monitoring widget

    Args:
        config: Application configuration
        output_dir: Output directory path
    """
    # Check if document/chapter is selected
    chapter = st.session_state.get('selected_chapter')
    doc = st.session_state.get('selected_doc', {})

    if not chapter:
        st.info("⚙️ **Processing Options**")
        st.caption("← Select a document from Columns 2-3 to view processing options")
        st.markdown("""
            **Available Actions:**
            - Submit Stage 2 (Definition Processing)
            - Submit Stage 3 (Summary Generation)
            - Ask Questions (for Stage 3 documents)
        """)
        return

    # Check if we're monitoring a job
    monitoring_job_id = st.session_state.get('monitoring_job_id')
    if monitoring_job_id:
        # Show sticky job monitoring widget at top
        # Pass both parse_file and processed_file so job_monitoring can match correctly
        current_files = {
            'parse_file': chapter.get('parse_file'),
            'processed_file': chapter.get('processed_file')
        }
        render_job_monitoring(monitoring_job_id, config, current_files)
        st.divider()

    # Document/Chapter Info Header
    st.header("Processing Control")

    # Show document title
    doc_title = doc.get('short_title', 'Unknown Document')
    st.caption(f"📄 {doc_title}")

    # Show organizational units if available
    org_units = chapter.get('organizational_units', {})
    if org_units:
        from ui.structure_display import format_organizational_units
        st.caption(f"📍 {format_organizational_units(org_units)}")

    st.divider()

    # Get document status
    status = get_document_status(chapter)

    # Stage Progression Tracker
    render_stage_progression(status, chapter)

    st.divider()

    # Processing Parameters Section
    render_processing_parameters(config)

    st.divider()

    # Action Buttons Section
    render_action_buttons(chapter, status, config)

    st.divider()

    # Quick Question Interface (for Stage 3 complete documents)
    if status.get('stage_3_complete'):
        render_quick_question_interface(chapter, config)


def render_stage_progression(status: Dict, chapter: Dict):
    """
    Render visual stage progression tracker.

    Shows completion status for Parse → Stage 2 → Stage 3.

    Args:
        status: Document status dictionary
        chapter: Chapter/parsed file dictionary
    """
    st.subheader("Stage Progression")

    # Stage 1: Parse
    parse_file = chapter.get('parse_file')
    parse_complete = parse_file and Path(parse_file).exists()
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if parse_complete:
            st.success("✅ Parse")
        else:
            st.info("⏳ Parse")
    
    with col2:
        if status.get('stage_2_complete'):
            st.success("✅ Stage 2")
        elif parse_complete:
            st.warning("⏳ Stage 2")
        else:
            st.info("— Stage 2")
    
    with col3:
        if status.get('stage_3_complete'):
            st.success("✅ Stage 3")
        elif status.get('stage_2_complete'):
            st.warning("⏳ Stage 3")
        else:
            st.info("— Stage 3")

    # Show file paths in expander
    with st.expander("File Paths", expanded=False):
        st.caption(f"**Parse File:**")
        st.code(parse_file if parse_file else "Not available", language='text')
        
        processed_file = chapter.get('processed_file')
        st.caption(f"**Processed File:**")
        st.code(processed_file if processed_file else "Not available", language='text')


def render_processing_parameters(config: Dict):
    """
    Render processing parameter controls.

    Args:
        config: Application configuration
    """
    st.subheader("Processing Parameters")

    # Checkpoint threshold
    default_checkpoint = get_checkpoint_threshold(config)
    checkpoint_threshold = st.number_input(
        "Checkpoint Threshold",
        min_value=1,
        max_value=100,
        value=st.session_state.get('checkpoint_threshold', default_checkpoint),
        help="Number of items to process before saving checkpoint"
    )
    st.session_state.checkpoint_threshold = checkpoint_threshold

    # Max items
    max_items = st.number_input(
        "Max Items",
        min_value=1,
        max_value=100000,
        value=st.session_state.get('max_items', 300),
        help="Maximum number of items to process in this run"
    )
    st.session_state.max_items = max_items


def render_action_buttons(chapter: Dict, status: Dict, config: Dict):
    """
    Render action buttons for job submission.

    Args:
        chapter: Chapter/parsed file dictionary
        status: Document status dictionary
        config: Application configuration
    """
    st.subheader("Actions")

    parse_file = chapter.get('parse_file')
    processed_file = chapter.get('processed_file')

    # Check for active jobs - use monitoring_job_id if available (more reliable),
    # otherwise fall back to file path matching
    db_path = get_job_queue_database(config)
    queue = JobQueue(db_path)
    
    stage_2_job_active = False
    stage_3_job_active = False
    
    # First, check if we have a monitored job that's still active
    monitoring_job_id = st.session_state.get('monitoring_job_id')
    if monitoring_job_id:
        try:
            job_status = queue.get_job_status(monitoring_job_id)
            if job_status['status'] in ('queued', 'running'):
                monitored_job_type = job_status.get('job_type')
                if monitored_job_type == 'stage_2':
                    stage_2_job_active = True
                elif monitored_job_type == 'stage_3':
                    stage_3_job_active = True
        except (ValueError, Exception):
            # Job not found or error - treat as not active, will check by file path below
            pass
    
    # Also check by file path as fallback (in case monitoring_job_id is cleared or not set)
    if not stage_2_job_active and parse_file and Path(parse_file).exists():
        stage_2_job_active = queue.has_active_job(parse_file, 'stage_2')
    
    if not stage_3_job_active and processed_file and Path(processed_file).exists():
        stage_3_job_active = queue.has_active_job(processed_file, 'stage_3')

    # Stage 2 Button
    parse_complete = parse_file and Path(parse_file).exists()
    stage_2_complete = status.get('stage_2_complete', False)

    if not parse_complete:
        st.button(
            "Start Stage 2",
            disabled=True,
            use_container_width=True,
            help="Parse must be completed first",
            key="stage2_btn_disabled_parse"
        )
    elif stage_2_complete:
        st.button(
            "Stage 2 Complete",
            disabled=True,
            use_container_width=True,
            help="Stage 2 processing already completed",
            key="stage2_btn_disabled_complete"
        )
    elif stage_2_job_active:
        st.button(
            "⏳ Stage 2 Running...",
            disabled=True,
            use_container_width=True,
            help="A Stage 2 job is already running for this document",
            key="stage2_btn_disabled_running"
        )
    else:
        if st.button("Start Stage 2", type="primary", use_container_width=True, key="stage2_btn_active"):
            st.session_state.submit_stage_2_requested = True
            st.session_state.submit_stage_2_chapter = chapter

    # Stage 3 Button
    stage_3_complete = status.get('stage_3_complete', False)

    if not stage_2_complete:
        st.button(
            "Start Stage 3",
            disabled=True,
            use_container_width=True,
            help="Stage 2 must be completed first",
            key="stage3_btn_disabled_stage2"
        )
    elif stage_3_complete:
        st.button(
            "Stage 3 Complete",
            disabled=True,
            use_container_width=True,
            help="Stage 3 processing already completed",
            key="stage3_btn_disabled_complete"
        )
    elif stage_3_job_active:
        st.button(
            "⏳ Stage 3 Running...",
            disabled=True,
            use_container_width=True,
            help="A Stage 3 job is already running for this document",
            key="stage3_btn_disabled_running"
        )
    else:
        if st.button("Start Stage 3", type="primary", use_container_width=True, key="stage3_btn_active"):
            st.session_state.submit_stage_3_requested = True
            st.session_state.submit_stage_3_chapter = chapter

    # Handle job submissions (after all buttons have been rendered)
    # This runs after the button click, using the flags set above
    if st.session_state.get('submit_stage_2_requested'):
        _handle_stage_2_submission(config)

    if st.session_state.get('submit_stage_3_requested'):
        _handle_stage_3_submission(config)


def _handle_stage_2_submission(config: Dict):
    """Handle Stage 2 job submission after button click."""
    chapter = st.session_state.get('submit_stage_2_chapter')

    # Clear the request flag first
    st.session_state.submit_stage_2_requested = False

    if not chapter:
        st.error("Error: Chapter data not found")
        return

    parse_file = chapter.get('parse_file')

    if not parse_file or not Path(parse_file).exists():
        st.error("Parse file not found. Please parse the document first.")
        return

    try:
        with st.spinner("Submitting Stage 2 job..."):
            db_path = get_job_queue_database(config)
            queue = JobQueue(db_path)

            params = {
                'checkpoint_threshold': st.session_state.get('checkpoint_threshold', 30),
                'max_items': st.session_state.get('max_items', 300),
                'config': 'config.json'
            }

            job_id = queue.enqueue(
                job_type='stage_2',
                file_path=parse_file,
                params=params,
                max_retries=0
            )

            # Set monitoring state
            st.session_state.monitoring_job_id = job_id
            st.session_state.processing_active = True

            st.success(f"✅ Stage 2 job {job_id} submitted successfully!")
            st.rerun()

    except Exception as e:
        show_error_with_action(st,
            "Failed to submit Stage 2 job",
            "Check worker status",
            "Ensure worker is running: python worker/run_worker.py",
            exception=e)


def _handle_stage_3_submission(config: Dict):
    """Handle Stage 3 job submission after button click."""
    chapter = st.session_state.get('submit_stage_3_chapter')

    # Clear the request flag first
    st.session_state.submit_stage_3_requested = False

    if not chapter:
        st.error("Error: Chapter data not found")
        return

    processed_file = chapter.get('processed_file')

    if not processed_file or not Path(processed_file).exists():
        st.error("Processed file not found. Please complete Stage 2 first.")
        return

    try:
        with st.spinner("Submitting Stage 3 job..."):
            db_path = get_job_queue_database(config)
            queue = JobQueue(db_path)

            params = {
                'checkpoint_threshold': st.session_state.get('checkpoint_threshold', 30),
                'max_items': st.session_state.get('max_items', 300),
                'config': 'config.json'
            }

            job_id = queue.enqueue(
                job_type='stage_3',
                file_path=processed_file,
                params=params,
                max_retries=0
            )

            # Set monitoring state
            st.session_state.monitoring_job_id = job_id
            st.session_state.processing_active = True

            st.success(f"✅ Stage 3 job {job_id} submitted successfully!")
            st.rerun()

    except Exception as e:
        show_error_with_action(st,
            "Failed to submit Stage 3 job",
            "Check worker status",
            "Ensure worker is running: python worker/run_worker.py",
            exception=e)


def render_quick_question_interface(chapter: Dict, config: Dict):
    """
    Render quick question interface for Stage 3 complete documents.

    Args:
        chapter: Chapter/parsed file dictionary
        config: Application configuration
    """
    st.subheader("Ask a Question")

    processed_file = chapter.get('processed_file')
    
    if not processed_file or not Path(processed_file).exists():
        return

    # Clear question input if flag is set (before widget creation)
    if st.session_state.get('clear_quick_question_input', False):
        st.session_state.quick_question_input = ""
        st.session_state.clear_quick_question_input = False

    question_text = st.text_area(
        "Enter your question about this document:",
        height=100,
        placeholder="e.g., What are the requirements for X?",
        key="quick_question_input"
    )

    col1, col2 = st.columns([3, 1])
    
    with col1:
        submit_enabled = bool(question_text.strip())

    with col2:
        # Classic Streamlit button pattern
        if st.button("Submit", type="primary", use_container_width=True, disabled=not submit_enabled, key="submit_question_btn"):
            st.session_state.submit_question_requested = True
            st.session_state.submit_question_text = question_text.strip()
            st.session_state.submit_question_chapter = chapter

    # Handle question submission (after button rendered)
    if st.session_state.get('submit_question_requested'):
        _handle_question_submission(config)


def _handle_question_submission(config: Dict):
    """Handle question job submission after button click."""
    chapter = st.session_state.get('submit_question_chapter')
    question_text = st.session_state.get('submit_question_text', '')

    # Clear the request flag first
    st.session_state.submit_question_requested = False

    if not chapter:
        st.error("Error: Chapter data not found")
        return

    processed_file = chapter.get('processed_file')

    if not processed_file or not Path(processed_file).exists():
        st.error("Processed file not found.")
        return

    if not question_text:
        st.error("Please enter a question.")
        return

    try:
        with st.spinner("Submitting question..."):
            db_path = get_job_queue_database(config)
            queue = JobQueue(db_path)

            params = {
                'question_text': question_text,
                'max_items': st.session_state.get('max_items', 300),
                'max_tokens': 1000,
                'max_iterations': 3,
                'qa_mode': st.session_state.get('qa_mode', 'standard'),
                'config': 'config.json'
            }

            job_id = queue.enqueue(
                job_type='question',
                file_path=processed_file,
                params=params,
                max_retries=0
            )

            # Set monitoring state
            st.session_state.monitoring_job_id = job_id
            st.session_state.processing_active = True
            st.session_state.clear_quick_question_input = True

            st.success(f"✅ Question submitted successfully! Job ID: {job_id}")
            st.rerun()

    except Exception as e:
        show_error_with_action(st,
            "Failed to submit question",
            "Check worker status",
            "Ensure worker is running: python worker/run_worker.py",
            exception=e)
