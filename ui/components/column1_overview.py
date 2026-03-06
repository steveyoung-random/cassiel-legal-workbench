"""
Column 1: Project Overview Component.

Displays project-level statistics, system status, view mode selector, and global actions.
Always visible in all view modes.

Features:
- Document statistics (total, parsed, Stage 2/3 complete)
- View mode selector (Browse Documents, Job History, Global Questions)
- Parse New Document expander
- Worker status
- Cache information
"""
# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License

import streamlit as st
import os
from pathlib import Path
from typing import Dict

from worker.queue import JobQueue
from utils.config import get_job_queue_database, get_checkpoint_threshold
from parsers.registry import get_registry, load_parsers_from_config
from ui.utils import show_error, show_error_with_action


def get_document_statistics(output_dir: str) -> Dict[str, int]:
    """
    Calculate document statistics from scanned documents.

    Args:
        output_dir: Output directory path

    Returns:
        Dict with keys: total, parsed_only, stage_2_done, fully_processed
    """
    from ui.app import scan_documents
    from ui.utils import get_overall_document_status

    docs = scan_documents(output_dir)
    total = len(docs)

    parsed_only = 0
    stage_2_done = 0
    fully_processed = 0

    for doc in docs:
        status = get_overall_document_status(doc)

        if status['all_stage_3']:
            fully_processed += 1
        elif status['all_stage_2']:
            stage_2_done += 1
        elif status['any_parsed']:
            parsed_only += 1

    return {
        'total': total,
        'parsed_only': parsed_only,
        'stage_2_done': stage_2_done,
        'fully_processed': fully_processed
    }


def render_parse_new_document(config: Dict, output_dir: str):
    """
    Render the 'Parse New Document' expander with full parsing interface.

    Args:
        config: Application configuration
        output_dir: Output directory path
    """
    st.subheader("Parse New Document")

    # Load parsers from config before getting registry
    load_parsers_from_config(config)
    
    # Get parser registry
    registry = get_registry()
    parser_types = registry.list_parsers()  # Returns list of parser type strings

    if not parser_types:
        show_error_with_action(st,
            "No parsers available",
            "To check configuration",
            "Verify parsers/ directory exists and config.json has correct parser settings")
        return

    # Parser selection
    parser_type = st.selectbox(
        "Parser Type",
        parser_types,
        format_func=lambda x: registry.get(x).get_capabilities().display_name,
        key="parse_parser_type"
    )

    # File path input
    source_file = st.text_input(
        "Source File Path",
        placeholder="C:\\path\\to\\source\\file.xml",
        key="parse_source_file"
    )

    # Get parser capabilities
    parser = registry.get(parser_type)
    capabilities = parser.get_capabilities()

    # Parse mode selection
    if capabilities.supports_splitting:
        parse_mode = st.radio(
            "Parse Mode",
            ["auto", "full", "split"],
            format_func=lambda x: {
                'auto': 'Auto (detect and split if needed)',
                'full': 'Full Document (no splitting)',
                'split': 'Split by Units'
            }[x],
            horizontal=True,
            key="parse_mode"
        )

        # Specific units (if split mode)
        specific_units = None
        if parse_mode == 'split':
            units_input = st.text_area(
                f"Specific Units ({capabilities.split_unit_name}s, comma-separated)",
                placeholder="1, 5, 10-15",
                help="Leave empty to split all units",
                key="parse_specific_units"
            )
            if units_input.strip():
                specific_units = [u.strip() for u in units_input.split(',')]
    else:
        parse_mode = 'full'
        specific_units = None
        st.info(f"This parser does not support splitting.")

    # Submit button
    if st.button("Submit Parse Job", type="primary", use_container_width=True):
        if not source_file.strip():
            st.error("Please enter a source file path")
            return

        if not os.path.exists(source_file):
            st.error(f"File not found: {source_file}")
            return

        # Submit to job queue
        try:
            queue = JobQueue(get_job_queue_database(config))

            params = {
                'parse_mode': parse_mode,
                'specific_units': specific_units,
                'config': 'config.json'
            }

            job_id = queue.enqueue(
                job_type='parse',
                parser_type=parser_type,
                file_path=source_file,
                params=params,
                max_retries=0
            )

            st.success(f"Parse job submitted! Job ID: {job_id}")
            st.info("Check Job History to monitor progress.")

        except Exception as e:
            show_error_with_action(st,
                "Failed to submit parse job",
                "Check worker status",
                "Ensure worker is running: python worker/run_worker.py",
                exception=e)


def render_worker_status(config: Dict):
    """
    Render worker status indicator.

    Args:
        config: Application configuration
    """
    st.subheader("Worker Status")

    try:
        queue = JobQueue(get_job_queue_database(config))
        stats = queue.get_queue_stats()

        # Simple heuristic: worker is running if there are running jobs
        # or if there are queued jobs that aren't stuck
        running_jobs = stats.get('running', 0)

        if running_jobs > 0:
            st.success("✅ Active")
            st.caption(f"{running_jobs} job(s) running")
        else:
            queued_jobs = stats.get('queued', 0)
            if queued_jobs > 0:
                st.warning("⏳ Idle (jobs queued)")
                st.caption(f"{queued_jobs} job(s) waiting")
            else:
                st.info("💤 Idle")
                st.caption("No jobs in queue")

    except Exception as e:
        show_error(st, "Unable to check worker status", exception=e)


def render_cache_info(config: Dict):
    """
    Render cache information.

    Args:
        config: Application configuration
    """
    from ui.app import get_cache_stats

    st.subheader("Cache Info")

    try:
        cache_stats = get_cache_stats(config)

        if cache_stats['exists']:
            st.caption(f"📦 {cache_stats['total_entries']} entries")
            st.caption(f"💾 {cache_stats['size_mb']:.1f} MB")
        else:
            st.caption("No cache file found")

    except Exception as e:
        st.error("Unable to read cache")
        with st.expander("Show technical details", expanded=False):
            st.code(str(e), language='text')


def render(config: Dict, output_dir: str):
    """
    Main render function for Column 1 (Project Overview).

    Args:
        config: Application configuration
        output_dir: Output directory path
    """
    st.header("Project Overview")

    # Document Statistics
    try:
        stats = get_document_statistics(output_dir)

        st.metric("Total Documents", stats['total'])
        st.metric("Parsed Only", stats['parsed_only'], help="Parsed but not processed")
        st.metric("Stage 2 Done", stats['stage_2_done'], help="Stage 2 complete")
        st.metric("Fully Processed", stats['fully_processed'], help="Stage 3 complete")

    except Exception as e:
        st.error("Failed to load document statistics")
        with st.expander("Show technical details", expanded=False):
            st.code(str(e), language='text')

    st.divider()

    # View Mode Selector
    st.subheader("View Mode")

    view_mode = st.radio(
        "Select view",
        ["Browse Documents", "Job History", "Global Questions"],
        index=["Browse Documents", "Job History", "Global Questions"].index(
            st.session_state.get('view_mode', 'Browse Documents')
        ),
        key="view_mode_selector",
        label_visibility="collapsed"
    )

    # Update session state if changed
    if st.session_state.get('view_mode') != view_mode:
        st.session_state.view_mode = view_mode
        st.rerun()

    st.divider()

    # Global Actions
    with st.expander("📝 Parse New Document", expanded=False):
        render_parse_new_document(config, output_dir)

    st.divider()

    # System Status
    render_worker_status(config)

    st.divider()

    render_cache_info(config)

    st.divider()

    # Refresh button
    if st.button("🔄 Refresh Documents", use_container_width=True):
        st.cache_data.clear()
        st.rerun()
