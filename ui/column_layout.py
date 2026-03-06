"""
Multi-Column Progressive Disclosure UI Layout.

This module orchestrates the 5-column progressive disclosure interface,
replacing the old tab-based UI with an intuitive left-to-right navigation flow.

Column Structure:
    Column 1: Project Overview (always visible)
    Column 2: Document Categories (parser types)
    Column 3: Documents List (filtered by category)
    Column 4: Sub-Documents (conditional, for split documents)
    Column 5: Processing Control (actions and monitoring)

View Modes:
    - Browse Documents: 5-column navigation (default)
    - Job History: Full job history in columns 2-5
    - Global Questions: All questions in columns 2-5
"""
# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License

import streamlit as st
from typing import Dict

from ui.utils import should_show_column_4, get_column_widths


def _truncate_text(text: str, max_length: int = 40) -> str:
    """
    Truncate text to max_length, adding ellipsis if needed.

    Args:
        text: Text to truncate
        max_length: Maximum length before truncation

    Returns:
        Truncated text with ellipsis if needed
    """
    if len(text) <= max_length:
        return text
    return text[:max_length-3] + "..."


def render_breadcrumbs(st_module):
    """
    Render breadcrumb trail at top of page showing current selection path.

    Breadcrumbs are clickable and allow navigation back to previous levels.
    Icons indicate the type of each breadcrumb level.

    Args:
        st_module: Streamlit module instance
    """
    from ui.app import get_parser_display_name
    from ui.structure_display import format_organizational_units

    view_mode = st_module.session_state.get('view_mode', 'Browse Documents')

    # Build breadcrumb data
    breadcrumbs = []

    if view_mode == 'Browse Documents':
        # Category level
        category = st_module.session_state.get('selected_category')
        if category:
            breadcrumbs.append({
                'icon': '📁',
                'text': get_parser_display_name(category),
                'full_text': get_parser_display_name(category),
                'action': 'clear_document'
            })

        # Document level
        doc = st_module.session_state.get('selected_doc')
        if doc:
            doc_title = doc.get('short_title', 'Unknown')
            breadcrumbs.append({
                'icon': '📄',
                'text': _truncate_text(doc_title),
                'full_text': doc_title,
                'action': 'clear_chapter'
            })

        # Chapter level (only if multiple parsed files)
        chapter = st_module.session_state.get('selected_chapter')
        if chapter and doc and len(doc.get('parsed_files', [])) > 1:
            org_units = chapter.get('organizational_units', {})
            if org_units:
                chapter_text = format_organizational_units(org_units)
                breadcrumbs.append({
                    'icon': '📑',
                    'text': _truncate_text(chapter_text),
                    'full_text': chapter_text,
                    'action': None  # Current item, not clickable
                })

    elif view_mode == 'Job History':
        breadcrumbs.append({
            'icon': '📋',
            'text': 'Job History',
            'full_text': 'Job History',
            'action': None
        })

    elif view_mode == 'Global Questions':
        breadcrumbs.append({
            'icon': '❓',
            'text': 'Questions',
            'full_text': 'Questions',
            'action': None
        })

    # Render breadcrumbs
    if breadcrumbs:
        cols = st_module.columns([0.1] + [1] * len(breadcrumbs) + [5])

        # Home icon
        with cols[0]:
            if st_module.button("🏠", key="breadcrumb_home", help="Home", use_container_width=True):
                st_module.session_state.selected_category = None
                st_module.session_state.selected_doc = None
                st_module.session_state.selected_chapter = None
                st_module.rerun()

        # Breadcrumb items
        for idx, crumb in enumerate(breadcrumbs):
            with cols[idx + 1]:
                is_last = (idx == len(breadcrumbs) - 1)

                # Current item (last one) - not clickable
                if is_last or crumb['action'] is None:
                    st_module.caption(f"{crumb['icon']} {crumb['text']}")
                else:
                    # Clickable breadcrumb
                    button_label = f"{crumb['icon']} {crumb['text']}"
                    if st_module.button(
                        button_label,
                        key=f"breadcrumb_{idx}",
                        help=crumb['full_text'],
                        use_container_width=True
                    ):
                        # Handle navigation action
                        if crumb['action'] == 'clear_document':
                            st_module.session_state.selected_doc = None
                            st_module.session_state.selected_chapter = None
                        elif crumb['action'] == 'clear_chapter':
                            st_module.session_state.selected_chapter = None
                        st_module.rerun()

        st_module.divider()


def render_document_browser(config: Dict, output_dir: str):
    """
    Render the 5-column document browser interface.

    This is the main "Browse Documents" view showing progressive disclosure
    from categories → documents → sub-documents → processing controls.

    Args:
        config: Application configuration dict
        output_dir: Output directory path
    """
    # Import components
    from ui.components import column1_overview
    from ui.components import column2_categories
    from ui.components import column3_documents
    from ui.components import column4_subdocs
    from ui.components import column5_processing

    # Determine if Column 4 should be shown
    doc = st.session_state.get('selected_doc')
    show_col4 = should_show_column_4(doc)

    # Auto-select single parsed file for documents that don't need Column 4
    if doc and not show_col4:
        # Single-file document - auto-select the only parsed file
        parsed_files = doc.get('parsed_files', [])
        if len(parsed_files) == 1:
            # Auto-select if not already selected
            if not st.session_state.get('selected_chapter'):
                st.session_state.selected_chapter = parsed_files[0]

    # Create columns based on whether Column 4 is needed
    if show_col4:
        # All 5 columns
        col1, col2, col3, col4, col5 = st.columns([1, 1.5, 2, 1.5, 2.5])
    else:
        # Only 4 columns (Column 4 hidden, Column 5 gets extra space)
        col1, col2, col3, col5 = st.columns([1, 1.5, 2, 4])
        col4 = None  # No Column 4

    # Render each column
    with col1:
        column1_overview.render(config, output_dir)

    with col2:
        column2_categories.render(config, output_dir)

    with col3:
        column3_documents.render(config, output_dir)

    # Column 4 only rendered if needed
    if show_col4 and col4 is not None:
        with col4:
            column4_subdocs.render(config, output_dir)

    with col5:
        column5_processing.render(config, output_dir)


def render_job_history_mode(config: Dict, output_dir: str):
    """
    Render Job History view mode.

    Columns 2-5 area shows full job history table with filtering and management.

    Args:
        config: Application configuration dict
        output_dir: Output directory path
    """
    from ui.components import column1_overview
    from ui.components import job_history_view

    # Create layout: Column 1 narrow, Columns 2-5 as one wide area
    col1, col_main = st.columns([1, 7])

    with col1:
        column1_overview.render(config, output_dir)

    with col_main:
        job_history_view.render(config, output_dir)


def render_questions_mode(config: Dict, output_dir: str):
    """
    Render Global Questions view mode.

    Columns 2-5 area shows all questions across documents with submission interface.

    Args:
        config: Application configuration dict
        output_dir: Output directory path
    """
    from ui.components import column1_overview
    from ui.components import questions_view

    # Create layout: Column 1 narrow, Columns 2-5 as one wide area
    col1, col_main = st.columns([1, 7])

    with col1:
        column1_overview.render(config, output_dir)

    with col_main:
        questions_view.render(config, output_dir)


def render_column_layout(config: Dict, output_dir: str):
    """
    Main entry point for column-based UI layout.

    Renders breadcrumbs and routes to appropriate view based on view_mode session state.

    Args:
        config: Application configuration dict
        output_dir: Output directory path
    """
    # Get current view mode
    view_mode = st.session_state.get('view_mode', 'Browse Documents')

    # Render breadcrumbs for all view modes
    render_breadcrumbs(st)

    # Route to appropriate view
    if view_mode == 'Browse Documents':
        render_document_browser(config, output_dir)

    elif view_mode == 'Job History':
        render_job_history_mode(config, output_dir)

    elif view_mode == 'Global Questions':
        render_questions_mode(config, output_dir)

    else:
        # Unknown view mode - default to browse
        st.error(f"Unknown view mode: {view_mode}")
        st.session_state.view_mode = 'Browse Documents'
        st.rerun()
