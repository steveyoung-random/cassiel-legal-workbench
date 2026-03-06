"""
Column 4: Sub-Documents Component.

Displays parsed files (chapters/sections) for split documents.
This column is conditional - only shown when selected document has multiple parsed files.

Features:
- List of parsed files with organizational unit labels
- Status indicators for each file (✅ parsed, S2, S3)
- Selection highlighting
- Auto-selection for single-file documents (handled in column_layout.py)
"""
# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License

import streamlit as st
from typing import Dict

from ui.structure_display import format_organizational_units
from ui.utils import get_status_icons, show_error_with_retry


def get_parsed_file_label(parsed_file: Dict, index: int) -> str:
    """
    Generate display label for a parsed file.

    Args:
        parsed_file: Parsed file dict
        index: File index (for fallback labeling)

    Returns:
        Display label string
    """
    file_type = parsed_file.get('type', '').replace('_', ' ').title()
    org_units = parsed_file.get('organizational_units', {})

    if org_units:
        # Use organizational units for label (e.g., "Title 42, Chapter 6A")
        return format_organizational_units(org_units)
    else:
        # Fallback to file type and index
        return f"{file_type} {index + 1}"


def render_subdocument_card(parsed_file: Dict, index: int, is_selected: bool):
    """
    Render a single sub-document card.

    Args:
        parsed_file: Parsed file dict
        index: File index
        is_selected: Whether this file is currently selected
    """
    from ui.app import get_document_status

    # Get label
    label = get_parsed_file_label(parsed_file, index)

    # Get status
    status = get_document_status(parsed_file)
    icons = get_status_icons(status)
    status_text = " | ".join(icons) if icons else "—"

    # Card container
    with st.container():
        col_select, col_status = st.columns([3, 1])

        with col_select:
            # Generate unique key using parse_file path
            key = f"chapter_{parsed_file.get('parse_file', '')}_{index}"

            if st.button(
                label,
                key=key,
                type="primary" if is_selected else "secondary",
                use_container_width=True
            ):
                st.session_state.selected_chapter = parsed_file
                st.rerun()

        with col_status:
            st.caption(status_text)


def render(config: Dict, output_dir: str):
    """
    Main render function for Column 4 (Sub-Documents).

    Args:
        config: Application configuration
        output_dir: Output directory path
    """
    # Get selected document
    doc = st.session_state.get('selected_doc')

    if not doc:
        # No document selected - this shouldn't happen if column is visible
        st.info("📄 **Select a document**")
        st.caption("← Choose a document from Column 3")
        return

    parsed_files = doc.get('parsed_files', [])

    # Auto-select if single file (failsafe - should be handled in column_layout)
    if len(parsed_files) == 1:
        if st.session_state.get('selected_chapter') != parsed_files[0]:
            st.session_state.selected_chapter = parsed_files[0]
        # Single file auto-selected - no need to show message
        return

    # Multiple files - show selection
    st.header("Select Part")
    st.caption(f"{len(parsed_files)} part{'s' if len(parsed_files) != 1 else ''}")

    st.divider()

    try:
        # Get currently selected chapter
        selected_chapter = st.session_state.get('selected_chapter')

        # Render each parsed file
        for index, parsed_file in enumerate(parsed_files):
            is_selected = (
                selected_chapter is not None and
                selected_chapter.get('parse_file') == parsed_file.get('parse_file')
            )

            render_subdocument_card(parsed_file, index, is_selected)

            # Spacer
            st.write("")

    except Exception as e:
        def retry_load():
            st.rerun()
        show_error_with_retry(st, "Failed to load sub-documents", retry_load, "Refresh", exception=e)
