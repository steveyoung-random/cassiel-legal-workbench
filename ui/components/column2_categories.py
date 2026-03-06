"""
Column 2: Document Categories Component.

Displays document categories grouped by parser type (USLM, FormEx, CA HTML).
User selects a category to populate Column 3 with documents.

Features:
- Category cards showing parser type and document count
- Status summary for each category (parsed, Stage 2, Stage 3 counts)
- Selection highlighting (primary button for selected category)
- Automatic clearing of downstream selections when category changes
"""
# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License

import streamlit as st
from typing import Dict

from ui.utils import get_overall_document_status, get_status_icons, show_error_with_retry


def get_documents_by_type(output_dir: str) -> Dict[str, list]:
    """
    Group documents by parser type.

    Args:
        output_dir: Output directory path

    Returns:
        Dict mapping parser_type to list of documents
    """
    from ui.app import scan_documents

    docs = scan_documents(output_dir)
    docs_by_type = {}

    for doc in docs:
        parser_type = doc.get('parser_type', 'unknown')
        if parser_type not in docs_by_type:
            docs_by_type[parser_type] = []
        docs_by_type[parser_type].append(doc)

    return docs_by_type


def get_category_status_counts(type_docs: list) -> Dict[str, int]:
    """
    Calculate status counts for all documents in a category.

    Args:
        type_docs: List of documents for this category

    Returns:
        Dict with keys: parsed, stage_2, stage_3
    """
    counts = {
        'parsed': 0,
        'stage_2': 0,
        'stage_3': 0
    }

    for doc in type_docs:
        status = get_overall_document_status(doc)

        if status['any_parsed']:
            counts['parsed'] += 1
        if status['all_stage_2']:
            counts['stage_2'] += 1
        if status['all_stage_3']:
            counts['stage_3'] += 1

    return counts


def render_category_card(parser_type: str, type_docs: list, is_selected: bool):
    """
    Render a single category card.

    Args:
        parser_type: Parser type string ('uslm', 'formex', 'ca_html')
        type_docs: List of documents for this category
        is_selected: Whether this category is currently selected
    """
    from ui.app import get_parser_display_name

    display_name = get_parser_display_name(parser_type)
    doc_count = len(type_docs)

    # Calculate status counts
    status_counts = get_category_status_counts(type_docs)

    # Button container
    with st.container():
        # Icon and button
        col_icon, col_button = st.columns([1, 5])

        with col_icon:
            st.write("📚")

        with col_button:
            if st.button(
                f"{display_name}\n{doc_count} document{'s' if doc_count != 1 else ''}",
                key=f"cat_{parser_type}",
                type="primary" if is_selected else "secondary",
                use_container_width=True
            ):
                # Update selection
                st.session_state.selected_category = parser_type
                # Clear downstream selections
                st.session_state.selected_doc = None
                st.session_state.selected_chapter = None
                # Reset pagination
                st.session_state.doc_page = 0
                st.rerun()

        # Show status badges if selected
        if is_selected:
            st.caption(
                f"✅ {status_counts['parsed']} parsed | "
                f"⚙️ {status_counts['stage_2']} S2 | "
                f"✨ {status_counts['stage_3']} S3"
            )


def render(config: Dict, output_dir: str):
    """
    Main render function for Column 2 (Document Categories).

    Args:
        config: Application configuration
        output_dir: Output directory path
    """
    st.header("Categories")

    # Get documents grouped by type
    try:
        docs_by_type = get_documents_by_type(output_dir)

        if not docs_by_type:
            # Empty state
            st.info("📭 **No documents found**")
            st.markdown("""
                **Get started:**
                1. Expand **Parse New Document** in Column 1 →
                2. Select a parser type and source file
                3. Submit the parse job
                4. Documents will appear here once parsing completes
            """)
            st.caption("💡 Tip: Check Worker Status above to ensure the worker is running")
            return

        # Get currently selected category
        selected_category = st.session_state.get('selected_category')

        # Render category cards
        for parser_type in sorted(docs_by_type.keys()):
            type_docs = docs_by_type[parser_type]
            is_selected = (selected_category == parser_type)

            render_category_card(parser_type, type_docs, is_selected)

            # Spacer between cards
            st.write("")

    except Exception as e:
        def retry_load():
            st.rerun()
        show_error_with_retry(st, "Failed to load document categories", retry_load, "Refresh", exception=e)
