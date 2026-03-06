"""
Column 3: Documents List Component.

Displays filtered list of documents for the selected category.
Includes search, pagination, and status indicators.

Features:
- Search/filter by document title
- Pagination for long lists (20+ documents)
- Status badges (✅ parsed, S2, S3)
- Selection highlighting
- Automatic clearing of sub-document selection when document changes
"""
# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License

import streamlit as st
from pathlib import Path
from typing import Dict, List

from ui.utils import get_overall_document_status, get_status_icons, format_status_badge, show_error_with_retry

# Page size for pagination
PAGE_SIZE = 20


def get_category_documents(output_dir: str, category: str) -> List[Dict]:
    """
    Get all documents for selected category.

    Args:
        output_dir: Output directory path
        category: Parser type string

    Returns:
        List of document dicts
    """
    from ui.app import scan_documents

    all_docs = scan_documents(output_dir)
    return [
        doc for doc in all_docs
        if doc.get('parser_type') == category
    ]


def filter_documents(docs: List[Dict], search_term: str) -> List[Dict]:
    """
    Filter documents by search term (case-insensitive).

    Args:
        docs: List of document dicts
        search_term: Search string

    Returns:
        Filtered list of documents
    """
    if not search_term:
        return docs

    search_lower = search_term.lower()
    return [
        doc for doc in docs
        if search_lower in doc.get('short_title', '').lower()
    ]


def render_document_card(doc: Dict, is_selected: bool):
    """
    Render a single document card.

    Args:
        doc: Document dict from scan_documents()
        is_selected: Whether this document is currently selected
    """
    display_title = doc.get('short_title') or Path(doc['source_file']).name

    # Get overall status
    status = get_overall_document_status(doc)
    status_text = format_status_badge(status)

    # Document card
    with st.container():
        col_select, col_status = st.columns([3, 1])

        with col_select:
            if st.button(
                display_title,
                key=f"doc_{doc['manifest_path']}",
                type="primary" if is_selected else "secondary",
                use_container_width=True
            ):
                st.session_state.selected_doc = doc
                st.session_state.selected_chapter = None  # Clear sub-document selection
                st.rerun()

        with col_status:
            st.caption(status_text)

        # Show details if selected
        if is_selected:
            st.caption(f"📁 {len(doc['parsed_files'])} file(s)")
            st.caption(f"📄 {Path(doc['source_file']).name}")


def render_pagination(total_docs: int, current_page: int) -> None:
    """
    Render pagination controls.

    Args:
        total_docs: Total number of documents (after filtering)
        current_page: Current page number (0-indexed)
    """
    total_pages = (total_docs + PAGE_SIZE - 1) // PAGE_SIZE

    if total_pages <= 1:
        return  # No pagination needed

    col_prev, col_info, col_next = st.columns([1, 2, 1])

    with col_prev:
        if st.button("← Prev", disabled=(current_page == 0), use_container_width=True):
            st.session_state.doc_page -= 1
            st.rerun()

    with col_info:
        st.caption(f"Page {current_page + 1} / {total_pages}")

    with col_next:
        if st.button("Next →", disabled=(current_page >= total_pages - 1), use_container_width=True):
            st.session_state.doc_page += 1
            st.rerun()


def render(config: Dict, output_dir: str):
    """
    Main render function for Column 3 (Documents List).

    Args:
        config: Application configuration
        output_dir: Output directory path
    """
    # Check if category is selected
    selected_category = st.session_state.get('selected_category')

    if not selected_category:
        # Empty state
        st.info("📂 **Select a category**")
        st.caption("← Choose a document category from Column 2 to view documents")
        return

    # Get parser display name
    from ui.app import get_parser_display_name
    display_name = get_parser_display_name(selected_category)

    st.header(display_name)

    try:
        # Get documents for category (with loading feedback)
        with st.spinner(f"Loading {display_name.lower()} documents..."):
            category_docs = get_category_documents(output_dir, selected_category)

        if not category_docs:
            st.info(f"📭 **No {display_name} documents found**")
            st.caption("Parse a document with this parser type to see it here")
            return

        st.caption(f"{len(category_docs)} document{'s' if len(category_docs) != 1 else ''}")

        # Search box
        search = st.text_input(
            "Search documents",
            placeholder="Type to search...",
            key="doc_search",
            label_visibility="collapsed"
        )

        # Filter documents
        filtered_docs = filter_documents(category_docs, search)

        if search and not filtered_docs:
            st.info(f"🔍 **No documents match '{search}'**")
            st.caption("Try a different search term or clear the search box")
            return

        # Pagination
        current_page = st.session_state.get('doc_page', 0)
        total_docs = len(filtered_docs)
        total_pages = (total_docs + PAGE_SIZE - 1) // PAGE_SIZE

        # Ensure current page is valid
        if current_page >= total_pages and total_pages > 0:
            st.session_state.doc_page = total_pages - 1
            current_page = total_pages - 1

        # Get page slice
        start_idx = current_page * PAGE_SIZE
        end_idx = min(start_idx + PAGE_SIZE, total_docs)
        page_docs = filtered_docs[start_idx:end_idx]

        st.divider()

        # Render documents
        selected_doc = st.session_state.get('selected_doc')

        for doc in page_docs:
            is_selected = (
                selected_doc is not None and
                selected_doc.get('manifest_path') == doc.get('manifest_path')
            )

            render_document_card(doc, is_selected)

            # Spacer
            st.write("")

        # Pagination controls
        if total_pages > 1:
            st.divider()
            render_pagination(total_docs, current_page)

    except Exception as e:
        def retry_load():
            st.rerun()
        show_error_with_retry(st, "Failed to load documents", retry_load, "Refresh", exception=e)
