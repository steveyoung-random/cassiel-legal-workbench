"""
Document Viewer - Interactive UI for browsing processed legal documents.

This Streamlit application provides summary-driven navigation through processed
legal documents, with progressive disclosure from document overview down to
individual sections with full text and definitions.

Usage:
    streamlit run ui/view_document.py -- path/to/document_processed.json
"""
# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License

import streamlit as st
import json
import sys
from pathlib import Path
from typing import Dict, Any, Optional, List

# Add project root to path for imports
project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from ui.viewer_components import document_overview, navigation


def load_document(file_path: str) -> Optional[Dict[str, Any]]:
    """
    Load and parse a processed document JSON file.

    Args:
        file_path: Path to processed document JSON file

    Returns:
        Parsed document dict, or None if loading fails
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            doc = json.load(f)

        # Validate it's a processed document
        if 'document_information' not in doc or 'content' not in doc:
            st.error("❌ Invalid document format: Missing required sections")
            return None

        return doc
    except FileNotFoundError:
        st.error(f"❌ File not found: {file_path}")
        return None
    except json.JSONDecodeError as e:
        st.error(f"❌ Invalid JSON format: {e}")
        return None
    except Exception as e:
        st.error(f"❌ Error loading document: {e}")
        return None


def init_viewer_session_state():
    """Initialize session state variables for document viewer."""
    defaults = {
        'viewer_document': None,
        'viewer_file_path': None,
        'viewer_nav_stack': [],  # Stack of navigation breadcrumbs
        'viewer_current_view': 'library',  # 'library', 'overview', 'org_unit', 'substantive_unit', 'qa'
        'viewer_current_unit': None,  # Current unit being viewed
        'viewer_summary_level': 1,  # 1 or 2 for summary level
        'viewer_search_query': '',
        'viewer_show_definitions': True,
    }

    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


def main():
    """Main application entry point."""
    st.set_page_config(
        page_title="Document Viewer",
        page_icon="📄",
        layout="wide",
        initial_sidebar_state="expanded"
    )

    init_viewer_session_state()

    # Check for command-line argument (direct file load)
    if len(sys.argv) > 1 and not st.session_state.viewer_file_path:
        file_path = sys.argv[1]
        with st.spinner("Loading document..."):
            doc = load_document(file_path)
            if doc:
                st.session_state.viewer_document = doc
                st.session_state.viewer_file_path = file_path
                st.session_state.viewer_current_view = 'overview'
                st.session_state.viewer_nav_stack = []

    # Sidebar: View options and navigation
    with st.sidebar:
        st.title("📄 Document Viewer")
        st.markdown("---")

        # Back to Library button
        if st.session_state.viewer_document:
            if st.button("📚 Back to Library", use_container_width=True):
                st.session_state.viewer_document = None
                st.session_state.viewer_file_path = None
                st.session_state.viewer_current_view = 'library'
                st.session_state.viewer_nav_stack = []
                st.rerun()

            st.markdown("---")

            # Show document info if loaded
            st.subheader("Current Document")
            doc_info = st.session_state.viewer_document.get('document_information', {})

            st.caption(f"**{doc_info.get('title', 'Untitled')}**")

            # Processing status
            status = doc_info.get('processing_status', {})
            status_icons = []
            if status.get('parsed'):
                status_icons.append('✅ Parsed')
            if status.get('stage_2_complete'):
                status_icons.append('📖 Stage 2')
            if status.get('stage_3_complete'):
                status_icons.append('📝 Stage 3')

            if status_icons:
                st.caption(" | ".join(status_icons))

            st.markdown("---")

            # View options
            st.subheader("View Options")

            summary_level = st.radio(
                "Summary Detail",
                options=[1, 2],
                format_func=lambda x: f"Level {x} ({'Concise' if x == 1 else 'Detailed'})",
                key='viewer_summary_level',
                help="Level 1: 3-sentence overview\nLevel 2: Detailed 5+ paragraph summary"
            )

            show_defs = st.checkbox(
                "Show Definitions",
                value=st.session_state.viewer_show_definitions,
                key='viewer_show_definitions',
                help="Display in-scope definitions for current unit"
            )

            st.markdown("---")

            # Q&A section
            st.subheader("Q&A")

            if st.button("💬 Ask Questions", use_container_width=True):
                st.session_state.viewer_current_view = 'qa'
                st.session_state.viewer_nav_stack = []  # Clear navigation
                st.rerun()

    # Main content area
    current_view = st.session_state.viewer_current_view

    # Route to appropriate view
    if current_view == 'library':
        # Show document library
        from ui.viewer_components import document_library
        document_library.render_document_library()

    elif not st.session_state.viewer_document:
        # No document loaded, show library
        from ui.viewer_components import document_library
        document_library.render_document_library()

    elif current_view == 'qa':
        # Q&A view (special case, no breadcrumbs)
        from ui.viewer_components import qa_panel
        qa_panel.render_qa_panel()

    else:
        # Document is loaded, show document views

        # Breadcrumb navigation (not for Q&A)
        navigation.render_breadcrumbs()

        st.markdown("---")

        # Route to appropriate document view
        if current_view == 'overview':
            document_overview.render_overview()
        elif current_view == 'org_unit':
            from ui.viewer_components import org_unit_view
            org_unit_view.render_org_unit()
        elif current_view == 'substantive_unit':
            from ui.viewer_components import substantive_unit_view
            substantive_unit_view.render_substantive_unit()
        else:
            st.error(f"❌ Unknown view type: {current_view}")


if __name__ == "__main__":
    main()
