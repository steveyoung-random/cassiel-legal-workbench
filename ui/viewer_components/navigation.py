"""
Navigation Component - Breadcrumb navigation and view transitions.

Manages the navigation stack and breadcrumb display for the document viewer.
"""
# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License

import streamlit as st
from typing import List, Dict, Any


def push_navigation(unit_type: str, unit_id: str, unit_title: str):
    """
    Push a new navigation level onto the stack.

    Args:
        unit_type: Type of unit ('org_unit' or 'substantive_unit')
        unit_id: Identifier for the unit (e.g., 'title.2', 'section.131')
        unit_title: Display title for the unit
    """
    st.session_state.viewer_nav_stack.append({
        'unit_type': unit_type,
        'unit_id': unit_id,
        'unit_title': unit_title
    })


def pop_navigation(levels: int = 1):
    """
    Pop navigation levels off the stack.

    Args:
        levels: Number of levels to pop (default 1)
    """
    for _ in range(levels):
        if st.session_state.viewer_nav_stack:
            st.session_state.viewer_nav_stack.pop()


def navigate_to_overview():
    """Navigate back to document overview."""
    st.session_state.viewer_current_view = 'overview'
    st.session_state.viewer_nav_stack = []
    st.session_state.viewer_current_unit = None
    st.rerun()


def navigate_to_level(level_index: int):
    """
    Navigate to a specific level in the navigation stack.

    Args:
        level_index: Index in navigation stack (-1 means overview)
    """
    if level_index == -1:
        navigate_to_overview()
        return

    # Remove everything after this level
    st.session_state.viewer_nav_stack = st.session_state.viewer_nav_stack[:level_index + 1]

    # Load the unit at this level
    nav_item = st.session_state.viewer_nav_stack[level_index]

    if nav_item['unit_type'] == 'org_unit':
        st.session_state.viewer_current_view = 'org_unit'
        st.session_state.viewer_current_unit = nav_item['unit_id']
    elif nav_item['unit_type'] == 'substantive_unit':
        st.session_state.viewer_current_view = 'substantive_unit'
        st.session_state.viewer_current_unit = nav_item['unit_id']

    st.rerun()


def render_breadcrumbs():
    """Render breadcrumb navigation at top of page."""
    doc = st.session_state.viewer_document
    if not doc:
        return

    doc_info = doc.get('document_information', {})
    doc_title = doc_info.get('title', 'Document')

    # Build breadcrumb items
    breadcrumbs = []

    # Home/Overview
    breadcrumbs.append({
        'label': f"📄 {doc_title}",
        'index': -1,
        'is_current': st.session_state.viewer_current_view == 'overview'
    })

    # Add navigation stack items
    for i, nav_item in enumerate(st.session_state.viewer_nav_stack):
        # Truncate long titles
        title = nav_item['unit_title']
        if len(title) > 40:
            title = title[:37] + "..."

        # Add icon based on type
        if nav_item['unit_type'] == 'org_unit':
            icon = "📂"
        else:
            icon = "📄"

        breadcrumbs.append({
            'label': f"{icon} {title}",
            'index': i,
            'is_current': i == len(st.session_state.viewer_nav_stack) - 1
        })

    # Render breadcrumbs
    if len(breadcrumbs) == 1:
        # Just show document title, not clickable
        st.markdown(f"### {breadcrumbs[0]['label']}")
    else:
        # Create clickable breadcrumb trail
        cols = st.columns([0.1] + [1] * len(breadcrumbs))

        with cols[0]:
            st.markdown("**Nav:**")

        for i, breadcrumb in enumerate(breadcrumbs):
            with cols[i + 1]:
                if breadcrumb['is_current']:
                    # Current level - not clickable, bold
                    st.markdown(f"**{breadcrumb['label']}**")
                else:
                    # Clickable link to navigate
                    if st.button(
                        breadcrumb['label'],
                        key=f"breadcrumb_{breadcrumb['index']}",
                        use_container_width=True
                    ):
                        navigate_to_level(breadcrumb['index'])


def navigate_to_org_unit(unit_path: List[Dict[str, str]], unit_title: str):
    """
    Navigate to an organizational unit.

    Args:
        unit_path: Path to the unit as list of dicts (e.g., [{'title': '2'}, {'chapter': '5'}])
        unit_title: Display title for the unit
    """
    # Build unit ID from path
    unit_id = '.'.join([f"{list(p.keys())[0]}.{list(p.values())[0]}" for p in unit_path])

    # Push to navigation stack
    push_navigation('org_unit', unit_id, unit_title)

    # Set current view
    st.session_state.viewer_current_view = 'org_unit'
    st.session_state.viewer_current_unit = unit_id

    st.rerun()


def navigate_to_substantive_unit(unit_type: str, unit_number: str, unit_title: str):
    """
    Navigate to a substantive unit (section, article, etc.).

    Args:
        unit_type: Type of substantive unit (e.g., 'sections', 'articles')
        unit_number: Unit number/identifier
        unit_title: Display title for the unit
    """
    # Build unit ID
    unit_id = f"{unit_type}.{unit_number}"

    # Push to navigation stack
    push_navigation('substantive_unit', unit_id, unit_title)

    # Set current view
    st.session_state.viewer_current_view = 'substantive_unit'
    st.session_state.viewer_current_unit = unit_id

    st.rerun()
