"""
Document Overview Component - Document-level summary and top-level structure.

Displays document summaries and the first level of organizational hierarchy,
providing the entry point for document exploration.
"""
# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License

import streamlit as st
from typing import Dict, Any, List
from ui.viewer_components import navigation


def get_top_level_org_units(doc: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Extract top-level organizational units from document.

    Args:
        doc: Document dictionary

    Returns:
        List of top-level organizational units with their info
    """
    org_content = doc.get('document_information', {}).get('organization', {}).get('content', {})

    units = []
    for org_type, org_units in org_content.items():
        for unit_id, unit_data in org_units.items():
            units.append({
                'type': org_type,
                'id': unit_id,
                'title': unit_data.get('unit_title', f'{org_type.title()} {unit_id}'),
                'summary_1': unit_data.get('summary_1', ''),
                'summary_2': unit_data.get('summary_2', ''),
                'path': [{org_type: unit_id}],
                'has_children': any(
                    k for k in unit_data.keys()
                    if k not in ['unit_title', 'summary_1', 'summary_2', 'unit_definitions',
                                 'begin_section', 'stop_section', 'begin_article', 'stop_article']
                )
            })

    return units


def render_summary_panel(summary_1: str, summary_2: str, level: int):
    """
    Render a summary panel with toggle between levels.

    Args:
        summary_1: Concise summary (3 sentences)
        summary_2: Detailed summary (5+ paragraphs)
        level: Current summary level (1 or 2)
    """
    if level == 1 and summary_1:
        st.markdown(summary_1)
        if summary_2:
            with st.expander("📝 Show detailed summary"):
                st.markdown(summary_2)
    elif level == 2 and summary_2:
        st.markdown(summary_2)
        if summary_1:
            with st.expander("📝 Show concise summary"):
                st.markdown(summary_1)
    elif summary_1:
        st.markdown(summary_1)
    elif summary_2:
        st.markdown(summary_2)
    else:
        st.info("No summary available")


def render_overview():
    """Render the document overview page."""
    doc = st.session_state.viewer_document
    if not doc:
        st.error("❌ No document loaded")
        return

    doc_info = doc.get('document_information', {})
    summary_level = st.session_state.viewer_summary_level

    # Header
    st.title("📄 Document Overview")

    long_title = doc_info.get('long_title', doc_info.get('title', 'Untitled Document'))
    st.markdown(f"**{long_title}**")
    
    # Display topic statement for quick scanning
    topic_statement = doc_info.get('topic_statement', '')
    if topic_statement:
        st.caption(f"📌 {topic_statement}")

    st.markdown("---")

    # Document-level summary
    st.subheader("📋 Document Summary")

    # Get document-level summaries from top-level org units
    org_structure = doc_info.get('organization', {})
    org_content = org_structure.get('content', {})

    # Find the highest-level summary
    doc_summary_1 = None
    doc_summary_2 = None

    # Try to get summary from the first top-level organizational unit
    for org_type, org_units in org_content.items():
        for unit_id, unit_data in org_units.items():
            # If this is the only top-level unit, use its summary as document summary
            if len(org_units) == 1:
                doc_summary_1 = unit_data.get('summary_1', '')
                doc_summary_2 = unit_data.get('summary_2', '')
                break
        if doc_summary_1 or doc_summary_2:
            break

    if doc_summary_1 or doc_summary_2:
        render_summary_panel(doc_summary_1, doc_summary_2, summary_level)
    else:
        st.info("No document-level summary available. Summaries are available for individual organizational units below.")

    st.markdown("---")

    # Top-level organizational units
    st.subheader("📂 Document Structure")

    top_units = get_top_level_org_units(doc)

    if not top_units:
        st.warning("⚠️ No organizational structure found. This document may only contain substantive units.")

        # Show substantive units directly if no org structure
        render_substantive_units_list(doc)
        return

    # Display units
    st.markdown(f"*Found {len(top_units)} top-level organizational unit(s)*")
    st.markdown("")

    for unit in top_units:
        with st.container():
            # Unit header with click-to-navigate
            col1, col2 = st.columns([4, 1])

            with col1:
                unit_label = f"**{unit['type'].title()} {unit['id']}**: {unit['title']}"
                st.markdown(unit_label)

            with col2:
                if st.button(
                    "Explore →",
                    key=f"nav_unit_{unit['type']}_{unit['id']}",
                    use_container_width=True
                ):
                    navigation.navigate_to_org_unit(unit['path'], unit['title'])

            # Show summary for this unit
            if unit['summary_1'] or unit['summary_2']:
                with st.expander("📝 View summary", expanded=False):
                    render_summary_panel(unit['summary_1'], unit['summary_2'], summary_level)

            # Visual indicator of children
            if unit['has_children']:
                st.caption("📂 Contains sub-units")

            st.markdown("---")


def render_substantive_units_list(doc: Dict[str, Any]):
    """
    Render list of substantive units (for documents with no org structure).

    Args:
        doc: Document dictionary
    """
    content = doc.get('content', {})
    doc_info = doc.get('document_information', {})
    parameters = doc_info.get('parameters', {})

    # Find operational parameters (substantive units)
    operational_params = {
        param_id: param_data
        for param_id, param_data in parameters.items()
        if param_data.get('operational') == 1
    }

    if not operational_params:
        st.info("No substantive units found in document.")
        return

    # Get the first operational parameter type
    param_id = list(operational_params.keys())[0]
    param_data = operational_params[param_id]
    unit_type = param_data.get('name_plural', 'units')

    st.subheader(f"📄 {unit_type.title()}")

    # Get units from content
    units = content.get(unit_type, {})

    if not units:
        st.info(f"No {unit_type} found in document.")
        return

    st.markdown(f"*Found {len(units)} {unit_type}*")
    st.markdown("")

    # Display units (paginated if many)
    unit_items = list(units.items())

    # Simple pagination for many units
    items_per_page = 20
    total_pages = (len(unit_items) + items_per_page - 1) // items_per_page

    if 'overview_page' not in st.session_state:
        st.session_state.overview_page = 0

    if total_pages > 1:
        col1, col2, col3 = st.columns([1, 2, 1])
        with col1:
            if st.button("← Previous", disabled=st.session_state.overview_page == 0):
                st.session_state.overview_page -= 1
                st.rerun()
        with col2:
            st.markdown(f"Page {st.session_state.overview_page + 1} of {total_pages}")
        with col3:
            if st.button("Next →", disabled=st.session_state.overview_page >= total_pages - 1):
                st.session_state.overview_page += 1
                st.rerun()

    # Get current page items
    start_idx = st.session_state.overview_page * items_per_page
    end_idx = min(start_idx + items_per_page, len(unit_items))
    page_items = unit_items[start_idx:end_idx]

    for unit_number, unit_data in page_items:
        with st.container():
            col1, col2 = st.columns([4, 1])

            unit_title = unit_data.get('unit_title', f'{param_data.get("name", "Unit")} {unit_number}')

            with col1:
                st.markdown(f"**{param_data.get('name', 'Unit').title()} {unit_number}**: {unit_title}")

            with col2:
                if st.button(
                    "View →",
                    key=f"nav_subst_{unit_type}_{unit_number}",
                    use_container_width=True
                ):
                    navigation.navigate_to_substantive_unit(unit_type, unit_number, unit_title)

            # Show summary preview
            summary_1 = unit_data.get('summary_1', '')
            if summary_1:
                st.caption(summary_1[:150] + "..." if len(summary_1) > 150 else summary_1)

            st.markdown("---")
