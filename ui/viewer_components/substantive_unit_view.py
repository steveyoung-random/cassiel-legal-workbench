"""
Substantive Unit View Component - Display substantive units with text, summaries, and definitions.

Shows substantive units (sections, articles, etc.) with their full text,
AI-generated summaries, and in-scope definitions.
"""
# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License

import streamlit as st
from typing import Dict, Any, List

from utils.document_handling import lookup_item, get_organizational_item_name_set


def get_substantive_unit_data(doc: Dict[str, Any], unit_id: str) -> tuple:
    """
    Retrieve substantive unit data from document.

    Args:
        doc: Document dictionary
        unit_id: Unit ID like 'sections.131' or 'supplements.No. 1 to Part 774'

    Returns:
        Tuple of (unit_type, unit_number, unit_data), or (None, None, None) if not found
    """
    # Use partition so dots in the unit number (e.g. "No. 1 to Part 774") are preserved.
    unit_type, sep, unit_number = unit_id.partition('.')
    if not sep or not unit_type or not unit_number:
        return None, None, None

    # lookup_item handles both top-level content and sub-units nested inside containers.
    unit_data = lookup_item(doc, unit_type, unit_number)
    if unit_data is not None:
        return unit_type, unit_number, unit_data

    return None, None, None


def collect_scoped_definitions(doc: Dict[str, Any], unit_data: Dict[str, Any]) -> Dict[str, List[Dict]]:
    """
    Collect all definitions in scope for a substantive unit.

    Args:
        doc: Document dictionary
        unit_data: Substantive unit data

    Returns:
        Dict with keys: 'document', 'organizational', 'unit', 'external'
    """
    definitions = {
        'document': [],
        'organizational': [],
        'unit': [],
        'external': []
    }

    # Document-level definitions
    doc_info = doc.get('document_information', {})
    doc_defs = doc_info.get('document_definitions', [])
    definitions['document'] = doc_defs

    # Organizational definitions from context
    context = unit_data.get('context', [])
    org_structure = doc_info.get('organization', {}).get('content', {})
    org_name_set = get_organizational_item_name_set(doc)

    # Walk through context collecting org definitions.
    # Skip sub-unit type entries (e.g. ccl_category, eccn) — they are not in the
    # org hierarchy and would stop traversal prematurely if not filtered out.
    current = org_structure
    for context_item in context:
        for org_type, org_id in context_item.items():
            if org_type not in org_name_set:
                continue  # sub-unit type — skip
            if org_type in current and org_id in current[org_type]:
                current_unit = current[org_type][org_id]

                # Collect definitions from this level
                org_defs = current_unit.get('unit_definitions', [])
                for def_item in org_defs:
                    definitions['organizational'].append({
                        **def_item,
                        'source_org_type': org_type,
                        'source_org_id': org_id
                    })

                # Move down to next level
                current = current_unit

    # Unit-specific definitions (defined in this unit)
    unit_defs = unit_data.get('defined_terms', [])
    definitions['unit'] = unit_defs

    # External definitions (from other units)
    ext_defs = unit_data.get('ext_definitions', [])
    definitions['external'] = ext_defs

    return definitions


def render_substantive_unit():
    """Render substantive unit view."""
    doc = st.session_state.viewer_document
    unit_id = st.session_state.viewer_current_unit

    if not doc or not unit_id:
        st.error("❌ No unit selected")
        return

    # Get unit data
    unit_type, unit_number, unit_data = get_substantive_unit_data(doc, unit_id)

    if not unit_data:
        st.error(f"❌ Unit not found: {unit_id}")
        return

    # Get unit name from parameters
    doc_info = doc.get('document_information', {})
    parameters = doc_info.get('parameters', {})

    unit_singular = None
    for param_id, param_data in parameters.items():
        if param_data.get('name_plural') == unit_type:
            unit_singular = param_data.get('name', unit_type[:-1] if unit_type.endswith('s') else unit_type)
            break

    if not unit_singular:
        unit_singular = unit_type[:-1] if unit_type.endswith('s') else unit_type

    # Header
    unit_title = unit_data.get('unit_title', f'{unit_singular.title()} {unit_number}')

    st.title(f"📄 {unit_singular.title()} {unit_number}")
    st.markdown(f"**{unit_title}**")

    st.markdown("---")

    # Two-column layout: Summary and Text
    col_left, col_right = st.columns([1, 1])

    summary_level = st.session_state.viewer_summary_level

    with col_left:
        st.subheader("📋 Summary")

        summary_1 = unit_data.get('summary_1', '')
        summary_2 = unit_data.get('summary_2', '')

        if summary_level == 1 and summary_1:
            st.markdown(summary_1)
            if summary_2:
                with st.expander("📝 Show detailed summary"):
                    st.markdown(summary_2)
        elif summary_level == 2 and summary_2:
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

    with col_right:
        st.subheader("📜 Original Text")

        text = unit_data.get('text', '')

        if text:
            # Display text in a scrollable text area
            st.text_area(
                label="Original text",
                value=text,
                height=500,
                key=f"text_{unit_id}",
                label_visibility="collapsed"
            )
        else:
            st.warning("No text available for this unit")

    st.markdown("---")

    # Notes section (if present)
    notes = unit_data.get('notes', {})
    if notes and isinstance(notes, dict) and len(notes) > 0:
        st.subheader("📌 Notes")

        with st.expander(f"View {len(notes)} note(s)", expanded=False):
            for note_id, note_text in notes.items():
                st.markdown(f"**Note {note_id}:** {note_text}")
                st.markdown("")

        st.markdown("---")

    # Definitions section
    if st.session_state.viewer_show_definitions:
        st.subheader("📖 Definitions in Scope")

        scoped_defs = collect_scoped_definitions(doc, unit_data)

        # Count total definitions
        total_defs = (
            len(scoped_defs['document']) +
            len(scoped_defs['organizational']) +
            len(scoped_defs['unit']) +
            len(scoped_defs['external'])
        )

        if total_defs == 0:
            st.info("No definitions in scope for this unit")
        else:
            st.markdown(f"*{total_defs} definition(s) applicable to this {unit_singular}*")
            st.markdown("")

            from ui.viewer_components import definitions_panel

            # Document-level
            if scoped_defs['document']:
                with st.expander(f"📘 Document-Level Definitions ({len(scoped_defs['document'])})", expanded=False):
                    definitions_panel.render_definitions_list(scoped_defs['document'])

            # Organizational
            if scoped_defs['organizational']:
                with st.expander(f"📂 Organizational Definitions ({len(scoped_defs['organizational'])})", expanded=False):
                    definitions_panel.render_definitions_list(scoped_defs['organizational'], show_source=True)

            # Unit-specific
            if scoped_defs['unit']:
                with st.expander(f"📄 Defined in This {unit_singular.title()} ({len(scoped_defs['unit'])})", expanded=True):
                    definitions_panel.render_definitions_list(scoped_defs['unit'])

            # External
            if scoped_defs['external']:
                with st.expander(f"🔗 External Definitions ({len(scoped_defs['external'])})", expanded=False):
                    definitions_panel.render_definitions_list(scoped_defs['external'], show_source=True)

    # Referenced sections (need_ref)
    need_ref = unit_data.get('need_ref', [])
    if need_ref:
        st.markdown("---")
        st.subheader("🔗 References")

        st.markdown(f"*This {unit_singular} references {len(need_ref)} other item(s)*")
        st.markdown("")

        for ref in need_ref:
            ref_type = ref.get('type', 'Unknown')
            ref_value = ref.get('value', '')

            if ref_type == 'Section':
                st.markdown(f"- 📄 Section {ref_value}")
            elif ref_type == 'Need_Definition':
                st.markdown(f"- 📖 Definition needed: {ref_value}")
            elif ref_type == 'External':
                st.markdown(f"- 🔗 External: {ref_value}")
            else:
                st.markdown(f"- {ref_type}: {ref_value}")
