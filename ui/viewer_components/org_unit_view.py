"""
Organizational Unit View Component - Display organizational units with summaries and children.

Shows organizational units (titles, chapters, parts, etc.) with their summaries,
child organizational units, and substantive units they contain.
"""
# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License

import streamlit as st
from typing import Dict, Any, List, Tuple, Optional
from ui.viewer_components import navigation


def parse_unit_path(unit_id: str) -> List[Tuple[str, str]]:
    """
    Parse unit ID into path components.

    Args:
        unit_id: Unit ID like 'title.2.chapter.5'

    Returns:
        List of (unit_type, unit_number) tuples
    """
    parts = unit_id.split('.')
    path = []

    for i in range(0, len(parts), 2):
        if i + 1 < len(parts):
            path.append((parts[i], parts[i + 1]))

    return path


def get_unit_data(doc: Dict[str, Any], unit_id: str) -> Optional[Dict[str, Any]]:
    """
    Retrieve unit data from document by unit ID.

    Args:
        doc: Document dictionary
        unit_id: Unit ID like 'title.2.chapter.5'

    Returns:
        Unit data dictionary, or None if not found
    """
    path = parse_unit_path(unit_id)

    # Navigate through organization structure
    org_content = doc.get('document_information', {}).get('organization', {}).get('content', {})

    current = org_content
    for unit_type, unit_number in path:
        if unit_type in current and unit_number in current[unit_type]:
            current = current[unit_type][unit_number]
        else:
            return None

    return current


def get_child_org_units(unit_data: Dict[str, Any], parent_path: List[Tuple[str, str]]) -> List[Dict[str, Any]]:
    """
    Get child organizational units from a parent unit.

    Args:
        unit_data: Parent unit data
        parent_path: Path to parent unit

    Returns:
        List of child organizational units
    """
    # Keys to exclude (not child org units)
    exclude_keys = {'unit_title', 'summary_1', 'summary_2', 'unit_definitions',
                    'begin_section', 'stop_section', 'begin_article', 'stop_article',
                    'begin_recital', 'stop_recital'}

    children = []
    for key, value in unit_data.items():
        if key in exclude_keys or not isinstance(value, dict):
            continue

        # This is a child organizational unit type (e.g., 'chapter', 'part')
        for child_id, child_data in value.items():
            child_path = parent_path + [(key, child_id)]

            children.append({
                'type': key,
                'id': child_id,
                'title': child_data.get('unit_title', f'{key.title()} {child_id}'),
                'summary_1': child_data.get('summary_1', ''),
                'summary_2': child_data.get('summary_2', ''),
                'path': child_path,
                'has_children': any(
                    k for k in child_data.keys()
                    if k not in exclude_keys and isinstance(child_data[k], dict)
                )
            })

    return children


def get_substantive_units_in_range(doc: Dict[str, Any], unit_data: Dict[str, Any]) -> List[Tuple[str, str, Dict[str, Any]]]:
    """
    Get substantive units within an organizational unit's range.

    Args:
        doc: Document dictionary
        unit_data: Organizational unit data (with begin/stop markers)

    Returns:
        List of (unit_type, unit_number, unit_data) tuples
    """
    content = doc.get('content', {})
    doc_info = doc.get('document_information', {})
    parameters = doc_info.get('parameters', {})

    # Find operational parameters; exclude sub-unit types (they have no org begin/stop markers)
    operational_params = [
        param_data.get('name_plural', 'units')
        for param_id, param_data in parameters.items()
        if param_data.get('operational') == 1 and not param_data.get('is_sub_unit')
    ]

    if not operational_params:
        return []

    units_in_range = []

    for unit_type in operational_params:
        # Check for begin/stop markers
        begin_key = f'begin_{unit_type[:-1] if unit_type.endswith("s") else unit_type}'  # singular
        stop_key = f'stop_{unit_type[:-1] if unit_type.endswith("s") else unit_type}'

        begin_num = unit_data.get(begin_key, '')
        stop_num = unit_data.get(stop_key, '')

        if not begin_num and not stop_num:
            continue

        # Get units of this type from content
        all_units = content.get(unit_type, {})

        if begin_num and stop_num:
            # Filter units in range
            for unit_num, unit_data_item in all_units.items():
                # Simple numeric comparison (works for most cases)
                try:
                    num_val = int(unit_num.split('-')[0])  # Handle ranges like "123-1"
                    begin_val = int(begin_num.split('-')[0])
                    stop_val = int(stop_num.split('-')[0])

                    if begin_val <= num_val <= stop_val:
                        units_in_range.append((unit_type, unit_num, unit_data_item))
                except (ValueError, AttributeError):
                    # Fallback: string comparison
                    if begin_num <= unit_num <= stop_num:
                        units_in_range.append((unit_type, unit_num, unit_data_item))
        else:
            # No range specified, include all
            for unit_num, unit_data_item in all_units.items():
                units_in_range.append((unit_type, unit_num, unit_data_item))

    return units_in_range


def render_org_unit():
    """Render organizational unit view."""
    doc = st.session_state.viewer_document
    unit_id = st.session_state.viewer_current_unit

    if not doc or not unit_id:
        st.error("❌ No unit selected")
        return

    # Get unit data
    unit_data = get_unit_data(doc, unit_id)
    if not unit_data:
        st.error(f"❌ Unit not found: {unit_id}")
        return

    # Parse unit path
    path = parse_unit_path(unit_id)
    unit_type, unit_number = path[-1] if path else ('Unit', '')

    # Header
    unit_title = unit_data.get('unit_title', f'{unit_type.title()} {unit_number}')

    st.title(f"📂 {unit_type.title()} {unit_number}")
    st.markdown(f"**{unit_title}**")

    st.markdown("---")

    # Summary section
    summary_level = st.session_state.viewer_summary_level
    summary_1 = unit_data.get('summary_1', '')
    summary_2 = unit_data.get('summary_2', '')

    if summary_1 or summary_2:
        st.subheader("📋 Summary")

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

        st.markdown("---")

    # Child organizational units
    child_units = get_child_org_units(unit_data, path)

    if child_units:
        st.subheader("📂 Sub-Units")
        st.markdown(f"*This {unit_type} contains {len(child_units)} organizational sub-unit(s)*")
        st.markdown("")

        for child in child_units:
            with st.container():
                col1, col2 = st.columns([4, 1])

                with col1:
                    child_label = f"**{child['type'].title()} {child['id']}**: {child['title']}"
                    st.markdown(child_label)

                with col2:
                    # Build path for navigation
                    child_path = [dict([p]) for p in child['path']]

                    if st.button(
                        "Explore →",
                        key=f"nav_child_{child['type']}_{child['id']}",
                        use_container_width=True
                    ):
                        navigation.navigate_to_org_unit(child_path, child['title'])

                # Summary preview
                if child['summary_1']:
                    preview = child['summary_1'][:150]
                    if len(child['summary_1']) > 150:
                        preview += "..."
                    st.caption(preview)

                # Visual indicator
                if child['has_children']:
                    st.caption("📂 Contains sub-units")

                st.markdown("---")

    # Substantive units in this org unit's range
    substantive_units = get_substantive_units_in_range(doc, unit_data)

    if substantive_units:
        st.subheader("📄 Sections")

        # Get unit type name from parameters
        doc_info = doc.get('document_information', {})
        parameters = doc_info.get('parameters', {})

        # Group by type
        by_type = {}
        for unit_type_plural, unit_num, unit_data_item in substantive_units:
            if unit_type_plural not in by_type:
                by_type[unit_type_plural] = []
            by_type[unit_type_plural].append((unit_num, unit_data_item))

        for unit_type_plural, units in by_type.items():
            st.markdown(f"*Contains {len(units)} {unit_type_plural}*")
            st.markdown("")

            # Pagination for many units
            items_per_page = 20
            total_pages = (len(units) + items_per_page - 1) // items_per_page

            page_key = f'orgunit_page_{unit_id}_{unit_type_plural}'
            if page_key not in st.session_state:
                st.session_state[page_key] = 0

            if total_pages > 1:
                col1, col2, col3 = st.columns([1, 2, 1])
                with col1:
                    if st.button(
                        "← Previous",
                        key=f"prev_{page_key}",
                        disabled=st.session_state[page_key] == 0
                    ):
                        st.session_state[page_key] -= 1
                        st.rerun()
                with col2:
                    st.markdown(f"Page {st.session_state[page_key] + 1} of {total_pages}")
                with col3:
                    if st.button(
                        "Next →",
                        key=f"next_{page_key}",
                        disabled=st.session_state[page_key] >= total_pages - 1
                    ):
                        st.session_state[page_key] += 1
                        st.rerun()

            # Display page
            start_idx = st.session_state[page_key] * items_per_page
            end_idx = min(start_idx + items_per_page, len(units))
            page_units = units[start_idx:end_idx]

            for unit_num, unit_data_item in page_units:
                with st.container():
                    col1, col2 = st.columns([4, 1])

                    unit_title_text = unit_data_item.get('unit_title', f'{unit_type_plural[:-1]} {unit_num}')

                    with col1:
                        # Get singular name from parameters
                        unit_singular = unit_type_plural[:-1] if unit_type_plural.endswith('s') else unit_type_plural

                        st.markdown(f"**{unit_singular.title()} {unit_num}**: {unit_title_text}")

                    with col2:
                        if st.button(
                            "View →",
                            key=f"nav_subst_{unit_type_plural}_{unit_num}",
                            use_container_width=True
                        ):
                            navigation.navigate_to_substantive_unit(
                                unit_type_plural,
                                unit_num,
                                unit_title_text
                            )

                    # Summary preview
                    summary_preview = unit_data_item.get('summary_1', '')
                    if summary_preview:
                        preview_text = summary_preview[:150]
                        if len(summary_preview) > 150:
                            preview_text += "..."
                        st.caption(preview_text)

                    st.markdown("---")

    # Definitions section
    if st.session_state.viewer_show_definitions:
        unit_definitions = unit_data.get('unit_definitions', [])

        if unit_definitions:
            st.subheader("📖 Definitions")
            st.markdown(f"*{len(unit_definitions)} definition(s) scoped to this {unit_type}*")
            st.markdown("")

            from ui.viewer_components import definitions_panel
            definitions_panel.render_definitions_list(unit_definitions)
