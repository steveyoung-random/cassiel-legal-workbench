"""
Definitions Panel Component - Display definitions with formatting.

Renders lists of definitions with appropriate formatting, showing term,
definition text, scope, and source information.
"""
# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License

import streamlit as st
from typing import List, Dict, Any


def render_definition_item(definition: Dict[str, Any], show_source: bool = False):
    """
    Render a single definition item.

    Args:
        definition: Definition dictionary with term, value, scope, etc.
        show_source: Whether to show source information
    """
    term = definition.get('term', 'Unknown')
    value = definition.get('value', 'No definition text available')
    scope = definition.get('scope', '')
    def_kind = definition.get('def_kind', 'direct')

    # Term header
    st.markdown(f"**{term}**")

    # Definition text
    st.markdown(value)

    # Metadata
    metadata_parts = []

    # Scope
    if scope:
        metadata_parts.append(f"*Scope: {scope}*")

    # Kind
    if def_kind == 'indirect':
        indirect_ref = definition.get('indirect', '')
        metadata_parts.append(f"*Type: Indirect reference{' to ' + indirect_ref if indirect_ref else ''}*")
    elif def_kind == 'elaborational':
        metadata_parts.append("*Type: Elaborational*")
    else:
        metadata_parts.append("*Type: Direct definition*")

    # Source (if requested and available)
    if show_source:
        source_type = definition.get('source_type', '')
        source_number = definition.get('source_number', '')

        if source_type and source_number:
            metadata_parts.append(f"*Source: {source_type.title()} {source_number}*")
        elif 'source_org_type' in definition and 'source_org_id' in definition:
            org_type = definition['source_org_type']
            org_id = definition['source_org_id']
            metadata_parts.append(f"*Source: {org_type.title()} {org_id}*")

    # Quality check status
    if definition.get('quality_checked'):
        metadata_parts.append("✓ *Quality checked*")

    # Display metadata
    if metadata_parts:
        st.caption(" | ".join(metadata_parts))

    st.markdown("")  # Spacing


def render_definitions_list(definitions: List[Dict[str, Any]], show_source: bool = False):
    """
    Render a list of definitions.

    Args:
        definitions: List of definition dictionaries
        show_source: Whether to show source information for each definition
    """
    if not definitions:
        st.info("No definitions")
        return

    for definition in definitions:
        render_definition_item(definition, show_source=show_source)


def render_definitions_grouped(definitions: Dict[str, List[Dict[str, Any]]]):
    """
    Render definitions grouped by category.

    Args:
        definitions: Dict with keys like 'document', 'organizational', 'unit', 'external'
                    Each value is a list of definition dicts
    """
    for category, def_list in definitions.items():
        if not def_list:
            continue

        st.markdown(f"### {category.title()} Definitions ({len(def_list)})")

        render_definitions_list(def_list, show_source=(category != 'unit'))

        st.markdown("---")
