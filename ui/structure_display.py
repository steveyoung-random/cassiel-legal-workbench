"""
Display Discovered Organizational Structure

Functions for displaying organizational structure discovered from parsed documents.
"""
# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License

import streamlit as st
from typing import Dict, Any, List, Optional
from parsers.discovery import discover_organizational_units, discover_filterable_units


def display_document_structure(parsed_content: Dict[str, Any]):
    """
    Display organizational structure discovered in document.

    Args:
        parsed_content: Parsed document content dictionary
    """
    org_content = parsed_content.get('document_information', {}).get('organization', {}).get('content', {})

    if not org_content:
        st.info("No organizational structure found in document")
        return

    def render_level(content_dict: Dict[str, Any], indent: int = 0):
        """Recursively render organizational structure"""
        for unit_type, unit_values in content_dict.items():
            # Skip metadata keys
            if unit_type in ['unit_title', 'begin_section', 'stop_section',
                           'begin_article', 'stop_article', 'begin_recital', 'stop_recital']:
                continue

            # unit_type is discovered (e.g., "chapter", "part", "division")
            if not isinstance(unit_values, dict):
                continue

            for unit_number, unit_content in unit_values.items():
                if not isinstance(unit_content, dict):
                    continue

                unit_title = unit_content.get('unit_title', '')
                indent_str = "  " * indent
                st.write(f"{indent_str}**{unit_type.capitalize()} {unit_number}**: {unit_title}")

                # Recurse to nested levels
                nested_content = {
                    k: v for k, v in unit_content.items()
                    if k not in ['unit_title', 'begin_section', 'stop_section',
                               'begin_article', 'stop_article', 'begin_recital', 'stop_recital']
                    and isinstance(v, dict)
                }
                if nested_content:
                    render_level(nested_content, indent + 1)

    st.subheader("Document Structure")
    render_level(org_content)


def display_filterable_units(manifest: Dict[str, Any]):
    """
    Display filterable organizational units from manifest.

    Args:
        manifest: Manifest dictionary
    """
    filterable = discover_filterable_units(manifest)

    if not filterable:
        st.info("No filterable organizational units found")
        return

    st.write("**Available filters:**")
    for unit_type in sorted(filterable):
        st.write(f"- {unit_type}")


def format_organizational_units(org_units: Dict[str, str]) -> str:
    """
    Format organizational units dict for display.

    Args:
        org_units: Dict of organizational unit values (e.g., {'title': '42', 'chapter': '6A'})

    Returns:
        Formatted string (e.g., "Title 42, Chapter 6A")
    """
    if not org_units:
        return "N/A"

    parts = []
    for key, value in sorted(org_units.items()):
        parts.append(f"{key.capitalize()} {value}")

    return ", ".join(parts)

