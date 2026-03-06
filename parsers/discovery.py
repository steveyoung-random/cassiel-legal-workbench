"""
Discovery Functions

Functions to discover organizational units from parsed output and manifests.

Key principle: Organizational structure is already in the parsed output - we just
need to read it. These functions scan the existing data structures to discover
what organizational units exist.
"""
# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License

from typing import Dict, Any, Set


def discover_organizational_units(parsed_content: Dict[str, Any]) -> Set[str]:
    """
    Discover all organizational unit type names in a parsed document.

    Scans organization.content nested dict to find all keys (which are unit type names).
    These unit type names are discovered during parsing, not predefined.

    Args:
        parsed_content: Parsed document structure (JSON dict)

    Returns:
        Set of organizational unit type names found in this document.
        Example: {'title', 'chapter', 'subchapter', 'part', 'division'}

    Note:
        This function discovers the unit TYPE NAMES (e.g., "chapter", "part"),
        not the specific unit values (e.g., "Chapter 6A").
    """
    org_content = parsed_content.get('document_information', {}).get('organization', {}).get('content', {})

    unit_names = set()

    # Metadata keys to skip (these are not organizational unit type names)
    metadata_keys = {
        'unit_title',
        'begin_section', 'stop_section',
        'begin_article', 'stop_article',
        'begin_recital', 'stop_recital',
        'begin_chapter', 'stop_chapter',
        'begin_part', 'stop_part',
        'begin_division', 'stop_division',
        'begin_subchapter', 'stop_subchapter',
        'begin_subtitle', 'stop_subtitle',
    }

    def scan_level(content_dict: Dict[str, Any]):
        """Recursively scan nested dict for unit type names"""
        if not isinstance(content_dict, dict):
            return

        for key, value in content_dict.items():
            # Skip metadata keys
            if key in metadata_keys:
                continue

            # This is an organizational unit type name
            unit_names.add(key)

            if isinstance(value, dict):
                # Scan nested levels
                for inner_value in value.values():
                    if isinstance(inner_value, dict):
                        scan_level(inner_value)

    scan_level(org_content)
    return unit_names


def discover_filterable_units(manifest: Dict[str, Any]) -> Set[str]:
    """
    Discover what organizational units can be filtered on from a manifest.

    Scans organizational_units dicts in parsed_files entries to find what
    unit type names appear in this manifest.

    Args:
        manifest: Manifest dictionary

    Returns:
        Set of filterable unit type names.
        Example: {'title', 'chapter'}

    Note:
        These are the keys that can be used in filter expressions for this manifest.
        Example: filter by {'title': '42', 'chapter': '6A'}
    """
    filterable_units = set()

    for parsed_file in manifest.get('parsed_files', []):
        org_units = parsed_file.get('organizational_units', {})
        filterable_units.update(org_units.keys())

    return filterable_units


def get_organizational_structure_tree(parsed_content: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract the organizational structure tree from parsed content.

    This returns the actual nested structure, not just the unit type names.
    Useful for displaying document structure in UI.

    Args:
        parsed_content: Parsed document structure

    Returns:
        The organization.content dict (nested structure)
    """
    return parsed_content.get('document_information', {}).get('organization', {}).get('content', {})


def format_organizational_units_display(org_units: Dict[str, str]) -> str:
    """
    Format organizational units dict for display.

    Args:
        org_units: Dict like {'title': '42', 'chapter': '6A'}

    Returns:
        Formatted string like "Title 42, Chapter 6A"
    """
    if not org_units:
        return ""

    parts = [f"{key.capitalize()} {value}" for key, value in org_units.items()]
    return ", ".join(parts)
