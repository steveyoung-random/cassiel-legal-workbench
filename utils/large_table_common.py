"""
Shared helpers for XML-level large table interception across parsers.

Used by USLM, Formex, CA, and CFR parsers to:
- Assign unique sub-unit keys for tables
- Find or create the generic 'table' parameter type
- Re-export LARGE_TABLE_ROW_THRESHOLD

Design: LARGE_TABLE_HANDLING.md
"""
# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License

import re
from typing import Any, Dict, List, Set


from .table_handling import LARGE_TABLE_ROW_THRESHOLD


def assign_table_key(local_num: int, parent_id: str, taken: Set[str]) -> str:
    """
    Return a globally unique sub-unit key for the local_num-th table in parent_id.

    Primary:  str(local_num)          e.g. "1", "2"
    Fallback: "{local_num}_{suffix}"  where suffix is the last 6 alphanumeric
              chars of parent_id, lowercased (e.g. "1_rt744").
    """
    candidate = str(local_num)
    if candidate not in taken:
        return candidate
    suffix = re.sub(r'[^A-Za-z0-9]', '', parent_id)[-6:].lower() or 'x'
    candidate = f"{local_num}_{suffix}"
    if candidate not in taken:
        return candidate
    i = 2
    while True:
        candidate = f"{local_num}_{suffix}{i}"
        if candidate not in taken:
            return candidate
        i += 1


def find_or_create_table_param_key(param_pointer: Dict[str, Any]) -> int:
    """
    Find the parameter key for the data-table sub-unit type.

    Identified by data_table: 1 + is_sub_unit: True (not by name, since the name
    may be 'table' or 'large_table' depending on whether 'table' was already taken
    by a regular non-sub-unit operational parameter when the entry was first created).

    If not already registered, allocates the next available integer key
    (max existing integer key + 1) and registers the parameter entry.
    Uses name 'table' unless that name is already taken by a non-sub-unit parameter,
    in which case it uses 'large_table' to avoid the conflict.
    Returns the integer key.
    """
    for key, p in param_pointer.items():
        if p.get('data_table') and p.get('is_sub_unit'):
            return int(key) if isinstance(key, str) else key
    # Determine name: use 'table' unless a non-sub-unit param already claims it.
    non_sub_unit_names = {p['name'] for p in param_pointer.values()
                          if not p.get('is_sub_unit') and 'name' in p}
    if 'table' in non_sub_unit_names:
        name, name_plural = 'large_table', 'large_tables'
    else:
        name, name_plural = 'table', 'tables'
    # Allocate next available key
    int_keys = [k for k in param_pointer if isinstance(k, int)]
    next_key = max(int_keys) + 1 if int_keys else 10
    param_pointer[next_key] = {
        "name": name,
        "name_plural": name_plural,
        "operational": 1,
        "is_sub_unit": True,
        "data_table": 1,
    }
    return next_key


def assign_method_section_key(hd1_text: str, local_num: int, taken: Set[str]) -> str:
    """
    Return a globally unique sub-unit key for a method-section sub-unit.

    Primary: the method/procedure number extracted from the HD1 heading text,
    e.g. "301" from "Method 301—Field Validation..." or "2G" from "Method 2G—...".
    Fallback: if that key is already taken (rare — different appendices share number
    ranges in practice), append a numeric suffix until unique.
    """
    import re
    m = re.match(
        r'^(?:Method|Performance Specification|Procedure)\s+(\d+[A-Za-z]*)',
        hd1_text, re.IGNORECASE
    )
    base = m.group(1) if m else str(local_num)
    if base not in taken:
        return base
    suffix = 2
    while f"{base}_{suffix}" in taken:
        suffix += 1
    return f"{base}_{suffix}"


def find_or_create_method_section_param_key(param_pointer: Dict[str, Any]) -> int:
    """
    Find the parameter key for the method-section sub-unit type, creating it if absent.

    Identified by method_section: 1 + is_sub_unit: True.
    Allocates the next available integer key and registers the parameter entry
    with name 'method_section' / 'method_sections'.
    Returns the integer key.
    """
    for key, p in param_pointer.items():
        if p.get('method_section') and p.get('is_sub_unit'):
            return int(key) if isinstance(key, str) else key
    int_keys = [k for k in param_pointer if isinstance(k, int)]
    next_key = max(int_keys) + 1 if int_keys else 10
    param_pointer[next_key] = {
        "name": "method_section",
        "name_plural": "method_sections",
        "operational": 1,
        "is_sub_unit": True,
        "method_section": 1,
    }
    return next_key


def build_table_sub_unit_from_html(
    html_str: str,
    row_count: int,
    column_headers: List[str],
    caption: str,
    local_counter: int,
    parent_context: List[Dict[str, str]],
    parent_type_name: str,
    parent_id: str,
) -> Dict[str, Any]:
    """
    Build a table sub-unit dict from pre-serialized HTML and metadata.

    Use when the parser has already serialized the table to HTML and extracted
    headers/caption. Avoids duplicating CFR's GPOTABLE/TABLE-specific logic.
    """
    return {
        "text": "",
        "table_html": html_str,
        "table_row_count": row_count,
        "table_column_headers": column_headers,
        "table_caption": caption,
        "unit_title": f"Table {local_counter}",
        "context": list(parent_context) + [{parent_type_name: parent_id}],
        "breakpoints": [],
    }
