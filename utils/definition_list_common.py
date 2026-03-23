"""
Shared helpers for definition-list sub-unit extraction across parsers.

A definition-list sub-unit is created at parse time when an article (or equivalent
containing element) has a title containing "definition"/"definitions" AND contains a
<LIST> element that is large enough to warrant independent chunked processing.

Two new optional sub-unit fields are introduced here:
  - placeholder (str): exact string used in the parent unit's text in place of the
    sub-unit's content, enabling deterministic text reconstruction.
  - chunk_prefix (str): text prepended to every chunk when the sub-unit is sent to AI
    models, providing scope context that lives only in the parent unit's text.

Design: ROADMAP.md Workstream 11, task 11.0
"""
# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License

import re
from typing import Any, Dict, List, Set


def find_or_create_definition_list_param_key(param_pointer: Dict[str, Any]) -> int:
    """
    Find the parameter key for the definition-list sub-unit type, creating it if absent.

    Identified by definition_list: 1 + is_sub_unit: True.
    Allocates the next available integer key and registers a parameter entry with
    name 'definition_list' / 'definition_lists'.
    Returns the integer key.
    """
    for key, p in param_pointer.items():
        if p.get('definition_list') and p.get('is_sub_unit'):
            return int(key) if isinstance(key, str) else key
    int_keys = [k for k in param_pointer if isinstance(k, int)]
    next_key = max(int_keys) + 1 if int_keys else 10
    param_pointer[next_key] = {
        "name": "definition_list",
        "name_plural": "definition_lists",
        "operational": 1,
        "is_sub_unit": True,
        "definition_list": 1,
    }
    return next_key


def assign_definition_list_key(local_num: int, parent_id: str, taken: Set[str]) -> str:
    """
    Return a unique sub-unit key for the local_num-th definition list in parent_id.

    Primary:  str(local_num)          e.g. "1"
    Fallback: "{local_num}_{suffix}"  where suffix is the last 6 alphanumeric
              chars of parent_id, lowercased.
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


def build_definition_list_sub_unit(
    text: str,
    breakpoints: List,
    chunk_prefix: str,
    placeholder: str,
    parent_context: List[Dict],
    parent_type_name: str,
    parent_id: str,
) -> Dict[str, Any]:
    """
    Build a definition-list sub-unit dict in the standard v0.5 sub-unit format.

    Args:
        text:             Full text of the list items.
        breakpoints:      [offset, level] breakpoints at each item boundary (after the first).
        chunk_prefix:     Scope preamble text to prepend to every AI chunk.
        placeholder:      Exact string used in the parent unit's text in place of this sub-unit.
        parent_context:   Context list from the parent unit.
        parent_type_name: Type name of the parent (e.g. "article").
        parent_id:        ID of the parent (e.g. "3").

    Returns:
        Sub-unit dict ready to be inserted into item_pointer['sub_units'][key_str][sub_unit_key].
    """
    return {
        "text": text,
        "breakpoints": breakpoints,
        "chunk_prefix": chunk_prefix,
        "placeholder": placeholder,
        "context": list(parent_context) + [{parent_type_name: parent_id}],
    }


def title_contains_definition(title_text: str) -> bool:
    """
    Return True if title_text contains the word 'definition' or 'definitions'
    (case-insensitive, whole-word match).
    """
    return bool(re.search(r'\bdefinitions?\b', title_text, re.IGNORECASE))
