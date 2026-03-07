"""
Large Table Handling Utilities

Provides functions for extracting large HTML tables from parsed document text
and converting them into first-class `table` sub-units. This allows the pipeline
to handle large data tables (e.g., regulatory entity lists, tariff schedules)
without attempting to process them as legal prose.

Design rationale: LARGE_TABLE_HANDLING.md
"""
# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License

import re
from typing import Any, Dict, List, Optional, Set, Tuple


# Minimum number of <TR> tags for a TABLE to be extracted as a sub-unit.
LARGE_TABLE_ROW_THRESHOLD = 50

# Regex to find TABLE elements (non-greedy, handles attributes on opening tag).
# Matches <TABLE ...> ... </TABLE> case-insensitively.
# Note: does not handle arbitrarily nested TABLEs within TABLEs.
_TABLE_RE = re.compile(
    r'<TABLE(?:\s[^>]*)?>.*?</TABLE\s*>',
    re.IGNORECASE | re.DOTALL,
)

# Regex to count <TR> tags within a TABLE block.
_TR_RE = re.compile(r'<TR[\s>]', re.IGNORECASE)

# Regex to find <TH> element text within a THEAD block.
_THEAD_RE = re.compile(r'<THEAD\b[^>]*>(.*?)</THEAD\s*>', re.IGNORECASE | re.DOTALL)
_TH_RE = re.compile(r'<TH\b[^>]*>(.*?)</TH\s*>', re.IGNORECASE | re.DOTALL)
_TAG_RE = re.compile(r'<[^>]+>')

# Regex to find <CAPTION> element text.
_CAPTION_RE = re.compile(r'<CAPTION\b[^>]*>(.*?)</CAPTION\s*>', re.IGNORECASE | re.DOTALL)


def _strip_tags(html: str) -> str:
    """Remove all HTML tags from a string and collapse whitespace."""
    return _TAG_RE.sub('', html).strip()


def _extract_column_headers(table_html: str) -> List[str]:
    """
    Extract column header text from <TH> elements within the first <THEAD>.

    Returns an empty list if no THEAD or no TH elements are found.
    """
    thead_match = _THEAD_RE.search(table_html)
    if not thead_match:
        return []
    thead_content = thead_match.group(1)
    headers = [_strip_tags(m.group(1)) for m in _TH_RE.finditer(thead_content)]
    return [h for h in headers if h]  # filter empty strings


def _extract_caption(table_html: str) -> str:
    """
    Extract text from the first <CAPTION> element in the table HTML.

    Returns an empty string if no CAPTION is present.
    """
    caption_match = _CAPTION_RE.search(table_html)
    if not caption_match:
        return ""
    return _strip_tags(caption_match.group(1))


def _make_parent_suffix(parent_id: str) -> str:
    """
    Derive a short alphanumeric suffix from a parent item ID for use in
    disambiguated table keys.

    Keeps only alphanumeric characters and takes the last 6, lowercased.
    E.g. "No. 4 to Part 744" → "rt744"
    """
    alphanum = re.sub(r'[^A-Za-z0-9]', '', parent_id)
    return alphanum[-6:].lower() if alphanum else "x"


def _unique_key(local_num: int, parent_id: str, taken: Set[str]) -> str:
    """
    Return a sub-unit key for the ``local_num``-th table in ``parent_id``.

    Primary:  str(local_num)  — e.g. "1", "2"
    Fallback: "{local_num}_{suffix}" where suffix is derived from parent_id.

    The returned key is guaranteed not to be in ``taken``; ``taken`` is NOT
    mutated here — the caller is responsible for adding the returned key.
    """
    candidate = str(local_num)
    if candidate not in taken:
        return candidate
    suffix = _make_parent_suffix(parent_id)
    candidate = f"{local_num}_{suffix}"
    if candidate not in taken:
        return candidate
    # Ultimate fallback: keep appending a counter until unique
    i = 2
    while True:
        candidate = f"{local_num}_{suffix}{i}"
        if candidate not in taken:
            return candidate
        i += 1


def extract_large_tables(
    text: str,
    context: List[Dict[str, str]],
    parent_type_name: str,
    parent_id: str,
    existing_keys: Optional[Set[str]] = None,
    table_type_name: str = "table",
) -> Tuple[str, Dict[str, Any], Dict[str, Any]]:
    """
    Scan ``text`` for large HTML TABLE elements and extract them as sub-units.

    A TABLE is "large" if it contains at least LARGE_TABLE_ROW_THRESHOLD <TR>
    tags.  Small tables are left in place; large tables are replaced with an
    inline placeholder and returned as sub-unit dicts.

    Sub-unit keys are derived from the local table counter ("1", "2", …) so
    that they match the natural label used in document prose ("Table 1", etc.)
    and the Stage 4 prompt header ("Table 1: Table 1") is unambiguous.  If a
    key would collide with one already registered in the document-level index
    (passed via ``existing_keys``), a short suffix derived from ``parent_id``
    is appended to make it globally unique.

    Args:
        text: The substantive unit text (may contain HTML TABLE elements).
        context: Context list from the parent item (list of {type: id} dicts).
        parent_type_name: Type name of the parent item (e.g. "supplement").
        parent_id: Identifier of the parent item (e.g. "No. 4 to Part 744").
        existing_keys: Optional set of sub-unit keys already registered in the
            document-level ``sub_unit_index`` for the table param type.  Used
            to avoid key collisions when multiple parent items each contain a
            "Table 1".  Pass ``set(sub_unit_index.get(str(TABLE_PARAM_KEY),
            {}).keys())`` from the parser.

    Returns:
        A 3-tuple:
          - modified_text (str): ``text`` with large TABLE HTML replaced by
            inline placeholder markers, e.g.
            ``[Table 1 extracted as sub-unit table 1]``.
          - table_sub_units (dict): Keys are the assigned sub-unit keys
            ("1", "2", … or disambiguated variants).
            Values are sub-unit dicts ready to be stored under a ``sub_units``
            type-keyed entry.
          - index_entries (dict): Keys match ``table_sub_units``; values are
            index entry dicts ``{container_plural, container_id, path}``.

        If no qualifying tables are found, returns ``(text, {}, {})``.
    """
    if not text:
        return text, {}, {}

    # Working copy of the taken-key set; we add to it as we assign keys so
    # that tables within the same parent also don't collide with each other.
    taken: Set[str] = set(existing_keys) if existing_keys else set()

    table_sub_units: Dict[str, Any] = {}
    index_entries: Dict[str, Any] = {}
    modified_text = text
    local_counter = 0   # counts qualifying tables within this parent item
    offset_adjustment = 0

    for match in _TABLE_RE.finditer(text):
        table_html = match.group(0)
        row_count = len(_TR_RE.findall(table_html))

        if row_count < LARGE_TABLE_ROW_THRESHOLD:
            continue  # Small table — leave in place

        local_counter += 1
        sub_unit_key = _unique_key(local_counter, parent_id, taken)
        taken.add(sub_unit_key)

        # Inline placeholder — references both the natural table number and
        # the assigned key so the LLM can connect prose references ("Table 1")
        # to the sub-unit identifier.
        placeholder = f"[Table {local_counter} extracted as sub-unit {table_type_name} {sub_unit_key}]"

        # Replace this occurrence in modified_text
        start = match.start() + offset_adjustment
        end = match.end() + offset_adjustment
        modified_text = modified_text[:start] + placeholder + modified_text[end:]
        offset_adjustment += len(placeholder) - len(table_html)

        # Extract metadata
        column_headers = _extract_column_headers(table_html)
        caption = _extract_caption(table_html)

        # Sub-unit context: parent context + this parent as final entry
        sub_context = list(context) + [{parent_type_name: parent_id}]

        # unit_title uses the natural table number so Stage 4 prompt headers
        # read "Table 1: Table 1" — matching what the document prose says.
        unit_title = f"Table {local_counter}"

        sub_unit = {
            "text": "",
            "table_html": table_html,
            "table_row_count": row_count,
            "table_column_headers": column_headers,
            "table_caption": caption,
            "unit_title": unit_title,
            "context": sub_context,
            "breakpoints": [],
        }
        table_sub_units[sub_unit_key] = sub_unit

        # Container plural form (simple heuristic: append 's')
        container_plural = parent_type_name + "s"
        index_entries[sub_unit_key] = {
            "container_plural": container_plural,
            "container_id": parent_id,
            "path": [parent_id],
        }

    if not table_sub_units:
        return text, {}, {}

    return modified_text, table_sub_units, index_entries
