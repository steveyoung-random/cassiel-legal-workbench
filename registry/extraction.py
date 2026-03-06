"""
Document extraction logic for the cross-reference registry.

Core functions shared by tools/extract_references.py (CLI batch tool) and
registry/post_stage3.py (post-Stage-3 pipeline hook).

The key function is extract_from_file(), which registers one processed document
in the corpus index and extracts all External cross-references from it.
"""
# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License

import json
import sys
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from registry.registry import Registry


# ---------------------------------------------------------------------------
# Document inspection helpers
# ---------------------------------------------------------------------------

def is_processed_document(data: dict) -> bool:
    """Return True if the JSON looks like a pipeline output document."""
    return (isinstance(data, dict)
            and 'document_information' in data
            and 'content' in data)


def detect_stage_reached(data: dict) -> int:
    """
    Inspect item fields to infer the highest pipeline stage completed.
    Returns 3 (summaries present), 2 (definitions present), or 1 (parsed only).
    """
    from utils.document_handling import iter_all_items
    for _, _, _, _, item in iter_all_items(data):
        if 'summary_1' in item:
            return 3
    for _, _, _, _, item in iter_all_items(data):
        if 'defined_terms' in item:
            return 2
    return 1


def infer_parser_type(file_path: Path, doc_info: dict) -> str:
    """
    Infer parser type from document_information or directory structure.
    Falls back to 'unknown' if neither source gives a clear answer.
    """
    if doc_info.get('parser_type'):
        return doc_info['parser_type']
    path_str = str(file_path).upper()
    for segment, ptype in (('\\CFR\\', 'cfr'), ('/CFR/', 'cfr'),
                           ('\\USC\\', 'uslm'), ('/USC/', 'uslm'),
                           ('\\CA\\', 'ca_html'), ('/CA/', 'ca_html'),
                           ('\\FORMEX\\', 'formex'), ('/FORMEX/', 'formex')):
        if segment in path_str:
            return ptype
    return 'unknown'


def get_title(doc_info: dict, file_path: Path) -> str:
    """Extract a human-readable title from document_information."""
    for key in ('title', 'document_title', 'name'):
        if doc_info.get(key):
            return doc_info[key]
    return file_path.stem


def collect_org_keys(doc_info: dict) -> dict:
    """
    Recursively collect all organizational structure keys and their values.

    Walks the entire organization.content hierarchy, recording every key
    whose value is a dict (an organizational level such as 'title', 'chapter',
    'part', 'subpart').  The dict's own keys are the identifier values at that
    level (e.g. {"title": ["12"], "part": ["39", "121", ...]}).
    """
    result: dict = {}

    def _walk(node: object) -> None:
        if not isinstance(node, dict):
            return
        for key, val in node.items():
            if not isinstance(val, dict):
                continue
            values = [str(k) for k in val.keys()]
            if key not in result:
                result[key] = []
            for v in values:
                if v not in result[key]:
                    result[key].append(v)
            for child in val.values():
                _walk(child)

    org_content = doc_info.get('organization', {}).get('content', {})
    _walk(org_content)
    return result


# ---------------------------------------------------------------------------
# Core extraction
# ---------------------------------------------------------------------------

def extract_from_file(file_path: Path, registry: 'Registry', force: bool = False) -> dict:
    """
    Register one document and extract all External references from it.

    Idempotent by default: if the document has already been scanned (has a
    last_scanned_at timestamp) and force=False, returns immediately with
    stats['skipped']=1.

    Returns a stats dict:
        processed   — 1 if the file was scanned, 0 if skipped
        skipped     — 1 if already scanned and force=False
        need_ref    — number of need_ref External entries extracted
        definition  — number of external_reference definition entries extracted
        errors      — 1 if an error prevented completion
    """
    from utils.document_handling import iter_all_items

    stats = dict(processed=0, skipped=0, need_ref=0, definition=0, errors=0)

    try:
        with open(file_path, encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        print(f"  ERROR loading {file_path.name}: {e}", file=sys.stderr)
        stats['errors'] = 1
        return stats

    if not is_processed_document(data):
        return stats  # not a pipeline document — silently skip

    doc_info = data.get('document_information', {})
    doc_id = doc_info.get('document_id') or doc_info.get('doc_id')
    title = get_title(doc_info, file_path)
    parser_type = infer_parser_type(file_path, doc_info)

    try:
        stage_reached = detect_stage_reached(data)
    except Exception as e:
        print(f"  ERROR inspecting {file_path.name}: {e}", file=sys.stderr)
        stats['errors'] = 1
        return stats

    org_keys = collect_org_keys(doc_info)

    content_scope = doc_info.get('content_scope') or []

    corpus_doc_id = registry.add_document(
        file_path=str(file_path),
        doc_id=doc_id,
        title=title,
        parser_type=parser_type,
        stage_reached=stage_reached,
        metadata={'org_keys': org_keys, 'content_scope': content_scope},
    )

    existing = registry.get_document_by_id(corpus_doc_id)
    if existing and existing.get('last_scanned_at') and not force:
        stats['skipped'] = 1
        return stats

    if stage_reached < 2:
        # No Stage 2 data: no defined_terms or need_ref to extract.
        # Still record content_items so resolution scoring works.
        try:
            content_items: dict = {}
            for item_type, _, _, item_number, _ in iter_all_items(data):
                item_num_str = str(item_number)
                if item_type not in content_items:
                    content_items[item_type] = []
                if item_num_str not in content_items[item_type]:
                    content_items[item_type].append(item_num_str)
            registry.update_document_metadata(
                corpus_doc_id,
                {'org_keys': org_keys, 'content_items': content_items,
                 'content_scope': content_scope},
            )
        except Exception:
            pass  # metadata is best-effort
        registry.mark_document_scanned(corpus_doc_id)
        stats['processed'] = 1
        return stats

    # Combined pass: collect content_items and extract references in one iteration.
    content_items = {}
    try:
        for item_type, _, _, item_number, item in iter_all_items(data):
            item_number_str = str(item_number)

            if item_type not in content_items:
                content_items[item_type] = []
            if item_number_str not in content_items[item_type]:
                content_items[item_type].append(item_number_str)

            for ref_entry in item.get('need_ref', []):
                if ref_entry.get('type') != 'External':
                    continue
                ref_text = (ref_entry.get('value') or '').strip()
                if not ref_text:
                    continue
                registry.add_reference(
                    source_doc_id=corpus_doc_id,
                    source_item_type=item_type,
                    source_item_number=item_number_str,
                    ref_text=ref_text,
                    ref_context='need_ref',
                )
                stats['need_ref'] += 1

            for defn in item.get('defined_terms', []):
                ext_ref = (defn.get('external_reference') or '').strip()
                if not ext_ref:
                    continue
                term = (defn.get('term') or '').strip() or None
                registry.add_reference(
                    source_doc_id=corpus_doc_id,
                    source_item_type=item_type,
                    source_item_number=item_number_str,
                    ref_text=ext_ref,
                    ref_context='definition',
                    term=term,
                )
                stats['definition'] += 1

    except Exception as e:
        print(f"  ERROR iterating {file_path.name}: {e}", file=sys.stderr)
        stats['errors'] = 1
        return stats

    registry.update_document_metadata(
        corpus_doc_id,
        {'org_keys': org_keys, 'content_items': content_items,
         'content_scope': content_scope},
    )
    registry.mark_document_scanned(corpus_doc_id)
    stats['processed'] = 1
    return stats
