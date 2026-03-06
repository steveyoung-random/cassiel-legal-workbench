"""
Registry integration helpers for Stage 4 — Tasks 3.7, 3.9–3.10

Keeps registry-access logic out of question_answering.py.
All functions fail silently (return empty/None) when the registry DB is absent
or unavailable, so Stage 4 works normally on documents with no registry.
"""
# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License

import json
import os
from pathlib import Path
from typing import Dict, List, Optional


def get_db_path(config: dict) -> Optional[Path]:
    """Return the registry DB path from config, or None if config is unavailable."""
    try:
        from utils.config import get_output_directory
        output_dir = get_output_directory(config)
        return Path(output_dir) / 'cross_reference_registry.db'
    except Exception:
        return None


def load_item_level_refs_for_document(
    processed_file_path: str,
    config: dict,
) -> List[Dict]:
    """
    Return all resolved, item-level cross-references for a processed document.

    Each dict has:
        source_item_type, source_item_number,
        ref_text,
        target_doc_id, target_file_path, target_title,
        target_item_type, target_item_number

    Returns [] if registry unavailable, document not registered,
    or no item-level-resolved refs exist.
    Only returns refs where target_item_type is not None.
    """
    if not processed_file_path:
        return []

    db_path = get_db_path(config)
    if db_path is None or not os.path.exists(str(db_path)):
        return []

    try:
        from registry.registry import Registry
        registry = Registry(str(db_path))

        source_doc = registry.get_document_by_path(processed_file_path)
        if source_doc is None:
            return []

        refs = registry.get_references(
            source_doc_id=source_doc['id'],
            resolution_status='resolved',
        )

        result = []
        target_docs_cache: Dict[int, Optional[Dict]] = {}
        for ref in refs:
            if ref.get('target_item_type') is None:
                continue
            target_doc_id = ref.get('target_doc_id')
            if target_doc_id not in target_docs_cache:
                target_docs_cache[target_doc_id] = registry.get_document_by_id(target_doc_id)
            target_doc = target_docs_cache[target_doc_id]
            if target_doc is None:
                continue
            result.append({
                'source_item_type': ref['source_item_type'],
                'source_item_number': ref['source_item_number'],
                'ref_text': ref['ref_text'],
                'target_doc_id': target_doc_id,
                'target_file_path': target_doc['file_path'],
                'target_title': target_doc.get('title', ''),
                'target_item_type': ref['target_item_type'],
                'target_item_number': ref['target_item_number'],
            })
        return result

    except Exception:
        return []


def load_parent_document_for_qa(
    processed_file_path: str,
    config: dict,
) -> Optional[Dict]:
    """
    Return the parent document's parsed content if a split relationship exists.

    A parent document is one whose content_scope is a proper prefix of this
    document's content_scope (registered via tools/link_splits.py).

    Returns None if:
    - registry DB is unavailable
    - this document is not registered
    - no parent is registered in document_splits
    - the parent's file cannot be read
    """
    if not processed_file_path:
        return None

    db_path = get_db_path(config)
    if db_path is None or not os.path.exists(str(db_path)):
        return None

    try:
        from registry.registry import Registry
        registry = Registry(str(db_path))

        source_doc = registry.get_document_by_path(processed_file_path)
        if source_doc is None:
            return None

        parent = registry.get_parent(source_doc['id'])
        if parent is None:
            return None

        parent_file = parent.get('file_path', '')
        if not parent_file or not os.path.exists(parent_file):
            return None

        with open(parent_file, encoding='utf-8') as f:
            return json.load(f)

    except Exception:
        return None
