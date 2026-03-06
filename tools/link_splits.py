"""
Detect and register parent-child split relationships among corpus documents — Task 3.9

Two corpus documents are in a parent-child relationship when they have the same
parser type and one document's content_scope is a proper prefix of the other's.

For example, if document A has:
    content_scope = [{"title": "8"}]
and document B has:
    content_scope = [{"title": "8"}, {"chapter": "12"}]
then A is the parent of B (split_type = "chapter").

The tool finds the IMMEDIATE parent for each document: the corpus document whose
content_scope is the longest proper prefix of the child's scope.

Documents with an empty content_scope are never treated as children (they are
not scoped sub-documents). They may be parents if other documents have scopes
that are proper extensions of theirs.

Usage:
    python tools/link_splits.py
    python tools/link_splits.py --db /path/to/registry.db
    python tools/link_splits.py --dry-run
"""
# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from registry.registry import Registry


# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------

def _load_config() -> dict:
    config_path = Path(__file__).resolve().parent.parent / 'config.json'
    if config_path.exists():
        with open(config_path, encoding='utf-8') as f:
            return json.load(f)
    return {}


def _default_db_path(config: dict) -> Path:
    raw = config.get('output', {}).get('directory', '~/document_analyzer_output')
    return Path(os.path.expanduser(raw)) / 'cross_reference_registry.db'


# ---------------------------------------------------------------------------
# Scope comparison helpers
# ---------------------------------------------------------------------------

def _scope_as_tuples(scope: list) -> List[Tuple[str, str]]:
    """
    Convert a content_scope list into a list of (type, value) tuples for comparison.

    content_scope is a list of single-key dicts like [{"title": "8"}, {"chapter": "12"}].
    Returns [(type, value), ...] in order.
    """
    result = []
    for entry in scope:
        if not isinstance(entry, dict):
            continue
        for k, v in entry.items():
            result.append((str(k), str(v)))
            break  # each dict should have exactly one key
    return result


def _is_proper_prefix(shorter: list, longer: list) -> bool:
    """
    Return True if shorter is a proper prefix of longer (both as tuple lists).
    """
    if not shorter:
        return False  # empty scope is never a meaningful prefix
    if len(shorter) >= len(longer):
        return False
    return longer[:len(shorter)] == shorter


def find_split_type(parent_tuples: list, child_tuples: list) -> str:
    """
    Return the type key of the first scope step that the child has beyond the parent.
    This is the dimension along which the document was split.
    """
    if len(child_tuples) > len(parent_tuples):
        return child_tuples[len(parent_tuples)][0]
    return 'unknown'


# ---------------------------------------------------------------------------
# Detection
# ---------------------------------------------------------------------------

def detect_splits(
    registry: Registry,
) -> List[Tuple[int, int, str]]:
    """
    Scan all corpus documents and return (parent_id, child_id, split_type) tuples
    for any parent-child split relationship found.

    For each document with a non-empty content_scope, finds the document whose
    content_scope is the longest proper prefix (immediate parent).

    Only considers documents with the same parser_type as candidates.
    """
    all_docs = registry.get_all_documents()

    # Build index: parser_type → list of (doc_id, scope_tuples, doc_row)
    by_parser: Dict[str, list] = {}
    for doc in all_docs:
        metadata = {}
        try:
            metadata = json.loads(doc.get('metadata') or '{}')
        except Exception:
            pass
        scope = _scope_as_tuples(metadata.get('content_scope') or [])
        parser_type = doc.get('parser_type', 'unknown')
        by_parser.setdefault(parser_type, []).append((doc['id'], scope, doc))

    splits: List[Tuple[int, int, str]] = []

    for parser_type, entries in by_parser.items():
        # For each document with a non-empty scope, find its immediate parent
        for child_id, child_scope, child_doc in entries:
            if not child_scope:
                continue  # not a scoped child document

            # Find best (longest prefix) parent among same-parser documents
            best_parent_id: Optional[int] = None
            best_parent_len = -1

            for parent_id, parent_scope, parent_doc in entries:
                if parent_id == child_id:
                    continue
                if _is_proper_prefix(parent_scope, child_scope):
                    if len(parent_scope) > best_parent_len:
                        best_parent_len = len(parent_scope)
                        best_parent_id = parent_id

            if best_parent_id is not None:
                best_parent_scope = next(
                    s for pid, s, _ in entries if pid == best_parent_id
                )
                split_type = find_split_type(best_parent_scope, child_scope)
                splits.append((best_parent_id, child_id, split_type))

    return splits


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description='Detect and register parent-child document split relationships.',
    )
    parser.add_argument(
        '--db',
        help='Path to the registry SQLite database. Defaults to output_dir from config.json.',
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Print detected splits without writing to the registry.',
    )
    args = parser.parse_args()

    config = _load_config()
    db_path = Path(args.db) if args.db else _default_db_path(config)

    if not db_path.exists():
        print(f"Registry database not found: {db_path}", file=sys.stderr)
        print("Run tools/extract_references.py --all first.", file=sys.stderr)
        sys.exit(1)

    registry = Registry(str(db_path))
    splits = detect_splits(registry)

    if not splits:
        print("No split relationships detected.")
        return

    print(f"Detected {len(splits)} parent-child split relationship(s):")
    for parent_id, child_id, split_type in splits:
        parent = registry.get_document_by_id(parent_id)
        child = registry.get_document_by_id(child_id)
        parent_name = Path(parent['file_path']).name if parent else f"id={parent_id}"
        child_name = Path(child['file_path']).name if child else f"id={child_id}"
        print(f"  [{split_type}] {parent_name} → {child_name}")

    if args.dry_run:
        print("(dry-run: no changes written)")
        return

    registered = 0
    for parent_id, child_id, split_type in splits:
        registry.add_document_split(parent_id, child_id, split_type)
        registered += 1

    print(f"Registered {registered} split relationship(s).")


if __name__ == '__main__':
    main()
