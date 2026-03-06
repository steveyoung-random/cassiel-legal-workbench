"""
Extract External Cross-References into Registry — Task 3.2

Scans processed document JSON files and populates the cross-reference registry
(cross_reference_registry.db) with all External references found in each document.

Two reference types are extracted per item:
  need_ref    — entries in item['need_ref'] with type == 'External'
                (section-level: this item depends on something in an external document)
  definition  — entries in item['defined_terms'] with a non-empty 'external_reference'
                (term-level: this term is defined by reference to an external document)

The tool is idempotent: re-scanning a document that was already scanned will not
create duplicate rows in the registry. Use --force to re-scan and pick up any
changes since the last scan.

Documents are only registered in corpus_documents if they contain the expected
top-level structure (document_information + content). Files that do not match
are silently skipped.

Usage:
    python tools/extract_references.py <path> [<path> ...]
    python tools/extract_references.py --all
    python tools/extract_references.py --all --force
    python tools/extract_references.py --db /path/to/registry.db <path>

Arguments:
    path        One or more JSON files or directories (searched recursively).
    --all       Use the output directory from config.json as the source.
    --force     Re-scan documents even if they have already been scanned.
    --db        Path to the registry SQLite database. Defaults to
                {output_dir}/cross_reference_registry.db from config.json.
"""
# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License

import argparse
import json
import os
import sys
from pathlib import Path

# Allow imports from the project root regardless of working directory
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from registry.registry import Registry
from registry.extraction import extract_from_file as _extract_from_file


# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------

def _load_config() -> dict:
    config_path = Path(__file__).resolve().parent.parent / 'config.json'
    if config_path.exists():
        with open(config_path, encoding='utf-8') as f:
            return json.load(f)
    return {}


def _output_dir(config: dict) -> Path:
    raw = config.get('output', {}).get('directory', '~/document_analyzer_output')
    return Path(os.path.expanduser(raw))


def _default_db_path(config: dict) -> Path:
    return _output_dir(config) / 'cross_reference_registry.db'


# ---------------------------------------------------------------------------
# File discovery
# ---------------------------------------------------------------------------

def _collect_json_files(paths: list) -> list:
    """
    Expand a list of file/directory paths into a sorted list of .json Paths.

    When a directory is given, only *_processed.json files are collected.
    Log files, manifest files, parsing-issues files, and *_parse_output.json
    files are excluded — they contain no Stage-2 reference data.

    When an explicit file path is given, it is accepted regardless of name
    (allowing deliberate registration of parse_output or other files).
    """
    result = []
    for raw in paths:
        p = Path(raw)
        if p.is_file():
            if p.suffix == '.json':
                result.append(p)
        elif p.is_dir():
            _SKIP_DIRS = {'old', 'archive'}
            result.extend(
                f for f in sorted(p.rglob('*_processed.json'))
                if not any(part.lower() in _SKIP_DIRS for part in f.parts)
            )
        else:
            print(f"Warning: {raw} is not a file or directory — skipping",
                  file=sys.stderr)
    return result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    config = _load_config()

    parser = argparse.ArgumentParser(
        description='Extract External cross-references from processed documents into the registry.'
    )
    parser.add_argument(
        'paths', nargs='*',
        help='JSON files or directories to scan.'
    )
    parser.add_argument(
        '--all', action='store_true',
        help='Scan the entire output directory (from config.json).'
    )
    parser.add_argument(
        '--force', action='store_true',
        help='Re-scan documents that have already been scanned.'
    )
    parser.add_argument(
        '--db', default=None,
        help='Path to registry database (default: {output_dir}/cross_reference_registry.db).'
    )
    args = parser.parse_args()

    if not args.paths and not args.all:
        parser.print_help()
        sys.exit(1)

    # Resolve DB path
    db_path = Path(args.db) if args.db else _default_db_path(config)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    # Collect files to scan
    scan_paths = list(args.paths)
    if args.all:
        scan_paths.append(str(_output_dir(config)))

    files = _collect_json_files(scan_paths)
    if not files:
        print("No JSON files found.")
        sys.exit(0)

    print(f"Registry: {db_path}")
    print(f"Scanning {len(files)} file(s)...")
    if args.force:
        print("(--force: re-scanning already-scanned documents)")
    print()

    registry = Registry(str(db_path))

    # Process each file
    totals = dict(processed=0, skipped=0, need_ref=0, definition=0, errors=0)
    for file_path in files:
        stats = _extract_from_file(file_path, registry, force=args.force)
        for key in totals:
            totals[key] += stats[key]

        if stats['errors']:
            status = 'ERROR'
        elif stats['skipped']:
            status = 'skipped (already scanned)'
        elif stats['processed']:
            refs = stats['need_ref'] + stats['definition']
            status = f"ok — {refs} references ({stats['need_ref']} need_ref, {stats['definition']} definition)"
        else:
            status = 'skipped (not a pipeline document)'

        print(f"  {file_path.name}: {status}")

    # Summary
    print()
    print("--- Summary ---")
    print(f"Files processed:    {totals['processed']}")
    print(f"Files skipped:      {totals['skipped']}")
    print(f"Errors:             {totals['errors']}")
    print(f"need_ref extracted: {totals['need_ref']}")
    print(f"definition extracted: {totals['definition']}")
    print(f"Total references:   {totals['need_ref'] + totals['definition']}")

    reg_stats = registry.get_registry_stats()
    print()
    print("--- Registry State ---")
    print(f"Corpus documents:   {reg_stats['corpus_documents']}")
    xref = reg_stats['cross_references']
    total_xref = sum(xref.values())
    print(f"Cross-references:   {total_xref} total")
    for status, count in sorted(xref.items()):
        print(f"  {status:<20} {count}")


if __name__ == '__main__':
    main()
