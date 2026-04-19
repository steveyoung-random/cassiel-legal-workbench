"""
Content Refresh & Diff Detection — Workstream 4

Detects which substantive units changed between a new parse output and an existing
processed document, then optionally archives the processed document for full re-run.

Workflow:
  1. User downloads an updated source XML/HTML.
  2. User re-runs the appropriate parser (overwrites *_parse_output.json).
  3. python tools/diff_document.py <path>             → report: what changed
  4. python tools/diff_document.py <path> --archive   → move *_processed.json to
       archive/YYYYMMDD/, clear registry entries, print re-run instructions
  5. python batch_process.py <dir> --stages 2,3       → re-process changed documents
       (API call cache handles unchanged units transparently)

The diff compares parse-time content fields (text, notes, annotation, breakpoints,
unit_title, context, table_html) so that Stage 2/3/4 fields do not trigger false diffs.

Usage:
    python tools/diff_document.py <path> [--archive]
    python tools/diff_document.py <directory> --all [--archive]

Arguments:
    path        Path to *_parse_output.json, *_processed.json, or a directory.
    --archive   Archive *_processed.json and clear its registry entries so Stage 2/3
                re-runs from scratch (cache handles unchanged units).
    --all       Batch mode: operate on every *_parse_output.json found in the
                directory (or the configured output directory if a directory
                is not given). Implies directory-level scanning.
"""
# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License

import argparse
import hashlib
import json
import os
import shutil
import sys
from datetime import date
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Tuple

# Allow imports from project root regardless of working directory
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from utils.manifest_utils import ManifestManager, compute_source_hash


# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------

def _load_config() -> dict:
    config_path = Path(__file__).resolve().parent.parent / 'config.json'
    if config_path.exists():
        with open(config_path, encoding='utf-8') as f:
            return json.load(f)
    return {}


def _get_output_dir(config: dict) -> str:
    from utils.config import get_output_directory
    try:
        return get_output_directory(config)
    except Exception:
        return ''


def _get_registry_db_path(config: dict) -> Optional[str]:
    output_dir = _get_output_dir(config)
    if not output_dir:
        return None
    db = Path(output_dir) / 'cross_reference_registry.db'
    return str(db) if db.exists() else None


# ---------------------------------------------------------------------------
# Content signature
# ---------------------------------------------------------------------------

_PARSE_TIME_FIELDS = ('text', 'notes', 'annotation', 'breakpoints',
                      'unit_title', 'context', 'table_html')


def _unit_signature(item: dict) -> str:
    """
    Hash parse-time content fields of a unit.

    Excludes Stage 2/3/4 fields (defined_terms, summary_1, etc.) so that
    post-processing does not affect the signature.
    """
    parts = [
        item.get('text', '') or '',
        json.dumps(item.get('notes', {}), sort_keys=True),
        item.get('annotation', '') or '',
        json.dumps(item.get('breakpoints', [])),
        item.get('unit_title', '') or '',
        json.dumps(item.get('context', [])),
        item.get('table_html', '') or '',
    ]
    combined = '\x00'.join(parts)
    return hashlib.sha256(combined.encode('utf-8')).hexdigest()


# ---------------------------------------------------------------------------
# Unit enumeration
# ---------------------------------------------------------------------------

def _iter_sub_units(item: dict, parent_path: tuple) -> Iterator[Tuple[tuple, dict]]:
    """Recursively yield (path, sub_item) for all nested sub-units."""
    for sub_type_key, sub_type_items in item.get('sub_units', {}).items():
        if not isinstance(sub_type_items, dict):
            continue
        for sub_item_number, sub_item in sub_type_items.items():
            path = parent_path + (('sub_units', sub_type_key, sub_item_number),)
            yield path, sub_item
            yield from _iter_sub_units(sub_item, path)


def iter_all_units(doc: dict) -> Iterator[Tuple[tuple, dict]]:
    """
    Yield (path, item) for every unit in the document.

    Covers both top-level items and all nested sub-units, including containers
    that have sub_units (which get their own signature over their own fields).

    path format:
        Top-level:  ((type_plural, item_number),)
        Sub-unit:   ((type_plural, item_number), ('sub_units', key, sub_number), ...)
    """
    param_pointer = doc.get('document_information', {}).get('parameters', {})
    content = doc.get('content', {})

    for param_key, p in param_pointer.items():
        if p.get('is_sub_unit', False):
            continue
        type_plural = p.get('name_plural', '')
        if not type_plural or type_plural not in content:
            continue
        for item_number, item in content[type_plural].items():
            base_path = ((type_plural, item_number),)
            yield base_path, item
            yield from _iter_sub_units(item, base_path)


def _path_display(path: tuple) -> str:
    """Human-readable representation of a unit path."""
    parts = []
    for segment in path:
        if segment[0] == 'sub_units':
            parts.append(f'sub_units[{segment[1]}][{segment[2]}]')
        else:
            parts.append(f'{segment[0]}/{segment[1]}')
    return ' → '.join(parts)


# ---------------------------------------------------------------------------
# Diff computation
# ---------------------------------------------------------------------------

def compute_diff(parse_doc: dict, processed_doc: dict) -> dict:
    """
    Compare parse_doc against processed_doc; return modified/added/removed units.

    Only parse-time content fields are compared (not Stage 2/3/4 data).

    Returns:
        {
            'modified': [(path, processed_sig, parse_sig), ...],
            'added':    [(path, item), ...],   # in parse_doc but not processed_doc
            'removed':  [(path, item), ...],   # in processed_doc but not parse_doc
        }
    """
    parse_units = {path: item for path, item in iter_all_units(parse_doc)}
    processed_units = {path: item for path, item in iter_all_units(processed_doc)}

    parse_sigs = {path: _unit_signature(item) for path, item in parse_units.items()}
    processed_sigs = {path: _unit_signature(item) for path, item in processed_units.items()}

    modified = [
        (path, processed_sigs[path], parse_sigs[path])
        for path in parse_sigs
        if path in processed_sigs and parse_sigs[path] != processed_sigs[path]
    ]
    added = [(path, parse_units[path]) for path in parse_sigs if path not in processed_sigs]
    removed = [(path, processed_units[path]) for path in processed_sigs if path not in parse_sigs]

    return {'modified': modified, 'added': added, 'removed': removed}


# ---------------------------------------------------------------------------
# Manifest / source-hash check
# ---------------------------------------------------------------------------

def _find_manifest_for_parse_file(parse_path: Path) -> Tuple[Optional[ManifestManager], Optional[dict]]:
    """
    Walk parent directories (up to 4 levels) looking for a *_manifest.json that
    references parse_path. Returns (ManifestManager, manifest) or (None, None).
    """
    parse_path_abs = parse_path.resolve()
    search_dir = parse_path_abs.parent
    for _ in range(4):
        for manifest_file in search_dir.glob('*_manifest.json'):
            try:
                mgr = ManifestManager(str(manifest_file))
                manifest = mgr.load()
                for pf in mgr.get_parsed_files(manifest):
                    if Path(pf['abs_path']).resolve() == parse_path_abs:
                        return mgr, manifest
            except Exception:
                pass
        parent = search_dir.parent
        if parent == search_dir:
            break
        search_dir = parent
    return None, None


def _check_source_hash(parse_path: Path) -> None:
    """
    Advisory check: warn if the source file has changed since the last parse.
    """
    mgr, manifest = _find_manifest_for_parse_file(parse_path)
    if manifest is None:
        return  # No manifest found; skip check silently

    stored_hash = manifest.get('source_hash', '')
    if not stored_hash:
        print("WARNING: No source hash in manifest; cannot verify parse output is current.")
        return

    source_path = mgr.get_source_file_path(manifest)
    if not source_path:
        return

    current_hash = compute_source_hash(source_path)
    if current_hash and current_hash != stored_hash:
        print("WARNING: Source file has changed since last parse. "
              "Re-run the parser before running diff.")


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------

def print_report(parse_path: Path, processed_path: Path, diff: dict) -> None:
    """Print a human-readable diff report."""
    total_modified = len(diff['modified'])
    total_added = len(diff['added'])
    total_removed = len(diff['removed'])

    print(f"\nDiff: {parse_path}")
    print(f"  vs: {processed_path}")
    print()

    if total_modified == 0 and total_added == 0 and total_removed == 0:
        print("  No changes detected.")
        return

    if total_modified:
        print(f"  Modified: {total_modified} unit{'s' if total_modified != 1 else ''}")
        for path, old_sig, new_sig in diff['modified']:
            print(f"    {_path_display(path)}")
        print()

    if total_added:
        print(f"  Added: {total_added} unit{'s' if total_added != 1 else ''}")
        for path, item in diff['added']:
            print(f"    {_path_display(path)}")
        print()

    if total_removed:
        print(f"  Removed: {total_removed} unit{'s' if total_removed != 1 else ''}")
        for path, item in diff['removed']:
            print(f"    {_path_display(path)}")
        print()

    print("  Run with --archive to archive the processed file and queue it for re-run.")


# ---------------------------------------------------------------------------
# Archive logic (Task 4.3)
# ---------------------------------------------------------------------------

def _archive_processed_file(processed_path: Path) -> Path:
    """
    Move processed_path into an archive/YYYYMMDD/ subdirectory alongside it.

    Returns the destination path. Handles name collisions by appending a counter
    suffix (_1, _2, …) before the .json extension.
    """
    archive_dir = processed_path.parent / 'archive' / date.today().strftime('%Y%m%d')
    archive_dir.mkdir(parents=True, exist_ok=True)

    dest = archive_dir / processed_path.name
    if dest.exists():
        stem = processed_path.stem
        suffix = processed_path.suffix
        counter = 1
        while dest.exists():
            dest = archive_dir / f'{stem}_{counter}{suffix}'
            counter += 1

    shutil.move(str(processed_path), str(dest))
    return dest


def _cleanup_registry_for_document(processed_path: Path, config: dict) -> None:
    """
    Delete all cross_references rows for a document from the registry.
    Silently skips if the registry DB doesn't exist or document isn't registered.
    """
    db_path = _get_registry_db_path(config)
    if db_path is None:
        return

    try:
        from registry.registry import Registry
    except ImportError:
        return

    registry = Registry(db_path)
    doc = registry.get_document_by_path(str(processed_path))
    if doc is None:
        return  # Document not in corpus; nothing to do

    deleted = registry.delete_all_references_for_document(doc['id'])
    if deleted:
        print(f"  Registry: deleted {deleted} cross-reference row{'s' if deleted != 1 else ''} "
              f"for this document.")
        print("  To re-extract: python tools/extract_references.py <processed_file>")
        print("  To re-resolve: python tools/resolve_references.py")


# ---------------------------------------------------------------------------
# File path resolution
# ---------------------------------------------------------------------------

def _resolve_file_pair(input_path: Path) -> Tuple[Optional[Path], Optional[Path]]:
    """
    Given any input path, return (parse_output_path, processed_path).
    Returns (None, None) if the input doesn't match expected naming.
    """
    name = input_path.name
    if name.endswith('_parse_output.json'):
        parse_path = input_path
        stem = name[:-len('_parse_output.json')]
        processed_path = input_path.parent / f'{stem}_processed.json'
    elif name.endswith('_processed.json'):
        processed_path = input_path
        stem = name[:-len('_processed.json')]
        parse_path = input_path.parent / f'{stem}_parse_output.json'
    else:
        return None, None

    return parse_path, processed_path


# ---------------------------------------------------------------------------
# Single-file diff + archive
# ---------------------------------------------------------------------------

def process_file_pair(parse_path: Path, processed_path: Path,
                      archive: bool, config: dict) -> Tuple[bool, str]:
    """
    Diff (and optionally archive) one (parse, processed) file pair.

    Returns (changed, status) where status is one of: 'error', 'new',
    'unchanged', 'changed', 'archived'.
    """
    if not parse_path.exists():
        print(f"ERROR: Parse output file not found: {parse_path}")
        return False, 'error'

    if not processed_path.exists():
        print(f"Document not yet processed — run Stage 2/3 first: {processed_path}")
        return False, 'new'

    # Advisory source hash check
    _check_source_hash(parse_path)

    # Load documents
    try:
        with open(parse_path, encoding='utf-8') as f:
            parse_doc = json.load(f)
        with open(processed_path, encoding='utf-8') as f:
            processed_doc = json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        print(f"ERROR loading files: {e}")
        return False, 'error'

    diff = compute_diff(parse_doc, processed_doc)
    total_changes = len(diff['modified']) + len(diff['added']) + len(diff['removed'])

    if not archive:
        print_report(parse_path, processed_path, diff)
        return total_changes > 0, 'changed' if total_changes else 'unchanged'

    # --- Archive mode ---
    if total_changes == 0:
        print(f"  No changes: {processed_path.name}")
        return False, 'unchanged'

    # Print what changed
    print(f"\nChanges detected: {processed_path.name}")
    print(f"  {len(diff['modified'])} modified, "
          f"{len(diff['added'])} added, "
          f"{len(diff['removed'])} removed")

    # Clear registry entries for the full document before archiving
    _cleanup_registry_for_document(processed_path, config)

    # Archive the processed file
    archive_dest = _archive_processed_file(processed_path)
    print(f"  Archived: {archive_dest}")
    print(f"  Re-run Stage 2/3 to reprocess:")
    print(f"    python batch_process.py \"{processed_path.parent}\" --stages 2,3")

    return True, 'archived'


# ---------------------------------------------------------------------------
# Batch mode (Task 4.5)
# ---------------------------------------------------------------------------

def run_batch(directory: Path, archive: bool, config: dict) -> None:
    """
    Walk directory, find all *_parse_output.json files, and diff (or archive) each.
    """
    parse_files = sorted(directory.rglob('*_parse_output.json'))
    if not parse_files:
        print(f"No *_parse_output.json files found under: {directory}")
        return

    counts = {'unchanged': 0, 'archived': 0, 'changed': 0, 'new': 0, 'error': 0}

    for parse_path in parse_files:
        stem = parse_path.name[:-len('_parse_output.json')]
        processed_path = parse_path.parent / f'{stem}_processed.json'

        if not processed_path.exists():
            print(f"NEW: {parse_path} — no processed file; run Stage 2/3")
            counts['new'] += 1
            continue

        _, result = process_file_pair(parse_path, processed_path, archive, config)
        counts[result] = counts.get(result, 0) + 1

    if archive:
        print(f"\nBatch summary: {counts['unchanged']} unchanged, "
              f"{counts['archived']} archived for re-run, "
              f"{counts['new']} new, "
              f"{counts['error']} errors")
        if counts['archived']:
            print(f"  Run: python batch_process.py \"{directory}\" --stages 2,3")
    else:
        print(f"\nBatch summary: {counts['unchanged']} unchanged, "
              f"{counts['changed']} changed, "
              f"{counts['new']} new, "
              f"{counts['error']} errors")
        if counts['changed']:
            print("  Run with --archive to archive changed documents for re-run.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description='Detect changed units between parse output and processed document.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('path', nargs='?',
                        help='Path to *_parse_output.json, *_processed.json, or a directory')
    parser.add_argument('--archive', action='store_true',
                        help='Archive *_processed.json and clear its registry entries '
                             'so Stage 2/3 re-runs from scratch (cache handles unchanged units)')
    parser.add_argument('--all', action='store_true', dest='all_mode',
                        help='Batch mode: process all *_parse_output.json files in directory')
    args = parser.parse_args()

    config = _load_config()

    # Resolve input path
    if args.path:
        input_path = Path(args.path).resolve()
    else:
        if args.all_mode:
            output_dir = _get_output_dir(config)
            if not output_dir:
                print("ERROR: --all requires a directory argument or a configured output_dir.")
                sys.exit(1)
            input_path = Path(output_dir)
        else:
            parser.print_help()
            sys.exit(0)

    # Batch mode
    if args.all_mode:
        if not input_path.is_dir():
            print(f"ERROR: --all requires a directory: {input_path}")
            sys.exit(1)
        run_batch(input_path, args.archive, config)
        return

    # Directory without --all: treat as batch
    if input_path.is_dir():
        run_batch(input_path, args.archive, config)
        return

    # Single file
    parse_path, processed_path = _resolve_file_pair(input_path)
    if parse_path is None:
        print(f"ERROR: File must end in _parse_output.json or _processed.json: {input_path}")
        sys.exit(1)

    process_file_pair(parse_path, processed_path, args.archive, config)


if __name__ == '__main__':
    main()
