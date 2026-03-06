"""
Resolve External Cross-References — Task 3.3

Runs the automatic resolution pass against all unresolved cross-references
in the registry, matching citation strings to corpus documents.

This tool is safe to run repeatedly: already-resolved references are skipped
unless --force is given. References whose citation strings cannot be parsed
(e.g., named acts without a USC or CFR cite) are left 'unresolved' for
manual handling via the CLI tool (task 3.4).

Usage:
    python tools/resolve_references.py
    python tools/resolve_references.py --force
    python tools/resolve_references.py --verbose
    python tools/resolve_references.py --ai
    python tools/resolve_references.py --ai --verbose
    python tools/resolve_references.py --db /path/to/registry.db
"""
# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License

import argparse
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from registry.registry import Registry
from registry.resolution import resolve_unresolved
from registry.ai_resolution import resolve_with_ai


def _load_config() -> dict:
    config_path = Path(__file__).resolve().parent.parent / 'config.json'
    if config_path.exists():
        with open(config_path, encoding='utf-8') as f:
            return json.load(f)
    return {}


def _default_db_path(config: dict) -> Path:
    raw = config.get('output', {}).get('directory', '~/document_analyzer_output')
    return Path(os.path.expanduser(raw)) / 'cross_reference_registry.db'


def main():
    config = _load_config()

    parser = argparse.ArgumentParser(
        description='Resolve unresolved cross-references against corpus documents.'
    )
    parser.add_argument(
        '--force', action='store_true',
        help='Re-resolve references that already have a resolution status.'
    )
    parser.add_argument(
        '--verbose', action='store_true',
        help='Print one line per reference showing the resolution outcome.'
    )
    parser.add_argument(
        '--db', default=None,
        help='Path to registry database (default: {output_dir}/cross_reference_registry.db).'
    )
    parser.add_argument(
        '--ai', action='store_true',
        help='Run an AI-assisted resolution pass after the automatic pass, targeting '
             'references whose citation strings could not be parsed by regex. '
             'Requires a model configured for task "registry.resolution.ai" '
             '(falls back to current_engine if not set).'
    )
    args = parser.parse_args()

    db_path = Path(args.db) if args.db else _default_db_path(config)
    if not db_path.exists():
        print(f"Registry database not found: {db_path}", file=sys.stderr)
        print("Run tools/extract_references.py first to populate the registry.",
              file=sys.stderr)
        sys.exit(1)

    registry = Registry(str(db_path))

    reg_stats = registry.get_registry_stats()
    xref = reg_stats['cross_references']
    n_unresolved = xref.get('unresolved', 0)
    n_total = sum(xref.values())

    print(f"Registry: {db_path}")
    print(f"Corpus documents: {reg_stats['corpus_documents']}")
    print(f"Cross-references: {n_total} total, {n_unresolved} unresolved")
    if args.force:
        print("(--force: re-resolving all references)")
    print()

    if n_unresolved == 0 and not args.force:
        print("Nothing to resolve.")
        return

    stats = resolve_unresolved(registry, force=args.force, verbose=args.verbose)

    print("--- Resolution Results ---")
    print(f"  resolved:      {stats['resolved']}")
    print(f"  ambiguous:     {stats['ambiguous']}")
    print(f"  not_in_corpus: {stats['not_in_corpus']}")
    print(f"  no_citation:   {stats['no_citation']}"
          f"  (citation string not parseable; left unresolved for manual review)")
    if stats['skipped']:
        print(f"  skipped:       {stats['skipped']}"
              f"  (already resolved; use --force to re-resolve)")

    print()
    print("--- Registry State After Automatic Pass ---")
    mid_stats = registry.get_registry_stats()
    for status, count in sorted(mid_stats['cross_references'].items()):
        print(f"  {status:<20} {count}")

    if args.ai:
        n_still_unresolved = mid_stats['cross_references'].get('unresolved', 0)
        if n_still_unresolved == 0:
            print("\nNo unresolved references remain; skipping AI pass.")
        else:
            print(f"\n--- AI Resolution Pass ({n_still_unresolved} unresolved) ---")
            ai_stats = resolve_with_ai(registry, verbose=args.verbose)
            print(f"  resolved:      {ai_stats['resolved']}")
            print(f"  not_in_corpus: {ai_stats['not_in_corpus']}")
            print(f"  errors:        {ai_stats['errors']}")
            if ai_stats.get('skipped'):
                print(f"  skipped:       {ai_stats['skipped']}")

            print()
            print("--- Registry State After AI Pass ---")
            final_stats = registry.get_registry_stats()
            for status, count in sorted(final_stats['cross_references'].items()):
                print(f"  {status:<20} {count}")


if __name__ == '__main__':
    main()
