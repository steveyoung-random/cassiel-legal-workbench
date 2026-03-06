"""
Registry CLI — Task 3.4

Command-line tool for viewing and managing the cross-reference registry.
Primary use case: manual review and resolution of cross-references that the
automated tools (extract_references.py, resolve_references.py) could not
resolve unambiguously.

Usage:
    python tools/registry_cli.py stats
    python tools/registry_cli.py docs [--parser-type TYPE]
    python tools/registry_cli.py refs [--status STATUS] [--doc DOC_ID]
                                      [--context {need_ref,definition}] [--limit N]
    python tools/registry_cli.py show <ref_id>
    python tools/registry_cli.py resolve <ref_id> <target_doc_id> [--notes TEXT]
    python tools/registry_cli.py not-in-corpus <ref_id> [--notes TEXT]
    python tools/registry_cli.py reset <ref_id>
    python tools/registry_cli.py --db /path/to/registry.db <subcommand> ...

Typical workflow:
    1. python tools/extract_references.py --all
    2. python tools/resolve_references.py
    3. python tools/registry_cli.py stats
    4. python tools/registry_cli.py refs --status ambiguous
    5. python tools/registry_cli.py show <ref_id>
    6. python tools/registry_cli.py docs
    7. python tools/registry_cli.py resolve <ref_id> <doc_id>
"""
# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License

import argparse
import json
import os
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from registry.registry import Registry


# ---------------------------------------------------------------------------
# Config helpers (same pattern as other tools)
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
# Formatting helpers
# ---------------------------------------------------------------------------

def _trunc(s, width: int) -> str:
    """Truncate to width, adding ellipsis if truncated. None → '-'."""
    if s is None:
        return '-'
    s = str(s)
    if len(s) <= width:
        return s
    return s[:width - 3] + '...'


def _doc_label(doc: dict) -> str:
    """Short human-readable label for a corpus document."""
    if not doc:
        return '-'
    return f"{Path(doc['file_path']).name} (id={doc['id']})"


def _item_str(item_type, item_number) -> str:
    parts = [p for p in (item_type, item_number) if p]
    return ' '.join(parts) if parts else '-'


# ---------------------------------------------------------------------------
# Subcommand: stats
# ---------------------------------------------------------------------------

def cmd_stats(registry: Registry, args):
    stats = registry.get_registry_stats()
    xref = stats['cross_references']
    total_refs = sum(xref.values())

    print(f"Corpus documents:   {stats['corpus_documents']}")
    print(f"Document splits:    {stats['document_splits']}")
    print(f"Cross-references:   {total_refs} total")

    if xref:
        # Show in a fixed order; any extra statuses shown after
        ordered = ['resolved', 'ambiguous', 'not_in_corpus', 'unresolved']
        shown = set()
        for status in ordered:
            count = xref.get(status, 0)
            bar = '#' * min(count, 40)
            print(f"  {status:<20} {count:>6}  {bar}")
            shown.add(status)
        for status, count in sorted(xref.items()):
            if status not in shown:
                print(f"  {status:<20} {count:>6}")
    else:
        print("  (no references)")


# ---------------------------------------------------------------------------
# Subcommand: docs
# ---------------------------------------------------------------------------

def cmd_docs(registry: Registry, args):
    docs = registry.get_all_documents()
    if getattr(args, 'parser_type', None):
        docs = [d for d in docs if d.get('parser_type') == args.parser_type]

    if not docs:
        print("No documents found.")
        return

    print(f"{'ID':>5}  {'Parser':<8}  {'Stage':>5}  Title")
    print(f"{'─' * 5}  {'─' * 8}  {'─' * 5}  {'─' * 60}")
    for doc in docs:
        title = doc.get('title') or Path(doc['file_path']).stem
        print(
            f"{doc['id']:>5}  "
            f"{doc.get('parser_type', '?'):<8}  "
            f"{doc.get('stage_reached', 1):>5}  "
            f"{_trunc(title, 60)}"
        )
    print(f"\n{len(docs)} document(s)")


# ---------------------------------------------------------------------------
# Subcommand: refs
# ---------------------------------------------------------------------------

def cmd_refs(registry: Registry, args):
    refs = registry.get_references(
        source_doc_id=getattr(args, 'doc', None),
        resolution_status=getattr(args, 'status', None),
    )

    context_filter = getattr(args, 'context', None)
    if context_filter:
        refs = [r for r in refs if r.get('ref_context') == context_filter]

    limit = getattr(args, 'limit', 50)
    total = len(refs)
    if limit and limit > 0:
        refs = refs[:limit]
    truncated = total > len(refs)

    if not refs:
        print("No references found.")
        return

    # Cache source doc names to avoid repeated DB calls
    doc_cache: dict = {}

    def _src_name(doc_id):
        if doc_id is None:
            return '-'
        if doc_id not in doc_cache:
            doc = registry.get_document_by_id(doc_id)
            doc_cache[doc_id] = Path(doc['file_path']).stem if doc else f'id={doc_id}'
        return doc_cache[doc_id]

    print(
        f"{'ID':>6}  "
        f"{'Status':<14}  "
        f"{'Ctx':<10}  "
        f"{'Source Document':<30}  "
        f"{'Item':<14}  "
        f"Citation"
    )
    print(
        f"{'─' * 6}  "
        f"{'─' * 14}  "
        f"{'─' * 10}  "
        f"{'─' * 30}  "
        f"{'─' * 14}  "
        f"{'─' * 40}"
    )

    for ref in refs:
        src = _trunc(_src_name(ref.get('source_doc_id')), 30)
        item = _trunc(
            _item_str(ref.get('source_item_type'), ref.get('source_item_number')), 14
        )
        print(
            f"{ref['id']:>6}  "
            f"{ref.get('resolution_status', 'unresolved'):<14}  "
            f"{ref.get('ref_context', ''):<10}  "
            f"{src:<30}  "
            f"{item:<14}  "
            f"{_trunc(ref.get('ref_text', ''), 40)}"
        )

    print()
    if truncated:
        print(f"Showing {len(refs)} of {total}. Use --limit to adjust (0 = no limit).")
    else:
        print(f"{total} reference(s)")


# ---------------------------------------------------------------------------
# Subcommand: show
# ---------------------------------------------------------------------------

def cmd_show(registry: Registry, args):
    ref = registry.get_reference_by_id(args.ref_id)
    if not ref:
        print(f"Reference {args.ref_id} not found.", file=sys.stderr)
        sys.exit(1)

    src_doc = registry.get_document_by_id(ref.get('source_doc_id'))
    tgt_doc = (
        registry.get_document_by_id(ref['target_doc_id'])
        if ref.get('target_doc_id') else None
    )

    def _row(label, value):
        print(f"  {label:<22} {value if value is not None else '-'}")

    print(f"Reference #{ref['id']}")
    _row("Status:", ref.get('resolution_status', 'unresolved'))
    _row("Citation:", ref.get('ref_text'))
    _row("Context:", ref.get('ref_context'))
    term = ref.get('term')
    _row("Term:", term)
    print()
    _row("Source document:", _doc_label(src_doc))
    _row("Source item:", _item_str(ref.get('source_item_type'), ref.get('source_item_number')))
    print()
    _row("Target document:", _doc_label(tgt_doc))
    tgt_item = _item_str(ref.get('target_item_type'), ref.get('target_item_number'))
    _row("Target item:", tgt_item if tgt_item != '-' else None)
    _row("Resolution method:", ref.get('resolution_method'))
    _row("Resolved at:", ref.get('resolved_at'))
    _row("Notes:", ref.get('notes'))


# ---------------------------------------------------------------------------
# Subcommand: resolve
# ---------------------------------------------------------------------------

def cmd_resolve(registry: Registry, args):
    ref = registry.get_reference_by_id(args.ref_id)
    if not ref:
        print(f"Reference {args.ref_id} not found.", file=sys.stderr)
        sys.exit(1)

    target_doc = registry.get_document_by_id(args.target_doc_id)
    if not target_doc:
        print(f"Corpus document {args.target_doc_id} not found.", file=sys.stderr)
        print(
            "Run 'python tools/registry_cli.py docs' to list available documents.",
            file=sys.stderr,
        )
        sys.exit(1)

    registry.resolve_reference(
        ref_id=args.ref_id,
        target_doc_id=args.target_doc_id,
        target_item_type=None,
        target_item_number=None,
        resolution_method='manual',
        notes=getattr(args, 'notes', None),
    )

    print(f"Resolved reference #{args.ref_id}:")
    print(f"  Citation:  {ref.get('ref_text')}")
    print(f"  Target:    {_doc_label(target_doc)}")
    if getattr(args, 'notes', None):
        print(f"  Notes:     {args.notes}")


# ---------------------------------------------------------------------------
# Subcommand: not-in-corpus
# ---------------------------------------------------------------------------

def cmd_not_in_corpus(registry: Registry, args):
    ref = registry.get_reference_by_id(args.ref_id)
    if not ref:
        print(f"Reference {args.ref_id} not found.", file=sys.stderr)
        sys.exit(1)

    registry.mark_not_in_corpus(args.ref_id, notes=getattr(args, 'notes', None))

    print(f"Marked reference #{args.ref_id} as not_in_corpus:")
    print(f"  Citation:  {ref.get('ref_text')}")
    if getattr(args, 'notes', None):
        print(f"  Notes:     {args.notes}")


# ---------------------------------------------------------------------------
# Subcommand: reset
# ---------------------------------------------------------------------------

def cmd_reset(registry: Registry, args):
    ref = registry.get_reference_by_id(args.ref_id)
    if not ref:
        print(f"Reference {args.ref_id} not found.", file=sys.stderr)
        sys.exit(1)

    old_status = ref.get('resolution_status', 'unresolved')
    registry.reset_reference(args.ref_id)

    print(f"Reset reference #{args.ref_id} to 'unresolved':")
    print(f"  Citation:        {ref.get('ref_text')}")
    print(f"  Previous status: {old_status}")
    print("Run 'python tools/resolve_references.py' to re-run automatic resolution.")


# ---------------------------------------------------------------------------
# Subcommand: report
# ---------------------------------------------------------------------------

def cmd_report(registry: Registry, args):
    """
    Print a comprehensive resolution report for user review.

    Sections:
      1. Summary — counts by status, resolved-method breakdown
      2. Ambiguous — full listing; these need user action (resolve or not-in-corpus)
      3. Unresolved — unique unparseable citations; suggest AI pass
      4. Not-in-corpus — unique citations whose target isn't in the corpus
      5. Action guide — commands to take next steps
    """
    DIVIDER = '=' * 72
    THIN    = '-' * 72

    # ------------------------------------------------------------------
    # Load all refs and build doc lookup once.
    # ------------------------------------------------------------------
    all_refs = registry.get_references()
    docs = {d['id']: d for d in registry.get_all_documents()}

    def _src_label(ref):
        doc = docs.get(ref.get('source_doc_id'))
        stem = Path(doc['file_path']).stem if doc else f"doc_id={ref.get('source_doc_id')}"
        item = _item_str(ref.get('source_item_type'), ref.get('source_item_number'))
        return stem, item

    by_status = {}
    for ref in all_refs:
        s = ref.get('resolution_status', 'unresolved')
        by_status.setdefault(s, []).append(ref)

    resolved_refs   = by_status.get('resolved', [])
    ambiguous_refs  = by_status.get('ambiguous', [])
    not_in_corpus   = by_status.get('not_in_corpus', [])
    unresolved_refs = by_status.get('unresolved', [])
    total = len(all_refs)

    def _pct(n):
        return f'{n/total*100:.1f}%' if total else '0%'

    # ------------------------------------------------------------------
    # Header
    # ------------------------------------------------------------------
    print(DIVIDER)
    print('CROSS-REFERENCE RESOLUTION REPORT')
    print(f'Generated: {datetime.now().strftime("%Y-%m-%d %H:%M")}')
    print(DIVIDER)
    print()

    # ------------------------------------------------------------------
    # 1. Summary
    # ------------------------------------------------------------------
    print('SUMMARY')
    print(THIN)
    print(f'  Corpus documents:  {len(docs)}')
    print(f'  Total references:  {total}')
    print()

    for label, refs, note in [
        ('Resolved',      resolved_refs,   ''),
        ('Ambiguous',     ambiguous_refs,  '  ** needs user review **'),
        ('Not in corpus', not_in_corpus,   ''),
        ('Unresolved',    unresolved_refs, '  (run AI pass or review manually)'),
    ]:
        count = len(refs)
        bar = '#' * min(count, 30)
        print(f'  {label:<15} {count:>6}  ({_pct(count)})  {bar}{note}')

    if resolved_refs:
        method_counts = Counter(r.get('resolution_method') or 'unknown' for r in resolved_refs)
        parts = ', '.join(f'{m}: {c}' for m, c in sorted(method_counts.items()))
        print(f'\n  Resolution methods: {parts}')

    print()

    # ------------------------------------------------------------------
    # 2. Ambiguous references — full listing
    # ------------------------------------------------------------------
    print('AMBIGUOUS REFERENCES')
    print(THIN)
    if not ambiguous_refs:
        print('  None — all references that could be parsed have been resolved.')
    else:
        print(f'  {len(ambiguous_refs)} reference(s) matched multiple corpus documents.')
        print('  Use  python tools/registry_cli.py show <ref_id>  to inspect,')
        print('  then python tools/registry_cli.py resolve <ref_id> <target_doc_id>')
        print('   or  python tools/registry_cli.py not-in-corpus <ref_id>')
        print()
        for ref in ambiguous_refs:
            stem, item = _src_label(ref)
            notes = ref.get('notes') or ''
            print(f"  #{ref['id']}  {_trunc(ref.get('ref_text', ''), 55)}")
            print(f"       from: {_trunc(stem, 40)} — {item}")
            if notes:
                print(f"       {_trunc(notes, 60)}")
            print()

    # ------------------------------------------------------------------
    # 3. Unresolved — unique citation strings with count
    # ------------------------------------------------------------------
    print('UNRESOLVED (UNPARSEABLE CITATIONS)')
    print(THIN)
    if not unresolved_refs:
        print('  None.')
    else:
        # Show unique citation texts with how many refs share each
        citation_counts = Counter(r.get('ref_text', '') for r in unresolved_refs)
        print(f'  {len(unresolved_refs)} reference(s) with {len(citation_counts)} unique citation string(s).')
        print('  These citations could not be parsed by the regex resolver.')
        print('  Next step: python tools/resolve_references.py --ai')
        print('   or:       python tools/registry_cli.py refs --status unresolved')
        print()
        limit = getattr(args, 'unresolved_limit', 10)
        shown = 0
        for citation, count in citation_counts.most_common():
            suffix = f'  (×{count})' if count > 1 else ''
            print(f"    {_trunc(citation, 62)}{suffix}")
            shown += 1
            if limit and shown >= limit:
                remaining = len(citation_counts) - shown
                if remaining:
                    print(f"    ... and {remaining} more. Use --unresolved-limit 0 to show all.")
                break

    print()

    # ------------------------------------------------------------------
    # 4. Not-in-corpus — unique citation strings
    # ------------------------------------------------------------------
    print('NOT IN CORPUS')
    print(THIN)
    if not not_in_corpus:
        print('  None — all parsed citations matched a corpus document.')
    else:
        citation_counts = Counter(r.get('ref_text', '') for r in not_in_corpus)
        print(f'  {len(not_in_corpus)} reference(s) citing {len(citation_counts)} unique document(s) not in corpus.')
        print('  These may be documents that have not been parsed yet.')
        print('  After parsing them, run: python tools/resolve_references.py')
        print()
        limit = getattr(args, 'nic_limit', 20)
        shown = 0
        for citation, count in citation_counts.most_common():
            suffix = f'  (×{count})' if count > 1 else ''
            print(f"    {_trunc(citation, 62)}{suffix}")
            shown += 1
            if limit and shown >= limit:
                remaining = len(citation_counts) - shown
                if remaining:
                    print(f"    ... and {remaining} more. Use --nic-limit 0 to show all.")
                break

    print()

    # ------------------------------------------------------------------
    # 5. Action guide
    # ------------------------------------------------------------------
    print('NEXT STEPS')
    print(THIN)
    if ambiguous_refs:
        print('  Resolve ambiguous references:')
        print('    python tools/registry_cli.py refs --status ambiguous')
        print('    python tools/registry_cli.py show <ref_id>')
        print('    python tools/registry_cli.py resolve <ref_id> <target_doc_id>')
        print('    python tools/registry_cli.py docs   (to list target doc IDs)')
        print()
    if unresolved_refs:
        print('  Resolve unparseable citations with AI:')
        print('    python tools/resolve_references.py --ai')
        print()
    if not_in_corpus:
        print('  Add missing documents to corpus, then re-run resolution:')
        print('    python tools/extract_references.py --all')
        print('    python tools/resolve_references.py')
        print()
    if not ambiguous_refs and not unresolved_refs and not not_in_corpus:
        print('  All references resolved — ready for Stage 4 cross-reference integration.')
    print(DIVIDER)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def cmd_splits(registry: Registry, args):
    """List all registered parent-child split relationships."""
    all_docs = registry.get_all_documents()
    doc_map = {d['id']: d for d in all_docs}

    splits_found = False
    for doc in all_docs:
        children = registry.get_children(doc['id'])
        if not children:
            continue
        splits_found = True
        parent_name = Path(doc['file_path']).name
        for child in children:
            child_name = Path(child['file_path']).name
            split_type = child.get('split_type', '?')
            print(f"  [{split_type}] {parent_name} (id={doc['id']}) → {child_name} (id={child['id']})")

    if not splits_found:
        print("No split relationships registered.")
        print("Run 'python tools/link_splits.py' to detect and register them.")


def main():
    config = _load_config()

    parser = argparse.ArgumentParser(
        description='View and manage the cross-reference registry.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
subcommands:
  report                            Comprehensive resolution report for user review
  stats                             Show registry statistics
  docs                              List corpus documents
  refs                              List cross-references
  show <ref_id>                     Show full detail for one reference
  resolve <ref_id> <target_doc_id>  Manually resolve a reference to a corpus document
  not-in-corpus <ref_id>            Mark a reference as not in the corpus
  reset <ref_id>                    Reset a reference to 'unresolved' for re-resolution
  splits                            List parent-child split relationships
""",
    )
    parser.add_argument(
        '--db', default=None,
        help='Path to registry database (default: {output_dir}/cross_reference_registry.db).',
    )

    subparsers = parser.add_subparsers(dest='subcommand')

    # report
    p_report = subparsers.add_parser(
        'report',
        help='Print a comprehensive resolution report for user review.',
    )
    p_report.add_argument(
        '--unresolved-limit', type=int, default=10, metavar='N',
        help='Max unparseable citation strings to list (default: 10; 0 = all).',
    )
    p_report.add_argument(
        '--nic-limit', type=int, default=20, metavar='N',
        help='Max not-in-corpus citation strings to list (default: 20; 0 = all).',
    )

    # stats
    subparsers.add_parser('stats', help='Show registry statistics.')

    # docs
    p_docs = subparsers.add_parser('docs', help='List corpus documents.')
    p_docs.add_argument(
        '--parser-type',
        help='Filter by parser type (uslm, cfr, ca_html, formex).',
    )

    # refs
    p_refs = subparsers.add_parser('refs', help='List cross-references.')
    p_refs.add_argument(
        '--status',
        help='Filter by resolution status (unresolved, ambiguous, resolved, not_in_corpus).',
    )
    p_refs.add_argument(
        '--doc', type=int, metavar='DOC_ID',
        help='Filter to references from this source document ID.',
    )
    p_refs.add_argument(
        '--context', choices=['need_ref', 'definition'],
        help='Filter by reference context.',
    )
    p_refs.add_argument(
        '--limit', type=int, default=50,
        help='Maximum references to show (default: 50; 0 = no limit).',
    )

    # show
    p_show = subparsers.add_parser('show', help='Show full detail for one reference.')
    p_show.add_argument('ref_id', type=int, help='Reference ID.')

    # resolve
    p_resolve = subparsers.add_parser(
        'resolve',
        help='Manually resolve a reference to a corpus document.',
    )
    p_resolve.add_argument('ref_id', type=int, help='Reference ID.')
    p_resolve.add_argument('target_doc_id', type=int, help='Target corpus document ID.')
    p_resolve.add_argument('--notes', help='Optional explanation of the resolution.')

    # not-in-corpus
    p_nic = subparsers.add_parser(
        'not-in-corpus',
        help='Mark a reference as not in the corpus.',
    )
    p_nic.add_argument('ref_id', type=int, help='Reference ID.')
    p_nic.add_argument('--notes', help='Optional explanation.')

    # reset
    p_reset = subparsers.add_parser(
        'reset',
        help="Reset a reference to 'unresolved' for re-resolution.",
    )
    p_reset.add_argument('ref_id', type=int, help='Reference ID.')

    # splits
    subparsers.add_parser('splits', help='List registered parent-child split relationships.')

    args = parser.parse_args()

    if not args.subcommand:
        parser.print_help()
        sys.exit(1)

    db_path = Path(args.db) if args.db else _default_db_path(config)
    if not db_path.exists():
        print(f"Registry database not found: {db_path}", file=sys.stderr)
        print(
            "Run tools/extract_references.py first to populate the registry.",
            file=sys.stderr,
        )
        sys.exit(1)

    registry = Registry(str(db_path))

    dispatch = {
        'report': cmd_report,
        'stats': cmd_stats,
        'docs': cmd_docs,
        'refs': cmd_refs,
        'show': cmd_show,
        'resolve': cmd_resolve,
        'not-in-corpus': cmd_not_in_corpus,
        'reset': cmd_reset,
        'splits': cmd_splits,
    }
    dispatch[args.subcommand](registry, args)


if __name__ == '__main__':
    main()
