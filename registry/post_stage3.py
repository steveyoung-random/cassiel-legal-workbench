"""
Post-Stage-3 cross-reference hook — Task 3.5

Called automatically after Stage 3 completes for a document (from both
batch_process.py and worker/jobs.py).  Updates the cross-reference registry
with references from the newly-processed document and re-runs resolution to
catch any references that are now resolvable against the expanded corpus.

Three steps:
  1. Extract — register the document and harvest its External references.
  2. Resolve new — run the automatic resolver over this document's freshly-
     extracted references (status='unresolved').
  3. Retry not_in_corpus — re-run the automatic resolver over all registry
     references previously marked 'not_in_corpus'; the new document may now
     be their target.
  4. AI pass (optional) — attempt to resolve any remaining 'unresolved'
     references using an LLM.  Controlled by config key
     'extract_refs_ai_pass' (default: false).

All steps are idempotent and safe to re-run.  Errors are logged but do not
propagate — a registry failure must never break the Stage 3 workflow.
"""
# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License

import os
import sys
from pathlib import Path
from typing import Dict, Optional

# Allow imports from the project root regardless of invocation context.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from registry.registry import Registry
from registry.extraction import extract_from_file
from registry.resolution import resolve_unresolved, retry_not_in_corpus


def _get_registry_db_path(config: dict) -> str:
    """Return the registry DB path from config, creating the directory if needed."""
    from utils.config import get_output_directory
    output_dir = get_output_directory(config)
    db_path = os.path.join(output_dir, 'cross_reference_registry.db')
    return db_path


def extract_and_resolve(
    processed_file_path: str,
    config: Optional[dict] = None,
    verbose: bool = False,
) -> Dict[str, int]:
    """
    Post-Stage-3 hook: register a newly-processed document and re-run resolution.

    Args:
        processed_file_path: Absolute path to the *_processed.json file that
                             has just completed Stage 3.
        config:              Project config dict (loaded from config.json if None).
        verbose:             If True, print one line per resolved/unresolved ref.

    Returns:
        Stats dict with keys:
            extracted       — references extracted from this document
            resolved        — references resolved in this run (across all steps)
            still_unresolved — references that remain unresolved after all passes
            errors          — non-zero if any step encountered an error
    """
    from utils.config import get_config
    if config is None:
        config = get_config()

    run_ai_pass = bool(config.get('extract_refs_ai_pass', False))

    stats = dict(extracted=0, resolved=0, still_unresolved=0, errors=0)

    try:
        db_path = _get_registry_db_path(config)
        registry = Registry(db_path)
    except Exception as e:
        print(f"  [registry] Failed to open registry: {e}", file=sys.stderr)
        stats['errors'] = 1
        return stats

    file_path = Path(processed_file_path)

    # Step 1: Extract references from this document.
    try:
        ext_stats = extract_from_file(file_path, registry, force=False)
        if ext_stats['errors']:
            stats['errors'] += 1
            if verbose:
                print(f"  [registry] Extraction errors for {file_path.name}",
                      file=sys.stderr)
        stats['extracted'] = ext_stats['need_ref'] + ext_stats['definition']
        if verbose:
            if ext_stats['skipped']:
                print(f"  [registry] {file_path.name}: already scanned, skipped extraction")
            else:
                print(f"  [registry] {file_path.name}: extracted {stats['extracted']} references"
                      f" ({ext_stats['need_ref']} need_ref, {ext_stats['definition']} definition)")
    except Exception as e:
        print(f"  [registry] Extraction failed for {file_path.name}: {e}", file=sys.stderr)
        stats['errors'] += 1

    # Step 2: Automatic resolution of this document's new references.
    try:
        res_stats = resolve_unresolved(registry, force=False, verbose=verbose)
        stats['resolved'] += res_stats.get('resolved', 0)
        if verbose and res_stats.get('resolved', 0):
            print(f"  [registry] Auto-resolved {res_stats['resolved']} reference(s)")
    except Exception as e:
        print(f"  [registry] Auto-resolution failed: {e}", file=sys.stderr)
        stats['errors'] += 1

    # Step 3: Retry previously 'not_in_corpus' references — the new document
    # may now be their target.
    try:
        retry_stats = retry_not_in_corpus(registry, verbose=verbose)
        stats['resolved'] += retry_stats.get('resolved', 0)
        if verbose and retry_stats.get('resolved', 0):
            print(f"  [registry] Resolved {retry_stats['resolved']} previously-unmatched reference(s)")
    except Exception as e:
        print(f"  [registry] not_in_corpus retry failed: {e}", file=sys.stderr)
        stats['errors'] += 1

    # Step 4 (optional): AI pass for references with unparseable citation strings.
    if run_ai_pass:
        try:
            from registry.ai_resolution import resolve_with_ai
            ai_stats = resolve_with_ai(registry, verbose=verbose, config=config)
            stats['resolved'] += ai_stats.get('resolved', 0)
            if verbose and ai_stats.get('resolved', 0):
                print(f"  [registry] AI resolved {ai_stats['resolved']} reference(s)")
        except Exception as e:
            print(f"  [registry] AI resolution failed: {e}", file=sys.stderr)
            stats['errors'] += 1

    # Count remaining unresolved for the caller's information.
    try:
        remaining = registry.get_references(resolution_status='unresolved')
        stats['still_unresolved'] = len(remaining)
    except Exception:
        pass

    return stats
