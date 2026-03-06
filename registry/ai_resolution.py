"""
AI-assisted cross-reference resolution — Task 3.3 (AI pass)

Handles cross-references that the regex-based automatic resolver could not
parse (references still with status='unresolved' after the automatic pass).
Sends each citation string plus a compact list of corpus documents to an AI
model, which identifies the matching document by index.

Only invoked as an optional second pass via the --ai flag in
tools/resolve_references.py.  It is always safe to run because:
  - Only 'unresolved' references are targeted (the automatic pass leaves those
    with no parseable citation in that state).
  - AI results are written with resolution_method='ai' for easy audit.

Task name for model assignment in config.json:
    registry.resolution.ai

Example config.json entry (uses current_engine if absent):
    "registry.resolution.ai": "claude-haiku-4-5"
"""
# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License

import sys
from pathlib import Path
from typing import Dict, List, Optional

# Allow imports from the project root
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from registry.registry import Registry
from utils.config import get_config, get_output_directory, create_client_for_task
from utils.ai_client import query_json


_TASK_NAME = 'registry.resolution.ai'
_MAX_TOKENS = 100      # response is just {"index": N}
_MAX_DOCS_IN_PROMPT = 200  # cap to keep prompt size manageable


def _compact_doc_description(doc: Dict) -> str:
    """Return a one-line description of a corpus document for the AI prompt."""
    title = doc.get('title') or Path(doc['file_path']).stem
    ptype = doc.get('parser_type', 'unknown')
    return f"{title} [{ptype}]"


def _filter_by_type_hint(ref_text: str, corpus_docs: List[Dict]) -> List[Dict]:
    """
    Apply simple keyword filtering to reduce the candidate list.

    If the citation text contains clear type indicators (e.g., 'United States
    Code'), return only documents of that parser type.  Otherwise return all
    documents so the AI can make a broader judgment.
    """
    ref_lower = ref_text.lower()

    has_usc = (
        'united states code' in ref_lower
        or 'u.s.c.' in ref_lower
        or 'u.s.c' in ref_lower
    )
    has_cfr = (
        'c.f.r.' in ref_lower
        or 'code of federal regulations' in ref_lower
    )
    has_ca = (
        'california' in ref_lower
        or ' cal.' in ref_lower
        or 'cal. ' in ref_lower
    )

    type_hints = [
        ('uslm',    has_usc),
        ('cfr',     has_cfr),
        ('ca_html', has_ca),
    ]

    filtered = []
    for ptype, matched in type_hints:
        if matched:
            filtered.extend(d for d in corpus_docs if d.get('parser_type') == ptype)

    return filtered if filtered else corpus_docs


def resolve_with_ai(
    registry: Registry,
    verbose: bool = False,
    config: Optional[Dict] = None,
) -> Dict[str, int]:
    """
    Resolve unresolved cross-references using an AI model.

    Targets references still with status='unresolved' — those the regex-based
    resolver could not parse.  For each, the AI receives the citation string
    and a numbered list of candidate corpus documents; it responds with the
    index of the matching document (0 = not in corpus).

    Args:
        registry: Open Registry instance.
        verbose:  Print one line per reference showing the outcome.
        config:   Project configuration dict (loaded from config.json if None).

    Returns:
        Stats dict: {resolved, not_in_corpus, errors, skipped}.
    """
    if config is None:
        config = get_config()

    refs = registry.get_references(resolution_status='unresolved')
    if not refs:
        return dict(resolved=0, not_in_corpus=0, errors=0, skipped=0)

    corpus_docs = registry.get_all_documents()
    if not corpus_docs:
        if verbose:
            print('  [ai] No corpus documents registered; skipping AI pass.',
                  file=sys.stderr)
        return dict(resolved=0, not_in_corpus=0, errors=0, skipped=len(refs))

    try:
        ai_client = create_client_for_task(config, _TASK_NAME)
    except Exception as e:
        print(f'  [ai] Failed to create AI client: {e}', file=sys.stderr)
        return dict(resolved=0, not_in_corpus=0, errors=len(refs), skipped=0)

    output_dir = get_output_directory(config)
    logfile = str(Path(output_dir) / 'registry_ai_resolution.log')

    stats = dict(resolved=0, not_in_corpus=0, errors=0, skipped=0)

    for ref in refs:
        ref_text = ref['ref_text']

        # Filter candidates and cap list length.
        candidates = _filter_by_type_hint(ref_text, corpus_docs)
        if len(candidates) > _MAX_DOCS_IN_PROMPT:
            candidates = candidates[:_MAX_DOCS_IN_PROMPT]

        if not candidates:
            registry.mark_not_in_corpus(
                ref['id'],
                notes='AI: no candidate documents found after type filtering.',
            )
            stats['not_in_corpus'] += 1
            if verbose:
                print(f'  [ai:not_in_corpus] {ref_text!r}')
            continue

        # Build numbered document list for the prompt.
        doc_lines = ['0: not in corpus']
        for i, doc in enumerate(candidates, 1):
            doc_lines.append(f'{i}: {_compact_doc_description(doc)}')

        prompt = (
            'You are resolving a cross-document reference in a legal document corpus.\n\n'
            f'Citation: "{ref_text}"\n\n'
            'Available corpus documents:\n'
            + '\n'.join(doc_lines)
            + '\n\nRespond with JSON: {"index": N} where N is the number of the '
            'matching document, or {"index": 0} if the citation does not refer to '
            'any document in the list.'
        )

        try:
            result = query_json(
                ai_client, [], prompt, logfile,
                max_tokens=_MAX_TOKENS,
                max_retries=3,
                expected_keys=['index'],
                config=config,
                task_name=_TASK_NAME,
            )

            # query_json returns the parsed JSON object; expect a dict {"index": N}.
            if isinstance(result, list) and result:
                result = result[0]
            if not isinstance(result, dict):
                raise ValueError(f'Unexpected response type: {type(result).__name__}')

            index = int(result.get('index', -1))

        except Exception as e:
            if verbose:
                print(f'  [ai:error] {ref_text!r}: {e}')
            stats['errors'] += 1
            continue

        if index == 0:
            registry.mark_not_in_corpus(ref['id'], notes='AI: not in corpus.')
            stats['not_in_corpus'] += 1
            if verbose:
                print(f'  [ai:not_in_corpus] {ref_text!r}')
        elif 1 <= index <= len(candidates):
            target = candidates[index - 1]
            registry.resolve_reference(
                ref_id=ref['id'],
                target_doc_id=target['id'],
                target_item_type=None,
                target_item_number=None,
                resolution_method='ai',
            )
            stats['resolved'] += 1
            if verbose:
                print(f'  [ai:resolved]      {ref_text!r}'
                      f' → {Path(target["file_path"]).name}')
        else:
            # Index out of range — treat as an error and leave unresolved.
            if verbose:
                print(f'  [ai:error] {ref_text!r}: '
                      f'index {index} out of range (max {len(candidates)})')
            stats['errors'] += 1

    return stats
