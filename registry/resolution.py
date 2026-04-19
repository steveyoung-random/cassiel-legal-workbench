"""
Cross-reference resolution logic — Task 3.3

Matches raw citation strings from cross_references against corpus documents,
using regex-based citation parsing and document metadata stored in the registry.

Resolution strategy (no AI required, no file I/O):
  1. Parse the citation string with regex to extract title/section identifiers.
  2. For each corpus document, check its stored metadata:
       org_keys      — organizational hierarchy (title, chapter, part, etc.)
       content_items — authoritative list of item numbers actually in that file
  3. Apply boundary-aware prefix matching for section/part designators.
  4. Record the result: resolved, ambiguous, not_in_corpus, or (unchanged) unresolved.

Boundary-aware prefix rule:
  candidate is a valid prefix of cited if, at position len(candidate) in cited,
  the next character is either a separator (. - _) or a character class change
  (digit→letter or letter→digit). Same-class continuation is NOT a valid prefix:
  "23" is not a valid prefix of "234" because "4" continues the digit class of "3".

Resolution statuses written back to the registry:
  resolved      — exactly one corpus document identified
  ambiguous     — two or more documents matched equally and could not be narrowed
  not_in_corpus — no corpus document matched the citation
  (unresolved)  — citation string unparseable; left for AI pass or manual review
"""
# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License

import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from registry.registry import Registry


# ---------------------------------------------------------------------------
# Citation parsing
# ---------------------------------------------------------------------------

# USC: "section 1813 of title 12, United States Code" or
#      "section 1813 of title 12"
_USC_SECTION_OF_TITLE = re.compile(
    r'section\s+(\d+\w*)\s+of\s+title\s+(\d+)'
    r'(?:[,\s]+(?:of\s+)?(?:the\s+)?United\s+States\s+Code)?',
    re.IGNORECASE,
)

# USC: "12 U.S.C. § 1813" or "12 U.S.C. 1813"
_USC_ABBREV_WITH_SECTION = re.compile(
    r'(\d+)\s+U\.S\.C\.[\s§]*(\d+\w*)',
    re.IGNORECASE,
)

# USC: "12 U.S.C." (title only, no section)
_USC_ABBREV_TITLE_ONLY = re.compile(
    r'(\d+)\s+U\.S\.C\b',
    re.IGNORECASE,
)

# USC: "title 12, United States Code"
_USC_TITLE_OF_CODE = re.compile(
    r'title\s+(\d+)[,\s]+(?:of\s+)?(?:the\s+)?United\s+States\s+Code',
    re.IGNORECASE,
)

# CFR: "14 C.F.R. § 39.5" (section-level — more specific than part; try first)
_CFR_WITH_SECTION = re.compile(
    r'(\d+)\s+C\.F\.R\.[,\s]*§\s*(\d+\.\d[\d.]*\w*)',
    re.IGNORECASE,
)

# CFR: "14 C.F.R. Part 39" or "14 C.F.R. § 39" (part-level)
_CFR_WITH_PART = re.compile(
    r'(\d+)\s+C\.F\.R\.[,\s]*(?:part|§)\s*(\d+)',
    re.IGNORECASE,
)

# CFR: "14 C.F.R." (title only)
_CFR_TITLE_ONLY = re.compile(
    r'(\d+)\s+C\.F\.R\b',
    re.IGNORECASE,
)

# Bare "Part N" — weaker signal, used only as fallback
_BARE_PART = re.compile(r'\bpart\s+(\d+)\b', re.IGNORECASE)


def parse_citation(ref_text: str) -> Dict[str, Any]:
    """
    Extract structured citation info from a raw citation string.

    Tries patterns from most specific to least specific; returns on first match.
    Keys in the returned dict:
        usc_title:   int — U.S. Code title number
        usc_section: str — U.S. Code section number (string to preserve suffixes)
        cfr_title:   int — C.F.R. title number
        cfr_part:    int — C.F.R. part number
        part_only:   int — bare part number without title context (low confidence)

    Returns an empty dict if no structured citation information can be extracted.
    """
    # USC section + title
    m = _USC_SECTION_OF_TITLE.search(ref_text)
    if m:
        return {'usc_title': int(m.group(2)), 'usc_section': m.group(1)}

    # USC abbreviated with section: "12 U.S.C. 1813"
    m = _USC_ABBREV_WITH_SECTION.search(ref_text)
    if m:
        return {'usc_title': int(m.group(1)), 'usc_section': m.group(2)}

    # USC title only (abbreviated): "12 U.S.C."
    m = _USC_ABBREV_TITLE_ONLY.search(ref_text)
    if m:
        return {'usc_title': int(m.group(1))}

    # USC title only (spelled out): "title 12, United States Code"
    m = _USC_TITLE_OF_CODE.search(ref_text)
    if m:
        return {'usc_title': int(m.group(1))}

    # CFR with section designator: "14 C.F.R. § 39.5" (try before part-level pattern)
    m = _CFR_WITH_SECTION.search(ref_text)
    if m:
        sec = m.group(2)
        part = int(sec.split('.')[0])
        return {'cfr_title': int(m.group(1)), 'cfr_part': part, 'cfr_section': sec}

    # CFR with part: "14 C.F.R. Part 39"
    m = _CFR_WITH_PART.search(ref_text)
    if m:
        return {'cfr_title': int(m.group(1)), 'cfr_part': int(m.group(2))}

    # CFR title only: "14 C.F.R."
    m = _CFR_TITLE_ONLY.search(ref_text)
    if m:
        return {'cfr_title': int(m.group(1))}

    # Bare part number — low confidence, only used when nothing else matches
    m = _BARE_PART.search(ref_text)
    if m:
        return {'part_only': int(m.group(1))}

    return {}


# ---------------------------------------------------------------------------
# Boundary-aware prefix matching
# ---------------------------------------------------------------------------

def _is_valid_prefix(cited: str, candidate: str) -> bool:
    """
    Check whether candidate is a valid prefix of cited.

    A prefix is valid if, at position len(candidate) in cited, the next
    character is either:
      - a separator: '.', '-', or '_', or
      - a different character class from the last character of candidate
        (digit→letter or letter→digit).

    Same-class continuation (e.g. digit→digit) is NOT valid: "23" should
    not match section "234" because "4" continues the same digit class as "3".

    Exact match (len(cited) == len(candidate)) is always valid.
    Empty candidate is never valid.
    """
    if not candidate:
        return False
    if not cited.startswith(candidate):
        return False
    if len(cited) == len(candidate):
        return True  # exact match
    next_char = cited[len(candidate)]
    prev_char = candidate[-1]
    if next_char in '.-_':
        return True
    return prev_char.isdigit() != next_char.isdigit()


def _best_section_match(cited: str, candidates: List[str]) -> Optional[str]:
    """
    Find the best matching candidate for cited.

    Returns the longest candidate that is a valid prefix of cited,
    or None if no candidate matches.  Longer matches are more specific:
    "2000a" beats "2000" as a match for "2000a-4".
    """
    best: Optional[str] = None
    best_len = -1
    for candidate in candidates:
        if _is_valid_prefix(cited, candidate) and len(candidate) > best_len:
            best = candidate
            best_len = len(candidate)
    return best


# ---------------------------------------------------------------------------
# Document metadata access
# ---------------------------------------------------------------------------

def _get_doc_metadata(doc: Dict) -> Dict:
    """Parse the metadata JSON column from a corpus_documents row."""
    raw = doc.get('metadata')
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except Exception:
        return {}


# ---------------------------------------------------------------------------
# Document scoring
# ---------------------------------------------------------------------------

def _score_doc(citation: Dict, doc: Dict) -> Tuple[int, int]:
    """
    Score how well a corpus document matches a parsed citation.

    Returns (main_score, tiebreak_length):
      main_score:
        0  — no match
        1  — bare part hint: part number found in org_keys (low confidence,
             no title context)
        2  — title match, parser type mismatch
        3  — title + parser type match, or: title match + part org match
             with type mismatch
        4  — title + parser type + USC section prefix match (corpus item is
             a broader section that contains the cited section)
        5  — title + parser type + section/part exact match
      tiebreak_length:
        length of the matched section or part designator string

    Matching approach by citation type:

      USC (usc_title / usc_section):
        - Title identified via org_keys["title"]
        - Section (if present) matched against content_items["section"] using
          boundary-aware prefix matching.  A corpus item "1813" is an exact
          match for citation "1813"; a corpus item "2000a" is a valid prefix
          of "2000a-4".  Prefix matching handles the case where only the
          parent section is in the corpus.

      CFR (cfr_title / cfr_part):
        - Title identified via org_keys["title"]
        - Part (if present) matched against org_keys["part"].
          A part is an organizational unit; org_keys is the right source.
          No prefix matching needed: the part is either in the org structure
          of this file or it is not.

    Adding a new parser type: add (a) citation-format regexes to parse_citation,
    (b) a branch here mapping the new citation keys to the appropriate org_keys
    and/or content_items lookups, (c) documentation in citation_patterns.md,
    and (d) keyword hints to registry/ai_resolution.py:_filter_by_type_hint.

    Note: if a document has no metadata (metadata IS NULL), org_keys and
    content_items will both be empty and this function returns 0.  Run
    tools/extract_references.py to populate metadata before resolving.
    """
    meta = _get_doc_metadata(doc)
    org_keys = meta.get('org_keys', {})
    content = meta.get('content_items', {})

    if 'usc_title' in citation:
        if str(citation['usc_title']) not in org_keys.get('title', []):
            return 0, 0
        type_match = (doc['parser_type'] == 'uslm')
        base = 3 if type_match else 2

        if 'usc_section' in citation and content:
            sections = content.get('section', [])
            match = _best_section_match(citation['usc_section'], sections)
            if match is not None:
                exact = (match == citation['usc_section'])
                if type_match:
                    return (5 if exact else 4), len(match)
                else:
                    return (3 if exact else 2), len(match)
        return base, 0

    elif 'cfr_title' in citation:
        if str(citation['cfr_title']) not in org_keys.get('title', []):
            return 0, 0
        type_match = (doc['parser_type'] == 'cfr')
        base = 3 if type_match else 2

        if 'cfr_part' in citation:
            part_str = str(citation['cfr_part'])
            if part_str in org_keys.get('part', []):
                # Part is an org unit; look it up in the org structure directly.
                return (5 if type_match else 3), len(part_str)
        return base, 0

    elif 'part_only' in citation:
        # Low-confidence bare "Part N" with no title context.
        # Check the org structure for any document type.
        part_str = str(citation['part_only'])
        if part_str in org_keys.get('part', []):
            return 1, len(part_str)
        return 0, 0

    return 0, 0


def _best_candidates(
    citation: Dict,
    corpus_docs: List[Dict],
) -> Tuple[int, List[Dict]]:
    """
    Find corpus documents that best match the citation.

    Returns (best_score, list_of_matching_docs).

    Only considers docs with main_score >= 2 (at least a title match).
    Among those at the best main_score, keeps only those with the longest
    tiebreak (most specific section/part match).
    """
    scored = []
    for doc in corpus_docs:
        main_score, tiebreak = _score_doc(citation, doc)
        if main_score >= 2:
            scored.append((main_score, tiebreak, doc))

    if not scored:
        return 0, []

    best_main = max(s for s, _, _ in scored)
    at_best = [(s, t, d) for s, t, d in scored if s == best_main]
    best_tb = max(t for _, t, _ in at_best)
    candidates = [d for _, t, d in at_best if t == best_tb]
    return best_main, candidates


def _deduplicate_by_stage(docs: List[Dict]) -> List[Dict]:
    """
    When both parse_output and processed versions of the same document are
    registered, keep only the highest-stage copy.
    Groups by filename stem with known stage suffixes stripped.
    """
    _STAGE_SUFFIXES = ('_processed', '_stage2_output', '_parse_output')

    def _base(fp: str) -> str:
        stem = Path(fp).stem
        for sfx in _STAGE_SUFFIXES:
            if stem.endswith(sfx):
                return stem[: -len(sfx)]
        return stem

    best: Dict[str, Dict] = {}
    for doc in docs:
        key = _base(doc['file_path'])
        if key not in best or (doc.get('stage_reached') or 0) > (best[key].get('stage_reached') or 0):
            best[key] = doc
    return list(best.values())


# ---------------------------------------------------------------------------
# Resolution entry points
# ---------------------------------------------------------------------------

def retry_not_in_corpus(
    registry: Registry,
    verbose: bool = False,
) -> Dict[str, int]:
    """
    Re-run automatic resolution over references previously marked 'not_in_corpus'.

    Called when a new document is added to the corpus — a reference that had no
    match before may now resolve against the new document.

    Only processes references with status='not_in_corpus'. Unparseable citations
    (no_citation) remain unchanged; those require the AI pass.

    Returns:
        Stats dict: {resolved, ambiguous, not_in_corpus, no_citation}.
    """
    refs = registry.get_references(resolution_status='not_in_corpus')
    if not refs:
        return dict(resolved=0, ambiguous=0, not_in_corpus=0, no_citation=0)

    corpus_docs = _deduplicate_by_stage(registry.get_all_documents())
    stats = dict(resolved=0, ambiguous=0, not_in_corpus=0, no_citation=0)

    for ref in refs:
        ref_text = ref['ref_text']
        citation = parse_citation(ref_text)

        if not citation:
            stats['no_citation'] += 1
            if verbose:
                print(f"  [no_citation]    {ref_text!r}")
            continue

        best_score, candidates = _best_candidates(citation, corpus_docs)

        if best_score == 0:
            stats['not_in_corpus'] += 1
            if verbose:
                print(f"  [still_not_in_corpus]  {ref_text!r}")
            continue

        if len(candidates) == 1:
            target = candidates[0]
            target_item_type = None
            target_item_number = None
            if 'usc_section' in citation:
                target_item_type = 'section'
                target_item_number = citation['usc_section']
            elif 'cfr_section' in citation:
                target_item_type = 'section'
                target_item_number = citation['cfr_section']
            registry.resolve_reference(
                ref_id=ref['id'],
                target_doc_id=target['id'],
                target_item_type=target_item_type,
                target_item_number=target_item_number,
                resolution_method='automatic',
                notes=f'Matched by metadata scoring (score {best_score}) after corpus update.',
            )
            stats['resolved'] += 1
            if verbose:
                print(f"  [resolved]       {ref_text!r}"
                      f" → {Path(target['file_path']).name}")
        else:
            names = ', '.join(Path(d['file_path']).name for d in candidates[:3])
            if len(candidates) > 3:
                names += f' (+ {len(candidates) - 3} more)'
            registry.mark_ambiguous(
                ref['id'],
                notes=f'Matched {len(candidates)} corpus documents after corpus update: {names}',
            )
            stats['ambiguous'] += 1
            if verbose:
                print(f"  [ambiguous]      {ref_text!r}"
                      f" → {len(candidates)} docs")

    return stats


def resolve_unresolved(
    registry: Registry,
    force: bool = False,
    verbose: bool = False,
) -> Dict[str, int]:
    """
    Resolve all unresolved cross-references in the registry.

    Algorithm for each reference:
      1. Parse citation string → structured {usc_title, usc_section, cfr_title, ...}
      2. Score all corpus documents using stored metadata; collect those at the
         highest score (>= 2) and with the longest section/part match.
      3. If 1 candidate:   mark resolved
         If N candidates:  mark ambiguous
         If 0 candidates:  mark not_in_corpus
         If unparseable:   leave unresolved (status unchanged; AI pass can handle it)

    Args:
        registry: Open Registry instance.
        force:    Re-resolve already-resolved references too.
        verbose:  Print one line per reference showing the outcome.

    Returns:
        Stats dict: {resolved, ambiguous, not_in_corpus, no_citation, skipped}.
        'skipped' is always 0 — pre-filtering by status_filter means already-resolved
        refs never enter the loop; force=True processes all refs with no guard needed.
        The key is retained for backward compatibility with callers.
    """
    status_filter = None if force else 'unresolved'
    refs = registry.get_references(resolution_status=status_filter)
    corpus_docs = _deduplicate_by_stage(registry.get_all_documents())

    # Warn if no metadata is available — resolution will not work correctly.
    n_with_metadata = sum(1 for d in corpus_docs if d.get('metadata'))
    if corpus_docs and n_with_metadata == 0:
        import sys
        print(
            "WARNING: No corpus documents have metadata stored.\n"
            "         Run tools/extract_references.py before resolving references.\n"
            "         Resolution results will be unreliable.",
            file=sys.stderr,
        )

    stats = dict(resolved=0, ambiguous=0, not_in_corpus=0, no_citation=0, skipped=0)

    for ref in refs:
        ref_text = ref['ref_text']
        citation = parse_citation(ref_text)

        if not citation:
            # Cannot parse any structured citation from this string.
            # Leave status as 'unresolved' so the AI pass or manual review can handle it.
            stats['no_citation'] += 1
            if verbose:
                print(f"  [no_citation]    {ref_text!r}")
            continue

        best_score, candidates = _best_candidates(citation, corpus_docs)

        if best_score == 0:
            registry.mark_not_in_corpus(
                ref['id'],
                notes='No corpus document matched the citation.',
            )
            stats['not_in_corpus'] += 1
            if verbose:
                print(f"  [not_in_corpus]  {ref_text!r}")
            continue

        if len(candidates) == 1:
            target = candidates[0]
            target_item_type = None
            target_item_number = None
            if 'usc_section' in citation:
                target_item_type = 'section'
                target_item_number = citation['usc_section']
            elif 'cfr_section' in citation:
                target_item_type = 'section'
                target_item_number = citation['cfr_section']
            registry.resolve_reference(
                ref_id=ref['id'],
                target_doc_id=target['id'],
                target_item_type=target_item_type,
                target_item_number=target_item_number,
                resolution_method='automatic',
                notes=f'Matched by metadata scoring (score {best_score}).',
            )
            stats['resolved'] += 1
            if verbose:
                print(f"  [resolved]       {ref_text!r}"
                      f" → {Path(target['file_path']).name}")
        else:
            names = ', '.join(Path(d['file_path']).name for d in candidates[:3])
            if len(candidates) > 3:
                names += f' (+ {len(candidates) - 3} more)'
            registry.mark_ambiguous(
                ref['id'],
                notes=f'Matched {len(candidates)} corpus documents: {names}',
            )
            stats['ambiguous'] += 1
            if verbose:
                print(f"  [ambiguous]      {ref_text!r}"
                      f" → {len(candidates)} docs")

    return stats
