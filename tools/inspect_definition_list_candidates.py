"""
inspect_definition_list_candidates.py — audit USLM definition-list detection.

Scans USC XML source files and reports every section/subsection that passes
the current three structural gates (length, chapeau, paragraph count) used
in the parser's definition-list detection, WITHOUT running the AI gate (Stage 4).

For each candidate the tool shows:
  - The element identifier and heading
  - Chapeau text
  - Total character count and paragraph count
  - First N paragraph content samples, annotated with [Q] if the paragraph
    starts with a quote character (the proposed Stage 4.5 signal)
  - A Stage 4.5 verdict based on the --quote-ratio threshold

Use this to evaluate whether a proposed "quoted-term ratio" gate would
correctly distinguish true multi-term definition lists from single-term
definitions with enumerated elements, before committing to any implementation.

Usage:
    python tools/inspect_definition_list_candidates.py <xml_dir>
           [--min-chars N] [--min-items N]
           [--quote-ratio R] [--sample N]
           [--rejected-only]

Arguments:
    xml_dir         Directory containing USC XML source files (*.xml)
    --min-chars N   Minimum total chars for Stage 1 (default: 6000)
    --min-items N   Minimum paragraph count for Stage 3 (default: 5)
    --quote-ratio R Fraction of sampled paragraphs that must start with a
                    quote for Stage 4.5 to PASS (default: 0.40)
    --sample N      Number of paragraph content samples to show (default: 5)
    --rejected-only Show only candidates that Stage 4.5 would reject

Output columns / annotations:
    [Q]   Paragraph content begins with a quote character (ASCII or Unicode)
    [ ]   Paragraph content does not begin with a quote character
    PASS  Stage 4.5 would keep this candidate (quote ratio >= threshold)
     REJECT  Stage 4.5 would discard this candidate (quote ratio < threshold)
"""
# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License

import argparse
import io
import re
import sys
import textwrap
from pathlib import Path

from lxml import etree as ET

# Force UTF-8 output so Unicode characters in USC text don't crash on Windows
# terminals that default to cp1252.
if hasattr(sys.stdout, 'buffer'):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

# ---------------------------------------------------------------------------
# Quote-character detection
# ---------------------------------------------------------------------------

# Characters considered "open quote" at the start of a term definition.
# ASCII double/single, Unicode curly double/single.
_OPEN_QUOTES = frozenset('"\'  \u201c\u2018')

_STARTS_WITH_QUOTE_RE = re.compile(r'^\s*["\'\u201c\u2018]')


def _starts_with_quote(text: str) -> bool:
    """Return True if text begins (ignoring leading whitespace) with a quote char."""
    return bool(_STARTS_WITH_QUOTE_RE.match(text))


# ---------------------------------------------------------------------------
# XML utilities (inlined to avoid project sys.path dependency)
# ---------------------------------------------------------------------------

def _strip_namespaces(root):
    """
    Strip namespaces from all element tags in-place (mirrors
    utils.xml_processing.drop_ns_and_prefix_to_underscore).
    Prefixed tags like <dc:term> become <dc_term>; default-namespace tags
    keep their local name unchanged.
    """
    if isinstance(root, ET._ElementTree):
        root = root.getroot()
    for el in root.iter():
        q = ET.QName(el)
        local = q.localname
        pref = el.prefix
        el.tag = f"{pref}_{local}" if pref else local
    return root


def _element_text(el) -> str:
    """Return all text content of an element as a single string."""
    return ''.join(el.itertext())


def _direct_child_text(el, tag: str):
    """Return the first direct child with the given tag, or None."""
    tag_lower = tag.lower()
    for child in el:
        if child.tag and child.tag.lower() == tag_lower:
            return child
    return None


def _direct_children_with_tag(el, tag: str):
    """Return all direct children with the given tag."""
    tag_lower = tag.lower()
    return [c for c in el if c.tag and c.tag.lower() == tag_lower]


def _first_content_text(para) -> str:
    """
    Return the leading text of a <paragraph> element.
    Prefers the text of a direct <content> child; falls back to all text.
    Strips leading/trailing whitespace.
    """
    content_el = _direct_child_text(para, 'content')
    if content_el is not None:
        return _element_text(content_el).strip()
    return _element_text(para).strip()


def _heading_text(el) -> str:
    """Return the text of a direct <heading> child, or ''."""
    h = _direct_child_text(el, 'heading')
    return _element_text(h).strip() if h is not None else ''


def _identifier(el) -> str:
    """Return the 'identifier' attribute if present, else the 'id' attribute."""
    return el.get('identifier') or el.get('id') or ''


# ---------------------------------------------------------------------------
# Gate logic (mirrors _check_definition_list_candidate, stages 1-3 only)
# ---------------------------------------------------------------------------

def _check_structural_gates(el, min_chars: int, min_items: int):
    """
    Run stages 1-3 of the definition-list gate on *el*.

    Returns (chapeau_text, paragraphs) if all three stages pass,
    or None if any stage fails.

    Stage 1: total itertext() chars >= min_chars
    Stage 2: direct <chapeau> child exists
    Stage 3: at least min_items direct <paragraph> children
    """
    # Stage 1: length gate
    total_chars = sum(len(s) for s in el.itertext())
    if total_chars < min_chars:
        return None

    # Stage 2: chapeau gate
    chapeau = _direct_child_text(el, 'chapeau')
    if chapeau is None:
        return None

    # Stage 3: paragraph count gate
    paragraphs = _direct_children_with_tag(el, 'paragraph')
    if len(paragraphs) < min_items:
        return None

    chapeau_text = _element_text(chapeau).strip()
    return chapeau_text, paragraphs, total_chars


# ---------------------------------------------------------------------------
# Stage 4.5 metric
# ---------------------------------------------------------------------------

def _quoted_term_stats(paragraphs, sample_size: int = 8):
    """
    For the first *sample_size* paragraphs return (quoted_count, sample_taken,
    per_para_list) where per_para_list is a list of (content_snippet, is_quoted)
    tuples.
    """
    sample = paragraphs[:sample_size]
    results = []
    for para in sample:
        text = _first_content_text(para)
        snippet = text[:120].replace('\n', ' ')
        results.append((snippet, _starts_with_quote(text)))
    quoted = sum(1 for _, q in results if q)
    return quoted, len(sample), results


# ---------------------------------------------------------------------------
# Candidate collection
# ---------------------------------------------------------------------------

def _candidates_in_section(section_el, min_chars: int, min_items: int):
    """
    Yield (element, level_label) for each sub-element of *section_el*
    (including *section_el* itself) that passes the structural gates.

    Mirrors _find_qualifying_definition_paragraphs: checks section level,
    then each direct <subsection> child.  Unlike the parser, this yields
    ALL matches (not just the first) for exhaustive inspection.
    """
    # Section level
    result = _check_structural_gates(section_el, min_chars, min_items)
    if result is not None:
        yield section_el, 'section', result

    # Each direct subsection
    for child in section_el:
        if child.tag and child.tag.lower() == 'subsection':
            result = _check_structural_gates(child, min_chars, min_items)
            if result is not None:
                yield child, 'subsection', result


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

def _wrap(text: str, width: int = 100, indent: str = '    ') -> str:
    """Wrap text to width with a uniform indent."""
    lines = textwrap.wrap(text, width=width - len(indent))
    return ('\n' + indent).join(lines)


def _format_candidate(el, level: str, result, sample_n: int,
                       quote_ratio: float, file_name: str) -> str:
    """Return a formatted multi-line string describing one candidate."""
    chapeau_text, paragraphs, total_chars = result
    quoted, sample_taken, para_samples = _quoted_term_stats(paragraphs, sample_n)

    ratio = quoted / sample_taken if sample_taken else 0.0
    passes_4_5 = ratio >= quote_ratio
    verdict = 'PASS  ' if passes_4_5 else 'REJECT'

    ident = _identifier(el)
    heading = _heading_text(el)
    heading_part = f'  "{heading}"' if heading else ''

    lines = [
        f'[{verdict}]  {ident}{heading_part}  ({level})',
        f'  Chars: {total_chars:,}  |  Paragraphs: {len(paragraphs)}'
        f'  |  Quoted starts: {quoted}/{sample_taken}'
        + (f'  ({ratio:.0%})' if sample_taken else ''),
        f'  Chapeau: {_wrap(chapeau_text, width=100, indent="           ")}',
        f'  Para content (first {len(para_samples)}):',
    ]
    for i, (snippet, is_q) in enumerate(para_samples, 1):
        marker = '[Q]' if is_q else '[ ]'
        lines.append(f'    ({i}) {marker} {snippet[:100]}')

    return '\n'.join(lines)


# ---------------------------------------------------------------------------
# Main scan
# ---------------------------------------------------------------------------

def scan_file(xml_path: Path, min_chars: int, min_items: int,
              quote_ratio: float, sample_n: int,
              rejected_only: bool) -> list:
    """
    Parse *xml_path*, find all definition-list candidates, and return a list
    of formatted strings (one per candidate).
    """
    try:
        tree = ET.parse(str(xml_path))
    except ET.XMLSyntaxError as exc:
        print(f'  WARNING: XML parse error in {xml_path.name}: {exc}', file=sys.stderr)
        return []

    root = _strip_namespaces(tree)

    formatted = []
    for section_el in root.iter('section'):
        for el, level, result in _candidates_in_section(section_el, min_chars, min_items):
            chapeau_text, paragraphs, total_chars = result
            quoted, sample_taken, _ = _quoted_term_stats(paragraphs, sample_n)
            ratio = quoted / sample_taken if sample_taken else 0.0
            passes_4_5 = ratio >= quote_ratio

            if rejected_only and passes_4_5:
                continue

            formatted.append(_format_candidate(
                el, level, result, sample_n, quote_ratio, xml_path.name
            ))

    return formatted


def main():
    p = argparse.ArgumentParser(
        description='Audit USLM definition-list detection candidates.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument('xml_dir',
                   help='Directory containing USC XML source files (*.xml)')
    p.add_argument('--min-chars', type=int, default=6000, metavar='N',
                   help='Stage 1 minimum total chars (default: 6000)')
    p.add_argument('--min-items', type=int, default=5, metavar='N',
                   help='Stage 3 minimum paragraph count (default: 5)')
    p.add_argument('--quote-ratio', type=float, default=0.40, metavar='R',
                   help='Stage 4.5 pass threshold: fraction of sampled paragraphs '
                        'that must start with a quote (default: 0.40)')
    p.add_argument('--sample', type=int, default=5, metavar='N',
                   help='Number of paragraph samples to display per candidate (default: 5)')
    p.add_argument('--rejected-only', action='store_true',
                   help='Show only candidates that Stage 4.5 would reject')
    p.add_argument('--output', metavar='FILE',
                   help='Write output to FILE (UTF-8) instead of stdout; '
                        'useful for large corpora like usc42')
    args = p.parse_args()

    xml_dir = Path(args.xml_dir)
    if not xml_dir.is_dir():
        print(f'Error: {xml_dir} is not a directory', file=sys.stderr)
        sys.exit(1)

    xml_files = sorted(xml_dir.glob('*.xml'))
    if not xml_files:
        print(f'No *.xml files found in {xml_dir}')
        sys.exit(0)

    # Open output destination
    if args.output:
        out = open(args.output, 'w', encoding='utf-8')
    else:
        out = sys.stdout

    def emit(*a, **kw):
        kw.setdefault('file', out)
        print(*a, **kw)

    total_candidates = 0
    total_pass = 0
    total_reject = 0
    per_file_reject: dict = {}

    emit(f'Stage 4.5 threshold: quote ratio >= {args.quote_ratio:.0%}')
    emit(f'Structural gates: min_chars={args.min_chars:,}, min_items={args.min_items}')
    emit(f'Scanning {len(xml_files)} file(s) in {xml_dir}\n')
    emit('=' * 80)

    for xml_path in xml_files:
        all_for_file: list = []

        try:
            tree = ET.parse(str(xml_path))
        except ET.XMLSyntaxError as exc:
            print(f'WARNING: XML parse error in {xml_path.name}: {exc}', file=sys.stderr)
            continue

        root = _strip_namespaces(tree)

        file_candidates = 0
        file_pass = 0
        file_reject = 0

        for section_el in root.iter('section'):
            for el, level, result in _candidates_in_section(section_el, args.min_chars, args.min_items):
                file_candidates += 1
                chapeau_text, paragraphs, total_chars = result
                quoted, sample_taken, _ = _quoted_term_stats(paragraphs, args.sample)
                ratio = quoted / sample_taken if sample_taken else 0.0
                passes_4_5 = ratio >= args.quote_ratio

                if passes_4_5:
                    file_pass += 1
                else:
                    file_reject += 1

                if args.rejected_only and passes_4_5:
                    continue

                all_for_file.append(_format_candidate(
                    el, level, result, args.sample, args.quote_ratio, xml_path.name
                ))

        total_candidates += file_candidates
        total_pass += file_pass
        total_reject += file_reject
        if file_reject:
            per_file_reject[xml_path.name] = file_reject

        label = f'=== {xml_path.name} ===  ({file_candidates} candidate(s): '
        label += f'{file_pass} PASS, {file_reject} REJECT)'
        emit(label)
        if all_for_file:
            emit()
            emit('\n\n'.join(all_for_file))
        emit()
        emit('=' * 80)

    # Summary
    emit()
    emit('SUMMARY')
    emit(f'  Files scanned:            {len(xml_files)}')
    emit(f'  Total candidates:         {total_candidates}')
    emit(f'  Would PASS  Stage 4.5:    {total_pass}')
    emit(f'  Would REJECT Stage 4.5:   {total_reject}')
    if per_file_reject:
        emit()
        emit('  Rejections by file:')
        for fname, count in sorted(per_file_reject.items()):
            emit(f'    {fname}: {count}')

    if args.output:
        out.close()
        print(f'Output written to: {args.output}', file=sys.stderr)


if __name__ == '__main__':
    main()
