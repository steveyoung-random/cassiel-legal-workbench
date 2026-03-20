"""
Detect Long Substantive Units

Scans parsed JSON output files for substantive units whose text exceeds a
configurable threshold and reports detected structural patterns that may
indicate candidates for sub-unit subdivision.

This is a reporting/analysis tool only — it does not modify files.

Usage:
    python tools/detect_long_units.py <path> [--threshold N]

    <path>       A single JSON file or a directory (scanned recursively for *.json)
    --threshold  Minimum character count to flag a unit (default: 100000)

Examples:
    python tools/detect_long_units.py ~/document_analyzer_output/CFR/ --threshold 100000
    python tools/detect_long_units.py output/title15_parse_output.json
"""
# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import List, Tuple


# Structural patterns to look for in long text.
# Each tuple: (display_name, regex, suggested_type, approach_description, min_matches)
# min_matches defaults to 5 if not provided (tuple length < 5).
PATTERN_CHECKS = [
    (
        "Bold ECCN markers",
        re.compile(r'\*\*(\d[A-E]\d{3}|EAR99)\b'),
        "eccn",
        "ECCN subdivision (bold **DXDDD markers)",
        5,
    ),
    (
        "Bold numbered headings",
        re.compile(r'\*\*\d+\.\d+'),
        "numbered_heading",
        "Numbered heading subdivision",
        5,
    ),
    (
        "Markdown-style headings",
        re.compile(r'^#{1,4}\s+', re.MULTILINE),
        "heading",
        "Heading-based subdivision",
        5,
    ),
    (
        "Repeating bold markers",
        re.compile(r'^\*\*[A-Z]', re.MULTILINE),
        "bold_marker",
        "Bold marker subdivision",
        5,
    ),
    (
        "HTML table rows",
        re.compile(r'<TR[\s>]', re.IGNORECASE),
        "html_table_rows",
        "HTML table subdivision (group by first-column value)",
        50,
    ),
    (
        "Multi-method HD1 sections (Method NNN)",
        re.compile(r'##HD1## Method \d+', re.IGNORECASE),
        "method_section",
        "HD1-based subdivision (split at each 'Method NNN' boundary) — already handled by parser",
        2,
    ),
    (
        "Multi-item HD1 sections (Performance Specification / Procedure NNN)",
        re.compile(r'##HD1## (?:Performance Specification|Procedure) \d+', re.IGNORECASE),
        "method_section",
        "HD1-based subdivision (split at each named-item boundary) — already handled by parser",
        2,
    ),
]


def scan_file(file_path: str, threshold: int) -> List[dict]:
    """Scan a single parsed JSON file for long units."""
    results = []

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except (json.JSONDecodeError, IOError) as e:
        print(f"  Warning: Could not read {file_path}: {e}", file=sys.stderr)
        return results

    if 'document_information' not in data or 'content' not in data:
        return results

    params = data.get('document_information', {}).get('parameters', {})
    content = data.get('content', {})
    doc_title = data.get('document_information', {}).get('title', os.path.basename(file_path))

    for param_key, p in params.items():
        type_name = p.get('name', '')
        type_name_plural = p.get('name_plural', '')
        is_sub_unit = p.get('is_sub_unit', False)

        if is_sub_unit:
            continue

        items_dict = content.get(type_name_plural, {})
        for item_number, item_data in items_dict.items():
            text = item_data.get('text', '')
            text_len = len(text)

            # Also check sub_units total if present
            sub_units = item_data.get('sub_units', {})
            if sub_units:
                sub_total = sum(len(su.get('text', '')) for su in sub_units.values())
                if sub_total > text_len:
                    text_len = sub_total
                    text = None  # Don't pattern-check aggregated text

            if text_len < threshold:
#               print(str(type_name) + ', ' + str(item_number) + ', ' + str(text_len) + '\n')
                continue

            entry = {
                'file': file_path,
                'document': doc_title,
                'type': type_name,
                'type_plural': type_name_plural,
                'number': item_number,
                'char_count': text_len,
                'has_sub_units': bool(sub_units),
                'sub_unit_count': len(sub_units) if sub_units else 0,
                'patterns': []
            }

            # Check for structural patterns (only if we have the raw text)
            if text:
                for pattern_tuple in PATTERN_CHECKS:
                    pattern_name, regex, suggested_type, approach = pattern_tuple[:4]
                    min_matches = pattern_tuple[4] if len(pattern_tuple) > 4 else 5
                    matches = regex.findall(text)
                    if len(matches) >= min_matches:
                        entry['patterns'].append({
                            'pattern': pattern_name,
                            'match_count': len(matches),
                            'suggested_type': suggested_type,
                            'approach': approach
                        })

            results.append(entry)

    return results


def main():
    parser = argparse.ArgumentParser(
        description='Detect long substantive units in parsed JSON output files.'
    )
    parser.add_argument('path', help='JSON file or directory to scan')
    parser.add_argument('--threshold', type=int, default=100_000,
                        help='Minimum character count to flag (default: 100000)')
    args = parser.parse_args()

    target = Path(args.path)
    if not target.exists():
        print(f"Error: Path not found: {args.path}", file=sys.stderr)
        sys.exit(1)

    # Collect files to scan
    if target.is_file():
        files = [str(target)]
    else:
        files = sorted(str(p) for p in target.rglob('*.json'))

    if not files:
        print(f"No JSON files found in {args.path}")
        sys.exit(0)

    print(f"Scanning {len(files)} file(s) with threshold {args.threshold:,} characters...\n")

    all_results = []
    for file_path in files:
        results = scan_file(file_path, args.threshold)
        all_results.extend(results)

    if not all_results:
        print("No units exceed the threshold.")
        return

    # Sort by character count descending
    all_results.sort(key=lambda r: r['char_count'], reverse=True)

    print(f"Found {len(all_results)} unit(s) exceeding {args.threshold:,} characters:\n")
    print(f"{'Type':<25} {'Number':<35} {'Chars':>12}  {'Sub-Units':>10}  Patterns")
    print("-" * 120)

    for r in all_results:
        sub_info = f"{r['sub_unit_count']} sub-units" if r['has_sub_units'] else "-"
        pattern_names = ", ".join(p['pattern'] + f" ({p['match_count']})" for p in r['patterns'])
        if not pattern_names:
            pattern_names = "(no patterns detected)" if not r['has_sub_units'] else "(already subdivided)"

        print(f"{r['type']:<25} {r['number']:<35} {r['char_count']:>12,}  {sub_info:>10}  {pattern_names}")

    # Summary
    print(f"\n--- Summary ---")
    print(f"Files scanned: {len(files)}")
    print(f"Long units found: {len(all_results)}")
    already_split = sum(1 for r in all_results if r['has_sub_units'])
    if already_split:
        print(f"Already subdivided: {already_split}")
    needs_work = sum(1 for r in all_results if not r['has_sub_units'] and r['patterns'])
    if needs_work:
        print(f"Candidates for subdivision (patterns detected): {needs_work}")

    # Detailed pattern suggestions
    unsplit_with_patterns = [r for r in all_results if not r['has_sub_units'] and r['patterns']]
    if unsplit_with_patterns:
        print(f"\n--- Subdivision Suggestions ---")
        for r in unsplit_with_patterns:
            print(f"\n  {r['type'].title()} {r['number']} ({r['char_count']:,} chars)")
            print(f"  File: {r['file']}")
            for p in r['patterns']:
                print(f"    Pattern: {p['pattern']} ({p['match_count']} matches)")
                print(f"    Suggested approach: {p['approach']}")


if __name__ == '__main__':
    main()
