"""
Analyze CFR XML files to inventory all XML elements used.

This script scans eCFR XML files and reports:
1. All unique element tags found
2. Frequency of each element
3. Sample contexts where elements appear
4. Comparison with elements handled by the parser

Usage:
    python analyze_cfr_elements.py <directory_path> [--recursive] [--limit N]
"""
# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License

import os
import sys
import argparse
from collections import defaultdict
from lxml import etree as ET


# Elements explicitly handled in cfr_set_parse.py (as of current version)
HANDLED_ELEMENTS = {
    # Structural/DIV elements
    'DIV1', 'DIV2', 'DIV3', 'DIV4', 'DIV5', 'DIV6', 'DIV7', 'DIV8', 'DIV9',
    'DIV',  # Generic div for tables

    # Text content - standard paragraphs
    'P', 'FP', 'FP1', 'FP2', 'PSPACE',
    'HEAD',
    'NOTE', 'EXTRACT', 'EXAMPLE',

    # Text content - additional flush paragraph variants
    'FP-1', 'FP-2', 'FP1-2', 'FP2-2', 'FP2-3', 'FRP', 'FRP0',

    # Text content - dash paragraph types (forms)
    'FP-DASH', 'P-DASH', 'HALFDASH',

    # Text content - numbered paragraph variants
    'P-1', 'P-2', 'P-3', 'P1', 'P2',

    # List elements
    'LI', 'SCOL2',

    # Leader work (forms)
    'LDRWK', 'FL-2', 'LDRFIG',

    # Headings
    'HD1', 'HD2', 'HD3', 'HD4', 'HD5', 'HD6',
    'HED', 'HED1', 'PARTHD', 'DOCKETHD',

    # Captions
    'TCAP', 'BCAP',

    # Tables
    'TABLE', 'GPOTABLE', 'TR', 'TD', 'TH', 'ROW', 'ENT',

    # Inline formatting
    'I', 'E', 'FR', 'SU', 'AC',
    'B', 'STRONG', 'EM',  # Bold/emphasis
    'SUP', 'SUB',  # HTML-style super/subscript

    # Footnotes
    'FTNT', 'FTREF',

    # Images
    'IMG', 'GPH',

    # Metadata (ignored but recognized, except EFFDNOT which goes to annotation)
    'XREF', 'CITA', 'EDNOTE', 'EFFDNOT', 'AUTH', 'SOURCE', 'SECAUTH',
    'APPRO', 'PARAUTH',  # Approval/authority - ignored

    # Document structure (implicit)
    'DLPSTEXTCLASS', 'HEADER', 'TEXT', 'BODY', 'ECFRBRWS',
    'FILEDESC', 'TITLESTMT', 'TITLE', 'AUTHOR', 'PUBLICATIONSTMT',
    'PUBLISHER', 'PUBPLACE', 'IDNO', 'DATE', 'SERIESSTMT',
    'PROFILEDESC', 'TEXTCLASS', 'KEYWORDS',

    # Amendment date
    'AMDDATE',
}

# Elements that are definitely ignorable (formatting/navigation only)
IGNORABLE_ELEMENTS = {
    'CFRTOC', 'CHAPTI', 'PTHD', 'SECHD', 'SUBCHIND',
    'ALPHHD', 'SUBJECT', 'PG', 'PT',
    'PRTPAGE',
    'APP',  # TOC appendix entry
    # Index subject lines
    'SUBJ1L', 'SUBJL', 'SUBJ2L', 'SUBJ3L', 'SUBJECT1', 'SUBJECT2',
    'CHAPNO',  # Chapter number (reserved chapters)
}


def analyze_file(file_path, element_stats, sample_limit=3):
    """
    Analyze a single XML file for element usage.

    Args:
        file_path: Path to XML file
        element_stats: Dict to accumulate stats
        sample_limit: Max samples to keep per element
    """
    try:
        tree = ET.parse(file_path)
        root = tree.getroot()
    except Exception as e:
        print(f"  Error parsing {file_path}: {e}", file=sys.stderr)
        return 0

    element_count = 0
    file_name = os.path.basename(file_path)

    for elem in root.iter():
        if not isinstance(elem.tag, str):
            continue

        tag = elem.tag.upper()
        element_count += 1

        if tag not in element_stats:
            element_stats[tag] = {
                'count': 0,
                'files': set(),
                'samples': [],
                'parent_tags': set(),
                'has_text': False,
                'has_children': False,
            }

        stats = element_stats[tag]
        stats['count'] += 1
        stats['files'].add(file_name)

        # Track parent
        parent = elem.getparent()
        if parent is not None and isinstance(parent.tag, str):
            stats['parent_tags'].add(parent.tag.upper())

        # Check for text content
        if elem.text and elem.text.strip():
            stats['has_text'] = True

        # Check for children
        if len(elem) > 0:
            stats['has_children'] = True

        # Collect samples
        if len(stats['samples']) < sample_limit:
            sample_text = get_sample_text(elem)
            if sample_text:
                context = get_context_info(elem)
                stats['samples'].append({
                    'file': file_name,
                    'context': context,
                    'text': sample_text[:200] + ('...' if len(sample_text) > 200 else '')
                })

    return element_count


def get_sample_text(elem):
    """Get a text sample from an element."""
    # Get direct text
    if elem.text and elem.text.strip():
        return elem.text.strip()

    # Get all text content
    all_text = ''.join(elem.itertext())
    if all_text.strip():
        return all_text.strip()[:200]

    return None


def get_context_info(elem):
    """Get context information about where an element appears."""
    parts = []
    current = elem.getparent()
    depth = 0

    while current is not None and depth < 4:
        if isinstance(current.tag, str):
            tag = current.tag.upper()
            if tag.startswith('DIV'):
                div_type = current.get('TYPE', '')
                div_n = current.get('N', '')
                if div_type:
                    parts.append(f"{div_type}={div_n}" if div_n else div_type)
        current = current.getparent()
        depth += 1

    parts.reverse()
    return ' > '.join(parts) if parts else '(root)'


def analyze_directory(dir_path, recursive=False, file_limit=None):
    """Analyze all XML files in a directory."""
    element_stats = {}
    total_files = 0
    total_elements = 0

    def process_dir(path):
        nonlocal total_files, total_elements

        items = sorted(os.listdir(path))
        for item in items:
            if file_limit and total_files >= file_limit:
                return

            item_path = os.path.join(path, item)

            if os.path.isfile(item_path) and item.endswith('.xml'):
                total_files += 1
                print(f"  Analyzing: {item}", end='\r')
                count = analyze_file(item_path, element_stats)
                total_elements += count
            elif recursive and os.path.isdir(item_path):
                process_dir(item_path)

    if os.path.isfile(dir_path) and dir_path.endswith('.xml'):
        total_files = 1
        total_elements = analyze_file(dir_path, element_stats)
    elif os.path.isdir(dir_path):
        process_dir(dir_path)
    else:
        print(f"Error: {dir_path} is not a valid file or directory", file=sys.stderr)
        sys.exit(1)

    print(" " * 60)  # Clear progress line

    # Categorize elements
    handled = {}
    ignorable = {}
    unhandled_with_text = {}
    unhandled_structural = {}

    for tag, stats in element_stats.items():
        if tag in HANDLED_ELEMENTS:
            handled[tag] = stats
        elif tag in IGNORABLE_ELEMENTS:
            ignorable[tag] = stats
        elif stats['has_text']:
            unhandled_with_text[tag] = stats
        else:
            unhandled_structural[tag] = stats

    # Print report
    print("=" * 80)
    print("CFR XML ELEMENT INVENTORY REPORT")
    print("=" * 80)
    print(f"\nFiles analyzed: {total_files}")
    print(f"Total elements: {total_elements:,}")
    print(f"Unique element types: {len(element_stats)}")

    print("\n" + "-" * 80)
    print("SUMMARY BY CATEGORY")
    print("-" * 80)
    print(f"  Handled by parser:        {len(handled):4} types")
    print(f"  Known ignorable:          {len(ignorable):4} types")
    print(f"  Unhandled WITH text:      {len(unhandled_with_text):4} types  <-- PRIORITY")
    print(f"  Unhandled structural:     {len(unhandled_structural):4} types")

    if unhandled_with_text:
        print("\n" + "-" * 80)
        print("UNHANDLED ELEMENTS WITH TEXT CONTENT (Priority)")
        print("-" * 80)
        print("These elements contain text that may not be captured:\n")

        for tag in sorted(unhandled_with_text.keys(), key=lambda t: -unhandled_with_text[t]['count']):
            stats = unhandled_with_text[tag]
            print(f"  {tag}")
            print(f"    Count: {stats['count']:,} in {len(stats['files'])} file(s)")
            print(f"    Parents: {', '.join(sorted(stats['parent_tags']))}")
            if stats['samples']:
                print(f"    Sample: {stats['samples'][0]['text']}")
            print()

    if unhandled_structural:
        print("\n" + "-" * 80)
        print("UNHANDLED STRUCTURAL ELEMENTS (Lower priority)")
        print("-" * 80)
        print("These elements don't directly contain text:\n")

        for tag in sorted(unhandled_structural.keys(), key=lambda t: -unhandled_structural[t]['count']):
            stats = unhandled_structural[tag]
            print(f"  {tag}: {stats['count']:,} occurrences in {len(stats['files'])} file(s)")
            print(f"    Parents: {', '.join(sorted(stats['parent_tags']))}")

    print("\n" + "-" * 80)
    print("HANDLED ELEMENTS (Already in parser)")
    print("-" * 80)
    for tag in sorted(handled.keys()):
        stats = handled[tag]
        print(f"  {tag}: {stats['count']:,}")

    print("\n" + "-" * 80)
    print("IGNORABLE ELEMENTS (Can be skipped)")
    print("-" * 80)
    for tag in sorted(ignorable.keys()):
        stats = ignorable[tag]
        print(f"  {tag}: {stats['count']:,}")


def main():
    parser = argparse.ArgumentParser(
        description='Inventory XML elements in CFR files.'
    )
    parser.add_argument(
        'path',
        help='Path to XML file or directory'
    )
    parser.add_argument(
        '--recursive', '-r',
        action='store_true',
        help='Process subdirectories recursively'
    )
    parser.add_argument(
        '--limit', '-l',
        type=int,
        default=None,
        help='Limit number of files to process'
    )

    args = parser.parse_args()
    analyze_directory(args.path, args.recursive, args.limit)


if __name__ == '__main__':
    main()
