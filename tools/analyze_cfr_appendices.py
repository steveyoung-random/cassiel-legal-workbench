"""
Analyze CFR XML files to identify non-Appendix items stored as APPENDIX DIV elements.

This script scans eCFR XML files and reports items categorized as appendices
that don't follow the "Appendix X" naming pattern, helping identify candidates
for separate unit types.

Usage:
    python analyze_cfr_appendices.py <directory_path> [--recursive]
"""
# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License

import os
import sys
import argparse
from collections import defaultdict
from lxml import etree as ET


def extract_text(elem):
    """Extract all text from an element."""
    if elem is None:
        return ""
    return "".join(elem.itertext()).strip()


def get_context_path(elem):
    """Walk up the tree to build a context path (title/chapter/part)."""
    parts = []
    current = elem.getparent()
    while current is not None:
        if isinstance(current.tag, str) and current.tag.upper().startswith('DIV'):
            div_type = current.get('TYPE', '').upper()
            div_n = current.get('N', '')
            if div_type in ('TITLE', 'CHAPTER', 'SUBCHAP', 'PART', 'SUBPART') and div_n:
                parts.append(f"{div_type.lower()}={div_n}")
        current = current.getparent()
    parts.reverse()
    return " > ".join(parts) if parts else "(no context)"


def analyze_file(file_path, results):
    """
    Analyze a single XML file for non-Appendix appendix items.

    Args:
        file_path: Path to XML file
        results: Dict to accumulate results by name pattern
    """
    try:
        tree = ET.parse(file_path)
        root = tree.getroot()
    except Exception as e:
        print(f"  Error parsing {file_path}: {e}", file=sys.stderr)
        return

    # Find all APPENDIX DIV elements
    appendix_count = 0
    non_appendix_count = 0

    for elem in root.iter():
        if not isinstance(elem.tag, str):
            continue
        if not elem.tag.upper().startswith('DIV'):
            continue
        if elem.get('TYPE', '').upper() != 'APPENDIX':
            continue

        appendix_count += 1
        n_attr = elem.get('N', '')

        # Skip items that start with "Appendix"
        if n_attr.lower().startswith('appendix'):
            continue

        non_appendix_count += 1

        # Get the HEAD text for more context
        head_elem = elem.find('HEAD')
        head_text = extract_text(head_elem) if head_elem is not None else ""

        # Get context path
        context = get_context_path(elem)

        # Categorize by the first word or pattern
        if n_attr:
            first_word = n_attr.split()[0] if ' ' in n_attr else n_attr
            # Also check for patterns like "SFAR No. 36"
            if 'SFAR' in n_attr.upper():
                category = "SFAR"
            elif 'Supplement' in n_attr:
                category = "Supplement"
            elif 'Table' in n_attr:
                category = "Table"
            elif 'Schedule' in n_attr:
                category = "Schedule"
            elif 'Exhibit' in n_attr:
                category = "Exhibit"
            elif 'Figure' in n_attr:
                category = "Figure"
            elif 'Form' in n_attr:
                category = "Form"
            else:
                category = first_word
        else:
            category = "(no N attribute)"

        results[category].append({
            'file': os.path.basename(file_path),
            'n_attr': n_attr,
            'head': head_text[:100] + "..." if len(head_text) > 100 else head_text,
            'context': context
        })

    return appendix_count, non_appendix_count


def analyze_directory(dir_path, recursive=False):
    """
    Analyze all XML files in a directory.

    Args:
        dir_path: Path to directory
        recursive: Whether to process subdirectories
    """
    results = defaultdict(list)
    total_files = 0
    total_appendices = 0
    total_non_appendix = 0

    def process_dir(path):
        nonlocal total_files, total_appendices, total_non_appendix

        for item in sorted(os.listdir(path)):
            item_path = os.path.join(path, item)

            if os.path.isfile(item_path) and item.endswith('.xml'):
                total_files += 1
                counts = analyze_file(item_path, results)
                if counts:
                    total_appendices += counts[0]
                    total_non_appendix += counts[1]
            elif recursive and os.path.isdir(item_path):
                process_dir(item_path)

    if os.path.isfile(dir_path) and dir_path.endswith('.xml'):
        total_files = 1
        counts = analyze_file(dir_path, results)
        if counts:
            total_appendices, total_non_appendix = counts
    elif os.path.isdir(dir_path):
        process_dir(dir_path)
    else:
        print(f"Error: {dir_path} is not a valid file or directory", file=sys.stderr)
        sys.exit(1)

    # Print summary
    print("=" * 80)
    print("CFR APPENDIX ANALYSIS REPORT")
    print("=" * 80)
    print(f"\nFiles analyzed: {total_files}")
    print(f"Total APPENDIX elements: {total_appendices}")
    print(f"Non-'Appendix' items: {total_non_appendix}")
    print()

    if not results:
        print("No non-'Appendix' items found.")
        return

    # Print results grouped by category
    print("-" * 80)
    print("NON-'APPENDIX' ITEMS BY CATEGORY")
    print("-" * 80)

    for category in sorted(results.keys()):
        items = results[category]
        print(f"\n{category} ({len(items)} items)")
        print("-" * 40)

        for item in items[:10]:  # Show first 10 of each category
            print(f"  N: {item['n_attr']}")
            if item['head'] and item['head'] != item['n_attr']:
                print(f"     HEAD: {item['head']}")
            print(f"     Context: {item['context']}")
            print(f"     File: {item['file']}")
            print()

        if len(items) > 10:
            print(f"  ... and {len(items) - 10} more\n")

    # Print category summary
    print("-" * 80)
    print("CATEGORY SUMMARY")
    print("-" * 80)
    print(f"{'Category':<40} {'Count':>10}")
    print("-" * 50)
    for category in sorted(results.keys(), key=lambda c: -len(results[c])):
        print(f"{category:<40} {len(results[category]):>10}")
    print("-" * 50)
    print(f"{'TOTAL':<40} {total_non_appendix:>10}")


def main():
    parser = argparse.ArgumentParser(
        description='Analyze CFR XML files for non-Appendix items stored as APPENDIX elements.'
    )
    parser.add_argument(
        'path',
        help='Path to XML file or directory containing XML files'
    )
    parser.add_argument(
        '--recursive', '-r',
        action='store_true',
        help='Process subdirectories recursively'
    )

    args = parser.parse_args()
    analyze_directory(args.path, args.recursive)


if __name__ == '__main__':
    main()
