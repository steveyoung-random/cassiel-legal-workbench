"""
find_chunk_prefixes.py — scan parsed output files for chunk_prefix values.

Usage:
    python tools/find_chunk_prefixes.py <directory>

Walks <directory> recursively, loads every *_parse_output.json file,
collects all chunk_prefix values found anywhere in the document tree,
deduplicates them, and prints each unique value.
"""

import argparse
import json
import sys
from pathlib import Path


def collect_chunk_prefixes(node, results: set) -> None:
    """Recursively walk a parsed-output data structure, collecting chunk_prefix values."""
    if isinstance(node, dict):
        cp = node.get('chunk_prefix')
        if cp:
            results.add(cp)
        for v in node.values():
            collect_chunk_prefixes(v, results)
    elif isinstance(node, list):
        for item in node:
            collect_chunk_prefixes(item, results)


def main() -> None:
    parser = argparse.ArgumentParser(description='Find unique chunk_prefix values in parsed output files.')
    parser.add_argument('directory', help='Root directory to search (searched recursively)')
    args = parser.parse_args()

    root = Path(args.directory)
    if not root.is_dir():
        print(f'Error: {root} is not a directory', file=sys.stderr)
        sys.exit(1)

    files = sorted(root.rglob('*_parse_output.json'))
    if not files:
        print(f'No *_parse_output.json files found under {root}')
        return

    all_prefixes: set = set()
    errors = []

    for path in files:
        try:
            with open(path, encoding='utf-8') as f:
                data = json.load(f)
            collect_chunk_prefixes(data, all_prefixes)
        except Exception as e:
            errors.append((path, e))

    if errors:
        print(f'Warning: {len(errors)} file(s) could not be read:', file=sys.stderr)
        for p, e in errors:
            print(f'  {p}: {e}', file=sys.stderr)

    if not all_prefixes:
        print('No chunk_prefix values found.')
        return

    print(f'Found {len(all_prefixes)} unique chunk_prefix value(s):\n')
    for prefix in sorted(all_prefixes):
        print(prefix)
        print()


if __name__ == '__main__':
    main()
