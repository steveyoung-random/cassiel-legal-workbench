#!/usr/bin/env python3
"""
Recover API Cache from Corrupted File

This script attempts to recover entries from a corrupted api_cache.json file
by reading it line-by-line and extracting valid cache entries.

Usage:
    python recover_cache.py <corrupted_file> [--output <output_file>]

Arguments:
    corrupted_file: Path to the corrupted cache file
    --output: Path to output recovered cache (default: api_cache_recovered.json)
"""
# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License

import json
import argparse
import re
from typing import Dict, Any


def recover_cache_entries(corrupted_file: str) -> Dict[str, Any]:
    """
    Attempt to recover cache entries from a corrupted file.

    Strategy:
    1. Read the file as text
    2. Try to find individual cache entry patterns
    3. Extract valid entries

    Args:
        corrupted_file: Path to corrupted cache file

    Returns:
        dict: Recovered cache entries
    """
    recovered = {}

    try:
        with open(corrupted_file, 'r', encoding='utf-8') as f:
            content = f.read()
    except Exception as e:
        print(f"Error reading file: {e}")
        return recovered

    print(f"File size: {len(content)} characters")

    # Strategy 1: Try to find the last valid JSON structure before corruption
    # The file might be valid up to a certain point
    for i in range(len(content) - 1, max(0, len(content) - 10000), -1):
        test_content = content[:i]
        # Try to close any open structures
        for ending in ['', '}', '}}', '}}}', '}}}}']:
            try:
                test_json = test_content + ending
                recovered = json.loads(test_json)
                print(f"Successfully recovered cache using truncation at position {i}")
                print(f"Recovered entries: {len(recovered)}")
                return recovered
            except json.JSONDecodeError:
                continue

    # Strategy 2: Extract individual cache entries using regex
    # Cache entries follow pattern: "hash": { "response": ..., "full_cache": ..., etc }
    print("\nAttempting entry-by-entry extraction...")

    # Look for hash keys (64-character hex strings)
    pattern = r'"([a-f0-9]{64})":\s*\{'
    matches = list(re.finditer(pattern, content))

    print(f"Found {len(matches)} potential cache entries")

    for i, match in enumerate(matches):
        hash_key = match.group(1)
        start_pos = match.end() - 1  # Start at the opening brace

        # Find the matching closing brace
        depth = 0
        end_pos = start_pos

        for j in range(start_pos, min(start_pos + 100000, len(content))):
            if content[j] == '{':
                depth += 1
            elif content[j] == '}':
                depth -= 1
                if depth == 0:
                    end_pos = j + 1
                    break

        if depth == 0:  # Found matching brace
            try:
                entry_text = content[start_pos:end_pos]
                entry_data = json.loads(entry_text)

                # Validate entry has required fields
                if 'response' in entry_data:
                    recovered[hash_key] = entry_data
                    if (i + 1) % 100 == 0:
                        print(f"Recovered {i + 1} entries...")
            except json.JSONDecodeError:
                continue

    print(f"\nTotal entries recovered: {len(recovered)}")
    return recovered


def main():
    """Main function."""
    parser = argparse.ArgumentParser(
        description='Recover entries from corrupted API cache file',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument('corrupted_file', help='Path to corrupted cache file')
    parser.add_argument('--output', default='api_cache_recovered.json',
                       help='Path to output recovered cache (default: api_cache_recovered.json)')

    args = parser.parse_args()

    print(f"Attempting to recover cache from: {args.corrupted_file}")
    print(f"Output will be saved to: {args.output}\n")

    # Attempt recovery
    recovered = recover_cache_entries(args.corrupted_file)

    if recovered:
        # Save recovered cache
        try:
            with open(args.output, 'w', encoding='utf-8') as f:
                json.dump(recovered, f, indent=2, ensure_ascii=False)
            print(f"\nRecovered cache saved to: {args.output}")
            print(f"\nNext steps:")
            print(f"1. Review the recovered cache file")
            print(f"2. If it looks good, rename it to api_cache.json")
            print(f"3. Rename/backup the corrupted file")
        except Exception as e:
            print(f"Error saving recovered cache: {e}")
    else:
        print("\nNo entries could be recovered.")
        print("\nAlternative: Use populate_cache_from_logs.py to rebuild cache from log files:")
        print("  python populate_cache_from_logs.py <log_directory>")


if __name__ == '__main__':
    main()
