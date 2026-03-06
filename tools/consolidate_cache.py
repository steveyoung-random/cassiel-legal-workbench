#!/usr/bin/env python3
"""
Consolidate Multiple API Cache Files

This script consolidates multiple api_cache.json files into a single cache file,
deduplicating entries based on their hash keys. Each unique hash key will appear
only once in the output, even if it exists in multiple input files.

Usage:
    python consolidate_cache.py <cache_file1> [cache_file2] ... [--output <output_file>]
    python consolidate_cache.py --glob "api_cache_*.json" [--output <output_file>]

Arguments:
    cache_file(s): One or more paths to api_cache.json files to consolidate
    --output: Path to output consolidated cache (default: api_cache_consolidated.json)
    --glob: Glob pattern to find cache files (e.g., "api_cache_*.json")
"""
# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License

import json
import argparse
import glob
import os
import sys
from typing import Dict, Any, List


def load_cache_file(cache_file: str) -> Dict[str, Dict[str, Any]]:
    """
    Load a cache file and return its contents.
    
    Args:
        cache_file: Path to the cache file to load
        
    Returns:
        Dictionary of cache entries (hash -> entry data)
        
    Raises:
        SystemExit: If the file cannot be read or is invalid JSON
    """
    if not os.path.exists(cache_file):
        print(f"Warning: File not found: {cache_file}", file=sys.stderr)
        return {}
    
    try:
        with open(cache_file, 'r', encoding='utf-8') as f:
            cache_data = json.load(f)
            
            # Validate that it's a dictionary (the expected cache format)
            if not isinstance(cache_data, dict):
                print(f"Warning: {cache_file} does not contain a dictionary. Skipping.", file=sys.stderr)
                return {}
            
            return cache_data
            
    except json.JSONDecodeError as e:
        print(f"Error: {cache_file} is not valid JSON: {e}", file=sys.stderr)
        print(f"Skipping {cache_file}", file=sys.stderr)
        return {}
    except IOError as e:
        print(f"Error reading {cache_file}: {e}", file=sys.stderr)
        print(f"Skipping {cache_file}", file=sys.stderr)
        return {}


def consolidate_cache_files(cache_files: List[str], output_file: str) -> None:
    """
    Consolidate multiple cache files into a single output file.
    
    Entries are deduplicated by their hash key. If the same hash appears in
    multiple files, only one entry is kept (the first one encountered).
    
    Args:
        cache_files: List of paths to cache files to consolidate
        output_file: Path to write the consolidated cache
    """
    consolidated_cache: Dict[str, Dict[str, Any]] = {}
    total_entries = 0
    duplicate_entries = 0
    
    print(f"Consolidating {len(cache_files)} cache file(s)...")
    
    for cache_file in cache_files:
        print(f"  Loading: {cache_file}")
        cache_data = load_cache_file(cache_file)
        
        file_entries = len(cache_data)
        file_duplicates = 0
        
        for hash_key, entry_data in cache_data.items():
            if hash_key in consolidated_cache:
                file_duplicates += 1
                duplicate_entries += 1
                # Skip duplicate - keep the first one encountered
                continue
            
            # Add entry to consolidated cache
            consolidated_cache[hash_key] = entry_data
        
        total_entries += file_entries
        print(f"    Entries in file: {file_entries} (duplicates: {file_duplicates})")
    
    print(f"\nTotal entries across all files: {total_entries}")
    print(f"Duplicate entries skipped: {duplicate_entries}")
    print(f"Unique entries in consolidated cache: {len(consolidated_cache)}")
    
    # Write consolidated cache to output file
    print(f"\nWriting consolidated cache to: {output_file}")
    try:
        # Create directory if it doesn't exist
        output_dir = os.path.dirname(output_file) or '.'
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)
        
        # Write with pretty formatting (indent=2 matches the format used by APICache.save_cache)
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(consolidated_cache, f, indent=2, ensure_ascii=False)
        
        print(f"Successfully wrote {len(consolidated_cache)} unique entries to {output_file}")
        
    except IOError as e:
        print(f"Error writing to {output_file}: {e}", file=sys.stderr)
        sys.exit(1)


def main():
    """Main function."""
    parser = argparse.ArgumentParser(
        description='Consolidate multiple api_cache.json files into a single cache file.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Consolidate specific files
  python consolidate_cache.py cache1.json cache2.json cache3.json --output consolidated.json
  
  # Use glob pattern to find files
  python consolidate_cache.py --glob "api_cache_*.json" --output consolidated.json
  
  # Consolidate all cache files in current directory
  python consolidate_cache.py --glob "*.json" --output api_cache_consolidated.json
        """
    )
    
    parser.add_argument(
        'cache_files',
        nargs='*',
        help='One or more cache files to consolidate'
    )
    
    parser.add_argument(
        '--output',
        default='api_cache_consolidated.json',
        help='Output file for consolidated cache (default: api_cache_consolidated.json)'
    )
    
    parser.add_argument(
        '--glob',
        help='Glob pattern to find cache files (e.g., "api_cache_*.json")'
    )
    
    args = parser.parse_args()
    
    # Collect cache files from arguments and/or glob pattern
    cache_files = list(args.cache_files) if args.cache_files else []
    
    if args.glob:
        glob_files = glob.glob(args.glob)
        if not glob_files:
            print(f"Warning: No files found matching pattern: {args.glob}", file=sys.stderr)
        else:
            cache_files.extend(glob_files)
    
    if not cache_files:
        print("Error: No cache files specified. Use file arguments or --glob option.", file=sys.stderr)
        parser.print_help()
        sys.exit(1)
    
    # Remove duplicates while preserving order
    seen = set()
    unique_files = []
    for f in cache_files:
        abs_path = os.path.abspath(f)
        if abs_path not in seen:
            seen.add(abs_path)
            unique_files.append(f)
    
    cache_files = unique_files
    
    # Don't include output file in input files
    output_abs = os.path.abspath(args.output)
    cache_files = [f for f in cache_files if os.path.abspath(f) != output_abs]
    
    if not cache_files:
        print("Error: No valid cache files to consolidate (output file excluded).", file=sys.stderr)
        sys.exit(1)
    
    consolidate_cache_files(cache_files, args.output)


if __name__ == '__main__':
    main()
