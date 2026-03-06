#!/usr/bin/env python3
"""
Populate API Cache from Log Files

This script reads existing log files and populates the API cache with
the responses from those logs. This allows re-running code with few
or no new API calls.

Usage:
    python populate_cache_from_logs.py <log_directory> [--cache-file <cache_file>] [--model <model_name>] [--pattern <pattern>]

Arguments:
    log_directory: Directory containing log files (or path to a single log file)
    --cache-file: Path to cache file (default: api_cache.json)
    --model: Model name to use for cache entries (default: from config)
    --pattern: Glob pattern to match log files (default: log*.json)
"""
# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License

import json
import os
import sys
import argparse
import glob
from typing import List, Tuple, Optional
from utils.api_cache import get_cache, set_cached_response
from utils.config import get_config


def parse_log_entry(entry: List) -> Optional[Tuple[str, str, str]]:
    """
    Parse a log entry and extract cache, prompt, and response.
    
    Args:
        entry: Log entry as a list
        
    Returns:
        tuple: (full_cache, query_prompt, result) or None if invalid
    """
    if not isinstance(entry, list) or len(entry) < 3:
        return None
    
    # Handle different log formats:
    # Format 1 (newer): [timestamp, full_cache, query_prompt, result, cache_created, cache_read]
    # Format 2 (older): [timestamp, query_prompt, result]
    
    if len(entry) >= 4:
        # Newer format: combine items 1 and 2 as cache, item 2 is prompt, item 3 is response
        full_cache = entry[1] if entry[1] else ""
        query_prompt = entry[2] if entry[2] else ""
        result = entry[3] if entry[3] else ""
    else:
        # Older format: item 1 is prompt, item 2 is response (no cache)
        full_cache = ""
        query_prompt = entry[1] if entry[1] else ""
        result = entry[2] if entry[2] else ""
    
    # Validate that we have at least a prompt and result
    if not query_prompt or not result:
        return None
    
    return (full_cache, query_prompt, result)


def load_log_file(log_file: str) -> List[Tuple[str, str, str]]:
    """
    Load entries from a log file.
    
    Args:
        log_file: Path to log file
        
    Returns:
        list: List of (full_cache, query_prompt, result) tuples
    """
    entries = []
    
    try:
        with open(log_file, 'r', encoding='utf-8') as f:
            content = f.read().strip()
        
        if not content:
            return entries
        
        # Try to parse as multiple JSON arrays
        # Log files contain multiple JSON arrays concatenated together
        # They might be separated by newlines or by '][' patterns
        
        # Strategy 1: Split by '][' pattern (most reliable for concatenated arrays)
        if '][' in content:
            parts = content.split('][')
            for i, part in enumerate(parts):
                # Clean up the part to make it a valid JSON array
                if i == 0:
                    # First part: might need closing bracket
                    part = part.rstrip()
                    if not part.endswith(']'):
                        part = part + ']'
                elif i == len(parts) - 1:
                    # Last part: might need opening bracket
                    part = part.lstrip()
                    if not part.startswith('['):
                        part = '[' + part
                else:
                    # Middle parts: need both brackets
                    part = '[' + part.strip() + ']'
                
                try:
                    entry = json.loads(part)
                    parsed = parse_log_entry(entry)
                    if parsed:
                        entries.append(parsed)
                except json.JSONDecodeError:
                    # If this fails, try the next strategy
                    continue
        else:
            # Strategy 2: Try to parse as separate JSON arrays on different lines
            # Look for patterns like "[ ... ]" that span multiple lines
            lines = content.split('\n')
            current_entry = []
            bracket_count = 0
            
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                
                # Count brackets to find complete JSON arrays
                bracket_count += line.count('[') - line.count(']')
                current_entry.append(line)
                
                # If bracket count is 0, we have a complete JSON array
                if bracket_count == 0 and current_entry:
                    try:
                        entry_text = '\n'.join(current_entry)
                        entry = json.loads(entry_text)
                        parsed = parse_log_entry(entry)
                        if parsed:
                            entries.append(parsed)
                    except json.JSONDecodeError:
                        pass
                    finally:
                        current_entry = []
            
            # If we still have an unclosed entry, try to parse it anyway
            if current_entry and bracket_count >= 0:
                try:
                    entry_text = '\n'.join(current_entry)
                    # Try to close it if needed
                    if bracket_count > 0:
                        entry_text += ']' * bracket_count
                    entry = json.loads(entry_text)
                    parsed = parse_log_entry(entry)
                    if parsed:
                        entries.append(parsed)
                except json.JSONDecodeError:
                    pass
                
    except FileNotFoundError:
        print(f"Warning: Log file not found: {log_file}")
    except Exception as e:
        print(f"Error loading log file {log_file}: {e}")
    
    return entries


def populate_cache_from_logs(log_path: str, cache_file: str = 'api_cache.json', model_name: Optional[str] = None, pattern: str = 'log*.json'):
    """
    Populate cache from log files.
    
    Args:
        log_path: Path to log directory or single log file
        cache_file: Path to cache file
        model_name: Model name to use (default: from config)
        pattern: Glob pattern to match log files
    """
    # Get model name from config if not provided
    if model_name is None:
        config = get_config()
        current_engine = config.get('current_engine', '')
        model_cfg = config.get('models', {}).get(current_engine, {})
        model_name = model_cfg.get('model', current_engine or 'unknown')
    
    print(f"Using model: {model_name}")
    print(f"Cache file: {cache_file}")
    
    # Get cache instance
    cache = get_cache(cache_file)
    
    # Find log files
    log_files = []
    if os.path.isfile(log_path):
        log_files = [log_path]
    elif os.path.isdir(log_path):
        # Find all log files matching the pattern
        pattern_path = os.path.join(log_path, pattern)
        log_files = glob.glob(pattern_path)
        log_files.sort()  # Sort for consistent processing
    else:
        print(f"Error: {log_path} is not a valid file or directory")
        return
    
    if not log_files:
        print(f"No log files found matching pattern: {pattern}")
        return
    
    print(f"Found {len(log_files)} log file(s)")
    
    # Process each log file
    total_entries = 0
    cached_entries = 0
    skipped_entries = 0
    
    for log_file in log_files:
        print(f"Processing: {log_file}")
        entries = load_log_file(log_file)
        
        for full_cache, query_prompt, result in entries:
            total_entries += 1
            
            # Check if this entry is already in cache
            existing = cache.get_cached_response(full_cache, query_prompt, model_name, 0)
            if existing is not None:
                # Skip if already cached (avoid duplicates)
                skipped_entries += 1
                continue
            
            # Store in cache
            cache.set_cached_response(full_cache, query_prompt, model_name, result, 0)
            cached_entries += 1
    
    # Print statistics
    print(f"\nCache population complete:")
    print(f"  Total entries processed: {total_entries}")
    print(f"  New entries cached: {cached_entries}")
    print(f"  Skipped (already cached): {skipped_entries}")
    print(f"  Total cache size: {cache.get_cache_stats()['size']}")


def main():
    """Main function."""
    parser = argparse.ArgumentParser(
        description='Populate API cache from log files',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Populate cache from all log files in current directory
  python populate_cache_from_logs.py .
  
  # Populate cache from specific log file
  python populate_cache_from_logs.py log0001.json
  
  # Populate cache with specific model name
  python populate_cache_from_logs.py . --model claude-sonnet-4-5
  
  # Use custom cache file
  python populate_cache_from_logs.py . --cache-file my_cache.json
        """
    )
    
    parser.add_argument('log_path', help='Path to log directory or log file')
    parser.add_argument('--cache-file', default='api_cache.json', help='Path to cache file (default: api_cache.json)')
    parser.add_argument('--model', help='Model name to use for cache entries (default: from config)')
    parser.add_argument('--pattern', default='log*.json', help='Glob pattern to match log files (default: log*.json)')
    
    args = parser.parse_args()
    
    populate_cache_from_logs(
        args.log_path,
        cache_file=args.cache_file,
        model_name=args.model,
        pattern=args.pattern
    )


if __name__ == '__main__':
    main()

