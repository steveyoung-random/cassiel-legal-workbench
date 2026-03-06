"""
API Cache System

This module provides caching functionality for AI API calls to avoid
duplicate API requests and reduce costs.

The cache stores responses keyed by a SHA256 hash of:
- Full cache content (concatenated cache_prompt_list)
- Query prompt
- Model name
- Max tokens

Cache entries store only the response, since the cache key (hash) already
uniquely identifies all request parameters. This minimizes cache file size
while maintaining full functionality.
"""
# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License

import json
import os
import hashlib
import shutil
import tempfile
import time
import glob
from datetime import datetime
from typing import Optional, Dict, Any


class APICache:
    """
    Cache for AI API responses.
    
    Stores responses in a JSON file keyed by a SHA256 hash of the request parameters
    (full_cache, query_prompt, model_name, max_tokens). Each cache entry contains
    only the response, since the cache key uniquely identifies the request.
    
    Supports cache consolidation by reading from an old cache file while
    writing only to the main cache file. Old-format entries are automatically
    normalized to the optimized format when promoted from old cache.
    """
    
    def __init__(self, cache_file: str = 'api_cache.json', old_cache_file: Optional[str] = None):
        """
        Initialize the cache.
        
        Args:
            cache_file: Path to the cache file (default: 'api_cache.json')
            old_cache_file: Optional path to an old cache file for consolidation.
                          If None, will auto-detect files matching api_cache_*.json pattern.
        """
        self.cache_file = cache_file
        self.old_cache_file: Optional[str] = old_cache_file
        self.cache: Dict[str, Dict[str, Any]] = {}
        self.old_cache: Dict[str, Dict[str, Any]] = {}
        
        # Auto-detect old cache file if not provided
        if self.old_cache_file is None:
            self.old_cache_file = self._detect_old_cache_file()
        
        self.load_cache()
    
    def _detect_old_cache_file(self) -> Optional[str]:
        """
        Auto-detect old cache files matching api_cache_*.json pattern.
        
        Returns:
            Path to the first matching old cache file, or None if none found.
        """
        cache_dir = os.path.dirname(self.cache_file) or '.'
        cache_basename = os.path.basename(self.cache_file)
        
        # Pattern to match: api_cache_*.json (but not the main cache file)
        pattern = os.path.join(cache_dir, 'api_cache_*.json')
        matches = glob.glob(pattern)
        
        # Filter out the main cache file
        matches = [m for m in matches if os.path.basename(m) != cache_basename]
        
        if matches:
            # Return the first match (could be sorted by modification time if desired)
            return matches[0]
        
        return None
    
    def _generate_cache_key(self, full_cache: str, query_prompt: str, model_name: str, max_tokens: int = 0) -> str:
        """
        Generate a cache key from request parameters.
        
        Args:
            full_cache: Concatenated cache_prompt_list content
            query_prompt: The query prompt
            model_name: Model name (e.g., 'claude-sonnet-4-5')
            max_tokens: Maximum tokens (for key generation, but response is model-agnostic)
            
        Returns:
            str: SHA256 hash of the request parameters
        """
        # Create a string representation of the request
        request_string = f"{full_cache}\n\n---QUERY---\n\n{query_prompt}\n\n---MODEL---\n\n{model_name}\n\n---MAX_TOKENS---\n\n{max_tokens}"
        
        # Generate hash
        hash_obj = hashlib.sha256(request_string.encode('utf-8'))
        return hash_obj.hexdigest()
    
    def get_cached_response(self, full_cache: str, query_prompt: str, model_name: str, max_tokens: int = 0) -> Optional[str]:
        """
        Get a cached response if available.
        
        Checks both the main cache and old cache file. If found in old cache,
        the entry is automatically promoted to the main cache for consolidation.
        
        Args:
            full_cache: Concatenated cache_prompt_list content
            query_prompt: The query prompt
            model_name: Model name
            max_tokens: Maximum tokens
            
        Returns:
            str: Cached response if found, None otherwise
        """
        cache_key = self._generate_cache_key(full_cache, query_prompt, model_name, max_tokens)
        
        # Check main cache first
        if cache_key in self.cache:
            entry = self.cache[cache_key]
            # Works with both old format (full fields) and new format (response only)
            return entry.get('response')
        
        # Check old cache if available
        if cache_key in self.old_cache:
            entry = self.old_cache[cache_key]
            response = entry.get('response')
            
            # Promote entry from old cache to main cache for consolidation
            # Normalize to new format (response only) when promoting
            if response is not None:
                self.cache[cache_key] = {'response': response}
                self.save_cache()
            
            return response
        
        return None
    
    def set_cached_response(self, full_cache: str, query_prompt: str, model_name: str, response: str, max_tokens: int = 0):
        """
        Store a response in the cache.
        
        Only writes to the main cache file. If the entry is already in the main cache,
        skips the write to avoid unnecessary file operations.
        
        The cache stores only the response, since the cache key (hash) already uniquely
        identifies the request parameters (full_cache, query_prompt, model_name, max_tokens).
        
        Args:
            full_cache: Concatenated cache_prompt_list content
            query_prompt: The query prompt
            model_name: Model name
            response: The response to cache
            max_tokens: Maximum tokens
        """
        cache_key = self._generate_cache_key(full_cache, query_prompt, model_name, max_tokens)
        
        # Only write if not already in main cache (avoids unnecessary writes)
        if cache_key not in self.cache:
            # Store only the response - the cache key already contains all request parameters
            self.cache[cache_key] = {
                'response': response
            }
            
            # Save cache after each addition (for persistence)
            self.save_cache()
    
    def _load_cache_file(self, cache_file_path: str) -> Dict[str, Dict[str, Any]]:
        """
        Load a cache file and return its contents.
        
        Args:
            cache_file_path: Path to the cache file to load
            
        Returns:
            Dictionary of cache entries
            
        Raises:
            RuntimeError: If the cache file is corrupted (only for main cache)
        """
        if os.path.exists(cache_file_path):
            try:
                with open(cache_file_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except (json.JSONDecodeError, IOError) as e:
                # For main cache, create backup and stop execution
                if cache_file_path == self.cache_file:
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    backup_file = f"{cache_file_path}.corrupted.{timestamp}.bak"

                    try:
                        shutil.copy2(cache_file_path, backup_file)
                        print(f"\nERROR: Cache file is corrupted or unreadable.")
                        print(f"Error details: {e}")
                        print(f"\nA backup has been saved to: {backup_file}")
                        print(f"\nTo prevent data loss, execution has been stopped.")
                        print(f"\nRecommended actions:")
                        print(f"1. Try to recover the cache file manually (it may be fixable)")
                        print(f"2. If recovery fails, rename or delete the corrupted file to start fresh")
                        print(f"3. You can use populate_cache_from_logs.py to rebuild cache from log files")
                    except Exception as backup_error:
                        print(f"\nERROR: Cache file is corrupted AND backup failed!")
                        print(f"Cache error: {e}")
                        print(f"Backup error: {backup_error}")
                        print(f"\nManually backup {cache_file_path} before proceeding!")

                    # Stop execution - DO NOT overwrite the cache file
                    raise RuntimeError(
                        f"Cache file {cache_file_path} is corrupted. "
                        f"See error messages above for recovery options."
                    )
                else:
                    # For old cache, just log a warning and return empty dict
                    print(f"Warning: Old cache file {cache_file_path} is corrupted or unreadable: {e}")
                    print(f"Continuing without old cache entries.")
                    return {}
        else:
            return {}
    
    def load_cache(self):
        """
        Load cache from both main and old cache files.

        If the main cache file is corrupted, creates a backup and stops execution
        to prevent data loss. If the old cache file is corrupted, logs a warning
        and continues without it.
        """
        # Load main cache
        self.cache = self._load_cache_file(self.cache_file)
        
        # Load old cache if specified
        if self.old_cache_file:
            self.old_cache = self._load_cache_file(self.old_cache_file)
            if self.old_cache:
                print(f"Loaded {len(self.old_cache)} entries from old cache file: {self.old_cache_file}")
    
    def save_cache(self):
        """
        Save cache to file using atomic write operation.

        This uses a temporary file and atomic rename to prevent corruption
        if the program is interrupted during writing.

        On Windows, retries with exponential backoff if the file is locked
        by another process.
        """
        max_retries = 5
        retry_delay = 0.1  # Start with 100ms

        for attempt in range(max_retries):
            try:
                # Create directory if it doesn't exist
                cache_dir = os.path.dirname(self.cache_file) or '.'
                if not os.path.exists(cache_dir):
                    os.makedirs(cache_dir, exist_ok=True)

                # Write to a temporary file first (atomic operation)
                temp_fd, temp_path = tempfile.mkstemp(
                    dir=cache_dir,
                    prefix='.api_cache_tmp_',
                    suffix='.json',
                    text=True
                )

                try:
                    with os.fdopen(temp_fd, 'w', encoding='utf-8') as f:
                        json.dump(self.cache, f, indent=2, ensure_ascii=False)

                    # Atomic rename (on most systems, this is atomic even if interrupted)
                    # On Windows, need to remove target first if it exists
                    if os.name == 'nt' and os.path.exists(self.cache_file):
                        # Windows-specific: try to remove with retry
                        for remove_attempt in range(3):
                            try:
                                os.remove(self.cache_file)
                                break
                            except PermissionError:
                                if remove_attempt < 2:
                                    time.sleep(0.05)  # 50ms wait
                                else:
                                    raise

                    shutil.move(temp_path, self.cache_file)
                    # Success - return immediately
                    return

                except Exception as e:
                    # Clean up temp file if something goes wrong
                    if os.path.exists(temp_path):
                        try:
                            os.remove(temp_path)
                        except:
                            pass  # Ignore cleanup errors
                    raise e

            except (IOError, OSError, PermissionError) as e:
                # If this is a "file in use" error and we have retries left
                if attempt < max_retries - 1:
                    if os.name == 'nt' and isinstance(e, (PermissionError, OSError)):
                        # Windows file locking - retry with backoff
                        time.sleep(retry_delay)
                        retry_delay *= 2  # Exponential backoff
                        continue

                # Last attempt failed or non-retryable error
                print(f"Warning: Failed to save cache to {self.cache_file}: {e}")
                return
    
    def clear_cache(self):
        """Clear all cache entries."""
        self.cache = {}
        self.save_cache()

    def remove_cache_entry(self, full_cache: str, query_prompt: str, model_name: str, max_tokens: int = 0) -> bool:
        """
        Remove a specific cache entry.

        Args:
            full_cache: Concatenated cache_prompt_list content
            query_prompt: The query prompt
            model_name: Model name
            max_tokens: Maximum tokens

        Returns:
            bool: True if entry was removed, False if not found
        """
        cache_key = self._generate_cache_key(full_cache, query_prompt, model_name, max_tokens)

        if cache_key in self.cache:
            del self.cache[cache_key]
            self.save_cache()
            return True

        return False

    def get_cache_stats(self) -> Dict[str, Any]:
        """
        Get cache statistics.
        
        Returns:
            dict: Cache statistics including size, cache file paths, and old cache info
        """
        stats = {
            'size': len(self.cache),
            'cache_file': self.cache_file
        }
        
        if self.old_cache_file:
            stats['old_cache_file'] = self.old_cache_file
            stats['old_cache_size'] = len(self.old_cache)
        
        return stats


# Global cache instance
_global_cache: Optional[APICache] = None


def get_cache(cache_file: str = 'api_cache.json', old_cache_file: Optional[str] = None) -> APICache:
    """
    Get the global cache instance.
    
    Args:
        cache_file: Path to the cache file
        old_cache_file: Optional path to an old cache file for consolidation.
                       If None, will auto-detect files matching api_cache_*.json pattern.
        
    Returns:
        APICache: The global cache instance
    """
    global _global_cache
    if _global_cache is None:
        _global_cache = APICache(cache_file, old_cache_file)
    return _global_cache


def set_cache_file(cache_file: str, old_cache_file: Optional[str] = None):
    """
    Set the cache file path and reinitialize the global cache.
    
    Args:
        cache_file: Path to the cache file
        old_cache_file: Optional path to an old cache file for consolidation.
                       If None, will auto-detect files matching api_cache_*.json pattern.
    """
    global _global_cache
    _global_cache = APICache(cache_file, old_cache_file)


def get_cached_response(full_cache: str, query_prompt: str, model_name: str, max_tokens: int = 0, cache_file: str = 'api_cache.json', old_cache_file: Optional[str] = None) -> Optional[str]:
    """
    Get a cached response if available.
    
    Args:
        full_cache: Concatenated cache_prompt_list content
        query_prompt: The query prompt
        model_name: Model name
        max_tokens: Maximum tokens
        cache_file: Path to the cache file
        old_cache_file: Optional path to an old cache file for consolidation.
                       If None, will auto-detect files matching api_cache_*.json pattern.
        
    Returns:
        str: Cached response if found, None otherwise
    """
    cache = get_cache(cache_file, old_cache_file)
    return cache.get_cached_response(full_cache, query_prompt, model_name, max_tokens)


def set_cached_response(full_cache: str, query_prompt: str, model_name: str, response: str, max_tokens: int = 0, cache_file: str = 'api_cache.json', old_cache_file: Optional[str] = None):
    """
    Store a response in the cache.

    Args:
        full_cache: Concatenated cache_prompt_list content
        query_prompt: The query prompt
        model_name: Model name
        response: The response to cache
        max_tokens: Maximum tokens
        cache_file: Path to the cache file
        old_cache_file: Optional path to an old cache file for consolidation.
                       If None, will auto-detect files matching api_cache_*.json pattern.
    """
    cache = get_cache(cache_file, old_cache_file)
    cache.set_cached_response(full_cache, query_prompt, model_name, response, max_tokens)


def remove_cached_response(full_cache: str, query_prompt: str, model_name: str, max_tokens: int = 0, cache_file: str = 'api_cache.json', old_cache_file: Optional[str] = None) -> bool:
    """
    Remove a specific cached response.

    Args:
        full_cache: Concatenated cache_prompt_list content
        query_prompt: The query prompt
        model_name: Model name
        max_tokens: Maximum tokens
        cache_file: Path to the cache file
        old_cache_file: Optional path to an old cache file for consolidation.
                       If None, will auto-detect files matching api_cache_*.json pattern.

    Returns:
        bool: True if entry was removed, False if not found
    """
    cache = get_cache(cache_file, old_cache_file)
    return cache.remove_cache_entry(full_cache, query_prompt, model_name, max_tokens)

