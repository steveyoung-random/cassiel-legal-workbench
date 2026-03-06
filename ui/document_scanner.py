"""
Document Scanner for Multi-Parser Support

Scans configured document roots and discovers available source files
using parser registry capabilities.
"""
# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License

import os
from pathlib import Path
from typing import Dict, List, Any, Optional
from parsers.registry import get_registry, load_parsers_from_config
from utils.config import get_config, get_document_roots_config


def scan_for_files(directory: str, extensions: List[str], recursive: bool = True) -> List[str]:
    """
    Scan directory for files with specified extensions.

    Args:
        directory: Path to directory to scan
        extensions: List of file extensions to match (e.g., ['.xml', '.html'])
        recursive: Whether to scan subdirectories recursively

    Returns:
        List of absolute paths to matching files
    """
    files = []
    dir_path = Path(directory)

    if not dir_path.exists():
        return files

    if recursive:
        for ext in extensions:
            files.extend([str(p) for p in dir_path.rglob(f"*{ext}") if p.is_file()])
    else:
        for ext in extensions:
            files.extend([str(p) for p in dir_path.glob(f"*{ext}") if p.is_file()])

    return sorted(files)


def scan_all_document_roots(config: Optional[Dict[str, Any]] = None) -> Dict[str, Dict[str, Any]]:
    """
    Scan all configured document roots.

    Returns available source files grouped by document root.

    Args:
        config: Optional configuration dict (uses get_config() if None)

    Returns:
        Dict mapping document root name to root info:
        {
            'root_name': {
                'display_name': str,
                'parser_type': str,
                'parser_display_name': str,
                'path': str,
                'source_files': List[str],
                'supports_splitting': bool,
                'split_unit_name': Optional[str],
                'parse_mode': str
            }
        }
    """
    if config is None:
        config = get_config()

    # Load parsers from config
    load_parsers_from_config(config)

    registry = get_registry()
    document_roots = get_document_roots_config(config)
    results = {}

    for root_name, root_config in document_roots.items():
        if not root_config.get('enabled', True):
            continue

        parser_type = root_config.get('parser')
        if not parser_type:
            continue

        adapter = registry.get(parser_type)
        if not adapter:
            # Parser not registered - skip this root
            continue

        capabilities = adapter.get_capabilities()
        path = root_config.get('path', '')
        if not path or not os.path.exists(path):
            continue

        extensions = capabilities.file_extensions
        recursive = root_config.get('scan_recursive', True)

        # Find source files
        files = scan_for_files(path, extensions, recursive)

        results[root_name] = {
            'display_name': root_config.get('display_name', root_name),
            'parser_type': parser_type,
            'parser_display_name': capabilities.display_name,
            'path': path,
            'source_files': files,
            'supports_splitting': capabilities.supports_splitting,
            'split_unit_name': capabilities.split_unit_name,
            'split_parent_name': capabilities.split_parent_name,
            'parse_mode': root_config.get('parse_mode', 'auto')
        }

    return results


def get_available_parsers() -> Dict[str, str]:
    """
    Get list of available parsers with their display names.

    Returns:
        Dict mapping parser_type to display_name
    """
    config = get_config()
    load_parsers_from_config(config)

    registry = get_registry()
    capabilities = registry.get_all_capabilities()

    return {
        parser_type: caps.display_name
        for parser_type, caps in capabilities.items()
    }

