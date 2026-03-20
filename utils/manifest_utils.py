"""
Manifest File Utilities

This module provides utilities for creating, reading, and updating manifest files
that track parsed documents and their organizational structure.

Manifest files use relative paths for portability - all paths are relative to the
manifest file location. Absolute paths are computed dynamically when needed.
"""
# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License

import hashlib
import json
import os
from datetime import datetime, UTC
from pathlib import Path
from typing import Dict, List, Optional, Any


def compute_source_hash(source_path: str) -> str:
    """
    Return SHA-256 hex digest of the source file bytes.
    Prefix with 'sha256:' for future algorithm agility.
    Returns empty string if file cannot be read.
    """
    try:
        h = hashlib.sha256()
        with open(source_path, 'rb') as f:
            for chunk in iter(lambda: f.read(65536), b''):
                h.update(chunk)
        return f'sha256:{h.hexdigest()}'
    except OSError:
        return ''


class ManifestManager:
    """
    Manages manifest files for tracking parsed documents.

    Manifest files contain metadata about parsed documents including:
    - Source file location (relative to manifest)
    - List of parsed files (paths relative to manifest directory)
    - Document structure (titles, chapters, etc.)
    - Timestamps for tracking updates
    """

    def __init__(self, manifest_path: str):
        """
        Initialize manifest manager.

        Args:
            manifest_path: Path to manifest file (may not exist yet)
        """
        self.manifest_path = Path(manifest_path)
        self.manifest_dir = self.manifest_path.parent

    def create_or_load(self, source_file: str, source_type: str, parser: str, parser_type: Optional[str] = None) -> Dict[str, Any]:
        """
        Create new manifest or load existing one.

        Args:
            source_file: Absolute path to source file
            source_type: Type of source (e.g., "uslm", "formex", "ca_html") - legacy field
            parser: Parser script name (e.g., "uslm_set_parse.py") - legacy field
            parser_type: Parser type identifier (e.g., 'uslm', 'formex', 'ca_html')

        Returns:
            Manifest dictionary
        """
        if self.manifest_path.exists():
            return self.load()
        else:
            return self.create_new(source_file, source_type, parser, parser_type)

    def create_new(self, source_file: str, source_type: str, parser: str, parser_type: Optional[str] = None) -> Dict[str, Any]:
        """
        Create a new manifest.

        Args:
            source_file: Absolute path to source file
            source_type: Type of source (legacy field, kept for backward compatibility)
            parser: Parser script name (legacy field, kept for backward compatibility)
            parser_type: Parser type identifier (e.g., 'uslm', 'formex', 'ca_html')

        Returns:
            New manifest dictionary
        """
        # Compute relative path from manifest to source file
        source_rel = self._make_relative_path(source_file)

        manifest = {
            "source_file": source_rel,
            "source_type": source_type,  # Legacy field
            "parser": parser,  # Legacy field
            "parser_type": parser_type or source_type,  # Use parser_type if provided, fallback to source_type
            "source_hash": compute_source_hash(source_file),
            "parsed_files": [],
            "metadata": {
                "created": datetime.now(UTC).isoformat().replace('+00:00', 'Z'),
                "last_updated": datetime.now(UTC).isoformat().replace('+00:00', 'Z')
            }
        }

        return manifest

    def load(self) -> Dict[str, Any]:
        """
        Load existing manifest from file.

        Returns:
            Manifest dictionary
        """
        with open(self.manifest_path, 'r', encoding='utf-8') as f:
            return json.load(f)

    def update_source_hash(self, manifest: Dict[str, Any], source_file: str) -> None:
        """
        Compute and store the SHA-256 hash of source_file in the manifest.
        Called by parsers after create_or_load() and before save(), so the hash
        always reflects the source file used in the most recent parse run.
        """
        manifest['source_hash'] = compute_source_hash(source_file)

    def update_short_title(self, manifest: Dict[str, Any], short_title: str) -> None:
        """
        Update manifest with short title extracted from parsed content.
        
        Args:
            manifest: Manifest dictionary
            short_title: Short title to store in manifest
        """
        if short_title and short_title.strip():
            manifest['short_title'] = short_title.strip()
    
    def save(self, manifest: Dict[str, Any]) -> None:
        """
        Save manifest to file.

        Args:
            manifest: Manifest dictionary to save
        """
        # Update last_updated timestamp
        if "metadata" not in manifest:
            manifest["metadata"] = {}
        manifest["metadata"]["last_updated"] = datetime.now(UTC).isoformat().replace('+00:00', 'Z')

        # Ensure parent directory exists
        self.manifest_dir.mkdir(parents=True, exist_ok=True)

        # Write manifest
        with open(self.manifest_path, 'w', encoding='utf-8') as f:
            json.dump(manifest, f, indent=2, ensure_ascii=False)

    def add_parsed_file(self, manifest: Dict[str, Any], file_path: str,
                       file_type: str, organizational_units: Optional[Dict[str, str]] = None,
                       **metadata) -> bool:
        """
        Add a parsed file entry to the manifest.

        Args:
            manifest: Manifest dictionary
            file_path: Absolute path to parsed file
            file_type: Type of entry ("full_document", "split_unit", or legacy "chapter")
            organizational_units: Dict of organizational unit values (e.g., {'title': '42', 'chapter': '6A'})
            **metadata: Additional metadata (legacy support - will be merged into entry)

        Returns:
            True if file was added, False if it already exists

        Note:
            - If organizational_units is provided, it will be stored in the entry
            - Legacy metadata fields (title, chapter, etc.) are also supported for backward compatibility
            - Both formats are supported: new code should use organizational_units, old code continues to work
        """
        # Compute path relative to manifest directory
        file_rel = self._make_relative_path(file_path)

        # Check if already exists
        for entry in manifest.get("parsed_files", []):
            if entry.get("file") == file_rel:
                # Update metadata if needed
                if organizational_units:
                    entry["organizational_units"] = organizational_units
                entry.update(metadata)
                return False

        # Add new entry
        entry = {
            "type": file_type,
            "file": file_rel
        }

        # Add organizational_units if provided
        if organizational_units:
            entry["organizational_units"] = organizational_units

        # Add legacy metadata fields (for backward compatibility)
        # These might include title, chapter, etc. as separate fields
        entry.update(metadata)

        if "parsed_files" not in manifest:
            manifest["parsed_files"] = []
        manifest["parsed_files"].append(entry)

        return True

    def get_parsed_files(self, manifest: Dict[str, Any],
                        filter_criteria: Optional[Dict[str, str]] = None) -> List[Dict[str, Any]]:
        """
        Get parsed files from manifest, optionally filtered.

        Args:
            manifest: Manifest dictionary
            filter_criteria: Optional dict of filter criteria (e.g., {"chapter": "6A"})
                           Can filter by:
                           - organizational_units keys (e.g., {"title": "42", "chapter": "6A"})
                           - Legacy top-level fields (e.g., {"title": "42", "chapter": "6A"})
                           - Entry type (e.g., {"type": "full_document"})

        Returns:
            List of parsed file entries with absolute paths

        Note:
            Filter criteria accepts ANY keys - no validation against predefined list.
            This allows filtering by any organizational unit discovered in the manifest.
        """
        results = []

        for entry in manifest.get("parsed_files", []):
            # Check filter criteria
            if filter_criteria:
                match = True
                for key, value in filter_criteria.items():
                    # First check organizational_units dict (new format)
                    org_units = entry.get("organizational_units", {})
                    if key in org_units:
                        if org_units[key] != value:
                            match = False
                            break
                    # Fallback to top-level entry fields (legacy format)
                    elif entry.get(key) != value:
                        match = False
                        break
                if not match:
                    continue

            # Add absolute path
            result = entry.copy()
            result["abs_path"] = str(self.manifest_dir / entry["file"])
            results.append(result)

        return results

    def get_source_file_path(self, manifest: Dict[str, Any]) -> str:
        """
        Get absolute path to source file.

        Args:
            manifest: Manifest dictionary

        Returns:
            Absolute path to source file
        """
        source_rel = manifest.get("source_file", "")
        if not source_rel:
            return ""

        # Compute absolute path from manifest directory + relative path
        abs_path = (self.manifest_dir / source_rel).resolve()
        return str(abs_path)

    def _make_relative_path(self, target_path: str) -> str:
        """
        Make a relative path from manifest directory to target.

        Args:
            target_path: Absolute path to target file

        Returns:
            Relative path from manifest directory to target
        """
        target = Path(target_path).resolve()

        try:
            # Try to compute relative path
            rel_path = target.relative_to(self.manifest_dir)
            return str(rel_path)
        except ValueError:
            # target is not relative to manifest_dir; try .. notation
            try:
                return os.path.relpath(str(target), str(self.manifest_dir))
            except ValueError:
                # Cross-drive on Windows: relative paths are impossible; store absolute path
                return str(target)


def find_manifests(output_dir: str) -> List[str]:
    """
    Find all manifest files in output directory.

    Args:
        output_dir: Path to output directory

    Returns:
        List of absolute paths to manifest files
    """
    output_path = Path(output_dir)
    if not output_path.exists():
        return []

    manifests = list(output_path.rglob("*_manifest.json"))
    return [str(m) for m in manifests]


def parse_filter_string(filter_string: str) -> Dict[str, str]:
    """
    Parse filter string into dictionary.

    Args:
        filter_string: Filter string (e.g., "title=42,chapter=6A")

    Returns:
        Dictionary of filter criteria
    """
    if not filter_string:
        return {}

    criteria = {}
    for part in filter_string.split(','):
        if '=' in part:
            key, value = part.split('=', 1)
            criteria[key.strip()] = value.strip()

    return criteria


def get_manifest_path(output_dir: str, file_stem: str) -> str:
    """
    Get path to manifest file for a given file stem.

    Args:
        output_dir: Path to output directory
        file_stem: File stem (e.g., "usc_title42")

    Returns:
        Absolute path to manifest file
    """
    return str(Path(output_dir) / f"{file_stem}_manifest.json")


def get_parser_from_manifest(manifest: Dict[str, Any]) -> Optional[str]:
    """
    Get parser type identifier from manifest.

    Args:
        manifest: Manifest dictionary

    Returns:
        Parser type identifier (e.g., 'uslm', 'formex', 'ca_html') or None if not found

    Note:
        Checks for 'parser_type' field first (new format), falls back to 'source_type' (legacy).
    """
    # Try new parser_type field first
    parser_type = manifest.get('parser_type')
    if parser_type:
        return parser_type

    # Fallback to legacy source_type field
    source_type = manifest.get('source_type')
    if source_type:
        return source_type

    return None


def discover_parse_files(output_dir: str, filter_criteria: Optional[Dict[str, str]] = None) -> List[Dict[str, Any]]:
    """
    Discover all parsed files in output directory, optionally filtered by criteria.

    Args:
        output_dir: Path to output directory
        filter_criteria: Optional filter dict (e.g., {'title': '42', 'chapter': '6A'})
                       Can filter by any organizational unit keys discovered in manifests.
                       No validation against predefined list - accepts any keys.

    Returns:
        List of dicts with 'parse_file', 'processed_file', 'manifest_path', 'parser_type', and metadata

    Note:
        Uses discovery-based filtering - filter criteria can contain any organizational unit
        keys found in the manifest's organizational_units dicts. No hardcoded validation.
    """
    results = []

    for manifest_path in find_manifests(output_dir):
        manifest_mgr = ManifestManager(manifest_path)
        try:
            manifest = manifest_mgr.load()
        except:
            continue

        # Get parser type from manifest
        parser_type = get_parser_from_manifest(manifest)

        # Get parsed files, optionally filtered
        # Filter criteria accepts any keys - no validation against predefined list
        parsed_files = manifest_mgr.get_parsed_files(manifest, filter_criteria)

        for pf in parsed_files:
            parse_file = pf['abs_path']

            # Derive processed file path
            processed_file = parse_file.replace('_parse_output.json', '_processed.json')

            result = {
                'parse_file': parse_file,
                'processed_file': processed_file,
                'manifest_path': manifest_path,
                'parser_type': parser_type,
                'metadata': {k: v for k, v in pf.items() if k not in ['file', 'abs_path']}
            }

            results.append(result)

    return results


def create_title_output_dir(doc_output_dir: str, title_name: str) -> str:
    """
    Create title-specific output directory within a document output directory.
    
    This function implements the standard pattern used by parsers to organize
    output files into title-specific subfolders (e.g., CFR/Title 15/, USC/Title 42/).
    
    Args:
        doc_output_dir: Base document output directory (e.g., "output/CFR" or "output/USC")
        title_name: Title name extracted by parser (e.g., "Title 15", "Title 42")
                   If empty string, returns doc_output_dir unchanged
    
    Returns:
        Path to use for parsed output files:
        - If title_name provided: doc_output_dir/title_name (created if needed)
        - If title_name empty: doc_output_dir (unchanged)
    
    Example:
        >>> create_title_output_dir("/output/CFR", "Title 15")
        "/output/CFR/Title 15"
        >>> create_title_output_dir("/output/USC", "")
        "/output/USC"
    """
    if title_name:
        parsed_output_dir = os.path.join(doc_output_dir, title_name)
        Path(parsed_output_dir).mkdir(parents=True, exist_ok=True)
        return parsed_output_dir
    else:
        return doc_output_dir
