"""
California HTML Parser Adapter

Wraps CA_parse_set.py to provide ParserAdapter interface.
"""
# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License

from typing import Dict, Any, Optional
import os
import re
import json
from pathlib import Path

from .adapter import ParserAdapter, ParserCapabilities, ParseResult, SplitDetectionResult


class CAHtmlParserAdapter(ParserAdapter):
    """Adapter for California Code HTML parser"""

    def get_capabilities(self) -> ParserCapabilities:
        """Return CA HTML parser capabilities"""
        return ParserCapabilities(
            parser_type='ca_html',
            display_name='California Code (HTML)',
            version='1.0',
            file_extensions=['.html'],
            supports_splitting=False,  # CA parser doesn't support splitting
            split_by_default=False,
            split_unit_name=None,
            split_parent_name=None,
            output_schema_version='0.3',
            module_name='CA_parse_set',
            adapter_class='CAHtmlParserAdapter'
        )

    def detect_split_units(self, file_path: str) -> SplitDetectionResult:
        """
        CA HTML parser does not support splitting.
        Always returns can_split=False.
        
        Accepts both file and directory paths (for consistency with parse_file).
        """
        # Validate that the path exists (file or directory)
        if not os.path.exists(file_path):
            # Return a result indicating no splitting (but path doesn't exist)
            # The actual error will be caught in parse_file
            return SplitDetectionResult(
                can_split=False,
                split_units=[],
                parent_units={}
            )
        
        return SplitDetectionResult(
            can_split=False,
            split_units=[],
            parent_units={}
        )

    def parse_file(
        self,
        file_path: str,
        config: Dict[str, Any],
        params: Optional[Dict[str, Any]] = None
    ) -> ParseResult:
        """
        Parse CA HTML file using existing CA_parse_set.py

        Always does full-document parsing (no splitting).
        
        Supports both file paths and directory paths. If a directory is provided,
        it will find the first .html file in that directory.
        """
        params = params or {}
        warnings = []

        # Warn if user requested split mode
        if params.get('parse_mode') == 'split':
            warnings.append("CA HTML parser does not support splitting, parsing as full document")

        try:
            from CA_parse_set import parse_html, get_parsing_issues_logfile
            from utils.config import get_output_directory, get_output_structure
            from utils.manifest_utils import ManifestManager

            # Normalize the path to handle trailing slashes (especially on Windows)
            normalized_path = os.path.normpath(file_path)
            
            # Handle directory input - find the first HTML file
            actual_file_path = normalized_path
            is_directory_input = os.path.isdir(normalized_path)
            
            if is_directory_input:
                files = os.listdir(normalized_path)
                html_files = [
                    f for f in files
                    if os.path.isfile(os.path.join(normalized_path, f)) and
                    f.endswith('.html')
                ]

                if not html_files:
                    return ParseResult(
                        success=False,
                        parsed_content=None,
                        manifest_entries=[],
                        error_message=f"No HTML files found in directory: {normalized_path}",
                        warnings=warnings,
                        files_created=[]
                    )

                # If multiple files, use batch parsing
                if len(html_files) > 1:
                    warnings.append(f"Found {len(html_files)} HTML files in directory, parsing all of them")
                    return self.parse_directory_batch(normalized_path, config, params)

                # Single file - continue with normal processing
                actual_file_path = os.path.join(normalized_path, html_files[0])
            elif not os.path.isfile(normalized_path):
                return ParseResult(
                    success=False,
                    parsed_content=None,
                    manifest_entries=[],
                    error_message=f"Input not a directory or file: {normalized_path}",
                    warnings=warnings,
                    files_created=[]
                )

            # Get output directory from config
            output_dir = get_output_directory(config)
            output_structure = get_output_structure(config)

            # Determine file stem and directory stem
            # file_stem: Used for output filenames (based on input filename)
            # dir_stem: Used for output directory (based on parent directory)
            file_stem = self.get_file_stem(file_path)
            dir_stem = self.get_directory_stem(file_path)

            if output_structure == 'per_document':
                doc_output_dir = os.path.join(output_dir, dir_stem)
            else:
                doc_output_dir = output_dir

            # Check for naming conflicts before parsing
            # Create directory first so we can check for manifests
            Path(doc_output_dir).mkdir(parents=True, exist_ok=True)

            # Use absolute path for reliable comparison and storage
            actual_file_path_abs = os.path.abspath(actual_file_path)

            skip_parsing, file_stem, conflict_status = self.resolve_output_conflict(
                doc_output_dir, file_stem, actual_file_path_abs
            )

            if skip_parsing:
                # Already parsed from this source
                output_path = os.path.join(doc_output_dir, f'{file_stem}_parse_output.json')
                manifest_path = os.path.join(doc_output_dir, f'{file_stem}_manifest.json')
                warnings.append(f"File already parsed from source: {actual_file_path_abs}")
                return ParseResult(
                    success=True,
                    parsed_content=None,  # Don't re-read the file
                    manifest_entries=[],
                    error_message=None,
                    warnings=warnings,
                    files_created=[output_path, manifest_path]
                )

            if conflict_status == "conflict":
                warnings.append(
                    f"Output name conflict detected. Using '{file_stem}' instead of original stem to avoid overwriting existing output from different source."
                )

            # Get parsing logfile
            parsing_logfile = get_parsing_issues_logfile(doc_output_dir)

            # Parse the file
            parsed_content = parse_html(actual_file_path, parsing_logfile)

            if not parsed_content or parsed_content == {}:
                return ParseResult(
                    success=False,
                    parsed_content=None,
                    manifest_entries=[],
                    error_message="Parsing returned empty content",
                    warnings=warnings,
                    files_created=[]
                )

            # Write output file
            output_filename = f'{file_stem}_parse_output.json'
            output_path = os.path.join(doc_output_dir, output_filename)

            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(parsed_content, f, indent=4, ensure_ascii=False)

            # Create/update manifest file
            manifest_path = os.path.join(doc_output_dir, f'{file_stem}_manifest.json')
            manifest_mgr = ManifestManager(manifest_path)

            # Create or load manifest (use absolute path for source_file)
            manifest = manifest_mgr.create_or_load(
                source_file=actual_file_path_abs,
                source_type='ca_html',  # Legacy field
                parser='CA_parse_set.py',  # Legacy field
                parser_type='ca_html'  # New field
            )

            # Add parsed file entry
            # CA HTML always does full document parsing (no splitting)
            manifest_mgr.add_parsed_file(
                manifest=manifest,
                file_path=output_path,
                file_type='full_document',
                organizational_units={}  # CA HTML full document has no split units
            )

            # Save manifest
            manifest_mgr.save(manifest)

            return ParseResult(
                success=True,
                parsed_content=parsed_content,
                manifest_entries=[],  # Manifest already saved directly
                error_message=None,
                warnings=warnings,
                files_created=[output_path, manifest_path]
            )

        except Exception as e:
            return ParseResult(
                success=False,
                parsed_content=None,
                manifest_entries=[],
                error_message=str(e),
                warnings=warnings,
                files_created=[]
            )

