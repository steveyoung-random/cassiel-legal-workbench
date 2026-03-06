"""
USLM Parser Adapter

Wraps uslm_set_parse.py to provide ParserAdapter interface.
"""
# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License

from typing import Dict, Any, Optional
import os
import re
from pathlib import Path

from .adapter import ParserAdapter, ParserCapabilities, ParseResult, SplitDetectionResult


class USLMParserAdapter(ParserAdapter):
    """Adapter for USLM parser"""

    def get_capabilities(self) -> ParserCapabilities:
        """Return USLM parser capabilities"""
        return ParserCapabilities(
            parser_type='uslm',
            display_name='US Code (USLM)',
            version='1.0',
            file_extensions=['.xml'],
            supports_splitting=True,
            split_by_default=True,
            split_unit_name='chapter',
            split_parent_name='title',
            output_schema_version='0.3',
            module_name='uslm_set_parse',
            adapter_class='USLMParserAdapter'
        )

    def detect_split_units(self, file_path: str) -> SplitDetectionResult:
        """
        Inspect USLM file to detect titles and chapters.

        Uses existing get_document_structure() from uslm_set_parse.py.
        This is a LIGHTWEIGHT scan - does NOT perform full parsing.
        
        Accepts both file and directory paths (for consistency with parse_file).
        If a directory is provided, finds the first .xml file.
        """
        from uslm_set_parse import get_document_structure

        # Normalize the path to handle trailing slashes (especially on Windows)
        normalized_path = os.path.normpath(file_path)
        
        # Handle directory input - find the first XML file
        actual_file_path = normalized_path
        if os.path.isdir(normalized_path):
            files = os.listdir(normalized_path)
            xml_files = [
                f for f in files
                if os.path.isfile(os.path.join(normalized_path, f)) and
                f.endswith('.xml')
            ]
            if xml_files:
                actual_file_path = os.path.join(normalized_path, xml_files[0])
            else:
                # No XML files found - return can_split=False
                return SplitDetectionResult(
                    can_split=False,
                    split_units=[],
                    parent_units={}
                )
        elif not os.path.isfile(normalized_path):
            # Path doesn't exist - return can_split=False (error will be caught in parse_file)
            return SplitDetectionResult(
                can_split=False,
                split_units=[],
                parent_units={}
            )

        structure = get_document_structure(actual_file_path)

        if not structure or 'titles' not in structure or len(structure['titles']) == 0:
            return SplitDetectionResult(
                can_split=False,
                split_units=[],
                parent_units={}
            )

        # For USLM, typically one title per file
        title_info = structure['titles'][0]
        title_num = title_info['num']
        chapters = title_info.get('chapters', [])

        if len(chapters) == 0:
            # Title has no chapters - cannot split
            return SplitDetectionResult(
                can_split=False,
                split_units=[],
                parent_units={'title': title_num}
            )

        # Has chapters - can split
        split_units = [
            {'title': title_num, 'chapter': chapter_num}
            for chapter_num in chapters
        ]

        return SplitDetectionResult(
            can_split=True,
            split_units=split_units,
            parent_units={'title': title_num}
        )

    def parse_file(
        self,
        file_path: str,
        config: Dict[str, Any],
        params: Optional[Dict[str, Any]] = None
    ) -> ParseResult:
        """
        Parse USLM file using existing uslm_set_parse.py

        Workflow:
        1. Determine parse mode (auto/split/full)
        2. If auto: call detect_split_units() to check if splitting possible
        3. Convert to uslm_set_parse mode ('split_chapters' or 'full_document')
        4. Call existing process_file()
        5. Return ParseResult

        Supports both file paths and directory paths. If a directory is provided,
        it will find the first .xml file in that directory.

        Note: The existing process_file() handles file writing and manifest updates internally.
        We return the parsed content if available, but files_created list would need to be
        extracted from the manifest or returned by process_file() in a future enhancement.
        """
        params = params or {}
        warnings = []

        try:
            from uslm_set_parse import process_file as uslm_process_file

            # Normalize the path to handle trailing slashes (especially on Windows)
            normalized_path = os.path.normpath(file_path)
            
            # Handle directory input - find the first XML file
            actual_file_path = normalized_path
            is_directory_input = os.path.isdir(normalized_path)
            
            if is_directory_input:
                files = os.listdir(normalized_path)
                xml_files = [
                    f for f in files
                    if os.path.isfile(os.path.join(normalized_path, f)) and
                    f.endswith('.xml')
                ]

                if not xml_files:
                    return ParseResult(
                        success=False,
                        parsed_content=None,
                        manifest_entries=[],
                        error_message=f"No XML files found in directory: {normalized_path}",
                        warnings=warnings,
                        files_created=[]
                    )

                # If multiple files, use batch parsing
                if len(xml_files) > 1:
                    warnings.append(f"Found {len(xml_files)} XML files in directory, parsing all of them")
                    return self.parse_directory_batch(normalized_path, config, params)

                # Single file - continue with normal processing
                actual_file_path = os.path.join(normalized_path, xml_files[0])
            elif not os.path.isfile(normalized_path):
                return ParseResult(
                    success=False,
                    parsed_content=None,
                    manifest_entries=[],
                    error_message=f"Input not a directory or file: {normalized_path}",
                    warnings=warnings,
                    files_created=[]
                )

            parse_mode = params.get('parse_mode', 'auto')

            # Determine actual mode
            if parse_mode == 'auto':
                capabilities = self.get_capabilities()
                if capabilities.supports_splitting and capabilities.split_by_default:
                    # Check if THIS document can be split
                    detection = self.detect_split_units(actual_file_path)
                    if detection.can_split:
                        parse_mode = 'split'
                    else:
                        parse_mode = 'full'
                        warnings.append(
                            f"Document cannot be split (no {capabilities.split_unit_name}s found), "
                            f"parsing as full document"
                        )
                else:
                    parse_mode = 'full'

            # Convert to uslm_set_parse format
            uslm_mode = 'split_chapters' if parse_mode == 'split' else 'full_document'

            # Extract specific unit parameters
            specific_units = params.get('specific_units', {})
            title = specific_units.get('title', '')
            chapter = specific_units.get('chapter', '')

            # Check for conflicts before parsing
            try:
                from utils.config import get_output_directory, get_output_structure

                output_dir = get_output_directory(config)
                output_structure = get_output_structure(config)

                file_stem = self.get_file_stem(file_path)
                dir_stem = self.get_directory_stem(file_path)

                if output_structure == 'per_document':
                    doc_output_dir = os.path.join(output_dir, dir_stem)
                else:
                    doc_output_dir = output_dir

                # Create directory if needed for conflict check
                Path(doc_output_dir).mkdir(parents=True, exist_ok=True)

                # Use absolute path for reliable comparison and storage
                actual_file_path_abs = os.path.abspath(actual_file_path)

                skip_parsing, resolved_stem, conflict_status = self.resolve_output_conflict(
                    doc_output_dir, file_stem, actual_file_path_abs
                )

                if skip_parsing:
                    # Already parsed from this source
                    warnings.append(f"File already parsed from source: {actual_file_path_abs}")
                    # Still return success and the existing files
                    from utils.manifest_utils import ManifestManager, get_manifest_path

                    manifest_path = get_manifest_path(doc_output_dir, resolved_stem)
                    files_created = []
                    if os.path.exists(manifest_path):
                        manifest_mgr = ManifestManager(manifest_path)
                        manifest = manifest_mgr.load()
                        for parsed_file in manifest.get('parsed_files', []):
                            file_path_rel = parsed_file.get('file', '')
                            if file_path_rel:
                                abs_path = os.path.join(doc_output_dir, file_path_rel)
                                if os.path.exists(abs_path):
                                    files_created.append(abs_path)

                    return ParseResult(
                        success=True,
                        parsed_content=None,
                        manifest_entries=[],
                        error_message=None,
                        warnings=warnings,
                        files_created=files_created
                    )

                if conflict_status == "conflict":
                    warnings.append(
                        f"Output name conflict detected. Using '{resolved_stem}' instead of original stem to avoid overwriting existing output from different source."
                    )
                    # Note: USLM parser would need to be updated to accept custom file stem
                    # For now, this is a limitation - USLM always uses directory-based naming

            except Exception as e:
                # If conflict detection fails, continue with parsing
                warnings.append(f"Could not check for conflicts: {e}")

            # Call existing parser (handles manifest creation internally)
            # Note: process_file() doesn't return the parsed content, it writes files
            # For now, we'll return success=True and note that files were created
            # In a future enhancement, process_file() could return the parsed content
            uslm_process_file(
                input_file_path=actual_file_path,
                config=config,
                parse_mode=uslm_mode,
                title=title,
                chapter=chapter
            )

            # Try to determine output files from manifest
            # This is a best-effort attempt - the actual files would need to be
            # read from the manifest or returned by process_file()
            files_created = []
            try:
                from utils.config import get_output_directory, get_output_structure
                from utils.manifest_utils import ManifestManager, get_manifest_path

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

                manifest_path = get_manifest_path(doc_output_dir, file_stem)
                if os.path.exists(manifest_path):
                    manifest_mgr = ManifestManager(manifest_path)
                    manifest = manifest_mgr.load()
                    for parsed_file in manifest.get('parsed_files', []):
                        file_path_rel = parsed_file.get('file', '')
                        if file_path_rel:
                            abs_path = os.path.join(doc_output_dir, file_path_rel)
                            if os.path.exists(abs_path):
                                files_created.append(abs_path)
            except Exception:
                # If we can't determine files, that's okay - process_file() handled it
                pass

            return ParseResult(
                success=True,
                parsed_content=None,  # Would need to be returned by process_file() in future
                manifest_entries=[],  # Already handled by uslm_process_file
                error_message=None,
                warnings=warnings,
                files_created=files_created
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

