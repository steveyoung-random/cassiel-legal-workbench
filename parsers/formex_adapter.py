"""
Formex Parser Adapter

Wraps formex_set_parse.py to provide ParserAdapter interface.
"""
# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License

from typing import Dict, Any, Optional
import os
import re
import json
from pathlib import Path

from .adapter import ParserAdapter, ParserCapabilities, ParseResult, SplitDetectionResult


class FormexParserAdapter(ParserAdapter):
    """Adapter for EU Formex parser"""

    def get_capabilities(self) -> ParserCapabilities:
        """Return Formex parser capabilities"""
        return ParserCapabilities(
            parser_type='formex',
            display_name='EU Formex',
            version='1.0',
            file_extensions=['.doc.xml', '.doc.fmx.xml'],
            supports_splitting=False,  # Formex doesn't support splitting
            split_by_default=False,
            split_unit_name=None,
            split_parent_name=None,
            output_schema_version='0.3',
            module_name='formex_set_parse',
            adapter_class='FormexParserAdapter'
        )

    def get_file_stem(self, file_path: str) -> str:
        """
        Determine the output file stem for a Formex file.

        For Formex files:
        - If file_path is a directory: finds the first .doc.xml/.doc.fmx.xml file
          and uses its filename (with extensions stripped)
        - If file_path is a file: strips .doc.xml or .doc.fmx.xml extensions

        This ensures unique stems based on actual filenames rather than parent directories.

        Args:
            file_path: Path to input file or directory

        Returns:
            File stem to use for output files and directories
        """
        # Normalize path
        normalized_path = os.path.normpath(str(file_path).strip())

        # Handle directory input - find the formex file inside
        if os.path.isdir(normalized_path):
            try:
                files = os.listdir(normalized_path)
                formex_files = [
                    f for f in files
                    if os.path.isfile(os.path.join(normalized_path, f)) and
                    (f.endswith('.doc.xml') or f.endswith('.doc.fmx.xml'))
                ]

                if formex_files:
                    # Use the first formex file found
                    file_name = formex_files[0]
                    # Strip formex extensions
                    if file_name.endswith('.doc.fmx.xml'):
                        return file_name[:-len('.doc.fmx.xml')]
                    elif file_name.endswith('.doc.xml'):
                        return file_name[:-len('.doc.xml')]
            except:
                pass

            # Fallback: use directory name
            return os.path.basename(normalized_path)

        # Handle file input - strip formex extensions
        file_name = os.path.basename(normalized_path)

        # Try to strip formex-specific extensions (longest first)
        if file_name.endswith('.doc.fmx.xml'):
            return file_name[:-len('.doc.fmx.xml')]
        elif file_name.endswith('.doc.xml'):
            return file_name[:-len('.doc.xml')]

        # Fallback: strip any extension
        return re.sub(r'\.\w+$', '', file_name)

    def detect_split_units(self, file_path: str) -> SplitDetectionResult:
        """
        Formex parser does not support splitting.
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
        Parse Formex file using existing formex_set_parse.py

        Always does full-document parsing (no splitting).
        
        Supports both file paths and directory paths. If a directory is provided,
        it will find the first .doc.xml or .doc.fmx.xml file in that directory.
        """
        params = params or {}
        warnings = []

        # Validate input
        if not file_path:
            return ParseResult(
                success=False,
                parsed_content=None,
                manifest_entries=[],
                error_message="File path is empty or None",
                warnings=warnings,
                files_created=[]
            )
        
        # Convert to string and strip whitespace
        file_path = str(file_path).strip()
        if not file_path:
            return ParseResult(
                success=False,
                parsed_content=None,
                manifest_entries=[],
                error_message="File path is empty after stripping whitespace",
                warnings=warnings,
                files_created=[]
            )

        # Warn if user requested split mode
        if params.get('parse_mode') == 'split':
            warnings.append("Formex parser does not support splitting, parsing as full document")

        try:
            from formex_set_parse import parse_formex_directive, get_parsing_issues_logfile
            from utils.config import get_output_directory, get_output_structure
            from utils.manifest_utils import ManifestManager

            # Normalize the path to handle trailing slashes (especially on Windows)
            normalized_path = os.path.normpath(str(file_path).strip())
            
            # Try to resolve to absolute path if it's relative
            if not os.path.isabs(normalized_path):
                # It's a relative path - try to resolve it
                abs_path = os.path.abspath(normalized_path)
                if os.path.exists(abs_path):
                    normalized_path = abs_path
                else:
                    # Also try relative to current working directory
                    cwd_path = os.path.join(os.getcwd(), normalized_path)
                    if os.path.exists(cwd_path):
                        normalized_path = os.path.abspath(cwd_path)
            
            # First check if the path exists at all
            if not os.path.exists(normalized_path):
                # Provide detailed error with both original and normalized paths
                error_msg = (
                    f"Path does not exist.\n"
                    f"  Original path: {file_path}\n"
                    f"  Normalized path: {normalized_path}\n"
                    f"  Current working directory: {os.getcwd()}\n"
                    f"  Is absolute: {os.path.isabs(normalized_path)}"
                )
                return ParseResult(
                    success=False,
                    parsed_content=None,
                    manifest_entries=[],
                    error_message=error_msg,
                    warnings=warnings,
                    files_created=[]
                )
            
            # Handle directory input - find the first formex file
            actual_file_path = normalized_path
            is_directory_input = os.path.isdir(normalized_path)
            
            if is_directory_input:
                # It's a directory - find the first formex file
                try:
                    files = os.listdir(normalized_path)
                except PermissionError:
                    return ParseResult(
                        success=False,
                        parsed_content=None,
                        manifest_entries=[],
                        error_message=f"Permission denied accessing directory: {normalized_path}",
                        warnings=warnings,
                        files_created=[]
                    )
                except Exception as e:
                    return ParseResult(
                        success=False,
                        parsed_content=None,
                        manifest_entries=[],
                        error_message=f"Error reading directory {normalized_path}: {str(e)}",
                        warnings=warnings,
                        files_created=[]
                    )
                
                formex_files = [
                    f for f in files
                    if os.path.isfile(os.path.join(normalized_path, f)) and
                    (f.endswith('.doc.xml') or f.endswith('.doc.fmx.xml'))
                ]

                if not formex_files:
                    return ParseResult(
                        success=False,
                        parsed_content=None,
                        manifest_entries=[],
                        error_message=f"No formex files (.doc.xml or .doc.fmx.xml) found in directory: {normalized_path}",
                        warnings=warnings,
                        files_created=[]
                    )

                # If multiple files, use batch parsing
                if len(formex_files) > 1:
                    warnings.append(f"Found {len(formex_files)} formex files in directory, parsing all of them")
                    return self.parse_directory_batch(normalized_path, config, params)

                # Single file - continue with normal processing
                actual_file_path = os.path.join(normalized_path, formex_files[0])
            elif os.path.isfile(normalized_path):
                # It's a file - use it directly
                actual_file_path = normalized_path
            else:
                # Path exists but is neither a file nor a directory (shouldn't happen, but handle it)
                return ParseResult(
                    success=False,
                    parsed_content=None,
                    manifest_entries=[],
                    error_message=f"Path exists but is neither a file nor a directory: {normalized_path}",
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

            # Verify the actual file path exists and is a file before parsing
            if not os.path.exists(actual_file_path):
                return ParseResult(
                    success=False,
                    parsed_content=None,
                    manifest_entries=[],
                    error_message=f"File not found: {actual_file_path}",
                    warnings=warnings,
                    files_created=[]
                )
            
            if not os.path.isfile(actual_file_path):
                return ParseResult(
                    success=False,
                    parsed_content=None,
                    manifest_entries=[],
                    error_message=f"Path is not a file: {actual_file_path}",
                    warnings=warnings,
                    files_created=[]
                )

            # Parse the file
            try:
                parsed_content = parse_formex_directive(actual_file_path, parsing_logfile)
            except Exception as parse_error:
                # Catch any errors from parse_formex_directive and provide detailed error message
                error_details = f"Error parsing file {actual_file_path}: {str(parse_error)}"
                return ParseResult(
                    success=False,
                    parsed_content=None,
                    manifest_entries=[],
                    error_message=error_details,
                    warnings=warnings,
                    files_created=[]
                )

            if not parsed_content or parsed_content == {}:
                return ParseResult(
                    success=False,
                    parsed_content=None,
                    manifest_entries=[],
                    error_message=f"Parsing returned empty content for file: {actual_file_path}",
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
                source_type='formex',  # Legacy field
                parser='formex_set_parse.py',  # Legacy field
                parser_type='formex'  # New field
            )

            # Add parsed file entry
            # Formex always does full document parsing (no splitting)
            manifest_mgr.add_parsed_file(
                manifest=manifest,
                file_path=output_path,
                file_type='full_document',
                organizational_units={}  # Formex full document has no split units
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
            # Provide detailed error information including the original path
            error_msg = f"Unexpected error processing path '{file_path}': {str(e)}"
            import traceback
            error_msg += f"\nTraceback: {traceback.format_exc()}"
            return ParseResult(
                success=False,
                parsed_content=None,
                manifest_entries=[],
                error_message=error_msg,
                warnings=warnings,
                files_created=[]
            )

