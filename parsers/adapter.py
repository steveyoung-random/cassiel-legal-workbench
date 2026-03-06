"""
Parser Adapter Interface

Defines the abstract base class and data structures for parser adapters.
All concrete parsers must implement the ParserAdapter interface.
"""
# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, Any, Optional, List


@dataclass
class ParserCapabilities:
    """
    Parser capabilities - declares ONLY splitting info, not organizational structure.

    Organizational units are discovered during parsing, not predefined.
    This class only contains what the parser CAN do (splitting support),
    not what structure documents have.
    """

    # Basic identification
    parser_type: str              # 'uslm', 'formex', 'ca_html'
    display_name: str             # 'US Code (USLM)', 'EU Formex'
    version: str                  # '1.0'

    # File handling
    file_extensions: List[str]    # ['.xml'], ['.doc.xml']

    # Splitting capabilities (parser-specific terminology)
    supports_splitting: bool           # Can this parser split documents?
    split_by_default: bool             # Should it split by default (when possible)?
    split_unit_name: Optional[str]     # For THIS parser: "chapter", "part", etc.
    split_parent_name: Optional[str]   # For THIS parser: "title", "regulation", etc.

    # Output format
    output_schema_version: str    # '0.3'

    # Module information
    module_name: str              # 'uslm_set_parse'
    adapter_class: str            # 'USLMParserAdapter'


@dataclass
class SplitDetectionResult:
    """
    Result of split detection for a specific document.

    This is the result of inspecting a document to see if it can be split
    (lightweight scan, not full parsing).
    """
    can_split: bool                         # Can this document be split?
    split_units: List[Dict[str, str]]      # e.g., [{'title': '42', 'chapter': '6A'}, ...]
    parent_units: Dict[str, str]           # e.g., {'title': '42'}


@dataclass
class ParseResult:
    """
    Result of parsing operation.

    Returned by ParserAdapter.parse_file().
    """
    success: bool
    parsed_content: Optional[Dict[str, Any]]
    manifest_entries: List[Dict[str, Any]]  # Entries to add to manifest
    error_message: Optional[str]
    warnings: List[str]
    files_created: List[str]  # Paths to files created


class ParserAdapter(ABC):
    """
    Abstract base class for parser adapters.

    All concrete parsers must implement this interface.
    Adapters are typically thin wrappers around existing parser functions.
    """

    @abstractmethod
    def get_capabilities(self) -> ParserCapabilities:
        """
        Return parser capabilities.

        Returns:
            ParserCapabilities describing what this parser can do
        """
        pass

    @abstractmethod
    def detect_split_units(self, file_path: str) -> SplitDetectionResult:
        """
        Inspect document to detect splittable units.

        This is a lightweight scan - does NOT perform full parsing.
        Used to check if a specific document can be split before parsing.

        For USLM: Checks if document has title with multiple chapters
        For EU: Checks if document has regulation with multiple parts
        etc.

        Args:
            file_path: Path to source file

        Returns:
            SplitDetectionResult with can_split flag and list of discovered units
        """
        pass

    @abstractmethod
    def parse_file(
        self,
        file_path: str,
        config: Dict[str, Any],
        params: Optional[Dict[str, Any]] = None
    ) -> ParseResult:
        """
        Parse a file and return results.

        Args:
            file_path: Path to source file
            config: Configuration dictionary
            params: Optional parsing parameters:
                - parse_mode: 'split', 'full', or 'auto'
                - specific_units: Dict[str, str] (e.g., {'title': '42', 'chapter': '6A'})
                - output_dir: Override default output directory

        Returns:
            ParseResult with parsed content and manifest entries

        Note:
            The parsed_content structure MUST conform to output schema v0.3.
            The organizational structure is discovered during parsing and stored
            in the nested organization.content dict.
        """
        pass

    def supports_splitting(self) -> bool:
        """
        Whether this parser supports document splitting.

        Returns:
            True if parser supports splitting, False otherwise
        """
        return self.get_capabilities().supports_splitting

    def parse_directory_batch(
        self,
        directory_path: str,
        config: Dict[str, Any],
        params: Optional[Dict[str, Any]] = None
    ) -> ParseResult:
        """
        Parse all matching files in a directory (batch processing).

        Finds all files in the directory that match this parser's file extensions,
        then calls parse_file() for each one individually. Aggregates the results.

        Args:
            directory_path: Path to directory containing files to parse
            config: Configuration dictionary
            params: Optional parsing parameters

        Returns:
            ParseResult with aggregated results from all files
        """
        import os

        # Normalize path
        normalized_path = os.path.normpath(str(directory_path).strip())

        if not os.path.isdir(normalized_path):
            return ParseResult(
                success=False,
                parsed_content=None,
                manifest_entries=[],
                error_message=f"Not a directory: {normalized_path}",
                warnings=[],
                files_created=[]
            )

        # Find all matching files
        capabilities = self.get_capabilities()
        extensions = capabilities.file_extensions

        matching_files = []
        try:
            files = os.listdir(normalized_path)
            for f in sorted(files):  # Sort for consistent ordering
                if os.path.isfile(os.path.join(normalized_path, f)):
                    for ext in extensions:
                        if f.endswith(ext):
                            matching_files.append(os.path.join(normalized_path, f))
                            break  # Only add once even if matches multiple extensions
        except Exception as e:
            return ParseResult(
                success=False,
                parsed_content=None,
                manifest_entries=[],
                error_message=f"Error listing directory: {e}",
                warnings=[],
                files_created=[]
            )

        if not matching_files:
            return ParseResult(
                success=False,
                parsed_content=None,
                manifest_entries=[],
                error_message=f"No matching files found in directory: {normalized_path} (looking for: {', '.join(extensions)})",
                warnings=[],
                files_created=[]
            )

        # Parse each file
        all_warnings = []
        all_files_created = []
        all_manifest_entries = []
        success_count = 0
        failed_files = []

        for file_path in matching_files:
            result = self.parse_file(file_path, config, params)

            if result.success:
                success_count += 1
            else:
                failed_files.append((os.path.basename(file_path), result.error_message))

            # Aggregate results
            all_warnings.extend(result.warnings)
            all_files_created.extend(result.files_created)
            all_manifest_entries.extend(result.manifest_entries)

        # Prepare summary
        summary_msg = f"Batch parse: {success_count}/{len(matching_files)} files successful"
        if failed_files:
            summary_msg += f", {len(failed_files)} failed"
            for filename, error in failed_files:
                all_warnings.append(f"Failed to parse {filename}: {error}")

        # Overall success if at least one file succeeded
        overall_success = success_count > 0

        return ParseResult(
            success=overall_success,
            parsed_content=None,  # No single parsed content for batch
            manifest_entries=all_manifest_entries,
            error_message=None if overall_success else "All files failed to parse",
            warnings=all_warnings,
            files_created=all_files_created
        )

    def resolve_output_conflict(self, output_dir: str, file_stem: str, source_file: str) -> tuple:
        """
        Check for naming conflicts and resolve them.

        Checks if a manifest already exists for the given file_stem. If it does:
        - If it's from the same source file: returns (True, file_stem, "already_parsed")
        - If it's from a different source: returns (False, unique_stem, "conflict")

        If no manifest exists: returns (False, file_stem, "no_conflict")

        Args:
            output_dir: Output directory path
            file_stem: Proposed file stem
            source_file: Absolute path to source file

        Returns:
            tuple: (skip_parsing, resolved_stem, status)
                - skip_parsing: True if already parsed from same source
                - resolved_stem: File stem to use (may have suffix if conflict)
                - status: "already_parsed", "conflict", or "no_conflict"
        """
        import os
        from utils.manifest_utils import ManifestManager

        manifest_path = os.path.join(output_dir, f'{file_stem}_manifest.json')

        # No manifest exists, no conflict
        if not os.path.exists(manifest_path):
            return (False, file_stem, "no_conflict")

        # Manifest exists, check if it's from the same source
        try:
            manifest_mgr = ManifestManager(manifest_path)
            manifest = manifest_mgr.load()

            # Get absolute path to source file from manifest
            # This properly handles relative->absolute conversion
            manifest_source = manifest_mgr.get_source_file_path(manifest)

            # Normalize both paths for comparison (ensure absolute)
            normalized_source = os.path.abspath(os.path.normpath(source_file))

            # Same source: already parsed
            if manifest_source and os.path.normpath(manifest_source) == normalized_source:
                return (True, file_stem, "already_parsed")

            # Different source: conflict, need unique name
            # Add numeric suffix to make unique
            counter = 2
            unique_stem = file_stem
            while True:
                unique_stem = f"{file_stem}_{counter}"
                unique_manifest_path = os.path.join(output_dir, f'{unique_stem}_manifest.json')
                if not os.path.exists(unique_manifest_path):
                    return (False, unique_stem, "conflict")

                # Check if this one matches our source
                try:
                    unique_mgr = ManifestManager(unique_manifest_path)
                    unique_manifest = unique_mgr.load()
                    unique_source = unique_mgr.get_source_file_path(unique_manifest)

                    if unique_source and os.path.normpath(unique_source) == normalized_source:
                        return (True, unique_stem, "already_parsed")
                except:
                    pass

                counter += 1
                if counter > 100:  # Safety limit
                    return (False, f"{file_stem}_{counter}", "conflict")

        except Exception as e:
            # If we can't read the manifest, treat as no conflict
            return (False, file_stem, "no_conflict")

    def get_file_stem(self, file_path: str) -> str:
        """
        Determine the output file stem for a given input file.

        The file stem is used for:
        - Output filenames (e.g., {stem}_parse_output.json)
        - Manifest filename (e.g., {stem}_manifest.json)

        Default implementation:
        - If file_path is a directory: finds first matching file and uses its name
        - If file_path is a file: strips known extensions for this parser

        Parsers can override this to implement custom stem logic.

        Args:
            file_path: Path to input file or directory

        Returns:
            File stem to use for output files
        """
        import os
        import re

        # Normalize path
        normalized_path = os.path.normpath(str(file_path).strip())

        # If it's a directory, find first matching file
        if os.path.isdir(normalized_path):
            try:
                files = os.listdir(normalized_path)
                capabilities = self.get_capabilities()
                extensions = capabilities.file_extensions

                # Find first file with matching extension
                for f in files:
                    if os.path.isfile(os.path.join(normalized_path, f)):
                        for ext in extensions:
                            if f.endswith(ext):
                                # Strip extension and return
                                for ext in sorted(extensions, key=len, reverse=True):
                                    if f.endswith(ext):
                                        return f[:-len(ext)]
                                return re.sub(r'\.\w+$', '', f)
            except:
                pass

            # Fallback: use directory name
            return os.path.basename(normalized_path)

        # If it's a file, strip extensions
        file_name = os.path.basename(normalized_path)

        # Get known extensions for this parser
        capabilities = self.get_capabilities()
        extensions = capabilities.file_extensions

        # Try to strip known extensions
        for ext in sorted(extensions, key=len, reverse=True):  # Try longest first
            if file_name.endswith(ext):
                return file_name[:-len(ext)]

        # Fallback: strip any single extension
        stem = re.sub(r'\.\w+$', '', file_name)
        return stem

    def get_directory_stem(self, file_path: str) -> str:
        """
        Determine the output directory stem for a given input file.

        The directory stem is used for:
        - Output directory name (when using per_document output structure)

        Default implementation:
        - Returns the parent directory name of the input file/directory

        This allows multiple files from the same directory to be organized together,
        while each file gets its own unique name based on get_file_stem().

        Parsers can override this to implement custom directory naming logic.

        Args:
            file_path: Path to input file or directory

        Returns:
            Directory stem to use for output directory
        """
        import os

        # Normalize path
        normalized_path = os.path.normpath(str(file_path).strip())

        # If it's a directory, use its name
        if os.path.isdir(normalized_path):
            return os.path.basename(normalized_path)

        # If it's a file, use the parent directory name
        parent_dir = os.path.dirname(normalized_path)
        if parent_dir:
            return os.path.basename(parent_dir)

        # Fallback: use file stem
        return self.get_file_stem(file_path)
