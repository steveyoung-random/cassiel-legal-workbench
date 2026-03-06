"""
CFR XML Parser Adapter

Wraps cfr_set_parse.py to provide ParserAdapter interface.
"""
# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License

from typing import Dict, Any, Optional
import os
import json
from pathlib import Path

from .adapter import ParserAdapter, ParserCapabilities, ParseResult, SplitDetectionResult


class CFRParserAdapter(ParserAdapter):
    """Adapter for CFR XML parser"""

    def get_capabilities(self) -> ParserCapabilities:
        return ParserCapabilities(
            parser_type='cfr',
            display_name='CFR (XML)',
            version='1.0',
            file_extensions=['.xml'],
            supports_splitting=True,
            split_by_default=False,
            split_unit_name='part',
            split_parent_name='chapter',
            output_schema_version='0.3',
            module_name='cfr_set_parse',
            adapter_class='CFRParserAdapter'
        )

    def detect_split_units(self, file_path: str) -> SplitDetectionResult:
        """
        Inspect CFR file to detect parts for splitting.

        Accepts both file and directory paths (for consistency with parse_file).
        If a directory is provided, finds the first .xml file.
        """
        from cfr_set_parse import detect_part_units

        normalized_path = os.path.normpath(file_path)
        actual_file_path = normalized_path

        if os.path.isdir(normalized_path):
            files = os.listdir(normalized_path)
            xml_files = [
                f for f in files
                if os.path.isfile(os.path.join(normalized_path, f)) and f.endswith('.xml')
            ]
            if not xml_files:
                return SplitDetectionResult(can_split=False, split_units=[], parent_units={})
            actual_file_path = os.path.join(normalized_path, xml_files[0])
        elif not os.path.isfile(normalized_path):
            return SplitDetectionResult(can_split=False, split_units=[], parent_units={})

        split_units = detect_part_units(actual_file_path)

        if not split_units:
            return SplitDetectionResult(can_split=False, split_units=[], parent_units={})

        parent_units = {}
        first = split_units[0]
        if 'title' in first:
            parent_units['title'] = first['title']
        if 'chapter' in first:
            parent_units['chapter'] = first['chapter']

        return SplitDetectionResult(
            can_split=True,
            split_units=split_units,
            parent_units=parent_units
        )

    def parse_file(
        self,
        file_path: str,
        config: Dict[str, Any],
        params: Optional[Dict[str, Any]] = None
    ) -> ParseResult:
        """
        Parse CFR XML file using cfr_set_parse.py

        Supports split and full-document modes.
        """
        params = params or {}
        warnings = []

        try:
            from cfr_set_parse import parse_cfr, get_parsing_issues_logfile
            from utils.config import get_output_directory, get_output_structure
            from utils.manifest_utils import ManifestManager

            normalized_path = os.path.normpath(file_path)
            actual_file_path = normalized_path
            is_directory_input = os.path.isdir(normalized_path)

            if is_directory_input:
                files = os.listdir(normalized_path)
                xml_files = [
                    f for f in files
                    if os.path.isfile(os.path.join(normalized_path, f)) and f.endswith('.xml')
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
                if len(xml_files) > 1:
                    warnings.append(f"Found {len(xml_files)} XML files in directory, parsing all of them")
                    return self.parse_directory_batch(normalized_path, config, params)
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

            output_dir = get_output_directory(config)
            output_structure = get_output_structure(config)

            file_stem = self.get_file_stem(file_path)
            dir_stem = self.get_directory_stem(file_path)

            if output_structure == 'per_document':
                doc_output_dir = os.path.join(output_dir, dir_stem)
            else:
                doc_output_dir = output_dir

            Path(doc_output_dir).mkdir(parents=True, exist_ok=True)

            actual_file_path_abs = os.path.abspath(actual_file_path)
            skip_parsing, file_stem, conflict_status = self.resolve_output_conflict(
                doc_output_dir, file_stem, actual_file_path_abs
            )

            if skip_parsing:
                output_path = os.path.join(doc_output_dir, f'{file_stem}_parse_output.json')
                manifest_path = os.path.join(doc_output_dir, f'{file_stem}_manifest.json')
                warnings.append(f"File already parsed from source: {actual_file_path_abs}")
                return ParseResult(
                    success=True,
                    parsed_content=None,
                    manifest_entries=[],
                    error_message=None,
                    warnings=warnings,
                    files_created=[output_path, manifest_path]
                )

            if conflict_status == "conflict":
                warnings.append(
                    f"Output name conflict detected. Using '{file_stem}' instead of original stem to avoid overwriting existing output from different source."
                )

            parsing_logfile = get_parsing_issues_logfile(doc_output_dir)

            parse_mode = params.get('parse_mode', 'auto')
            if parse_mode == 'auto':
                caps = self.get_capabilities()
                if caps.supports_splitting and caps.split_by_default:
                    detection = self.detect_split_units(actual_file_path)
                    parse_mode = 'split' if detection.can_split else 'full'
                    if not detection.can_split:
                        warnings.append(
                            f"Document cannot be split (no {caps.split_unit_name}s found), parsing as full document"
                        )
                else:
                    parse_mode = 'full'

            files_created: list[str] = []
            manifest_path = os.path.join(doc_output_dir, f'{file_stem}_manifest.json')
            manifest_mgr = ManifestManager(manifest_path)
            manifest = manifest_mgr.create_or_load(
                source_file=actual_file_path_abs,
                source_type='cfr',
                parser='cfr_set_parse.py',
                parser_type='cfr'
            )

            if parse_mode == 'split':
                detection = self.detect_split_units(actual_file_path)
                if not detection.can_split:
                    parse_mode = 'full'
                else:
                    specific_units = params.get('specific_units')
                    split_units = detection.split_units
                    if specific_units:
                        split_units = [u for u in split_units if unit_matches(u, specific_units)]

                    last_title = ""
                    for unit in split_units:
                        parsed_content = parse_cfr(actual_file_path, parsing_logfile, specific_units=unit)
                        if not parsed_content:
                            warnings.append(f"Split parse returned empty content for unit {unit}")
                            continue
                        doc_title = parsed_content.get('document_information', {}).get('title', '')
                        if doc_title:
                            last_title = doc_title

                        suffix = build_unit_suffix(unit)
                        output_filename = f'{file_stem}{suffix}_parse_output.json'
                        output_path = os.path.join(doc_output_dir, output_filename)
                        with open(output_path, 'w', encoding='utf-8') as f:
                            json.dump(parsed_content, f, indent=4, ensure_ascii=False)

                        files_created.append(output_path)
                        manifest_mgr.add_parsed_file(
                            manifest,
                            output_path,
                            'split_unit',
                            organizational_units=unit
                        )

                    if last_title:
                        manifest_mgr.update_short_title(manifest, last_title)
                    manifest_mgr.save(manifest)
                    files_created.append(manifest_path)

                    return ParseResult(
                        success=True,
                        parsed_content=None,
                        manifest_entries=[],
                        error_message=None,
                        warnings=warnings,
                        files_created=files_created
                    )

            # Full document parse
            parsed_content = parse_cfr(actual_file_path, parsing_logfile, specific_units=params.get('specific_units'))
            if not parsed_content:
                return ParseResult(
                    success=False,
                    parsed_content=None,
                    manifest_entries=[],
                    error_message="Parsing returned empty content",
                    warnings=warnings,
                    files_created=[]
                )

            output_filename = f'{file_stem}_parse_output.json'
            output_path = os.path.join(doc_output_dir, output_filename)
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(parsed_content, f, indent=4, ensure_ascii=False)
            files_created.append(output_path)

            manifest_mgr.add_parsed_file(
                manifest,
                output_path,
                'full_document',
                organizational_units={}
            )
            manifest_mgr.update_short_title(manifest, parsed_content.get('document_information', {}).get('title', ''))
            manifest_mgr.save(manifest)
            files_created.append(manifest_path)

            return ParseResult(
                success=True,
                parsed_content=parsed_content,
                manifest_entries=[],
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


def build_unit_suffix(unit: Dict[str, str]) -> str:
    parts = []
    for key in ["title", "chapter", "subchapter", "part", "subpart"]:
        if key in unit and unit[key]:
            parts.append(f"{key}{unit[key]}")
    if not parts:
        return ""
    return "_" + "_".join(parts)


def unit_matches(unit: Dict[str, str], specific_units: Dict[str, str]) -> bool:
    for key, value in specific_units.items():
        if key not in unit:
            return False
        if str(unit[key]).strip() != str(value).strip():
            return False
    return True
