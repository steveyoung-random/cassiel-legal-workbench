"""
eCFR XML Parser (cfr_set_parse.py)

Parses eCFR (Electronic Code of Federal Regulations) XML files into JSON v0.4 format
used by the Document Analyzer. This parser handles the eCFR format which uses DIV
elements with TYPE attributes for hierarchy.

For the older GPO Bulk Data format parser, see cfr_set_parse_old.py.
Design decisions are documented in CFR_ECFR_PARSER_PLAN.md.
"""
# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License

from __future__ import annotations

import os
import re
import json
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple

from lxml import etree as ET

from utils import clean_text, canonical_org_types, LARGE_TABLE_ROW_THRESHOLD
from utils.error_handling import ParseError, log_parsing_correction
from utils.config import get_config, get_output_directory, get_output_structure
from utils.manifest_utils import ManifestManager, get_manifest_path, parse_filter_string, create_title_output_dir
from utils.large_table_common import assign_table_key, find_or_create_table_param_key


# Unicode fraction mapping for common fractions
UNICODE_FRACTIONS = {
    "1/2": "\u00BD",  # ½
    "1/4": "\u00BC",  # ¼
    "3/4": "\u00BE",  # ¾
    "1/3": "\u2153",  # ⅓
    "2/3": "\u2154",  # ⅔
    "1/5": "\u2155",  # ⅕
    "2/5": "\u2156",  # ⅖
    "3/5": "\u2157",  # ⅗
    "4/5": "\u2158",  # ⅘
    "1/6": "\u2159",  # ⅙
    "5/6": "\u215A",  # ⅚
    "1/8": "\u215B",  # ⅛
    "3/8": "\u215C",  # ⅜
    "5/8": "\u215D",  # ⅝
    "7/8": "\u215E",  # ⅞
    "1/7": "\u2150",  # ⅐
    "1/9": "\u2151",  # ⅑
    "1/10": "\u2152",  # ⅒
}

# Mapping from eCFR DIV TYPE attributes to normalized organizational unit names
TYPE_TO_ORG_NAME = {
    "TITLE": "title",
    "SUBTITLE": "subtitle",
    "CHAPTER": "chapter",
    "SUBCHAP": "subchapter",
    "PART": "part",
    "SUBPART": "subpart",
    "SUBJGRP": "subject_group",
    "SECTION": "section",
    "APPENDIX": "appendix",
}


def convert_fraction(text: str) -> str:
    """Convert a fraction string to Unicode character if available, otherwise return as-is."""
    text = text.strip()
    return UNICODE_FRACTIONS.get(text, text)


def clean_unit_title(text: str) -> str:
    """Remove leading/trailing punctuation and whitespace from unit title text."""
    if not text:
        return text
    # Strip whitespace, then leading dashes, then trailing punctuation, then whitespace
    text = text.strip()
    text = text.lstrip('-—–')  # em-dash, en-dash, hyphen
    text = text.rstrip('.,:;')
    return text.strip()


def get_parsing_issues_logfile(dir_path: str = '') -> str:
    """
    Get a logfile path for parsing-level issues.

    Args:
        dir_path: Directory path where logfile should be created

    Returns:
        Path to logfile
    """
    if not os.path.isdir(dir_path):
        if os.path.isfile(dir_path):
            dir_path = os.path.dirname(dir_path)
        else:
            dir_path = os.path.abspath(os.path.curdir)
    count = 1
    log_stem = 'parsing_issues'
    while os.path.exists(os.path.join(dir_path, log_stem + str(count).zfill(4) + '.json')):
        count += 1
    return str(os.path.join(dir_path, log_stem + str(count).zfill(4) + '.json'))


def get_title_name(input_file_path: str) -> str:
    """
    Quickly extract the CFR title name (e.g., 'Title 15') from the XML
    without full parsing. Returns empty string if not found.
    """
    try:
        for _event, elem in ET.iterparse(input_file_path, events=('start',)):
            tag = elem.tag.upper()
            if tag.startswith('DIV') and elem.get('TYPE') == 'TITLE':
                title_num = elem.get('N', '').strip()
                if title_num:
                    return f"Title {title_num}"
                return ""
    except Exception:
        pass
    return ""


def set_items(item_type: int) -> Tuple[str, str, int, Optional[str]]:
    """
    Configure item type parameters for CFR documents.

    Args:
        item_type: 1 = section, 2 = appendix, 3 = supplement,
                   4 = special federal aviation regulation, 5 = schedule, 6 = table

    Returns:
        (name, name_plural, operational, reference_instruction)
    """
    if item_type == 1:
        return ("section", "sections", 1, None)
    if item_type == 2:
        reference_instruction = (
            "When identifying references to appendices, use the designator as it appears "
            "after 'Appendix' (e.g., for 'Appendix A to Part 17', look up 'A to Part 17'). "
            "For references like 'Appendix A of this part' within Part 17, resolve to 'A to Part 17'."
        )
        return ("appendix", "appendices", 1, reference_instruction)
    if item_type == 3:
        reference_instruction = (
            "When identifying references to supplements, use the designator as it appears "
            "after 'Supplement' (e.g., for 'Supplement No. 1 to Part 714', look up 'No. 1 to Part 714')."
        )
        return ("supplement", "supplements", 1, reference_instruction)
    if item_type == 4:
        reference_instruction = (
            "When identifying references to Special Federal Aviation Regulations (SFARs), "
            "use the designator as it appears after 'Special Federal Aviation Regulation' "
            "(e.g., for 'Special Federal Aviation Regulation No. 36', look up 'No. 36'). "
            "References may also appear as 'SFAR No. 36'."
        )
        return ("special federal aviation regulation", "special federal aviation regulations", 1, reference_instruction)
    if item_type == 5:
        reference_instruction = (
            "When identifying references to schedules, use the designator as it appears "
            "after 'Schedule' (e.g., for 'Schedule A to Part 117', look up 'A to Part 117')."
        )
        return ("schedule", "schedules", 1, reference_instruction)
    if item_type == 6:
        reference_instruction = (
            "When identifying references to tables stored as appendix-like items, use the designator "
            "as it appears after 'Table' (e.g., for 'Table 1 to Part 117', look up '1 to Part 117')."
        )
        return ("table", "tables", 1, reference_instruction)
    if item_type == 7:
        reference_instruction = (
            "When identifying references to annexes, use the designator as it appears "
            "after 'Annex' (e.g., for 'Annex A to Part 117', look up 'A to Part 117')."
        )
        return ("annex", "annexes", 1, reference_instruction)
    if item_type == 8:
        reference_instruction = (
            "When identifying references to exhibits, use the designator as it appears "
            "after 'Exhibit' (e.g., for 'Exhibit 1 to Part 117', look up '1 to Part 117')."
        )
        return ("exhibit", "exhibits", 1, reference_instruction)
    if item_type == 9:
        reference_instruction = (
            "When identifying references to figures stored as appendix-like items, use the designator "
            "as it appears after 'Figure' (e.g., for 'Figure 1 to Part 117', look up '1 to Part 117')."
        )
        return ("figure", "figures", 1, reference_instruction)
    if item_type == 10:
        reference_instruction = (
            "When identifying references to policy statements, use the designator as it appears "
            "after 'Policy Statement' (e.g., for 'Policy Statement No. 1', look up 'No. 1')."
        )
        return ("policy statement", "policy statements", 1, reference_instruction)
    raise ParseError(f"Unknown item type: {item_type}")


def parse_ecfr(file_path: str, parsing_logfile: Optional[str] = None,
               specific_units: Optional[Dict[str, str]] = None,
               residual_mode: bool = False) -> Dict[str, Any]:
    """
    Parse an eCFR XML file into JSON v0.4 structure.

    Args:
        file_path: Path to eCFR XML file
        parsing_logfile: Optional log file path for parsing issues
        specific_units: Optional filter for splitting (e.g., {'part': '17'})

    Returns:
        Parsed content dict
    """
    if not os.path.isfile(file_path):
        raise ParseError(f"File not found: {file_path}")

    tree = ET.parse(file_path)
    root = tree.getroot()

    content: Dict[str, Any] = {}
    content["document_information"] = {}
    content["document_information"]["version"] = "0.5"
    content["content"] = {}

    # Parameters - item types: 1=section, 2=appendix, 3=supplement,
    # 4=special federal aviation regulation, 5=schedule, 6=table,
    # 7=annex, 8=exhibit, 9=figure, 10=policy statement
    content["document_information"]["parameters"] = {}
    param_pointer = content["document_information"]["parameters"]
    for i in range(1, 11):  # Types 1-10
        item_type_name, item_type_name_plural, operational, reference_instruction = set_items(i)
        param_pointer[i] = {
            "name": item_type_name,
            "name_plural": item_type_name_plural,
            "operational": operational
        }
        if reference_instruction:
            param_pointer[i]["reference_instruction"] = reference_instruction
        content["content"][item_type_name_plural] = {}

    # Organization
    content["document_information"]["organization"] = {}
    org_pointer = content["document_information"]["organization"]
    org_pointer["item_types"] = list(range(1, 11))  # Types 1-10
    org_pointer["content"] = {}

    # Find the title DIV (DIV1 with TYPE="TITLE")
    title_div = root.find(".//DIV1[@TYPE='TITLE']")
    if title_div is None:
        # Try without namespace
        for div in root.iter():
            if div.tag.upper().startswith('DIV') and div.get('TYPE') == 'TITLE':
                title_div = div
                break

    if title_div is not None:
        title_num = title_div.get('N', '')
        title_head = title_div.find('HEAD')
        title_subject = ''
        if title_head is not None and title_head.text:
            # Extract subject from "Title 14—Aeronautics and Space"
            head_text = clean_text(title_head.text)
            match = re.search(r'Title\s+\d+[A-Za-z]*\s*[—–-]\s*(.+)', head_text)
            if match:
                title_subject = match.group(1).strip()
            else:
                title_subject = head_text

        if title_num:
            content["document_information"]["title"] = f"Title {title_num}"
            if title_subject:
                content["document_information"]["long_title"] = f"Title {title_num}—{title_subject}"
            else:
                content["document_information"]["long_title"] = f"Title {title_num}"
        else:
            content["document_information"]["title"] = title_subject or ""
            content["document_information"]["long_title"] = title_subject or ""

        # Set up organization root
        org_content_pointer = org_pointer["content"]
        if title_num:
            title_unit = ensure_org_unit(org_content_pointer, "title", title_num, title_subject)
            org_context: List[Dict[str, str]] = [{"title": title_num}]
        else:
            title_unit = org_content_pointer
            org_context = []

        # Parse the hierarchy starting from title
        parse_div_children(title_div, content, title_unit, org_context, parsing_logfile, specific_units, residual_mode)
    else:
        content["document_information"]["title"] = ""
        content["document_information"]["long_title"] = ""
        # Try to parse from root anyway
        parse_div_children(root, content, org_pointer["content"], [], parsing_logfile, specific_units, residual_mode)

    # Clean up internal counters
    if "_blank_appendix_counter" in content:
        del content["_blank_appendix_counter"]

    # Remove empty content sections and their metadata
    cleanup_empty_content_sections(content)

    return content


def cleanup_empty_content_sections(content: Dict[str, Any]) -> None:
    """
    Remove empty content sections and their corresponding metadata.

    For any item type (appendix, supplement, table, etc.) where no items were found,
    this removes:
    - The empty dict from content["content"]
    - The parameter entry from content["document_information"]["parameters"]
    - The type number from content["document_information"]["organization"]["item_types"]

    This avoids unnecessary clutter in parsed files.
    """
    params = content.get("document_information", {}).get("parameters", {})
    item_types = content.get("document_information", {}).get("organization", {}).get("item_types", [])
    content_sections = content.get("content", {})

    # Find which type numbers have empty content sections
    types_to_remove = []
    for type_num, param_info in list(params.items()):
        # Do not remove data-table sub-unit types: they have no top-level content,
        # only sub_units inside other items (e.g. supplements). Removing them would
        # break Stage 2/3/4 which look up data_table params.
        if param_info.get("data_table") and param_info.get("is_sub_unit"):
            continue
        name_plural = param_info.get("name_plural", "")
        if name_plural in content_sections:
            if not content_sections[name_plural]:  # Empty dict
                types_to_remove.append((type_num, name_plural))

    # Remove empty sections and their metadata
    for type_num, name_plural in types_to_remove:
        # Remove from content
        if name_plural in content_sections:
            del content_sections[name_plural]

        # Remove from parameters
        if type_num in params:
            del params[type_num]

        # Remove from item_types list
        if type_num in item_types:
            item_types.remove(type_num)


def parse_div_children(
    parent_elem: ET.Element,
    content: Dict[str, Any],
    org_pointer: Dict[str, Any],
    context: List[Dict[str, str]],
    parsing_logfile: Optional[str],
    specific_units: Optional[Dict[str, str]],
    residual_mode: bool = False
) -> None:
    """
    Recursively parse DIV children of a parent element.

    The eCFR format uses DIV1, DIV3, DIV4, DIV5, DIV6, DIV7, DIV8, DIV9 elements
    with TYPE attributes to indicate organizational level.
    """
    for child in list(parent_elem):
        tag = child.tag.upper() if isinstance(child.tag, str) else ''

        # Skip non-DIV elements at this level
        if not tag.startswith('DIV'):
            continue

        div_type = child.get('TYPE', '').upper()
        div_id = child.get('N', '')

        if div_type == 'SUBTITLE':
            parse_subtitle(child, content, org_pointer, context, parsing_logfile, specific_units, residual_mode)
        elif div_type == 'CHAPTER':
            parse_chapter(child, content, org_pointer, context, parsing_logfile, specific_units, residual_mode)
        elif div_type == 'SUBCHAP':
            parse_subchapter(child, content, org_pointer, context, parsing_logfile, specific_units, residual_mode)
        elif div_type == 'PART':
            if not residual_mode:
                parse_part(child, content, org_pointer, context, parsing_logfile, specific_units)
        elif div_type == 'SUBPART':
            parse_subpart(child, content, org_pointer, context, parsing_logfile, specific_units)
        elif div_type == 'SUBJGRP':
            # Subject groups are organizational groupings within subparts
            parse_subject_group(child, content, org_pointer, context, parsing_logfile, specific_units)
        elif div_type == 'SECTION':
            parse_section(child, content, org_pointer, context, parsing_logfile)
        elif div_type == 'APPENDIX':
            if not specific_units or context_matches(specific_units, context):
                parse_appendix(child, content, org_pointer, context, parsing_logfile)
        elif div_type == 'TITLE':
            # Nested title (shouldn't happen but handle gracefully)
            parse_div_children(child, content, org_pointer, context, parsing_logfile, specific_units, residual_mode)


def parse_subtitle(
    subtitle_elem: ET.Element,
    content: Dict[str, Any],
    org_parent_pointer: Dict[str, Any],
    base_context: List[Dict[str, str]],
    parsing_logfile: Optional[str],
    specific_units: Optional[Dict[str, str]],
    residual_mode: bool = False
) -> None:
    """Parse a SUBTITLE DIV element."""
    subtitle_id = subtitle_elem.get('N', '')
    subtitle_title = extract_head_text(subtitle_elem)

    # Clean up subtitle title (remove "Subtitle X—" prefix)
    if subtitle_title:
        subtitle_title = re.sub(r'^Subtitle\s+[A-Za-z0-9.\-]+\s*[—–-]\s*', '', subtitle_title, flags=re.IGNORECASE)

    if subtitle_id:
        subtitle_pointer = ensure_org_unit(org_parent_pointer, "subtitle", subtitle_id, subtitle_title)
        context = base_context + [{"subtitle": subtitle_id}]
    else:
        subtitle_pointer = org_parent_pointer
        context = list(base_context)

    # Capture content_scope if subtitle is the deepest level in specific_units
    if (subtitle_id and specific_units and 'subtitle' in specific_units and
            context_matches(specific_units, context) and
            'content_scope' not in content.get('document_information', {})):
        content['document_information']['content_scope'] = list(context)

    parse_div_children(subtitle_elem, content, subtitle_pointer, context, parsing_logfile, specific_units, residual_mode)


def parse_chapter(
    chapter_elem: ET.Element,
    content: Dict[str, Any],
    org_parent_pointer: Dict[str, Any],
    base_context: List[Dict[str, str]],
    parsing_logfile: Optional[str],
    specific_units: Optional[Dict[str, str]],
    residual_mode: bool = False
) -> None:
    """Parse a CHAPTER DIV element."""
    chapter_id = chapter_elem.get('N', '')
    chapter_title = extract_head_text(chapter_elem)

    if chapter_id:
        chapter_pointer = ensure_org_unit(org_parent_pointer, "chapter", chapter_id, chapter_title)
        context = base_context + [{"chapter": chapter_id}]
    else:
        chapter_pointer = org_parent_pointer
        context = list(base_context)

    # Capture content_scope if chapter is the deepest level in specific_units
    if (chapter_id and specific_units and 'chapter' in specific_units and
            context_matches(specific_units, context) and
            'content_scope' not in content.get('document_information', {})):
        content['document_information']['content_scope'] = list(context)

    parse_div_children(chapter_elem, content, chapter_pointer, context, parsing_logfile, specific_units, residual_mode)


def parse_subchapter(
    subchap_elem: ET.Element,
    content: Dict[str, Any],
    org_parent_pointer: Dict[str, Any],
    base_context: List[Dict[str, str]],
    parsing_logfile: Optional[str],
    specific_units: Optional[Dict[str, str]],
    residual_mode: bool = False
) -> None:
    """Parse a SUBCHAP DIV element."""
    subchap_id = subchap_elem.get('N', '')
    subchap_title = extract_head_text(subchap_elem)

    if subchap_id:
        subchap_pointer = ensure_org_unit(org_parent_pointer, "subchapter", subchap_id, subchap_title)
        context = base_context + [{"subchapter": subchap_id}]
    else:
        subchap_pointer = org_parent_pointer
        context = list(base_context)

    # Capture content_scope if subchapter is the deepest level in specific_units
    if (subchap_id and specific_units and 'subchapter' in specific_units and
            context_matches(specific_units, context) and
            'content_scope' not in content.get('document_information', {})):
        content['document_information']['content_scope'] = list(context)

    parse_div_children(subchap_elem, content, subchap_pointer, context, parsing_logfile, specific_units, residual_mode)


def parse_part(
    part_elem: ET.Element,
    content: Dict[str, Any],
    org_parent_pointer: Dict[str, Any],
    base_context: List[Dict[str, str]],
    parsing_logfile: Optional[str],
    specific_units: Optional[Dict[str, str]]
) -> None:
    """Parse a PART DIV element."""
    part_id = part_elem.get('N', '')
    part_title = extract_head_text(part_elem)

    # Clean up part title (remove "PART X—" prefix)
    if part_title:
        part_title = re.sub(r'^PART\s+[0-9A-Za-z.\-]+\s*[—–-]\s*', '', part_title, flags=re.IGNORECASE)

    if part_id:
        part_pointer = ensure_org_unit(org_parent_pointer, "part", part_id, part_title)
        context = base_context + [{"part": part_id}]
    else:
        part_pointer = org_parent_pointer
        context = list(base_context)

    # Check if this part matches the filter
    if specific_units and not context_matches(specific_units, context):
        return

    # Capture content_scope if part is the deepest level in specific_units
    # (context_matches already confirmed we're at the right location)
    if (specific_units and 'part' in specific_units and
            'content_scope' not in content.get('document_information', {})):
        content['document_information']['content_scope'] = list(context)

    parse_div_children(part_elem, content, part_pointer, context, parsing_logfile, specific_units)


def parse_subpart(
    subpart_elem: ET.Element,
    content: Dict[str, Any],
    org_parent_pointer: Dict[str, Any],
    base_context: List[Dict[str, str]],
    parsing_logfile: Optional[str],
    specific_units: Optional[Dict[str, str]]
) -> None:
    """Parse a SUBPART DIV element."""
    subpart_id = subpart_elem.get('N', '')
    subpart_title = extract_head_text(subpart_elem)

    # Clean up subpart title (remove "Subpart X—" prefix)
    if subpart_title:
        subpart_title = re.sub(r'^Subpart\s+[A-Za-z0-9.\-]+\s*[—–-]\s*', '', subpart_title, flags=re.IGNORECASE)

    if subpart_id:
        subpart_pointer = ensure_org_unit(org_parent_pointer, "subpart", subpart_id, subpart_title)
        context = base_context + [{"subpart": subpart_id}]
    else:
        subpart_pointer = org_parent_pointer
        context = list(base_context)

    if specific_units and not context_matches(specific_units, context):
        return

    # Capture content_scope if subpart is the deepest level in specific_units
    # (context_matches already confirmed we're at the right location)
    if (specific_units and 'subpart' in specific_units and
            'content_scope' not in content.get('document_information', {})):
        content['document_information']['content_scope'] = list(context)

    parse_div_children(subpart_elem, content, subpart_pointer, context, parsing_logfile, specific_units)


def parse_subject_group(
    subjgrp_elem: ET.Element,
    content: Dict[str, Any],
    org_parent_pointer: Dict[str, Any],
    base_context: List[Dict[str, str]],
    parsing_logfile: Optional[str],
    specific_units: Optional[Dict[str, str]]
) -> None:
    """
    Parse a SUBJGRP (Subject Group) DIV element.

    Subject groups are organizational groupings within subparts that group
    related sections together. They don't change the hierarchy context for
    sections but can be recorded in the organization structure.
    """
    # Subject groups have generated IDs like "ECFR8ebf6bddc82be9b"
    # We don't add them to the context but do parse their children
    parse_div_children(subjgrp_elem, content, org_parent_pointer, base_context, parsing_logfile, specific_units)


def parse_section(
    section_elem: ET.Element,
    content: Dict[str, Any],
    org_pointer: Dict[str, Any],
    context: List[Dict[str, str]],
    parsing_logfile: Optional[str]
) -> None:
    """Parse a SECTION DIV element."""
    # Section ID is in the N attribute
    section_id = section_elem.get('N', '')
    if not section_id:
        log_parsing_correction("", "missing_section_number", "SECTION without N attribute", parsing_logfile)
        return

    # Extract title from HEAD element
    head_elem = section_elem.find('HEAD')
    subject = ''
    if head_elem is not None:
        head_text = extract_element_text(head_elem)
        # Remove everything up to and including the section_id, plus trailing whitespace
        # This handles any prefix (§, "Section", or nothing) before the ID
        pattern = r'.*?' + re.escape(section_id) + r'\s*'
        subject = re.sub(pattern, '', head_text, count=1).strip()

    text_parts: List[str] = []
    annotation_parts: List[str] = []
    notes = extract_footnotes(section_elem)

    # Process child elements
    for child in list(section_elem):
        tag = child.tag.upper() if isinstance(child.tag, str) else ''

        if tag == 'HEAD':
            continue  # Already processed
        elif tag == 'XREF':
            # Future amendment reference - add to annotations
            xref_text = extract_element_text(child)
            if xref_text:
                annotation_parts.append(f"Amendment pending: {xref_text}")
        elif tag in ('P', 'PSPACE'):
            para = extract_paragraph_text(child)
            if para:
                text_parts.append(para)
        elif tag in ('FP', 'FP1', 'FP2'):
            # Flush paragraph
            para = extract_paragraph_text(child)
            if para:
                text_parts.append(para)
        # Additional flush paragraph variants
        elif tag in ('FP-1', 'FP-2', 'FP1-2', 'FP2-2', 'FP2-3', 'FRP', 'FRP0'):
            para = extract_paragraph_text(child)
            if para:
                text_parts.append(para)
        # Dash paragraph types - add line indicator after
        elif tag in ('FP-DASH', 'P-DASH'):
            para = extract_paragraph_text(child)
            if para:
                text_parts.append(para + " ________")
            else:
                text_parts.append("________")
        # Half-dash - write-in line starting midway through column
        elif tag == 'HALFDASH':
            text_parts.append("_____")
        # Additional numbered paragraph variants
        elif tag in ('P-1', 'P-2', 'P-3', 'P1', 'P2'):
            para = extract_paragraph_text(child)
            if para:
                text_parts.append(para)
        elif tag == 'NOTE':
            note_text = extract_note_text(child)
            if note_text:
                text_parts.append(f"Note: {note_text}")
        elif tag == 'EXAMPLE':
            # Example blocks - extract like NOTE
            example_text = extract_note_text(child)
            if example_text:
                text_parts.append(f"Example: {example_text}")
        elif tag == 'EDNOTE':
            # Editorial notes - ignore
            continue
        elif tag == 'EXTRACT':
            extract_text = extract_block_text(child)
            if extract_text:
                text_parts.append(extract_text)
        elif tag == 'CITA':
            cita_text = extract_element_text(child)
            if cita_text:
                annotation_parts.append(f"CITA: {cita_text}")
        elif tag in ('SECAUTH', 'APPRO', 'PARAUTH'):
            # Section-level authority, OMB approval, paragraph authority - ignore
            continue
        elif tag == 'FTNT':
            # Footnotes handled separately
            continue
        elif tag.startswith('DIV'):
            # Nested DIV elements may contain tables and other content
            nested_parts = extract_nested_div_content(child)
            text_parts.extend(nested_parts)
        elif tag in ('AUTH', 'SOURCE'):
            # Authority and source at section level - ignore
            continue
        elif tag == 'EFFDNOT':
            # Effective date note - add to annotations
            effdnot_text = extract_effdnot_text(child)
            if effdnot_text:
                annotation_parts.append(effdnot_text)
        elif tag in ('HED1', 'PARTHD', 'DOCKETHD'):
            # Additional heading types - treat like HD elements
            hd_text = extract_element_text(child)
            if hd_text:
                text_parts.append(f"\n{hd_text}\n")
        elif tag.startswith('HD'):
            # Sub-headings within section
            hd_text = extract_element_text(child)
            if hd_text:
                text_parts.append(f"\n{hd_text}\n")
        elif tag == 'MATH':
            # Mathematical content - extract text representation
            math_text = extract_element_text(child)
            if math_text:
                text_parts.append(math_text)
        elif tag == 'GPOTABLE' or tag == 'TABLE':
            # Tables - extract text content
            table_text = extract_table_text(child)
            if table_text:
                text_parts.append(table_text)
        # List items - each on own line
        elif tag == 'LI':
            para = extract_paragraph_text(child)
            if para:
                text_parts.append(para)
        # Two-column list container - extract contents
        elif tag == 'SCOL2':
            scol_text = extract_block_text(child)
            if scol_text:
                text_parts.append(scol_text)
        # Leader work (forms with left/right alignment)
        elif tag == 'LDRWK':
            ldrwk_text = extract_ldrwk_text(child)
            if ldrwk_text:
                text_parts.append(ldrwk_text)
        # Captions for images
        elif tag in ('TCAP', 'BCAP'):
            caption = extract_element_text(child)
            if caption:
                text_parts.append(clean_text(caption))
        elif tag == 'IMG' or tag == 'GPH':
            # Image element - insert placeholder
            text_parts.append("[Image omitted]")

    text, breakpoints = assemble_text_and_breakpoints(text_parts)

    section_entry = {
        "text": text,
        "unit_title": clean_unit_title(subject),
        "breakpoints": breakpoints,
        "notes": notes,
        "context": list(context)
    }
    if annotation_parts:
        section_entry["annotation"] = " ".join(annotation_parts).strip()

    # Ensure unique identifier
    section_name_plural = content["document_information"]["parameters"][1]["name_plural"]
    section_id = ensure_unique_id(content["content"][section_name_plural], section_id, parsing_logfile, "section")
    content["content"][section_name_plural][section_id] = section_entry

    update_begin_stop(org_pointer, "section", section_id)


def detect_appendix_item_type(raw_id: str) -> Tuple[int, str, str]:
    """
    Detect the item type from an APPENDIX DIV's N attribute and normalize the ID.

    DIV elements with TYPE="APPENDIX" may contain various item types:
    - Appendix (type 2): "Appendix A to Part 17" -> "A to Part 17"
    - Supplement (type 3): "Supplement No. 1 to Part 714" -> "No. 1 to Part 714"
    - Special Federal Aviation Regulation (type 4): "Special Federal Aviation Regulation No. 36" -> "No. 36"
    - Schedule (type 5): "Schedule A to Part 117" -> "A to Part 117"
    - Table (type 6): "Table 1 to Part 117" -> "1 to Part 117"
    - Annex (type 7): "Annex A to Part 117" -> "A to Part 117"
    - Exhibit (type 8): "Exhibit 1 to Part 117" -> "1 to Part 117"
    - Figure (type 9): "Figure 1 to Part 117" -> "1 to Part 117"
    - Policy Statement (type 10): "Policy Statement No. 1" -> "No. 1"

    Args:
        raw_id: The raw identifier from the N attribute

    Returns:
        Tuple of (item_type_number, normalized_id, name_plural)
    """
    if not raw_id:
        return (2, raw_id, "appendices")  # Default to appendix

    raw_lower = raw_id.lower()

    # Check for Special Federal Aviation Regulation (must check before shorter patterns)
    # Can appear as "Special Federal Aviation Regulation No. X" or "SFAR No. X"
    if raw_lower.startswith('special federal aviation regulation '):
        prefix_len = len('Special Federal Aviation Regulation ')
        return (4, raw_id[prefix_len:], "special federal aviation regulations")
    if raw_lower.startswith('sfar '):
        prefix_len = len('SFAR ')
        return (4, raw_id[prefix_len:], "special federal aviation regulations")

    # Check for Policy Statement (must check before shorter patterns)
    if raw_lower.startswith('policy statement '):
        prefix_len = len('Policy Statement ')
        return (10, raw_id[prefix_len:], "policy statements")

    # Check for Supplement
    if raw_lower.startswith('supplement '):
        prefix_len = len('Supplement ')
        return (3, raw_id[prefix_len:], "supplements")

    # Check for Schedule
    if raw_lower.startswith('schedule '):
        prefix_len = len('Schedule ')
        return (5, raw_id[prefix_len:], "schedules")

    # Check for Table
    if raw_lower.startswith('table '):
        prefix_len = len('Table ')
        return (6, raw_id[prefix_len:], "tables")

    # Check for Annex
    if raw_lower.startswith('annex '):
        prefix_len = len('Annex ')
        return (7, raw_id[prefix_len:], "annexes")

    # Check for Exhibit
    if raw_lower.startswith('exhibit '):
        prefix_len = len('Exhibit ')
        return (8, raw_id[prefix_len:], "exhibits")

    # Check for Figure
    if raw_lower.startswith('figure '):
        prefix_len = len('Figure ')
        return (9, raw_id[prefix_len:], "figures")

    # Check for Appendix
    if raw_lower.startswith('appendix '):
        prefix_len = len('Appendix ')
        return (2, raw_id[prefix_len:], "appendices")

    # Default: treat as appendix but keep full name
    return (2, raw_id, "appendices")


# ---------------------------------------------------------------------------
# ECCN Sub-Unit Detection and Subdivision (v0.4)
# ---------------------------------------------------------------------------

# Minimum text length before attempting ECCN subdivision (characters)
ECCN_SUBDIVISION_THRESHOLD = 100_000


# ---------------------------------------------------------------------------
# Large Table Sub-Unit Helpers
# ---------------------------------------------------------------------------

def _count_xml_table_rows(elem: ET.Element) -> int:
    """Count data rows in a GPOTABLE (ROW elements) or TABLE (TR elements) XML element."""
    tag_upper = elem.tag.upper() if isinstance(elem.tag, str) else ''
    if tag_upper == 'TABLE':
        return len(elem.findall('.//TR'))
    return len(elem.findall('.//ROW'))


def _build_xml_table_sub_unit(
    xml_elem: ET.Element,
    local_counter: int,
    parent_context: List[Dict[str, str]],
    parent_type_name: str,
    parent_id: str,
) -> Dict[str, Any]:
    """
    Build a table sub-unit dict from a GPOTABLE or TABLE XML element.

    For TABLE elements, serializes to HTML (preserving cell transformations).
    For GPOTABLE elements, serializes to raw XML (readable by Stage 3 AI).
    Column headers and caption are extracted at the XML level so regex-based
    helpers in table_handling.py are not needed for this path.
    """
    import copy

    tag_upper = xml_elem.tag.upper() if isinstance(xml_elem.tag, str) else ''

    if tag_upper == 'TABLE':
        row_count = len(xml_elem.findall('.//TR'))
        column_headers = [
            clean_text(th.text or '')
            for th in xml_elem.findall('.//THEAD//TH')
            if th.text
        ]
        caption_elem = xml_elem.find('.//CAPTION')
        caption = clean_text(caption_elem.text or '') if caption_elem is not None and caption_elem.text else ''
        table_copy = copy.deepcopy(xml_elem)
        process_table_element(table_copy)
        table_html = ET.tostring(table_copy, encoding='unicode', method='html')
    else:  # GPOTABLE
        row_count = len(xml_elem.findall('.//ROW'))
        column_headers = [
            clean_text(extract_element_text(ched))
            for ched in xml_elem.findall('.//BOXHD/CHED')
        ]
        column_headers = [h for h in column_headers if h]
        ttitle = xml_elem.find('TTITLE')
        caption = clean_text(extract_element_text(ttitle)) if ttitle is not None else ''
        table_html = ET.tostring(xml_elem, encoding='unicode')

    return {
        "text": "",
        "table_html": table_html,
        "table_row_count": row_count,
        "table_column_headers": column_headers,
        "table_caption": caption,
        "unit_title": f"Table {local_counter}",
        "context": list(parent_context) + [{parent_type_name: parent_id}],
        "breakpoints": [],
    }


# Regex matching bold ECCN markers: **0A001, **1C351, **EAR99, etc.
_ECCN_BOUNDARY_RE = re.compile(r'\*\*(\d[A-E]\d{3}|EAR99)\b')

# Regex matching CCL category headers embedded in ##HD1## markers
# E.g., "Category 0—Nuclear Materials, Facilities and Equipment..."
_CCL_CATEGORY_HEADER_RE = re.compile(r'^Category\s+(\d+)\s*[—\-]', re.IGNORECASE)

# Regex matching CCL lettered section headers: 'A. "End Items"...' or "A. 'End Items'..."
_CCL_SECTION_HEADER_RE = re.compile(r'^([A-Z])\.\s+[\"\u201c\']')


def detect_eccn_boundaries(text: str) -> List[Tuple[int, str, str]]:
    """
    Detect bold ECCN markers in Commerce Control List text.

    Returns a list of (offset, eccn_id, title_line) tuples sorted by offset.
    The title_line is the text after the ECCN designator on the same line,
    with bold markers stripped — i.e. the human-readable title without the key.
    """
    boundaries = []
    for m in _ECCN_BOUNDARY_RE.finditer(text):
        eccn_id = m.group(1)
        # Find end of heading line
        line_end = text.find('\n', m.start())
        if line_end == -1:
            line_end = len(text)
        # m.end() is past the ECCN designator (e.g. past "**0A979").
        # Strip remaining bold markers and whitespace to get the clean title.
        title_line = text[m.end():line_end].strip('* \t')
        boundaries.append((m.start(), eccn_id, title_line))
    return boundaries


def detect_ccl_boundaries(text: str) -> List[Tuple[int, str, str, str]]:
    """
    Detect all CCL structure boundaries in assembled text.

    Returns sorted list of (offset, boundary_type, item_id, title_text):
      boundary_type: 'category' | 'section' | 'eccn' | 'heading'
      item_id: category number, section letter, ECCN designator, or ''
      title_text: full header/title text
    """
    boundaries = []

    # Find ##HD1## markers and classify them
    for m in re.finditer(r'##HD1##\s*(.+)', text):
        hd_text = m.group(1).strip()
        cat_m = _CCL_CATEGORY_HEADER_RE.match(hd_text)
        sec_m = _CCL_SECTION_HEADER_RE.match(hd_text)
        if cat_m:
            boundaries.append((m.start(), 'category', cat_m.group(1), hd_text))
        elif sec_m:
            boundaries.append((m.start(), 'section', sec_m.group(1), hd_text))
        else:
            boundaries.append((m.start(), 'heading', '', hd_text))

    # Find ECCN markers (same logic as detect_eccn_boundaries)
    for m in _ECCN_BOUNDARY_RE.finditer(text):
        eccn_id = m.group(1)
        line_end = text.find('\n', m.start())
        if line_end == -1:
            line_end = len(text)
        # m.end() is past the designator; strip remaining bold markers for clean title
        title_line = text[m.end():line_end].strip('* \t')
        boundaries.append((m.start(), 'eccn', eccn_id, title_line))

    boundaries.sort(key=lambda x: x[0])
    return boundaries


def subdivide_ccl(
    parent_text: str,
    parent_context: List[Dict[str, str]],
    parent_type_name: str,
    parent_number: str,
    breakpoints: List[List[int]],
    cat_param_key: int,
    sec_param_key: int,
    eccn_param_key: int,
) -> Optional[Tuple[Dict, Dict]]:
    """
    Subdivide CCL supplement text into a 3-level hierarchy:
    Category -> Section -> ECCN.

    Returns (type_keyed_sub_units, sub_unit_index_entries) or None if no ECCN
    boundaries are found.
      type_keyed_sub_units: {str(cat_param_key): {cat_id: cat_item, ...}}
      sub_unit_index_entries: {eccn_id: {"container_plural", "container_id", "path"}}
    """
    boundaries = detect_ccl_boundaries(parent_text)
    if not any(b[1] == 'eccn' for b in boundaries):
        return None

    cat_key = str(cat_param_key)
    sec_key = str(sec_param_key)
    eccn_key = str(eccn_param_key)

    supplement_context = parent_context + [{parent_type_name: parent_number}]
    container_plural = parent_type_name + 's'

    def clean_markers(t: str) -> str:
        return re.sub(r'##HD1##\s*', '', t)

    def slice_breakpoints(bps, start, end):
        return [[bp[0] - start, bp[1]] for bp in bps if start < bp[0] < end]

    def find_line_end(offset: int) -> int:
        """Return position just after the '\n' ending the line at offset."""
        pos = parent_text.find('\n', offset)
        return pos + 1 if pos != -1 else len(parent_text)

    # --- Phase 1: Scan boundaries, build intermediate structure ---
    #
    # cat_items[cat_id] = {
    #   'title': str,
    #   'text_start': int,   # start of text after category header line
    #   'text_end': int,     # end of text (start of first section, or next cat, or len)
    #   'sections': OrderedDict of sec_id -> {
    #     'title': str,
    #     'text_start': int, # start of text after section header line
    #     'text_end': int,   # end of text (start of first ECCN, or len)
    #     'eccns': OrderedDict of eccn_id -> {'title': str, 'start': int, 'end': int}
    #   }
    # }
    cat_items: Dict[str, Any] = {}
    current_cat_id: Optional[str] = None
    current_sec_id: Optional[str] = None
    current_eccn_id: Optional[str] = None
    cat_text_start: Optional[int] = None   # open iff waiting for first section/eccn
    sec_text_start: Optional[int] = None   # open iff waiting for first eccn

    cat_key_counts: Dict[str, int] = {}
    sec_key_counts: Dict[str, int] = {}   # reset per category
    eccn_key_counts: Dict[str, int] = {}  # reset per section

    def make_unique(counts: Dict[str, int], raw_id: str) -> str:
        if raw_id in counts:
            counts[raw_id] += 1
            return f"{raw_id}_{counts[raw_id]}"
        counts[raw_id] = 0
        return raw_id

    n = len(boundaries)

    for i, (offset, btype, item_id, title) in enumerate(boundaries):

        if btype == 'category':
            # Close current ECCN
            if current_eccn_id is not None:
                cat_items[current_cat_id]['sections'][current_sec_id]['eccns'][current_eccn_id]['end'] = offset
                current_eccn_id = None
            # Close section text window if the section had no ECCNs (e.g., [Reserved])
            if current_cat_id is not None and current_sec_id is not None and sec_text_start is not None:
                cat_items[current_cat_id]['sections'][current_sec_id]['text_end'] = offset
                sec_text_start = None
            # Close category text window (text before first section of this cat)
            if current_cat_id is not None and cat_text_start is not None:
                cat_items[current_cat_id]['text_end'] = offset
                cat_text_start = None

            uid = make_unique(cat_key_counts, item_id)
            current_cat_id = uid
            current_sec_id = None
            sec_text_start = None
            sec_key_counts = {}    # section letters are unique within a category
            eccn_key_counts = {}   # ECCN IDs restart per category
            line_end = find_line_end(offset)
            cat_items[uid] = {
                'title': title,
                'text_start': line_end,
                'text_end': len(parent_text),
                'sections': {}
            }
            cat_text_start = line_end

        elif btype == 'section':
            # Close current ECCN
            if current_eccn_id is not None:
                cat_items[current_cat_id]['sections'][current_sec_id]['eccns'][current_eccn_id]['end'] = offset
                current_eccn_id = None
            # Close previous section text window if it had no ECCNs (e.g., [Reserved])
            if current_cat_id is not None and current_sec_id is not None and sec_text_start is not None:
                cat_items[current_cat_id]['sections'][current_sec_id]['text_end'] = offset
                sec_text_start = None
            # Close category text window (cat text ends at first section header)
            if current_cat_id is not None and cat_text_start is not None:
                cat_items[current_cat_id]['text_end'] = offset
                cat_text_start = None

            if current_cat_id is not None:
                uid = make_unique(sec_key_counts, item_id)
                current_sec_id = uid
                sec_text_start = None
                eccn_key_counts = {}   # ECCN IDs are unique within a section
                line_end = find_line_end(offset)
                cat_items[current_cat_id]['sections'][uid] = {
                    'title': title,
                    'text_start': line_end,
                    'text_end': len(parent_text),
                    'eccns': {}
                }
                sec_text_start = line_end

        elif btype == 'eccn':
            # Close current ECCN
            if current_eccn_id is not None:
                cat_items[current_cat_id]['sections'][current_sec_id]['eccns'][current_eccn_id]['end'] = offset
            # Close section text window (sec text ends at first ECCN)
            if current_sec_id is not None and sec_text_start is not None and current_cat_id is not None:
                cat_items[current_cat_id]['sections'][current_sec_id]['text_end'] = offset
                sec_text_start = None

            if current_cat_id is not None:
                if current_sec_id is None:
                    # ECCN before any section — create a synthetic default section
                    uid = make_unique(sec_key_counts, "default")
                    current_sec_id = uid
                    cat_items[current_cat_id]['sections'][uid] = {
                        'title': '',
                        'text_start': offset,
                        'text_end': offset,
                        'eccns': {}
                    }
                uid = make_unique(eccn_key_counts, item_id)
                current_eccn_id = uid
                cat_items[current_cat_id]['sections'][current_sec_id]['eccns'][uid] = {
                    'title': title,
                    'start': offset,
                    'end': len(parent_text)
                }

        elif btype == 'heading':
            pass  # Absorbed into current ECCN text; no state change

    # Finalize last ECCN
    if current_eccn_id is not None:
        cat_items[current_cat_id]['sections'][current_sec_id]['eccns'][current_eccn_id]['end'] = len(parent_text)

    # --- Phase 2: Build output structure ---

    # Supplement preamble: text before first category boundary
    first_cat_offset = next((b[0] for b in boundaries if b[1] == 'category'), len(parent_text))
    supp_preamble_text = clean_markers(parent_text[:first_cat_offset]).strip()

    cat_sub_units: Dict[str, Any] = {}
    sub_unit_index: Dict[str, Any] = {}

    if supp_preamble_text:
        cat_sub_units["_preamble"] = {
            "text": supp_preamble_text,
            "unit_title": "",
            "context": list(supplement_context),
            "breakpoints": slice_breakpoints(breakpoints, 0, first_cat_offset),
            "sub_units": {}
        }

    for cat_id, cat_data in cat_items.items():
        cat_context = list(supplement_context)
        # Category text: text between category header and first section header (if any)
        cat_text_raw = clean_markers(
            parent_text[cat_data['text_start']:cat_data['text_end']]
        ).strip()

        sec_sub_units: Dict[str, Any] = {}

        for sec_id, sec_data in cat_data['sections'].items():
            sec_context = list(supplement_context) + [{"ccl_category": cat_id}]
            # Section text: text between section header and first ECCN
            sec_text_raw = clean_markers(
                parent_text[sec_data['text_start']:sec_data['text_end']]
            ).strip()

            eccn_sub_units: Dict[str, Any] = {}

            # Text before the first ECCN in this section → _preamble ECCN entry
            if sec_text_raw:
                eccn_sub_units["_preamble"] = {
                    "text": sec_text_raw,
                    "unit_title": "",
                    "context": list(sec_context) + [{"ccl_section": sec_id}],
                    "breakpoints": slice_breakpoints(
                        breakpoints, sec_data['text_start'], sec_data['text_end']
                    )
                }

            for eccn_id, eccn_data in sec_data['eccns'].items():
                eccn_context = list(supplement_context) + [
                    {"ccl_category": cat_id}, {"ccl_section": sec_id}
                ]
                start = eccn_data['start']
                end = eccn_data['end']
                # Body text starts after the heading line (title already in unit_title)
                heading_end = parent_text.find('\n', start)
                body_start = heading_end + 1 if heading_end != -1 else end
                eccn_body_text = clean_markers(parent_text[body_start:end]).rstrip()
                # Some ECCNs have no body text — all meaningful content is in the heading,
                # which is stored in unit_title. Stage 3 skips AI summarization when text is
                # empty (assigning a trivial "is blank" string instead), so we use a sentinel
                # to force the AI summary path. The sentinel is NOT the title text, because
                # putting title content in text would cause Stage 2 to extract false definitions
                # from ECCN headings (the problem that was fixed in session 18). Instead, Stage 3
                # already includes unit_title in model-facing prompts, so the AI summarizer will
                # see the title and produce a meaningful summary even with this minimal text field.
                eccn_sub_units[eccn_id] = {
                    "text": eccn_body_text if eccn_body_text else "[No further unit content.]",
                    "unit_title": eccn_data['title'],
                    "context": eccn_context,
                    "breakpoints": slice_breakpoints(breakpoints, body_start, end)
                }
                # Index entry for fast lookup
                sub_unit_index[eccn_id] = {
                    "container_plural": container_plural,
                    "container_id": parent_number,
                    "path": [cat_key, cat_id, sec_key, sec_id]
                }

            sec_entry: Dict[str, Any] = {
                "text": "",
                "unit_title": sec_data['title'],
                "context": sec_context,
                "breakpoints": [],
            }
            # Only add sub_units when there are actual ECCN items (including _preamble).
            # An empty eccn_sub_units dict would produce {"<eccn_key>": {}} which looks
            # like a container to has_sub_units but contains no items — omitting the key
            # keeps the section a clean leaf item.
            if eccn_sub_units:
                sec_entry["sub_units"] = {eccn_key: eccn_sub_units}
            sec_sub_units[sec_id] = sec_entry

        cat_sub_units[cat_id] = {
            "text": cat_text_raw,
            "unit_title": cat_data['title'],
            "context": cat_context,
            "breakpoints": [],
            "sub_units": {sec_key: sec_sub_units}
        }

    return {cat_key: cat_sub_units}, sub_unit_index


def subdivide_into_eccns(
    parent_text: str,
    parent_context: List[Dict[str, str]],
    parent_type_name: str,
    parent_number: str,
    breakpoints: List[List[int]]
) -> Optional[Dict[str, dict]]:
    """
    Subdivide a long Commerce Control List text into ECCN sub-units.

    Args:
        parent_text: Full text of the parent substantive unit.
        parent_context: Organizational context of the parent.
        parent_type_name: Type name of the parent (e.g., "supplement").
        parent_number: Number/ID of the parent (e.g., "No. 1 to Part 774").
        breakpoints: Original breakpoints for the parent text.

    Returns:
        OrderedDict of sub-unit key -> sub-unit dict, or None if < 2 boundaries found.
    """
    boundaries = detect_eccn_boundaries(parent_text)
    if len(boundaries) < 2:
        return None

    sub_unit_context = parent_context + [{parent_type_name: parent_number}]
    sub_units = {}

    # Create _preamble for text before first boundary
    first_offset = boundaries[0][0]
    preamble_text = parent_text[:first_offset].rstrip()
    if preamble_text:
        preamble_bps = [
            [bp[0], bp[1]] for bp in breakpoints
            if bp[0] < first_offset
        ]
        sub_units["_preamble"] = {
            "text": preamble_text,
            "context": list(sub_unit_context),
            "breakpoints": preamble_bps,
            "unit_title": ""
        }

    # Create a sub-unit for each boundary
    # Track key usage to handle duplicates (e.g., multiple EAR99 entries)
    key_counts = {}

    for i, (offset, eccn_id, title_line) in enumerate(boundaries):
        # Determine end of this sub-unit's text
        if i + 1 < len(boundaries):
            end_offset = boundaries[i + 1][0]
        else:
            end_offset = len(parent_text)

        # Body text starts after the heading line (which is already in unit_title)
        heading_end = parent_text.find('\n', offset)
        body_start = heading_end + 1 if heading_end != -1 else end_offset
        sub_text = parent_text[body_start:end_offset].rstrip()

        # Compute relative breakpoints for this slice
        sub_bps = []
        for bp in breakpoints:
            if body_start <= bp[0] < end_offset:
                sub_bps.append([bp[0] - body_start, bp[1]])

        # Generate unique key for duplicates
        if eccn_id in key_counts:
            key_counts[eccn_id] += 1
            unique_key = f"{eccn_id}_{key_counts[eccn_id]}"
        else:
            key_counts[eccn_id] = 0
            unique_key = eccn_id

        sub_units[unique_key] = {
            "text": sub_text,
            "context": list(sub_unit_context),
            "breakpoints": sub_bps,
            "unit_title": title_line
        }

    return sub_units


def parse_appendix(
    appendix_elem: ET.Element,
    content: Dict[str, Any],
    org_pointer: Dict[str, Any],
    context: List[Dict[str, str]],
    parsing_logfile: Optional[str]
) -> None:
    """
    Parse an APPENDIX DIV element.

    DIV elements with TYPE="APPENDIX" may contain various item types which are
    detected and routed to the appropriate content section:
    - Appendix (type 2): stored in "appendices"
    - Supplement (type 3): stored in "supplements"
    - Special Federal Aviation Regulation (type 4): stored in "special federal aviation regulations"
    - Schedule (type 5): stored in "schedules"
    - Table (type 6): stored in "tables"
    - Annex (type 7): stored in "annexes"
    - Exhibit (type 8): stored in "exhibits"
    - Figure (type 9): stored in "figures"
    - Policy Statement (type 10): stored in "policy statements"
    """
    # Get raw ID from N attribute (e.g., "Appendix A to Part 17", "Supplement No. 1 to Part 714")
    raw_item_id = appendix_elem.get('N', '')

    # Detect item type and normalize the ID
    item_type_num, item_id, name_plural = detect_appendix_item_type(raw_item_id)
    item_name = content["document_information"]["parameters"][item_type_num]["name"]

    if not item_id:
        # Generate a blank ID
        if "_blank_appendix_counter" not in content:
            content["_blank_appendix_counter"] = 0
        content["_blank_appendix_counter"] += 1
        item_id = f"Blank_{content['_blank_appendix_counter']}"
        raw_item_id = item_id  # Use generated ID for title as well
        log_parsing_correction("", "blank_appendix_identifier",
                             f"APPENDIX without N attribute, assigned {item_id}", parsing_logfile)

    # Check if reserved
    head_elem = appendix_elem.find('HEAD')
    head_text = ''
    is_reserved = False
    if head_elem is not None:
        head_text = extract_element_text(head_elem)
        if '[Reserved]' in head_text or '[RESERVED]' in head_text:
            is_reserved = True

    # Large XML table elements collected during text assembly (populated in else branch).
    # Declared here so it's defined regardless of which branch runs.
    pending_large_tables: List[ET.Element] = []

    if is_reserved:
        item_entry = {
            "text": "",
            "unit_title": clean_unit_title(raw_item_id),
            "breakpoints": [],
            "notes": {},
            "context": list(context),
            "annotation": "Reserved"
        }
    else:
        # Use the full head text as title (includes item name)
        title = head_text if head_text else raw_item_id

        text_parts: List[str] = []
        annotation_parts: List[str] = []
        notes = extract_footnotes(appendix_elem)

        for child in list(appendix_elem):
            tag = child.tag.lower() if isinstance(child.tag, str) else ''
            tag_upper = tag.upper()

            if tag == 'img':
                # Image element - insert placeholder
                text_parts.append("[Image omitted]")
            elif tag_upper == 'HEAD':
                continue
            elif tag_upper == 'XREF':
                xref_text = extract_element_text(child)
                if xref_text:
                    annotation_parts.append(f"Amendment pending: {xref_text}")
            elif tag_upper in ('P', 'PSPACE'):
                para = extract_paragraph_text(child)
                if para:
                    text_parts.append(para)
            elif tag_upper in ('FP', 'FP1', 'FP2'):
                para = extract_paragraph_text(child)
                if para:
                    text_parts.append(para)
            # Additional flush paragraph variants
            elif tag_upper in ('FP-1', 'FP-2', 'FP1-2', 'FP2-2', 'FP2-3', 'FRP', 'FRP0'):
                para = extract_paragraph_text(child)
                if para:
                    text_parts.append(para)
            # Dash paragraph types - add line indicator after
            elif tag_upper in ('FP-DASH', 'P-DASH'):
                para = extract_paragraph_text(child)
                if para:
                    text_parts.append(para + " ________")
                else:
                    text_parts.append("________")
            # Half-dash - write-in line starting midway through column
            elif tag_upper == 'HALFDASH':
                text_parts.append("_____")
            # Additional numbered paragraph variants
            elif tag_upper in ('P-1', 'P-2', 'P-3', 'P1', 'P2'):
                para = extract_paragraph_text(child)
                if para:
                    text_parts.append(para)
            elif tag_upper == 'NOTE':
                note_text = extract_note_text(child)
                if note_text:
                    text_parts.append(f"Note: {note_text}")
            elif tag_upper == 'EXAMPLE':
                example_text = extract_note_text(child)
                if example_text:
                    text_parts.append(f"Example: {example_text}")
            elif tag_upper == 'EDNOTE':
                continue
            elif tag_upper == 'EXTRACT':
                extract_text = extract_block_text(child)
                if extract_text:
                    text_parts.append(extract_text)
            elif tag_upper == 'CITA':
                cita_text = extract_element_text(child)
                if cita_text:
                    annotation_parts.append(f"CITA: {cita_text}")
            elif tag_upper in ('FTNT', 'APPRO', 'PARAUTH', 'AUTH', 'SOURCE'):
                # Footnotes handled separately; approval/authority ignored
                continue
            elif tag_upper == 'EFFDNOT':
                # Effective date note - add to annotations
                effdnot_text = extract_effdnot_text(child)
                if effdnot_text:
                    annotation_parts.append(effdnot_text)
            elif tag_upper in ('HED1', 'PARTHD', 'DOCKETHD'):
                # Additional heading types
                hd_text = extract_element_text(child)
                if hd_text:
                    text_parts.append(f"\n{hd_text}\n")
            elif tag_upper.startswith('HD'):
                hd_text = extract_element_text(child)
                if hd_text:
                    if tag_upper == 'HD1':
                        text_parts.append(f"\n##HD1## {hd_text}\n")
                    else:
                        text_parts.append(f"\n{hd_text}\n")
            elif tag_upper == 'GPOTABLE' or tag_upper == 'TABLE':
                xml_row_count = _count_xml_table_rows(child)
                if xml_row_count >= LARGE_TABLE_ROW_THRESHOLD:
                    # Large table: collect for sub-unit extraction; add placeholder.
                    pending_large_tables.append(child)
                    local_num = len(pending_large_tables)
                    text_parts.append(f"[Table {local_num} pending sub-unit extraction]")
                else:
                    table_text = extract_table_text(child)
                    if table_text:
                        text_parts.append(table_text)
            # List items - each on own line
            elif tag_upper == 'LI':
                para = extract_paragraph_text(child)
                if para:
                    text_parts.append(para)
            # Two-column list container - extract contents
            elif tag_upper == 'SCOL2':
                scol_text = extract_block_text(child)
                if scol_text:
                    text_parts.append(scol_text)
            # Leader work (forms with left/right alignment)
            elif tag_upper == 'LDRWK':
                ldrwk_text = extract_ldrwk_text(child)
                if ldrwk_text:
                    text_parts.append(ldrwk_text)
            # Captions for images
            elif tag_upper in ('TCAP', 'BCAP'):
                caption = extract_element_text(child)
                if caption:
                    text_parts.append(clean_text(caption))
            elif tag_upper.startswith('DIV'):
                # Nested DIVs within appendix may contain tables and other content
                nested_parts = extract_nested_div_content(child, pending_large_tables)
                text_parts.extend(nested_parts)
            elif tag_upper == 'IMG' or tag_upper == 'GPH':
                # Image element - insert placeholder
                text_parts.append("[Image omitted]")

        text, breakpoints = assemble_text_and_breakpoints(text_parts)

        # If CCL subdivision will run and we have pending_large_tables, inline table
        # content in text_parts and reassemble so breakpoints stay correct. Replacing
        # in the final text would change its length and invalidate breakpoints.
        if (item_type_num == 3
                and len(text) >= ECCN_SUBDIVISION_THRESHOLD
                and pending_large_tables):
            for i, part in enumerate(text_parts):
                for local_num, xml_elem in enumerate(pending_large_tables, 1):
                    placeholder = f"[Table {local_num} pending sub-unit extraction]"
                    if part == placeholder:
                        text_parts[i] = extract_table_text(xml_elem) or ""
                        break
            text, breakpoints = assemble_text_and_breakpoints(text_parts)
            pending_large_tables.clear()

        item_entry = {
            "text": text,
            "unit_title": clean_unit_title(title),
            "breakpoints": breakpoints,
            "notes": notes,
            "context": list(context)
        }
        if annotation_parts:
            item_entry["annotation"] = " ".join(annotation_parts).strip()

    # Attempt CCL subdivision for long supplements (3-level: Category → Section → ECCN)
    if (item_type_num == 3
            and len(item_entry.get("text", "")) >= ECCN_SUBDIVISION_THRESHOLD):
        result = subdivide_ccl(
            item_entry["text"],
            item_entry["context"],
            item_name,        # e.g., "supplement"
            item_id,          # e.g., "No. 1 to Part 774"
            item_entry["breakpoints"],
            cat_param_key=12,
            sec_param_key=13,
            eccn_param_key=11,
        )
        if result:
            type_keyed_sub_units, index_entries = result
            # Count total leaf ECCNs for the log message
            total_eccns = len(index_entries)
            log_parsing_correction("", "ccl_subdivision",
                                   f"Subdivided {item_name} '{item_id}' ({len(item_entry['text']):,} chars) "
                                   f"into {total_eccns} ECCNs (3-level CCL hierarchy)", parsing_logfile)
            item_entry["text"] = ""
            item_entry["breakpoints"] = []
            item_entry["sub_units"] = type_keyed_sub_units

            # Ensure sub-unit parameter types exist
            param_pointer = content["document_information"]["parameters"]
            if 11 not in param_pointer:
                param_pointer[11] = {
                    "name": "eccn", "name_plural": "eccns",
                    "operational": 1, "is_sub_unit": True
                }
            if 12 not in param_pointer:
                param_pointer[12] = {
                    "name": "ccl_category", "name_plural": "ccl_categories",
                    "operational": 1, "is_sub_unit": True
                }
            if 13 not in param_pointer:
                param_pointer[13] = {
                    "name": "ccl_section", "name_plural": "ccl_sections",
                    "operational": 1, "is_sub_unit": True
                }

            # Write index to document_information (merge with any existing entries)
            di = content["document_information"]
            if "sub_unit_index" not in di:
                di["sub_unit_index"] = {}
            idx = di["sub_unit_index"]
            eccn_key_str = str(11)
            if eccn_key_str not in idx:
                idx[eccn_key_str] = {}
            idx[eccn_key_str].update(index_entries)

    # Register XML-intercepted large tables as sub-units.
    # Guard: skip if the item already has sub_units (e.g., CCL subdivision ran above).
    if pending_large_tables and not item_entry.get("sub_units"):
        param_pointer = content["document_information"]["parameters"]
        table_param_key = find_or_create_table_param_key(param_pointer)
        table_key_str = str(table_param_key)
        di = content["document_information"]
        di.setdefault("sub_unit_index", {})
        di["sub_unit_index"].setdefault(table_key_str, {})
        taken: set = set(di["sub_unit_index"][table_key_str].keys())
        table_sub_units: Dict[str, Any] = {}
        index_entries: Dict[str, Any] = {}
        current_text = item_entry["text"]
        for local_num, xml_elem in enumerate(pending_large_tables, 1):
            sub_unit_key = assign_table_key(local_num, item_id, taken)
            taken.add(sub_unit_key)
            sub_unit = _build_xml_table_sub_unit(
                xml_elem, local_num, item_entry["context"], item_name, item_id
            )
            # Replace the pending placeholder with the final key-based marker.
            current_text = current_text.replace(
                f"[Table {local_num} pending sub-unit extraction]",
                f"[Table {local_num} extracted as sub-unit table {sub_unit_key}]",
            )
            table_sub_units[sub_unit_key] = sub_unit
            index_entries[sub_unit_key] = {
                "container_plural": name_plural,
                "container_id": item_id,
                "path": [item_id],
            }
        item_entry["text"] = current_text
        item_entry["sub_units"] = {table_key_str: table_sub_units}
        di["sub_unit_index"][table_key_str].update(index_entries)
        log_parsing_correction("", "large_table_extraction",
                               f"Extracted {len(table_sub_units)} large table(s) from "
                               f"{item_name} '{item_id}'", parsing_logfile)

    # Get the target content section for this item type
    target_dict = content["content"][name_plural]

    # Handle duplicates with special logic
    final_id = handle_appendix_duplicate(
        target_dict, item_id, item_entry, parsing_logfile
    )

    if final_id:
        content["content"][name_plural][final_id] = item_entry
        update_begin_stop(org_pointer, item_name, final_id)


def extract_head_text(elem: ET.Element) -> str:
    """Extract text from the HEAD child element."""
    head = elem.find('HEAD')
    if head is not None:
        return clean_text(extract_element_text(head))
    return ''


def extract_element_text(elem: ET.Element) -> str:
    """Extract all text content from an element, including nested elements."""
    parts: List[str] = []

    if elem.text:
        parts.append(elem.text)

    for child in elem:
        child_tag = child.tag.lower() if isinstance(child.tag, str) else ''
        child_tag_upper = child_tag.upper()

        if child_tag == 'img':
            # Image element - insert placeholder
            parts.append("[Image omitted]")
        elif child_tag_upper == 'I':
            # Italics - wrap in asterisks for markdown-style formatting
            inner = extract_element_text(child)
            if inner:
                parts.append(f"*{inner}*")
        elif child_tag_upper in ('B', 'STRONG'):
            # Bold - wrap in double asterisks for markdown-style formatting
            inner = extract_element_text(child)
            if inner:
                parts.append(f"**{inner}**")
        elif child_tag_upper == 'EM':
            # Emphasis (HTML-style) - treat as bold
            inner = extract_element_text(child)
            if inner:
                parts.append(f"**{inner}**")
        elif child_tag_upper == 'E':
            # Emphasis element (still used in some places)
            inner = extract_element_text(child)
            t_value = str(child.get("T", "")).strip()
            if t_value == "03":
                parts.append(f"*{inner}*")
            elif t_value in ["52", "54"]:
                parts.append(f"<sub>{inner}</sub>")
            elif t_value == "51":
                parts.append(apply_overbar(inner))
            else:
                parts.append(inner)
        elif child_tag_upper == 'FR':
            # Fraction
            fr_text = clean_text(child.text or "").strip()
            parts.append(convert_fraction(fr_text))
        elif child_tag_upper == 'SU':
            # Superscript (usually footnote marker) - preserve as text
            parts.append(clean_text(child.text or ""))
        elif child_tag_upper == 'SUP':
            # HTML-style superscript - preserve formatting
            inner = extract_element_text(child)
            if inner:
                parts.append(f"<sup>{inner}</sup>")
        elif child_tag_upper == 'SUB':
            # HTML-style subscript - preserve formatting
            inner = extract_element_text(child)
            if inner:
                parts.append(f"<sub>{inner}</sub>")
        elif child_tag_upper == 'AC':
            # Accent element - extract text
            parts.append(extract_element_text(child))
        else:
            # Recursively extract text from other elements
            parts.append(extract_element_text(child))

        if child.tail:
            parts.append(child.tail)

    return ''.join(parts)


def extract_paragraph_text(p_elem: ET.Element) -> str:
    """Extract text from a P element with proper formatting."""
    parts: List[str] = []
    last_su = ""
    prev_trailing_space = True  # whether last raw text fragment ended with whitespace

    def append_fragment(text: Optional[str], force_no_space: bool = False) -> None:
        nonlocal prev_trailing_space
        if not text:
            return
        # Check raw boundaries BEFORE cleaning/stripping for correct space decisions
        raw_starts_space = text[0].isspace()
        raw_ends_space = text[-1].isspace()
        cleaned = clean_text(text).strip()
        if not cleaned:
            # Whitespace-only fragment: propagate trailing-space state but add nothing
            if raw_ends_space:
                prev_trailing_space = True
            return
        if parts and not force_no_space:
            # Add a space only if the original XML had whitespace at this boundary:
            # either the preceding fragment ended with space, or this one starts with space.
            needs_space = prev_trailing_space or raw_starts_space
            if needs_space and cleaned[0] not in [".", ",", ";", ":", ")", "]", "%"]:
                parts.append(" ")
        prev_trailing_space = raw_ends_space
        parts.append(cleaned)

    if p_elem.text:
        append_fragment(p_elem.text)

    for child in list(p_elem):
        tag = child.tag.upper() if isinstance(child.tag, str) else ''

        if tag == 'I':
            # Italics
            inner = extract_element_text(child)
            if inner:
                inner_clean = clean_text(inner).strip()
                if inner_clean:
                    append_fragment(f"*{inner_clean}*")
        elif tag in ('B', 'STRONG'):
            # Bold - wrap in double asterisks for markdown
            inner = extract_element_text(child)
            if inner:
                inner_clean = clean_text(inner).strip()
                if inner_clean:
                    append_fragment(f"**{inner_clean}**")
        elif tag == 'EM':
            # Emphasis (HTML-style) - treat as bold
            inner = extract_element_text(child)
            if inner:
                inner_clean = clean_text(inner).strip()
                if inner_clean:
                    append_fragment(f"**{inner_clean}**")
        elif tag == 'E':
            # Emphasis
            formatted = format_e_text(child)
            if formatted:
                append_fragment(formatted)
        elif tag == 'FR':
            # Fraction
            fr_text = clean_text(child.text or "").strip()
            fraction = convert_fraction(fr_text)
            if parts and parts[-1] and parts[-1][-1].isdigit():
                parts.append(fraction)
                prev_trailing_space = False  # fraction strings never end with whitespace
            else:
                append_fragment(fraction)
            if child.tail:
                tail_clean = clean_text(child.tail).strip()
                if tail_clean.startswith('-'):
                    parts.append(tail_clean)
                    prev_trailing_space = child.tail[-1].isspace()
                else:
                    append_fragment(child.tail)
            continue
        elif tag == 'SU':
            last_su = clean_text(child.text or "")
        elif tag == 'SUP':
            # HTML-style superscript - preserve formatting
            # force_no_space: superscripts attach to preceding text (e.g., x<sup>2</sup>)
            inner = extract_element_text(child)
            if inner:
                append_fragment(f"<sup>{inner}</sup>", force_no_space=True)
        elif tag == 'SUB':
            # HTML-style subscript - preserve formatting
            # force_no_space: subscripts attach to preceding text (e.g., CO<sub>2</sub>)
            inner = extract_element_text(child)
            if inner:
                append_fragment(f"<sub>{inner}</sub>", force_no_space=True)
        elif tag == 'FTREF':
            if last_su:
                append_fragment(f"[{last_su}]")
            else:
                append_fragment("[*]")
        elif tag == 'AC':
            inner = extract_element_text(child)
            if inner:
                append_fragment(inner)
        else:
            inner = extract_element_text(child)
            if inner:
                append_fragment(inner)

        if child.tail:
            append_fragment(child.tail)

    return clean_text("".join(parts))


def format_e_text(e_elem: ET.Element) -> str:
    """Format an E element with its content and apply appropriate styling."""
    text = extract_element_text(e_elem)
    if not text:
        return ""
    text = clean_text(text).strip()
    t_value = str(e_elem.get("T", "")).strip()
    if t_value == "03":
        return f"*{text}*"
    if t_value in ["52", "54"]:
        return f"<sub>{text}</sub>"
    if t_value == "51":
        return apply_overbar(text)
    return text


def apply_overbar(text: str) -> str:
    """Apply combining overbar to text characters."""
    return "".join(ch + "\u0305" if not ch.isspace() else ch for ch in text)


def extract_note_text(note_elem: ET.Element) -> str:
    """Extract text from a NOTE element."""
    parts: List[str] = []

    # Look for HED (header) element
    hed = note_elem.find('HED')
    if hed is not None:
        hed_text = extract_element_text(hed)
        if hed_text and hed_text.strip().lower() != 'note:':
            parts.append(clean_text(hed_text))

    # Extract paragraphs and handle images
    for child in note_elem:
        tag = child.tag.upper() if isinstance(child.tag, str) else ''
        if tag == 'P':
            para = extract_paragraph_text(child)
            if para:
                parts.append(para)
        elif tag == 'IMG' or tag == 'GPH':
            parts.append("[Image omitted]")

    return ' '.join(parts)


def extract_effdnot_text(effdnot_elem: ET.Element) -> str:
    """Extract text from an EFFDNOT (effective date note) element."""
    parts: List[str] = []

    # Look for HED (header) element
    hed = effdnot_elem.find('HED')
    if hed is not None:
        hed_text = extract_element_text(hed)
        if hed_text:
            parts.append(clean_text(hed_text))

    # Extract paragraphs (PSPACE, P)
    for child in effdnot_elem:
        tag = child.tag.upper() if isinstance(child.tag, str) else ''
        if tag in ('PSPACE', 'P'):
            para = extract_paragraph_text(child)
            if para:
                parts.append(para)
        elif tag in ('REVTXT', 'SUPERSED'):
            # These contain revised/superseded text - note their presence
            parts.append(f"[{tag} content omitted]")

    return ' '.join(parts)


def extract_block_text(elem: ET.Element) -> str:
    """Extract text from a block element like EXTRACT."""
    parts: List[str] = []

    for child in elem.iter():
        tag = child.tag.upper() if isinstance(child.tag, str) else ''

        # Standard paragraph types
        if tag in ('P', 'FP', 'FP1', 'FP2', 'PSPACE'):
            para = extract_paragraph_text(child)
            if para:
                parts.append(para)
        # Additional flush paragraph variants
        elif tag in ('FP-1', 'FP-2', 'FP1-2', 'FP2-2', 'FP2-3', 'FRP', 'FRP0'):
            para = extract_paragraph_text(child)
            if para:
                parts.append(para)
        # Dash paragraph types - add line indicator after
        elif tag in ('FP-DASH', 'P-DASH'):
            para = extract_paragraph_text(child)
            if para:
                parts.append(para + " ________")
            else:
                parts.append("________")
        # Half-dash - write-in line starting midway through column
        elif tag == 'HALFDASH':
            parts.append("_____")
        # Additional numbered paragraph variants
        elif tag in ('P-1', 'P-2', 'P-3', 'P1', 'P2'):
            para = extract_paragraph_text(child)
            if para:
                parts.append(para)
        # List items - each on own line
        elif tag == 'LI':
            para = extract_paragraph_text(child)
            if para:
                parts.append(para)
        # Captions
        elif tag in ('TCAP', 'BCAP'):
            caption = extract_element_text(child)
            if caption:
                parts.append(clean_text(caption))
        # Leader work (forms with left/right alignment)
        elif tag == 'LDRWK':
            ldrwk_text = extract_ldrwk_text(child)
            if ldrwk_text:
                parts.append(ldrwk_text)
        # Headings
        elif tag in ('HED1', 'PARTHD', 'DOCKETHD') or tag.startswith('HD'):
            hd_text = extract_element_text(child)
            if hd_text:
                parts.append(f"\n{clean_text(hd_text)}\n")
        elif tag == 'IMG' or tag == 'GPH':
            # Image element - insert placeholder
            parts.append("[Image omitted]")

    return '\n'.join(parts)


def extract_ldrwk_text(ldrwk_elem: ET.Element) -> str:
    """
    Extract text from a LDRWK (leader work) element.

    LDRWK contains FL-2 elements (left-aligned) and LDRFIG elements (right-aligned).
    Each FL-2 starts a new line, followed by spacing and any LDRFIG on the same line.
    """
    lines: List[str] = []
    current_line_parts: List[str] = []

    for child in ldrwk_elem:
        tag = child.tag.upper() if isinstance(child.tag, str) else ''

        if tag == 'FL-2':
            # Start a new line with FL-2 content
            if current_line_parts:
                lines.append(''.join(current_line_parts))
                current_line_parts = []
            text = extract_element_text(child)
            if text:
                current_line_parts.append(clean_text(text))
        elif tag == 'LDRFIG':
            # Add to current line with spacing
            text = extract_element_text(child)
            if text:
                if current_line_parts:
                    current_line_parts.append('     ')  # 5 spaces
                current_line_parts.append(clean_text(text))

    # Don't forget the last line
    if current_line_parts:
        lines.append(''.join(current_line_parts))

    return '\n'.join(lines)


def extract_nested_div_content(
    div_elem: ET.Element,
    pending_large_tables: Optional[List[ET.Element]] = None,
) -> List[str]:
    """
    Extract text content from a nested DIV element, including tables.

    This handles DIV elements that appear inside SECTION or APPENDIX elements
    and may contain paragraphs, tables, headings, and other content.

    If pending_large_tables is provided (e.g. from parse_appendix), large
    GPOTABLE/TABLE elements (>= LARGE_TABLE_ROW_THRESHOLD rows) are appended
    to it and replaced with placeholders; they will be extracted as sub-units
    by the caller. Otherwise, tables are converted to text via extract_table_text.

    Args:
        div_elem: The DIV element to extract content from
        pending_large_tables: Optional list to collect large table elements for
            sub-unit extraction. When provided, large tables are deferred
            instead of converted to text.

    Returns:
        List of text parts extracted from the DIV
    """
    text_parts: List[str] = []

    for child in list(div_elem):
        tag = child.tag.upper() if isinstance(child.tag, str) else ''

        if tag in ('P', 'PSPACE'):
            para = extract_paragraph_text(child)
            if para:
                text_parts.append(para)
        elif tag in ('FP', 'FP1', 'FP2'):
            para = extract_paragraph_text(child)
            if para:
                text_parts.append(para)
        # Additional flush paragraph variants
        elif tag in ('FP-1', 'FP-2', 'FP1-2', 'FP2-2', 'FP2-3', 'FRP', 'FRP0'):
            para = extract_paragraph_text(child)
            if para:
                text_parts.append(para)
        # Dash paragraph types - add line indicator after
        elif tag in ('FP-DASH', 'P-DASH'):
            para = extract_paragraph_text(child)
            if para:
                text_parts.append(para + " ________")
            else:
                text_parts.append("________")
        # Half-dash - write-in line starting midway through column
        elif tag == 'HALFDASH':
            text_parts.append("_____")
        # Additional numbered paragraph variants
        elif tag in ('P-1', 'P-2', 'P-3', 'P1', 'P2'):
            para = extract_paragraph_text(child)
            if para:
                text_parts.append(para)
        elif tag in ('HED1', 'PARTHD', 'DOCKETHD'):
            # Additional heading types
            hd_text = extract_element_text(child)
            if hd_text:
                text_parts.append(f"\n{hd_text}\n")
        elif tag.startswith('HD'):
            hd_text = extract_element_text(child)
            if hd_text:
                text_parts.append(f"\n{hd_text}\n")
        elif tag == 'GPOTABLE' or tag == 'TABLE':
            if pending_large_tables is not None:
                xml_row_count = _count_xml_table_rows(child)
                if xml_row_count >= LARGE_TABLE_ROW_THRESHOLD:
                    pending_large_tables.append(child)
                    local_num = len(pending_large_tables)
                    text_parts.append(f"[Table {local_num} pending sub-unit extraction]")
                    continue
            table_text = extract_table_text(child)
            if table_text:
                text_parts.append(table_text)
        elif tag == 'NOTE':
            note_text = extract_note_text(child)
            if note_text:
                text_parts.append(f"Note: {note_text}")
        elif tag == 'EXAMPLE':
            example_text = extract_note_text(child)
            if example_text:
                text_parts.append(f"Example: {example_text}")
        elif tag == 'EXTRACT':
            extract_text = extract_block_text(child)
            if extract_text:
                text_parts.append(extract_text)
        # List items - each on own line
        elif tag == 'LI':
            para = extract_paragraph_text(child)
            if para:
                text_parts.append(para)
        # Two-column list container - extract contents
        elif tag == 'SCOL2':
            scol_text = extract_block_text(child)
            if scol_text:
                text_parts.append(scol_text)
        # Leader work (forms with left/right alignment)
        elif tag == 'LDRWK':
            ldrwk_text = extract_ldrwk_text(child)
            if ldrwk_text:
                text_parts.append(ldrwk_text)
        # Captions for images
        elif tag in ('TCAP', 'BCAP'):
            caption = extract_element_text(child)
            if caption:
                text_parts.append(clean_text(caption))
        elif tag.startswith('DIV'):
            # Recursively handle nested DIVs
            nested_parts = extract_nested_div_content(child, pending_large_tables)
            text_parts.extend(nested_parts)
        elif tag == 'IMG' or tag == 'GPH':
            # Image element - insert placeholder
            text_parts.append("[Image omitted]")

    return text_parts


def process_table_element(elem: ET.Element) -> None:
    """
    Process a table element in-place, applying text transformations.

    Transforms:
    - FR elements: Convert fraction text to Unicode characters
    - SU elements: Convert superscript markers to [n] notation
    - img elements: Replace with [Image omitted] text
    """
    # Process text content
    if elem.text:
        elem.text = clean_text(elem.text)

    # Process children
    children_to_process = list(elem)
    for child in children_to_process:
        child_tag = child.tag.upper() if isinstance(child.tag, str) else ''

        if child_tag == 'FR':
            # Convert fraction to Unicode
            fr_text = clean_text(child.text or "").strip()
            unicode_frac = convert_fraction(fr_text)
            # Replace FR element with its converted text
            if child.tail:
                unicode_frac += child.tail
            parent = elem
            idx = list(parent).index(child)
            parent.remove(child)
            if idx == 0:
                parent.text = (parent.text or '') + unicode_frac
            else:
                prev = list(parent)[idx - 1]
                prev.tail = (prev.tail or '') + unicode_frac

        elif child_tag == 'SU':
            # Convert superscript to [n] notation
            su_text = clean_text(child.text or "").strip()
            notation = f"[{su_text}]" if su_text else ""
            if child.tail:
                notation += child.tail
            parent = elem
            idx = list(parent).index(child)
            parent.remove(child)
            if idx == 0:
                parent.text = (parent.text or '') + notation
            else:
                prev = list(parent)[idx - 1]
                prev.tail = (prev.tail or '') + notation

        elif child.tag == 'img':
            # Replace image with placeholder
            placeholder = "[Image omitted]"
            if child.tail:
                placeholder += child.tail
            parent = elem
            idx = list(parent).index(child)
            parent.remove(child)
            if idx == 0:
                parent.text = (parent.text or '') + placeholder
            else:
                prev = list(parent)[idx - 1]
                prev.tail = (prev.tail or '') + placeholder

        else:
            # Recursively process child elements
            process_table_element(child)

        # Process tail text
        if child.tail:
            child.tail = clean_text(child.tail)


def extract_table_text(table_elem: ET.Element) -> str:
    """
    Extract table content as processed HTML/XML.

    For HTML-style TABLE elements, returns the HTML with text transformations
    applied (fractions converted to Unicode, superscripts to [n] notation,
    images replaced with placeholders).

    For GPOTABLE format, falls back to simple pipe-separated extraction.

    Note: A plain-text table conversion function (html_table_to_plaintext) exists
    in utils/text_processing.py but is not yet accurate enough for general use.
    It may be used in the future for human-readable output presentation.
    """
    table_tag = table_elem.tag.upper() if isinstance(table_elem.tag, str) else ''

    if table_tag == 'TABLE':
        try:
            # Make a deep copy to avoid modifying the original
            import copy
            table_copy = copy.deepcopy(table_elem)

            # Process text transformations in-place
            process_table_element(table_copy)

            # Serialize to HTML string
            table_html = ET.tostring(table_copy, encoding='unicode', method='html')
            return table_html
        except Exception:
            pass  # Fall through to simple extraction

    # Simple extraction for GPOTABLE or fallback
    rows: List[str] = []

    for row in table_elem.iter():
        row_tag = row.tag.upper() if isinstance(row.tag, str) else ''
        if row_tag == 'TR':
            cells: List[str] = []
            for cell in row:
                cell_tag = cell.tag.upper() if isinstance(cell.tag, str) else ''
                if cell_tag in ('TD', 'TH', 'ENT'):
                    cell_text = extract_element_text(cell)
                    if cell_text:
                        cells.append(clean_text(cell_text).strip())
            if cells:
                rows.append(' | '.join(cells))
        elif row_tag == 'ROW':
            # GPOTABLE format
            cells = []
            for ent in row.findall('ENT'):
                ent_text = extract_element_text(ent)
                if ent_text:
                    cells.append(clean_text(ent_text).strip())
            if cells:
                rows.append(' | '.join(cells))

    return '\n'.join(rows)


def extract_footnotes(elem: ET.Element) -> Dict[str, str]:
    """Extract footnotes from an element."""
    notes: Dict[str, str] = {}
    for ftnt in elem.findall(".//FTNT"):
        # Find the SU (superscript) marker
        su = ftnt.find(".//SU")
        su_text = clean_text(su.text or "") if su is not None else ""
        if not su_text:
            continue

        # Extract the footnote text from P elements
        p = ftnt.find(".//P")
        note_text = extract_paragraph_text(p) if p is not None else ""
        if note_text:
            notes[su_text] = note_text
    return notes


def assemble_text_and_breakpoints(paragraphs: List[str]) -> Tuple[str, List[List[int]]]:
    """Assemble paragraphs into text with breakpoints."""
    text = ""
    breakpoints: List[List[int]] = []
    for para in paragraphs:
        if not para:
            continue
        pos = len(text)
        if text:
            text += "\n"
            pos = len(text)
        priority = 2
        if re.match(r"^\(\w+\)", para.strip()):
            priority = 1
        if pos > 0:
            breakpoints.append([pos, priority])
        text += para
    return text, breakpoints


def ensure_unique_id(target_dict: Dict[str, Any], base_id: str, parsing_logfile: Optional[str],
                     item_label: str = "identifier") -> str:
    """Ensure ID is unique, appending _dup if necessary."""
    if base_id not in target_dict:
        return base_id
    original = base_id
    while base_id in target_dict:
        base_id = f"{base_id}_dup"
    log_parsing_correction("", "duplicate_identifier",
                          f"Duplicate {item_label} {original}, created {base_id}", parsing_logfile)
    return base_id


def handle_appendix_duplicate(
    target_dict: Dict[str, Any],
    item_id: str,
    new_entry: Dict[str, Any],
    parsing_logfile: Optional[str]
) -> Optional[str]:
    """
    Handle duplicate entries for appendix-like items with special logic.

    This function is used for all item types parsed from APPENDIX DIV elements:
    appendices, supplements, special federal aviation regulations, schedules, and tables.

    Rules:
    - If new item has no text and ID already exists → skip (return None)
    - If new item has text and existing one has no text → replace existing (return original ID)
    - If both have text → create duplicate with _dup suffix (return new ID)
    - If ID doesn't exist → use original ID (return original ID)

    Args:
        target_dict: The dictionary to store the item in
        item_id: The identifier for the item
        new_entry: The new entry to potentially store
        parsing_logfile: Optional log file for parsing corrections

    Returns:
        The ID to use for storing the entry, or None if entry should be skipped.
    """
    new_has_text = bool(new_entry.get("text", "").strip())

    # If ID doesn't exist, just use it
    if item_id not in target_dict:
        return item_id

    # ID already exists - check existing entry
    existing_entry = target_dict[item_id]
    existing_has_text = bool(existing_entry.get("text", "").strip())

    if not new_has_text:
        # New entry has no text - skip it (existing entry takes precedence)
        log_parsing_correction("", "duplicate_item_skipped",
                              f"Skipped duplicate item '{item_id}' with no text (keeping existing entry)",
                              parsing_logfile)
        return None

    if new_has_text and not existing_has_text:
        # New entry has text, existing doesn't - replace existing
        log_parsing_correction("", "duplicate_item_replaced",
                              f"Replaced item '{item_id}' (had no text) with version containing text",
                              parsing_logfile)
        return item_id

    # Both have text - this is a real duplicate, create _dup suffix
    dup_id = item_id
    while dup_id in target_dict:
        dup_id = f"{dup_id}_dup"
    log_parsing_correction("", "duplicate_identifier",
                          f"Duplicate item {item_id}, created {dup_id}", parsing_logfile)
    return dup_id


def update_begin_stop(org_pointer: Dict[str, Any], item_name: str, item_id: str) -> None:
    """Update begin/stop markers in organization structure."""
    begin_tag = f"begin_{item_name}"
    stop_tag = f"stop_{item_name}"
    if begin_tag not in org_pointer:
        org_pointer[begin_tag] = ""
    if org_pointer[begin_tag] == "":
        org_pointer[begin_tag] = item_id
    org_pointer[stop_tag] = item_id


def ensure_org_unit(parent_pointer: Dict[str, Any], unit_type: str, unit_id: str,
                    unit_title: str) -> Dict[str, Any]:
    """Ensure organizational unit exists in structure."""
    if unit_type not in parent_pointer:
        parent_pointer[unit_type] = {}
    if unit_id not in parent_pointer[unit_type]:
        parent_pointer[unit_type][unit_id] = {}
    unit_pointer = parent_pointer[unit_type][unit_id]
    if unit_title and "unit_title" not in unit_pointer:
        unit_pointer["unit_title"] = clean_unit_title(unit_title)
    return unit_pointer


def context_matches(specific_units: Dict[str, str], context: List[Dict[str, str]]) -> bool:
    """Check if context matches specific unit filter."""
    if not specific_units:
        return True
    flat_context = {}
    for entry in context:
        for k, v in entry.items():
            flat_context[k] = v
    for key, value in specific_units.items():
        if key not in flat_context:
            return False
        if str(flat_context[key]).strip() != str(value).strip():
            return False
    return True


def detect_part_units(file_path: str) -> List[Dict[str, str]]:
    """
    Detect all parts in an eCFR file for splitting.

    Returns list of unit dicts (e.g., {'title': '14', 'chapter': 'I', 'part': '1'})
    """
    tree = ET.parse(file_path)
    root = tree.getroot()

    units: List[Dict[str, str]] = []

    def collect_parts(elem: ET.Element, context: Dict[str, str]) -> None:
        for child in list(elem):
            if not isinstance(child.tag, str) or not child.tag.upper().startswith('DIV'):
                continue

            div_type = child.get('TYPE', '').upper()
            div_id = child.get('N', '')

            new_context = dict(context)

            if div_type == 'TITLE' and div_id:
                new_context['title'] = div_id
                collect_parts(child, new_context)
            elif div_type == 'SUBTITLE' and div_id:
                new_context['subtitle'] = div_id
                collect_parts(child, new_context)
            elif div_type == 'CHAPTER' and div_id:
                new_context['chapter'] = div_id
                collect_parts(child, new_context)
            elif div_type == 'SUBCHAP' and div_id:
                new_context['subchapter'] = div_id
                collect_parts(child, new_context)
            elif div_type == 'PART' and div_id:
                new_context['part'] = div_id
                units.append(new_context)
            else:
                collect_parts(child, new_context)

    collect_parts(root, {})
    return units


def build_unit_suffix(unit: Dict[str, str]) -> str:
    """Build filename suffix from unit dict."""
    parts = []
    for key in ["title", "subtitle", "chapter", "subchapter", "part", "subpart"]:
        if key in unit and unit[key]:
            parts.append(f"{key}{unit[key]}")
    if not parts:
        return ""
    return "_" + "_".join(parts)


def process_file(
    input_file_path: str,
    config: Optional[Dict[str, Any]] = None,
    parse_mode: str = "auto",
    specific_units: Optional[Dict[str, str]] = None
) -> None:
    """
    Process an eCFR XML file with manifest support.

    Args:
        input_file_path: Path to input XML file
        config: Configuration dictionary (optional; loaded from config.json if None)
        parse_mode: 'auto', 'split', or 'full'
        specific_units: Optional dict of units for filtered parsing
    """
    config = config or get_config()
    file_name = os.path.basename(input_file_path)
    file_stem = re.sub(r'\.\w+$', '', file_name)

    print(f'Processing: {file_name}')

    output_dir = get_output_directory(config)
    output_structure = get_output_structure(config)

    dir_stem = os.path.basename(os.path.dirname(os.path.abspath(input_file_path)))
    if output_structure == 'per_document':
        doc_output_dir = os.path.join(output_dir, dir_stem)
    else:
        doc_output_dir = output_dir

    Path(doc_output_dir).mkdir(parents=True, exist_ok=True)

    title_name = get_title_name(input_file_path)
    parsed_output_dir = create_title_output_dir(doc_output_dir, title_name)

    manifest_path = get_manifest_path(doc_output_dir, file_stem)
    manifest_mgr = ManifestManager(manifest_path)
    manifest = manifest_mgr.create_or_load(
        source_file=os.path.abspath(input_file_path),
        source_type='ecfr',
        parser='cfr_set_parse.py',
        parser_type='ecfr'
    )

    parsing_logfile = get_parsing_issues_logfile(doc_output_dir)

    if parse_mode == 'auto':
        parse_mode = 'full'

    if parse_mode == 'split':
        split_units = detect_part_units(input_file_path)
        if specific_units:
            split_units = [u for u in split_units if context_matches(specific_units, [u])]
        if not split_units:
            parse_mode = 'full'
        else:
            for unit in split_units:
                parsed_content = parse_ecfr(input_file_path, parsing_logfile, specific_units=unit)
                if not parsed_content:
                    continue
                suffix = build_unit_suffix(unit)
                output_filename = f'{file_stem}{suffix}_parse_output.json'
                output_path = os.path.join(parsed_output_dir, output_filename)
                with open(output_path, 'w', encoding='utf-8') as f:
                    json.dump(parsed_content, f, indent=4, ensure_ascii=False)

                manifest_mgr.add_parsed_file(
                    manifest,
                    output_path,
                    'split_unit',
                    organizational_units=unit
                )

            # Residual pass: capture non-Part content (title/chapter-level appendices, etc.)
            # Only done for full-title splits, not when a specific_units filter is active.
            if not specific_units:
                residual_content = parse_ecfr(input_file_path, parsing_logfile, residual_mode=True)
                if residual_content and residual_content.get("content"):
                    residual_filename = f'{file_stem}_non_part_parse_output.json'
                    residual_path = os.path.join(parsed_output_dir, residual_filename)
                    with open(residual_path, 'w', encoding='utf-8') as f:
                        json.dump(residual_content, f, indent=4, ensure_ascii=False)
                    manifest_mgr.add_parsed_file(
                        manifest,
                        residual_path,
                        'non_part',
                        organizational_units={}
                    )
                    print(f'  Wrote non-part residual: {residual_filename}')

            manifest_mgr.update_short_title(manifest,
                parsed_content.get('document_information', {}).get('title', ''))
            manifest_mgr.save(manifest)
            print(f'  Wrote split outputs to: {parsed_output_dir}')
            return

    parsed_content = parse_ecfr(input_file_path, parsing_logfile, specific_units=specific_units)
    if not parsed_content:
        print('  Parsing returned empty content')
        return

    suffix = build_unit_suffix(specific_units) if specific_units else ""
    output_filename = f'{file_stem}{suffix}_parse_output.json'
    output_path = os.path.join(parsed_output_dir, output_filename)
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(parsed_content, f, indent=4, ensure_ascii=False)

    manifest_mgr.add_parsed_file(
        manifest,
        output_path,
        'full_document',
        organizational_units={}
    )
    manifest_mgr.update_short_title(manifest,
        parsed_content.get('document_information', {}).get('title', ''))
    manifest_mgr.save(manifest)
    print(f'  Output written: {output_path}')


def process_directory(dir_path: str, config: Optional[Dict[str, Any]] = None,
                      recursive: bool = True) -> None:
    """
    Process directory of eCFR XML files.

    Args:
        dir_path: Path to directory or file
        config: Configuration dictionary (optional)
        recursive: Whether to process subdirectories
    """
    config = config or get_config()

    if os.path.isfile(dir_path):
        if dir_path.endswith('.xml'):
            process_file(dir_path, config)
        return

    if not os.path.isdir(dir_path):
        raise ParseError('Input not a directory or file.')

    for item in os.listdir(dir_path):
        item_path = os.path.join(dir_path, item)
        if os.path.isfile(item_path) and item.endswith('.xml'):
            process_file(item_path, config)
        elif recursive and os.path.isdir(item_path):
            print(f'Moving to directory: {item_path}')
            process_directory(item_path, config, recursive)


def main() -> None:
    """Main entry point for eCFR parser."""
    import argparse

    parser = argparse.ArgumentParser(
        description='Parse eCFR XML files into structured JSON.'
    )
    parser.add_argument(
        'input_path',
        help='Path to eCFR XML file or directory containing eCFR XML files'
    )
    parser.add_argument(
        '--config',
        default='config.json',
        help='Path to config.json (default: config.json)'
    )
    parser.add_argument(
        '--mode',
        choices=['auto', 'split', 'full'],
        default='auto',
        help='Parse mode: auto (default), split, or full'
    )
    parser.add_argument(
        '--specific',
        default='',
        help='Filter specific units as key=value pairs (e.g., "part=17,subpart=G")'
    )

    args = parser.parse_args()
    config = get_config(args.config)
    specific_units = parse_filter_string(args.specific) if args.specific else None

    if os.path.isdir(args.input_path) and args.mode != 'full':
        process_directory(args.input_path, config)
    else:
        process_file(args.input_path, config, parse_mode=args.mode, specific_units=specific_units)


if __name__ == '__main__':
    main()
