# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License
from lxml import etree as ET
import re
import json
import os
import sys
import argparse
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Tuple, Optional, Any
from utils import *
from utils.error_handling import log_parsing_correction
from utils.table_handling import LARGE_TABLE_ROW_THRESHOLD
from utils.large_table_common import (
    assign_table_key,
    find_or_create_table_param_key,
    build_table_sub_unit_from_html,
)
from utils.config import get_config, get_output_directory, get_output_structure, get_parse_mode
from utils.manifest_utils import ManifestManager, get_manifest_path, create_title_output_dir

def get_parsing_issues_logfile(dir_path=''):
    """
    Get a logfile path for parsing-level issues (similar to document issues logfile).
    
    Args:
        dir_path (str): Directory path where logfile should be created
        
    Returns:
        str: Path to the logfile
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
    logfile = str(os.path.join(dir_path, log_stem + str(count).zfill(4) + '.json'))
    return logfile

def clean_unbalanced_brackets(text):
    """
    Remove unbalanced leading/trailing square brackets from text.
    
    Square brackets in USC titles typically indicate repealed or reserved content,
    but when split across XML elements (e.g., num and heading), they appear
    unbalanced. This function removes only unbalanced brackets while preserving
    balanced ones and the content within.
    
    Examples:
        "[CHAPTER 231" -> "CHAPTER 231"  (unbalanced opening bracket)
        "REPEALED]" -> "REPEALED"  (unbalanced closing bracket)
        "[REPEALED]" -> "[REPEALED]"  (balanced, preserved)
        "  [CHAPTER  " -> "CHAPTER"  (unbalanced with whitespace)
        
    Args:
        text (str): Text potentially containing unbalanced brackets
        
    Returns:
        str: Text with unbalanced brackets removed
    """
    if not text:
        return text
    
    result = text.strip()
    
    # Count brackets to check balance
    open_count = result.count('[')
    close_count = result.count(']')
    
    # If brackets are balanced, leave them alone
    if open_count == close_count and open_count > 0:
        # Check if they're properly paired (opening before closing)
        # Simple check: first '[' should come before first ']'
        first_open = result.find('[')
        first_close = result.find(']')
        if first_open >= 0 and first_close >= 0 and first_open < first_close:
            return result
    
    # Remove unbalanced leading opening brackets
    while result.startswith('[') and result.count('[') > result.count(']'):
        result = result[1:].strip()
    
    # Remove unbalanced trailing closing brackets
    while result.endswith(']') and result.count(']') > result.count('['):
        result = result[:-1].strip()
    
    return result

def get_element_text(element, allow_notes=True, list_marker='', list_level=0, prev_line_return=False):
    # Same as get_element_text2, except it returns only the text string.
    return get_element_text2(element, allow_notes, list_marker, list_level, prev_line_return)[0]


def _build_uslm_table_sub_unit(
    table_elem,
    local_counter: int,
    parent_context: List[Dict[str, str]],
    parent_type_name: str,
    parent_id: str,
) -> Dict[str, Any]:
    """Build a table sub-unit dict from a USLM table element (HTML-like structure)."""
    row_count = len(table_elem.findall('.//tr')) if table_elem is not None else 0
    column_headers = [
        clean_text(th.text or '')
        for th in table_elem.findall('.//thead//th')
        if th.text
    ]
    column_headers = [h for h in column_headers if h]
    caption_elem = table_elem.find('.//caption')
    caption = clean_text(caption_elem.text or '') if caption_elem is not None and caption_elem.text else ''
    table_html = ET.tostring(table_elem, encoding='unicode', method='html')
    return build_table_sub_unit_from_html(
        table_html, row_count, column_headers, caption,
        local_counter, parent_context, parent_type_name, parent_id
    )

def _register_uslm_large_tables(content, item_pointer, pending_large_tables, section_id, org_context, parsing_logfile=None):
    """Register XML-intercepted large tables as sub-units on a section item_pointer."""
    if not pending_large_tables:
        return
    param_pointer = content['document_information']['parameters']
    table_param_key = find_or_create_table_param_key(param_pointer)
    table_key_str = str(table_param_key)
    di = content['document_information']
    di.setdefault('sub_unit_index', {})
    di['sub_unit_index'].setdefault(table_key_str, {})
    taken = set(di['sub_unit_index'][table_key_str].keys())
    table_sub_units = {}
    index_entries = {}
    current_text = item_pointer['text']
    for local_num, table_elem in enumerate(pending_large_tables, 1):
        sub_unit_key = assign_table_key(local_num, section_id, taken)
        taken.add(sub_unit_key)
        sub_unit = _build_uslm_table_sub_unit(
            table_elem, local_num, org_context, 'section', section_id
        )
        current_text = current_text.replace(
            f"[Table {local_num} pending sub-unit extraction]",
            f"[Table {local_num} extracted as sub-unit table {sub_unit_key}]",
        )
        table_sub_units[sub_unit_key] = sub_unit
        index_entries[sub_unit_key] = {
            "container_plural": "sections",
            "container_id": section_id,
            "path": [section_id],
        }
    item_pointer['text'] = current_text
    item_pointer['sub_units'] = {table_key_str: table_sub_units}
    di['sub_unit_index'][table_key_str].update(index_entries)
    log_parsing_correction("", "large_table_extraction",
        f"Extracted {len(table_sub_units)} large table(s) from section '{section_id}'",
        parsing_logfile)


def get_indent_from_identifier(identifier):
    result = None
    result = max(0, int(identifier.count('/')) - 4)
    return result

# Note: deduplicate_breakpoints and remove_blank_lines are now imported from utils.text_processing

def get_element_text2(element, allow_notes=True, list_marker='', list_level=0, prev_line_return=False, pending_large_tables=None):
    # Main function for iteratively fetching text from a node.
    # For many node types, just use the .text structure, but deal with specific
    # node types that need special handling.
    # list_marker, if provided, will be used for items within a list.
    # list_level determines the depth of nested lists, used for indenting lines.
    # prev_line_return indicates whether the immediately preceding text from an element calling this one ends with a line return.
    # pending_large_tables: optional list; when provided, large table elements (>= LARGE_TABLE_ROW_THRESHOLD rows)
    #   are appended here and replaced with placeholders instead of being converted to text.

    result = ''
    breakpoints = []
    notes = {}
    paragraph_types = ['p', 'tr', 'item', 'level', 'subsection', 'paragraph', 'subparagraph', 'clause', 'subclause', 'item', 'subitem', 'subsubitem']
    quote_types = []
    need_following_space = []
    need_following_linebreak = ['heading', 'content']
    need_separators = ['td']
    no_breakpoints_passthrough = ['table', 'tbody', 'p', 'paragraph', 'note', 'notes']
    generate_breakpoints = ['subsection', 'block', 'content']
    suppress = ['sourcecredit']

    # Skip of we don't process this type.
    if element.tag.lower() in suppress:
        return result, breakpoints, notes

    # Set flags to determine whether breakpoints from lower elements are passed through and whether
    # this element generates its own breakpoint.
    break_pass_flag = True
    if element.tag.lower() in no_breakpoints_passthrough:
        break_pass_flag = False
    gen_break_flag = False
    if element.tag.lower() in generate_breakpoints:
        gen_break_flag = True

    if element.attrib and 'identifier' in element.attrib.keys():
        local_indent = get_indent_from_identifier(element.attrib['identifier'])
        if not local_indent is None:
            list_level = local_indent

    # Handle anything that needs to be added to the beginning.
    if element.tag.lower() in need_separators:
        result = '[ ' + result

    # Handle indenting and leading line returns.
    if list_level > 0 and (element.tag.lower() in paragraph_types or prev_line_return): # Indenting if following a line return, or paragraph type.
        if not prev_line_return:
            result = '\n' + '    '*(list_level) + result
        else:
            result = '    '*(list_level) + result

    if 'note' == element.tag.lower():  # For now, we only manage footnotes.
        if (allow_notes and 
            element.attrib and 
            'type' in element.attrib.keys() and 
            element.attrib['type'].lower() == 'footnote'):
            note_num_element = get_first_element(element, 'num')
            if not note_num_element is None:
                note_val = clean_text(note_num_element.text).strip()
                if not note_val is None and len(note_val) > 0:
                    # Below is a mini version of the process of gathering text, modified for it being note text (e.g. no interior notes).
                    notes[note_val] = ''
                    if element.text:
                        notes[note_val] += clean_text(element.text)
                    for index, child in enumerate(element):
                        if re.search(r'\n$', notes[note_val]):
                            prev_line_return = True
                        else:
                            prev_line_return = False
                        down_result, _, _ = get_element_text2(child, False, list_marker, 0, prev_line_return)
                        # No need for down_breakpoints or down_notes.
                        notes[note_val] += down_result
    elif ('ref' == element.tag.lower() and
          allow_notes and
          element.attrib and
          'class' in element.attrib.keys() and
          element.text and
          'footnoteref' == element.attrib['class'].lower()): # For now, we only manage footnotes for special rules.  Others pass through.
        note_val = element.text
        if len(note_val) > 0:
            result = '(note: ' + note_val + ')'
    else:
        if element.text:
            result += clean_text(element.text)
        # Special handling for section elements (skip processing of first num and heading elements).
        supress_num_heading = False
        if 'section' == element.tag.lower():
            supress_num_heading = True
        for index, child in enumerate(element):
            if supress_num_heading:
                if not child.tag.lower() in ['num', 'heading']:
                    supress_num_heading = False # Turn off the flag when a non-num or non-heading is encountered.
                else:
                    continue # No further processing of leading num or heading tags.
            if re.search(r'\n$', result): # Line ends with a line return.
                prev_line_return = True
            else:
                prev_line_return = False
            # XML-level large table interception: defer large tables to sub-unit extraction
            if (pending_large_tables is not None and child.tag and child.tag.lower() == 'table'):
                tr_count = len(child.findall('.//tr')) if hasattr(child, 'findall') else 0
                if tr_count >= LARGE_TABLE_ROW_THRESHOLD:
                    pending_large_tables.append(child)
                    local_num = len(pending_large_tables)
                    down_result = f"[Table {local_num} pending sub-unit extraction]"
                    down_breakpoints = []
                    down_notes = {}
                else:
                    down_result, down_breakpoints, down_notes = get_element_text2(child, allow_notes, list_marker, list_level, prev_line_return, pending_large_tables)
            else:
                down_result, down_breakpoints, down_notes = get_element_text2(child, allow_notes, list_marker, list_level, prev_line_return, pending_large_tables)
            initial_result_size = len(result)
            result += down_result
            # Handle adding breakpoints from below to our current set of breakpoints.
            if break_pass_flag: # For some element types, we do not pass on breakpoints, to ensure that text always stays together.
                for break_element in down_breakpoints:
                    break_element[0] += initial_result_size # The breakpoint location from below has to be offset to account for the earlier part of the result.
                    if gen_break_flag: # If we are generating a breakpoint, we need to bump the level of the ones from below.
                        break_element[1] += 1
                    breakpoints.append(break_element)
            # Handle adding notes from below to our current set of notes.
            for note_num in down_notes:
                if not note_num in notes.keys(): # It is possible that a single note appears more than once.  The contents *should* be the same, so we should be able to skip.
                    notes[note_num] = down_notes[note_num]
    if element.tail:
        result += clean_text(element.tail)

    # Handle things that need to be added to the end.
    if element.tag.lower() in paragraph_types + need_following_linebreak:
        result += '\n'
    # elif element.tag.lower() in need_following_space:
    #     result += ' '
    elif element.tag.lower() in need_separators:
        result = result + ' ]'

    if gen_break_flag: # This element is one that generates a new breakpoint.
        # First, see whether there is already a breakpoint.
        down_break_flag = False # Lets us know whether this breakpoint is supplied by an element further down the hierarchy.
        for break_element in breakpoints:
            if break_element[0] == len(result):
                break_element[1] = 1 # Reset the level, since we are now claiming this breakpoint for this element.
                down_break_flag = True
                break
        if not down_break_flag: # This breakpoint is not yet set, so need to set it.
            breakpoints.append([len(result), 1]) # A new breakpoint is always at level 1.
    return result, breakpoints, notes 

# Note: get_all_elements, get_first_element, and drop_ns_and_prefix_to_underscore 
# are now imported from utils.xml_processing

def set_items(item_type):
    """
    Configure item type parameters for United States Code documents.
    
    Args:
        item_type (int): The type of legal item (1 = sections)
        
    Returns:
        tuple: (item_type_name, item_type_name_plural, operational)
            - item_type_name: Singular name of the item type
            - item_type_name_plural: Plural name of the item type  
            - operational: 1 if the item has legal effect, 0 otherwise
    """
    # item_type: 1=sections (only type included, no recitals or annexes)
    item_type_name = ''
    item_type_name_plural = ''
    # Operational indicates primary legal meaning (as opposed to things like recitals).
    operational = 0 # 0 means not operational. 1 is operational.
    if 1 == item_type:
        item_type_name = 'section'
        item_type_name_plural = 'sections'
        operational = 1 
    return item_type_name, item_type_name_plural, operational

def get_org_context_from_id_dict(identifier, id_dict):
    # Find the organizational context for the given identifier by looking it up in id_dict.
    # The id_dict maps identifiers to tuples of (org_pointer, org_context).
    # This is more robust than parsing the identifier string.
    
    if identifier == '' or identifier not in id_dict.keys():
        return []
    
    # The id_dict entry is a tuple: (org_pointer, org_context)
    _, org_context = id_dict[identifier]
    return org_context

# Note: add_substantive_markers_org is now imported from utils.document_handling 

def get_element_values(element):
    # Returns tag, identifier, num_value, num_text, and heading from the given element.
    tag = ''
    identifier = ''
    num_value = ''
    num_text = ''
    heading = ''
    if element.tag:
        tag = element.tag
    if element.attrib and 'identifier' in element.attrib.keys():
        identifier = element.attrib['identifier']
    num_element = None
    for child_element in element: # num element should be a direct child of the element.
        if child_element.tag and 'num' == child_element.tag:
            num_element = child_element
            break
    if (not num_element is None and 
        num_element.attrib and 
        'value' in num_element.attrib.keys()):
        num_value = clean_text(num_element.attrib['value']).strip()
    if not num_element is None and num_element.text:
        num_text = clean_text(num_element.text).strip()
    heading_element = None
    for child_element in element: # heading element should be a direct child of the element.
        if child_element.tag and 'heading' == child_element.tag:
            heading_element = child_element
            break
    if not heading_element is None and not heading_element.text is None:
        heading = clean_text(heading_element.text).strip()
    return tag, identifier, num_value, num_text, heading

def process_section_element(element, content, id_dict, org_context, parsing_logfile=None):
    local_tag, local_identifier, local_num_value, local_num_text, local_heading = get_element_values(element)
    if not 'section' == local_tag:
        raise ParseError('process_section_element called on non-section: ' + str(local_tag) + ' ' + str(local_num_value))
        exit(1)
    if not 'content' in content.keys():
        content['content'] = {}
    if not 'sections' in content['content'].keys():
        content['content']['sections'] = {}
    content_pointer = content['content']['sections']
    if local_num_value in content['content']['sections'].keys():
        raise ParseError('Called process_section_element on section that already appears: ' + str(local_tag) + ' ' + str(local_num_value))
        exit(1)
    content_pointer[local_num_value] = {}
    content_pointer[local_num_value]['text'] = ''
    content_pointer[local_num_value]['breakpoints'] = []
    content_pointer[local_num_value]['notes'] = {}
    content_pointer[local_num_value]['unit_title'] = local_heading
    section_text = ''
    breakpoints = []
    notes = {}
    pending_large_tables = []
    section_text, breakpoints, notes = get_element_text2(element, True, '', 0, True, pending_large_tables)  # Use True for prev_line_return to avoid starting with an immediate line return.
    # Adjust to remove blank lines.
    section_text, breakpoints = remove_blank_lines(section_text, breakpoints)
    # Remove duplicate breakpoint locations, keeping the one with lowest priority for each location.
    breakpoints = deduplicate_breakpoints(breakpoints)
    content_pointer[local_num_value]['text'] = section_text
    for break_entry in breakpoints:
        content_pointer[local_num_value]['breakpoints'].append(break_entry)
    while len(content_pointer[local_num_value]['breakpoints']) > 0: # It is possible that there are multiple breakpoints at the end that need to be removed.
        if content_pointer[local_num_value]['breakpoints'][-1][0] >= len(content_pointer[local_num_value]['text']):
            del content_pointer[local_num_value]['breakpoints'][-1]
        else:
            break
    for note_num in notes.keys():
        if not note_num in content_pointer[local_num_value]['notes'].keys():
            content_pointer[local_num_value]['notes'][note_num] = notes[note_num].strip()

    # Register XML-intercepted large tables as sub-units
    _register_uslm_large_tables(content, content_pointer[local_num_value], pending_large_tables, local_num_value, org_context, parsing_logfile)
    
    # Use the organizational context passed down from parent elements
    content_pointer[local_num_value]['context'] = org_context
    
    # Add begin_ and stop_ markers to the organizational information
    add_substantive_markers_org(content, content_pointer[local_num_value]['context'], 'section', local_num_value)


def process_general_element(element, content, id_dict, org_context, parsing_logfile=None):
    ignore_elements = ['toc', 'notes']
    org_elements = ['title', 'subtitle', 'chapter', 'subchapter', 'part',
                    'subpart', 'division', 'subdivision', 'article', 'subarticle', 'level']
    section_elements = ['section', 'subsection', 'paragraph', 'subparagraph',
                            'clause', 'subclause', 'item', 'subitem', 'subsubitem']
    local_tag, local_identifier, local_num_value, local_num_text, local_heading = get_element_values(element)
    if local_tag in ignore_elements:
        return
    elif local_tag in org_elements:
        if '' == local_identifier:  # In rare cases, an organizational element may not have an identifier, number or heading.
            # Use the given org_context to find the right parent element:
            for id_entry in id_dict.keys():
                _, local_org_context = id_dict[id_entry]
                if local_org_context == org_context:
                    temp_identifier = id_entry + '/' + local_tag.strip() # We're creating our own new identifier now.
                    if not '' == local_num_value:
                        temp_identifier += local_num_value.strip()
                    # We need to make sure this is a new identifier value.
                    local_identifier = temp_identifier
                    count = 0
                    while local_identifier in id_dict.keys():
                        count += 1
                        local_identifier = temp_identifier + '_' + str(count)
                    log_parsing_correction("", "generated_identifier", 
                                         f"Generated identifier '{local_identifier}' for {local_tag} with num_value '{local_num_value}'", 
                                         parsing_logfile)
                    break
            if '' == local_identifier:
                log_parsing_correction("", "missing_identifier_failure", 
                                     f"Org tag '{local_tag}' with no identifier and no matching parent found", 
                                     parsing_logfile)
                raise InputError('Org tag with no identifier: ' + str(local_tag))
                exit(1)

        # Need to see whether a new level needs to be added to the organization structure.
        parent_identifier = re.sub(r'/[^/]+$', '', local_identifier)
        if not parent_identifier in id_dict.keys():
            raise InputError('Parent identifier not found for element: ' + str(local_tag) + ' ' + str(local_num_value))
            exit(1)
        # Get parent's org_pointer and context
        parent_org_pointer, parent_org_context = id_dict[parent_identifier]
        org_pointer = parent_org_pointer # Points to parent organization information.       
        if not local_tag in org_pointer.keys():
            org_pointer[local_tag] = {}
        if '' == local_num_value: # We need to create a stand-in number value.
            local_num_value = '_'
            count = 0
            while local_num_value in org_pointer[local_tag].keys(): # Rare that this would occur, but we should be careful.
                count += 1
                local_num_value = '_' + str(count)
        if not local_num_value in org_pointer[local_tag].keys():
            org_pointer[local_tag][local_num_value] = {}
        org_pointer = org_pointer[local_tag][local_num_value] # Now this points to the organization information for this element.
        # Build the context for this element by extending the parent's context
        new_org_context = parent_org_context.copy()
        new_org_context.append({local_tag: local_num_value})
        if not local_identifier in id_dict.keys():
            id_dict[local_identifier] = (org_pointer, new_org_context)
        if not 'unit_title' in org_pointer.keys():
            org_pointer['unit_title'] = clean_unbalanced_brackets(local_heading)
        # Pass the new context down to children
        for child in element:
            process_general_element(child, content, id_dict, new_org_context, parsing_logfile)
    elif local_tag in section_elements: # If section, add and work through sub-items to add all text (with breakpoints, notes).
        if 'section' == local_tag:
            process_section_element(element, content, id_dict, org_context, parsing_logfile)
        else:
            raise InputError("Unexpected element encountered in section_elements: {}".format(local_tag))
            exit(1)
    else:
        if local_tag in ['num', 'heading'] and element.getparent() is not None and element.getparent().tag and element.getparent().tag in org_elements:
            pass
        else:
            log_parsing_correction("", "unhandled_tag", 
                                 f"Unhandled tag '{local_tag}' with identifier '{local_identifier}', num_value '{local_num_value}', num_text '{local_num_text}', heading '{local_heading}'", 
                                 parsing_logfile)
            print('Otherwise unhandled tag: ' + str(local_tag))
            print(local_identifier)
            print(local_num_value)
            print(local_num_text)
            print(local_heading)
            raise InputError('Unhandled tag: ' + str(local_tag))
            exit(1)
        
def process_chapter(chapter_element, content, title_num, chapter_num, chapter_identifier, id_dict, org_context, parsing_logfile=None):
    # Process a single chapter.
    # Pass the chapter's organizational context down to child elements
    for chapter_child in chapter_element:
        process_general_element(chapter_child, content, id_dict, org_context, parsing_logfile)

def process_all_chapters(title_element, content, title_num, title_identifier, id_dict, parsing_logfile=None):
    # All chapters in the specified title are to be processed.
    org_content_pointer = content['document_information']['organization']['content']['title'][title_num]
    # Get the title's context from id_dict
    _, title_context = id_dict[title_identifier]
    for chapter_element in get_all_elements(title_element, 'chapter'):  # *** I think this is sufficient.  Confirm whether there are any <level> elements with role="chapter".
        _, chapter_identifier, chapter_num, _, heading_text = get_element_values(chapter_element)
        if title_identifier in chapter_identifier: # This chapter element is within the right title.
            if 'chapter' not in org_content_pointer.keys():
                org_content_pointer['chapter'] = {}
            if chapter_num not in org_content_pointer['chapter'].keys():
                org_content_pointer['chapter'][chapter_num] = {}
            org_chapter_pointer = org_content_pointer['chapter'][chapter_num]
            org_chapter_pointer['unit_title'] = clean_unbalanced_brackets(heading_text)
            # Build chapter context by extending title context
            chapter_context = title_context.copy()
            chapter_context.append({'chapter': chapter_num})
            if chapter_identifier not in id_dict.keys():
                id_dict[chapter_identifier] = (org_chapter_pointer, chapter_context)
            # Pass the chapter context to process_chapter
            process_chapter(chapter_element, content, title_num, chapter_num, chapter_identifier, id_dict, chapter_context, parsing_logfile)

def process_all_titles(root, content, id_dict, parsing_logfile=None):
    # All titles in the tree defined by root are to be processed.
    org_content_pointer = content['document_information']['organization']['content'] 
    for title_element in get_all_elements(root, 'title'):
        # Confirm that this is a direct child to 'main'.
        parent = title_element.getparent()
        if parent is None or not parent.tag.lower() == 'main':
            continue
        _, title_identifier, title_num, long_title_text, heading_text = get_element_values(title_element)
        title_text = long_title_text.strip('-')
        if not '' == heading_text:
            long_title_text += ' ' + heading_text # Heading is only for the long title.
        # Add to the organizational structure.
        if not 'title' in org_content_pointer.keys():
            org_content_pointer['title'] = {}
        if not title_num in org_content_pointer['title'].keys():
            org_content_pointer['title'][title_num] = {}
        org_title_pointer = org_content_pointer['title'][title_num]
        if (not 'unit_title' in org_title_pointer.keys() or 
            '' == org_title_pointer['unit_title']):
            org_title_pointer['unit_title'] = clean_unbalanced_brackets(heading_text)
        # Build title context (title is the top level, so context is just the title itself)
        title_context = [{'title': title_num}]
        if title_identifier not in id_dict.keys():
            id_dict[title_identifier] = (org_title_pointer, title_context)
        process_all_chapters(title_element, content, title_num, title_identifier, id_dict, parsing_logfile)

def get_title_name(input_file_path):
    """
    Quickly extract the USC title name (e.g., 'Title 42') from the XML
    without full parsing. Returns empty string if not found.
    """
    try:
        xml_content = ET.parse(input_file_path)
        root = xml_content.getroot()
        root = drop_ns_and_prefix_to_underscore(root)

        # Find title element that is a direct child of main
        for title_element in get_all_elements(root, 'title'):
            parent = title_element.getparent()
            if parent is None or not parent.tag.lower() == 'main':
                continue

            _, _, title_num, _, _ = get_element_values(title_element)
            if title_num:
                return f"Title {title_num}"
        return ""
    except Exception:
        return ""


def get_document_structure(file_path):
    """
    Extract document structure (titles and chapters) without full parsing.

    Args:
        file_path: Path to USLM XML file

    Returns:
        Dict with structure like: {'titles': [{'num': '42', 'chapters': ['6A', '6B']}]}
        Returns empty dict if file cannot be parsed or is not valid USLM
    """
    try:
        xml_content = ET.parse(file_path)
    except:
        return {}

    root = xml_content.getroot()
    root = drop_ns_and_prefix_to_underscore(root)

    # Verify it's a USC Title document
    test_tag = './/dc_type'
    test_value = 'USCTitle'
    matches = root.findall(test_tag)
    if not matches:
        return {}

    format_flag = False
    for m in matches:
        if m.text and m.text.upper() == test_value.upper():
            format_flag = True
            break
    if not format_flag:
        return {}

    structure = {'titles': []}

    # Find all title elements that are direct children of main
    for title_element in get_all_elements(root, 'title'):
        parent = title_element.getparent()
        if parent is None or not parent.tag.lower() == 'main':
            continue

        _, _, title_num, _, _ = get_element_values(title_element)
        if title_num:
            title_info = {'num': title_num, 'chapters': []}

            # Get all chapters in this title
            for chapter_element in get_all_elements(title_element, 'chapter'):
                _, _, chapter_num, _, _ = get_element_values(chapter_element)
                if chapter_num:
                    title_info['chapters'].append(chapter_num)

            structure['titles'].append(title_info)

    return structure

def parse_uslm(file_path, usc_title, usc_chapter, parsing_logfile=None):
    # Parse the USLM file indicated by file_path.
    # If usc_title or usc_chapter are given, limit results to those.
    # parsing_logfile: Optional path to logfile for parsing corrections

    content = {} # This is the object that will hold the parsed information.
    id_dict = {} # Each element identifier is a key pointing to an organizational location.
    try:
        xml_content = ET.parse(file_path)
    except:
        InputWarning('Unable to parse as XML.')
        return content

    root = xml_content.getroot()

    # I'm sure someone somewhere loves namespaces, but they simply get in the way for what we're doing here.  Remove them.
    root = drop_ns_and_prefix_to_underscore(root)

    # Determine whether this file is in-scope.
    test_tag = './/dc_type'
    test_value = 'USCTitle'

    matches = root.findall(test_tag)
    if not matches:
        InputWarning('Not a valid file.  Missing ' + test_tag)
        return content
    if len(matches) > 1:
        InputWarning('More than one ' + test_tag + ' found.')
    format_flag = False
    for m in matches:
        if m.text.upper() == test_value.upper():
            format_flag = True
            break
    if not format_flag:
        InputWarning('Not a supported type of document.')
        return content    

    directory = os.path.dirname(file_path)
    if not directory or not os.path.exists(directory):
        raise InputError('Directory not found.')
        exit(1)
    print('Directory: ' + directory)

    # Start setting up document_information
    content['document_information'] = {}
    content['document_information']['version'] = '0.5'
    content['content'] = {}

    # Set parameters
    content['document_information']['parameters'] = {}
    param_pointer = content['document_information']['parameters']
    for i in [1]:
        item_type_name, item_type_name_plural, operational = set_items(i)
        param_pointer[i] = {}
        param_pointer[i]['name'] = item_type_name
        param_pointer[i]['name_plural'] = item_type_name_plural # Change from version 0.2 which used the confusing 'names' to indicate plural of the 'name'.
        param_pointer[i]['operational'] = operational
        content['content'][item_type_name_plural] = {}

    # Set up the organizational structure.
    content['document_information']['organization'] = {}
    org_pointer = content['document_information']['organization']   
    org_pointer['item_types'] = [1]
    org_pointer['content'] = {}
    org_content_pointer = org_pointer['content']

    # Get the document title in the event the full document is to be parsed.
    title_identifier = ''
    chapter_identifier = ''
    if '' == usc_title:  # Either the title is specified or the whole document is to be processed.
        meta_element = get_first_element(root, 'meta')
        if meta_element is None:
            log_parsing_correction(file_path, "missing_meta_element", 
                                 "No meta element found in document", 
                                 parsing_logfile)
            InputWarning('No meta element found.')
            return ''
        dc_title_element = get_first_element(meta_element, 'dc_title')
        if dc_title_element is None: # No title.  Rather than fail altogether, blank the title and move on.
            log_parsing_correction(file_path, "missing_dc_title", 
                                 "No dc_title element found in meta section", 
                                 parsing_logfile)
            InputWarning('No dc_title element found.')
            content['document_information']['title'] = ''
            content['document_information']['long_title'] = ''
        else:
            title_text = clean_text(get_element_text(dc_title_element, False)).strip()
            content['document_information']['title'] = title_text
            content['document_information']['long_title'] = title_text
        process_all_titles(root, content, id_dict, parsing_logfile)
    else:
        title_text = ''
        long_title_text = ''
        found_full_specified_flag = False # Indicates whether we have found the part that was specified in the call (title and/or chapter).
        if not '' == usc_title: # Title is specified
            org_title_pointer = {}
            found_title_flag = False
            for title_element in get_all_elements(root, 'title'): # Note that 'title' here is title in the colloquial sense, not the USC title number, which is int usc_title.
                # Confirm that this is a direct child to 'main'.
                parent = title_element.getparent()
                if parent is None or not parent.tag.lower() == 'main':
                    continue

                _, title_identifier, title_num, title_num_text, heading_text = get_element_values(title_element)
                if title_num.upper() == usc_title.upper().strip():
                    found_title_flag = True
                    if '' == usc_chapter: # We only know that the right part is found if title is all we are looking for.
                        found_full_specified_flag = True
                    title_text += clean_unbalanced_brackets(title_num_text.strip('-'))
                    long_title_text = title_num_text
                    if not '' == heading_text:
                        long_title_text += ' ' + heading_text # Heading is only for the long title.
                    if '' == title_identifier:
                        raise InputError('Title has no identifier: ' + str(title_num))
                        exit(1)
                    # Add to the organizational structure.
                    org_content_pointer['title'] = {}
                    org_content_pointer['title'][title_num] = {}
                    org_content_pointer['title'][title_num]['unit_title'] = clean_unbalanced_brackets(heading_text)
                    org_title_pointer = org_content_pointer['title'][title_num]
                    # Build title context
                    title_context = [{'title': title_num}]
                    if title_identifier not in id_dict.keys():
                        id_dict[title_identifier] = (org_title_pointer, title_context)
                    if not '' == usc_chapter: # Chapter is specified
                        for chapter_element in get_all_elements(title_element, 'chapter'):  # *** I think this is sufficient.  Confirm whether there are any <level> elements with role="chapter".
                            _, chapter_identifier, chapter_num, chapter_num_text, heading_text = get_element_values(chapter_element)
                            if title_identifier in chapter_identifier: # This chapter element is within the right title.  *** Is this really needed?  Isn't it always true?
                                if chapter_num.upper() == usc_chapter.upper().strip():  # This is the right chapter element.
                                    found_full_specified_flag = True
                                    if not '' == title_text:
                                        title_text += ', '
                                    if not '' == long_title_text:
                                        long_title_text += ', '
                                    title_text += clean_unbalanced_brackets(chapter_num_text.strip('-'))
                                    long_title_text += chapter_num_text
                                    if not '' == heading_text:
                                        long_title_text += ' ' + heading_text # Heading is only for the long title.
                                    if '' == chapter_identifier:
                                        raise InputError('Chapter has no identifier: ' + usc_chapter)
                                        exit(1)
                                    org_title_pointer['chapter'] = {}
                                    org_title_pointer['chapter'][chapter_num] = {}
                                    org_chapter_pointer = org_title_pointer['chapter'][chapter_num]
                                    org_chapter_pointer['unit_title'] = clean_unbalanced_brackets(heading_text)
                                    content['document_information']['title'] = title_text
                                    content['document_information']['long_title'] = long_title_text
                                    # Build chapter context by extending title context
                                    chapter_context = title_context.copy()
                                    chapter_context.append({'chapter': chapter_num})
                                    # Capture content_scope from the parse-time context
                                    content['document_information']['content_scope'] = list(chapter_context)
                                    if not chapter_identifier in id_dict.keys():
                                        id_dict[chapter_identifier] = (org_chapter_pointer, chapter_context)
                                    # Pass the chapter context to process_chapter
                                    process_chapter(chapter_element, content, title_num, chapter_num, chapter_identifier, id_dict, chapter_context, parsing_logfile)
                                    break
                    else: # Title is specified, but not chapter.
                        content['document_information']['title'] = title_text
                        content['document_information']['long_title'] = long_title_text
                        # Capture content_scope from the parse-time title context
                        content['document_information']['content_scope'] = list(title_context)
                        process_all_chapters(title_element, content, title_num, title_identifier, id_dict, parsing_logfile)
                    break # The right title was found, no need to continue.

            if not found_title_flag: # In this case, a title was specified, but it was not found.
                return {}
        if not found_full_specified_flag: # The specified part is not in this document.
            return {}

    return content

def process_file(input_file_path, config, parse_mode='split_chapters', title='', chapter=''):
    """
    Process a single USLM file with manifest support.

    Args:
        input_file_path: Path to input XML file
        config: Configuration dictionary
        parse_mode: 'split_chapters' or 'full_document'
        title: Optional title to parse (for backward compatibility)
        chapter: Optional chapter to parse (for backward compatibility)
    """
    file_name = os.path.basename(input_file_path)
    file_stem = re.sub(r'\.\w+$', '', file_name)

    print(f'Processing: {file_name}')

    # Get output directory
    output_dir = get_output_directory(config)

    # All USC files go to a common 'USC' subdirectory
    doc_output_dir = os.path.join(output_dir, 'USC')
    Path(doc_output_dir).mkdir(parents=True, exist_ok=True)

    # Extract title name and create title-specific subfolder
    title_name = get_title_name(input_file_path)
    parsed_output_dir = create_title_output_dir(doc_output_dir, title_name)

    # Get manifest path and manager (manifest stays at USC level)
    manifest_path = get_manifest_path(doc_output_dir, file_stem)
    manifest_mgr = ManifestManager(manifest_path)

    # Create or load manifest
    manifest = manifest_mgr.create_or_load(
        source_file=input_file_path,
        source_type='uslm',
        parser='uslm_set_parse.py'
    )

    # Get parsing logfile (at USC level)
    parsing_logfile = get_parsing_issues_logfile(doc_output_dir)

    # If title and chapter are specified (backward compatibility), just parse that
    if title or chapter:
        title_suffix = f'_title{title}' if title else ''
        chapter_suffix = f'_chapter{chapter}' if chapter else ''
        output_filename = f'{file_stem}{title_suffix}{chapter_suffix}_parse_output.json'
        output_path = os.path.join(parsed_output_dir, output_filename)

        # Check if already exists in manifest
        existing_files = manifest_mgr.get_parsed_files(
            manifest,
            filter_criteria={'title': title, 'chapter': chapter} if title and chapter else None
        )
        if existing_files and os.path.exists(existing_files[0]['abs_path']):
            print(f'  Already parsed (in manifest): {output_filename}')
            return

        print(f'  Parsing title={title}, chapter={chapter}')
        try:
            parsed_content = parse_uslm(input_file_path, title, chapter, parsing_logfile)
        except (ConfigError, ParseError) as e:
            print(f"  Error: {e}")
            return

        if parsed_content and parsed_content != {}:
            print('  Writing output...')
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(parsed_content, f, indent=4, ensure_ascii=False)
            print(f'  Output written: {output_filename}')

            # Update manifest
            file_type = 'chapter' if chapter else ('title' if title else 'full_document')
            metadata = {}
            if title:
                metadata['title'] = title
            if chapter:
                metadata['chapter'] = chapter
            manifest_mgr.add_parsed_file(manifest, output_path, file_type, **metadata)
            manifest_mgr.save(manifest)
        return

    # Determine parsing strategy based on mode
    if parse_mode == 'full_document':
        # Parse entire document as single file
        output_filename = f'{file_stem}_parse_output.json'
        output_path = os.path.join(parsed_output_dir, output_filename)

        # Check if already exists in manifest
        existing_files = manifest_mgr.get_parsed_files(
            manifest,
            filter_criteria={'type': 'full_document'}
        )
        if existing_files and os.path.exists(existing_files[0]['abs_path']):
            print(f'  Already parsed (in manifest): {output_filename}')
            return

        print(f'  Parsing full document')
        try:
            parsed_content = parse_uslm(input_file_path, '', '', parsing_logfile)
        except (ConfigError, ParseError) as e:
            print(f"  Error: {e}")
            return

        if parsed_content and parsed_content != {}:
            print('  Writing output...')
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(parsed_content, f, indent=4, ensure_ascii=False)
            print(f'  Output written: {output_filename}')

            # Update manifest
            manifest_mgr.add_parsed_file(manifest, output_path, 'full_document')
            manifest_mgr.save(manifest)

    else:  # split_chapters mode
        # Get document structure
        structure = get_document_structure(input_file_path)
        if not structure or 'titles' not in structure or len(structure['titles']) == 0:
            print('  Could not determine document structure, skipping')
            return

        # Check if this is a single title with multiple chapters
        if len(structure['titles']) == 1 and len(structure['titles'][0]['chapters']) > 1:
            # Split by chapters
            title_num = structure['titles'][0]['num']
            chapters = structure['titles'][0]['chapters']

            print(f'  Found Title {title_num} with {len(chapters)} chapters')
            print(f'  Splitting into separate files...')

            for chapter_num in chapters:
                output_filename = f'{file_stem}_title{title_num}_chapter{chapter_num}_parse_output.json'
                output_path = os.path.join(parsed_output_dir, output_filename)

                # Check if already exists in manifest
                existing_files = manifest_mgr.get_parsed_files(
                    manifest,
                    filter_criteria={'title': title_num, 'chapter': chapter_num}
                )
                if existing_files and os.path.exists(existing_files[0]['abs_path']):
                    print(f'    Chapter {chapter_num}: Already parsed (in manifest)')
                    continue

                print(f'    Chapter {chapter_num}: Parsing...')
                try:
                    parsed_content = parse_uslm(input_file_path, title_num, chapter_num, parsing_logfile)
                except (ConfigError, ParseError) as e:
                    print(f"    Chapter {chapter_num}: Error - {e}")
                    continue

                if parsed_content and parsed_content != {}:
                    with open(output_path, 'w', encoding='utf-8') as f:
                        json.dump(parsed_content, f, indent=4, ensure_ascii=False)
                    print(f'    Chapter {chapter_num}: Output written')

                    # Update manifest
                    manifest_mgr.add_parsed_file(
                        manifest, output_path, 'chapter',
                        title=title_num, chapter=chapter_num
                    )

            # Save manifest after all chapters processed
            manifest_mgr.save(manifest)
            print(f'  Wrote split outputs to: {parsed_output_dir}')
        else:
            # Parse as full document (either multiple titles or single title with one chapter)
            print(f'  Document does not match split criteria, parsing as full document')
            output_filename = f'{file_stem}_parse_output.json'
            output_path = os.path.join(parsed_output_dir, output_filename)

            # Check if already exists in manifest
            existing_files = manifest_mgr.get_parsed_files(
                manifest,
                filter_criteria={'type': 'full_document'}
            )
            if existing_files and os.path.exists(existing_files[0]['abs_path']):
                print(f'  Already parsed (in manifest): {output_filename}')
                return

            try:
                parsed_content = parse_uslm(input_file_path, '', '', parsing_logfile)
            except (ConfigError, ParseError) as e:
                print(f"  Error: {e}")
                return

            if parsed_content and parsed_content != {}:
                print('  Writing output...')
                with open(output_path, 'w', encoding='utf-8') as f:
                    json.dump(parsed_content, f, indent=4, ensure_ascii=False)
                print(f'  Output written: {output_filename}')

                # Update manifest
                manifest_mgr.add_parsed_file(manifest, output_path, 'full_document')
                manifest_mgr.save(manifest)

def process_directory(dir_path, config, parse_mode, title='', chapter='', recursive=True):
    """
    Process directory of USLM files.

    Args:
        dir_path: Path to directory or file
        config: Configuration dictionary
        parse_mode: 'split_chapters' or 'full_document'
        title: Optional title filter (for backward compatibility)
        chapter: Optional chapter filter (for backward compatibility)
        recursive: Whether to process subdirectories
    """
    # Handle single file
    if os.path.isfile(dir_path):
        if dir_path.endswith('.xml'):
            process_file(dir_path, config, parse_mode, title, chapter)
        return

    # Handle directory
    if not os.path.isdir(dir_path):
        raise InputError('Input not a directory or file.')

    for item in os.listdir(dir_path):
        item_path = os.path.join(dir_path, item)

        if os.path.isfile(item_path) and item.endswith('.xml'):
            process_file(item_path, config, parse_mode, title, chapter)
        elif recursive and os.path.isdir(item_path):
            print(f'Moving to directory: {item_path}')
            process_directory(item_path, config, parse_mode, title, chapter, recursive)

# Main execution block
def main():
    """Main entry point for USLM parser."""
    parser = argparse.ArgumentParser(
        description='Parse USLM (United States Legislative Markup) XML files into structured JSON.'
    )
    parser.add_argument(
        'input_path',
        help='Path to USLM XML file or directory containing USLM files'
    )
    parser.add_argument(
        '--title',
        default='',
        help='Optional: Parse only the specified USC Title number (e.g., "42")'
    )
    parser.add_argument(
        '--chapter',
        default='',
        help='Optional: Parse only the specified Chapter number (requires --title, e.g., "6A")'
    )
    parser.add_argument(
        '--full-document',
        action='store_true',
        help='Parse entire document as single file (overrides default chapter splitting)'
    )
    parser.add_argument(
        '--config',
        default='config.json',
        help='Path to configuration file (default: config.json)'
    )
    parser.add_argument(
        '--no-recursive',
        action='store_true',
        help='Do not process subdirectories recursively'
    )

    args = parser.parse_args()

    # Load configuration
    config = get_config(args.config)

    # Determine parse mode
    if args.full_document:
        parse_mode = 'full_document'
    else:
        parse_mode = get_parse_mode(config)

    # Validate input path
    if not os.path.exists(args.input_path):
        print(f"Error: Input path does not exist: {args.input_path}")
        sys.exit(1)

    # Process directory or file
    try:
        process_directory(
            args.input_path,
            config,
            parse_mode,
            title=clean_text(args.title) if args.title else '',
            chapter=clean_text(args.chapter) if args.chapter else '',
            recursive=not args.no_recursive
        )
    except InputError as e:
        print(f"Input Error: {e}")
        sys.exit(1)
    except ConfigError as e:
        print(f"Configuration Error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == '__main__':
    main()