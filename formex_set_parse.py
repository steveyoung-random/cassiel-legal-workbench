# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License
import xml.etree.ElementTree as ET
import re
import json
import os
import sys
import argparse
from pathlib import Path
from collections import defaultdict
from typing import Dict, Iterable, Iterator, List, Tuple, Optional, Any
from utils import *
from utils.error_handling import log_parsing_correction
from utils.table_handling import LARGE_TABLE_ROW_THRESHOLD
from utils.large_table_common import (
    assign_table_key,
    find_or_create_table_param_key,
    build_table_sub_unit_from_html,
)
from utils.definition_list_common import (
    find_or_create_definition_list_param_key,
    assign_definition_list_key,
    build_definition_list_sub_unit,
    title_contains_definition,
)
from utils.config import get_config, get_output_directory, get_output_structure, get_definition_list_thresholds
from utils.manifest_utils import ManifestManager, get_manifest_path


# Local versions of XML traversal functions for xml.etree.ElementTree
# These override the lxml-based versions from utils.xml_processing
def get_all_elements(tree, pattern):
    """
    Get all elements matching a pattern (xml.etree.ElementTree version).

    Args:
        tree: xml.etree.ElementTree.Element to search
        pattern: Tag pattern to match

    Yields:
        Matching elements
    """
    if pattern in tree.tag:
        yield tree
    matches = tree.findall(".//" + pattern)
    if matches:
        for m in matches:
            yield m


def get_first_element(tree, pattern):
    """
    Get first element matching a pattern (xml.etree.ElementTree version).

    Args:
        tree: xml.etree.ElementTree.Element to search
        pattern: Tag pattern to match

    Returns:
        First matching element or None
    """
    result = None
    matches = tree.findall(".//" + pattern)
    if matches and len(matches) > 0:
        result = matches[0]
    return result


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

def get_element_text(element, allow_notes=True, list_marker='', list_level=0, prev_line_return=False):
    # Same as get_element_text2, except it returns only the text string.
    return get_element_text2(element, allow_notes, list_marker, list_level, prev_line_return)[0]

def get_element_text2(element, allow_notes=True, list_marker='', list_level=0, prev_line_return=False, pending_large_tables=None, pending_definition_lists=None):
    # Main function for iteratively fetching text from a node.
    # For many node types, just use the .text structure, but deal with specific
    # node types that need special handling.
    # list_marker, if provided, will be used for items within a list.
    # list_level determines the depth of nested lists, used for indenting lines.
    # prev_line_return indicates whether the immediately preceding text from an element calling this one ends with a line return.
    #
    # *** Future improvements:
    # 1. Handle tables intelligently.  Currently just returns for rows, and brackets for cells.

    result = ''
    breakpoints = []
    notes = {}
    paragraph_types = ['P', 'PARAG', 'NP', 'ROW', 'ALINEA', 'LIST']
    quote_types = ['QUOT.START', 'QUOT.END']
    need_following_space = ['NO.PARAG', 'NO.P']
    need_separators = ['CELL']
    no_breakpoints_passthrough = ['LIST', 'ALINEA', 'NP', 'ROW']
    generate_breakpoints = ['PARAG', 'ALINEA', 'GR.SEQ']
    # Set flags to determine whether breakpoints from lower elements are passed through and whether
    # this element generates its own breakpoint.
    break_pass_flag = True
    if element.tag.upper() in no_breakpoints_passthrough:
        break_pass_flag = False
    gen_break_flag = False
    if element.tag.upper() in generate_breakpoints:
        gen_break_flag = True

    # Handle lists
    if 'LIST' == element.tag.upper():
        list_level = list_level + 1
        if element.attrib and 'TYPE' in element.attrib.keys():
            # We only add list indicators for these, because most others (e.g. ALPHA, alpha, ARAB, ROMAN, roman) put the indicator in the text itself.
            if element.attrib['TYPE'] in ['DASH', 'NDASH']:
                list_marker = '-- '
            elif element.attrib['TYPE'] in ['BULLET', 'OTHER']:  # *** Not sure what circumstances use OTHER.  Look for instances.
                list_marker = '* '
            else:
                list_marker = ''

    # Handle anything that needs to be added to the beginning.
    if element.tag.upper() in need_separators:
        result = '[ ' + result
    if element.tag.upper() == 'ITEM' and list_level > 1:
        if not prev_line_return:
            result = '\n' + '    '*(list_level - 1) + result
        else:
            result = '    '*(list_level - 1) + result
    if element.tag.upper() == 'ITEM'and not '' == list_marker:
        result = result + list_marker

    if 'NOTE' == element.tag.upper():
        if allow_notes and element.attrib and 'NOTE.ID' in element.attrib.keys():
            id_num = element.attrib['NOTE.ID']
            id_num = re.sub(r'^E0*', '', id_num) # Strip out 'E' and any zeros that precede note number.
            if len(id_num) > 0:
                result = '(note: ' + id_num + ')'
                # Below is a mini version of the process of gathering text, modified for it being note text (e.g. no interior notes).
                notes[id_num] = ''
                if element.text:
                    notes[id_num] += clean_text(element.text)
                for index, child in enumerate(element):
                    if re.search(r'\n$', notes[id_num]):
                        prev_line_return = True
                    else:
                        prev_line_return = False
                    down_result, down_breakpoints, down_notes = get_element_text2(child, False, list_marker, list_level, prev_line_return)
                    # No need for down_breakpoints or down_notes.
                    notes[id_num] += down_result
    elif element.tag.upper() in quote_types:
        if element.attrib and 'CODE' in element.attrib.keys():
            code_value = element.attrib['CODE']
            if code_value in ['0022', '00AB', '00BB', '201C', '201D']:
                result += '"'
            elif code_value in ['2018', '2019']:
                result += '\''
            else:
                result += '"'
        else:
            result += '"'
    else:
        if element.text:
            result += clean_text(element.text)
        for index, child in enumerate(element):
            if re.search(r'\n$', result):
                prev_line_return = True
            else:
                prev_line_return = False
            # XML-level large table interception: defer large TGROUP tables to sub-unit extraction
            if (pending_large_tables is not None and child.tag and child.tag.upper() == 'TGROUP'):
                row_count = len(child.findall('.//ROW'))
                if row_count >= LARGE_TABLE_ROW_THRESHOLD:
                    pending_large_tables.append(child)
                    local_num = len(pending_large_tables)
                    down_result = f"[Table {local_num} pending sub-unit extraction]"
                    down_breakpoints = []
                    down_notes = {}
                else:
                    down_result, down_breakpoints, down_notes = get_element_text2(child, allow_notes, list_marker, list_level, prev_line_return, pending_large_tables, pending_definition_lists)
            # XML-level definition-list interception: defer qualifying LIST elements to sub-unit extraction
            elif (pending_definition_lists is not None and child.tag and child.tag.upper() == 'LIST'
                  and id(child) in pending_definition_lists['to_intercept']):
                pending_definition_lists['lists'].append(child)
                local_num = len(pending_definition_lists['lists'])
                down_result = f"[Definition list {local_num} pending sub-unit extraction]"
                down_breakpoints = []
                down_notes = {}
            else:
                down_result, down_breakpoints, down_notes = get_element_text2(child, allow_notes, list_marker, list_level, prev_line_return, pending_large_tables, pending_definition_lists)
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
    if element.tag.upper() in paragraph_types:
        # result = re.sub(r'\n+$', '\n', result) + '\n' # No more than two line returns at the end.
        result = result.rstrip() + '\n'
    elif element.tag.upper() in need_following_space:
        result += ' '
    elif element.tag.upper() in need_separators:
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

# Note: get_all_elements and get_first_element are defined locally above (xml.etree.ElementTree versions)

def set_items(item_type):
    """
    Configure item type parameters for European Union legal documents.
    
    Args:
        item_type (int): The type of legal item (1=recital, 2=article, 3=annex)
        
    Returns:
        tuple: (item_type_name, item_type_name_plural, operational)
            - item_type_name: Singular name of the item type
            - item_type_name_plural: Plural name of the item type  
            - operational: 1 if the item has legal effect, 0 otherwise
    """
    # item_type: 1=recital, 2=article, 3=annex
    item_type_name = ''
    item_type_name_plural = ''
    # Operational indicates primary legal meaning (as opposed to things like recitals).
    operational = 0 # 0 means not operational. 1 is operational.
    if 1 == item_type:
        item_type_name = 'recital'
    elif 2 == item_type:
        item_type_name = 'article'
        operational = 1      
    elif 3 == item_type:
        item_type_name = 'annex'
        operational = 1
    item_type_name, item_type_name_plural = canonical_org_types(item_type_name)    
    return item_type_name, item_type_name_plural, operational

# Note: get_org_top_unit is now imported from utils.document_handling

def _register_pending_large_tables(
    parsed_content,
    item_pointer,
    pending_large_tables,
    parent_type_name,
    parent_id,
    name_plural,
    file_path=None,
    parsing_logfile=None,
):
    """Register XML-intercepted large TGROUP tables as sub-units on the item."""
    if not pending_large_tables:
        return
    param_pointer = parsed_content['document_information']['parameters']
    table_param_key = find_or_create_table_param_key(param_pointer)
    table_type_name = param_pointer[table_param_key]['name']
    table_key_str = str(table_param_key)
    di = parsed_content['document_information']
    di.setdefault('sub_unit_index', {})
    di['sub_unit_index'].setdefault(table_key_str, {})
    taken = set(di['sub_unit_index'][table_key_str].keys())
    table_sub_units = {}
    index_entries = {}
    parent_context = item_pointer.get('context', [])
    current_text = item_pointer['text']
    for local_num, tgroup_elem in enumerate(pending_large_tables, 1):
        sub_unit_key = assign_table_key(local_num, parent_id, taken)
        taken.add(sub_unit_key)
        sub_unit = _build_formex_table_sub_unit(
            tgroup_elem, local_num, parent_context, parent_type_name, parent_id, parsed_content
        )
        final_placeholder = f"[Table {local_num} extracted as sub-unit {table_type_name} {sub_unit_key}]"
        sub_unit['placeholder'] = final_placeholder
        current_text = current_text.replace(
            f"[Table {local_num} pending sub-unit extraction]",
            final_placeholder,
        )
        table_sub_units[sub_unit_key] = sub_unit
        index_entries[sub_unit_key] = {
            "container_plural": name_plural,
            "container_id": parent_id,
            "path": [parent_id],
        }
    item_pointer['text'] = current_text
    item_pointer['sub_units'] = {table_key_str: table_sub_units}
    di['sub_unit_index'][table_key_str].update(index_entries)
    log_parsing_correction(file_path or "", "large_table_extraction",
                           f"Extracted {len(table_sub_units)} large table(s) from "
                           f"{parent_type_name} '{parent_id}'", parsing_logfile)


def _build_formex_table_sub_unit(tgroup_elem, local_counter, parent_context, parent_type_name, parent_id, parsed_content):
    """Build a table sub-unit dict from a Formex TGROUP element (ROW/CELL structure)."""
    row_count = len(tgroup_elem.findall('.//ROW'))
    table_xml = ET.tostring(tgroup_elem, encoding='unicode')
    # Formex TGROUP uses ROW/ENTRY structure without HTML thead/th elements;
    # header extraction is not attempted here. The parent TABLE/TITLE text
    # appears as inline text in the article body, and Stage 3 reads the
    # raw XML serialization to extract structure.
    return build_table_sub_unit_from_html(
        table_xml, row_count, [], '',
        local_counter, parent_context, parent_type_name, parent_id
    )


def add_text_brkpts_notes(item_pointer, node, note_flag=True, pending_large_tables=None, pending_definition_lists=None):
    # Take the item_pointer within the content structure and place within the
    # text, breakpoints, and notes fields the output from the get_element_text2 function.
    element_text, breakpoints, notes = get_element_text2(node, note_flag, pending_large_tables=pending_large_tables, pending_definition_lists=pending_definition_lists)
    text_size = len(item_pointer['text'])
    item_pointer['text'] += element_text + '\n'
    # Calculate breakpoint offsets, and add them to the content structure.
    for break_entry in breakpoints:
        break_entry[0] += text_size # To account for offset of previous text.
        item_pointer['breakpoints'].append(break_entry)
    # Add any new notes to the content structure.
    for note_num in notes.keys():
        if not note_num in item_pointer['notes'].keys():
            item_pointer['notes'][note_num] = notes[note_num].strip()

def extract_recitals(tree, parsed_content, org_keyword_set, file_path=None, parsing_logfile=None):
    # Get the recitals from the PREAMBLE node(s).
    rec_pointer = parsed_content['content']['recitals']
    for preamble_node in get_all_elements(tree, 'PREAMBLE'):
        for consid_node in get_all_elements(preamble_node, 'CONSID'):
            # Find out whether the recital is numbered using the NO.P element.
            rec_num = ''
            rec_num_element = get_first_element(consid_node, 'NO.P')
            if not rec_num_element is None and rec_num_element.text:
                rec_num = re.search(r'\(([^()]*)\)', rec_num_element.text)
                rec_num = rec_num.group(1) if rec_num else ''
            if '' == rec_num: # Not found in an NO.P element.
                rec_num = 1
                while 'recital_' + str(rec_num) in rec_pointer.keys():
                    rec_num = rec_num + 1
                rec_num = 'recital_' + str(rec_num)
                log_parsing_correction(file_path, "recital_number_generated", 
                                     f"Generated recital number (NO.P not found or parsed): '{rec_num}'", 
                                     parsing_logfile)
            rec_pointer[rec_num] = {}
            rec_pointer[rec_num]['text'] = ''
            rec_pointer[rec_num]['breakpoints'] = []
            rec_pointer[rec_num]['notes'] = {}
            # Now, find the text portion.  Typically, there is a TXT element.
            for text_node in get_all_elements(consid_node, 'TXT'):
                add_text_brkpts_notes(rec_pointer[rec_num], text_node, True)
            if '' == rec_pointer[rec_num]['text'].strip(): # Fallback method, in case there are no TXT elements.
                element_text, breakpoints, notes = get_element_text2(consid_node, True)
                if not rec_num_element is None:
                    rec_pointer[rec_num]['text'] = re.sub('^' + re.escape(rec_num_element.text.strip()), '', element_text) # This line removes the recital number from the text.
                else:
                    rec_pointer[rec_num]['text'] = element_text
                for break_entry in breakpoints:
                    rec_pointer[rec_num]['breakpoints'].append(break_entry)
                for note_num in notes.keys():
                    if not note_num in rec_pointer[rec_num]['notes'].keys():
                        rec_pointer[rec_num]['notes'][note_num] = notes[note_num].strip()
            rec_pointer[rec_num]['text'] = rec_pointer[rec_num]['text'].rstrip()
            # Remove any breakpoints that are now after the end of the text (since the rstrip may have removed non-printing characters at the end).
            while len(rec_pointer[rec_num]['breakpoints']) > 0: # It is possible that there are multiple breakpoints at the end that need to be removed.
                if rec_pointer[rec_num]['breakpoints'][-1][0] > len(rec_pointer[rec_num]['text']):
                    del rec_pointer[rec_num]['breakpoints'][-1]
                else:
                    break

            # Assume that recitals are always at the level of the top organizational unit.
            rec_pointer[rec_num]['context'] = get_org_top_unit(parsed_content, org_keyword_set)
            # Now, add begin_recital and stop_recital markers to the organizational information.
            add_substantive_markers_org(parsed_content, rec_pointer[rec_num]['context'], 'recital', rec_num)

def get_org_info(element):
    # Given the DIVISION element, get the type, number, and title.
    org_type = ''
    org_num = ''
    org_title = ''
    TI_element_text = ''
    STI_element_text = ''
    if not element.tag or not 'DIVISION' == element.tag: # If this is called on something other than a DIVISION element tag, this is a mistake.
        raise ParseError('get_org_info called on an element that is not of type "DIVISION".')
        exit(1)
    title_element = get_first_element(element, 'TITLE')
    if not title_element is None:
        for child in title_element:
            if 'TI' == child.tag.upper():
                TI_element_text += get_element_text(child, False)
            elif 'STI' == child.tag.upper():
                STI_element_text += get_element_text(child, False)
    else:
        raise ParseError('No TITLE element found in DIVISION in call to get_or_info.')
        exit(1)
    if not '' == TI_element_text:
        TI_element_text = TI_element_text.strip()
        org_type, sep, org_num = TI_element_text.partition(" ")
        if not '' == STI_element_text:
            org_title = STI_element_text.strip()
    return org_type, org_num, org_title

def get_org_context(element, parsed_content, parent_map, org_keyword_set):
    # Find the organizational context for the given element.
    org_context = []    # The org_context is a list of dictionaries.  Each dictionary has one key (a type of organizational unit) 
                        # and one value (the name/number of the organizational unit).  The list is ordered from the top-most organizationl
                        # unit to the lowest which contains the given element.
    parent = parent_map.get(element)
    org_titles = {} # Needed to keep track of org unit titles, so they can be added into the organizational information for the document.
    while not parent == None:
        parent_type = parent.tag
        if not parent_type == None:
            if 'DIVISION' == parent_type:
                org_type, org_num, org_title = get_org_info(parent)
                org_type = org_type.lower()
                if not '' == org_type and not '' == org_num:
                    org_element = {org_type: org_num}
                    org_context = [org_element] + org_context # Add to the beginning of the list.
                    org_title_element = {org_type: {org_num: {'unit_title': org_title}}}
                    if not {} == org_titles:
                        org_title_element[org_type][org_num].update(org_titles)
                        org_titles = org_title_element
                    else:
                        org_titles = org_title_element
        parent = parent_map.get(parent) # Move one element up the tree.
    # Add top unit.
    org_top = get_org_top_unit(parsed_content, org_keyword_set)[0]
    org_context = [org_top] + org_context

    # Set local_org_pointer to top unit of organizational information for document, and org_titles_pointer to the calculated org_titles_pointer.
    # The org_titles_pointer does not include the top level unit, which does not correspond to a DIVISION node.  So, this is needed to add the
    # org_titles information to the document organizational information.
    # The two pointers will move forward in lock-step, following the path of the org_titles information and filling out what is absent in the
    # overall document organizational information.
    org_top_key = list(org_top.keys())[0]
    org_top_value = org_top[org_top_key]
    local_org_pointer = parsed_content['document_information']['organization']['content'][org_top_key][org_top_value]
    org_titles_pointer = org_titles
    org_titles_continue = True
    # Fill out any part of the organizational information for the document that is missing.
    while org_titles_continue:
        org_titles_continue = False
        org_type = ''
        for entry in list(org_titles_pointer.keys()): # Need to find actual organizational type, not unit_title.
            if not 'unit_title' == entry:
                org_type = entry
                org_titles_continue = True
                break
        if org_titles_continue and len(list(org_titles_pointer[org_type].keys())) > 0:
            org_num = list(org_titles_pointer[org_type].keys())[0] # Should be only one option here, the number of the current one.
            org_title = org_titles_pointer[org_type][org_num]['unit_title']
            # Now, see if this is already included in the document organizational information.
            if not org_type in local_org_pointer.keys():
                local_org_pointer[org_type] = {}
            local_org_pointer = local_org_pointer[org_type]
            if not org_num in local_org_pointer.keys():
                local_org_pointer[org_num] = {}
            local_org_pointer = local_org_pointer[org_num]
            if not 'unit_title' in local_org_pointer.keys() or '' == local_org_pointer['unit_title']:
                local_org_pointer['unit_title'] = org_title
            org_titles_pointer = org_titles_pointer[org_type][org_num]
    return org_context

# Note: add_substantive_markers_org is now imported from utils.document_handling 

def get_title_from_sub_doc(tree, parsed_content, directory):
    # The short form of the title often is available only in the first subsidiary document.
    title = ''
    ref_node = get_first_element(tree, 'REF.PHYS')
    if not ref_node is None and ref_node.attrib and 'FILE' in ref_node.attrib.keys():
        filename = os.path.join(directory, ref_node.attrib['FILE']).strip()
        if filename and re.search(r'\.xml$', filename, re.IGNORECASE):
            if not os.path.exists(filename):
                raise InputError('Referred to file not found.')
                exit(1)
            local_xml_content = ET.parse(filename)
            local_root = local_xml_content.getroot()
            for title_element in get_all_elements(local_root, "TITLE"):
                title_text = get_element_text(title_element, False).strip()
                if len(title_text) > 0:
                    short_title = re.sub(r'\n.*', '', title_text)
                    return short_title
                    break            

def _find_qualifying_list_elements(article_node, min_items, min_chars):
    """
    Pre-scan an article element for LIST elements that qualify for definition-list
    sub-unit extraction.

    Returns a set of Python id() values for qualifying LIST elements.  Using id()
    avoids the cost of storing element references in a second structure and makes
    O(1) lookup inside get_element_text2.

    A LIST qualifies when it has >= min_items direct <ITEM> children AND the total
    character length of those items' text >= min_chars.
    """
    qualifying = set()
    for list_elem in article_node.iter():
        if not (list_elem.tag and list_elem.tag.upper() == 'LIST'):
            continue
        items = [c for c in list_elem if c.tag and c.tag.upper() == 'ITEM']
        if len(items) < min_items:
            continue
        total_chars = sum(len(get_element_text(item)) for item in items)
        if total_chars >= min_chars:
            qualifying.add(id(list_elem))
    return qualifying


def _extract_definition_list_text_and_breakpoints(list_elem):
    """
    Build the sub-unit text and item-boundary breakpoints for a LIST element.

    Iterates <ITEM> children, calling get_element_text2 on each to produce the
    same text that the normal traversal would produce.  Records a [offset, 1]
    breakpoint at the start of each item after the first.

    Returns (text, breakpoints).
    """
    list_type = list_elem.attrib.get('TYPE', '') if list_elem.attrib else ''
    if list_type in ['DASH', 'NDASH']:
        list_marker = '-- '
    elif list_type in ['BULLET', 'OTHER']:
        list_marker = '* '
    else:
        list_marker = ''

    full_text = ''
    breakpoints = []
    first_item = True
    for child in list_elem:
        if not (child.tag and child.tag.upper() == 'ITEM'):
            continue
        if not first_item:
            breakpoints.append([len(full_text), 1])
        prev_line_return = (full_text == '' or full_text.endswith('\n'))
        item_text, _, _ = get_element_text2(
            child, list_marker=list_marker, list_level=1,
            prev_line_return=prev_line_return,
        )
        full_text += item_text
        first_item = False
    return full_text.rstrip(), breakpoints


def _register_pending_definition_lists(
    parsed_content,
    item_pointer,
    pending_definition_lists,
    parent_type_name,
    parent_id,
    name_plural,
    file_path=None,
    parsing_logfile=None,
):
    """
    Register XML-intercepted definition-list elements as sub-units on the item.

    Mirrors _register_pending_large_tables.  For each intercepted LIST:
    - Extracts item text and item-boundary breakpoints.
    - Derives chunk_prefix from the parent text before the placeholder.
    - Builds a definition_list sub-unit dict with placeholder and chunk_prefix fields.
    - Replaces the temporary placeholder in item_pointer['text'] with the final one.
    - Updates sub_unit_index.
    """
    lists = pending_definition_lists.get('lists', [])
    if not lists:
        return
    param_pointer = parsed_content['document_information']['parameters']
    dl_param_key = find_or_create_definition_list_param_key(param_pointer)
    dl_type_name = param_pointer[dl_param_key]['name']
    dl_key_str = str(dl_param_key)
    di = parsed_content['document_information']
    di.setdefault('sub_unit_index', {})
    di['sub_unit_index'].setdefault(dl_key_str, {})
    taken = set(di['sub_unit_index'][dl_key_str].keys())

    parent_context = item_pointer.get('context', [])
    current_text = item_pointer['text']

    # Accumulate sub-units; merge into existing sub_units dict if tables were also extracted.
    dl_sub_units = {}
    index_entries = {}

    for local_num, list_elem in enumerate(lists, 1):
        sub_unit_key = assign_definition_list_key(local_num, parent_id, taken)
        taken.add(sub_unit_key)

        # Build sub-unit text and breakpoints from the LIST's ITEM children.
        dl_text, dl_breakpoints = _extract_definition_list_text_and_breakpoints(list_elem)

        # The temporary placeholder is the text that was emitted during traversal.
        temp_placeholder = f"[Definition list {local_num} pending sub-unit extraction]"

        # Derive chunk_prefix from the parent text before the placeholder.
        # Format it as a scope label so the model understands the definitional context.
        # The parser owns this format decision; processing stages prepend chunk_prefix verbatim.
        placeholder_pos = current_text.find(temp_placeholder)
        if placeholder_pos >= 0:
            preamble = current_text[:placeholder_pos].strip()
            chunk_prefix = f'[Scope: "{preamble}"]' if preamble else ''
        else:
            chunk_prefix = ''

        # Final placeholder references the sub-unit type name and key.
        final_placeholder = f"[Definition list {local_num} extracted as sub-unit {dl_type_name} {sub_unit_key}]"

        sub_unit = build_definition_list_sub_unit(
            dl_text, dl_breakpoints, chunk_prefix, final_placeholder,
            parent_context, parent_type_name, parent_id,
        )
        current_text = current_text.replace(temp_placeholder, final_placeholder)

        dl_sub_units[sub_unit_key] = sub_unit
        index_entries[sub_unit_key] = {
            "container_plural": name_plural,
            "container_id": parent_id,
            "path": [parent_id],
        }

    item_pointer['text'] = current_text

    # Merge into sub_units: may already have table sub-units under a different key.
    item_pointer.setdefault('sub_units', {})
    item_pointer['sub_units'][dl_key_str] = dl_sub_units
    di['sub_unit_index'][dl_key_str].update(index_entries)

    log_parsing_correction(file_path or "", "definition_list_extraction",
                           f"Extracted {len(dl_sub_units)} definition list(s) from "
                           f"{parent_type_name} '{parent_id}'", parsing_logfile)


def extract_articles(tree, parsed_content, parent_map, org_keyword_set, file_path=None, parsing_logfile=None):
    # Get the articles from the ENACTING.TERMS node(s).
    art_pointer = parsed_content['content']['articles']
    for enacting_node in get_all_elements(tree, 'ENACTING.TERMS'):
        for article_node in get_all_elements(enacting_node, 'ARTICLE'):
            # First, let's get the article number (an identifier, it may not be an actual number)
            art_num = ''
            ti_art_node = get_first_element(article_node, "TI.ART")
            if not ti_art_node is None:
                art_num = get_element_text(ti_art_node, False)
            if not '' == art_num:
                art_num = clean_text(re.sub(r'Article', '', art_num, flags=re.IGNORECASE))
            art_num = art_num.strip()
            if '' == art_num: # Alternative, if previous failed.
                if article_node.attrib and 'IDENTIFIER' in article_node.attrib.keys():
                    art_num = clean_text(re.sub(r'^0*', '', article_node.attrib['IDENTIFIER']))
                    log_parsing_correction(file_path, "article_number_fallback", 
                                         f"Used IDENTIFIER attribute for article number: '{art_num}'", 
                                         parsing_logfile)
            if '' == art_num: # We really shouldn't get here, but let's fill in something.
                art_num = 1
                while 'art_' + str(art_num) in art_pointer.keys():
                    art_num = art_num + 1
                art_num = 'art_' + str(art_num)
            # Now, let's get the title for the article.
            art_title = ''
            sti_art_node = get_first_element(article_node, "STI.ART")
            if not sti_art_node is None:
                art_title = clean_text(get_element_text(sti_art_node, False))
            art_pointer[art_num] = {}
            art_pointer[art_num]['unit_title'] = art_title
            art_pointer[art_num]['text'] = ''
            art_pointer[art_num]['breakpoints'] = []
            art_pointer[art_num]['notes'] = {}
            # Now, find the text portion.  We need to find all text that is not at the top-level TI.ART or STI.ART elements.
            pending_large_tables = []
            # If the article title contains "definition(s)", pre-scan for qualifying LIST elements
            # so they can be intercepted and extracted as definition-list sub-units.
            pending_definition_lists = None
            if title_contains_definition(art_title):
                min_items, min_chars = get_definition_list_thresholds(parser='formex')
                qualifying = _find_qualifying_list_elements(article_node, min_items, min_chars)
                if qualifying:
                    pending_definition_lists = {'to_intercept': qualifying, 'lists': []}
            for child in article_node:
                if not child.tag is None and not 'TI.ART' == child.tag and not 'STI.ART' == child.tag:
                    add_text_brkpts_notes(art_pointer[art_num], child, True, pending_large_tables, pending_definition_lists)
            art_pointer[art_num]['text'] = art_pointer[art_num]['text'].rstrip()
            # Remove any breakpoints that are now after the end of the text (since the rstrip may have removed non-printing characters at the end).
            while len(art_pointer[art_num]['breakpoints']) > 0: # It is possible that there are multiple breakpoints at the end that need to be removed.
                if art_pointer[art_num]['breakpoints'][-1][0] > len(art_pointer[art_num]['text']):
                    del art_pointer[art_num]['breakpoints'][-1]
                else:
                    break
            art_pointer[art_num]['context'] = get_org_context(article_node, parsed_content, parent_map, org_keyword_set)
            # Register XML-intercepted large tables as sub-units
            _register_pending_large_tables(
                parsed_content, art_pointer[art_num], pending_large_tables,
                'article', art_num, 'articles', file_path, parsing_logfile
            )
            # Register XML-intercepted definition lists as sub-units
            if pending_definition_lists:
                _register_pending_definition_lists(
                    parsed_content, art_pointer[art_num], pending_definition_lists,
                    'article', art_num, 'articles', file_path, parsing_logfile
                )
            # Now, add begin_article and stop_article markers to the organizational information.
            add_substantive_markers_org(parsed_content, art_pointer[art_num]['context'], 'article', art_num)

def extract_annexes(tree, parsed_content, org_keyword_set, file_path=None, parsing_logfile=None):
    # Get the annex, if any, from the ANNEX node.
    annex_pointer = parsed_content['content']['annexes']
    for annex_node in get_all_elements(tree, 'ANNEX'):
        # First, let's get the annex number (an identifier, it may not be an actual number) and title.
        annex_num = ''
        annex_title = ''
        no_num_flag = False
        title_annex_node = get_first_element(annex_node, 'TITLE')
        if not title_annex_node is None:
            ti_annex_node = get_first_element(title_annex_node, "TI")
            if not ti_annex_node is None:
                # Enhanced logic: Check if TI contains multiple P elements (malformed structure)
                p_elements = ti_annex_node.findall('P')
                if len(p_elements) > 1:
                    # Handle malformed case: first P is designation, subsequent P elements are title
                    annex_num = get_element_text(p_elements[0], False)
                    # Combine remaining P elements as title
                    title_parts = [get_element_text(p, False) for p in p_elements[1:]]
                    annex_title = ' '.join(title_parts).strip()
                    # Log the correction
                    log_parsing_correction(file_path, "malformed_annex_structure", 
                                         f"Multiple P elements in TI. Corrected designation: '{annex_num}', extracted title: '{annex_title}'", 
                                         parsing_logfile)
                else:
                    # Normal case: single content extraction
                    annex_num = get_element_text(ti_annex_node, False)
            if not '' == annex_num:
                # Validation: detect malformed designations with meaningful content issues
                if '\n' in annex_num or len(annex_num.strip().split()) > 3:
                    # Check if this is just trailing whitespace/newlines (not worth logging)
                    stripped = annex_num.strip()
                    lines = annex_num.split('\n')
                    potential_designation = lines[0].strip()
                    
                    # Determine if this is actually malformed or just trailing whitespace
                    is_just_trailing_whitespace = (
                        len(lines) == 2 and  # Only two parts
                        lines[0].strip() and  # First part has content
                        not lines[1].strip()  # Second part is empty/whitespace
                    )
                    
                    if potential_designation:
                        original_annex_num = annex_num
                        annex_num = potential_designation
                        # If we haven't found a title yet, use remaining content
                        if '' == annex_title and len(lines) > 1:
                            remaining_content = ' '.join(lines[1:]).strip()
                            if remaining_content:  # Only use if there's actual content
                                annex_title = remaining_content
                        
                        # Only log if this represents actual malformed content, not just trailing whitespace
                        if not is_just_trailing_whitespace:
                            log_parsing_correction(file_path, "malformed_annex_designation", 
                                                 f"Designation contained newlines/excess content. Corrected from '{original_annex_num.replace(chr(10), '\\n')}' to '{annex_num}'", 
                                                 parsing_logfile)
                
                annex_num = clean_text(re.sub(r'ANNEX', '', annex_num, flags=re.IGNORECASE))
                if '' == annex_num:
                    no_num_flag = True
            sti_annex_node = get_first_element(title_annex_node, "STI")
            if not sti_annex_node is None:
                annex_title = clean_text(get_element_text(sti_annex_node, False))
        annex_num = annex_num.strip()
        if '' == annex_num and not no_num_flag: # Alternative, if previous failed.
            if re.search(r'^\s*ANNEX.+', get_element_text(annex_node, False), flags=re.IGNORECASE):
                annex_num = re.search(r'^\s*ANNEX(.+)', get_element_text(annex_node, False), flags=re.IGNORECASE)
                annex_num = annex_num.group(1) if annex_num else ''
                annex_num = annex_num.strip()
                if annex_num:
                    log_parsing_correction(file_path, "annex_number_fallback_regex", 
                                         f"Used regex fallback for annex number: '{annex_num}'", 
                                         parsing_logfile)
        if '' == annex_num: # Typically this is because there is no number given for the annex.
            annex_num = 1
            while 'annex_' + str(annex_num) in annex_pointer.keys():
                annex_num = annex_num + 1
            annex_num = 'annex_' + str(annex_num)
            log_parsing_correction(file_path, "annex_number_generated", 
                                 f"Generated annex number (no number found): '{annex_num}'", 
                                 parsing_logfile)
        if '' == annex_title:
            content_node = get_first_element(annex_node, "CONTENTS")
            if not content_node is None:
                # Improved fallback: Look for title patterns rather than just first TI
                for ti_element in content_node.findall('.//TI'):
                    text = clean_text(get_element_text(ti_element, False))
                    # Skip numbered items (likely content, not title)
                    if text and not re.match(r'^\s*\d+\.', text):
                        annex_title = text
                        log_parsing_correction(file_path, "annex_title_fallback", 
                                             f"Found title in fallback search: '{annex_title}'", 
                                             parsing_logfile)
                        break
                
                # If still no title found, use the old logic as final fallback
                if '' == annex_title:
                    ti_annex_node = get_first_element(content_node, "TI")
                    if not ti_annex_node is None:
                        annex_title = clean_text(get_element_text(ti_annex_node, False))
                        if annex_title:
                            log_parsing_correction(file_path, "annex_title_final_fallback", 
                                                 f"Using first TI element as title: '{annex_title}'", 
                                                 parsing_logfile)

        annex_pointer[annex_num] = {}
        annex_pointer[annex_num]['unit_title'] = annex_title
        annex_pointer[annex_num]['text'] = ''
        annex_pointer[annex_num]['breakpoints'] = []
        annex_pointer[annex_num]['notes'] = {}
        # Now, find the text portion.  We need to find all text that is not at the top-level TI.ART or STI.ART elements.
        pending_large_tables = []
        pending_definition_lists = None
        if title_contains_definition(annex_title):
            min_items, min_chars = get_definition_list_thresholds(parser='formex')
            qualifying = _find_qualifying_list_elements(annex_node, min_items, min_chars)
            if qualifying:
                pending_definition_lists = {'to_intercept': qualifying, 'lists': []}
        for content_node in get_all_elements(annex_node, 'CONTENTS'):
            add_text_brkpts_notes(annex_pointer[annex_num], content_node, True, pending_large_tables, pending_definition_lists)
            # annex_pointer[annex_num]['text'] += get_element_text(content_node)
        if '' == annex_pointer[annex_num]['text']: # As a fallback, if there is no CONTENTS node, just use the whole ANNEX node.
            add_text_brkpts_notes(annex_pointer[annex_num], annex_node, True, pending_large_tables, pending_definition_lists)
            log_parsing_correction(file_path, "annex_text_fallback", 
                                 f"Used full ANNEX node for text extraction (no CONTENTS) for annex '{annex_num}'", 
                                 parsing_logfile)
            # annex_pointer[annex_num]['text'] = get_element_text(annex_node)
        annex_pointer[annex_num]['text'] = annex_pointer[annex_num]['text'].rstrip()
        # Remove any breakpoints that are now after the end of the text (since the rstrip may have removed non-printing characters at the end).
        while len(annex_pointer[annex_num]['breakpoints']) > 0: # It is possible that there are multiple breakpoints at the end that need to be removed.
            if annex_pointer[annex_num]['breakpoints'][-1][0] > len(annex_pointer[annex_num]['text']):
                del annex_pointer[annex_num]['breakpoints'][-1]
            else:
                break

        # As with recitals, assume that annexes are always at the level of the top organizational unit.
        annex_pointer[annex_num]['context'] = get_org_top_unit(parsed_content, org_keyword_set)
        # Register XML-intercepted large tables as sub-units
        _register_pending_large_tables(
            parsed_content, annex_pointer[annex_num], pending_large_tables,
            'annex', annex_num, 'annexes', file_path, parsing_logfile
        )
        # Register XML-intercepted definition lists as sub-units
        if pending_definition_lists:
            _register_pending_definition_lists(
                parsed_content, annex_pointer[annex_num], pending_definition_lists,
                'annex', annex_num, 'annexes', file_path, parsing_logfile
            )
        # Now, add begin_annex and stop_annex markers to the organizational information.
        add_substantive_markers_org(parsed_content, annex_pointer[annex_num]['context'], 'annex', annex_num)

def extract_content(tree, parsed_content, directory, current_file=None, parsing_logfile=None):
    # Go through the document described in tree, and put the various
    # types of content into the parsed_content dictionary.  Find other
    # files in the indicated directory.
    # parent_map is used for traversing the tree upwards.

    parent_map = {child: parent for parent in tree.iter() for child in parent} # Create parent map to make traversal of the tree upwards practical.
    operational_item_name_set = get_operational_item_name_set(parsed_content)
    org_keyword_set = {'unit_title', 'unit_definitions'}
    for item_name in operational_item_name_set:
        org_keyword_set.add('begin_' + item_name)
        org_keyword_set.add('stop_' + item_name)

    # # Recitals
    extract_recitals(tree, parsed_content, org_keyword_set, current_file, parsing_logfile)

    # # Articles
    extract_articles(tree, parsed_content, parent_map, org_keyword_set, current_file, parsing_logfile)

    # # Annexes
    extract_annexes(tree, parsed_content, org_keyword_set, current_file, parsing_logfile)

    # Now, do the same for any documents referred to in the tree.
    for ref_node in get_all_elements(tree, 'REF.PHYS'):
        if ref_node.attrib and 'FILE' in ref_node.attrib.keys():
            filename = os.path.join(directory, ref_node.attrib['FILE']).strip()
            if filename and re.search(r'\.xml$', filename, re.IGNORECASE):
                if not os.path.exists(filename):
                    raise InputError('Referred to file not found.')
                    exit(1)
                local_xml_content = ET.parse(filename)
                local_root = local_xml_content.getroot()
                extract_content(local_root, parsed_content, directory, filename, parsing_logfile)

def parse_formex_directive(file_path, parsing_logfile=None):
    legal_value = {'DIR': "directive", 'REG': "regulation"} # Probably need to add DIRDEL and DIRIMP later.
    xml_content = ET.parse(file_path)
    root = xml_content.getroot()
    content = {} # This is the object that will hold the parsed information.
    directory = os.path.dirname(file_path)
    if not directory or not os.path.exists(directory):
        raise InputError('Directory not found.')
        exit(1)
    print('Directory: ' + directory)
    # Determine whether this is a directive or regulation.
    matches = root.findall(".//LEGAL.VALUE")
    if not matches:
        raise InputError('LEGAL.VALUE not found.')
        exit(1)
    if len(matches) > 1:
        InputWarning('More than one LEGAL.VALUE found.')
    legal_value_flag = False
    top_org_type = ''
    for m in matches:
        if m.text.upper() in legal_value.keys():
            legal_value_flag = True
            top_org_type = legal_value[m.text.upper()]
            break
    if not legal_value_flag:
        print('Not a supported type of document.')
        return content

    # Start setting up document_information
    content['document_information'] = {}
    content['document_information']['version'] = '0.5'
    content['content'] = {}

    # Get the document title
    for title_element in get_all_elements(root, "TITLE"):
        title_text = get_element_text(title_element, False).strip()
        if len(title_text) > 0:
            short_title = re.sub(r'\n.*', '', title_text)
            content['document_information']['title'] = short_title
            content['document_information']['long_title'] = title_text
            break
    # Often, the only place to find the actual short title is in the first subsidiary document.
    if content['document_information']['title'].strip() == content['document_information']['long_title'].strip():
        short_title = get_title_from_sub_doc(root, content, directory)
        if not '' == short_title and not short_title == content['document_information']['long_title']:
            content['document_information']['title'] = short_title
        
    # Set parameters
    content['document_information']['parameters'] = {}
    param_pointer = content['document_information']['parameters']
    for i in [1, 2, 3]:
        item_type_name, item_type_name_plural, operational = set_items(i)
        param_pointer[i] = {}
        param_pointer[i]['name'] = item_type_name
        param_pointer[i]['name_plural'] = item_type_name_plural
        param_pointer[i]['operational'] = operational
        content['content'][item_type_name_plural] = {}

    content['document_information']['organization'] = {}
    org_pointer = content['document_information']['organization']   
    org_pointer['item_types'] = [2, 3]
    org_pointer['content'] = {}
    org_content_pointer = org_pointer['content']
    org_content_pointer[top_org_type] = {}
    # Strip the name of the type from short_title.
    short_title = re.sub(r'^' + re.escape(top_org_type.strip()), '', short_title, flags=re.IGNORECASE).strip()
    org_content_pointer[top_org_type][short_title] = {}
    
    # Create parsing issues logfile (use provided one or create new)
    if parsing_logfile is None:
        parsing_logfile = get_parsing_issues_logfile(directory)
    
    extract_content(root, content, directory, file_path, parsing_logfile)

    return content

def process_file(input_file_path, config):
    """
    Process a single Formex file with manifest support.

    Args:
        input_file_path: Path to input Formex XML file
        config: Configuration dictionary
    """
    file_name = os.path.basename(input_file_path)

    # Determine file stem - strip .doc.fmx.xml or .doc.xml
    if file_name.endswith('.doc.fmx.xml'):
        file_stem = file_name[:-len('.doc.fmx.xml')]
    elif file_name.endswith('.doc.xml'):
        file_stem = file_name[:-len('.doc.xml')]
    else:
        file_stem = re.sub(r'\.\w+$', '', file_name)

    print(f'Processing: {file_name}')

    # Get output directory
    output_dir = get_output_directory(config)

    # All Formex files go to a common 'Formex' subdirectory
    doc_output_dir = os.path.join(output_dir, 'Formex')
    Path(doc_output_dir).mkdir(parents=True, exist_ok=True)

    # Get manifest path and manager
    manifest_path = get_manifest_path(doc_output_dir, file_stem)
    manifest_mgr = ManifestManager(manifest_path)

    # Create or load manifest
    manifest = manifest_mgr.create_or_load(
        source_file=os.path.abspath(input_file_path),
        source_type='formex',
        parser='formex_set_parse.py',
        parser_type='formex'
    )
    manifest_mgr.update_source_hash(manifest, input_file_path)

    # Output file path
    output_filename = f'{file_stem}_parse_output.json'
    output_path = os.path.join(doc_output_dir, output_filename)

    # Check if already exists in manifest
    existing_files = manifest_mgr.get_parsed_files(
        manifest,
        filter_criteria={'type': 'full_document'}
    )
    if existing_files and os.path.exists(existing_files[0]['abs_path']):
        print(f'  Already parsed (in manifest): {output_filename}')
        return

    # Get parsing logfile
    parsing_logfile = get_parsing_issues_logfile(doc_output_dir)

    print(f'  Parsing Formex file')
    try:
        parsed_content = parse_formex_directive(input_file_path, parsing_logfile)
    except (ConfigError, ParseError) as e:
        print(f"  Error: {e}")
        return

    if parsed_content and parsed_content != {}:
        print('  Writing output...')
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(parsed_content, f, indent=4, ensure_ascii=False)
        print(f'  Output written: {output_filename}')

        # Update manifest
        manifest_mgr.add_parsed_file(
            manifest,
            output_path,
            'full_document',
            organizational_units={}
        )
        manifest_mgr.save(manifest)


def process_directory(dir_path, config, recursive=True):
    """
    Process directory of Formex files.

    Args:
        dir_path: Path to directory or file
        config: Configuration dictionary
        recursive: Whether to process subdirectories
    """
    # Handle single file
    if os.path.isfile(dir_path):
        if dir_path.endswith('.doc.xml') or dir_path.endswith('.doc.fmx.xml'):
            process_file(dir_path, config)
        return

    # Handle directory
    if not os.path.isdir(dir_path):
        raise InputError('Input not a directory or file.')

    for item in os.listdir(dir_path):
        item_path = os.path.join(dir_path, item)

        if os.path.isfile(item_path) and (item.endswith('.doc.xml') or item.endswith('.doc.fmx.xml')):
            process_file(item_path, config)
        elif recursive and os.path.isdir(item_path):
            print(f'Moving to directory: {item_path}')
            process_directory(item_path, config, recursive)


# Main execution block
def main():
    """Main entry point for Formex parser."""
    parser = argparse.ArgumentParser(
        description='Parse EU Formex XML files into structured JSON.'
    )
    parser.add_argument(
        'input_path',
        help='Path to Formex XML file or directory containing Formex files'
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

    # Validate input path
    if not os.path.exists(args.input_path):
        print(f"Error: Input path does not exist: {args.input_path}")
        sys.exit(1)

    # Process directory or file
    try:
        process_directory(
            args.input_path,
            config,
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