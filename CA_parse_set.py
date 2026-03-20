"""
California Legal Document Parser (CA_parse_set.py)

This module parses HTML files of California statutes and regulations from the California Legislative Information website
(https://leginfo.legislature.ca.gov/faces/codes.xhtml) and converts them into a standardized JSON format.

The parser handles California legal documents that contain sections as their primary legal units. Each section
is extracted with its text content, organizational context, and metadata.

Output Format:
- document_information: Metadata about the document including version, parameters, and organizational structure
- content: The actual legal content organized by section numbers
- Each section includes text, context (organizational hierarchy), and optional annotations

Key Features:
- Extracts organizational hierarchy (Code, Title, Chapter, Part, etc.)
- Handles section numbering and text extraction
- Manages indentation and formatting
- Extracts annotations (amendments, repeals, etc.)
- Maintains context mapping for each section
"""
# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License

from bs4 import BeautifulSoup
from bs4.element import Tag
from bs4.element import Comment
from collections import defaultdict
import re
import json
import os
import sys
import argparse
from pathlib import Path
from utils import clean_text
from utils import ConfigError
from utils import ParseError
from utils import ParseWarning
from utils import InputError
from utils import canonical_org_types
from utils import table_to_text
from utils import html_table_to_plaintext
from utils.table_handling import LARGE_TABLE_ROW_THRESHOLD
from utils.large_table_common import (
    assign_table_key,
    find_or_create_table_param_key,
    build_table_sub_unit_from_html,
)
from utils import extract_trailing_paren
from utils import GetClient
from utils import GetLogfile
from utils.error_handling import log_parsing_correction
from utils.config import get_config, get_output_directory, get_output_structure
from utils.manifest_utils import ManifestManager, get_manifest_path

# Parses for version 0.3
# Parses files from https://leginfo.legislature.ca.gov/faces/codes.xhtml

def _build_ca_table_sub_unit(table_tag, local_counter, parent_context, section_number):
    """Build a table sub-unit dict from a BeautifulSoup table Tag."""
    row_count = len(table_tag.find_all('tr'))
    table_html = str(table_tag)
    headers = []
    thead = table_tag.find('thead')
    if thead:
        for th in thead.find_all('th'):
            headers.append(clean_text(th.get_text()))
    caption_tag = table_tag.find('caption')
    caption = clean_text(caption_tag.get_text()) if caption_tag else ''
    return build_table_sub_unit_from_html(
        table_html, row_count, headers, caption,
        local_counter, parent_context, 'section', section_number
    )


def _register_ca_large_tables(content, item_pointer, pending_large_tables, section_number, org_context, parsing_logfile=None):
    """Register large HTML tables as sub-units on the section."""
    if not pending_large_tables:
        return
    param_pointer = content['document_information']['parameters']
    table_param_key = find_or_create_table_param_key(param_pointer)
    table_type_name = param_pointer[table_param_key]['name']
    table_key_str = str(table_param_key)
    di = content['document_information']
    di.setdefault('sub_unit_index', {})
    di['sub_unit_index'].setdefault(table_key_str, {})
    taken = set(di['sub_unit_index'][table_key_str].keys())
    table_sub_units = {}
    index_entries = {}
    # Build section context (org hierarchy + section) for sub-units
    parent_context = [{entry['name']: entry['number']} for entry in org_context] if org_context else []
    current_text = item_pointer['text']
    for local_num, table_tag in enumerate(pending_large_tables, 1):
        sub_unit_key = assign_table_key(local_num, section_number, taken)
        taken.add(sub_unit_key)
        sub_unit = _build_ca_table_sub_unit(table_tag, local_num, parent_context, section_number)
        current_text = current_text.replace(
            f"[Table {local_num} pending sub-unit extraction]",
            f"[Table {local_num} extracted as sub-unit {table_type_name} {sub_unit_key}]",
        )
        table_sub_units[sub_unit_key] = sub_unit
        index_entries[sub_unit_key] = {
            "container_plural": "sections",
            "container_id": section_number,
            "path": [section_number],
        }
    item_pointer['text'] = current_text
    item_pointer['sub_units'] = {table_key_str: table_sub_units}
    di['sub_unit_index'][table_key_str].update(index_entries)
    log_parsing_correction("", "large_table_extraction",
                           f"Extracted {len(table_sub_units)} large table(s) from section '{section_number}'",
                           parsing_logfile)


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

def set_items(item_type):
    """
    Configure item type parameters for California legal documents.
    
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

def parse_html(file_path, parsing_logfile=None):
    """
    Parse a California legal document HTML file into structured JSON format.
    
    This function processes HTML files from the California Legislative Information website,
    extracting the organizational structure and legal content. It handles:
    - Document metadata and title extraction
    - Organizational hierarchy (Code, Title, Chapter, Part, etc.)
    - Section content with proper formatting and indentation
    - Context mapping for each section
    - Annotation extraction (amendments, repeals, etc.)
    
    Args:
        file_path (str): Path to the HTML file to parse
        parsing_logfile (str, optional): Path to logfile for parsing corrections
        
    Returns:
        dict: Structured JSON representation of the legal document
        
    Raises:
        ParseError: If the document structure cannot be parsed
    """
    # Load the HTML file
    with open(file_path, 'r', encoding='utf-8') as f:
        soup = BeautifulSoup(f, 'html.parser')

    # Initialize data structure
    content = defaultdict(dict)

    content['document_information'] = {}
    content['document_information']['version'] = '0.5'
    
    # Set parameters
    content['document_information']['parameters'] = {}
    param_pointer = content['document_information']['parameters']
    for i in [1]:
        item_type_name, item_type_name_plural, operational = set_items(i)
        param_pointer[i] = {}
        param_pointer[i]['name'] = item_type_name
        param_pointer[i]['name_plural'] = item_type_name_plural
        param_pointer[i]['operational'] = operational  

    content['document_information']['organization'] = {}
    org_pointer = content['document_information']['organization']   
    org_pointer['item_types'] = [1]
    org_pointer['content'] = {}
    org_content_pointer = org_pointer['content']

    content['content'] = {}
    cont_pointer = content['content']

    # Find the main content container - California documents use specific div IDs
    law = soup.find('div', id='manylawsections')
    if not law:
        law = soup.find('div', id='single_law_section')
    
    if not law:
        raise ParseError('Unable to find code sections.')
        exit(1)

    current_div = law.find('div', style=lambda x: x and not x == '')
    # Parse organization items

    org_name = ''
    org_short_name = ''
    text = ''
    # org_context is a list of dictionaries.  Each successive item is one level deeper in the context.
    # Each dictionary in the context includes:
    # 'name' - the singular version of the name for the organizational type (e.g. title, chapter, part).
    # 'name_plural' - the plural version of the 'name', because in some cases (like appendix) the plural is more complicated than adding an 's'.
    # 'number' - the identifier of this particular instance of 'name'.  It may be a number, but can be a more complex string (e.g. '285m-2', or 'XXI').
    # 'unit_title' - the title of the instance.
    org_context = []
    
    # Check for org information.
    text = current_div.get_text()

    # Extract Code information - California documents follow pattern "Code Name - Short Name"
    if re.search(r'.*Code.*-.*\w+.*', text, re.IGNORECASE):
        org_content_pointer['code'] = {} # All CA code sections are in a designated Code (e.g. Business and Profressions Code).
        regex_out = re.search(r'^\s*(\w[\w\s]*)\s\-\s(\w[\w\s]*)\s*$', text)
        if regex_out and len(regex_out.group(1)) > 0 and len(regex_out.group(2)) > 0:
            org_name = clean_text(regex_out.group(1))
            org_short_name = clean_text(regex_out.group(2))
            if not '' == org_name and not '' == org_short_name:
                org_type, org_type_plural = canonical_org_types('code')
                org_context.append({'name': org_type, 'name_plural': org_type_plural, 'number': org_short_name, 'unit_title': org_name})                
                content['document_information']['title'] = clean_text(text)
                content['document_information']['long_title'] = clean_text(text) + '\n'
        else:
            log_parsing_correction(file_path, "org_regex_failure", 
                                 f"Failed to parse code name/short name from: '{text}'", 
                                 parsing_logfile)
            raise ParseError('Failed to find top-level org information.\n')
            exit(1)
    else:
        log_parsing_correction(file_path, "org_format_failure", 
                             f"Text does not match expected Code format: '{text}'", 
                             parsing_logfile)
        raise ParseError('Failed to find top-level org information.\n')
        exit(1)
    
    # Process organizational hierarchy (Title, Chapter, Part, etc.)
    current_div = current_div.find_next('div')
    while current_div and not re.search(r'^\s*\d', current_div.get_text()): # Go through each successive level of organization.
        if not current_div.div:  # Skip div objects that contain other div objects.
            if re.search(r'^\s*\w[\w]*\s+\d[\d\.]*\.\s+', current_div.get_text()):
                regex_out = re.search(r'^\s*(\w[\w]*)\s+(\d[\d\.]*)\.\s+([^\(\[]*)', current_div.get_text())
                if regex_out and len(regex_out.group(1)) > 0 and len(regex_out.group(2)) > 0:
                    org_name = clean_text(regex_out.group(1))
                    org_num = clean_text(regex_out.group(2))
                    org_title = ''
                    if len(regex_out.groups()) > 2 and len(regex_out.group(3)) > 0:
                        org_title = clean_text(regex_out.group(3)).strip()
                    if not '' == org_name and not '' == org_num:
                        org_type, org_type_plural = canonical_org_types(org_name)
                        org_context.append({'name': org_type, 'name_plural': org_type_plural, 'number': org_num, 'unit_title': org_title})                                            
            if len(clean_text(current_div.get_text())) > 0:                  
                content['document_information']['long_title'] += clean_text(current_div.get_text()) + '\n'                
        current_div = current_div.find_next('div')
    
    # Write organizational information to the 'organization' structure.
    local_org_pointer = org_content_pointer # Start at the root.
    for entry in org_context:
        name = entry['name']
        number = entry['number']
        if not name in local_org_pointer.keys():
            local_org_pointer[name] = {}
        if not number in local_org_pointer[name]:
            local_org_pointer[name][number] = {}
        local_org_pointer = local_org_pointer[name][number]
        if not 'unit_title' in local_org_pointer.keys():
            local_org_pointer['unit_title'] = entry['unit_title']        

    # Now step through the sections.  
    section_name_plural = str(param_pointer[1]['name_plural']) # Although there is only the one type in these documents, best to keep the name in one place.
    cont_pointer[section_name_plural] = {}
    while current_div:
        section_text = ''
        if not '' == clean_text(current_div.get_text()) and re.search(r'^\s*\d+[\.\d]*', clean_text(current_div.get_text())): # Test for text that begins with a number that may contain period markers.  
            regex_out = re.search(r'^\s*(\d+[\.\d]*)', clean_text(current_div.get_text())) # No spaces in section number.
            if (regex_out):
                section_number = str(regex_out.group(1))
                if section_number:
                    section_number = section_number.rstrip(".") # Remove trailing period mark.
                    original_section_number = section_number
                    while section_number in cont_pointer[section_name_plural].keys():
                        section_number += '_dup'
                        ParseWarning('Created duplicate entry: ' + section_number)
                        log_parsing_correction(file_path, "duplicate_section", 
                                             f"Duplicate section {original_section_number}, created {section_number}", 
                                             parsing_logfile)
                    cont_pointer[section_name_plural][section_number] = {} # Create entry in 'contents' structure for this section.
                    local_content_pointer = cont_pointer[section_name_plural][section_number]                    
                    if current_div.string:
                        ParseError("Uncaptured text: " + current_div.string)
                        exit(1)
                    # Finding the statutory text: 'p' tags (leaf only) and 'table' tags in document order.
                    last_subdiv = None # Variable to point to the last <p> tag.
                    breakpoints = []
                    current_indent_level = 0
                    pending_large_tables = []
                    for subdiv in current_div.find_all(['p', 'table']):  # Document order
                        if subdiv.name == 'table':
                            tr_list = subdiv.find_all('tr')
                            if len(tr_list) >= LARGE_TABLE_ROW_THRESHOLD:
                                pending_large_tables.append(subdiv)
                                local_num = len(pending_large_tables)
                                section_text += f"[Table {local_num} pending sub-unit extraction]"
                                breakpoints.append([len(section_text), 1])
                            else:
                                tbl_text = html_table_to_plaintext(subdiv)
                                if tbl_text:
                                    section_text += tbl_text + '\n'
                            continue
                        # p element - leaf check
                        child_p = subdiv.find_all('p')
                        if not child_p or len(child_p) < 1:
                            paragraph_indent_level = 0
                            last_subdiv = subdiv
                            breakpoint_value = len(section_text) # This is where in the text it may be broken into separate chunks.
                            breakpoint_flag = False
                            if subdiv.get('style') and (re.search(r'margin-left:\s*\d+\.*\d*em', subdiv.get('style'))): # Handle indenting.
                                regex_out = re.search(r'margin-left:\s*(\d+\.*\d*)em', subdiv.get('style'))
                                if regex_out:
                                    paragraph_indent_level = int(float(regex_out.group(1)))
                                    section_text += '    '*paragraph_indent_level
                            # Any zero-indent paragraph that starts with an opening parenthetical is likely to be a new subsection, so assume that a breakpoint is appropriate.
                            if 0 == paragraph_indent_level and re.search(r'^\s*\(', subdiv.get_text()):
                                breakpoint_flag = True
                            if len(clean_text(subdiv.get_text())) > 0:    # Only applies if there is actual text in this div.
                                section_text += clean_text(subdiv.get_text()) + '\n'
                                if ((breakpoint_value > 0) and 
                                    (paragraph_indent_level < current_indent_level or breakpoint_flag)):
                                    breakpoints.append([breakpoint_value, int(paragraph_indent_level+1)])
                                current_indent_level = paragraph_indent_level
                    # Need to detect if there is text after the last <p> tag.  If so, tack it on to the section_text (it is likely annotation text detected below).
                    # It looks like documents with only one section don't include the annotation (with parenthetical) within a <p> tag, so we need to look for extra text.
                    # Skip table elements - they are already processed in the main loop.
                    if not None == last_subdiv:
                        for extra_text in last_subdiv.next_siblings:
                            if hasattr(extra_text, 'name') and extra_text.name == 'table':
                                continue  # already processed in main loop
                            section_text += clean_text(extra_text.get_text() if hasattr(extra_text, 'get_text') else str(extra_text))
                    section_text, annotation = extract_trailing_paren(section_text)
                    if not '' == annotation:
                        # Any trailing parenthetical will be deemed an annotation.  We could check that it starts with common words (e.g. Amended, Repealed, Added) but that does not
                        # seem necessary.
                        local_content_pointer['annotation'] = annotation

                    local_content_pointer['text'] = section_text

                    # Register XML-level intercepted large tables as sub-units
                    if pending_large_tables:
                        _register_ca_large_tables(
                            content, local_content_pointer, pending_large_tables,
                            section_number, org_context, parsing_logfile
                        )
                    
                    # If last breakpoint is at the end of the text, remove it.
                    if len(breakpoints) > 0:
                        breakpoint_entry = breakpoints.pop()
                        if breakpoint_entry[0] < len(section_text.strip()):
                            breakpoints.append(breakpoint_entry)

                    # Record breakpoints
                    local_content_pointer['breakpoints'] = breakpoints

                    # As far as I can tell, these statutes do not include footnotes.
                    local_content_pointer['notes'] = {}
                    
                    # In a change from version 0.2, context will be stored as a list of dictionaries that correspond
                    # to the keys needed to find the relevant context within the organization structure.
                    local_content_pointer['context'] = []
                    if not [] == org_context:
                        local_org_pointer = content['document_information']['organization']['content'] # Start at the root.
                        begin_label = 'begin_' + item_type_name
                        stop_label = 'stop_' + item_type_name            
                        for entry in org_context:
                            local_content_pointer['context'].append({entry['name']: entry['number']})
                            # Fill out beginning and ending numbers.
                            name = entry['name']
                            number = entry['number']
                            if name in local_org_pointer.keys() and number in local_org_pointer[name]:
                                local_org_pointer = local_org_pointer[name][number]
                                if name == org_context[-1]['name']: # Only update begin and stop labels for the deepest organizational entry.                                
                                    if not begin_label in local_org_pointer.keys():
                                        local_org_pointer[begin_label] = ''    
                                    if '' == local_org_pointer[begin_label]:  # Only need this for the first one encountered.
                                        local_org_pointer[begin_label] = section_number
                                    local_org_pointer[stop_label] = section_number # By always updating the stop item we will automatically have the right value.                   
                else:
                    log_parsing_correction(file_path, "section_regex_extraction_failure", 
                                         f"Found matching pattern but failed to extract section number from: '{clean_text(current_div.get_text())}'", 
                                         parsing_logfile)
                    ParseError('Did not find section number in regex that should contain it: ' + clean_text(current_div.get_text()))
                    exit(1)
            else:
                log_parsing_correction(file_path, "section_regex_pattern_failure", 
                                     f"Text should contain section number but pattern not found: '{clean_text(current_div.get_text())}'", 
                                     parsing_logfile)
                ParseError('No regex found where section number where it should be: ' + clean_text(current_div.get_text()))
                exit(1)
        else:
            if not '' == clean_text(current_div.get_text()):
                log_parsing_correction(file_path, "extra_material_found", 
                                     f"Found possible extra material: '{clean_text(current_div.get_text())}'", 
                                     parsing_logfile)
                ParseError('Found possible extra material: ' + clean_text(current_div.get_text()))
                exit(1)
        current_div = current_div.next_sibling                        
    return content

def process_file(input_file_path, config):
    """
    Process a single CA HTML file with manifest support.

    Args:
        input_file_path: Path to input HTML file
        config: Configuration dictionary
    """
    file_name = os.path.basename(input_file_path)
    file_stem = re.sub(r'\.\w+$', '', file_name)

    print(f'Processing: {file_name}')

    # Get output directory
    output_dir = get_output_directory(config)

    # All CA files go to a common 'CA' subdirectory
    doc_output_dir = os.path.join(output_dir, 'CA')
    Path(doc_output_dir).mkdir(parents=True, exist_ok=True)

    # Get manifest path and manager
    manifest_path = get_manifest_path(doc_output_dir, file_stem)
    manifest_mgr = ManifestManager(manifest_path)

    # Create or load manifest
    manifest = manifest_mgr.create_or_load(
        source_file=os.path.abspath(input_file_path),
        source_type='ca_html',
        parser='CA_parse_set.py',
        parser_type='ca_html'
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

    print(f'  Parsing HTML file')
    try:
        parsed_content = parse_html(input_file_path, parsing_logfile)
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
    Process directory of CA HTML files.

    Args:
        dir_path: Path to directory or file
        config: Configuration dictionary
        recursive: Whether to process subdirectories
    """
    # Handle single file
    if os.path.isfile(dir_path):
        if dir_path.endswith('.html'):
            process_file(dir_path, config)
        return

    # Handle directory
    if not os.path.isdir(dir_path):
        raise InputError('Input not a directory or file.')

    for item in os.listdir(dir_path):
        item_path = os.path.join(dir_path, item)

        if os.path.isfile(item_path) and item.endswith('.html'):
            process_file(item_path, config)
        elif recursive and os.path.isdir(item_path):
            print(f'Moving to directory: {item_path}')
            process_directory(item_path, config, recursive)


# Main execution block
def main():
    """Main entry point for CA HTML parser."""
    parser = argparse.ArgumentParser(
        description='Parse California legal code HTML files into structured JSON.'
    )
    parser.add_argument(
        'input_path',
        help='Path to HTML file or directory containing HTML files'
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