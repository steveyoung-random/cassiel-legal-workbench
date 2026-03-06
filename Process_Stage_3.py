"""
Document Analysis Stage 3 Processor (Process_Stage_3.py)

This module performs the third stage of legal document processing, processing JSON files created by
Process_Stage_2.py to add AI-generated summaries.

The processor uses AI models to:
- Generate level 1 summaries of individual legal units (sections, articles, etc.)
- Create hierarchical summaries of organizational units

This stage prepares documents for the final question-answering stage by creating a rich
knowledge base of summarized content and properly scoped definitions.

Key Features:
- AI-powered content summarization
- Incremental processing with state preservation
- Manifest-based file discovery
- Processing status tracking
"""
# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License

import re
import json
import os
import sys
import argparse
import time as time_module
from pathlib import Path
from functools import cache
from math import e
from bs4 import BeautifulSoup
from bs4.element import Tag
from bs4.element import Comment
from collections import defaultdict
from thefuzz import process  # For fuzzy term matching
from utils import *
from utils.config import get_config, get_output_directory, get_checkpoint_threshold
from utils.ai_client import query_text_with_retry
from utils.processing_status import (
    init_processing_status,
    update_stage_3_progress,
    count_stage_3_progress,
    is_stage_2_complete,
    is_stage_3_complete,
    get_processing_status
)
from utils.document_handling import iter_operational_items, iter_containers, has_sub_units, lookup_item, build_metadata_suffix, augment_chunk_with_metadata
from utils.text_processing import clean_summary_text, strip_emphasis_marks
from utils.chunking_helpers import create_chunk_summary_prompt, synthesize_final_summary
from utils.manifest_utils import discover_parse_files, parse_filter_string

class SummaryProcessor:
    """
    Processor class for managing summary creation.
    
    This class provides a centralized interface for processing summaries,
    managing AI client interactions, and handling document state updates.
    
    Attributes:
        client: AI client for making queries
        logfile: Log file for tracking operations
        parsed_content: The document content being processed
        out_path: Output file path for saving updates
        table_of_contents: Document table of contents
        dirty: Flag indicating if content has been modified
        org_item_name_set: Set of organizational item names
        org_item_name_string: Formatted string of organizational item names
        type_name_list: List of operational item type names
        type_list_or_string: Formatted string of operational item types
    """
    def __init__(self, client, logfile, parsed_content, out_path, clients=None, config=None):
        self.client = client
        self.logfile = logfile
        self.parsed_content = parsed_content
        self.out_path = out_path
        self.table_of_contents = create_table_of_contents(parsed_content, parsed_content['document_information']['organization']['content'], 0, 0, 1)
        self.dirty = 0
        self.last_flush_time = time_module.time()  # Track when last flush occurred

        # Task-specific clients and config for flexible model system
        self.clients = clients if clients is not None else {'organizational': client, 'level1': client, 'level1_with_references': client, 'level2': client, 'topic_statement': client}
        self.config = config

        # Helpers:
        self.org_item_name_set = get_organizational_item_name_set(parsed_content)
        self.org_item_name_string = get_list_string(sorted(self.org_item_name_set), 'or') # String of org_item_name names separated by commas, ending with 'or' if multiple.
        self.type_name_list = sorted(get_full_item_name_set(parsed_content)) # Note that this gathers all item_name names, not just operational ones.
        self.type_list_or_string = get_list_string(self.type_name_list, 'or') # String of all item_name names separated by commas, ending with 'or' if multiple.

    def flush(self):
        """
        Write updated content to file if any changes have been made.
        Updates last_flush_time after successful write.
        Forces sync to disk to ensure immediate visibility on Windows.
        """
        self.dirty = write_if_updated(self.parsed_content, self.out_path, self.dirty)
        if self.dirty == 0:  # Write was successful (dirty flag reset)
            self.last_flush_time = time_module.time()
            # Force sync to disk on Windows to ensure immediate visibility
            try:
                fd = os.open(self.out_path, os.O_RDONLY)
                os.fsync(fd)
                os.close(fd)
            except Exception:
                pass  # If sync fails, continue anyway (file was still written)
    
    def should_flush_by_time(self, time_threshold_seconds=30):
        """
        Check if enough time has passed since last flush to trigger a flush.
        
        Args:
            time_threshold_seconds: Minimum seconds since last flush to trigger (default: 30)
            
        Returns:
            True if time threshold exceeded, False otherwise
        """
        elapsed = time_module.time() - self.last_flush_time
        return elapsed >= time_threshold_seconds

def get_document_issues_logfile(dir_path=''):
    if not os.path.isdir(dir_path):
        if os.path.isfile(dir_path):
            dir_path = os.path.dirname(dir_path)
        else:
            dir_path = os.path.abspath(os.path.curdir)
    count = 1
    log_stem = 'document_issues'
    while os.path.exists(os.path.join(dir_path, log_stem + str(count).zfill(4) + '.json')):
        count += 1
    logfile = str(os.path.join(dir_path, log_stem + str(count).zfill(4) + '.json'))
    return logfile

def log_exception_to_file(logfile_path, exception, context='', traceback_str=''):
    """
    Log an exception to a JSON log file.
    
    Args:
        logfile_path: Path to the log file
        exception: The exception object or exception message
        context: Additional context about where the exception occurred
        traceback_str: Full traceback string
    """
    import datetime
    log_entry = {
        'type': 'exception',
        'timestamp': datetime.datetime.now().isoformat(),
        'context': context,
        'exception': str(exception),
        'exception_type': type(exception).__name__ if hasattr(exception, '__class__') else 'Unknown',
        'traceback': traceback_str
    }
    with open(logfile_path, "a", encoding='utf-8') as logfile_handle:
        logfile_handle.write(json.dumps(log_entry, indent=4) + '\n')

def log_document_issue(logfile_path, issue_type, item_type_name=None, item_number=None, issue_description='', **kwargs):
    log_entry = {
        'issue_type': issue_type,
        'issue': issue_description,
        'timestamp': str(os.path.getmtime(logfile_path) if os.path.exists(logfile_path) else '')
    }
    if item_type_name:
        log_entry['item_type'] = item_type_name
    if item_number:
        log_entry['item_number'] = item_number
    log_entry.update(kwargs)
    with open(logfile_path, "a", encoding='utf-8') as logfile_handle:
        logfile_handle.write(json.dumps(log_entry, indent=4) + '\n')


def count_operational_items(parsed_content):
    """Count total operational items in the document."""
    count = 0
    for _, _, _, _, _ in iter_operational_items(parsed_content):
        count += 1
    return count


def discover_files_to_process(input_path, config, filter_str=None):
    """
    Discover files to process based on input path.

    Args:
        input_path: Path to directory, JSON file, or XML/HTML file
        config: Configuration dictionary
        filter_str: Optional filter string for manifest-based discovery

    Returns:
        List of (parse_file_path, output_file_path) tuples
    """
    files_to_process = []

    # If input is a JSON file directly
    if os.path.isfile(input_path) and input_path.endswith('.json'):
        if '_parse_output.json' in input_path:
            parse_file = input_path
            output_file = input_path.replace('_parse_output.json', '_processed.json')
        elif '_processed.json' in input_path:
            output_file = input_path
            parse_file = input_path.replace('_processed.json', '_parse_output.json')
        else:
            print(f'Warning: Unexpected JSON file format: {input_path}')
            return []

        files_to_process.append((parse_file, output_file))
        return files_to_process

    # If input is directory, try manifest-based discovery first
    if os.path.isdir(input_path):
        filter_criteria = parse_filter_string(filter_str) if filter_str else None
        discovered = discover_parse_files(input_path, filter_criteria)

        # If manifest files were found OR a filter was specified, use manifest-based results
        # (even if empty - an empty result with a filter means "no matches", not "fall back")
        if discovered or filter_str:
            for item in discovered:
                files_to_process.append((item['parse_file'], item['processed_file']))
            return files_to_process

        # Fall back to scanning for XML/HTML files (backward compatibility, only when no filter)
        dir_path = input_path
        for file_name in os.listdir(dir_path):
            if file_name.endswith('.xml') or file_name.endswith('.html'):
                file_stem = re.sub(r'\.\w+$', '', file_name)
                parse_file = os.path.join(dir_path, file_stem + '_parse_output.json')
                output_file = os.path.join(dir_path, file_stem + '_processed.json')

                if os.path.exists(parse_file) or os.path.exists(output_file):
                    files_to_process.append((parse_file, output_file))

        return files_to_process

    # If input is XML/HTML file (backward compatibility)
    if os.path.isfile(input_path) and (input_path.endswith('.xml') or input_path.endswith('.html')):
        dir_path = os.path.dirname(input_path)
        file_name = os.path.basename(input_path)
        file_stem = re.sub(r'\.\w+$', '', file_name)

        parse_file = os.path.join(dir_path, file_stem + '_parse_output.json')
        output_file = os.path.join(dir_path, file_stem + '_processed.json')

        files_to_process.append((parse_file, output_file))
        return files_to_process

    return []


def top_organization_summaries(proc, summary_number=1):
    # Because organization_summaries is a recursive function, it needs to be called with the specific values below for the top-level.
    try:
        parsed_content = proc.parsed_content
        org_content = parsed_content['document_information']['organization']['content']
        content_scope = parsed_content['document_information'].get('content_scope')

        if content_scope:
            from utils.document_handling import get_org_pointer_from_scope
            scope_type, scope_id, scope_node = get_org_pointer_from_scope(org_content, content_scope)
            if scope_node is not None:
                limited_content = {scope_type: {scope_id: scope_node}}
            else:
                limited_content = org_content  # fallback: scope not resolved
        else:
            limited_content = org_content

        organization_summaries(proc, limited_content, summary_number)
        # CRITICAL: Mark as dirty after organization_summaries modifies the content
        # This ensures flush() will actually write the changes to disk
        # write_if_updated only writes if dirty_flag == 1
        proc.dirty = 1
    except Exception as e:
        # Log error but don't fail - this allows processing to continue
        import traceback
        error_msg = f'top_organization_summaries error: {e}\n{traceback.format_exc()}'
        if hasattr(proc, 'logfile') and proc.logfile:
            try:
                with open(proc.logfile, 'a', encoding='utf-8') as f:
                    f.write(f'\nERROR in top_organization_summaries: {error_msg}\n')
            except:
                pass
        # Re-raise to let caller handle it
        raise
    proc.flush()

def organization_summaries(proc, limited_content, summary_number=1):
    """
    Generate summaries for organizational units based on their contained content.
    
    This function recursively processes the organizational hierarchy, creating summaries
    for each organizational unit (chapters, parts, etc.) based on the summaries of
    their contained operational items (sections, articles, etc.).
    
    Args:
        proc: SummaryProcessing object
        limited_content (dict): The organizational content to process (subset of full org structure)
        summary_number (int): The level of summary to generate (1 = level 1 summaries)

    The function works by:
    1. Identifying operational items directly contained in each organizational unit
    2. Recursively processing sub-units to get their summaries
    3. Combining summaries from both local items and sub-units
    4. Using AI to generate a cohesive summary of the organizational unit
    """
    client = proc.clients.get('organizational', proc.client) # AI client for organizational summaries
    parsed_content = proc.parsed_content # The document content being processed
    logfile = proc.logfile # Log file for tracking operations
    config = proc.config # Configuration for fallback model support

    # Add summaries to the organizational structure, if not already there.
    summary_text_label = ''
    if summary_number > 0:
        summary_text_label = 'summary_' + str(summary_number)
    else:
        InputError('Call to organization_summaries with bad summary_number: ' + str(summary_number))
        exit(1)
    if ('document_information' in parsed_content.keys() 
        and 'organization' in parsed_content['document_information'].keys()
        and 'parameters' in parsed_content['document_information'].keys()
        and 'content' in parsed_content['document_information']['organization'].keys()        
        and 'content' in parsed_content.keys()):
        param_pointer = parsed_content['document_information']['parameters']
        org_pointer = parsed_content['document_information']['organization']
        org_content_pointer = parsed_content['document_information']['organization']['content']
        content_pointer = parsed_content['content']

        for level_name in limited_content.keys():
            if (not re.search('unit_title', level_name) and 
                not re.search('unit_definitions', level_name) and 
                not re.search('begin_', level_name) and 
                not re.search('stop_', level_name) and
                not re.search('summary_', level_name)): # Anything other than these should be org unit types.
                # Check that level_name actually points to a dictionary (not a string or other type)
                if not isinstance(limited_content[level_name], dict):
                    continue
                
                for org_item_number in limited_content[level_name]:  # This loops org_item_number over each instance of the organizational type at the current location.
                    # Check that org_item_number actually points to a dictionary
                    if not isinstance(limited_content[level_name][org_item_number], dict):
                        continue
                    
                    working_item = limited_content[level_name][org_item_number]
                    if not summary_text_label in working_item.keys() or '' == working_item[summary_text_label]: # There is not a summary of the indicated level here, yet.                 

                        # First, find out if there are any operational provisions at this level.  Only operational provisions matter for summaries.
                        local_provisions = 0 # 0 means there are none.  1 means there are some, but their summaries are not complete.  2 means there are some, with complete summaries.
                        local_prov_nums = [] # A list of item_name / item_name_plural / item_number tuple for the local level.
                        if 'item_types' in org_pointer.keys():
                            for item_type in org_pointer['item_types']:  # This loops over the types of items that are relevant to summaries. *** Change parser to include appendices ***
                                if str(item_type) in param_pointer.keys():
                                    item_name = param_pointer[str(item_type)]['name']
                                    item_name_plural = param_pointer[str(item_type)]['name_plural']
                                    first_item = ''
                                    last_item = ''
                                    begin_tag = 'begin_' + item_name
                                    stop_tag = 'stop_' + item_name
                                    if begin_tag in working_item.keys():
                                        first_item = working_item[begin_tag]
                                    if stop_tag in working_item.keys():
                                        last_item = working_item[stop_tag]
                                    if not '' == first_item and not '' == last_item:  # This means that there may be some item types for summarization at this level.
                                        if not 1 == local_provisions: # If we have already determined that there are missing summaries, then we don't want to lose that information.
                                            local_provisions = 2 # We only shift to 1 if any are incomplete.
                                        include_flag = 0
                                        if item_name_plural in content_pointer.keys():
                                            for item_num in content_pointer[item_name_plural].keys(): # Not very efficient, but this way we don't need to guess how the item numbers progress.
                                                if item_num == first_item:
                                                    include_flag = 1
                                                if 1 == include_flag:
                                                    # first_item and last_item may overlap with organizational items deeper down, so we will need to check for that.  We only
                                                    # want items that are directly at the level pointed to by working_item.  If this is at the right level, then the key for the
                                                    # only dictionary item in the last item in the 'context' list should be the same as level_name.
                                                    if 'context' in content_pointer[item_name_plural][item_num] and level_name in content_pointer[item_name_plural][item_num]['context'][-1].keys():                                                    
                                                        if not summary_text_label in content_pointer[item_name_plural][item_num].keys():
                                                            local_provisions = 1 # Summaries not complete.
                                                        else:
                                                            local_prov_nums.append([item_name, item_name_plural, item_num])
                                                if item_num == last_item:
                                                    include_flag = 0
                        # Now, look for sub-units within this organizational unit.
                        sub_provisions = 0 # 0 means none.  1 means there are some, but summaries are not complete.  2 means there are some, with summaries.             
                        sub_prov_nums = [] # A list of org_item_name / org_item_names / org_item_nums at the next level down.
                        for sub_level_name in working_item.keys():
                            if (not re.search('unit_title', sub_level_name) and
                                not re.search('unit_definitions', sub_level_name) and
                                not re.search('begin_', sub_level_name) and 
                                not re.search('stop_', sub_level_name) and
                                not re.search('summary_', sub_level_name)): # Anything other than these should be org unit types.
                                # Check that sub_level_name actually points to a dictionary (not a string or other type)
                                if not isinstance(working_item[sub_level_name], dict):
                                    continue
                                
                                sub_level_name, sub_level_name_plural = canonical_org_types(sub_level_name)
                                if not 1 == sub_provisions: # Don't information if we already know there are sub-units without the summary.
                                    sub_provisions = 2 # Don't know yet whether summaries will be available, but we know that there are sub-units.
                                sub_limited_content = working_item                               
                                organization_summaries(proc, sub_limited_content, summary_number) # Call to fill out tree below, if possible.                                
                                for sub_level_num in working_item[sub_level_name].keys():
                                    # Check for summaries.
                                    if not summary_text_label in working_item[sub_level_name][sub_level_num].keys():
                                        # Check if this sub-unit actually has content to summarize.
                                        # Reserved/empty organizational units have no begin_/stop_ tags
                                        # and no child org units — only unit_title.  These are not
                                        # "incomplete"; they are intentionally empty and should be skipped.
                                        sub_unit = working_item[sub_level_name][sub_level_num]
                                        has_content = any(
                                            re.search('begin_', k) or re.search('stop_', k)
                                            for k in sub_unit.keys()
                                        ) or any(
                                            isinstance(sub_unit[k], dict)
                                            for k in sub_unit.keys()
                                            if not re.search('unit_title', k) and not re.search('unit_definitions', k)
                                               and not re.search('begin_', k) and not re.search('stop_', k)
                                               and not re.search('summary_', k)
                                        )
                                        if has_content:
                                            sub_provisions = 1 # Not complete.
                                        # else: empty/reserved unit, skip it
                                    else:
                                        sub_prov_nums.append([sub_level_name, sub_level_name_plural, sub_level_num])
                        # Pull it all together
                        if not 1 == local_provisions and not 1 == sub_provisions and (2 == local_provisions or 2 == sub_provisions): # We have what we need for this summary.
                            # Check if we can use a single sub-unit summary directly (avoid unnecessary AI re-summarization)
                            if 0 == local_provisions and 2 == sub_provisions and len(sub_prov_nums) == 1:
                                # There's exactly one sub-unit summary of the required type and no local items
                                # Use that summary directly without asking AI to re-summarize
                                sub_list = sub_prov_nums[0]
                                sub_level_name = sub_list[0]
                                sub_level_num = sub_list[2]
                                single_summary = working_item[sub_level_name][sub_level_num][summary_text_label]
                                if single_summary and single_summary.strip():
                                    working_item[summary_text_label] = single_summary
                            else:
                                # Multiple summaries or local items - need to combine them via AI
                                summaries = ''
                                if 2 == local_provisions:
                                    for local_list in local_prov_nums:
                                        item_name = local_list[0]
                                        item_name_plural = local_list[1]
                                        item_num = local_list[2]
                                        summaries += item_name + ' ' + item_num + ': ' + content_pointer[item_name_plural][item_num][summary_text_label] + '\n\n'
                                if 2 == sub_provisions:
                                    for sub_list in sub_prov_nums:
                                        sub_level_name = sub_list[0]
                                        sub_level_name_plural = sub_list[1]
                                        sub_level_num = sub_list[2]
                                        summaries += sub_level_name + ' ' + sub_level_num + ': ' + working_item[sub_level_name][sub_level_num][summary_text_label]+ '\n\n'
                                if not '' == summaries:
                                    prompt = ''
                                    if 1 == summary_number: # A more brief summary is called for at level 1.
                                        prompt += 'Please write a short summary of this set of one or more items. Describe them as a whole, without reference to their individual numbers. '
                                    else:
                                        prompt += 'Please write a summary of this set of one or more items. Describe them as a whole, without reference to their individual numbers. '
                                        prompt += 'While the summary should abstract away some detail, the intention is that he summary will give a reader a good understanding of what can '
                                        prompt += 'be found in the material. '
                                    prompt += 'Please provide your response without any preamble, just the actual summary.\n\n'
                                    prompt += 'Here is the content to summarize:\n\n' + summaries
                                    # Use query_text_with_retry() to enable fallback model support
                                    result = query_text_with_retry(client, [], prompt, logfile, max_tokens=0, max_retries=3, config=config, task_name='stage3.summary.organizational')
                                    if not '' == clean_summary_text(result):
                                        working_item[summary_text_label] = clean_summary_text(result)         
    else:
        InputError('organization_summaries: Document information is not correct.')
        exit(1)

def get_summary_JSON_format(item_name_list):
    """
    Generate a template JSON format for AI model responses when creating summaries.

    This function creates a standardized JSON template that the AI model should follow
    when generating summaries, including defined terms and references.

    Args:
        item_name_list (list): List of item type names for reference types

    Returns:
        str: JSON template string showing the expected response format
    """
    summary_JSON_format = """{
      "summary": "Establishes the regulation's purpose to promote 'human-centric and trustworthy AI' while protecting against harmful effects and sets out seven key areas of rules, including market requirements, prohibitions, high-risk system obligations, transparency rules, governance, and innovation support.",
      "references": ["""
    for item_type_name in item_name_list:
        cap_type_name = item_type_name[0].upper() + item_type_name[1:]
        summary_JSON_format += '\n        { "type": "' + cap_type_name + '", "value": "2" },'
    summary_JSON_format += """
        { "type": "External", "value": "XYZ paper" },
        { "type": "External", "value": "Article 5 of Regulation ABC" },
        { "type": "Need_Definition", "value": "Provider" },
        { "type": "Need_Definition", "value": "V<sub>A</sub>" }
      ]
}"""
    return summary_JSON_format


def get_reference_instructions(parsed_content) -> str:
    """
    Build reference formatting guidance from parameter reference_instruction fields.

    Args:
        parsed_content: Parsed document content

    Returns:
        str: Formatted guidance string (may be empty)
    """
    instructions = []
    try:
        params = parsed_content.get('document_information', {}).get('parameters', {})
        for param_key in sorted(params.keys(), key=lambda x: int(x)):
            entry = params.get(param_key, {})
            instruction = entry.get('reference_instruction', '')
            if instruction:
                name = entry.get('name', f'item {param_key}')
                instructions.append(f"- {name}: {instruction}")
    except Exception:
        return ''
    return "\n".join(instructions)

# Helper functions for level_2_summaries()

def collect_scoped_definitions(parsed_content, working_item) -> list:
    """
    Collect definitions that are in scope for the given operational item.
    
    Args:
        parsed_content: The document content
        working_item: The operational item dict (section/article/etc.)
        
    Returns:
        list: List of definition dictionaries (full entries preserved) with added metadata:
              - source_kind: 'document' | 'organization' | 'item'
              - org_depth: integer proximity along the org path (higher = closer), or -1 for document, 999 for item
    """
    definitions = []
    
    # Get the item's context list
    if 'context' not in working_item:
        raise InputError('collect_scoped_definitions: working_item missing context')
    
    org_context = working_item['context']
    
    # Start with document-wide definitions
    if 'document_information' in parsed_content and 'document_definitions' in parsed_content['document_information']:
        for def_entry in parsed_content['document_information']['document_definitions']:
            if 'term' in def_entry and 'value' in def_entry:
                entry = def_entry.copy()
                entry['source_kind'] = 'document'
                entry['org_depth'] = -1
                definitions.append(entry)
    
    # Walk up the organizational hierarchy
    if ('document_information' in parsed_content
        and 'organization' in parsed_content['document_information']
        and 'content' in parsed_content['document_information']['organization']):

        org_content_pointer = parsed_content['document_information']['organization']['content']
        org_name_set = get_organizational_item_name_set(parsed_content)

        # Walk through each level in the context path
        depth = 0
        for ctx_entry in org_context:
            for ctx_type, ctx_number in ctx_entry.items():
                if ctx_type in org_name_set:
                    # Organizational type: walk down the org tree
                    if ctx_type in org_content_pointer and ctx_number in org_content_pointer[ctx_type]:
                        org_content_pointer = org_content_pointer[ctx_type][ctx_number]

                        # Check for unit_definitions at this level
                        if 'unit_definitions' in org_content_pointer:
                            for def_entry in org_content_pointer['unit_definitions']:
                                if 'term' in def_entry and 'value' in def_entry:
                                    definition_entry = def_entry.copy()
                                    definition_entry['source_kind'] = 'organization'
                                    definition_entry['org_depth'] = depth
                                    definitions.append(definition_entry)
                        depth += 1
                else:
                    # Substantive parent type (e.g., "supplement"): look up parent container
                    # and include its defined_terms and ext_definitions
                    _, type_plural = canonical_org_types(ctx_type)
                    parent_item = lookup_item(parsed_content, type_plural, ctx_number)
                    if parent_item:
                        for field in ('defined_terms', 'ext_definitions'):
                            if field in parent_item:
                                for def_entry in parent_item[field]:
                                    if 'term' in def_entry and 'value' in def_entry:
                                        definition_entry = def_entry.copy()
                                        definition_entry['source_kind'] = 'organization'
                                        definition_entry['org_depth'] = depth
                                        definitions.append(definition_entry)
                        depth += 1
    
    # Check for item-specific definitions (ext_definitions - definitions from elsewhere)
    if 'ext_definitions' in working_item:
        for def_entry in working_item['ext_definitions']:
            if 'term' in def_entry and 'value' in def_entry:
                entry = def_entry.copy()
                entry['source_kind'] = 'item'
                entry['org_depth'] = 999
                definitions.append(entry)
    
    # Check for definitions defined in this substantive unit and scoped to it (defined_terms)
    if 'defined_terms' in working_item:
        for def_entry in working_item['defined_terms']:
            if 'term' in def_entry and 'value' in def_entry:
                entry = def_entry.copy()
                entry['source_kind'] = 'item'
                entry['org_depth'] = 999
                definitions.append(entry)
    
    return definitions

def collect_referenced_sections(parsed_content, need_ref_list, item_type_name, item_number) -> str:
    """
    Collect summary_1 text from referenced sections.
    
    Args:
        parsed_content: The document content
        need_ref_list: List of reference dicts from need_ref field
        item_type_name: Type name of the current item (e.g., 'section', 'article')
        item_number: Number/identifier of the current item
        
    Returns:
        str: Formatted string with referenced sections and their summary_1
    """
    referenced_sections = []
    
    if 'content' not in parsed_content:
        return ''
    
    content_pointer = parsed_content['content']
    
    # Extract all references that are NOT "Need_Definition" or "External"
    for ref in need_ref_list:
        if not isinstance(ref, dict) or 'type' not in ref or 'value' not in ref:
            continue
        
        ref_type = ref['type']
        ref_value = ref['value']
        
        if ref_type in ['Need_Definition', 'External']:
            continue
        
        # Note: Indirect definitions in definitions themselves are handled separately
        # We only process direct references from need_ref here
        
        # Get the plural form of the type
        try:
            type_singular, type_plural = canonical_org_types(ref_type.lower())
        except:
            # If canonical_org_types fails, try to construct the plural
            type_plural = ref_type.lower() + 's'
        
        # Skip if this is a reference to the current item itself
        if ref_type.lower() == item_type_name.lower() and ref_value == item_number:
            continue
        
        # Look up the referenced item (supports both top-level and sub-unit items)
        ref_item = lookup_item(parsed_content, type_plural, ref_value)
        if ref_item is not None:
            # Check if summary_1 exists
            if 'summary_1' in ref_item and ref_item['summary_1']:
                cap_type = ref_type[0].upper() + ref_type[1:] if len(ref_type) > 0 else ref_type
                referenced_sections.append({
                    'type': cap_type,
                    'number': ref_value,
                    'summary': ref_item['summary_1']
                })
    
    # Format output
    if not referenced_sections:
        return ''
    
    # Determine the section type label (SECTIONS, ARTICLES, etc.)
    # Use the type from the first reference, or default to item_type_name
    if referenced_sections:
        first_type = referenced_sections[0]['type']
        section_label = first_type.upper() + 'S' if not first_type.endswith('s') else first_type.upper()
    else:
        section_label = item_type_name.upper() + 'S'
    
    result = section_label + ':\n\n'
    
    for ref in referenced_sections:
        result += ref['type'] + ' ' + ref['number'] + ':\n' + ref['summary'] + '\n\n'
    
    return result

def format_definition_with_source(def_entry: dict, current_item_type: str, current_item_number: str) -> str:
    """
    Format a definition value with source information if available and from a different source.

    If the definition has source_type and source_number fields, and these indicate the definition
    is from a different substantive unit than the one currently being summarized, prepend source
    information to help the AI understand that internal references within the definition refer to
    the source location.

    This applies to all definition types (document_definitions, unit_definitions, ext_definitions,
    defined_terms) as they all may contain internal references to sub-units that need clarification.

    Args:
        def_entry: Definition dictionary with 'value' and optionally 'source_type', 'source_number'
        current_item_type: Type name of the item being summarized (e.g., 'section', 'article')
        current_item_number: Number/identifier of the item being summarized

    Returns:
        str: Formatted definition string with source information if from different source
    """
    value = def_entry.get('value', '').strip()

    if not value:
        return value

    # Check if this definition has source information
    if 'source_type' in def_entry and 'source_number' in def_entry:
        source_type = def_entry['source_type']
        source_number = def_entry['source_number']

        # Only add source formatting if the definition is from a DIFFERENT substantive unit
        # Compare case-insensitively since source_type might be stored differently
        if source_type.lower() != current_item_type.lower() or source_number != current_item_number:
            # Capitalize the source type for display
            cap_source_type = source_type[0].upper() + source_type[1:] if len(source_type) > 0 else source_type

            # Prepend source information
            return f"[This definition is from {cap_source_type} {source_number}] {value}"

    return value

def fuzzy_match_term(needed_term: str, available_term_names: list) -> str:
    """
    Find the best matching term name using fuzzy matching.

    Tries exact match first (fast path), then fuzzy matching with threshold > 87,
    then singular/plural variations.

    Args:
        needed_term: The term to match (e.g., "AI systems")
        available_term_names: List of available term names (e.g., ["ai system", "provider", ...])

    Returns:
        str: The matched term name (lowercase), or None if no good match found
    """
    needed_lower = strip_emphasis_marks(needed_term).lower()

    # Try exact match first (case-insensitive)
    if needed_lower in available_term_names:
        return needed_lower

    # Try fuzzy matching
    if len(available_term_names) > 0:
        best_match = process.extractOne(needed_term, available_term_names)
        if best_match and best_match[1] > 87:  # Threshold > 87
            return best_match[0].lower()

    # Try singular/plural variation (remove trailing 's')
    if len(needed_term) > 1 and needed_term[-1].lower() == 's':
        singular = needed_term[:-1]
        singular_lower = singular.lower()

        # Try exact match on singular
        if singular_lower in available_term_names:
            return singular_lower

        # Try fuzzy matching on singular
        if len(available_term_names) > 0:
            best_match = process.extractOne(singular, available_term_names)
            if best_match and best_match[1] > 87:
                return best_match[0].lower()

    return None

def build_level_2_context(parsed_content, working_item, item_type_name, item_number) -> tuple[str, list]:
    """
    Build context string with definitions and referenced sections.

    Uses fuzzy matching (threshold > 87) to resolve terms from need_ref that need definitions,
    falling back to fuzzy matching when exact matches fail. For each term, selects:
    - Direct definitions: All definitions at the highest specificity level (item > org > document)
    - Elaborative definitions: All unique elaborations from the organizational hierarchy

    Args:
        parsed_content: The document content
        working_item: The operational item being summarized
        item_type_name: Type name of the item
        item_number: Number/identifier of the item

    Returns:
        tuple[str, list]: (Formatted context string, list of conflict records detected for direct definitions)
    """
    # Collect scoped definitions (full entries preserved)
    definitions = collect_scoped_definitions(parsed_content, working_item)

    # Terms needed (from need_ref)
    need_ref = working_item.get('need_ref', [])
    needed_terms = []
    for ref in need_ref:
        if isinstance(ref, dict) and ref.get('type') == 'Need_Definition' and 'value' in ref:
            term_value = strip_emphasis_marks(ref['value'].strip())
            if term_value:
                needed_terms.append(term_value)

    # Group definitions by term (case-insensitive)
    term_groups = {}
    for d in definitions:
        if 'term' not in d or 'value' not in d:
            continue
        key = strip_emphasis_marks(d['term']).lower()
        term_groups.setdefault(key, []).append(d)

    # Helper to rank source specificity
    def specificity_key(d):
        # Higher is better
        kind = d.get('source_kind', 'organization')
        depth = d.get('org_depth', 0)
        if kind == 'item':
            return (3, depth)
        if kind == 'organization':
            return (2, depth)
        if kind == 'document':
            return (1, depth)
        return (0, depth)

    conflicts = []

    # Build selected definitions for needed terms
    selected_per_term = {}  # term_lower -> {'directs': [def_entry,...], 'elaborations': [def_entry,...]}
    available_term_names = list(term_groups.keys())  # Pre-build list for fuzzy matching

    for needed in needed_terms:
        key = needed.lower()

        # Try exact match first (fast path)
        group = term_groups.get(key, [])

        # If no exact match, try fuzzy matching
        if not group:
            matched_term = fuzzy_match_term(needed, available_term_names)
            if matched_term:
                group = term_groups.get(matched_term, [])

        if not group:
            continue  # No match found after exact and fuzzy matching

        directs = [g for g in group if g.get('def_kind', 'direct').lower() == 'direct']
        elaborations = [g for g in group if g.get('def_kind', 'direct').lower() == 'elaboration']

        chosen_directs = []
        if directs:
            # Sort by specificity (highest first)
            directs_sorted = sorted(directs, key=specificity_key, reverse=True)

            # Get all definitions at the highest specificity level
            highest_specificity = specificity_key(directs_sorted[0])
            chosen_directs = [d for d in directs_sorted if specificity_key(d) == highest_specificity]

            # Log conflicts when multiple direct definitions exist at same specificity level
            if len(chosen_directs) > 1:
                unique_values = list({g.get('value', '').strip() for g in chosen_directs if g.get('value', '').strip() != ''})
                conflicts.append({
                    'term': needed,
                    'direct_count': len(chosen_directs),
                    'same_specificity': True,
                    'specificity_level': highest_specificity,
                    'unique_values': unique_values[:5]  # cap for log brevity
                })

        # Deduplicate elaborations by value text
        seen_elab_values = set()
        dedup_elabs = []
        for e in elaborations:
            val = e.get('value', '').strip()
            if val and val.lower() not in seen_elab_values:
                seen_elab_values.add(val.lower())
                dedup_elabs.append(e)

        selected_per_term[key] = {'directs': chosen_directs, 'elaborations': dedup_elabs}
    
    # Collect referenced sections from need_ref
    referenced_sections = collect_referenced_sections(parsed_content, need_ref, item_type_name, item_number)
    
    # Also collect referenced sections from indirect definitions in need_ref
    # (definitions that have indirect_loc_type/indirect_loc_number pointing to other sections)
    indirect_refs = []
    # If any of a needed term's chosen directs have indirect references, include them
    for sel in selected_per_term.values():
        directs = sel.get('directs', [])
        for def_entry in directs:
            # Check if this definition has indirect location info and was referenced in need_ref
            if 'indirect_loc_type' in def_entry and 'indirect_loc_number' in def_entry:
                indirect_refs.append({
                    'type': def_entry['indirect_loc_type'],
                    'number': def_entry['indirect_loc_number']
                })
    
    # Add indirect definition references to the referenced sections
    # Track what we've already added to avoid duplicates
    added_sections = set()
    if referenced_sections:
        # Parse existing referenced sections to extract section identifiers
        for line in referenced_sections.split('\n'):
            # Look for patterns like "Section 123:" or "Article 5:"
            match = re.match(r'^(\w+)\s+(\S+):', line)
            if match:
                added_sections.add((match.group(1).lower(), match.group(2)))
    
    if indirect_refs and 'content' in parsed_content:
        content_pointer = parsed_content['content']
        new_sections = []
        
        for ref in indirect_refs:
            ref_type = ref['type']
            ref_number = ref['number']
            
            # Skip if this is a reference to the current item itself
            if ref_type.lower() == item_type_name.lower() and ref_number == item_number:
                continue
            
            # Skip if already added
            if (ref_type.lower(), ref_number) in added_sections:
                continue
            
            # Get the plural form
            try:
                type_singular, type_plural = canonical_org_types(ref_type.lower())
            except:
                type_plural = ref_type.lower() + 's'
            
            # Look up the referenced item (supports both top-level and sub-unit items)
            ref_item = lookup_item(parsed_content, type_plural, ref_number)
            if ref_item is not None:
                # Check if summary_1 exists
                if 'summary_1' in ref_item and ref_item['summary_1']:
                    cap_type = ref_type[0].upper() + ref_type[1:] if len(ref_type) > 0 else ref_type
                    new_sections.append({
                        'type': cap_type,
                        'number': ref_number,
                        'summary': ref_item['summary_1']
                    })
                    added_sections.add((ref_type.lower(), ref_number))
        
        # Add new sections to referenced_sections
        if new_sections:
            # Determine section label (use first section's type)
            first_type = new_sections[0]['type']
            section_label = first_type.upper() + 'S:\n\n' if not first_type.endswith('s') else first_type.upper() + ':\n\n'
            
            new_sections_text = section_label
            for ref in new_sections:
                new_sections_text += ref['type'] + ' ' + ref['number'] + ':\n' + ref['summary'] + '\n\n'
            
            if referenced_sections:
                referenced_sections += new_sections_text
            else:
                referenced_sections = new_sections_text
    
    # Format output
    context_parts = []

    if selected_per_term:
        context_parts.append('Here are definitions that may be useful context for this request:\n\n')
        # Preserve order of needed_terms
        for needed in needed_terms:
            key = needed.lower()
            sel = selected_per_term.get(key)
            if not sel:
                continue
            directs = sel.get('directs', [])
            elaborations = sel.get('elaborations', [])

            # Header line with direct definition(s)
            if len(directs) == 1 and directs[0].get('value', '').strip():
                # Single direct definition - render on main line with source info if from different source
                formatted_value = format_definition_with_source(directs[0], item_type_name, item_number)
                context_parts.append('"' + needed + '":  ' + formatted_value + '\n')
            elif len(directs) > 1:
                # Multiple direct definitions at same specificity - render as bullets with source info if from different source
                context_parts.append('"' + needed + '":\n')
                for d in directs:
                    formatted_value = format_definition_with_source(d, item_type_name, item_number)
                    if formatted_value:
                        context_parts.append('  - ' + formatted_value + '\n')
            else:
                # No direct definitions (only elaborations)
                context_parts.append('"' + needed + '":\n')

            # Indented bullets for elaborations with source info if from different source
            for e in elaborations:
                formatted_value = format_definition_with_source(e, item_type_name, item_number)
                if formatted_value:
                    context_parts.append('  - ' + formatted_value + '\n')
            context_parts.append('\n')

    if referenced_sections:
        context_parts.append('Here are summaries of substantive units that may be useful context for this request:\n\n')
        context_parts.append(referenced_sections)
    
    return ''.join(context_parts), conflicts

def create_level_2_base_prompt(proc, item_type_name, item_number, total_chunks) -> str:
    """
    Create base prompt for level 2 summary generation.
    
    Args:
        proc: SummaryProcessor instance
        item_type_name: Type name of the item
        item_number: Number/identifier of the item
        total_chunks: Total number of chunks (for paragraph count calculation)
        
    Returns:
        str: Base prompt string
    """
    # Calculate max paragraphs: 5 + max(0, total_chunks - 1)
    max_paragraphs = 5 + max(0, total_chunks - 1)
    
    # Get document title if available
    doc_title = ''
    if ('document_information' in proc.parsed_content 
        and 'title' in proc.parsed_content['document_information']):
        doc_title = proc.parsed_content['document_information']['title']
    
    prompt = ''
    if doc_title:
        prompt += 'I will be asking you about this regulation: ' + doc_title + '\n\n'
    
    prompt += 'Here is my request.  Please provide a summary of the following portion of the regulation.  '
    prompt += 'The summary should allow a legal practitioner to understand the rights, restrictions, and obligations '
    prompt += 'set forth in this portion.  Please do not exceed ' + str(max_paragraphs) + ' paragraphs for the summary '
    prompt += '(shorter is fine).  '
    prompt += 'The summary should not re-iterate which ' + item_type_name + ' is being summarized, '
    prompt += 'that will be attached to the summary separately.  '
    prompt += '**IMPORTANT: The summary must not be longer than the text being summarized.**  '
    prompt += 'What you return will be incorporated into a compilation of summaries of the regulation, '
    prompt += 'so it is important that you return nothing beyond the summary itself (no preamble, no commentary about how the '
    prompt += 'task was completed).\n\n'
    prompt += 'Context is provided above that includes definitions and referenced sections.  '
    prompt += 'Use this context to help you understand the text, but do NOT summarize any aspect of the context itself.  '
    prompt += 'Only summarize the text that is provided to be summarized.\n\n'
    
    return prompt

def prepare_context_for_caching(context_string, table_of_contents='') -> list:
    """
    Prepare context components for optimal caching.
    
    Args:
        context_string: The context string (definitions + referenced sections)
        table_of_contents: Table of contents string if available
        
    Returns:
        list: List of strings optimized for caching (each >4500 chars if possible)
    """
    cache_list = []
    
    # Combine table_of_contents and context_string if both exist
    if table_of_contents and context_string:
        combined = table_of_contents + '\n\n' + context_string
        if len(combined) > 4500:
            cache_list.append(combined)
        else:
            # If combined is too short, keep separate (query_json will handle combining)
            cache_list.append(table_of_contents)
            cache_list.append(context_string)
    elif table_of_contents:
        cache_list.append(table_of_contents)
    elif context_string:
        cache_list.append(context_string)
    
    return cache_list


def _get_parent_key(parsed_content, working_item):
    """
    Detect whether the current item is a sub-unit by examining its last context entry.

    Returns (parent_type, parent_number) if the item is a sub-unit (last context entry
    is a substantive parent type, not an organizational type). Returns None if the item
    is a regular item or if context is empty.

    For v0.3 documents, all context entries are organizational types, so this always
    returns None.
    """
    context = working_item.get('context', [])
    if not context:
        return None

    last_entry = context[-1]
    # Each context entry is a single-key dict like {"part": "774"} or {"supplement": "No. 1"}
    key = list(last_entry.keys())[0]

    org_name_set = get_organizational_item_name_set(parsed_content)
    if key not in org_name_set:
        # This is a substantive parent type (e.g., "supplement"), not an org type
        value = last_entry[key]
        return (key, value)

    return None


def container_summaries(proc, summary_number):
    """
    Aggregate sub-unit summaries into each parent container's summary field.

    After sub-unit summaries are complete, this function iterates over parent
    containers (items that have sub_units) and generates an aggregated summary
    from all sub-unit summaries.  The summary is stored on the parent item's
    summary_1 or summary_2 field so that organization_summaries() can pick it
    up normally.

    For v0.3 documents (no containers), iter_containers yields nothing, so this
    is a no-op with no version check needed.

    Args:
        proc: SummaryProcessor object
        summary_number: 1 or 2, indicating which summary level to aggregate
    """
    client = proc.clients.get('organizational', proc.client)
    parsed_content = proc.parsed_content
    logfile = proc.logfile
    config = proc.config
    summary_key = f'summary_{summary_number}'

    for item_type_name, item_type_name_plural, cap_item_type_name, item_number, working_item in iter_containers(parsed_content):
        # Skip if parent already has this summary (idempotent for interrupted re-runs)
        if summary_key in working_item and working_item[summary_key]:
            continue

        # Collect sub-unit summaries
        # v0.5: sub_units is type-keyed {param_key: {sub_num: sub_item, ...}, ...}
        sub_summaries = []
        all_present = True
        for sub_type_key, sub_type_items in working_item['sub_units'].items():
            for sub_key, sub_item in sub_type_items.items():
                if summary_key in sub_item and sub_item[summary_key]:
                    sub_summaries.append(f'{sub_key}: {sub_item[summary_key]}')
                else:
                    all_present = False
                    break
            if not all_present:
                break

        # Skip if any sub-unit is missing its summary (don't aggregate incomplete data)
        if not all_present or not sub_summaries:
            continue

        container_text = working_item.get('text', '').strip()

        if container_text:
            # Container has its own substantive text in addition to sub-units.
            # Generate a combined summary that covers both the container's own content
            # and the scope of its subsidiary units.
            prompt = (
                f'The following is the text content of {cap_item_type_name} {item_number}, '
                f'followed by summaries of its subsidiary units. '
                f'Please provide a comprehensive summary that covers the {item_type_name}\'s '
                f'own content and the scope of its subsidiary entries.\n\n'
                f'--- {cap_item_type_name} {item_number} content ---\n'
                f'{container_text}\n\n'
                f'--- Summaries of subsidiary units ---\n'
            )
        else:
            # Container has no own text: aggregate sub-unit summaries only.
            prompt = (
                f'Please provide a concise summary of {cap_item_type_name} {item_number} '
                f'based on the following summaries of its sub-units. '
                f'The summary should capture the overall scope and key themes without '
                f're-iterating which {item_type_name} is being summarized.\n\n'
            )
        for s in sub_summaries:
            prompt += s + '\n\n'
        prompt += 'Return only the summary text, no preamble.'

        try:
            result = query_text_with_retry(
                client, [], prompt, logfile, max_tokens=0, max_retries=3,
                config=config, task_name=f'stage3.summary.container_{summary_number}'
            )
            summary_text = clean_summary_text(result)
            if summary_text:
                working_item[summary_key] = summary_text
                proc.dirty = 1
                print(f'  [OK] Container {cap_item_type_name} {item_number} {summary_key} ({len(summary_text)} chars)')
            else:
                print(f'  [WARNING] Empty container summary for {cap_item_type_name} {item_number}')
        except Exception as e:
            print(f'  [ERROR] Container summary failed for {cap_item_type_name} {item_number}: {e}')
            import traceback
            traceback.print_exc()

    proc.flush()


def _generate_table_structure_summary(working_item: dict, proc) -> None:
    """
    Generate a structural summary for a `table` sub-unit and store it in ``summary_1``.

    Uses the first 12,000 characters of the table HTML (enough for THEAD + first ~30 rows)
    to ask the AI to describe the table's purpose, columns, and row organization.

    Falls back to a generic description if:
    - The AI returns "CANNOT_DETERMINE"
    - The AI call raises an exception

    The result is stored in ``working_item['summary_1']``.
    """
    table_html_sample = working_item.get("table_html", "")[:12000]
    row_count = working_item.get("table_row_count", 0)
    column_headers = working_item.get("table_column_headers", [])
    caption = working_item.get("table_caption", "")

    def _fallback_summary() -> str:
        parts = [f"Large table ({row_count:,} rows)."]
        if caption:
            parts.append(f"Caption: {caption}.")
        if column_headers:
            parts.append(f"Columns: {', '.join(column_headers)}.")
        else:
            parts.append("Column headers not available.")
        return " ".join(parts)

    if not table_html_sample:
        working_item["summary_1"] = _fallback_summary()
        return

    context_lines = []
    if caption:
        context_lines.append(f"Caption: {caption}")
    if column_headers:
        context_lines.append(f"Columns: {', '.join(column_headers)}")
    context_lines.append(f"Total rows (approximate): {row_count:,}")

    context_block = "\n".join(context_lines)
    prompt = (
        f"The following is the beginning of a large HTML table from a legal/regulatory document.\n"
        f"{context_block}\n\n"
        f"Describe in 2–4 sentences: (1) what this table appears to contain and its likely purpose, "
        f"(2) what each column represents, and (3) how the rows are organized. "
        f"If you cannot determine the structure from this sample, respond with exactly: CANNOT_DETERMINE\n\n"
        f"Table HTML (first {len(table_html_sample):,} characters):\n{table_html_sample}"
    )

    try:
        result = query_text_with_retry(
            proc.client, [], prompt, proc.logfile,
            max_tokens=400, max_retries=3,
            config=proc.config, task_name="stage3.summary.table",
        )
        result = result.strip() if result else ""
        if not result or result == "CANNOT_DETERMINE":
            working_item["summary_1"] = _fallback_summary()
        else:
            working_item["summary_1"] = result
    except Exception as e:
        print(f"    WARNING: Table summary AI call failed ({e}); using fallback.")
        working_item["summary_1"] = _fallback_summary()


def level_1_summaries(proc, count=30, checkpoint_threshold=30) -> bool:
    """
    Generate level 1 summaries for operational items in the document.
    
    This function creates initial summaries for each operational item (sections, articles, etc.)
    using AI model analysis. These summaries provide a foundation for higher-level
    organizational summaries and question-answering capabilities.
    
    Args:
        proc: SummaryProcessor object
        count (int): Maximum number of items to process in this run
        
    The function:
    1. Iterates through operational items that need summaries
    2. Uses AI model to generate concise summaries
    3. Extracts defined terms and references for each item
    4. Maintains cumulative context for better summary quality
    5. Triggers organizational summary generation at boundaries
    """

    client = proc.clients.get('level1_with_references', proc.client) # AI client for level 1 summaries with references
    parsed_content = proc.parsed_content # The document content being processed
    out_path = proc.out_path # Output file path for saving updates
    logfile = proc.logfile # Log file for tracking operations
    config = proc.config # Configuration for fallback model support

    # Create (if needed) the initial summaries (summary_1)
    update_json_content = 0
    if ('document_information' not in parsed_content.keys() or 
        'parameters' not in parsed_content['document_information'].keys() or
        'content' not in parsed_content.keys()):
        raise InputError('level_1_summaries: invalid parsed_content structure.')
        exit(1)

    param_pointer = parsed_content['document_information']['parameters']
    content_pointer = parsed_content['content']
    first_flag = True # True if addition to the cache_summary_list will be the first substantive addition.
    cumulative_summary_list = []
    current_parent_key = None  # Track current parent for sub-unit boundary detection
    cumulative_summary_list_pre = '\nFor the context of this request, consider these summarizations of earlier portions of the document:\n' # This is a running summary of what has gone before, and is reset at each organizational level.
    table_of_contents_cache = ''
    if not '' == proc.table_of_contents: # Always start with the table of contents, if available.
        table_of_contents_cache = 'Here is a table of contents for a document that is relevant to this request:\n\n' + proc.table_of_contents + '\n'

    if not '' == table_of_contents_cache:
        cumulative_summary_list.append(table_of_contents_cache) # Add table of contents if there is one.
    # Count total items and items needing processing for progress reporting.
    # Containers (has_sub_units) count toward total but are handled by container_summaries()
    # after this loop, not here.
    total_items = 0
    items_needing_summary = 0
    items_processed = 0
    for _, _, _, _, item in iter_operational_items(parsed_content):
        total_items += 1
        if has_sub_units(item):
            continue
        if 'summary_1' not in item.keys() and item.get('text', ''):
            items_needing_summary += 1

    print(f'  Found {total_items} total operational items, {items_needing_summary} need summary_1')

    # Build set of data-table type names (data_table: 1 flag in parameters).
    _params = parsed_content['document_information']['parameters']
    data_table_type_names = {p['name'] for p in _params.values()
                             if p.get('data_table') and p.get('is_sub_unit')}

    for item_type_name, item_type_name_plural, cap_item_type_name, item_number, working_item in iter_operational_items(parsed_content):
        if has_sub_units(working_item):  # handled by container_summaries() after this loop
            continue
        if item_type_name in data_table_type_names:  # data table sub-units get a structural AI summary, not prose summarization.
            if 'summary_1' not in working_item:
                print(f'  Generating table summary for {item_number}')
                _generate_table_structure_summary(working_item, proc)
                proc.dirty = 1
            continue
        if '' == working_item['text']:
            working_item['summary_1'] = cap_item_type_name + ' is blank.'
            working_item['defined_terms'] = []
            working_item['need_ref'] = []
        else:
            if 'summary_1' in working_item.keys(): # Existing summary.
                print(f'  [{items_processed+1}/{items_needing_summary}] Existing: {item_number}')
                if first_flag:
                    first_flag = False
                    cumulative_summary_list.append(cumulative_summary_list_pre)

                # Defensive: Handle case where summary_1 is incorrectly stored as a list
                # (can happen if AI returned list instead of string in previous run)
                summary_text = working_item['summary_1']
                if isinstance(summary_text, list):
                    print(f'    WARNING: summary_1 is a list ({len(summary_text)} items), converting to string')
                    summary_text = ' '.join(summary_text)
                    # Fix the stored value so future runs don't need this
                    working_item['summary_1'] = summary_text
                    proc.dirty = 1

                cumulative_summary_list.append('\n' + cap_item_type_name + ' ' + item_number + ':\n' + summary_text + '\n')
            else:
                items_processed += 1
                print(f'\n  [{items_processed}/{items_needing_summary}] Processing: {cap_item_type_name} {item_number}')
                
                # Build base prompt
                base_prompt = 'Please provide two things in your response: (i) a short (no more than three sentences) summary of this ' + item_type_name + ' from a longer document, and '
                base_prompt += '(ii) a list of references that are explicitly cited or mentioned in the text that would be needed to make a better summary.  '
                base_prompt += 'The summary should not re-iterate which ' + item_type_name + ' is being summarized, '
                base_prompt += 'that will be attached to the summary separately.\n\n'
                base_prompt += 'Please return these two things in JSON format, following the form of this example, with no preamble and no '
                base_prompt += 'response other than in this JSON format:\n'
                base_prompt += get_summary_JSON_format(proc.type_name_list) + '\n\n'
                base_prompt += 'Only include references that are explicitly cited or mentioned in the text being summarized. Use these types:\n'
                for list_item_type_name in proc.type_name_list:
                    cap_type_name = list_item_type_name[0].upper() + list_item_type_name[1:]
                    base_prompt += ' "' + cap_type_name + '" - for ' + list_item_type_name + 's mentioned in the text (use the identifier that appears in the table of contents),\n'
                base_prompt += ' "External" - for references to other documents,\n'
                base_prompt += ' "Need_Definition" - for terms needing definitions (return the term EXACTLY as it appears in the text, preserving any HTML/XML font markup like <sub>, <sup>, <i>, <b>, etc.).\n\n'
                reference_guidance = get_reference_instructions(parsed_content)
                if reference_guidance:
                    base_prompt += 'Reference formatting guidance:\n' + reference_guidance + '\n\n'
                base_prompt += 'If the text cites a sub-unit that doesn\'t appear in the table of contents (e.g., "' + item_type_name + ' 12(b)"), report the closest parent that does appear (e.g., "12").\n\n'
                
                # UNIFIED CHUNKING PATH (handles both chunked and non-chunked)
                text = working_item['text']
                breakpoints = working_item.get('breakpoints', [])

                # Build metadata suffix for sections in duplicate sets
                metadata_suffix = build_metadata_suffix(item_number, working_item, content_pointer, item_type_name_plural)

                # chunk_text will yield single chunk if text is short or no breakpoints
                chunks = list(chunk_text(text, breakpoints, preferred_length=15000))
                total_chunks = len(chunks)

                chunk_summaries = []
                all_references = []

                # Process each chunk (or single chunk for non-chunked case)
                for i, chunk in enumerate(chunks):
                    # Augment this chunk with metadata (for _dup sections)
                    augmented_chunk = augment_chunk_with_metadata(chunk, metadata_suffix)

                    # Helper handles single vs multi-chunk prompt differences
                    chunk_prompt = create_chunk_summary_prompt(
                        base_prompt,
                        augmented_chunk,  # Use augmented chunk instead of plain chunk
                        i + 1,
                        total_chunks,
                        item_type_name,
                        item_number,
                        chunk_summaries,  # Empty on first iteration
                        unit_title=working_item.get('unit_title', '')
                    )
                    
                    print(f'    Chunk {i+1}/{total_chunks} of {cap_item_type_name} {item_number}...')
                    
                    # Use SAME cumulative_summary_list for all chunks
                    # (preserves caching - doesn't change during this item)
                    try:
                        # Level 1 summaries must have 'summary' key
                        # 'references' may be empty but 'summary' is always required
                        result = query_json(client, cumulative_summary_list, chunk_prompt, logfile, expected_keys=['summary'], config=config, task_name='stage3.summary.level1_with_references')
                    except Exception as e:
                        print(f'    ERROR: Failed to query AI for chunk {i+1} of {cap_item_type_name} {item_number}: {e}')
                        import traceback
                        traceback.print_exc()
                        raise ModelError(f'Failed to get response for chunk {i+1} of {cap_item_type_name} {item_number}: {e}')
                    
                    print(f'    Chunk {i+1} complete')
                    
                    if not result:
                        raise ModelError('Failed to get response.\n')
                    
                    if 'summary' in result.keys():
                        # Defensive: Ensure summary is a string, not a list
                        # (can happen if AI returns malformed JSON)
                        summary_text = result['summary']
                        if isinstance(summary_text, list):
                            print(f'    WARNING: Chunk {i+1} summary is a list ({len(summary_text)} items), converting to string')
                            summary_text = ' '.join(str(item) for item in summary_text)
                        elif not isinstance(summary_text, str):
                            print(f'    WARNING: Chunk {i+1} summary is not a string (type: {type(summary_text)}), converting to string')
                            summary_text = str(summary_text)
                        chunk_summaries.append(summary_text)
                    if 'references' in result.keys():
                        all_references.extend(result['references'])
                
                # Synthesize final summary (no-op if only one chunk)
                try:
                    final_summary = synthesize_final_summary(
                        chunk_summaries,
                        item_type_name,
                        item_number,
                        client,
                        logfile,
                        config
                    )
                    # Defensive: Ensure final_summary is a string, not a list
                    # (can happen if synthesize_final_summary returns a list or if chunk_summaries contained lists)
                    if isinstance(final_summary, list):
                        print(f'    WARNING: Final summary is a list ({len(final_summary)} items), converting to string')
                        final_summary = ' '.join(str(item) for item in final_summary)
                    elif not isinstance(final_summary, str):
                        print(f'    WARNING: Final summary is not a string (type: {type(final_summary)}), converting to string')
                        final_summary = str(final_summary)
                    
                    if not final_summary:
                        print(f'    WARNING: Empty summary for {cap_item_type_name} {item_number}')
                        final_summary = f"{cap_item_type_name} {item_number} summary unavailable"
                    working_item['summary_1'] = final_summary
                    print(f'    [OK] {cap_item_type_name} {item_number} summary_1 complete ({len(final_summary)} chars)')
                except Exception as e:
                    print(f'    ERROR: Failed to synthesize summary for {cap_item_type_name} {item_number}: {e}')
                    import traceback
                    traceback.print_exc()
                    # Use a fallback summary to prevent complete failure
                    working_item['summary_1'] = f"{cap_item_type_name} {item_number} summary generation failed: {str(e)}"
                    proc.dirty = 1
                
                # CRITICAL: Ensure summary_1 is saved immediately to prevent data loss
                # Wrap subsequent operations in try/except to ensure flush happens even if code fails
                try:
                    # Deduplicate references
                    working_item['need_ref'] = deduplicate_references(all_references)
                    
                    # Update progress after each item for UI visibility
                    operational_counts, organizational_counts = count_stage_3_progress(parsed_content)
                    update_stage_3_progress(parsed_content, operational_counts, organizational_counts)
                    proc.dirty = 1
                    
                    # CRITICAL: Flush immediately if it's been more than 10 seconds since last flush
                    # This prevents data loss if an exception occurs before the regular flush
                    if proc.should_flush_by_time(time_threshold_seconds=10):
                        proc.flush()
                        print(f'  [Immediate flush] Saved {cap_item_type_name} {item_number} summary_1 to prevent data loss')
                    
                    # Periodic checkpoint: save progress every checkpoint_threshold items
                    if items_processed % checkpoint_threshold == 0:
                        proc.flush()
                        print(f'  [Checkpoint] Progress saved: {operational_counts["summary_1"]}/{operational_counts["total"]} operational items with summary_1')
                    else:
                        # Flush more frequently for UI updates (every 5 items OR if 30+ seconds have passed)
                        should_flush = False
                        if items_processed % 5 == 0:
                            should_flush = True
                        elif proc.should_flush_by_time(time_threshold_seconds=30):
                            should_flush = True
                            print(f'  [Time-based flush] Flushing after {int(time_module.time() - proc.last_flush_time)} seconds')
                        
                        if should_flush:
                            proc.flush()
                except Exception as e:
                    # If anything fails after setting summary_1, ensure we flush before re-raising
                    # This prevents data loss if an exception occurs in progress updates or other operations
                    print(f'  [ERROR] Exception after setting summary_1 for {cap_item_type_name} {item_number}: {e}')
                    print(f'  [SAFETY] Flushing to save {cap_item_type_name} {item_number} summary_1 before re-raising exception...')
                    try:
                        proc.flush()
                    except Exception as flush_error:
                        print(f'  [CRITICAL] Failed to flush after exception: {flush_error}')
                    raise  # Re-raise the original exception
                
                # NOW add to cumulative_summary_list for next items (after all chunks done)
                # Use working_item['summary_1'] instead of final_summary variable to ensure we always have a string
                # (working_item['summary_1'] is always set to a string, either from final_summary or a fallback)
                summary_for_context = working_item.get('summary_1', f"{cap_item_type_name} {item_number} summary")
                
                # Defensive: Ensure summary_for_context is a string (should always be, but be safe)
                if isinstance(summary_for_context, list):
                    print(f'    WARNING: summary_1 is a list at append time ({len(summary_for_context)} items), converting to string')
                    summary_for_context = ' '.join(str(item) for item in summary_for_context)
                    # Fix the stored value so future runs don't need this
                    working_item['summary_1'] = summary_for_context
                    proc.dirty = 1
                elif not isinstance(summary_for_context, str):
                    print(f'    WARNING: summary_1 is not a string at append time (type: {type(summary_for_context)}), converting to string')
                    summary_for_context = str(summary_for_context)
                    # Fix the stored value so future runs don't need this
                    working_item['summary_1'] = summary_for_context
                    proc.dirty = 1
                
                if first_flag:
                    first_flag = False
                    cumulative_summary_list.append(cumulative_summary_list_pre)
                cumulative_summary_list.append('\n' + cap_item_type_name + ' ' + item_number + ':\n' + summary_for_context + '\n')
                
                proc.dirty = 1
                count = count - 1
                if count < 1:
                    print(f'\n  Reached processing limit. More processing needed.')
                    break
        # Cut off cumulative summary when crossing a boundary.
        if not 'context' in working_item.keys():
            raise InputError('level_1_summaries: invalid parsed_content structure, no context in working_item.')
            exit(1)

        parent_key = _get_parent_key(parsed_content, working_item)
        if parent_key is not None:
            # Sub-unit path: reset cumulative context when switching parent containers
            if current_parent_key is not None and parent_key != current_parent_key:
                cumulative_summary_list = []
                if not '' == table_of_contents_cache:
                    cumulative_summary_list.append(table_of_contents_cache)
                first_flag = True
            current_parent_key = parent_key
        else:
            # Regular item path: existing stop_tag logic
            current_parent_key = None
            org_content_pointer = get_org_pointer(parsed_content, working_item)
            if 'stop_' + item_type_name in org_content_pointer.keys():
                stop_tag = org_content_pointer['stop_' + item_type_name]
                if stop_tag == item_number:
                    cumulative_summary_list = []
                    if not '' == table_of_contents_cache:
                        cumulative_summary_list.append(table_of_contents_cache)
                    first_flag = True
                    top_organization_summaries(proc, 1)
    proc.flush()
    # Check if we processed all items that needed processing
    # If count was exhausted (count < 1), we hit the limit and need more processing
    if count < 1:
        print('More processing needed.  Please run again.\n')
        return False
    # Otherwise, we finished all items that needed processing
    return True

def level_2_summaries(proc, count=30, checkpoint_threshold=30) -> bool:
    """
    Generate level 2 summaries for operational items in the document.
    
    This function creates detailed, context-aware summaries using:
    - Scope-aware definition collection
    - Referenced section context
    - Text chunking for long documents
    - Enhanced summarization (5+ paragraphs)
    
    Args:
        proc: SummaryProcessor object
        count (int): Maximum number of items to process in this run
    """
    client = proc.clients.get('level2', proc.client) # AI client for level 2 summaries
    parsed_content = proc.parsed_content # The document content being processed
    out_path = proc.out_path # Output file path for saving updates
    logfile = proc.logfile # Log file for tracking operations
    config = proc.config # Configuration for fallback model support

    # Setup and Validation
    if ('document_information' not in parsed_content.keys() or 
        'parameters' not in parsed_content['document_information'].keys() or
        'content' not in parsed_content.keys()):
        raise InputError('level_2_summaries: invalid parsed_content structure.')
        exit(1)

    param_pointer = parsed_content['document_information']['parameters']
    content_pointer = parsed_content['content']

    # Get table of contents for caching
    table_of_contents_cache = ''
    if proc.table_of_contents:
        table_of_contents_cache = 'Here is a table of contents for a document that is relevant to this request:\n\n' + proc.table_of_contents + '\n'

    current_parent_key = None  # Track current parent for sub-unit boundary detection

    # Count total items and items needing processing for progress reporting.
    # Containers (has_sub_units) count toward total but are handled by container_summaries()
    # after this loop, not here.
    total_items = 0
    items_needing_summary = 0
    items_processed = 0
    for _, _, _, _, item in iter_operational_items(parsed_content):
        total_items += 1
        if has_sub_units(item):
            continue
        if 'summary_2' not in item.keys() and item.get('text', ''):
            items_needing_summary += 1

    print(f'  Found {total_items} total operational items, {items_needing_summary} need summary_2')

    # Iterate through operational items
    for item_type_name, item_type_name_plural, cap_item_type_name, item_number, working_item in iter_operational_items(parsed_content):
        if has_sub_units(working_item):  # handled by container_summaries() after this loop
            continue
        # Skip items that already have summary_2
        if 'summary_2' in working_item.keys():
            # Defensive: Handle case where summary_2 is incorrectly stored as a list
            # (can happen if AI returned list instead of string in previous run)
            summary_2_text = working_item['summary_2']
            if isinstance(summary_2_text, list):
                print(f'    WARNING: summary_2 is a list ({len(summary_2_text)} items), converting to string')
                summary_2_text = ' '.join(summary_2_text)
                # Fix the stored value so future runs don't need this
                working_item['summary_2'] = summary_2_text
                proc.dirty = 1
            continue
        
        # Skip items without summary_1 (prerequisite check)
        if 'summary_1' not in working_item.keys():
            raise InputError(f'level_2_summaries: summary_1 missing for {item_type_name} {item_number}')
            exit(1)
        
        # Check prerequisites
        if '' == working_item.get('text', ''):
            working_item['summary_2'] = cap_item_type_name + ' is blank.'
            proc.dirty = 1
            continue
        
        items_processed += 1
        print(f'\n  [{items_processed}/{items_needing_summary}] Processing: {cap_item_type_name} {item_number}')
        
        # Build context (once per item)
        context_string, conflicts = build_level_2_context(parsed_content, working_item, item_type_name, item_number)

        # Log conflicting direct definitions if any
        if conflicts:
            document_issues_logfile = get_document_issues_logfile(proc.out_path)
            for c in conflicts:
                log_document_issue(
                    document_issues_logfile,
                    'conflicting_direct_definitions',
                    item_type_name=item_type_name,
                    item_number=item_number,
                    issue_description=f"Multiple direct definitions in scope for term '{c.get('term','')}'",
                    details=c
                )
        
        # Prepare chunking
        text = working_item['text']
        breakpoints = working_item.get('breakpoints', [])

        # Build metadata suffix for sections in duplicate sets
        metadata_suffix = build_metadata_suffix(item_number, working_item, content_pointer, item_type_name_plural)

        # chunk_text will yield single chunk if text is short or no breakpoints
        try:
            chunks = list(chunk_text(text, breakpoints, preferred_length=15000))
            total_chunks = len(chunks)
        except Exception as e:
            raise InputError(f'level_2_summaries: chunking failed for {item_type_name} {item_number}: {e}')
            exit(1)

        # Create base prompt
        base_prompt = create_level_2_base_prompt(proc, item_type_name, item_number, total_chunks)

        # Setup caching
        cache_prompt_list = prepare_context_for_caching(context_string, table_of_contents_cache)

        # Process chunks
        chunk_summaries = []

        for i, chunk in enumerate(chunks):
            # Augment this chunk with metadata (for _dup sections)
            augmented_chunk = augment_chunk_with_metadata(chunk, metadata_suffix)

            # Create chunk-specific prompt
            chunk_prompt = create_chunk_summary_prompt(
                base_prompt,
                augmented_chunk,  # Use augmented chunk instead of plain chunk
                i + 1,
                total_chunks,
                item_type_name,
                item_number,
                chunk_summaries,  # Empty on first iteration
                unit_title=working_item.get('unit_title', '')
            )
            
            print(f'    Chunk {i+1}/{total_chunks} of {cap_item_type_name} {item_number}...')
            
            # Query AI model (expecting plain text, not JSON)
            # Use query_text_with_retry() to benefit from retry mechanism and proper cache handling
            try:
                result = query_text_with_retry(client, cache_prompt_list, chunk_prompt, logfile, max_tokens=0, max_retries=3, config=config, task_name='stage3.summary.level2')
            except ModelError as e:
                # Retry mechanism exhausted - this is a real failure
                raise ModelError(f'level_2_summaries: Failed to get response for {item_type_name} {item_number}, chunk {i+1} after retries: {e}')
                exit(1)
            except Exception as e:
                raise ModelError(f'level_2_summaries: Unexpected error for {item_type_name} {item_number}, chunk {i+1}: {e}')
                exit(1)

            # Extract summary text and clean it
            summary_text = clean_summary_text(result)
            if not summary_text:
                # Clean up cache entry for this empty/meaningless response before raising error
                # (query_text_with_retry should have handled this, but double-check)
                from utils.api_cache import remove_cached_response
                full_cache = ''.join(cache_prompt_list)
                model_name = getattr(client, 'model', 'unknown')
                remove_cached_response(full_cache, chunk_prompt, model_name, 0)
                print(f'    [Cache cleanup] Removed empty summary cache entry for {item_type_name} {item_number}, chunk {i+1}')
                raise ModelError(f'level_2_summaries: Empty summary response for {item_type_name} {item_number}, chunk {i+1}')
                exit(1)
            
            chunk_summaries.append(summary_text)
            
            print(f'    Chunk {i+1} complete ({len(chunk_summaries[-1])} chars)')
        
        # Synthesize final summary
        if total_chunks == 1:
            final_summary = chunk_summaries[0]
        else:
            # For level 2 summaries, synthesize with appropriate paragraph count
            try:
                # Create synthesis prompt for level 2 (more detailed than level 1)
                max_paragraphs = 5 + max(0, total_chunks - 1)
                # Calculate total length of chunk summaries for length constraint
                total_chunk_length = sum(len(s) for s in chunk_summaries)
                synthesis_prompt = f"""Please create a final, cohesive summary of {item_type_name} {item_number}
by synthesizing these summaries of its parts. The summary should be up to {max_paragraphs} paragraphs and should allow
a legal practitioner to understand the rights, restrictions, and obligations set forth in this portion.
The summary should not re-iterate which {item_type_name} is being summarized, that will be attached to the summary separately.
**IMPORTANT: The final summary must not be longer than the combined length of the part summaries being synthesized.**

"""
                for i, summary in enumerate(chunk_summaries, 1):
                    synthesis_prompt += f"Part {i}: {summary}\n\n"

                synthesis_prompt += """
Create a unified summary that captures the overall content without referencing part numbers.
Return only the summary text, no preamble."""

                # Use query_text_with_retry to benefit from retry mechanism for empty responses
                result = query_text_with_retry(client, [], synthesis_prompt, logfile, max_tokens=0, max_retries=3, config=config, task_name='stage3.summary.level2.synthesis')
                final_summary = clean_summary_text(result)
            except ModelError as e:
                # Retry mechanism exhausted - this is a real failure
                raise ModelError(f'level_2_summaries: synthesis failed for {item_type_name} {item_number} after retries: {e}')
                exit(1)
            except Exception as e:
                raise ModelError(f'level_2_summaries: unexpected synthesis error for {item_type_name} {item_number}: {e}')
                exit(1)

        if not final_summary:
            # Clean up cache entry for this empty synthesis response before raising error
            from utils.api_cache import remove_cached_response
            model_name = getattr(client, 'model', 'unknown')
            remove_cached_response('', synthesis_prompt, model_name, 0)
            print(f'    [Cache cleanup] Removed empty synthesis cache entry for {item_type_name} {item_number}')
            raise ModelError(f'level_2_summaries: Final summary is empty for {item_type_name} {item_number}')
            exit(1)
        
        # CRITICAL: Set summary_2 and immediately ensure it's saved to prevent data loss
        # Wrap in try/except to ensure flush happens even if subsequent code fails
        try:
            working_item['summary_2'] = final_summary
            print(f'    [OK] {cap_item_type_name} {item_number} summary_2 complete ({len(final_summary)} chars)')
            
            # Update progress after each item for UI visibility
            operational_counts, organizational_counts = count_stage_3_progress(parsed_content)
            update_stage_3_progress(parsed_content, operational_counts, organizational_counts)
            proc.dirty = 1
            
            # CRITICAL: Flush immediately if it's been more than 10 seconds since last flush
            # This prevents data loss if an exception occurs before the regular flush
            if proc.should_flush_by_time(time_threshold_seconds=10):
                proc.flush()
                print(f'  [Immediate flush] Saved {cap_item_type_name} {item_number} summary_2 to prevent data loss')
            
            count = count - 1
            
            # Periodic checkpoint: save progress every checkpoint_threshold items
            if items_processed % checkpoint_threshold == 0:
                proc.flush()
                print(f'  [Checkpoint] Progress saved: {operational_counts["summary_2"]}/{operational_counts["total"]} operational items with summary_2')
            else:
                # Flush more frequently for UI updates (every 5 items OR if 30+ seconds have passed)
                should_flush = False
                if items_processed % 5 == 0:
                    should_flush = True
                elif proc.should_flush_by_time(time_threshold_seconds=30):
                    should_flush = True
                    print(f'  [Time-based flush] Flushing after {int(time_module.time() - proc.last_flush_time)} seconds')
                
                if should_flush:
                    proc.flush()
        except Exception as e:
            # If anything fails after setting summary_2, ensure we flush before re-raising
            # This prevents data loss if an exception occurs in progress updates or organizational handling
            print(f'  [ERROR] Exception after setting summary_2 for {cap_item_type_name} {item_number}: {e}')
            print(f'  [SAFETY] Flushing to save {cap_item_type_name} {item_number} summary_2 before re-raising exception...')
            try:
                proc.flush()
            except Exception as flush_error:
                print(f'  [CRITICAL] Failed to flush after exception: {flush_error}')
            raise  # Re-raise the original exception
        
        if count < 1:
            print(f'\n  Reached processing limit ({items_processed} items). More processing needed.')
            break
        
        # Boundary handling
        if 'context' not in working_item.keys():
            raise InputError('level_2_summaries: invalid parsed_content structure, no context in working_item.')
            exit(1)

        parent_key = _get_parent_key(parsed_content, working_item)
        if parent_key is not None:
            # Sub-unit path: trigger org summaries when switching parent containers
            if current_parent_key is not None and parent_key != current_parent_key:
                top_organization_summaries(proc, 2)
            current_parent_key = parent_key
        else:
            # Regular item path: existing stop_tag logic
            current_parent_key = None
            org_content_pointer = get_org_pointer(parsed_content, working_item)
            if 'stop_' + item_type_name in org_content_pointer.keys():
                stop_tag = org_content_pointer['stop_' + item_type_name]
                if stop_tag == item_number:
                    top_organization_summaries(proc, 2)
    
    proc.flush()
    # Check if we processed all items that needed processing
    # If count was exhausted (count < 1), we hit the limit and need more processing
    if count < 1:
        print('More processing needed.  Please run again.\n')
        return False
    # Otherwise, we finished all items that needed processing
    return True


def get_top_level_summaries(parsed_content: dict) -> str:
    """
    Extract summaries from top-level organizational units.
    
    Collects summary_2 (preferred) or summary_1 (fallback) from all top-level
    organizational units to use as input for topic statement generation.
    
    Args:
        parsed_content: The parsed document content dictionary
        
    Returns:
        str: Combined summaries text, with unit titles, separated by double newlines.
             Returns empty string if no summaries found.
    """
    org_content = parsed_content.get('document_information', {}).get('organization', {}).get('content', {})

    if not org_content:
        return ''

    content_scope = parsed_content.get('document_information', {}).get('content_scope')
    if content_scope:
        from utils.document_handling import get_org_pointer_from_scope
        scope_type, scope_id, scope_node = get_org_pointer_from_scope(org_content, content_scope)
        if scope_node is not None:
            summary = scope_node.get('summary_2') or scope_node.get('summary_1', '')
            if isinstance(summary, list):
                summary = ' '.join(summary)
            if summary and summary.strip():
                unit_title = scope_node.get('unit_title', f'{scope_type.title()} {scope_id}')
                return f"{unit_title}:\n{summary}"
        return ''

    summaries = []
    for org_type, org_units in org_content.items():
        for unit_id, unit_data in org_units.items():
            # Prefer summary_2 (more detailed), fall back to summary_1
            summary = unit_data.get('summary_2') or unit_data.get('summary_1', '')
            # Defensive: Handle case where summary_2 is incorrectly stored as a list
            if isinstance(summary, list):
                print(f'    WARNING: summary_2 is a list for {org_type} {unit_id} ({len(summary)} items), converting to string')
                summary = ' '.join(summary)
                # Fix the stored value so future runs don't need this
                if 'summary_2' in unit_data:
                    unit_data['summary_2'] = summary
            if summary and summary.strip():
                unit_title = unit_data.get('unit_title', f'{org_type.title()} {unit_id}')
                summaries.append(f"{unit_title}:\n{summary}")

    return '\n\n'.join(summaries) if summaries else ''


def generate_topic_statement(proc: SummaryProcessor) -> str:
    """
    Generate a topic statement for the entire document.
    
    Creates a concise, phrase-based statement capturing key subject areas.
    Uses top-level organizational unit summaries as input. The output is
    designed to be easily scannable by humans for quick topic identification.
    
    Args:
        proc: SummaryProcessor instance with parsed_content loaded
        
    Returns:
        str: Topic statement (phrase-based, 2-4 lines, comma-separated topics).
             Returns empty string if generation fails (topic statements are optional).
             
    Note:
        This function will retry up to 2 times if it receives an empty response.
        If all attempts fail, it returns an empty string rather than raising an error,
        allowing Stage 3 processing to complete successfully.
    """
    client = proc.clients.get('topic_statement', proc.client)
    parsed_content = proc.parsed_content
    logfile = proc.logfile
    config = proc.config
    
    # Extract top-level summaries
    summaries_text = get_top_level_summaries(parsed_content)
    
    if not summaries_text:
        # No summaries available - cannot generate topic statement
        print(f'  [WARNING] No top-level summaries found, cannot generate topic statement')
        return ''
    
    # Get document title for context
    doc_title = ''
    if ('document_information' in parsed_content and 
        'title' in parsed_content['document_information']):
        doc_title = parsed_content['document_information']['title']
    
    # Build prompt emphasizing scannability and human readability
    prompt = f"""Based on the document summaries provided below, create a brief topic statement
that captures the key subject areas covered in this document.

CRITICAL REQUIREMENTS FOR HUMAN SCANNABILITY:
- Use phrase-based format (NOT full sentences)
- Separate topics with commas
- Length: 2-4 lines maximum when displayed (approximately 150-300 characters)
- Focus on main topics, themes, or subject areas
- Make it easily scannable - a human should be able to quickly identify key topics at a glance
- More detailed than the title but much shorter than the summary
- Use clear, concise phrases that immediately convey what the document covers

FORMAT EXAMPLE:
"Library governance and funding, Congressional Research Service operations, film preservation and registry, trust fund management, Inspector General oversight"

BAD EXAMPLES (too verbose, not scannable):
- "This document covers library governance and funding mechanisms, including operations of the Congressional Research Service..."
- "The document addresses several key areas: library governance, funding mechanisms, film preservation..."

GOOD EXAMPLES (scannable, phrase-based):
- "Library governance, funding mechanisms, Congressional Research Service operations, film preservation"
- "Healthcare privacy, HIPAA compliance, covered entities, patient rights, enforcement procedures"

Document Title: {doc_title if doc_title else 'Not specified'}

Document Summaries:
{summaries_text}

Return ONLY the topic statement itself, nothing else. No preamble, no explanation, no commentary."""
    
    try:
        # Make AI query using shared retry mechanism with fallback model support
        # This uses the same retry logic as query_json() and will benefit from
        # alternative model fallback configuration
        try:
            result = query_text_with_retry(client, [], prompt, logfile, max_tokens=200, max_retries=3, config=config, task_name='stage3.summary.topic_statement')
        except ModelError as e:
            # Retry mechanism exhausted - return empty string (topic statement is optional)
            print(f'  [WARNING] Topic statement generation failed after retries: {e}')
            print(f'  [INFO] This is optional - Stage 3 will complete successfully without topic statement')
            return ''
        
        # Clean and validate the response
        topic_statement = result.strip()
        
        # Remove any JSON wrapper if present (some models might wrap in JSON)
        if topic_statement.startswith('{') or topic_statement.startswith('['):
            # Try to extract text from JSON
            try:
                import json
                parsed = json.loads(topic_statement)
                # If it's a dict, look for common keys
                if isinstance(parsed, dict):
                    topic_statement = parsed.get('topic_statement') or parsed.get('topics') or parsed.get('statement') or ''
                    if not topic_statement:
                        # Try to get first string value
                        for v in parsed.values():
                            if isinstance(v, str) and v.strip():
                                topic_statement = v
                                break
                elif isinstance(parsed, list) and parsed:
                    topic_statement = ', '.join(str(item) for item in parsed if item)
                else:
                    topic_statement = str(parsed)
            except:
                # If JSON parsing fails, try to extract text between quotes
                import re
                match = re.search(r'["\']([^"\']+)["\']', topic_statement)
                if match:
                    topic_statement = match.group(1)
        
        # Final cleanup
        topic_statement = topic_statement.strip()
        
        # Remove common prefixes that models might add
        prefixes_to_remove = [
            'Topic statement:',
            'Topics:',
            'Subject areas:',
            'The document covers:',
            'Key topics:',
        ]
        for prefix in prefixes_to_remove:
            if topic_statement.lower().startswith(prefix.lower()):
                topic_statement = topic_statement[len(prefix):].strip()
                # Remove leading colon if present
                if topic_statement.startswith(':'):
                    topic_statement = topic_statement[1:].strip()
        
        # Validate length (warn if too long, but don't fail)
        if len(topic_statement) > 400:
            print(f'  [WARNING] Topic statement is longer than recommended ({len(topic_statement)} chars), truncating to 400 chars')
            topic_statement = topic_statement[:397] + '...'
        elif len(topic_statement) < 20:
            print(f'  [WARNING] Topic statement is very short ({len(topic_statement)} chars)')
        
        # Validate it's phrase-based (not sentences) - check for sentence endings
        sentence_endings = ['. ', '! ', '? ']
        has_sentences = any(ending in topic_statement for ending in sentence_endings)
        if has_sentences:
            print(f'  [WARNING] Topic statement appears to contain sentences - should be phrase-based')
            # Try to convert sentences to phrases (remove periods, capitalize after)
            # This is a best-effort cleanup
            topic_statement = topic_statement.replace('. ', ', ').replace('! ', ', ').replace('? ', ', ')
            # Remove trailing period if present
            if topic_statement.endswith('.'):
                topic_statement = topic_statement[:-1]
        
        if not topic_statement:
            # Return empty string instead of raising error - topic statement is optional
            print(f'  [WARNING] Topic statement is empty after cleaning - skipping')
            return ''
        
        return topic_statement
        
    except Exception as e:
        # Don't raise error - topic statement generation is optional
        # Log warning and return empty string so Stage 3 can complete
        print(f'  [WARNING] Failed to generate topic statement: {e}')
        import traceback
        traceback.print_exc()
        return ''  # Return empty string instead of raising error


def process_file_stage_3(parse_file_path, output_file_path, client, logfile, checkpoint_threshold, max_items=300, clients=None, config=None):
    """
    Process a single file through Stage 3.

    Args:
        parse_file_path: Path to parse output JSON file
        output_file_path: Path to processed output JSON file
        client: AI client for processing (backward compatibility - now uses clients dict if provided)
        logfile: Log file path
        checkpoint_threshold: Number of items between checkpoints
        max_items: Maximum number of items to process in this run (default: 300)
        clients: Optional dict of task-specific AI clients (new flexible model system)
        config: Optional configuration dictionary (enables fallback model support)

    Returns:
        True if processing completed, False if more processing needed
    """
    parsed_content = None

    # Stage 3 MUST load from processed file only (Stage 2 must be complete first)
    if not os.path.exists(output_file_path):
        print(f'Error: Processed file not found (Stage 2 must be run first)')
        print(f'  Expected file: {output_file_path}')
        print(f'  Please run Process_Stage_2.py on this file first')
        return False

    with open(output_file_path, encoding='utf-8') as json_file:
        parsed_content = json.load(json_file)

    # Initialize processing status
    init_processing_status(parsed_content)

    # Check if Stage 2 is complete (prerequisite for Stage 3)
    if not is_stage_2_complete(parsed_content):
        print(f'  Error: Stage 2 not complete (prerequisite for Stage 3)')
        print(f'  Please run Process_Stage_2.py on this file first')
        return False

    # Check if already complete
    if is_stage_3_complete(parsed_content):
        print(f'  Stage 3 already complete, skipping')
        return True

    # Store original content for change detection
    original_content = json.dumps(parsed_content, indent=4)

    CheckVersion(parsed_content)

    # Count progress for initial status
    operational_counts, organizational_counts = count_stage_3_progress(parsed_content)
    print(f'\n=== Stage 3 Processing Started ===')
    print(f'  Operational items: {operational_counts["summary_1"]}/{operational_counts["total"]} with summary_1, {operational_counts["summary_2"]}/{operational_counts["total"]} with summary_2')
    print(f'  Organizational units: {organizational_counts["summary_2"]}/{organizational_counts["total"]} with summary_2')

    # Update initial progress
    update_stage_3_progress(parsed_content, operational_counts, organizational_counts)

    # Save initial status
    proc = SummaryProcessor(client, logfile, parsed_content, output_file_path, clients=clients, config=config)
    proc.flush()

    # Run Stage 3 processing (level 1 and level 2 summaries)
    try:
        print(f'\n=== Phase 1: Organizational Level 1 Summaries ===')
        top_organization_summaries(proc, 1)
    except Exception as e:
        print(f'  ERROR in Phase 1: {e}')
        import traceback
        traceback_str = traceback.format_exc()
        traceback.print_exc()
        # Log to exception log file
        try:
            exception_logfile = os.path.join(os.path.dirname(output_file_path), 'exceptions.log')
            log_exception_to_file(exception_logfile, e, context=f'Phase 1: Organizational Level 1 Summaries for {os.path.basename(output_file_path)}', traceback_str=traceback_str)
        except:
            pass  # Don't fail if logging fails
        return False
    
    try:
        print(f'\n=== Phase 2: Operational Level 1 Summaries ===')
        print(f'  Processing up to {max_items} items that need summary_1...')
        level_1_complete = level_1_summaries(proc, max_items, checkpoint_threshold)
    except Exception as e:
        print(f'  ERROR in Phase 2: {e}')
        import traceback
        traceback_str = traceback.format_exc()
        traceback.print_exc()
        # Log to exception log file
        try:
            exception_logfile = os.path.join(os.path.dirname(output_file_path), 'exceptions.log')
            log_exception_to_file(exception_logfile, e, context=f'Phase 2: Operational Level 1 Summaries for {os.path.basename(output_file_path)}', traceback_str=traceback_str)
        except:
            pass  # Don't fail if logging fails
        # Update progress before returning
        operational_counts, organizational_counts = count_stage_3_progress(parsed_content)
        update_stage_3_progress(parsed_content, operational_counts, organizational_counts)
        proc.flush()
        return False

    # Update progress after level 1
    operational_counts, organizational_counts = count_stage_3_progress(parsed_content)
    update_stage_3_progress(parsed_content, operational_counts, organizational_counts)
    proc.flush()
    print(f'\n  Progress after Level 1: {operational_counts["summary_1"]}/{operational_counts["total"]} operational items with summary_1')
    
    if not level_1_complete:
        print(f'\n  [WARNING] Level 1 summaries not complete - more processing needed')
        print(f'  Please run Stage 3 again to continue processing')
        return False

    # Phase 2b: Aggregate sub-unit summary_1 into parent containers
    try:
        print(f'\n=== Phase 2b: Container Level 1 Summaries ===')
        container_summaries(proc, 1)
    except Exception as e:
        print(f'  ERROR in Phase 2b: {e}')
        import traceback
        traceback_str = traceback.format_exc()
        traceback.print_exc()
        try:
            exception_logfile = os.path.join(os.path.dirname(output_file_path), 'exceptions.log')
            log_exception_to_file(exception_logfile, e, context=f'Phase 2b: Container Level 1 Summaries for {os.path.basename(output_file_path)}', traceback_str=traceback_str)
        except:
            pass
        return False

    # Phase 2c: Re-run org level 1 summaries now that containers have summary_1.
    # Phase 1 ran before containers existed; org units whose children are containers
    # (e.g., the Part that contains the CCL supplement) never received summary_1.
    try:
        print(f'\n=== Phase 2c: Organizational Level 1 Summaries (post-container) ===')
        top_organization_summaries(proc, 1)
    except Exception as e:
        print(f'  ERROR in Phase 2c: {e}')
        import traceback
        traceback_str = traceback.format_exc()
        traceback.print_exc()
        try:
            exception_logfile = os.path.join(os.path.dirname(output_file_path), 'exceptions.log')
            log_exception_to_file(exception_logfile, e, context=f'Phase 2c: Organizational Level 1 Summaries (post-container) for {os.path.basename(output_file_path)}', traceback_str=traceback_str)
        except:
            pass
        return False

    # Generate level 2 summaries (detailed, context-aware)
    try:
        print(f'\n=== Phase 3: Organizational Level 2 Summaries ===')
        top_organization_summaries(proc, 2)  # Update org summaries first
    except Exception as e:
        print(f'  ERROR in Phase 3: {e}')
        import traceback
        traceback_str = traceback.format_exc()
        traceback.print_exc()
        # Log to exception log file
        try:
            exception_logfile = os.path.join(os.path.dirname(output_file_path), 'exceptions.log')
            log_exception_to_file(exception_logfile, e, context=f'Phase 3: Organizational Level 2 Summaries for {os.path.basename(output_file_path)}', traceback_str=traceback_str)
        except:
            pass  # Don't fail if logging fails
        return False
    
    try:
        print(f'\n=== Phase 4: Operational Level 2 Summaries ===')
        print(f'  Processing up to {max_items} items that need summary_2...')
        level_2_complete = level_2_summaries(proc, max_items, checkpoint_threshold)
    except Exception as e:
        print(f'  ERROR in Phase 4: {e}')
        import traceback
        traceback_str = traceback.format_exc()
        traceback.print_exc()
        # Log to exception log file
        try:
            exception_logfile = os.path.join(os.path.dirname(output_file_path), 'exceptions.log')
            log_exception_to_file(exception_logfile, e, context=f'Phase 4: Operational Level 2 Summaries for {os.path.basename(output_file_path)}', traceback_str=traceback_str)
        except:
            pass  # Don't fail if logging fails
        # Update progress before returning
        operational_counts, organizational_counts = count_stage_3_progress(parsed_content)
        update_stage_3_progress(parsed_content, operational_counts, organizational_counts)
        proc.flush()
        return False
    
    if not level_2_complete:
        print(f'\n  [WARNING] Level 2 summaries not complete - more processing needed')
        print(f'  Please run Stage 3 again to continue processing')
        return False

    # Phase 4b: Aggregate sub-unit summary_2 into parent containers
    try:
        print(f'\n=== Phase 4b: Container Level 2 Summaries ===')
        container_summaries(proc, 2)
    except Exception as e:
        print(f'  ERROR in Phase 4b: {e}')
        import traceback
        traceback_str = traceback.format_exc()
        traceback.print_exc()
        try:
            exception_logfile = os.path.join(os.path.dirname(output_file_path), 'exceptions.log')
            log_exception_to_file(exception_logfile, e, context=f'Phase 4b: Container Level 2 Summaries for {os.path.basename(output_file_path)}', traceback_str=traceback_str)
        except:
            pass
        return False

    # Update progress status after level 2
    operational_counts, organizational_counts = count_stage_3_progress(parsed_content)
    update_stage_3_progress(parsed_content, operational_counts, organizational_counts)

    # Phase 5: Final organizational summaries (after all operational summaries are done)
    try:
        print(f'\n=== Phase 5: Final Organizational Summaries ===')
        print(f'  Organizational units before: {organizational_counts["summary_2"]}/{organizational_counts["total"]} with summary_2')

        # Generate organizational summary_2 fields
        top_organization_summaries(proc, 2)

        # Flush changes immediately
        proc.flush()

        # Update counts after Phase 5
        operational_counts, organizational_counts = count_stage_3_progress(parsed_content)
        update_stage_3_progress(parsed_content, operational_counts, organizational_counts)

        print(f'  Organizational units after: {organizational_counts["summary_2"]}/{organizational_counts["total"]} with summary_2')

        if organizational_counts["summary_2"] < organizational_counts["total"]:
            print(f'  [WARNING] Some organizational summaries could not be generated')

    except Exception as e:
        print(f'  ERROR in Phase 5: {e}')
        import traceback
        traceback_str = traceback.format_exc()
        traceback.print_exc()
        # Log to exception log file
        try:
            exception_logfile = os.path.join(os.path.dirname(output_file_path), 'exceptions.log')
            log_exception_to_file(exception_logfile, e, context=f'Phase 5: Final Organizational Summaries for {os.path.basename(output_file_path)}', traceback_str=traceback_str)
        except:
            pass  # Don't fail if logging fails
        # Don't return False here - we want to continue to topic statement generation

    # Update final progress status
    operational_counts, organizational_counts = count_stage_3_progress(parsed_content)
    update_stage_3_progress(parsed_content, operational_counts, organizational_counts)

    # Generate topic statement if Stage 3 is complete
    if level_2_complete:
        try:
            print(f'\n=== Phase 6: Topic Statement Generation ===')
            
            # Check if topic statement already exists
            existing_topic = parsed_content.get('document_information', {}).get('topic_statement', '')
            if existing_topic and existing_topic.strip():
                print(f'  Topic statement already exists, skipping generation')
                print(f'  Existing: {existing_topic[:100]}...' if len(existing_topic) > 100 else f'  Existing: {existing_topic}')
            else:
                topic_statement = generate_topic_statement(proc)
                
                if topic_statement:
                    # Store in document_information
                    if 'document_information' not in parsed_content:
                        parsed_content['document_information'] = {}
                    parsed_content['document_information']['topic_statement'] = topic_statement
                    proc.parsed_content = parsed_content
                    proc.dirty = 1
                    
                    print(f'  [OK] Topic statement generated ({len(topic_statement)} chars)')
                    # Show preview (first 150 chars or full if shorter)
                    preview = topic_statement[:150] + '...' if len(topic_statement) > 150 else topic_statement
                    print(f'  Topic: {preview}')
                    
                    # Flush immediately to save topic statement
                    proc.flush()
                else:
                    print(f'  [WARNING] Topic statement generation returned empty result')
                    
        except Exception as e:
            print(f'  [WARNING] Failed to generate topic statement: {e}')
            import traceback
            traceback.print_exc()
            # Don't fail Stage 3 if topic statement generation fails - it's optional
            # Log to exception log file
            try:
                exception_logfile = os.path.join(os.path.dirname(output_file_path), 'exceptions.log')
                log_exception_to_file(exception_logfile, e, 
                    context=f'Topic Statement Generation for {os.path.basename(output_file_path)}', 
                    traceback_str=traceback.format_exc())
            except:
                pass  # Don't fail if logging fails
    
    print(f'\n=== Stage 3 Processing Complete ===')
    print(f'  Final status:')
    print(f'    Operational items: {operational_counts["summary_1"]}/{operational_counts["total"]} with summary_1, {operational_counts["summary_2"]}/{operational_counts["total"]} with summary_2')
    print(f'    Organizational units: {organizational_counts["summary_2"]}/{organizational_counts["total"]} with summary_2')
    
    status = get_processing_status(parsed_content)
    if status['stage_3_complete']:
        print(f'  [SUCCESS] Stage 3 is complete!')
    else:
        print(f'  [WARNING] Stage 3 not yet complete - more processing needed')

    # Output only if changes were made
    json_content = json.dumps(parsed_content, indent=4, ensure_ascii=False)
    if json_content != original_content:
        with open(output_file_path, "w", encoding='utf-8') as outfile:
            outfile.write(json_content)
        print(f'  Output written: {output_file_path}')
    else:
        print(f'  No changes detected, skipping file write')

    return True


def main():
    """Main entry point for Stage 3 processing."""
    parser = argparse.ArgumentParser(
        description='Process documents through Stage 3 (summary generation).'
    )
    parser.add_argument(
        'input_path',
        help='Path to JSON file, directory, or source file (XML/HTML)'
    )
    parser.add_argument(
        '--filter',
        default='',
        help='Filter criteria for manifest-based discovery (e.g., "title=42,chapter=6A")'
    )
    parser.add_argument(
        '--config',
        default='config.json',
        help='Path to configuration file (default: config.json)'
    )
    parser.add_argument(
        '--checkpoint-threshold',
        type=int,
        help='Number of items between checkpoints (overrides config)'
    )
    parser.add_argument(
        '--max-items',
        type=int,
        default=300,
        help='Maximum number of items to process in this run before pausing (default: 300)'
    )

    args = parser.parse_args()

    # Load configuration
    config = get_config(args.config)

    # Determine checkpoint threshold
    if args.checkpoint_threshold:
        checkpoint_threshold = args.checkpoint_threshold
    else:
        checkpoint_threshold = get_checkpoint_threshold(config)
    
    # Get max items
    max_items = args.max_items

    # Discover files to process
    files_to_process = discover_files_to_process(args.input_path, config, args.filter)

    if not files_to_process:
        print(f'No files found to process in: {args.input_path}')
        if args.filter:
            print(f'Filter criteria: {args.filter}')
        sys.exit(1)

    print(f'Found {len(files_to_process)} file(s) to process')

    # Create task-specific AI clients and logfile
    # Determine directory for logfile
    if os.path.isdir(args.input_path):
        log_dir = args.input_path
    else:
        log_dir = os.path.dirname(args.input_path)

    # Create clients for different Stage 3 tasks
    from utils.config import create_client_for_task
    clients = {
        'organizational': create_client_for_task(config, 'stage3.summary.organizational'),
        'level1': create_client_for_task(config, 'stage3.summary.level1'),
        'level1_with_references': create_client_for_task(config, 'stage3.summary.level1_with_references'),
        'level2': create_client_for_task(config, 'stage3.summary.level2'),
        'topic_statement': create_client_for_task(config, 'stage3.summary.topic_statement')
    }

    # For backward compatibility, keep a default client
    client = clients['level1']
    logfile = GetLogfile(log_dir)

    # Process each file
    all_complete = True
    for parse_file, output_file in files_to_process:
        print(f'\nProcessing: {os.path.basename(parse_file)}')
        try:
            result = process_file_stage_3(parse_file, output_file, client, logfile, checkpoint_threshold, max_items, clients=clients, config=config)
            if not result:
                all_complete = False
                print(f'  [WARNING] Processing incomplete - more processing needed for this file')
        except Exception as e:
            print(f'  Error processing file: {e}')
            import traceback
            traceback_str = traceback.format_exc()
            traceback.print_exc()
            # Log to exception log file
            try:
                log_dir = os.path.dirname(parse_file) if os.path.isfile(parse_file) else parse_file
                exception_logfile = os.path.join(log_dir, 'exceptions.log')
                log_exception_to_file(exception_logfile, e, context=f'process_file_stage_3 for {os.path.basename(parse_file)}', traceback_str=traceback_str)
            except:
                pass  # Don't fail if logging fails
            all_complete = False
            continue

    if all_complete:
        print('\n[SUCCESS] Stage 3 processing complete')
    else:
        print('\n[WARNING] Stage 3 processing incomplete - some files need more processing')
        print('  Please run Stage 3 again to continue processing')
        sys.exit(1)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('\n[INTERRUPTED] Processing interrupted by user')
        sys.exit(130)  # Standard exit code for Ctrl+C
    except Exception as e:
        # Top-level exception handler - log to file and stderr
        import traceback
        error_msg = f'[CRITICAL] Unhandled exception in main(): {e}'
        print(error_msg, file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        
        # Try to log to a file if we can determine the output directory
        try:
            # Try to get log directory from command line args
            import argparse
            parser = argparse.ArgumentParser()
            parser.add_argument('input_path')
            parser.add_argument('--config', default='config.json')
            args, _ = parser.parse_known_args()
            
            # Determine log directory
            if os.path.isdir(args.input_path):
                log_dir = args.input_path
            elif os.path.isfile(args.input_path):
                log_dir = os.path.dirname(args.input_path)
            else:
                # Try to get from config
                try:
                    config = get_config(args.config)
                    log_dir = get_output_directory(config)
                except:
                    log_dir = os.path.abspath(os.path.curdir)
            
            # Create exception log file
            exception_logfile = os.path.join(log_dir, 'exceptions.log')
            log_exception_to_file(
                exception_logfile,
                e,
                context='main() - top-level exception',
                traceback_str=traceback.format_exc()
            )
            print(f'[INFO] Exception logged to: {exception_logfile}', file=sys.stderr)
        except Exception as log_error:
            print(f'[WARNING] Failed to log exception to file: {log_error}', file=sys.stderr)
        
        sys.exit(1)

