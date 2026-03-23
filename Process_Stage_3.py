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
from utils.config import (get_config, get_output_directory, get_checkpoint_threshold,
                          get_cumulative_summary_list_max_chars, get_org_summary_batch_threshold,
                          get_level2_tool_use_threshold)
from utils.ai_client import query_text_with_retry
from utils.processing_status import (
    init_processing_status,
    update_stage_3_progress,
    count_stage_3_progress,
    is_stage_2_complete,
    is_stage_3_complete,
    get_processing_status
)
from utils.document_handling import iter_operational_items, has_sub_units, lookup_item, build_metadata_suffix, augment_chunk_with_metadata, _resolve_param_key
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


def trim_cumulative_summary_list(summary_list, max_chars, preserve_entry):
    """
    Trim oldest entries from cumulative_summary_list to stay under max_chars.

    Removes entries from the front (oldest first) until total size <= max_chars.
    Always skips entries equal to preserve_entry so the header is never removed.

    Args:
        summary_list: list of strings, modified in place
        max_chars: maximum total character count
        preserve_entry: string that must never be removed (the pre-header entry)
    """
    total = sum(len(s) for s in summary_list)
    if total <= max_chars:
        return
    i = 0
    while total > max_chars and i < len(summary_list):
        if summary_list[i] == preserve_entry:
            i += 1
            continue
        total -= len(summary_list[i])
        summary_list.pop(i)
        # don't increment i — next entry is now at the same index


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
                                # Collect individual item parts in document order
                                item_parts = []
                                if 2 == local_provisions:
                                    for local_list in local_prov_nums:
                                        item_name = local_list[0]
                                        item_name_plural = local_list[1]
                                        item_num = local_list[2]
                                        item_parts.append(item_name + ' ' + item_num + ': ' + content_pointer[item_name_plural][item_num][summary_text_label] + '\n\n')
                                if 2 == sub_provisions:
                                    for sub_list in sub_prov_nums:
                                        sub_level_name = sub_list[0]
                                        sub_level_name_plural = sub_list[1]
                                        sub_level_num = sub_list[2]
                                        item_parts.append(sub_level_name + ' ' + sub_level_num + ': ' + working_item[sub_level_name][sub_level_num][summary_text_label] + '\n\n')
                                if item_parts:
                                    org_batch_threshold = get_org_summary_batch_threshold(config)
                                    total_chars = sum(len(p) for p in item_parts)

                                    def _make_org_prompt(is_brief):
                                        p = ''
                                        if is_brief:
                                            p += 'Please write a short summary of this set of one or more items. Describe them as a whole, without reference to their individual numbers. '
                                        else:
                                            p += 'Please write a summary of this set of one or more items. Describe them as a whole, without reference to their individual numbers. '
                                            p += 'While the summary should abstract away some detail, the intention is that the summary will give a reader a good understanding of what can be found in the material. '
                                        p += 'Please provide your response without any preamble, just the actual summary.\n\n'
                                        return p

                                    if total_chars <= org_batch_threshold:
                                        # Single call — same as before
                                        summaries = ''.join(item_parts)
                                        prompt = _make_org_prompt(1 == summary_number)
                                        prompt += 'Here is the content to summarize:\n\n' + summaries
                                        result = query_text_with_retry(client, [], prompt, logfile, max_tokens=0, max_retries=3, config=config, task_name='stage3.summary.organizational')
                                        if not '' == clean_summary_text(result):
                                            working_item[summary_text_label] = clean_summary_text(result)
                                    else:
                                        # Batch-and-synthesize path
                                        batches = []
                                        current_batch = []
                                        current_batch_len = 0
                                        for part in item_parts:
                                            if current_batch and current_batch_len + len(part) > org_batch_threshold:
                                                batches.append(current_batch)
                                                current_batch = [part]
                                                current_batch_len = len(part)
                                            else:
                                                current_batch.append(part)
                                                current_batch_len += len(part)
                                        if current_batch:
                                            batches.append(current_batch)

                                        # Generate interim summary per batch
                                        interim_summaries = []
                                        for batch_idx, batch in enumerate(batches):
                                            batch_text = ''.join(batch)
                                            interim_prompt = (
                                                f'Please write a brief summary of the following items (part {batch_idx + 1} of {len(batches)}). '
                                                f'Describe them as a whole, without reference to their individual numbers. '
                                                f'Please provide your response without any preamble, just the actual summary.\n\n'
                                                f'Here is the content to summarize:\n\n{batch_text}'
                                            )
                                            interim_result = query_text_with_retry(client, [], interim_prompt, logfile, max_tokens=0, max_retries=3, config=config, task_name='stage3.summary.organizational.batch_interim')
                                            interim_text = clean_summary_text(interim_result)
                                            if interim_text:
                                                interim_summaries.append(f'Part {batch_idx + 1}: {interim_text}')

                                        # Synthesize interim summaries into final summary
                                        if interim_summaries:
                                            synthesis_content = '\n\n'.join(interim_summaries)
                                            synthesis_prompt = _make_org_prompt(1 == summary_number)
                                            synthesis_prompt += 'Here is the content to summarize:\n\n' + synthesis_content
                                            result = query_text_with_retry(client, [], synthesis_prompt, logfile, max_tokens=0, max_retries=3, config=config, task_name='stage3.summary.organizational')
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



def _summarize_leaf_level1(type_name, type_plural, cap_type_name, item_number, working_item, state):
    """
    Generate a level 1 summary for a single leaf operational item.

    Handles three cases: blank text (fixed summary, no AI call), existing summary
    (idempotency — just append to cumulative context), and new summary (AI call,
    reference extraction, flush logic).  Updates state in place.
    """
    parsed_content = state['parsed_content']
    content_pointer = state['content_pointer']
    client = state['client']
    logfile = state['logfile']
    config = state['config']
    proc = state['proc']
    checkpoint_threshold = state['checkpoint_threshold']

    if '' == working_item['text']:
        working_item['summary_1'] = cap_type_name + ' is blank.'
        working_item['defined_terms'] = []
        working_item['need_ref'] = []
        return

    if 'summary_1' in working_item:  # Existing summary — idempotency path
        print(f'  [{state["items_processed"][0]+1}/{state["items_needing_summary"]}] Existing: {item_number}')
        if state['first_flag'][0]:
            state['first_flag'][0] = False
            state['cumulative_summary_list'].append(state['cumulative_summary_list_pre'])
        summary_text = working_item['summary_1']
        if isinstance(summary_text, list):
            print(f'    WARNING: summary_1 is a list ({len(summary_text)} items), converting to string')
            summary_text = ' '.join(summary_text)
            working_item['summary_1'] = summary_text
            proc.dirty = 1
        state['cumulative_summary_list'].append('\n' + cap_type_name + ' ' + item_number + ':\n' + summary_text + '\n')
        trim_cumulative_summary_list(state['cumulative_summary_list'], state.get('cumulative_summary_max_chars', 30000), state['cumulative_summary_list_pre'])
        return

    state['items_processed'][0] += 1
    print(f'\n  [{state["items_processed"][0]}/{state["items_needing_summary"]}] Processing: {cap_type_name} {item_number}')

    # Build base prompt
    base_prompt = 'Please provide two things in your response: (i) a short (no more than three sentences) summary of this ' + type_name + ' from a longer document, and '
    base_prompt += '(ii) a list of references that are explicitly cited or mentioned in the text that would be needed to make a better summary.  '
    base_prompt += 'The summary should not re-iterate which ' + type_name + ' is being summarized, '
    base_prompt += 'that will be attached to the summary separately.\n\n'
    base_prompt += 'Please return these two things in JSON format, following the form of this example, with no preamble and no '
    base_prompt += 'response other than in this JSON format:\n'
    base_prompt += get_summary_JSON_format(state['type_name_list']) + '\n\n'
    base_prompt += 'Only include references that are explicitly cited or mentioned in the text being summarized. Use these types:\n'
    for list_item_type_name in state['type_name_list']:
        cap_tn = list_item_type_name[0].upper() + list_item_type_name[1:]
        base_prompt += ' "' + cap_tn + '" - for ' + list_item_type_name + 's mentioned in the text (use the identifier that appears in the table of contents),\n'
    base_prompt += ' "External" - for references to other documents,\n'
    base_prompt += ' "Need_Definition" - for terms needing definitions (return the term EXACTLY as it appears in the text, preserving any HTML/XML font markup like <sub>, <sup>, <i>, <b>, etc.).\n\n'
    reference_guidance = get_reference_instructions(parsed_content)
    if reference_guidance:
        base_prompt += 'Reference formatting guidance:\n' + reference_guidance + '\n\n'
    base_prompt += 'If the text cites a sub-unit that doesn\'t appear in the table of contents (e.g., "' + type_name + ' 12(b)"), report the closest parent that does appear (e.g., "12").\n\n'

    # UNIFIED CHUNKING PATH (handles both chunked and non-chunked)
    text = working_item['text']
    breakpoints = working_item.get('breakpoints', [])
    metadata_suffix = build_metadata_suffix(item_number, working_item, content_pointer, type_plural)
    chunks = list(chunk_text(text, breakpoints, preferred_length=15000))
    total_chunks = len(chunks)
    chunk_summaries = []
    all_references = []

    chunk_prefix = working_item.get('chunk_prefix', '')
    for i, chunk in enumerate(chunks):
        if chunk_prefix:
            chunk = chunk_prefix + '\n\n' + chunk
        augmented_chunk = augment_chunk_with_metadata(chunk, metadata_suffix)
        chunk_prompt = create_chunk_summary_prompt(
            base_prompt, augmented_chunk, i + 1, total_chunks,
            type_name, item_number, chunk_summaries,
            unit_title=working_item.get('unit_title', '')
        )
        print(f'    Chunk {i+1}/{total_chunks} of {cap_type_name} {item_number}...')
        try:
            result = query_json(client, state['cumulative_summary_list'], chunk_prompt, logfile,
                                expected_keys=['summary'], config=config,
                                task_name='stage3.summary.level1_with_references')
        except Exception as e:
            print(f'    ERROR: Failed to query AI for chunk {i+1} of {cap_type_name} {item_number}: {e}')
            import traceback
            traceback.print_exc()
            raise ModelError(f'Failed to get response for chunk {i+1} of {cap_type_name} {item_number}: {e}')
        print(f'    Chunk {i+1} complete')
        if not result:
            raise ModelError('Failed to get response.\n')
        if 'summary' in result.keys():
            summary_text = result['summary']
            if isinstance(summary_text, list):
                print(f'    WARNING: Chunk {i+1} summary is a list ({len(summary_text)} items), converting to string')
                summary_text = ' '.join(str(x) for x in summary_text)
            elif not isinstance(summary_text, str):
                print(f'    WARNING: Chunk {i+1} summary is not a string (type: {type(summary_text)}), converting to string')
                summary_text = str(summary_text)
            chunk_summaries.append(summary_text)
        if 'references' in result.keys():
            all_references.extend(result['references'])

    # Synthesize final summary (no-op if only one chunk)
    try:
        final_summary = synthesize_final_summary(
            chunk_summaries, type_name, item_number, client, logfile, config
        )
        if isinstance(final_summary, list):
            print(f'    WARNING: Final summary is a list ({len(final_summary)} items), converting to string')
            final_summary = ' '.join(str(x) for x in final_summary)
        elif not isinstance(final_summary, str):
            print(f'    WARNING: Final summary is not a string (type: {type(final_summary)}), converting to string')
            final_summary = str(final_summary)
        if not final_summary:
            print(f'    WARNING: Empty summary for {cap_type_name} {item_number}')
            final_summary = f'{cap_type_name} {item_number} summary unavailable'
        working_item['summary_1'] = final_summary
        print(f'    [OK] {cap_type_name} {item_number} summary_1 complete ({len(final_summary)} chars)')
    except Exception as e:
        print(f'    ERROR: Failed to synthesize summary for {cap_type_name} {item_number}: {e}')
        import traceback
        traceback.print_exc()
        working_item['summary_1'] = f'{cap_type_name} {item_number} summary generation failed: {str(e)}'
        proc.dirty = 1

    # CRITICAL: Flush immediately after setting summary_1 to prevent data loss
    try:
        working_item['need_ref'] = deduplicate_references(all_references)
        operational_counts, organizational_counts = count_stage_3_progress(parsed_content)
        update_stage_3_progress(parsed_content, operational_counts, organizational_counts)
        proc.dirty = 1
        if proc.should_flush_by_time(time_threshold_seconds=10):
            proc.flush()
            print(f'  [Immediate flush] Saved {cap_type_name} {item_number} summary_1 to prevent data loss')
        if state['items_processed'][0] % checkpoint_threshold == 0:
            proc.flush()
            print(f'  [Checkpoint] Progress saved: {operational_counts["summary_1"]}/{operational_counts["total"]} operational items with summary_1')
        else:
            should_flush = False
            if state['items_processed'][0] % 5 == 0:
                should_flush = True
            elif proc.should_flush_by_time(time_threshold_seconds=30):
                should_flush = True
                print(f'  [Time-based flush] Flushing after {int(time_module.time() - proc.last_flush_time)} seconds')
            if should_flush:
                proc.flush()
    except Exception as e:
        print(f'  [ERROR] Exception after setting summary_1 for {cap_type_name} {item_number}: {e}')
        print(f'  [SAFETY] Flushing to save {cap_type_name} {item_number} summary_1 before re-raising exception...')
        try:
            proc.flush()
        except Exception as flush_error:
            print(f'  [CRITICAL] Failed to flush after exception: {flush_error}')
        raise

    # Add to cumulative context
    summary_for_context = working_item.get('summary_1', f'{cap_type_name} {item_number} summary')
    if isinstance(summary_for_context, list):
        print(f'    WARNING: summary_1 is a list at append time ({len(summary_for_context)} items), converting to string')
        summary_for_context = ' '.join(str(x) for x in summary_for_context)
        working_item['summary_1'] = summary_for_context
        proc.dirty = 1
    elif not isinstance(summary_for_context, str):
        print(f'    WARNING: summary_1 is not a string at append time (type: {type(summary_for_context)}), converting to string')
        summary_for_context = str(summary_for_context)
        working_item['summary_1'] = summary_for_context
        proc.dirty = 1
    if state['first_flag'][0]:
        state['first_flag'][0] = False
        state['cumulative_summary_list'].append(state['cumulative_summary_list_pre'])
    state['cumulative_summary_list'].append('\n' + cap_type_name + ' ' + item_number + ':\n' + summary_for_context + '\n')
    trim_cumulative_summary_list(state['cumulative_summary_list'], state.get('cumulative_summary_max_chars', 30000), state['cumulative_summary_list_pre'])
    proc.dirty = 1
    state['count'][0] -= 1


def _summarize_container_level1(type_name, type_plural, cap_type_name, item_number, working_item, state):
    """
    Generate a level 1 summary for a container item (one with sub-units).

    Called after all sub-units have been recursively processed by
    _process_item_level1, so sub-unit summary_1 values are already populated.

    If the container has own substantive text: uses query_json (same as leaf items)
    to extract both a summary and a reference list.  Single chunk — container
    own-text is structural preamble, not dense prose.

    If the container is a pure aggregator (no own text): uses query_text_with_retry
    to synthesize from the sub-unit summaries.

    Both branches pass the current cumulative_summary_list as context so the AI
    has TOC and prior-item context.
    """
    # Idempotency: if already summarised, just add to cumulative context and return
    if 'summary_1' in working_item and working_item['summary_1']:
        if state['first_flag'][0]:
            state['first_flag'][0] = False
            state['cumulative_summary_list'].append(state['cumulative_summary_list_pre'])
        state['cumulative_summary_list'].append(
            '\n' + cap_type_name + ' ' + item_number + ':\n' + working_item['summary_1'] + '\n'
        )
        trim_cumulative_summary_list(state['cumulative_summary_list'], state.get('cumulative_summary_max_chars', 30000), state['cumulative_summary_list_pre'])
        return

    # Collect sub-unit summaries
    sub_summaries = []
    all_present = True
    for sub_type_key, sub_type_items in working_item['sub_units'].items():
        for sub_key, sub_item in sub_type_items.items():
            if 'summary_1' in sub_item and sub_item['summary_1']:
                sub_summaries.append(f'{sub_key}: {sub_item["summary_1"]}')
            else:
                all_present = False
                break
        if not all_present:
            break

    if not all_present or not sub_summaries:
        print(f'  [WARNING] Container {cap_type_name} {item_number}: missing sub-unit summaries, skipping')
        return

    parsed_content = state['parsed_content']
    client = state['client']
    logfile = state['logfile']
    config = state['config']
    proc = state['proc']
    container_text = working_item.get('text', '').strip()

    state['items_processed'][0] += 1
    print(f'\n  [{state["items_processed"][0]}/{state["items_needing_summary"]}] Processing container: {cap_type_name} {item_number}')

    if container_text:
        # Container has own text: query_json for summary + references, single chunk.
        base_prompt = 'Please provide two things in your response: (i) a short (no more than three sentences) summary of this ' + type_name + ' from a longer document, and '
        base_prompt += '(ii) a list of references that are explicitly cited or mentioned in the text that would be needed to make a better summary.  '
        base_prompt += 'The summary should not re-iterate which ' + type_name + ' is being summarized, '
        base_prompt += 'that will be attached to the summary separately.\n\n'
        base_prompt += 'Please return these two things in JSON format, following the form of this example, with no preamble and no '
        base_prompt += 'response other than in this JSON format:\n'
        base_prompt += get_summary_JSON_format(state['type_name_list']) + '\n\n'
        base_prompt += 'Only include references that are explicitly cited or mentioned in the text being summarized. Use these types:\n'
        for list_item_type_name in state['type_name_list']:
            cap_tn = list_item_type_name[0].upper() + list_item_type_name[1:]
            base_prompt += ' "' + cap_tn + '" - for ' + list_item_type_name + 's mentioned in the text (use the identifier that appears in the table of contents),\n'
        base_prompt += ' "External" - for references to other documents,\n'
        base_prompt += ' "Need_Definition" - for terms needing definitions (return the term EXACTLY as it appears in the text, preserving any HTML/XML font markup like <sub>, <sup>, <i>, <b>, etc.).\n\n'
        reference_guidance = get_reference_instructions(parsed_content)
        if reference_guidance:
            base_prompt += 'Reference formatting guidance:\n' + reference_guidance + '\n\n'

        # Combine container text + sub-unit summaries as a single chunk
        sub_block = '\n[Subsidiary unit summaries:]\n' + '\n'.join(sub_summaries)
        combined_text = container_text + '\n' + sub_block
        chunk_prompt = create_chunk_summary_prompt(
            base_prompt, combined_text, 1, 1, type_name, item_number, [],
            unit_title=working_item.get('unit_title', '')
        )
        try:
            result = query_json(client, state['cumulative_summary_list'], chunk_prompt, logfile,
                                expected_keys=['summary'], config=config,
                                task_name='stage3.summary.level1_with_references')
        except Exception as e:
            print(f'    ERROR: Container summary failed for {cap_type_name} {item_number}: {e}')
            import traceback
            traceback.print_exc()
            raise ModelError(f'Container summary failed for {cap_type_name} {item_number}: {e}')
        if not result:
            raise ModelError(f'Empty response for container {cap_type_name} {item_number}')
        summary_text = result.get('summary', '')
        if isinstance(summary_text, list):
            summary_text = ' '.join(str(x) for x in summary_text)
        elif not isinstance(summary_text, str):
            summary_text = str(summary_text)
        all_references = result.get('references', [])
        working_item['summary_1'] = summary_text
        working_item['need_ref'] = deduplicate_references(all_references)
        proc.dirty = 1
        print(f'    [OK] {cap_type_name} {item_number} summary_1 complete ({len(summary_text)} chars)')
    else:
        # Pure aggregator: synthesise from sub-unit summaries, passing current context
        prompt = (
            f'Please provide a concise summary of {cap_type_name} {item_number} '
            f'based on the following summaries of its sub-units. '
            f'The summary should capture the overall scope and key themes without '
            f're-iterating which {type_name} is being summarized.\n\n'
        )
        for s in sub_summaries:
            prompt += s + '\n\n'
        prompt += 'Return only the summary text, no preamble.'
        try:
            result = query_text_with_retry(
                client, state['cumulative_summary_list'], prompt, logfile,
                max_tokens=0, max_retries=3, config=config,
                task_name='stage3.summary.container_1'
            )
            summary_text = clean_summary_text(result)
            if summary_text:
                working_item['summary_1'] = summary_text
                proc.dirty = 1
                print(f'  [OK] Container {cap_type_name} {item_number} summary_1 ({len(summary_text)} chars)')
            else:
                print(f'  [WARNING] Empty container summary for {cap_type_name} {item_number}')
                return
        except Exception as e:
            print(f'  [ERROR] Container summary failed for {cap_type_name} {item_number}: {e}')
            import traceback
            traceback.print_exc()
            return

    # Flush and add to cumulative context
    if proc.should_flush_by_time(time_threshold_seconds=10):
        proc.flush()
    if state['first_flag'][0]:
        state['first_flag'][0] = False
        state['cumulative_summary_list'].append(state['cumulative_summary_list_pre'])
    state['cumulative_summary_list'].append(
        '\n' + cap_type_name + ' ' + item_number + ':\n' + working_item['summary_1'] + '\n'
    )
    trim_cumulative_summary_list(state['cumulative_summary_list'], state.get('cumulative_summary_max_chars', 30000), state['cumulative_summary_list_pre'])
    proc.dirty = 1
    state['count'][0] -= 1


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


def _process_item_level1(type_name, type_plural, cap_type_name, item_number, working_item, state):
    """
    Recursively process one operational item for level 1 summary generation.

    Post-order for containers: recurses into all sub-unit descendants first,
    then summarises the container using the then-current cumulative context.
    Leaf items are summarised directly.  Data-table sub-units get structural
    summaries via _generate_table_structure_summary.

    Sub-unit context isolation: each container's sub-units receive a fresh
    cumulative_summary_list (reset to TOC only) so their context does not bleed
    across parent containers.  The parent's list is restored before the
    container itself is summarised.

    Checks state['count'][0] before each AI-generating call; returns immediately
    when the processing limit is reached.

    Stop-tag boundary logic fires only for top-level items (those whose
    _get_parent_key returns None).  Sub-unit context is managed by the
    save/restore above, so the old parent-key switch logic is not needed.
    """
    if state['count'][0] < 1:
        return

    if has_sub_units(working_item):
        # Save parent cumulative context; sub-units use their own fresh list
        saved_cumulative = state['cumulative_summary_list']
        saved_first_flag = state['first_flag'][0]
        state['cumulative_summary_list'] = []
        if state['table_of_contents_cache']:
            state['cumulative_summary_list'].append(state['table_of_contents_cache'])
        state['first_flag'][0] = True

        # Recurse into sub-units first (post-order)
        param_pointer = state['param_pointer']
        for sub_type_key, sub_type_items in working_item['sub_units'].items():
            if state['count'][0] < 1:
                break
            sub_p = _resolve_param_key(param_pointer, sub_type_key)
            if sub_p is None or sub_p.get('operational', 0) != 1:
                continue
            stn = sub_p['name']
            stp = sub_p['name_plural']
            cstn = stn[:1].upper() + stn[1:] if stn else ''
            for sub_key, sub_item in sub_type_items.items():
                if state['count'][0] < 1:
                    break
                _process_item_level1(stn, stp, cstn, sub_key, sub_item, state)

        # Restore parent context; sub-units' context is discarded
        state['cumulative_summary_list'] = saved_cumulative
        state['first_flag'][0] = saved_first_flag
        if state['count'][0] >= 1:
            _summarize_container_level1(type_name, type_plural, cap_type_name,
                                        item_number, working_item, state)
    elif type_name in state['data_table_type_names']:
        # Data-table sub-unit: structural summary, not prose summarisation
        if 'summary_1' not in working_item:
            print(f'  Generating table summary for {item_number}')
            _generate_table_structure_summary(working_item, state['proc'])
            state['proc'].dirty = 1
    else:
        _summarize_leaf_level1(type_name, type_plural, cap_type_name,
                               item_number, working_item, state)

    # Stop-tag boundary logic (top-level items only; sub-unit context handled above)
    if 'context' not in working_item:
        raise InputError(f'level_1_summaries: no context in {type_name} {item_number}.')
    parent_key = _get_parent_key(state['parsed_content'], working_item)
    if parent_key is None:
        org_content_pointer = get_org_pointer(state['parsed_content'], working_item)
        if 'stop_' + type_name in org_content_pointer:
            if org_content_pointer['stop_' + type_name] == item_number:
                state['cumulative_summary_list'] = []
                if state['table_of_contents_cache']:
                    state['cumulative_summary_list'].append(state['table_of_contents_cache'])
                state['first_flag'][0] = True
                top_organization_summaries(state['proc'], 1)


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

    client = proc.clients.get('level1_with_references', proc.client)
    parsed_content = proc.parsed_content
    out_path = proc.out_path
    logfile = proc.logfile
    config = proc.config

    if ('document_information' not in parsed_content.keys() or
        'parameters' not in parsed_content['document_information'].keys() or
        'content' not in parsed_content.keys()):
        raise InputError('level_1_summaries: invalid parsed_content structure.')
        exit(1)

    param_pointer = parsed_content['document_information']['parameters']
    content_pointer = parsed_content['content']
    cumulative_summary_list_pre = '\nFor the context of this request, consider these summarizations of earlier portions of the document:\n'
    table_of_contents_cache = ''
    if not '' == proc.table_of_contents:
        table_of_contents_cache = 'Here is a table of contents for a document that is relevant to this request:\n\n' + proc.table_of_contents + '\n'
    cumulative_summary_list = []
    if table_of_contents_cache:
        cumulative_summary_list.append(table_of_contents_cache)

    # Count items needing summary_1 (leaves with text + all containers)
    total_items = 0
    items_needing_summary = 0
    for _, _, _, _, item in iter_operational_items(parsed_content):
        total_items += 1
        if 'summary_1' not in item and (item.get('text', '') or has_sub_units(item)):
            items_needing_summary += 1
    print(f'  Found {total_items} total operational items, {items_needing_summary} need summary_1')

    # Build set of data-table type names (data_table: 1 flag in parameters)
    data_table_type_names = {p['name'] for p in param_pointer.values()
                             if p.get('data_table') and p.get('is_sub_unit')}

    state = {
        'parsed_content':              parsed_content,
        'param_pointer':               param_pointer,
        'content_pointer':             content_pointer,
        'client':                      client,
        'logfile':                     logfile,
        'config':                      config,
        'proc':                        proc,
        'type_name_list':              proc.type_name_list,
        'data_table_type_names':       data_table_type_names,
        'cumulative_summary_list':     cumulative_summary_list,
        'cumulative_summary_list_pre': cumulative_summary_list_pre,
        'table_of_contents_cache':     table_of_contents_cache,
        'first_flag':                  [True],
        'items_processed':             [0],
        'items_needing_summary':       items_needing_summary,
        'count':                       [count],
        'checkpoint_threshold':        checkpoint_threshold,
        'cumulative_summary_max_chars': get_cumulative_summary_list_max_chars(config),
    }

    for item_type in param_pointer.keys():
        p = param_pointer[item_type]
        if p.get('is_sub_unit', False) or p.get('operational') != 1:
            continue
        item_type_name = p['name']
        item_type_name_plural = p['name_plural']
        if item_type_name_plural not in content_pointer:
            raise InputError(f'level_1_summaries: {item_type_name_plural} not present.')
        cap_item_type_name = item_type_name[:1].upper() + item_type_name[1:] if item_type_name else ''
        for item_number, working_item in content_pointer[item_type_name_plural].items():
            if state['count'][0] < 1:
                break
            _process_item_level1(item_type_name, item_type_name_plural, cap_item_type_name,
                                  item_number, working_item, state)
        if state['count'][0] < 1:
            break

    proc.flush()
    if state['count'][0] < 1:
        print('More processing needed.  Please run again.\n')
        return False
    return True

def _build_level2_tools():
    """Return the tool schema list for the level-2 tool-use path."""
    return [
        {
            "name": "lookup_definition",
            "description": (
                "Look up the definition of a term that is used in the legal document being summarized. "
                "Call this before writing the summary if you need to understand what a key term means."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "term": {
                        "type": "string",
                        "description": "The term to look up (as it appears in the document)",
                    }
                },
                "required": ["term"],
            },
        },
        {
            "name": "lookup_toc_section",
            "description": (
                "Look up a section in the document's table of contents by its identifier "
                "(e.g., section number, article number). Use this to understand the document "
                "structure and context around the section being summarized."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "identifier": {
                        "type": "string",
                        "description": "The section identifier to look up (e.g., '744.17', 'Article 5')",
                    }
                },
                "required": ["identifier"],
            },
        },
    ]


def _build_level2_tool_resolver(parsed_content, working_item, item_type_name, item_number, toc_string):
    """
    Build a tool resolver for the level-2 tool-use path.

    Returns a callable(tool_name, tool_input) -> str that resolves tool calls locally
    using already-loaded definitions and the document table of contents.
    """
    definitions = collect_scoped_definitions(parsed_content, working_item)

    # Group definitions by normalised term for fast lookup
    term_to_defs = {}
    for d in definitions:
        raw = strip_emphasis_marks(d.get('term', '')).lower().strip()
        if raw:
            term_to_defs.setdefault(raw, []).append(d)

    available_term_names = list(term_to_defs.keys())

    def resolver(tool_name, tool_input):
        if tool_name == 'lookup_definition':
            term = tool_input.get('term', '').strip()
            key = strip_emphasis_marks(term).lower()
            matches = term_to_defs.get(key, [])
            if not matches and available_term_names:
                matched_name = fuzzy_match_term(term, available_term_names)
                if matched_name:
                    matches = term_to_defs.get(matched_name, [])
            if not matches:
                return f'No definition found for "{term}".'
            lines = [f'Definition of "{term}":']
            for d in matches:
                value = format_definition_with_source(d, item_type_name, item_number)
                if value:
                    lines.append('  - ' + value)
            return '\n'.join(lines)

        if tool_name == 'lookup_toc_section':
            identifier = tool_input.get('identifier', '').strip()
            if not toc_string or not identifier:
                return 'No table of contents available.'
            toc_lines = toc_string.split('\n')
            for i, line in enumerate(toc_lines):
                if identifier in line:
                    start = max(0, i - 2)
                    end = min(len(toc_lines), i + 4)
                    return '\n'.join(toc_lines[start:end])
            return f'Identifier "{identifier}" not found in table of contents.'

        return f'Unknown tool: {tool_name}'

    return resolver


def _create_level2_tool_use_prompt(proc, item_type_name, item_number, total_chunks) -> str:
    """
    Variant of create_level_2_base_prompt for the tool-use path.
    Replaces the 'Context is provided above' line with tool instructions.
    """
    max_paragraphs = 5 + max(0, total_chunks - 1)
    doc_title = ''
    if ('document_information' in proc.parsed_content
            and 'title' in proc.parsed_content['document_information']):
        doc_title = proc.parsed_content['document_information']['title']

    prompt = ''
    if doc_title:
        prompt += 'I will be asking you about this regulation: ' + doc_title + '\n\n'
    prompt += 'Here is my request.  Please provide a summary of the following portion of the regulation.  '
    prompt += 'The summary should allow a legal practitioner to understand the rights, restrictions, and obligations '
    prompt += f'set forth in this portion.  Please do not exceed {max_paragraphs} paragraphs for the summary '
    prompt += '(shorter is fine).  '
    prompt += f'The summary should not re-iterate which {item_type_name} is being summarized, '
    prompt += 'that will be attached to the summary separately.  '
    prompt += '**IMPORTANT: The summary must not be longer than the text being summarized.**  '
    prompt += 'What you return will be incorporated into a compilation of summaries of the regulation, '
    prompt += 'so it is important that you return nothing beyond the summary itself (no preamble, no commentary about how the '
    prompt += 'task was completed).\n\n'
    prompt += ('You have access to two tools: lookup_definition (to look up the meaning of key terms) '
                'and lookup_toc_section (to look up sections in the table of contents). '
                'Use these tools as needed to understand the text before writing the summary.  '
                'Do NOT summarize the tool results themselves — only summarize the provided text.\n\n')
    return prompt


def _summarize_leaf_level2(type_name, type_plural, cap_type_name, item_number, working_item, state):
    """
    Generate a level 2 summary for a single leaf operational item.
    Extracted from the level_2_summaries main loop.
    """
    parsed_content = state['parsed_content']
    client = state['client']
    logfile = state['logfile']
    config = state['config']
    proc = state['proc']
    checkpoint_threshold = state['checkpoint_threshold']
    table_of_contents_cache = state['table_of_contents_cache']

    # Idempotency: skip if already done
    if 'summary_2' in working_item:
        summary_2_text = working_item['summary_2']
        if isinstance(summary_2_text, list):
            print(f'    WARNING: summary_2 is a list ({len(summary_2_text)} items), converting to string')
            summary_2_text = ' '.join(summary_2_text)
            working_item['summary_2'] = summary_2_text
            proc.dirty = 1
        return

    # Prerequisite
    if 'summary_1' not in working_item:
        raise InputError(f'level_2_summaries: summary_1 missing for {type_name} {item_number}')
        exit(1)

    # Blank item
    if '' == working_item.get('text', ''):
        working_item['summary_2'] = cap_type_name + ' is blank.'
        proc.dirty = 1
        return

    state['items_processed'][0] += 1
    print(f'\n  [{state["items_processed"][0]}/{state["items_needing_summary"]}] Processing: {cap_type_name} {item_number}')

    context_string, conflicts = build_level_2_context(parsed_content, working_item, type_name, item_number)
    if conflicts:
        document_issues_logfile = get_document_issues_logfile(proc.out_path)
        for c in conflicts:
            log_document_issue(
                document_issues_logfile,
                'conflicting_direct_definitions',
                item_type_name=type_name,
                item_number=item_number,
                issue_description=f"Multiple direct definitions in scope for term '{c.get('term','')}'",
                details=c
            )

    text = working_item['text']
    breakpoints = working_item.get('breakpoints', [])
    metadata_suffix = build_metadata_suffix(item_number, working_item, state['content_pointer'], type_plural)
    try:
        chunks = list(chunk_text(text, breakpoints, preferred_length=15000))
        total_chunks = len(chunks)
    except Exception as e:
        raise InputError(f'level_2_summaries: chunking failed for {type_name} {item_number}: {e}')
        exit(1)

    # Decide whether to use the tool-use path (oversized context) or normal path
    tool_use_threshold = get_level2_tool_use_threshold(config)
    context_total_chars = len(context_string) + len(table_of_contents_cache)
    use_tool_path = context_total_chars > tool_use_threshold

    if use_tool_path:
        print(f'    [Tool-use path] Context {context_total_chars} chars exceeds threshold {tool_use_threshold}')
        base_prompt = _create_level2_tool_use_prompt(proc, type_name, item_number, total_chunks)
        cache_prompt_list = []
        level2_tools = _build_level2_tools()
        tool_resolver = _build_level2_tool_resolver(
            parsed_content, working_item, type_name, item_number, table_of_contents_cache
        )
    else:
        base_prompt = create_level_2_base_prompt(proc, type_name, item_number, total_chunks)
        cache_prompt_list = prepare_context_for_caching(context_string, table_of_contents_cache)
        level2_tools = None
        tool_resolver = None

    chunk_summaries = []

    chunk_prefix = working_item.get('chunk_prefix', '')
    for i, chunk in enumerate(chunks):
        if chunk_prefix:
            chunk = chunk_prefix + '\n\n' + chunk
        augmented_chunk = augment_chunk_with_metadata(chunk, metadata_suffix)
        chunk_prompt = create_chunk_summary_prompt(
            base_prompt, augmented_chunk, i + 1, total_chunks,
            type_name, item_number, chunk_summaries,
            unit_title=working_item.get('unit_title', '')
        )
        print(f'    Chunk {i+1}/{total_chunks} of {cap_type_name} {item_number}...')
        try:
            if use_tool_path:
                from utils.api_helpers import query_with_tools
                result = query_with_tools(
                    client, cache_prompt_list, chunk_prompt,
                    level2_tools, tool_resolver, logfile,
                    max_tokens=0, config=config, task_name='stage3.summary.level2'
                )
            else:
                result = query_text_with_retry(client, cache_prompt_list, chunk_prompt, logfile,
                                               max_tokens=0, max_retries=3, config=config,
                                               task_name='stage3.summary.level2')
        except ModelError as e:
            raise ModelError(f'level_2_summaries: Failed to get response for {type_name} {item_number}, chunk {i+1} after retries: {e}')
            exit(1)
        except Exception as e:
            raise ModelError(f'level_2_summaries: Unexpected error for {type_name} {item_number}, chunk {i+1}: {e}')
            exit(1)
        summary_text = clean_summary_text(result)
        if not summary_text:
            if not use_tool_path:
                from utils.api_cache import remove_cached_response
                full_cache = ''.join(cache_prompt_list)
                model_name = getattr(client, 'model', 'unknown')
                remove_cached_response(full_cache, chunk_prompt, model_name, 0)
                print(f'    [Cache cleanup] Removed empty summary cache entry for {type_name} {item_number}, chunk {i+1}')
            raise ModelError(f'level_2_summaries: Empty summary response for {type_name} {item_number}, chunk {i+1}')
            exit(1)
        chunk_summaries.append(summary_text)
        print(f'    Chunk {i+1} complete ({len(chunk_summaries[-1])} chars)')

    # Synthesize final summary
    if total_chunks == 1:
        final_summary = chunk_summaries[0]
    else:
        try:
            max_paragraphs = 5 + max(0, total_chunks - 1)
            synthesis_prompt = f"""Please create a final, cohesive summary of {type_name} {item_number}
by synthesizing these summaries of its parts. The summary should be up to {max_paragraphs} paragraphs and should allow
a legal practitioner to understand the rights, restrictions, and obligations set forth in this portion.
The summary should not re-iterate which {type_name} is being summarized, that will be attached to the summary separately.
**IMPORTANT: The final summary must not be longer than the combined length of the part summaries being synthesized.**

"""
            for idx, summary in enumerate(chunk_summaries, 1):
                synthesis_prompt += f"Part {idx}: {summary}\n\n"
            synthesis_prompt += """
Create a unified summary that captures the overall content without referencing part numbers.
Return only the summary text, no preamble."""
            result = query_text_with_retry(client, [], synthesis_prompt, logfile,
                                           max_tokens=0, max_retries=3, config=config,
                                           task_name='stage3.summary.level2.synthesis')
            final_summary = clean_summary_text(result)
        except ModelError as e:
            raise ModelError(f'level_2_summaries: synthesis failed for {type_name} {item_number} after retries: {e}')
            exit(1)
        except Exception as e:
            raise ModelError(f'level_2_summaries: unexpected synthesis error for {type_name} {item_number}: {e}')
            exit(1)

    if not final_summary:
        from utils.api_cache import remove_cached_response
        model_name = getattr(client, 'model', 'unknown')
        remove_cached_response('', synthesis_prompt, model_name, 0)
        print(f'    [Cache cleanup] Removed empty synthesis cache entry for {type_name} {item_number}')
        raise ModelError(f'level_2_summaries: Final summary is empty for {type_name} {item_number}')
        exit(1)

    try:
        working_item['summary_2'] = final_summary
        print(f'    [OK] {cap_type_name} {item_number} summary_2 complete ({len(final_summary)} chars)')
        operational_counts, organizational_counts = count_stage_3_progress(parsed_content)
        update_stage_3_progress(parsed_content, operational_counts, organizational_counts)
        proc.dirty = 1
        if proc.should_flush_by_time(time_threshold_seconds=10):
            proc.flush()
            print(f'  [Immediate flush] Saved {cap_type_name} {item_number} summary_2 to prevent data loss')
        state['count'][0] -= 1
        if state['items_processed'][0] % checkpoint_threshold == 0:
            proc.flush()
            print(f'  [Checkpoint] Progress saved: {operational_counts["summary_2"]}/{operational_counts["total"]} operational items with summary_2')
        else:
            should_flush = False
            if state['items_processed'][0] % 5 == 0:
                should_flush = True
            elif proc.should_flush_by_time(time_threshold_seconds=30):
                should_flush = True
                print(f'  [Time-based flush] Flushing after {int(time_module.time() - proc.last_flush_time)} seconds')
            if should_flush:
                proc.flush()
    except Exception as e:
        print(f'  [ERROR] Exception after setting summary_2 for {cap_type_name} {item_number}: {e}')
        print(f'  [SAFETY] Flushing to save {cap_type_name} {item_number} summary_2 before re-raising exception...')
        try:
            proc.flush()
        except Exception as flush_error:
            print(f'  [CRITICAL] Failed to flush after exception: {flush_error}')
        raise


def _summarize_container_level2(type_name, type_plural, cap_type_name, item_number, working_item, state):
    """
    Generate a level 2 summary for a container item.
    Called after all sub-units have been recursively processed.

    If the container has own text: uses build_level_2_context + level_2 prompt style
    (single chunk, same as leaf items but combined with sub-unit summary_2 block).
    If the container is a pure aggregator: synthesises from sub-unit summary_2 values
    with TOC context.
    """
    # Idempotency
    if 'summary_2' in working_item and working_item['summary_2']:
        summary_2_text = working_item['summary_2']
        if isinstance(summary_2_text, list):
            summary_2_text = ' '.join(str(x) for x in summary_2_text)
            working_item['summary_2'] = summary_2_text
            state['proc'].dirty = 1
        return

    # Prerequisite
    if 'summary_1' not in working_item:
        raise InputError(f'level_2_summaries: summary_1 missing for container {type_name} {item_number}')
        exit(1)

    # Collect sub-unit summary_2 values.
    # Data-table sub-units only ever have summary_1 (level-2 processing is skipped for them);
    # fall back to summary_1 for those types so the container is not blocked.
    sub_summaries = []
    all_present = True
    data_table_type_names = state['data_table_type_names']
    for sub_type_key, sub_type_items in working_item['sub_units'].items():
        sub_p = _resolve_param_key(state['param_pointer'], sub_type_key)
        is_data_table = (sub_p['name'] in data_table_type_names) if sub_p else False
        for sub_key, sub_item in sub_type_items.items():
            if 'summary_2' in sub_item and sub_item['summary_2']:
                sub_summaries.append(f'{sub_key}: {sub_item["summary_2"]}')
            elif is_data_table and 'summary_1' in sub_item and sub_item['summary_1']:
                # Data-table sub-units receive a structural description (what the table contains,
                # its columns, purpose) as summary_1 via _generate_table_structure_summary.
                # They have no summary_2 — the structural description is the complete summary;
                # there is no prose content to analyse at a deeper level.  Use summary_1 here.
                sub_summaries.append(f'{sub_key}: {sub_item["summary_1"]}')
            else:
                all_present = False
                break
        if not all_present:
            break

    if not all_present or not sub_summaries:
        print(f'  [WARNING] Container {cap_type_name} {item_number}: missing sub-unit summary_2, skipping')
        return

    parsed_content = state['parsed_content']
    client = state['client']
    logfile = state['logfile']
    config = state['config']
    proc = state['proc']
    table_of_contents_cache = state['table_of_contents_cache']
    container_text = working_item.get('text', '').strip()

    state['items_processed'][0] += 1
    print(f'\n  [{state["items_processed"][0]}/{state["items_needing_summary"]}] Processing container: {cap_type_name} {item_number}')

    if container_text:
        # Container has own text: level_2 prompt style with definition context, single chunk
        context_string, conflicts = build_level_2_context(parsed_content, working_item, type_name, item_number)
        if conflicts:
            document_issues_logfile = get_document_issues_logfile(proc.out_path)
            for c in conflicts:
                log_document_issue(
                    document_issues_logfile,
                    'conflicting_direct_definitions',
                    item_type_name=type_name,
                    item_number=item_number,
                    issue_description=f"Multiple direct definitions in scope for term '{c.get('term','')}'",
                    details=c
                )
        tool_use_threshold = get_level2_tool_use_threshold(config)
        context_total_chars = len(context_string) + len(table_of_contents_cache)
        use_tool_path = context_total_chars > tool_use_threshold
        if use_tool_path:
            print(f'    [Tool-use path] Context {context_total_chars} chars exceeds threshold {tool_use_threshold}')
            cache_prompt_list = []
            base_prompt = _create_level2_tool_use_prompt(proc, type_name, item_number, 1)
            level2_tools = _build_level2_tools()
            tool_resolver = _build_level2_tool_resolver(
                parsed_content, working_item, type_name, item_number, table_of_contents_cache
            )
        else:
            cache_prompt_list = prepare_context_for_caching(context_string, table_of_contents_cache)
            base_prompt = create_level_2_base_prompt(proc, type_name, item_number, 1)
            level2_tools = None
            tool_resolver = None
        sub_block = '\n[Subsidiary unit summaries:]\n' + '\n'.join(sub_summaries)
        combined_text = container_text + '\n' + sub_block
        chunk_prompt = create_chunk_summary_prompt(
            base_prompt, combined_text, 1, 1, type_name, item_number, [],
            unit_title=working_item.get('unit_title', '')
        )
        try:
            if use_tool_path:
                from utils.api_helpers import query_with_tools
                result = query_with_tools(
                    client, cache_prompt_list, chunk_prompt,
                    level2_tools, tool_resolver, logfile,
                    max_tokens=0, config=config, task_name='stage3.summary.level2'
                )
            else:
                result = query_text_with_retry(client, cache_prompt_list, chunk_prompt, logfile,
                                               max_tokens=0, max_retries=3, config=config,
                                               task_name='stage3.summary.level2')
        except Exception as e:
            raise ModelError(f'Container level_2 summary failed for {cap_type_name} {item_number}: {e}')
        final_summary = clean_summary_text(result)
        if not final_summary:
            raise ModelError(f'Empty level_2 container summary for {cap_type_name} {item_number}')
    else:
        # Pure aggregator: synthesise from sub-unit summary_2 with TOC context
        cache_prompt_list = prepare_context_for_caching('', table_of_contents_cache)
        prompt = (
            f'Please provide a concise summary of {cap_type_name} {item_number} '
            f'based on the following summaries of its sub-units. '
            f'The summary should allow a legal practitioner to understand the rights, '
            f'restrictions, and obligations set forth in this portion. '
            f'Do not re-iterate which {type_name} is being summarized.\n\n'
        )
        for s in sub_summaries:
            prompt += s + '\n\n'
        prompt += 'Return only the summary text, no preamble.'
        try:
            result = query_text_with_retry(client, cache_prompt_list, prompt, logfile,
                                           max_tokens=0, max_retries=3, config=config,
                                           task_name='stage3.summary.container_2')
        except Exception as e:
            raise ModelError(f'Container level_2 aggregation failed for {cap_type_name} {item_number}: {e}')
        final_summary = clean_summary_text(result)
        if not final_summary:
            print(f'  [WARNING] Empty container level_2 summary for {cap_type_name} {item_number}')
            return

    working_item['summary_2'] = final_summary
    print(f'  [OK] Container {cap_type_name} {item_number} summary_2 ({len(final_summary)} chars)')
    proc.dirty = 1
    if proc.should_flush_by_time(time_threshold_seconds=10):
        proc.flush()
    state['count'][0] -= 1


def _process_item_level2(type_name, type_plural, cap_type_name, item_number, working_item, state):
    """
    Recursively process one operational item for level 2 summary generation.

    Post-order for containers: recurses into all sub-unit descendants first,
    fires top_organization_summaries after sub-units are done (mirrors the
    parent-switch org-summary trigger from the old flat loop), then summarises
    the container itself.

    Data-table sub-units are skipped (they have only summary_1, not summary_2).

    Stop-tag boundary logic fires only for top-level items.
    """
    if state['count'][0] < 1:
        return

    if has_sub_units(working_item):
        # Recurse into sub-units first
        param_pointer = state['param_pointer']
        for sub_type_key, sub_type_items in working_item['sub_units'].items():
            if state['count'][0] < 1:
                break
            sub_p = _resolve_param_key(param_pointer, sub_type_key)
            if sub_p is None or sub_p.get('operational', 0) != 1:
                continue
            stn = sub_p['name']
            stp = sub_p['name_plural']
            cstn = stn[:1].upper() + stn[1:] if stn else ''
            for sub_key, sub_item in sub_type_items.items():
                if state['count'][0] < 1:
                    break
                _process_item_level2(stn, stp, cstn, sub_key, sub_item, state)

        # Trigger org summaries after all sub-units are done (mirrors parent-switch logic)
        top_organization_summaries(state['proc'], 2)

        if state['count'][0] >= 1:
            _summarize_container_level2(type_name, type_plural, cap_type_name,
                                        item_number, working_item, state)

    elif type_name in state['data_table_type_names']:
        pass  # Data-table sub-units have only summary_1; no level_2 processing

    else:
        _summarize_leaf_level2(type_name, type_plural, cap_type_name,
                               item_number, working_item, state)

    # Stop-tag boundary logic (top-level items only)
    if 'context' not in working_item:
        raise InputError(f'level_2_summaries: no context in {type_name} {item_number}.')
        exit(1)
    parent_key = _get_parent_key(state['parsed_content'], working_item)
    if parent_key is None:
        org_content_pointer = get_org_pointer(state['parsed_content'], working_item)
        if 'stop_' + type_name in org_content_pointer:
            if org_content_pointer['stop_' + type_name] == item_number:
                top_organization_summaries(state['proc'], 2)


def level_2_summaries(proc, count=30, checkpoint_threshold=30) -> bool:
    """
    Generate level 2 summaries for operational items in the document.

    Creates detailed, context-aware summaries using:
    - Scope-aware definition collection
    - Referenced section context
    - Text chunking for long documents
    - Enhanced summarization (5+ paragraphs)

    Args:
        proc: SummaryProcessor object
        count (int): Maximum number of items to process in this run
    """
    client = proc.clients.get('level2', proc.client)
    parsed_content = proc.parsed_content
    out_path = proc.out_path
    logfile = proc.logfile
    config = proc.config

    if ('document_information' not in parsed_content.keys() or
        'parameters' not in parsed_content['document_information'].keys() or
        'content' not in parsed_content.keys()):
        raise InputError('level_2_summaries: invalid parsed_content structure.')
        exit(1)

    param_pointer = parsed_content['document_information']['parameters']
    content_pointer = parsed_content['content']
    table_of_contents_cache = ''
    if proc.table_of_contents:
        table_of_contents_cache = 'Here is a table of contents for a document that is relevant to this request:\n\n' + proc.table_of_contents + '\n'

    # Count items needing summary_2 (leaves with text + all containers)
    total_items = 0
    items_needing_summary = 0
    for _, _, _, _, item in iter_operational_items(parsed_content):
        total_items += 1
        if has_sub_units(item):
            if 'summary_2' not in item:
                items_needing_summary += 1
        elif 'summary_2' not in item and item.get('text', ''):
            items_needing_summary += 1
    print(f'  Found {total_items} total operational items, {items_needing_summary} need summary_2')

    data_table_type_names = {p['name'] for p in param_pointer.values()
                             if p.get('data_table') and p.get('is_sub_unit')}

    state = {
        'parsed_content':        parsed_content,
        'param_pointer':         param_pointer,
        'content_pointer':       content_pointer,
        'client':                client,
        'logfile':               logfile,
        'config':                config,
        'proc':                  proc,
        'data_table_type_names': data_table_type_names,
        'table_of_contents_cache': table_of_contents_cache,
        'items_processed':       [0],
        'items_needing_summary': items_needing_summary,
        'count':                 [count],
        'checkpoint_threshold':  checkpoint_threshold,
    }

    for item_type in param_pointer.keys():
        p = param_pointer[item_type]
        if p.get('is_sub_unit', False) or p.get('operational') != 1:
            continue
        item_type_name = p['name']
        item_type_name_plural = p['name_plural']
        if item_type_name_plural not in content_pointer:
            continue
        cap_item_type_name = item_type_name[:1].upper() + item_type_name[1:] if item_type_name else ''
        for item_number, working_item in content_pointer[item_type_name_plural].items():
            if state['count'][0] < 1:
                break
            _process_item_level2(item_type_name, item_type_name_plural, cap_item_type_name,
                                  item_number, working_item, state)
        if state['count'][0] < 1:
            break

    proc.flush()
    if state['count'][0] < 1:
        print(f'\n  Reached processing limit ({state["items_processed"][0]} items). More processing needed.')
        return False
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

