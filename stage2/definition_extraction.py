"""
Definition extraction functions for identifying and extracting defined terms from legal text.

This module provides functions for extracting defined terms from operational items
in legal documents using AI model analysis.
"""
# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License

from utils import (
    iter_operational_items,
    query_json,
    build_defined_terms_prompt,
    build_definition_prompt,
    build_scope_prompt,
    chunk_text,
)
from utils.processing_status import update_stage_2_progress
from utils.text_processing import strip_emphasis_marks
from utils.config import create_client_for_task
from .processor import DefinitionsProcessor

_STAGE2_CHUNK_SIZE = 15000  # preferred chunk length (consistent with Stage 3)


def _build_item_cache_prompt(item_type_name, cap_item_type_name, item_number, chunk_prefix, text):
    """Build the cache prompt for one chunk of item text."""
    cp = ('You will be asked about definitions within this ' + item_type_name +
          ' from a larger statute or legal document:\n\n***Begin ' + item_type_name + '***\n')
    cp += cap_item_type_name + ' ' + item_number + ':\n'
    if chunk_prefix:
        cp += chunk_prefix + '\n\n'
    cp += text
    cp += '\n***End ' + item_type_name + '***\n\n'
    return cp


def find_defined_terms(proc: DefinitionsProcessor, count=30):
    """
    Extract defined terms from operational items in the document.
    
    This function processes each operational item (sections, articles, etc.) to identify
    and extract defined terms using AI model analysis. It handles both direct definitions
    and indirect references to other sections.
    
    Args:
        proc (DefinitionsProcessor): The processor instance containing document context
        count (int): Maximum number of items to process in this run
        
    The function:
    1. Iterates through operational items that haven't been processed yet
    2. Uses AI model to identify defined terms in each item's text
    3. Extracts scope information and indirect references
    4. Stores results in the item's 'defined_terms' field
    5. Updates progress and flushes to disk periodically
    """
    # Get total items and current progress for tracking
    proc_status = proc.parsed_content.get('document_information', {}).get('processing_status', {})
    stage_2_progress = proc_status.get('stage_2_progress', {})
    total_items = stage_2_progress.get('total_items', 0)
    initial_processed = stage_2_progress.get('processed_items', 0)

    # Track items visited (all items, including skipped) and items needing API calls
    total_items_visited = 0
    items_processed_in_run = 0  # Only items that made API calls

    # Flush every 5 items to keep progress visible (or every item if total is small)
    flush_interval = max(1, min(5, total_items // 4)) if total_items > 0 else 5

    # Create task-specific clients if config is available
    # This allows for model routing based on task-specific assignments in config.json
    if proc.config:
        terms_client = create_client_for_task(proc.config, 'stage2.definitions.extract_terms')
        definition_client = create_client_for_task(proc.config, 'stage2.definitions.extract_definition')
        scope_client = create_client_for_task(proc.config, 'stage2.definitions.extract_scope')

        # Check if a special model is configured for scope extraction with indirect references
        # This allows using a more capable model for complex indirect definitions
        model_assignments = proc.config.get('model_assignments', {})
        has_indirect_scope_model = 'stage2.definitions.extract_scope_with_indirect' in model_assignments
        if has_indirect_scope_model:
            scope_with_indirect_client = create_client_for_task(proc.config, 'stage2.definitions.extract_scope_with_indirect')
        else:
            scope_with_indirect_client = None
    else:
        # Fallback to default client for backward compatibility
        terms_client = proc.client
        definition_client = proc.client
        scope_client = proc.client
        scope_with_indirect_client = None
        has_indirect_scope_model = False

    # Build set of data-table type names (data_table: 1 flag in parameters).
    # These sub-units contain tabular rows, not legal prose, so definition extraction is skipped.
    _params = proc.parsed_content['document_information']['parameters']
    data_table_type_names = {p['name'] for p in _params.values()
                             if p.get('data_table') and p.get('is_sub_unit')}

    # Get potential defined terms for each substantive portion of the document.
    for item_type_name, item_type_name_plural, cap_item_type_name, item_number, working_item in iter_operational_items(proc.parsed_content):
        total_items_visited += 1
        if '' == working_item.get('text', ''): # If, for whatever reason, there is no text, there is nothing to inspect for definitions.  Mark and move on.
            if 'defined_terms' not in working_item:
                working_item['defined_terms'] = []
                proc.dirty = 1
            continue
        if item_type_name in data_table_type_names:  # data table sub-units contain rows, not definitions; skip.
            working_item.setdefault('defined_terms', [])
            continue
        if 'defined_terms' in working_item: # Already processed; skip
            continue
        # Split into chunks using breakpoints (chunk_text yields full text as a single chunk
        # when there are no breakpoints or the text is short — no threshold guard needed).
        chunk_prefix = working_item.get('chunk_prefix', '')
        item_text = working_item['text']
        breakpoints = working_item.get('breakpoints', [])
        chunks = list(chunk_text(item_text, breakpoints, preferred_length=_STAGE2_CHUNK_SIZE))
        chunk_caches = [
            _build_item_cache_prompt(item_type_name, cap_item_type_name, item_number, chunk_prefix, c)
            for c in chunks
        ]

        # extract_terms: one call per chunk; record which chunk each term came from.
        # First occurrence wins — avoids duplicates on the rare chance the same term
        # name appears in output from multiple chunks.
        term_to_chunk_idx = {}
        prompt = build_defined_terms_prompt(item_type_name)
        for chunk_idx, cache in enumerate(chunk_caches):
            terms_object = query_json(terms_client, [cache], prompt, proc.logfile, config=proc.config, task_name='stage2.definitions.extract_terms')
            for def_term in terms_object:
                if isinstance(def_term, str) and def_term:
                    def_term = strip_emphasis_marks(def_term)
                    if def_term and def_term not in term_to_chunk_idx:
                        term_to_chunk_idx[def_term] = chunk_idx

        if not 'defined_terms' in working_item.keys():
            working_item['defined_terms'] = []
            proc.dirty = 1

        # Per-term extraction: use the cache for the chunk where the term was found.
        # extract_definition and extract_scope share the same cached chunk context.
        for def_term, chunk_idx in term_to_chunk_idx.items():
            cache = chunk_caches[chunk_idx]
            prompt = build_definition_prompt(def_term, item_type_name, proc.type_list_or_string)
            definition_object = query_json(definition_client, [cache], prompt, proc.logfile, config=proc.config, task_name='stage2.definitions.extract_definition')
            if (len(definition_object) > 0 and
            isinstance(definition_object, list) and
            isinstance(definition_object[0], dict) and
            'definition' in definition_object[0].keys()):
                definition = definition_object[0]['definition']
                indirect = ''
                if 'indirect' in definition_object[0].keys():
                    indirect = definition_object[0]['indirect']

                # Determine which client and task name to use for scope extraction
                # Use specialized model for definitions with indirect references if configured
                if has_indirect_scope_model and indirect and indirect.strip():
                    # Use the indirect-specific model
                    active_scope_client = scope_with_indirect_client
                    active_scope_task = 'stage2.definitions.extract_scope_with_indirect'
                else:
                    # Use the regular scope extraction model
                    active_scope_client = scope_client
                    active_scope_task = 'stage2.definitions.extract_scope'

                prompt = build_scope_prompt(def_term, definition, item_type_name, proc.type_list_or_string, proc.org_item_name_string)
                scope_object = query_json(active_scope_client, [cache], prompt, proc.logfile, config=proc.config, task_name=active_scope_task)
                scope = ''
                if (len(scope_object) > 0 and
                    isinstance(scope_object, list) and
                    isinstance(scope_object[0], str) and
                    len(scope_object[0]) > 0):
                        scope = scope_object[0]
                print(def_term + ': ' + definition + ': ' + scope)
                def_kind = 'direct'
                if isinstance(definition_object[0], dict) and 'def_kind' in definition_object[0].keys():
                    if isinstance(definition_object[0]['def_kind'], str) and definition_object[0]['def_kind'].strip() != '':
                        def_kind = definition_object[0]['def_kind'].strip().lower()
                        if def_kind not in ['direct', 'elaboration']:
                            def_kind = 'direct'
                def_dict = {'term': def_term, 'value': definition, 'indirect': indirect, 'scope': scope, 'def_kind': def_kind}
                working_item['defined_terms'].append(def_dict)
                proc.dirty = 1
        
        # Item has been processed (either found definitions or marked as empty)
        items_processed_in_run += 1
        current_processed = initial_processed + items_processed_in_run
        
        # Update progress every flush_interval items or at the end
        if items_processed_in_run % flush_interval == 0 or count <= 1:
            update_stage_2_progress(proc.parsed_content, total_items, current_processed, 'extracting_definitions', set_complete=False)
            proc.dirty = 1
            proc.flush()
        
        count = count - 1
        if count < 1:
            break
    
    # Final progress update and flush.
    # Use set_complete=False so stage_2_complete is only set by process_file_stage_2
    # after ALL Stage 2 sub-functions have run.
    if count >= 1:
        # Loop completed without hitting count limit — all items were visited.
        # Report total_items as processed (extraction complete), but don't mark stage done.
        update_stage_2_progress(proc.parsed_content, total_items, total_items, 'extracting_definitions', set_complete=False)
        proc.dirty = 1
    elif total_items_visited > 0:
        # Hit count limit — partial pass. Report actual items visited so far.
        final_processed = min(initial_processed + total_items_visited, total_items - 1)
        update_stage_2_progress(proc.parsed_content, total_items, final_processed, 'extracting_definitions', set_complete=False)
        proc.dirty = 1
    proc.flush()
    if count < 1:
        print('More processing needed.  Please run again.\n')
        exit(0)

