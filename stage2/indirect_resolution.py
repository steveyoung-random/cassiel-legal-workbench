"""
Indirect definition resolution functions.

This module provides functions for processing and resolving indirect definitions,
including finding definitions at target locations and removing definitions.
"""
# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License

from collections import defaultdict
from utils import (
    iter_operational_items,
    iter_definitions,
    canonical_org_types,
    get_document_issues_logfile,
    log_document_issue,
    lookup_item,
    query_json,
    build_definition_construction_prompt,
    InputError,
    ModelError,
)
from utils.config import create_client_for_task
from .processor import DefinitionsProcessor


def construct_definition_from_target(proc, term, target_loc_type, target_loc_number, target_text, cache_prompt, client=None):
    """
    Construct a definition from target location text.

    Args:
        proc (DefinitionsProcessor): The processor instance
        term (str): The term to construct a definition for
        target_loc_type (str): Type of the target location
        target_loc_number (str): Number/identifier of the target location
        target_text (str): The target location text (not used directly, already in cache_prompt)
        cache_prompt (str): Cached prompt containing the target text (text is already in cache)
        client: Optional AI client for task-specific model routing

    Returns:
        str or None: Constructed definition if found, None otherwise

    Raises:
        ModelError: If AI model fails to respond
    """
    # Build construction prompt (target_text is already in cache_prompt, so we don't need to include it)
    prompt = build_definition_construction_prompt(term, target_loc_type, target_loc_number)
    # Note: target_text is already in cache_prompt, so we just use the cache

    # Use provided client or fall back to default
    active_client = client if client is not None else proc.client

    try:
        result = query_json(active_client, [cache_prompt], prompt, proc.logfile,
                           config=proc.config, task_name='stage2.definitions.construct_from_target')
        
        # Parse result (expect {"term": "...", "value": "..."} or {})
        if isinstance(result, dict):
            if 'term' in result and 'value' in result:
                constructed_def = result['value'].strip()
                if constructed_def and constructed_def.lower() != term.lower():
                    return constructed_def
        
        return None
    except Exception as e:
        # Fail fast on errors
        raise ModelError(f"Failed to construct definition for term '{term}' from {target_loc_type} {target_loc_number}: {e}")


def find_definition_at_location(parsed_content, loc_type, loc_number, term):
    """
    Find a specific definition at a given location.
    
    Args:
        parsed_content (dict): The document content being processed
        loc_type (str): Type of the location (e.g., 'section', 'article')
        loc_number (str): Number/identifier of the location
        term (str): The term to look for
        
    Returns:
        dict: The definition if found, None otherwise
    """
    # First, check if the location exists in the content
    if 'content' not in parsed_content:
        raise InputError(f"Document missing 'content' section when trying to find definition for '{term}' at {loc_type} {loc_number}")
        
    # Get the plural form of the location type
    try:
        loc_type_singular, loc_type_plural = canonical_org_types(loc_type)
    except:
        # If canonical_org_types fails, try to construct the plural
        loc_type_plural = loc_type + 's'
    
    # Look up the location item (handles both top-level and sub-unit items)
    location_item = lookup_item(parsed_content, loc_type_plural, loc_number)
    if location_item is None:
        return None
    
    if 'defined_terms' in location_item:
        # Import normalization function for consistent term comparison
        from .definition_management import _normalize_term_for_comparison
        normalized_search_term = _normalize_term_for_comparison(term)
        
        for def_entry in location_item['defined_terms']:
            def_term = def_entry.get('term', '')
            normalized_def_term = _normalize_term_for_comparison(def_term)
            if normalized_def_term == normalized_search_term:
                return def_entry
    
    # Note: We don't check ext_definitions here because those are definitions
    # that were defined elsewhere and migrated to this location. We're looking
    # for the original definition at this location.
    
    return None


def remove_definition(proc, def_entry, org_context, operational_context, logfile_path=None, previous_definition=None, reason=''):
    """
    Remove a definition from its location in the document.
    
    Args:
        proc (DefinitionsProcessor): The processor instance
        def_entry (dict): The definition entry to remove
        org_context (list): Organizational context (list of dicts) if definition is in org structure
        operational_context (dict): Operational context (dict with source_type/source_number) if definition is in operational item
        logfile_path (str, optional): Path to document issues logfile for logging the removal
        previous_definition (str, optional): The previous definition value before removal (for logging)
        reason (str, optional): Reason for removal (for logging)
    """
    if 'term' not in def_entry:
        raise InputError(f"Cannot remove definition entry missing 'term' field: {def_entry}")
    
    term = def_entry['term']
    parsed_content = proc.parsed_content
    removed = False
    
    # Get the current definition value before removal (if not provided)
    if previous_definition is None:
        previous_definition = def_entry.get('value', '').strip()
    
    # Import normalization function for consistent term comparison
    from .definition_management import _normalize_term_for_comparison
    normalized_term = _normalize_term_for_comparison(term)
    
    # Case 1: Definition is in document_definitions
    if not org_context and not operational_context:
        if 'document_information' in parsed_content and 'document_definitions' in parsed_content['document_information']:
            definitions_list = parsed_content['document_information']['document_definitions']
            original_length = len(definitions_list)
            definitions_list[:] = [
                d for d in definitions_list 
                if _normalize_term_for_comparison(d.get('term', '')) != normalized_term or d is not def_entry
            ]
            if len(definitions_list) < original_length:
                removed = True
    
    # Case 2: Definition is in organizational structure
    elif org_context:
        # Navigate to the organizational location
        # org_context is a list of dicts with 'name' and 'number' keys: [{"name": "title", "number": "2"}, ...]
        org_content = parsed_content['document_information']['organization']['content']
        for level in org_context:
            if 'name' not in level or 'number' not in level:
                raise InputError(f"Invalid org_context format: {level}. Expected dict with 'name' and 'number' keys.")
            level_name = level['name']
            level_number = level['number']
            
            if level_name not in org_content:
                raise InputError(f"Cannot navigate to organizational location: {org_context}. Missing level '{level_name}' in organization structure.")
            org_content = org_content[level_name]
            
            if level_number not in org_content:
                raise InputError(f"Cannot navigate to organizational location: {org_context}. Missing '{level_name}' '{level_number}' in organization structure.")
            org_content = org_content[level_number]
        
        # Remove from unit_definitions
        if 'unit_definitions' in org_content:
            original_length = len(org_content['unit_definitions'])
            org_content['unit_definitions'][:] = [
                d for d in org_content['unit_definitions'] 
                if _normalize_term_for_comparison(d.get('term', '')) != normalized_term or d is not def_entry
            ]
            if len(org_content['unit_definitions']) < original_length:
                removed = True
    
    # Case 3: Definition is in operational item (ext_definitions or defined_terms)
    elif operational_context:
        source_type = operational_context.get('source_type')
        source_number = operational_context.get('source_number')
        if source_type and source_number:
            source_type, source_type_plural = canonical_org_types(source_type)
            item = lookup_item(parsed_content, source_type_plural, source_number)
            if item is not None:
                # Try to remove from ext_definitions
                if 'ext_definitions' in item:
                    original_length = len(item['ext_definitions'])
                    item['ext_definitions'][:] = [
                        d for d in item['ext_definitions']
                        if _normalize_term_for_comparison(d.get('term', '')) != normalized_term or d is not def_entry
                    ]
                    if len(item['ext_definitions']) < original_length:
                        removed = True

                # Also try to remove from defined_terms (for definitions scoped to their own unit)
                if 'defined_terms' in item:
                    original_length = len(item['defined_terms'])
                    item['defined_terms'][:] = [
                        d for d in item['defined_terms']
                        if _normalize_term_for_comparison(d.get('term', '')) != normalized_term or d is not def_entry
                    ]
                    if len(item['defined_terms']) < original_length:
                        removed = True
    
    # Mark as dirty if a definition was actually removed
    if removed:
        proc.dirty = 1
        
        # Log the removal if logfile_path is provided
        if logfile_path:
            # Determine location information for logging
            item_type_name = None
            item_number = None
            location_description = ''
            
            if operational_context:
                item_type_name = operational_context.get('source_type', '')
                item_number = operational_context.get('source_number', '')
                if item_type_name and item_number:
                    location_description = f"{item_type_name} {item_number}"
            elif org_context:
                # Build location description from org context
                location_parts = []
                for level in org_context:
                    if 'name' in level and 'number' in level:
                        location_parts.append(f"{level['name']} {level['number']}")
                if location_parts:
                    location_description = ', '.join(location_parts)
                    # Use the last level as item_type/item_number
                    last_level = org_context[-1]
                    item_type_name = last_level.get('name', '')
                    item_number = last_level.get('number', '')
            else:
                location_description = 'document-wide'
            
            # Build issue description
            issue_description = f"Removed definition for term '{term}'"
            if reason:
                issue_description += f": {reason}"
            if location_description:
                issue_description += f" (location: {location_description})"
            
            # Log the issue
            log_document_issue(
                logfile_path,
                'definition_removal',
                item_type_name=item_type_name,
                item_number=item_number,
                issue_description=issue_description,
                term=term,
                previous_definition=previous_definition if previous_definition else '(empty or missing)',
                location=location_description
            )


def process_indirect_definitions(proc):
    """
    Process indirect definitions by resolving their locations and removing invalid ones.
    
    This function goes through all existing definitions in 'defined_terms' sections of operational units,
    and for any where the 'indirect' string is not empty, uses the proc.get_indirect function to get
    the indirect_loc_type and indirect_loc_number. If none are available, the definition is deleted.
    
    Args:
        proc (DefinitionsProcessor): The processor instance containing document context
        
    The function:
    1. Iterates through all operational items with defined_terms
    2. For each definition with an 'indirect' string, attempts to resolve the location
    3. Stores indirect_loc_type and indirect_loc_number in the definition entry
    4. Moves information to store of unresolved indirect references if no location can be resolved
    """
    print("Processing indirect definitions...")
    
    # Iterate through all operational items
    for item_type_name, item_type_name_plural, cap_item_type_name, item_number, working_item in iter_operational_items(proc.parsed_content):
        defined_term_list = working_item.get('defined_terms', [])
        if not defined_term_list:
            continue
            
        # Process each definition in this item
        processed_indirect = False
        for def_term in defined_term_list:
            # Check if this definition has an indirect reference
            if 'indirect' in def_term.keys() and not '' == def_term['indirect']:
                # Skip if already processed:
                # - Resolved: has indirect_loc_type and indirect_loc_number
                # - Unresolved: has external_reference (marked as processed but couldn't be resolved)
                if ('indirect_loc_type' in def_term and 'indirect_loc_number' in def_term) or \
                   'external_reference' in def_term:
                    continue
                
                processed_indirect = True
                indirect_string = def_term['indirect']
                
                # Try to resolve the indirect reference
                indirect_info = proc.get_indirect(indirect_string)
                
                if indirect_info and 'indirect_loc_type' in indirect_info.keys() and 'indirect_loc_number' in indirect_info.keys():
                    if 'term' not in def_term:
                        raise InputError(f"Definition entry missing 'term' field: {def_term}")
                    # Guard: if the AI resolved to the same item where the definition was found,
                    # it made a self-reference error — treat as external reference instead.
                    if (indirect_info['indirect_loc_type'] == item_type_name and
                            indirect_info['indirect_loc_number'] == item_number):
                        print(f"Self-referencing indirect definition '{def_term['term']}' resolved to its own location ({item_type_name} {item_number}) — treating as external reference")
                        def_term['external_reference'] = indirect_string
                    else:
                        # Store the resolved location information
                        def_term['indirect_loc_type'] = indirect_info['indirect_loc_type']
                        def_term['indirect_loc_number'] = indirect_info['indirect_loc_number']
                        # Preserve the original indirect string in case the location doesn't exist in the document (external reference)
                        # This allows us to detect external references later
                        if 'indirect' in def_term and def_term['indirect']:
                            # Keep the indirect string for later use in detecting external references
                            def_term['indirect'] = indirect_string
                        print(f"Resolved indirect definition '{def_term['term']}' -> {indirect_info['indirect_loc_type']} {indirect_info['indirect_loc_number']}")
                else:
                    # Could not resolve the indirect reference, note as a possible external reference
                    if 'term' not in def_term:
                        raise InputError(f"Definition entry missing 'term' field: {def_term}")
                    print(f"Unresolved indirect definition '{def_term['term']}' with indirect string: '{indirect_string}'")
                    def_term['external_reference'] = indirect_string
        
        # Mark as dirty if we processed any indirect definitions (either resolved or deleted)
        if processed_indirect:
            proc.dirty = 1
    proc.flush()
    print("Indirect definition processing complete.")


def enhance_resolve_indirect_definitions(proc, count=30):
    """
    Enhanced indirect definition resolution with Step 3b (construction from target text).

    This function resolves indirect definitions by:
    1. Step 3a: Copying existing definitions from target locations
    2. Step 3b: Constructing definitions from target location text
    3. Removing definitions that cannot be resolved or constructed

    Modified to resolve each unique indirect definition only once, then update all copies
    across different locations. This ensures consistency when the same indirect definition
    appears in multiple substantive units.

    Args:
        proc (DefinitionsProcessor): The processor instance containing document context
        count (int): Maximum number of unique indirect definitions to process in this run

    The function:
    1. Deduplicates indirect definitions by (indirect_loc_type, indirect_loc_number, term)
    2. Tracks all locations where each unique indirect definition appears
    3. Resolves each unique indirect definition once (Step 3a or 3b)
    4. Updates all copies with the same result
    5. Removes all copies if resolution fails
    """
    print("Enhancing indirect definition resolution...")

    # Create task-specific client if config is available
    if proc.config:
        construct_client = create_client_for_task(proc.config, 'stage2.definitions.construct_from_target')
    else:
        # Fallback to None (function will use proc.client)
        construct_client = None

    # Get document issues logfile for logging definition removals
    document_issues_logfile = get_document_issues_logfile(proc.out_path)

    # Track unique indirect definitions and all their locations
    # Key: (indirect_loc_type, indirect_loc_number, term)
    # Value: {
    #   'canonical_def': def_entry (the first copy found, used for resolution),
    #   'all_copies': [(def_entry, org_context, operational_context), ...]
    # }
    unique_indirect_defs = {}
    target_texts = {}

    # Collect all indirect definitions that need processing and group by unique key
    for def_entry, org_context, operational_context in iter_definitions(proc.parsed_content):
        # Skip if not an indirect definition
        if 'indirect_loc_type' not in def_entry or 'indirect_loc_number' not in def_entry:
            continue

        # Skip if already quality checked (indirect definitions that have been processed)
        if def_entry.get('quality_checked', False):
            continue

        indirect_loc_type = def_entry['indirect_loc_type']
        indirect_loc_number = def_entry['indirect_loc_number']
        term = def_entry.get('term', '')

        # Create unique key: same term pointing to same target location
        unique_key = (indirect_loc_type, indirect_loc_number, term)

        # Track this indirect definition
        if unique_key not in unique_indirect_defs:
            unique_indirect_defs[unique_key] = {
                'canonical_def': def_entry,
                'all_copies': []
            }

        # Add this copy to the list
        unique_indirect_defs[unique_key]['all_copies'].append((def_entry, org_context, operational_context))

        # Get target text if not already cached
        target_key = (indirect_loc_type, indirect_loc_number)
        if target_key not in target_texts:
            target_loc_type, target_loc_type_plural = canonical_org_types(indirect_loc_type)
            target_item = lookup_item(proc.parsed_content, target_loc_type_plural, indirect_loc_number)
            if target_item is not None:
                target_texts[target_key] = target_item.get('text', '')
            else:
                target_texts[target_key] = ''

    # Step 3a: Try to find existing definitions at target locations
    for unique_key, def_info in unique_indirect_defs.items():
        if count < 1:
            break

        indirect_loc_type, indirect_loc_number, term = unique_key
        canonical_def = def_info['canonical_def']
        all_copies = def_info['all_copies']

        # Try to find definition at target location
        target_definition = find_definition_at_location(proc.parsed_content, indirect_loc_type, indirect_loc_number, term)

        if target_definition and target_definition.get('value', '').strip():
            # Step 3a succeeded: Copy definition from target to all copies
            for def_copy, org_context, operational_context in all_copies:
                def_copy['value'] = target_definition['value']

                # Remove indirect location markers and original indirect string (no longer needed)
                if 'indirect_loc_type' in def_copy:
                    del def_copy['indirect_loc_type']
                if 'indirect_loc_number' in def_copy:
                    del def_copy['indirect_loc_number']
                if 'indirect' in def_copy:
                    del def_copy['indirect']

                # Mark as quality checked (definition is good if it came from another definition)
                def_copy['quality_checked'] = True

            proc.dirty = 1
            print(f"Resolved indirect definition for '{term}' from {indirect_loc_type} {indirect_loc_number} (updated {len(all_copies)} location(s))")
            count -= 1

            # Mark this unique definition as resolved
            unique_indirect_defs[unique_key]['resolved'] = True
        else:
            # Mark as not yet resolved, will try Step 3b
            unique_indirect_defs[unique_key]['resolved'] = False

    # Step 3b: Construct definitions from target text for remaining unique indirect definitions
    for unique_key, def_info in unique_indirect_defs.items():
        if count < 1:
            break

        # Skip if already resolved in Step 3a
        if def_info.get('resolved', False):
            continue

        indirect_loc_type, indirect_loc_number, term = unique_key
        canonical_def = def_info['canonical_def']
        all_copies = def_info['all_copies']

        # Get target text
        target_key = (indirect_loc_type, indirect_loc_number)
        target_text = target_texts.get(target_key, '')

        if not target_text:
            # No target text available - check if this is an external reference
            # Check if the target location actually exists in the document structure
            target_loc_type_singular, target_loc_type_plural = canonical_org_types(indirect_loc_type)
            target_exists = lookup_item(proc.parsed_content, target_loc_type_plural, indirect_loc_number) is not None

            # If target doesn't exist in document, treat as external reference
            if not target_exists:
                # Process all copies - mark as external reference
                for def_copy, org_context, operational_context in all_copies:
                    # Check if we have the original indirect string to use as external reference
                    if 'indirect' in def_copy and def_copy.get('indirect'):
                        # Use original indirect string as external reference
                        def_copy['external_reference'] = def_copy['indirect']
                    elif 'external_reference' not in def_copy:
                        # Create external reference from the resolved location info
                        def_copy['external_reference'] = f"{indirect_loc_type} {indirect_loc_number}"

                    # Remove indirect location markers (they don't apply to external refs)
                    if 'indirect_loc_type' in def_copy:
                        del def_copy['indirect_loc_type']
                    if 'indirect_loc_number' in def_copy:
                        del def_copy['indirect_loc_number']

                    # Mark as checked (external reference is valid)
                    def_copy['quality_checked'] = True

                proc.dirty = 1
                print(f"Kept external reference for '{term}': {all_copies[0][0].get('external_reference', f'{indirect_loc_type} {indirect_loc_number}')} (updated {len(all_copies)} location(s))")
            else:
                # Target exists but no text - remove all copies
                reason = 'target location exists but has no text for construction'
                for def_copy, org_context, operational_context in all_copies:
                    remove_definition(proc, def_copy, org_context, operational_context,
                                     logfile_path=document_issues_logfile,
                                     previous_definition=def_copy.get('value', ''),
                                     reason=reason)
                print(f"Removed indirect definition for '{term}' from {len(all_copies)} location(s) (target has no text)")

            count -= 1
            continue

        # Build cache prompt for target location
        target_loc_type_singular, target_loc_type_plural = canonical_org_types(indirect_loc_type)
        cap_target_type = indirect_loc_type[0].upper() + indirect_loc_type[1:] if indirect_loc_type else 'Section'
        cache_prompt = f'You will be asked about definitions within this {indirect_loc_type} from a larger statute or legal document:\n\n***Begin {indirect_loc_type}***\n'
        cache_prompt += f'{cap_target_type} {indirect_loc_number}:\n'
        cache_prompt += target_text
        cache_prompt += f'\n***End {indirect_loc_type}***\n\n'

        # Step 3b: Try to construct definition from target text (once for this unique definition)
        constructed_def = construct_definition_from_target(proc, term, indirect_loc_type, indirect_loc_number, target_text, cache_prompt, construct_client)

        if constructed_def:
            # Step 3b succeeded: Update all copies with constructed definition
            for def_copy, org_context, operational_context in all_copies:
                def_copy['value'] = constructed_def

                # Remove indirect location markers and original indirect string (no longer needed)
                if 'indirect_loc_type' in def_copy:
                    del def_copy['indirect_loc_type']
                if 'indirect_loc_number' in def_copy:
                    del def_copy['indirect_loc_number']
                if 'indirect' in def_copy:
                    del def_copy['indirect']

                # Mark as quality checked (will be evaluated in quality control step)
                def_copy['quality_checked'] = False  # Let quality control evaluate it

            proc.dirty = 1
            print(f"Constructed definition for '{term}' from {indirect_loc_type} {indirect_loc_number} (updated {len(all_copies)} location(s))")
        else:
            # Step 3b failed: Remove all copies
            reason = 'could not construct definition from target location text'
            for def_copy, org_context, operational_context in all_copies:
                remove_definition(proc, def_copy, org_context, operational_context,
                                 logfile_path=document_issues_logfile,
                                 previous_definition=def_copy.get('value', ''),
                                 reason=reason)
            print(f"Removed indirect definition for '{term}' from {len(all_copies)} location(s) (could not construct from target)")

        count -= 1
    
    proc.flush()
    if count < 1:
        print('More indirect definition resolution needed.  Please run again.\n')
        exit(0)
    print("Indirect definition resolution complete.")

