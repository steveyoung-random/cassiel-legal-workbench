"""
Definition management functions for placing definitions in appropriate locations.

This module provides functions for adding definitions to organizational units,
substantive units, and document-wide definition lists.
"""
# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License

from utils import canonical_org_types, InputError, log_document_issue, lookup_item, query_json
import re


def _normalize_term_for_comparison(term: str) -> str:
    """
    Normalize a term for comparison by removing leading articles and converting to lowercase.
    
    This function handles:
    - Case-insensitive matching: "board" and "Board" match
    - Leading articles: "a board", "an apple", "the board" all match "board"
    - Articles must be followed by a space and a substantive word
    
    Examples:
    - "board" -> "board"
    - "Board" -> "board"
    - "a board" -> "board"
    - "The Board" -> "board"
    - "an apple" -> "apple"
    - "A" -> "a" (single letter, no article removal)
    - "the" -> "the" (single word, no article removal)
    
    Args:
        term (str): The term to normalize
        
    Returns:
        str: Normalized term (lowercase, leading articles removed)
    """
    if not term:
        return ''
    
    # Strip whitespace and convert to lowercase
    normalized = term.strip().lower()
    
    if not normalized:
        return ''
    
    # Remove leading articles: "a ", "an ", "the " (must be followed by space and word)
    # Pattern: start of string, article word, space, then rest
    pattern = r'^(a|an|the)\s+(.+)$'
    match = re.match(pattern, normalized)
    
    if match:
        # Extract the part after the article
        normalized = match.group(2).strip()
    
    return normalized


def _definitions_match(def1, def2):
    """
    Check if two definition dictionaries are true duplicates (term AND value match).
    
    Only blocks if both term and value are identical. Different values are allowed
    (elaborations can coexist, conflicting direct definitions are logged separately).
    
    Args:
        def1 (dict): First definition dictionary
        def2 (dict): Second definition dictionary
        
    Returns:
        bool: True if term AND value match exactly, False otherwise
    """
    # Only check term and value - these must match exactly to be considered a duplicate
    # Other fields (source, def_kind, indirect info) can differ and still be considered duplicates
    # if the term and definition text are the same
    term1 = def1.get('term', '').strip()
    term2 = def2.get('term', '').strip()
    value1 = def1.get('value', '').strip()
    value2 = def2.get('value', '').strip()
    
    # Normalize terms for comparison (case-insensitive, remove leading articles)
    normalized_term1 = _normalize_term_for_comparison(term1)
    normalized_term2 = _normalize_term_for_comparison(term2)
    
    if normalized_term1 != normalized_term2:
        return False
    
    # Value must match exactly to be considered a duplicate
    if value1 != value2:
        return False
    
    return True


def _check_conflicting_direct_definitions(def1, def2):
    """
    Check if two definitions with the same term but different values are both direct definitions.
    
    This is used to log conflicts when two direct definitions for the same term have different values.
    
    Args:
        def1 (dict): First definition dictionary
        def2 (dict): Second definition dictionary
        
    Returns:
        bool: True if both are direct definitions with same term but different values
    """
    term1 = def1.get('term', '').strip()
    term2 = def2.get('term', '').strip()
    value1 = def1.get('value', '').strip()
    value2 = def2.get('value', '').strip()
    def_kind1 = def1.get('def_kind', 'direct')
    def_kind2 = def2.get('def_kind', 'direct')
    
    # Normalize terms for comparison (case-insensitive, remove leading articles)
    normalized_term1 = _normalize_term_for_comparison(term1)
    normalized_term2 = _normalize_term_for_comparison(term2)
    
    if normalized_term1 != normalized_term2:
        return False
    
    # Check if values differ
    if value1 == value2:
        return False
    
    # Check if both are direct definitions
    # Default to 'direct' if def_kind is not specified
    if def_kind1 != 'elaboration' and def_kind2 != 'elaboration':
        return True
    
    return False


def experimental_resolve_conflicting_definitions(proc, def1, def2, logfile_path=None):
    """
    Experimental function to resolve conflicting direct definitions using AI.
    
    When two conflicting direct definitions are found, this function asks the AI model
    whether they can be considered effectively the same. If so, it returns the better
    version. If not, it returns None to indicate the conflict should be kept.
    
    Args:
        proc (DefinitionsProcessor): The processor instance (for AI client access)
        def1 (dict): First definition dictionary
        def2 (dict): Second definition dictionary
        logfile_path (str, optional): Path to document issues logfile for logging
        
    Returns:
        dict or None: The better definition if they can be considered effectively the same,
                     None if they cannot be considered the same (keep current behavior)
    """
    term1 = def1.get('term', '').strip()
    term2 = def2.get('term', '').strip()
    value1 = def1.get('value', '').strip()
    value2 = def2.get('value', '').strip()
    
    # Normalize terms for comparison (case-insensitive, remove leading articles)
    normalized_term1 = _normalize_term_for_comparison(term1)
    normalized_term2 = _normalize_term_for_comparison(term2)
    
    # Verify these are for the same term
    if normalized_term1 != normalized_term2:
        return None
    
    # Verify values differ
    if value1 == value2:
        return None
    
    # Build prompt to ask AI if definitions are effectively the same
    cache_prompt = f'You are analyzing two definitions for the term "{term1}" from a legal document.\n\n'
    cache_prompt += f'**Definition 1:**\n{value1}\n\n'
    cache_prompt += f'**Definition 2:**\n{value2}\n\n'
    
    prompt = 'Your task is to determine whether these two definitions can be considered effectively the same.\n\n'
    prompt += 'Two definitions are "effectively the same" if they convey the same meaning, even if worded differently. '
    prompt += 'They may use different phrasing, synonyms, or structure, but they should define the term in fundamentally the same way.\n\n'
    prompt += 'If the definitions are effectively the same, return a JSON object with:\n'
    prompt += '  - "effectively_same": true\n'
    prompt += '  - "better_definition": the text of the definition that is better written (clearer, more precise, or more complete)\n'
    prompt += '  - "better_source": 1 or 2 (indicating which definition is better)\n\n'
    prompt += 'If the definitions are NOT effectively the same (they convey different meanings), return:\n'
    prompt += '  - "effectively_same": false\n\n'
    prompt += '**CRITICAL: RETURN JSON ONLY**\n'
    prompt += '- Return ONLY the JSON object - no explanation, no preamble, no commentary\n'
    prompt += '- Do not include any text before or after the JSON\n'
    prompt += '- Do not explain your reasoning in the response\n'
    prompt += '- The response must be parseable JSON\n\n'
    
    try:
        result = query_json(proc.client, [cache_prompt], prompt, proc.logfile)
        
        if isinstance(result, dict) and result.get('effectively_same') is True:
            better_source = result.get('better_source')
            better_def = result.get('better_definition', '').strip()
            
            if better_def and better_source in [1, 2]:
                # Return the better definition (use the original dict structure)
                chosen_def = def1 if better_source == 1 else def2
                # Update with the better definition text
                resolved_def = chosen_def.copy()
                resolved_def['value'] = better_def
                
                # Log the resolution
                if logfile_path:
                    log_document_issue(
                        logfile_path,
                        'conflicting_definitions_resolved',
                        item_type_name=chosen_def.get('source_type', 'unknown'),
                        item_number=chosen_def.get('source_number', 'unknown'),
                        issue_description=f"Resolved conflicting direct definitions for term '{term1}' - definitions were effectively the same",
                        term=term1,
                        definition_1=value1[:200],  # Truncate for logging
                        definition_2=value2[:200],
                        resolved_definition=better_def[:200],
                        chosen_source=better_source
                    )
                
                return resolved_def
        
        # Definitions are not effectively the same - return None to keep current behavior
        return None
        
    except Exception as e:
        # If AI query fails, return None to keep current behavior
        # Log the error if logfile_path is provided
        if logfile_path:
            log_document_issue(
                logfile_path,
                'conflicting_definitions_resolution_failed',
                item_type_name=def1.get('source_type', 'unknown'),
                item_number=def1.get('source_number', 'unknown'),
                issue_description=f"Failed to resolve conflicting definitions for term '{term1}' using AI: {e}",
                term=term1
            )
        return None


def add_org_definition(parsed_content, def_term, local_org_pointer, source_type, source_number, logfile_path=None, proc=None):
    """
    Add a definition to a specific organizational level.
    
    This function adds a definition to an organizational unit (like a chapter or part)
    so it will be available to all items within that organizational scope.
    
    Args:
        parsed_content (dict): The document content being processed
        def_term (dict): The definition term information
        local_org_pointer (dict): Pointer to the organizational unit where definition should be added
        source_type (str): Type of the item where the definition was found
        source_number (str): Number/identifier of the source item
        logfile_path (str, optional): Path to document issues logfile for logging duplicate attempts
        proc (DefinitionsProcessor, optional): Processor instance for experimental conflict resolution
    """
    # Adds a definition to a specific organizational level (e.g. chapter), based on a definition provided in a substantive provision.
    if not 'unit_definitions' in local_org_pointer.keys():
        local_org_pointer['unit_definitions'] = []
    def_dict = {'term': def_term['term'], 'value': def_term['value'], 'source_type': source_type, 'source_number': source_number}
    # Preserve def_kind if present
    if 'def_kind' in def_term.keys():
        def_dict['def_kind'] = def_term['def_kind']
    # Preserve scope field if present (important for conflict resolution to know if definition has express scoping)
    if 'scope' in def_term.keys():
        def_dict['scope'] = def_term['scope']
    # Add indirect location information if it exists
    if 'indirect_loc_type' in def_term.keys():
        def_dict['indirect_loc_type'] = def_term['indirect_loc_type']
    if 'indirect_loc_number' in def_term.keys():
        def_dict['indirect_loc_number'] = def_term['indirect_loc_number']
    # Preserve original indirect string for external reference detection
    if 'indirect' in def_term.keys() and def_term['indirect']:
        def_dict['indirect'] = def_term['indirect']
    # Handle unresolved external references
    if 'external_reference' in def_term.keys():
        def_dict['external_reference'] = def_term['external_reference']
    
    # Check for duplicates and conflicts before adding
    for existing_def in local_org_pointer['unit_definitions']:
        # Check for true duplicate (term AND value match exactly)
        if _definitions_match(def_dict, existing_def):
            # True duplicate found - this should not happen if tracking in calling code is working correctly
            if logfile_path:
                # Try to identify the organizational unit for logging
                org_unit_name = 'unknown'
                # Try to find org unit info from local_org_pointer structure
                if 'unit_title' in local_org_pointer:
                    org_unit_name = local_org_pointer.get('unit_title', 'unknown')
                log_document_issue(
                    logfile_path,
                    'duplicate_definition_attempt',
                    item_type_name=source_type,
                    item_number=source_number,
                    issue_description=f"Attempted to add duplicate definition for term '{def_dict.get('term', 'unknown')}' to organizational unit",
                    term=def_dict.get('term', ''),
                    target_location='organizational_unit',
                    org_unit_title=org_unit_name
                )
            return  # True duplicate found, skip adding
        
        # Check for conflicting direct definitions (same term, different value, both direct)
        if logfile_path and _check_conflicting_direct_definitions(def_dict, existing_def):
            org_unit_name = 'unknown'
            if 'unit_title' in local_org_pointer:
                org_unit_name = local_org_pointer.get('unit_title', 'unknown')
            
            # Try experimental conflict resolution if proc is available
            resolved_def = None
            if proc is not None:
                resolved_def = experimental_resolve_conflicting_definitions(proc, def_dict, existing_def, logfile_path)
            
            if resolved_def is not None:
                # Replace existing definition with resolved one
                existing_index = local_org_pointer['unit_definitions'].index(existing_def)
                local_org_pointer['unit_definitions'][existing_index] = resolved_def
                # Don't add the new conflicting definition
                return
            else:
                # Log conflict and allow both definitions (current behavior)
                log_document_issue(
                    logfile_path,
                    'conflicting_direct_definitions',
                    item_type_name=source_type,
                    item_number=source_number,
                    issue_description=f"Conflicting direct definitions for term '{def_dict.get('term', 'unknown')}' in organizational unit",
                    term=def_dict.get('term', ''),
                    target_location='organizational_unit',
                    org_unit_title=org_unit_name,
                    existing_value=existing_def.get('value', '')[:200],  # Truncate for logging
                    new_value=def_dict.get('value', '')[:200]
                )
                # Continue - allow both definitions
    
    local_org_pointer['unit_definitions'].append(def_dict)


def add_substantive_unit_scope_definition(parsed_content, def_term, scope_item_type, scope_item_number, source_type, source_number, logfile_path=None, proc=None):
    """
    Add a definition to a specific operational item (section, article, etc.).
    
    This function adds a definition to a specific operational item when the scope
    refers to that individual item rather than an organizational unit.
    
    Args:
        parsed_content (dict): The document content being processed
        def_term (dict): The definition term information
        scope_item_type (str): Type of the item where definition should be added
        scope_item_number (str): Number/identifier of the target item
        source_type (str): Type of the item where the definition was found
        source_number (str): Number/identifier of the source item
        logfile_path (str, optional): Path to document issues logfile for logging duplicate attempts
        proc (DefinitionsProcessor, optional): Processor instance for experimental conflict resolution
    """
    # Adds a definition to a specific operational item (e.g. section), based on a definition provided elsewhere.
    # We don't add definitions where the source and target scope are the same.
    if not scope_item_type == source_type or not scope_item_number == source_number:
        scope_item_type, scope_item_type_plural = canonical_org_types(scope_item_type)
        scope_item = lookup_item(parsed_content, scope_item_type_plural, scope_item_number)
        if scope_item is not None:
            if 'ext_definitions' not in scope_item:
                scope_item['ext_definitions'] = []
            ext_def_pointer = scope_item['ext_definitions']  # Pointer to the list of definitions provided elsewhere for this unit.
            def_dict = {'term': def_term['term'], 'value': def_term['value'], 'source_type': source_type, 'source_number': source_number}
            # Preserve def_kind if present
            if 'def_kind' in def_term.keys():
                def_dict['def_kind'] = def_term['def_kind']
            # Preserve scope field if present (important for conflict resolution to know if definition has express scoping)
            if 'scope' in def_term.keys():
                def_dict['scope'] = def_term['scope']
            # Add indirect location information if it exists
            if 'indirect_loc_type' in def_term.keys():
                def_dict['indirect_loc_type'] = def_term['indirect_loc_type']
            if 'indirect_loc_number' in def_term.keys():
                def_dict['indirect_loc_number'] = def_term['indirect_loc_number']
            # Preserve original indirect string for external reference detection
            if 'indirect' in def_term.keys() and def_term['indirect']:
                def_dict['indirect'] = def_term['indirect']
            # Handle unresolved external references
            if 'external_reference' in def_term.keys():
                def_dict['external_reference'] = def_term['external_reference']

            # Check for duplicates and conflicts before adding
            for existing_def in ext_def_pointer:
                # Check for true duplicate (term AND value match exactly)
                if _definitions_match(def_dict, existing_def):
                    # True duplicate found - this should not happen if tracking in calling code is working correctly
                    if logfile_path:
                        log_document_issue(
                            logfile_path,
                            'duplicate_definition_attempt',
                            item_type_name=scope_item_type,
                            item_number=scope_item_number,
                            issue_description=f"Attempted to add duplicate definition for term '{def_dict.get('term', 'unknown')}' to substantive unit",
                            term=def_dict.get('term', ''),
                            target_location=f"{scope_item_type} {scope_item_number}",
                            source_location=f"{source_type} {source_number}"
                        )
                    return  # True duplicate found, skip adding

                # Check for conflicting direct definitions (same term, different value, both direct)
                if logfile_path and _check_conflicting_direct_definitions(def_dict, existing_def):
                    # Try experimental conflict resolution if proc is available
                    resolved_def = None
                    if proc is not None:
                        resolved_def = experimental_resolve_conflicting_definitions(proc, def_dict, existing_def, logfile_path)

                    if resolved_def is not None:
                        # Replace existing definition with resolved one
                        existing_index = ext_def_pointer.index(existing_def)
                        ext_def_pointer[existing_index] = resolved_def
                        # Don't add the new conflicting definition
                        return
                    else:
                        # Log conflict and allow both definitions (current behavior)
                        log_document_issue(
                            logfile_path,
                            'conflicting_direct_definitions',
                            item_type_name=scope_item_type,
                            item_number=scope_item_number,
                            issue_description=f"Conflicting direct definitions for term '{def_dict.get('term', 'unknown')}' in substantive unit",
                            term=def_dict.get('term', ''),
                            target_location=f"{scope_item_type} {scope_item_number}",
                            source_location=f"{source_type} {source_number}",
                            existing_value=existing_def.get('value', '')[:200],  # Truncate for logging
                            new_value=def_dict.get('value', '')[:200]
                        )
                        # Continue - allow both definitions

            ext_def_pointer.append(def_dict)


def add_no_scope_definition(parsed_content, def_term, source_type, source_number, logfile_path=None, proc=None):
    """
    Add a definition that has no scope limitations.
    
    This function adds definitions that apply throughout the entire document
    (no scope specified) to the document-level definitions list.
    
    Args:
        parsed_content (dict): The document content being processed
        def_term (dict): The definition term information
        source_type (str): Type of the item where the definition was found
        source_number (str): Number/identifier of the source item
        logfile_path (str, optional): Path to document issues logfile for logging duplicate attempts
        proc (DefinitionsProcessor, optional): Processor instance for experimental conflict resolution
    """
    # Adds a definition to the list that is not scope-limited.
    if 'document_information' in parsed_content.keys():
        if not 'document_definitions' in parsed_content['document_information'].keys():
            parsed_content['document_information']['document_definitions'] = []
        definitions_pointer = parsed_content['document_information']['document_definitions']
        def_dict = {'term': def_term['term'], 'value': def_term['value'], 'source_type': source_type, 'source_number': source_number}
        # Preserve def_kind if present
        if 'def_kind' in def_term.keys():
            def_dict['def_kind'] = def_term['def_kind']
        # Preserve scope field if present (even if definition ends up at document level,
        # the original scope information may be useful for conflict resolution)
        if 'scope' in def_term.keys():
            def_dict['scope'] = def_term['scope']
        # Add indirect location information if it exists
        if 'indirect_loc_type' in def_term.keys():
            def_dict['indirect_loc_type'] = def_term['indirect_loc_type']
        if 'indirect_loc_number' in def_term.keys():
            def_dict['indirect_loc_number'] = def_term['indirect_loc_number']
        # Preserve original indirect string for external reference detection
        if 'indirect' in def_term.keys() and def_term['indirect']:
            def_dict['indirect'] = def_term['indirect']
        # Handle unresolved external references
        if 'external_reference' in def_term.keys():
            def_dict['external_reference'] = def_term['external_reference']
        
        # Check for duplicates and conflicts before adding
        for existing_def in definitions_pointer:
            # Check for true duplicate (term AND value match exactly)
            if _definitions_match(def_dict, existing_def):
                # True duplicate found - this should not happen if tracking in calling code is working correctly
                if logfile_path:
                    log_document_issue(
                        logfile_path,
                        'duplicate_definition_attempt',
                        item_type_name=source_type,
                        item_number=source_number,
                        issue_description=f"Attempted to add duplicate definition for term '{def_dict.get('term', 'unknown')}' to document-wide definitions",
                        term=def_dict.get('term', ''),
                        target_location='document-wide'
                    )
                return  # True duplicate found, skip adding
            
            # Check for conflicting direct definitions (same term, different value, both direct)
            if logfile_path and _check_conflicting_direct_definitions(def_dict, existing_def):
                # Try experimental conflict resolution if proc is available
                resolved_def = None
                if proc is not None:
                    resolved_def = experimental_resolve_conflicting_definitions(proc, def_dict, existing_def, logfile_path)
                
                if resolved_def is not None:
                    # Replace existing definition with resolved one
                    existing_index = definitions_pointer.index(existing_def)
                    definitions_pointer[existing_index] = resolved_def
                    # Don't add the new conflicting definition
                    return
                else:
                    # Log conflict and allow both definitions (current behavior)
                    log_document_issue(
                        logfile_path,
                        'conflicting_direct_definitions',
                        item_type_name=source_type,
                        item_number=source_number,
                        issue_description=f"Conflicting direct definitions for term '{def_dict.get('term', 'unknown')}' in document-wide definitions",
                        term=def_dict.get('term', ''),
                        target_location='document-wide',
                        existing_value=existing_def.get('value', '')[:200],  # Truncate for logging
                        new_value=def_dict.get('value', '')[:200]
                    )
                    # Continue - allow both definitions
        
        definitions_pointer.append(def_dict)
    else:
        raise InputError('No document_information found.')

