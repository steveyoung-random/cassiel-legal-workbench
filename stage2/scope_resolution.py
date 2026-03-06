"""
Scope resolution functions for mapping scope phrases to document locations.

This module provides functions for resolving scope references in definitions,
including handling "current" references, ranges, and organizational paths.
"""
# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License

from utils import (
    iter_operational_items,
    query_json,
    get_full_item_name_set,
    get_list_string,
    get_organizational_item_name_set,
    get_document_issues_logfile,
    log_document_issue,
    build_scope_resolution_prompt_v2,
    strip_sub_prefix,
    expand_element_range,
    find_substantive_unit_with_maximum_matching,
    resolve_current_from_context,
    find_organizational_unit_path,
    resolve_compound_organizational_path,
    canonical_org_types,
    get_item_numbers_for_type,
    InputError,
    ParseWarning,
    InputWarning,
)
from utils.config import create_client_for_task
from .processor import DefinitionsProcessor
from .definition_management import (
    add_org_definition,
    add_substantive_unit_scope_definition,
    add_no_scope_definition,
)


def _is_working_unit_reference_type(element_type, org_item_name_set, substantive_unit_types):
    """
    Check if element_type should be treated as referring to the working unit.
    
    This handles special cases like "paragraph" and "subparagraph" that, when not recognized
    as formal unit types, should be interpreted as referring to the current working unit
    (the substantive unit where the scope language was found).
    
    Args:
        element_type (str): The element type to check
        org_item_name_set (set): Set of recognized organizational unit types
        substantive_unit_types (set): Set of recognized substantive unit types
        
    Returns:
        bool: True if element_type should refer to the working unit, False otherwise
    """
    # List of unit types that should refer to the working unit if not recognized
    # This can be easily updated to add more types
    working_unit_reference_types = ['paragraph', 'subparagraph']
    
    element_type_lower = element_type.lower()
    if element_type_lower not in [t.lower() for t in working_unit_reference_types]:
        return False
    
    # Check if this type is recognized as an organizational or substantive unit type
    if element_type_lower in [t.lower() for t in org_item_name_set]:
        return False
    
    if element_type_lower in [t.lower() for t in substantive_unit_types]:
        return False
    
    # Check if stripping "sub" prefix gives a recognized substantive type
    stripped_type = strip_sub_prefix(element_type)
    if stripped_type.lower() in [t.lower() for t in substantive_unit_types]:
        return False
    
    # Not recognized as any type - should refer to working unit
    return True


def _resolve_sub_working_unit_type(parsed_content, element_type, element_designation, item_type_name, item_number):
    """
    Helper function to resolve "sub" + unit_type references.
    
    If element_type is "sub" + some recognized substantive unit type,
    AND element_type is NOT already recognized as an organizational or substantive unit type,
    this function finds the longest matching unit number from the beginning of element_designation.
    
    Two cases:
    1. If stripped type matches working unit type: Returns unit number (or current working unit if no match)
    2. If stripped type is a recognized substantive unit type (but not working unit type): 
       Returns dict {type: number} if match found, None otherwise
    
    Args:
        parsed_content (dict): The document content
        element_type (str): The element type (e.g., "subsection", "subparagraph")
        element_designation (str): The element designation (e.g., "454(a)" or "(a)")
        item_type_name (str): The working unit type (e.g., "section")
        item_number (str): The current working unit number
        
    Returns:
        str, dict, or None: 
            - str: Unit number if stripped type matches working unit type
            - dict: {type: number} if stripped type is a different recognized substantive unit type
            - None: If not a "sub" + recognized type reference
    """
    # First, check if element_type is already recognized as an organizational or substantive unit type
    # If it is, we should NOT apply this logic - let existing rules handle it
    substantive_unit_types = get_full_item_name_set(parsed_content)
    org_item_name_set = get_organizational_item_name_set(parsed_content)
    
    # Check if it's an organizational unit (exact match only - don't strip "sub" for org units)
    is_organizational = element_type in org_item_name_set
    
    # For substantive units, check if element_type itself is recognized
    # (not base_type, because we want to catch "subsection" when it's not a recognized type)
    is_substantive = element_type in substantive_unit_types
    
    # If element_type is already recognized, don't apply this logic
    if is_organizational or is_substantive:
        return None
    
    # Strip "sub" prefix and check if the stripped type is a recognized substantive unit type
    stripped_type = strip_sub_prefix(element_type)
    
    # Check if stripped type is a recognized substantive unit type
    stripped_type_recognized = None
    for st in substantive_unit_types:
        if st.lower() == stripped_type.lower():
            stripped_type_recognized = st
            break
    
    if stripped_type_recognized is None:
        # Stripped type is not recognized - not a "sub" + recognized type reference
        return None
    
    # Stripped type is recognized - try to match designation
    element_designation_str = str(element_designation)
    longest_matching_unit = None
    longest_match_length = 0
    
    stripped_type_singular, stripped_type_plural = canonical_org_types(stripped_type_recognized)
    unit_numbers = get_item_numbers_for_type(parsed_content, stripped_type_plural)
    for unit_num in unit_numbers:
        unit_num_str = str(unit_num)
        if element_designation_str.startswith(unit_num_str):
            # Found a match - keep track of the longest one
            if len(unit_num_str) > longest_match_length:
                longest_match_length = len(unit_num_str)
                longest_matching_unit = unit_num
    
    # Check if stripped type matches working unit type
    if stripped_type.lower() == item_type_name.lower():
        # Case 1: Matches working unit type
        if longest_matching_unit is not None:
            # Use the unit whose number matches the most characters from the beginning
            return longest_matching_unit
        else:
            # No unit number matches - treat as current working unit
            return item_number
    else:
        # Case 2: Different recognized substantive unit type
        if longest_matching_unit is not None:
            # Return dict with the matched type and number
            return {stripped_type_recognized: longest_matching_unit}
        else:
            # No match found for this type
            return None


def resolve_scope_response_v2(proc, scope_response, scope_phrase, item_type_name, item_number,
                               current_item_context, document_issues_logfile):
    """
    Resolve the AI's scope response to actual document locations.

    This function processes the structured scope response from the AI, resolving "current"
    references, expanding ranges, and mapping to document structure. It also handles
    compound organizational paths (lists of organizational units).

    Args:
        proc (DefinitionsProcessor): The processor instance
        scope_response (list): AI response as list of scope items (dictionaries or lists)
        scope_phrase (str): Original scope phrase
        item_type_name (str): Type of item containing the scope reference
        item_number (str): Number/identifier of the item
        current_item_context (list): Current item's organizational context
        document_issues_logfile (str): Path to logfile for document-level issues

    Returns:
        list: List of resolved scopes in format compatible with existing code
              Each scope is a list of dicts representing organizational path or substantive unit
    """
    parsed_content = proc.parsed_content
    resolved_scopes = []

    if not isinstance(scope_response, list):
        return []

    for scope_item in scope_response:
        # ============================================================
        # NEW: Handle compound organizational paths (lists)
        # ============================================================
        if isinstance(scope_item, list):
            # This is a compound organizational path
            compound_path = resolve_compound_organizational_path(
                parsed_content,
                scope_item,  # List of {element_type, element_designation} objects
                current_item_context,
                document_issues_logfile,
                item_type_name,
                item_number,
                scope_phrase
            )

            if compound_path:
                resolved_scopes.append(compound_path)
            else:
                # Compound path resolution failed - log it
                log_document_issue(
                    document_issues_logfile, 'scope_resolution', item_type_name, item_number,
                    f'Failed to resolve compound organizational path',
                    scope_phrase=scope_phrase,
                    compound_elements=[f"{obj.get('element_type')} {obj.get('element_designation')}"
                                      for obj in scope_item if isinstance(obj, dict)]
                )
            continue  # Move to next scope_item

        # ============================================================
        # Existing logic for single scope objects (dictionaries)
        # ============================================================
        scope_obj = scope_item
        if not isinstance(scope_obj, dict):
            continue
        
        # Handle external references
        if scope_obj.get('in_this_document') is False:
            # External reference - return empty list to indicate no scope in this document
            # The calling code should handle this by storing the original scope phrase
            return []
        
        if scope_obj.get('in_this_document') is not True:
            continue  # Skip invalid entries
        
        element_type = scope_obj.get('element_type')
        element_designation = scope_obj.get('element_designation')
        
        if not element_type or not element_designation:
            continue
        
        # Check if this is a substantive unit type or organizational unit type
        substantive_unit_types = get_full_item_name_set(parsed_content)
        org_item_name_set = get_organizational_item_name_set(parsed_content)
        
        # First check if it's an organizational unit (exact match only - don't strip "sub" for org units)
        is_organizational = element_type in org_item_name_set
        
        # For substantive units, check if element_type itself is recognized
        # (not base_type, because we want to catch "subsection" when it's not a recognized type
        # but matches "sub" + working_unit_type - that will be handled in the else block)
        is_substantive = element_type in substantive_unit_types
        
        # Note: We don't check base_type here because if element_type is not recognized but
        # base_type matches the working unit type, we want to apply the special "sub" + 
        # working_unit_type logic, not treat it as a substantive unit type
        
        # Handle "current" references
        if element_designation == "current":
            if is_substantive:
                # "current" for substantive unit means the current item itself
                # Return scope pointing to this exact item so downstream logic can keep it locally
                # Log if element_type doesn't match the working unit type (unusual case)
                if element_type.lower() != item_type_name.lower():
                    log_document_issue(
                        document_issues_logfile, 'scope_resolution', item_type_name, item_number,
                        f'Substantive unit type mismatch: "current {element_type}" resolved to current {item_type_name} {item_number}',
                        scope_phrase=scope_phrase,
                        element_type=element_type,
                        working_unit_type=item_type_name
                    )
                resolved_scopes.append([{item_type_name: item_number}])
                continue
            elif is_organizational:
                # Resolve "current" from context and get its path
                resolved_designation, org_path = resolve_current_from_context(
                    parsed_content, element_type, current_item_context
                )
                if resolved_designation is None:
                    # Failed to resolve "current" - log and treat as no scope
                    log_document_issue(
                        document_issues_logfile, 'scope_resolution', item_type_name, item_number,
                        f'Could not resolve "current {element_type}" - type not found in context',
                        scope_phrase=scope_phrase
                    )
                    continue
                element_designation = resolved_designation
                # Use the built path for organizational units
                resolved_scopes.append(org_path)
                continue
            else:
                # Check if this is a "sub" + working_unit_type reference (e.g., "subsection" when working unit is "section")
                resolved_unit = _resolve_sub_working_unit_type(parsed_content, element_type, "current", item_type_name, item_number)
                if resolved_unit is not None:
                    if isinstance(resolved_unit, dict):
                        # Matched a different recognized substantive unit type - shouldn't happen for "current"
                        # but handle it anyway
                        resolved_scopes.append([resolved_unit])
                    else:
                        # Matched working unit type (string return value) - "current" means current working unit
                        resolved_scopes.append([{item_type_name: item_number}])
                    continue

                # Any unrecognized element_type with "current" designation refers to the working unit
                # This handles any unrecognized type like "paragraph", "subparagraph", or any other
                # type that doesn't match organizational or substantive unit types
                # Log this for tracking purposes
                log_document_issue(
                    document_issues_logfile, 'scope_resolution', item_type_name, item_number,
                    f'Unrecognized element type "{element_type}" with "current" designation resolved to working unit',
                    scope_phrase=scope_phrase,
                    element_type=element_type,
                    working_unit_type=item_type_name
                )
                resolved_scopes.append([{item_type_name: item_number}])
                continue
        
        # Handle ranges
        if isinstance(element_designation, dict):
            if 'first' in element_designation and 'last' in element_designation:
                first = element_designation['first']
                last = element_designation['last']
                
                # Track original values to detect if they were subsection designations
                original_first = first
                original_last = last
                first_resolved_to_current = False
                last_resolved_to_current = False
                
                # Handle "current" in range endpoints
                if first == "current":
                    if is_substantive:
                        first = item_number  # Current item
                    else:
                        resolved, _ = resolve_current_from_context(
                            parsed_content, element_type, current_item_context
                        )
                        if resolved is None:
                            log_document_issue(
                                document_issues_logfile, 'scope_resolution', item_type_name, item_number,
                                f'Could not resolve "current {element_type}" in range',
                                scope_phrase=scope_phrase
                            )
                            continue
                        first = resolved
                elif not is_substantive and not is_organizational:
                    # Check if this is a "sub" + working_unit_type reference for first endpoint
                    resolved_first = _resolve_sub_working_unit_type(parsed_content, element_type, first, item_type_name, item_number)
                    if resolved_first is not None:
                        if isinstance(resolved_first, dict):
                            # Matched a different type - extract the number (range expansion will need same type for both endpoints)
                            # For now, log and use the number - this might need refinement
                            first = list(resolved_first.values())[0]
                        else:
                            # Check if it resolved to current working unit (meaning original was a subsection designation)
                            if resolved_first == item_number and original_first != item_number:
                                first_resolved_to_current = True
                            first = resolved_first
                
                if last == "current":
                    if is_substantive:
                        last = item_number
                    else:
                        resolved, _ = resolve_current_from_context(
                            parsed_content, element_type, current_item_context
                        )
                        if resolved is None:
                            log_document_issue(
                                document_issues_logfile, 'scope_resolution', item_type_name, item_number,
                                f'Could not resolve "current {element_type}" in range',
                                scope_phrase=scope_phrase
                            )
                            continue
                        last = resolved
                elif not is_substantive and not is_organizational:
                    # Check if this is a "sub" + working_unit_type reference for last endpoint
                    resolved_last = _resolve_sub_working_unit_type(parsed_content, element_type, last, item_type_name, item_number)
                    if resolved_last is not None:
                        if isinstance(resolved_last, dict):
                            # Matched a different type - extract the number (range expansion will need same type for both endpoints)
                            # For now, log and use the number - this might need refinement
                            last = list(resolved_last.values())[0]
                        else:
                            # Check if it resolved to current working unit (meaning original was a subsection designation)
                            if resolved_last == item_number and original_last != item_number:
                                last_resolved_to_current = True
                            last = resolved_last
                
                # Expand range
                if is_substantive:
                    expanded = expand_element_range(parsed_content, element_type, first, last)
                    for designation in expanded:
                        matched_type, matched_designation = find_substantive_unit_with_maximum_matching(
                            parsed_content, element_type, designation
                        )
                        if matched_type and matched_designation:
                            resolved_scopes.append([{matched_type: matched_designation}])
                elif is_organizational:
                    # Ranges for organizational units are not typically used, but handle if needed
                    log_document_issue(
                        document_issues_logfile, 'scope_resolution', item_type_name, item_number,
                        f'Range specified for organizational unit type: {element_type}',
                        scope_phrase=scope_phrase
                    )
                else:
                    # Check if this is a "sub" + working_unit_type range
                    # Both endpoints should have been resolved above, now treat as substantive range
                    is_sub_working_unit_range = _resolve_sub_working_unit_type(parsed_content, element_type, original_first, item_type_name, item_number) is not None
                    if is_sub_working_unit_range:
                        # Check if both endpoints resolved to the current working unit (meaning they were subsection designations, not section numbers)
                        # In this case, we should just return the current working unit, not expand a range
                        if first_resolved_to_current and last_resolved_to_current:
                            # Both endpoints are subsection designations within the current section (e.g., "(a)" through "(f)")
                            # Since we can't represent nested units, just return the current working unit
                            resolved_scopes.append([{item_type_name: item_number}])
                        else:
                            # Endpoints matched actual section numbers - expand the range
                            expanded = expand_element_range(parsed_content, item_type_name, first, last)
                            for designation in expanded:
                                resolved_scopes.append([{item_type_name: designation}])
                    elif _is_working_unit_reference_type(element_type, org_item_name_set, substantive_unit_types):
                        # This should refer to the working unit (the substantive unit where scope language was found)
                        # For ranges, we'll treat it as referring to the current working unit
                        resolved_scopes.append([{item_type_name: item_number}])
                    else:
                        # Unknown type range - log and skip
                        log_document_issue(
                            document_issues_logfile, 'scope_resolution', item_type_name, item_number,
                            f'Range specified for unknown element type: {element_type}',
                            scope_phrase=scope_phrase
                        )
                continue
        
        # Handle single element (not range, not "current")
        if is_substantive:
            # Find the substantive unit with maximum matching
            matched_type, matched_designation = find_substantive_unit_with_maximum_matching(
                parsed_content, element_type, element_designation
            )
            if matched_type and matched_designation:
                resolved_scopes.append([{matched_type: matched_designation}])
        elif is_organizational:
            # For organizational units, we need to find the unit in the organizational structure
            # and build its full path from root. We'll search through the organizational structure
            # to find the unit and build its path.
            org_path = find_organizational_unit_path(parsed_content, element_type, element_designation, current_item_context)
            if org_path:
                resolved_scopes.append(org_path)
            else:
                log_document_issue(
                    document_issues_logfile, 'scope_resolution', item_type_name, item_number,
                    f'Could not find organizational unit: {element_type} {element_designation}',
                    scope_phrase=scope_phrase
                )
        else:
            # Check if this is a "sub" + unit_type reference
            # Example: "subsection (a)" when working unit is "section" and there's no section "(a)"
            # Or "subsection 454(a)" when working unit is "section" and section "454" exists -> use section "454"
            # Or "subparagraph 5" when working unit is "section" but "paragraph" is a recognized type -> use paragraph 5
            resolved_unit = _resolve_sub_working_unit_type(parsed_content, element_type, element_designation, item_type_name, item_number)
            if resolved_unit is not None:
                if isinstance(resolved_unit, dict):
                    # Matched a different recognized substantive unit type
                    resolved_scopes.append([resolved_unit])
                else:
                    # Matched working unit type (string return value)
                    resolved_scopes.append([{item_type_name: resolved_unit}])
                continue
            
            # Check if this is a special type that should refer to the working unit
            if _is_working_unit_reference_type(element_type, org_item_name_set, substantive_unit_types):
                # This should refer to the working unit (the substantive unit where scope language was found)
                # regardless of whether it's marked as "current" or has a number designation
                resolved_scopes.append([{item_type_name: item_number}])
                continue
            
            # Unknown type - log and skip
            log_document_issue(
                document_issues_logfile, 'scope_resolution', item_type_name, item_number,
                f'Unknown element type: {element_type}',
                scope_phrase=scope_phrase
            )
    
    return resolved_scopes


def find_defined_terms_scopes(proc, count=30):
    """
    Resolve scope information for defined terms using AI model analysis.

    This function processes defined terms that have scope information and uses the AI model
    to map scope phrases (like "in this chapter") to specific organizational units. This
    allows definitions to be properly categorized and made available only where they apply.

    Uses the new v2 approach: AI extracts structured information (element type, designation),
    and code handles resolution, range expansion, and "current" references.

    Args:
        proc (DefinitionsProcessor): The processor instance containing document context
        count (int): Maximum number of terms to process in this run

    The function:
    1. Iterates through operational items that have defined terms with scope information
    2. Uses AI model to extract structured scope information (element type, designation)
    3. Resolves "current" references, expands ranges, and maps to document structure
    4. Adds definitions to appropriate organizational levels or individual items
    5. Handles external references by storing original scope phrase
    """
    parsed_content = proc.parsed_content  # Use proc.parsed_content instead of relying on global variable
    if ('document_information' not in parsed_content.keys()
        or 'organization' not in parsed_content['document_information'].keys()
        or 'content' not in parsed_content['document_information']['organization'].keys()):
        InputError('No organizational content found.')
        exit(1)

    org_content_pointer = parsed_content['document_information']['organization']['content'] # Pointer to the 'content' portion of the 'organization' object.

    # Get document title information for the new prompt
    document_title = parsed_content['document_information'].get('title', '')
    document_long_title = parsed_content['document_information'].get('long_title', '')

    # Get all substantive unit types (not just operational)
    substantive_unit_types = sorted(get_full_item_name_set(parsed_content))
    substantive_unit_type_string = get_list_string(substantive_unit_types, 'or')

    # Create task-specific client if config is available
    if proc.config:
        resolve_scope_client = create_client_for_task(proc.config, 'stage2.definitions.resolve_scope')
    else:
        # Fallback to default client for backward compatibility
        resolve_scope_client = proc.client

    # Create document issues logfile
    document_issues_logfile = get_document_issues_logfile(proc.out_path)

    for item_type_name, item_type_name_plural, cap_item_type_name, item_number, working_item in iter_operational_items(proc.parsed_content):
        defined_term_list = working_item.get('defined_terms', [])
        if not len(defined_term_list) > 0:
            continue
        
        # Get current item's context for resolving "current" references
        current_item_context = working_item.get('context', [])
        
        # Track which definitions should be kept in defined_terms (scoped to this unit)
        definitions_to_keep = []
        
        local_scopes = {} # This is a dictionary that is local to this particular working_item.  It maps scope text to the lists used to formally define the scope.
        for def_term in defined_term_list:
            # If already processed, preserve it in defined_terms if it's still there
            # (it was kept because it's scoped to this unit)
            if def_term.get('quality_checked', False) or def_term.get('scope_processed', False):
                definitions_to_keep.append(def_term)
                continue

            # Initialize tracking variables for all definitions
            definition_kept = False  # Track if this definition was kept in defined_terms (scoped to this unit)
            # Track which locations this definition has already been added to, to prevent duplicate additions
            added_to_substantive_units = set()  # Set of (item_type, item_number) tuples
            added_to_org_units = set()  # Set of id(local_org_pointer) to track unique org units
            added_no_scope = False  # Track if definition has been added to document-wide definitions

            if 'scope' in def_term.keys() and not '' == def_term['scope']: # Only go forward here if there is scope information.
                scope_phrase = def_term['scope']

                if 0 == len(local_scopes.keys()) or not scope_phrase in local_scopes.keys(): # No need to query if this scope has already been resolved for this working_item.
                    # Use new v2 prompt
                    cache_prompt, prompt = build_scope_resolution_prompt_v2(
                        scope_phrase,
                        document_title,
                        document_long_title,
                        proc.org_item_name_string,
                        substantive_unit_type_string
                    )

                    res_object = query_json(resolve_scope_client, [cache_prompt], prompt, proc.logfile,
                                           config=proc.config, task_name='stage2.definitions.resolve_scope')

                    # Check for external references before resolving
                    is_external = False
                    if isinstance(res_object, list) and len(res_object) > 0:
                        if isinstance(res_object[0], dict) and res_object[0].get('in_this_document') is False:
                            is_external = True

                    if is_external:
                        # Store as external reference
                        def_term['external_reference'] = scope_phrase
                        def_term['scope_processed'] = True  # Mark as scope-processed
                        local_scopes[scope_phrase] = []  # Mark as processed (empty list indicates external)
                        proc.dirty = 1
                        count = count - 1
                        continue

                    # Resolve the scope response using the new v2 resolution function
                    resolved_scope = resolve_scope_response_v2(
                        proc,
                        res_object,
                        scope_phrase,
                        item_type_name,
                        item_number,
                        current_item_context,
                        document_issues_logfile
                    )

                    local_scopes[scope_phrase] = resolved_scope
                    count = count - 1

                resolved_scope = local_scopes[scope_phrase] # Get scope for this definition from local_scopes, to avoid repeated queries of the same input.
                # The scope may involve more than one distinct scopes (e.g. "this chapter and sections 215 and 216").  So the resolved_scope is a list
                # of individual scopes, and each of those is a list of dictionaries that defines a place in the organizational structure or an operational element.
                if len(resolved_scope) > 0:  # resolved_scope is a list of individual scopes.  Each individual_scope is a list of dictionaries.
                    for individual_scope in resolved_scope:
                        local_org_pointer = org_content_pointer # Initialize the local_org_pointer to point to the top of the organizational content structure.
                        if len(individual_scope) > 0:
                            # Address any case where the scope is a single substantive unit (e.g. a section).
                            last_key = next(iter(individual_scope[-1]))
                            if last_key in get_full_item_name_set(parsed_content): # The substantive unit of the scope may not be the same as the item_type_name.
                                scope_item_number = individual_scope[-1][last_key]
                                # Check if scope is to the same substantive unit where definition was found
                                if last_key == item_type_name and scope_item_number == item_number:
                                    # Keep this definition in defined_terms - it's scoped to this unit
                                    # Don't mark as quality_checked yet - that will happen during quality evaluation
                                    # Only add once, even if multiple scopes point to this unit
                                    if not definition_kept:
                                        # Add source information for quality evaluation
                                        if 'source_type' not in def_term:
                                            def_term['source_type'] = item_type_name
                                        if 'source_number' not in def_term:
                                            def_term['source_number'] = item_number
                                        definitions_to_keep.append(def_term)
                                        definition_kept = True
                                        def_term['scope_processed'] = True  # Mark as scope-processed
                                        proc.dirty = 1
                                    # Continue processing other scopes - this definition may also be scoped to other locations
                                else:
                                    # Scope is to a different substantive unit - add it there
                                    # Check if we've already added to this substantive unit
                                    substantive_key = (last_key, scope_item_number)
                                    if substantive_key not in added_to_substantive_units:
                                        def_term['scope_processed'] = True  # Mark as scope-processed
                                        add_substantive_unit_scope_definition(parsed_content, def_term, last_key, scope_item_number, item_type_name, item_number, document_issues_logfile, proc)
                                        added_to_substantive_units.add(substantive_key)
                                        proc.dirty = 1
                            else: # Use organizational structure.
                                unresolved_reference = 0 # Flag for case where scope points somewhere unresolvable (e.g. another statute).
                                for org_level in individual_scope: # Go down each level of the organizational structure by level.  Each org_level is a dictionary.
                                    org_level_key = next(iter(org_level))
                                    org_level_value = org_level[org_level_key]
                                    if org_level_key in local_org_pointer.keys() and org_level_value in local_org_pointer[org_level_key].keys():
                                        local_org_pointer = local_org_pointer[org_level_key][org_level_value]
                                    else:
                                        ParseWarning('Invalid organizational info received from model: ' + org_level_key + ', ' + org_level_value)
                                        unresolved_reference = 1 # Because this points somewhere else, we cannot record the definition for that place.
                                        break
                                if 0 == unresolved_reference:
                                    # Add to organizational unit - this definition may also be kept in defined_terms if another scope points to this unit
                                    # Check if we've already added to this organizational unit
                                    org_unit_id = id(local_org_pointer)
                                    if org_unit_id not in added_to_org_units:
                                        def_term['scope_processed'] = True  # Mark as scope-processed
                                        add_org_definition(parsed_content, def_term, local_org_pointer, item_type_name, item_number, document_issues_logfile, proc)
                                        added_to_org_units.add(org_unit_id)
                                        proc.dirty = 1
                        else: # Treat as no scope given.  Sometimes, though, this scope may go beyond this document.
                            if not added_no_scope:
                                def_term['scope_processed'] = True  # Mark as scope-processed
                                add_no_scope_definition(parsed_content, def_term, item_type_name, item_number, document_issues_logfile, proc)
                                added_no_scope = True
                                proc.dirty = 1
                            InputWarning('Empty resolved scope for: ' + scope_phrase)
                else: # No scope has been found.  Place this definition in the default location within the organizational content.
                    if not added_no_scope:
                        def_term['scope_processed'] = True  # Mark as scope-processed
                        add_no_scope_definition(parsed_content, def_term, item_type_name, item_number, document_issues_logfile, proc)
                        added_no_scope = True
                        proc.dirty = 1
            else: # This is the case if no scope information is given.
                if not added_no_scope:
                    def_term['scope_processed'] = True  # Mark as scope-processed (no scope to process)
                    add_no_scope_definition(parsed_content, def_term, item_type_name, item_number, document_issues_logfile, proc)
                    added_no_scope = True
                    proc.dirty = 1
        
        # Keep only the definitions that should remain in defined_terms (those scoped to this unit)
        working_item['defined_terms'] = definitions_to_keep
        if len(definitions_to_keep) > 0:
            proc.dirty = 1
        if count < 1:
            break
    
    proc.flush()
    if count < 1:
        print('More processing needed.  Please run again.\n')
        exit(0)

