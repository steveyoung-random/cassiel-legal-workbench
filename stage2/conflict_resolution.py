"""
Conflict resolution for document-level definitions.

This module provides functions to resolve conflicts among document-level definitions
by moving non-scoped conflicting definitions down the organizational tree toward
their source substantive units.
"""
# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License

from typing import List, Dict, Tuple, Optional
from utils import (
    InputError,
    log_document_issue,
    get_organizational_item_name_set,
    get_org_pointer_from_context,
    canonical_org_types,
    iter_operational_items,
    lookup_item,
)
from utils.ai_client import query_json
from utils.config import create_client_for_task
from .definition_management import (
    add_org_definition,
    _definitions_match,
    _check_conflicting_direct_definitions,
    experimental_resolve_conflicting_definitions,
    _normalize_term_for_comparison,
)


def _is_sub_unit_type(parsed_content: dict, type_name_singular: str) -> bool:
    """Return True if type_name_singular is declared as a sub-unit type in parameters."""
    param_pointer = parsed_content.get('document_information', {}).get('parameters', {})
    for p in param_pointer.values():
        if p.get('name') == type_name_singular and p.get('is_sub_unit', False):
            return True
    return False


def _get_sub_unit_plural(parsed_content: dict, type_name_singular: str) -> Optional[str]:
    """Return the plural name for a sub-unit type, or None if not found."""
    param_pointer = parsed_content.get('document_information', {}).get('parameters', {})
    for p in param_pointer.values():
        if p.get('name') == type_name_singular and p.get('is_sub_unit', False):
            return p.get('name_plural')
    return None


def _has_express_scoping(def_entry: dict) -> bool:
    """
    Check if a definition has express scoping (e.g., "for purposes of this title").
    
    A definition has express scoping if it has a non-empty 'scope' field.
    
    Args:
        def_entry (dict): Definition dictionary
        
    Returns:
        bool: True if definition has express scoping, False otherwise
    """
    scope = def_entry.get('scope', '')
    return isinstance(scope, str) and scope.strip() != ''


def _are_sibling_substantive_units(parsed_content: dict, source_type1: str, source_number1: str,
                                     source_type2: str, source_number2: str) -> bool:
    """
    Check if two substantive units are siblings (have the same organizational parent).
    
    Args:
        parsed_content (dict): The document content
        source_type1 (str): Type of first source unit
        source_number1 (str): Number of first source unit
        source_type2 (str): Type of second source unit
        source_number2 (str): Number of second source unit
        
    Returns:
        bool: True if both units have the same organizational parent context
    """
    context1 = _get_source_context(parsed_content, source_type1, source_number1)
    context2 = _get_source_context(parsed_content, source_type2, source_number2)
    
    if not context1 or not context2:
        return False
    
    # Check if they have the same parent (all but last level should match)
    # If contexts are [parent1, parent2, ...], they're siblings if all but last match
    # Actually, for siblings, the full context should be the same (same org parent)
    if len(context1) != len(context2):
        return False
    
    # Compare all levels - siblings should have identical organizational context
    for i in range(len(context1)):
        if (context1[i].get('name') != context2[i].get('name') or
            context1[i].get('number') != context2[i].get('number')):
            return False
    
    return True


def _detect_elaborative_relationship(proc, def1: dict, def2: dict, logfile_path: Optional[str] = None, client=None) -> Optional[dict]:
    """
    Detect if one definition elaborates on another using AI.

    Args:
        proc: The processor instance
        def1: First definition dictionary
        def2: Second definition dictionary
        logfile_path: Optional path to log file
        client: Optional AI client for task-specific model routing

    Returns:
        dict with keys:
        - 'is_elaborative': bool - True if one elaborates on the other
        - 'elaborative_def': dict - The definition that is elaborative (or None)
        - 'direct_def': dict - The direct definition (or None)
        Or None if not elaborative or detection fails
    """
    if proc is None or proc.client is None:
        return None

    term1 = def1.get('term', '').strip()
    term2 = def2.get('term', '').strip()
    value1 = def1.get('value', '').strip()
    value2 = def2.get('value', '').strip()

    # Normalize terms - must be the same term
    normalized_term1 = _normalize_term_for_comparison(term1)
    normalized_term2 = _normalize_term_for_comparison(term2)

    if normalized_term1 != normalized_term2:
        return None

    # Build prompt to ask AI if one elaborates on the other
    cache_prompt = f'You are analyzing two definitions for the term "{term1}" from a legal document.\n\n'
    cache_prompt += f'**Definition 1** (from {def1.get("source_type", "unknown")} {def1.get("source_number", "unknown")}):\n{value1}\n\n'
    cache_prompt += f'**Definition 2** (from {def2.get("source_type", "unknown")} {def2.get("source_number", "unknown")}):\n{value2}\n\n'

    prompt = 'Your task is to determine whether one of these definitions elaborates on the other.\n\n'
    prompt += 'A definition "elaborates on" another if it:\n'
    prompt += '- Expands on or provides additional details about the same concept\n'
    prompt += '- Describes components, structure, or characteristics of what was defined\n'
    prompt += '- Provides supplementary information without contradicting the base definition\n'
    prompt += '- Uses phrases like "shall consist of", "includes", "means", etc. that build on the first definition\n\n'
    prompt += 'If one definition elaborates on the other, return a JSON object with:\n'
    prompt += '  - "is_elaborative": true\n'
    prompt += '  - "elaborative_source": 1 or 2 (which definition is the elaboration)\n'
    prompt += '  - "direct_source": 1 or 2 (which definition is the direct/base definition)\n\n'
    prompt += 'If neither definition elaborates on the other (they are truly conflicting or independent), return:\n'
    prompt += '  - "is_elaborative": false\n\n'
    prompt += '**CRITICAL: RETURN JSON ONLY**\n'
    prompt += '- Return ONLY the JSON object - no explanation, no preamble, no commentary\n'
    prompt += '- Do not include any text before or after the JSON\n'
    prompt += '- Do not explain your reasoning in the response\n'
    prompt += '- The response must be parseable JSON\n\n'

    # Use provided client or fall back to default
    active_client = client if client is not None else proc.client

    try:
        result = query_json(active_client, [cache_prompt], prompt, proc.logfile,
                           config=proc.config, task_name='stage2.definitions.detect_elaborative')
        
        if isinstance(result, dict) and result.get('is_elaborative') is True:
            elaborative_source = result.get('elaborative_source')
            direct_source = result.get('direct_source')
            
            if elaborative_source in [1, 2] and direct_source in [1, 2]:
                elaborative_def = def1 if elaborative_source == 1 else def2
                direct_def = def1 if direct_source == 1 else def2
                
                # Log the detection
                if logfile_path:
                    log_document_issue(
                        logfile_path,
                        'elaborative_relationship_detected',
                        item_type_name=elaborative_def.get('source_type', 'unknown'),
                        item_number=elaborative_def.get('source_number', 'unknown'),
                        issue_description=f"Detected elaborative relationship for term '{term1}' - {elaborative_def.get('source_type', 'unknown')} {elaborative_def.get('source_number', 'unknown')} elaborates on {direct_def.get('source_type', 'unknown')} {direct_def.get('source_number', 'unknown')}",
                        term=term1,
                        direct_source=f"{direct_def.get('source_type', 'unknown')} {direct_def.get('source_number', 'unknown')}",
                        elaborative_source=f"{elaborative_def.get('source_type', 'unknown')} {elaborative_def.get('source_number', 'unknown')}"
                    )
                
                return {
                    'is_elaborative': True,
                    'elaborative_def': elaborative_def,
                    'direct_def': direct_def
                }
        
        return None
        
    except Exception as e:
        # Log the error if logfile_path is provided
        if logfile_path:
            log_document_issue(
                logfile_path,
                'elaborative_detection_failed',
                item_type_name=def1.get('source_type', 'unknown'),
                item_number=def1.get('source_number', 'unknown'),
                issue_description=f"Failed to detect elaborative relationship for term '{term1}': {e}",
                term=term1
            )
        return None


def _get_source_context(parsed_content: dict, source_type: str, source_number: str) -> Optional[List[dict]]:
    """
    Get the organizational context path for a source substantive unit.

    For sub-unit types (e.g. eccn), the source item's own type and number are appended
    as an extra level beyond the organizational context, so that push-down logic can
    navigate all the way into the sub-unit's defined_terms.

    Args:
        parsed_content (dict): The document content
        source_type (str): Type of the source unit (e.g., "section")
        source_number (str): Number/identifier of the source unit

    Returns:
        Optional[List[dict]]: Organizational context path (list of dicts with 'name' and 'number'),
                              or None if source unit not found or has no context
    """
    source_type_singular, source_type_plural = canonical_org_types(source_type)

    source_item = lookup_item(parsed_content, source_type_plural, source_number)
    if source_item is None:
        return None

    if 'context' not in source_item.keys():
        return None

    context = source_item['context']

    # Convert context format if needed (context might be list of dicts with single key-value pairs)
    # or list of dicts with 'name' and 'number' keys
    if not isinstance(context, list):
        return None

    normalized_context = []
    for entry in context:
        if isinstance(entry, dict):
            # Check if it's in format {'name': 'value'} or {'name': 'name', 'number': 'number'}
            if 'name' in entry and 'number' in entry:
                normalized_context.append({'name': entry['name'], 'number': entry['number']})
            elif len(entry) == 1:
                # Format: {'title': '42'} -> {'name': 'title', 'number': '42'}
                key, value = next(iter(entry.items()))
                normalized_context.append({'name': key, 'number': value})

    if not normalized_context:
        return None

    # For sub-unit sources (e.g. ECCNs), append the sub-unit itself as an extra level.
    # This lets push-down logic navigate one step beyond the org ancestor into the
    # sub-unit's defined_terms, bridging the gap that org-hierarchy traversal alone
    # cannot cross.
    if _is_sub_unit_type(parsed_content, source_type_singular):
        normalized_context.append({'name': source_type_singular, 'number': source_number})

    return normalized_context


def _find_conflicts_at_level(definitions: List[dict]) -> Dict[str, List[dict]]:
    """
    Find all conflicts (same term, different values) among definitions at a given level.
    
    Terms are normalized for comparison (case-insensitive, leading articles removed).
    So "board", "Board", "a board", and "The Board" are all considered the same term.
    
    Args:
        definitions (List[dict]): List of definition dictionaries
        
    Returns:
        Dict[str, List[dict]]: Dictionary mapping normalized term to list of conflicting definitions
    """
    conflicts = {}
    term_to_defs = {}
    
    # Group definitions by normalized term
    for def_entry in definitions:
        term = def_entry.get('term', '').strip()
        if not term:
            continue
        
        normalized_term = _normalize_term_for_comparison(term)
        if not normalized_term:
            continue
        
        if normalized_term not in term_to_defs:
            term_to_defs[normalized_term] = []
        term_to_defs[normalized_term].append(def_entry)
    
    # Find conflicts (same normalized term, different values)
    for normalized_term, defs in term_to_defs.items():
        if len(defs) < 2:
            continue  # No conflict if only one definition
        
        # Check if any definitions have different values
        values = [d.get('value', '').strip() for d in defs]
        if len(set(values)) > 1:
            conflicts[normalized_term] = defs
    
    return conflicts


def _get_definitions_at_org_level(parsed_content: dict, org_context: List[dict]) -> List[dict]:
    """
    Get all definitions at a specific organizational level or sub-unit level.

    When the last entry in org_context is a sub-unit type (e.g. eccn), the sub-unit's
    defined_terms list is returned instead of a unit_definitions list.

    Args:
        parsed_content (dict): The document content
        org_context (List[dict]): Organizational context path

    Returns:
        List[dict]: List of definitions at that organizational level
    """
    if not org_context:
        # Document level
        if ('document_information' in parsed_content.keys() and
            'document_definitions' in parsed_content['document_information'].keys()):
            return parsed_content['document_information']['document_definitions'].copy()
        return []

    # Check if the last entry is a sub-unit type
    last_entry = org_context[-1]
    if _is_sub_unit_type(parsed_content, last_entry.get('name', '')):
        plural = _get_sub_unit_plural(parsed_content, last_entry['name'])
        if plural:
            sub_unit = lookup_item(parsed_content, plural, last_entry['number'])
            if sub_unit is not None:
                return sub_unit.get('defined_terms', []).copy()
        return []

    # Organizational level
    org_pointer = get_org_pointer_from_context(parsed_content, org_context)
    if org_pointer and 'unit_definitions' in org_pointer.keys():
        return org_pointer['unit_definitions'].copy()

    return []


def _move_definition_down_one_level(parsed_content: dict, def_entry: dict, 
                                     source_context: List[dict],
                                     current_org_context: Optional[List[dict]] = None,
                                     logfile_path: Optional[str] = None,
                                     proc=None,
                                     debug_log: Optional[List[str]] = None) -> bool:
    """
    Move a definition down one level in the organizational tree toward its source.
    
    Args:
        parsed_content (dict): The document content
        def_entry (dict): Definition dictionary to move
        source_context (List[dict]): Organizational context path to the source unit
        current_org_context (List[dict], optional): Current organizational context (empty list for document level)
        logfile_path (str, optional): Path to document issues logfile
        proc: Optional processor instance (for conflict resolution in add_org_definition)
        debug_log (List[str], optional): List to append debug messages to
        
    Returns:
        bool: True if definition was moved, False otherwise
    """
    if debug_log is not None:
        debug_log.append(f"  _move_definition_down_one_level: term='{def_entry.get('term', 'unknown')}', source={def_entry.get('source_type', 'unknown')} {def_entry.get('source_number', 'unknown')}")

    if not source_context:
        # Can't move down if there's no source context
        if debug_log is not None:
            debug_log.append(f"    ERROR: No source context")
        return False
    # Determine target context: move down one level from current level
    if current_org_context is None or len(current_org_context) == 0:
        # We're at document level - move to first level of source context
        target_context = [source_context[0]]
    else:
        # We're at an org level - move down one more level toward source
        # Check if we can move deeper (not at or past source level)
        if len(current_org_context) >= len(source_context):
            # Already at or past source level - can't move down further
            if debug_log is not None:
                debug_log.append(f"    Already at or past source level, cannot move down")
            return False
        
        # Move to next level in source context path
        target_context = source_context[:len(current_org_context) + 1]
    if debug_log is not None:
        debug_log.append(f"    Target context: {target_context}")

    # --- Sub-unit target handling ---
    # If the target context ends with a sub-unit type, navigate directly into the
    # sub-unit's defined_terms instead of using the org-hierarchy pointer.
    last_target_entry = target_context[-1]
    if _is_sub_unit_type(parsed_content, last_target_entry.get('name', '')):
        sub_type_singular = last_target_entry['name']
        sub_number = last_target_entry['number']
        plural = _get_sub_unit_plural(parsed_content, sub_type_singular)
        if not plural:
            if debug_log is not None:
                debug_log.append(f"    ERROR: Could not find plural for sub-unit type '{sub_type_singular}'")
            return False
        sub_unit = lookup_item(parsed_content, plural, sub_number)
        if sub_unit is None:
            if debug_log is not None:
                debug_log.append(f"    ERROR: Could not find sub-unit {sub_type_singular} {sub_number}")
            return False
        if debug_log is not None:
            debug_log.append(f"    Found sub-unit {sub_type_singular} {sub_number}, checking for duplicates...")
        # Check for true duplicate in defined_terms
        defined_terms = sub_unit.setdefault('defined_terms', [])
        for existing_def in defined_terms:
            if _definitions_match(def_entry, existing_def):
                if debug_log is not None:
                    debug_log.append(f"    Found duplicate in sub-unit defined_terms, returning True")
                return True
        defined_terms.append(def_entry)
        if debug_log is not None:
            debug_log.append(f"    SUCCESS: Appended to sub-unit defined_terms")
        return True

    # --- Org-level target handling (existing path) ---
    # Get pointer to target organizational unit
    org_pointer = get_org_pointer_from_context(parsed_content, target_context)
    if not org_pointer:
        if logfile_path:
            log_document_issue(
                logfile_path,
                'conflict_resolution_failed',
                item_type_name=def_entry.get('source_type', 'unknown'),
                item_number=def_entry.get('source_number', 'unknown'),
                issue_description=f"Could not find organizational unit to move definition for term '{def_entry.get('term', 'unknown')}'",
                term=def_entry.get('term', ''),
                target_context=str(target_context)
            )
        if debug_log is not None:
            debug_log.append(f"    ERROR: Could not find org_pointer for target_context")
        return False
    if debug_log is not None:
        debug_log.append(f"    Found org_pointer, checking for duplicates...")
    # Check if definition already exists at this level (true duplicate - same term AND value)
    # If it does, we can safely remove from document level without adding again
    if 'unit_definitions' in org_pointer.keys():
        for existing_def in org_pointer['unit_definitions']:
            if _definitions_match(def_entry, existing_def):
                # True duplicate already exists - safe to remove from document level
                if debug_log is not None:
                    debug_log.append(f"    Found duplicate, returning True")
                return True
    # Add definition to target organizational unit
    source_type = def_entry.get('source_type', 'unknown')
    source_number = def_entry.get('source_number', 'unknown')
    # Store the count and check for existing conflicting definition before adding
    before_count = len(org_pointer.get('unit_definitions', []))
    had_conflicting = False
    if 'unit_definitions' in org_pointer.keys():
        for existing_def in org_pointer['unit_definitions']:
            # Check if there's a conflicting definition (same normalized term, different value)
            if (_normalize_term_for_comparison(existing_def.get('term', '')) ==
                _normalize_term_for_comparison(def_entry.get('term', '')) and
                existing_def.get('value', '').strip() != def_entry.get('value', '').strip()):
                had_conflicting = True
                break
    if debug_log is not None:
        debug_log.append(f"    Before count: {before_count}, had_conflicting: {had_conflicting}")
        debug_log.append(f"    Calling add_org_definition...")
    add_org_definition(
        parsed_content,
        def_entry,
        org_pointer,
        source_type,
        source_number,
        logfile_path,
        proc
    )
    # Verify the definition was actually added or handled
    after_count = len(org_pointer.get('unit_definitions', []))
    
    if debug_log is not None:
        debug_log.append(f"    After count: {after_count}")
    
    # Definition was added if count increased, or if there was a conflicting definition
    # that might have been resolved (replaced existing)
    if after_count > before_count:
        # Successfully added
        if debug_log is not None:
            debug_log.append(f"    SUCCESS: Definition added (count increased)")
        return True
    elif had_conflicting and after_count == before_count:
        # Had conflicting definition - might have been resolved/replaced
        # Check if our definition (or resolved version) is now in the list
        normalized_term = _normalize_term_for_comparison(def_entry.get('term', ''))
        for existing_def in org_pointer.get('unit_definitions', []):
            if _normalize_term_for_comparison(existing_def.get('term', '')) == normalized_term:
                # Definition exists (might be resolved version) - safe to remove from document level
                if debug_log is not None:
                    debug_log.append(f"    SUCCESS: Definition exists (may be resolved version)")
                return True
        
        # Conflicting definition was there but our definition isn't now - log and return False
        if logfile_path:
            log_document_issue(
                logfile_path,
                'conflict_resolution_definition_not_added',
                item_type_name=source_type,
                item_number=source_number,
                issue_description=f"Definition for term '{def_entry.get('term', 'unknown')}' was not added to organizational unit after conflict resolution",
                term=def_entry.get('term', ''),
                target_context=str(target_context)
            )
        if debug_log is not None:
            debug_log.append(f"    ERROR: Had conflicting but definition not found after add")
        return False
    else:
        # No count change and no conflicting definition - something went wrong
        if logfile_path:
            log_document_issue(
                logfile_path,
                'conflict_resolution_definition_not_added',
                item_type_name=source_type,
                item_number=source_number,
                issue_description=f"Definition for term '{def_entry.get('term', 'unknown')}' was not added to organizational unit (unknown reason)",
                term=def_entry.get('term', ''),
                target_context=str(target_context)
            )
        if debug_log is not None:
            debug_log.append(f"    ERROR: No count change and no conflicting definition")
        return False


def _remove_definition_from_level(parsed_content: dict, def_entry: dict,
                                    org_context: List[dict],
                                    debug_log: Optional[List[str]] = None) -> bool:
    """
    Remove a definition from a specific organizational level or sub-unit level.

    When the last entry in org_context is a sub-unit type, removes from that
    sub-unit's defined_terms instead of an org node's unit_definitions.

    Args:
        parsed_content (dict): The document content
        def_entry (dict): Definition dictionary to remove
        org_context (List[dict]): Organizational context path (empty list for document level)
        debug_log (List[str], optional): List to append debug messages to

    Returns:
        bool: True if definition was removed, False otherwise
    """
    if debug_log is not None:
        debug_log.append(f"  _remove_definition_from_level: term='{def_entry.get('term', 'unknown')}', org_context={org_context}")

    if not org_context:
        # Document level
        if ('document_information' not in parsed_content.keys() or
            'document_definitions' not in parsed_content['document_information'].keys()):
            if debug_log is not None:
                debug_log.append(f"    ERROR: No document_definitions found")
            return False

        def_list = parsed_content['document_information']['document_definitions']
        if debug_log is not None:
            debug_log.append(f"    Document level: checking {len(def_list)} definitions")

        for i, existing_def in enumerate(def_list):
            if _definitions_match(def_entry, existing_def):
                if debug_log is not None:
                    debug_log.append(f"    MATCH FOUND at index {i}, removing...")
                    debug_log.append(f"      def_entry: term='{def_entry.get('term', '')}', value='{def_entry.get('value', '')[:50]}...'")
                    debug_log.append(f"      existing_def: term='{existing_def.get('term', '')}', value='{existing_def.get('value', '')[:50]}...'")
                def_list.pop(i)
                if debug_log is not None:
                    debug_log.append(f"    SUCCESS: Removed, new count: {len(def_list)}")
                return True

        if debug_log is not None:
            debug_log.append(f"    ERROR: No match found for removal")
            for i, existing_def in enumerate(def_list):
                debug_log.append(f"      [{i}] term='{existing_def.get('term', '')}', value='{existing_def.get('value', '')[:50]}...'")
        return False

    # Sub-unit level
    last_entry = org_context[-1]
    if _is_sub_unit_type(parsed_content, last_entry.get('name', '')):
        plural = _get_sub_unit_plural(parsed_content, last_entry['name'])
        if not plural:
            if debug_log is not None:
                debug_log.append(f"    ERROR: No plural for sub-unit type '{last_entry['name']}'")
            return False
        sub_unit = lookup_item(parsed_content, plural, last_entry['number'])
        if sub_unit is None:
            if debug_log is not None:
                debug_log.append(f"    ERROR: Sub-unit {last_entry['name']} {last_entry['number']} not found")
            return False
        def_list = sub_unit.get('defined_terms', [])
        if debug_log is not None:
            debug_log.append(f"    Sub-unit level: checking {len(def_list)} defined_terms")
        for i, existing_def in enumerate(def_list):
            if _definitions_match(def_entry, existing_def):
                if debug_log is not None:
                    debug_log.append(f"    MATCH FOUND at index {i}, removing...")
                def_list.pop(i)
                if debug_log is not None:
                    debug_log.append(f"    SUCCESS: Removed, new count: {len(def_list)}")
                return True
        if debug_log is not None:
            debug_log.append(f"    ERROR: No match found in sub-unit defined_terms")
        return False

    # Organizational level
    org_pointer = get_org_pointer_from_context(parsed_content, org_context)
    if not org_pointer or 'unit_definitions' not in org_pointer.keys():
        if debug_log is not None:
            debug_log.append(f"    ERROR: No org_pointer or unit_definitions")
        return False

    def_list = org_pointer['unit_definitions']
    if debug_log is not None:
        debug_log.append(f"    Org level: checking {len(def_list)} definitions")

    for i, existing_def in enumerate(def_list):
        if _definitions_match(def_entry, existing_def):
            if debug_log is not None:
                debug_log.append(f"    MATCH FOUND at index {i}, removing...")
            def_list.pop(i)
            if debug_log is not None:
                debug_log.append(f"    SUCCESS: Removed, new count: {len(def_list)}")
            return True

    if debug_log is not None:
        debug_log.append(f"    ERROR: No match found for removal")
    return False


def resolve_document_level_conflicts(parsed_content: dict,
                                      logfile_path: Optional[str] = None,
                                      proc=None,
                                      elaborative_client=None) -> int:
    """
    Resolve conflicts among document-level definitions by moving non-scoped conflicting
    definitions down the organizational tree toward their source units.
    
    This function implements the following algorithm:
    1. Find all conflicts at document level (same term, different values)
    2. For each conflicting term, check for elaborative relationships (sibling substantive units)
    3. If elaborative, mark appropriately and don't treat as conflicting
    4. For remaining conflicts, check if definitions are effectively equivalent
    5. If equivalent, keep the better definition and remove duplicates
    6. For remaining non-equivalent conflicts, identify non-scoped definitions (exclude those with express scoping)
    7. Move each non-scoped conflicting definition down one level following the path to its source
    8. If conflicts remain at the new level, repeat the process
    9. Continue until no conflicts remain or definition reaches level above source unit
    
    Note: Equivalency checks may have already been performed during initial definition placement
    (in add_org_definition, add_no_scope_definition, etc.). This function checks again because:
    - Conflicts may have been logged but not resolved during initial placement
    - New conflicts may have emerged after all definitions were placed
    - The context of checking all conflicts at a level together may yield different results
    
    Args:
        parsed_content (dict): The document content
        logfile_path (str, optional): Path to document issues logfile
        proc: Optional processor instance (for compatibility with other functions)
        
    Returns:
        int: Number of definitions moved
    """
    if ('document_information' not in parsed_content.keys() or
        'document_definitions' not in parsed_content['document_information'].keys()):
        return 0
    
    total_moved = 0
    max_iterations = 100  # Safety limit to prevent infinite loops
    iteration = 0
    debug_log = []  # Collect debug messages
    
    while iteration < max_iterations:
        iteration += 1
        debug_log.append(f"\n=== Iteration {iteration} ===")
        
        # Get all document-level definitions
        doc_definitions = parsed_content['document_information']['document_definitions']
        debug_log.append(f"Document-level definitions count: {len(doc_definitions)}")
        
        if not doc_definitions:
            break  # No definitions left at document level
        
        # Find conflicts at document level
        conflicts = _find_conflicts_at_level(doc_definitions)
        debug_log.append(f"Found {len(conflicts)} conflicting terms")
        
        if not conflicts:
            break  # No conflicts remaining
        
        # Process each conflicting term
        moved_this_iteration = 0
        
        for term, conflicting_defs in conflicts.items():
            debug_log.append(f"\nProcessing term '{term}' with {len(conflicting_defs)} conflicting definitions")
            
            # Step 1: Check for elaborative relationships (sibling substantive units)
            # Only check if definitions come from sibling substantive units
            elaborative_relationships = []
            non_elaborative_defs = []
            
            for i in range(len(conflicting_defs)):
                def1 = conflicting_defs[i]
                is_elaborative = False
                
                for j in range(i + 1, len(conflicting_defs)):
                    def2 = conflicting_defs[j]
                    
                    # Check if they're siblings (same organizational parent)
                    source_type1 = def1.get('source_type')
                    source_number1 = def1.get('source_number')
                    source_type2 = def2.get('source_type')
                    source_number2 = def2.get('source_number')
                    
                    if (source_type1 and source_number1 and source_type2 and source_number2 and
                        _are_sibling_substantive_units(parsed_content, source_type1, source_number1, 
                                                       source_type2, source_number2)):
                        
                        # They're siblings - check if one elaborates on the other
                        elaborative_result = _detect_elaborative_relationship(proc, def1, def2, logfile_path, elaborative_client)
                        
                        if elaborative_result and elaborative_result.get('is_elaborative'):
                            elaborative_def = elaborative_result['elaborative_def']
                            direct_def = elaborative_result['direct_def']
                            
                            # Mark the elaborative definition
                            elaborative_def['def_kind'] = 'elaboration'
                            
                            # Associate elaborative with direct (store source info)
                            if 'elaborates_on' not in elaborative_def:
                                elaborative_def['elaborates_on'] = {
                                    'source_type': direct_def.get('source_type'),
                                    'source_number': direct_def.get('source_number')
                                }
                            
                            elaborative_relationships.append({
                                'direct': direct_def,
                                'elaborative': elaborative_def
                            })
                            is_elaborative = True
                            debug_log.append(f"  Detected elaborative: {elaborative_def.get('source_type')} {elaborative_def.get('source_number')} elaborates on {direct_def.get('source_type')} {direct_def.get('source_number')}")
                            break
                
                if not is_elaborative:
                    # Check if this definition is already marked as elaborative in a relationship
                    found_in_relationship = False
                    for rel in elaborative_relationships:
                        if def1 is rel['direct'] or def1 is rel['elaborative']:
                            found_in_relationship = True
                            break
                    if not found_in_relationship:
                        non_elaborative_defs.append(def1)
            
            # If we found elaborative relationships, group them with their direct definitions
            # Elaborative definitions should move WITH their direct definitions
            # Create groups: each group contains a direct definition and its elaborative definition(s)
            definition_groups = []
            used_defs = set()
            
            # First, create groups for elaborative relationships
            for rel in elaborative_relationships:
                direct_def = rel['direct']
                elaborative_def = rel['elaborative']
                definition_groups.append({
                    'direct': direct_def,
                    'elaborative': elaborative_def,
                    'defs': [direct_def, elaborative_def]  # Both move together
                })
                used_defs.add(id(direct_def))
                used_defs.add(id(elaborative_def))
                debug_log.append(f"  Created group: direct={direct_def.get('source_type')} {direct_def.get('source_number')}, elaborative={elaborative_def.get('source_type')} {elaborative_def.get('source_number')}")
            
            # Add remaining non-elaborative definitions as individual groups
            for def_entry in non_elaborative_defs:
                if id(def_entry) not in used_defs:
                    definition_groups.append({
                        'direct': def_entry,
                        'elaborative': None,
                        'defs': [def_entry]  # Single definition moves alone
                    })
                    used_defs.add(id(def_entry))
            
            # Now check for conflicts between groups (conflicts between direct definitions)
            # If two groups conflict (their direct definitions conflict), both groups need to move down
            # A group conflicts if its direct definition conflicts with ANY other group's direct definition
            conflicting_groups = []
            for i in range(len(definition_groups)):
                group1 = definition_groups[i]
                has_conflict = False
                for j in range(len(definition_groups)):
                    if i == j:
                        continue
                    group2 = definition_groups[j]
                    # Check if direct definitions conflict (same normalized term, different values)
                    normalized_term1 = _normalize_term_for_comparison(group1['direct'].get('term', ''))
                    normalized_term2 = _normalize_term_for_comparison(group2['direct'].get('term', ''))
                    value1 = group1['direct'].get('value', '').strip()
                    value2 = group2['direct'].get('value', '').strip()
                    
                    if (normalized_term1 == normalized_term2 and value1 != value2):
                        has_conflict = True
                        break
                if has_conflict:
                    conflicting_groups.append(group1)
            
            if elaborative_relationships:
                debug_log.append(f"  Found {len(elaborative_relationships)} elaborative relationship(s)")
                debug_log.append(f"  Created {len(definition_groups)} definition groups, {len(conflicting_groups)} conflicting groups")
            
            # Step 2: For conflicting groups, check if their direct definitions are effectively equivalent
            # Only proceed with moving definitions down if they are NOT equivalent
            groups_to_move = []
            
            if proc is not None and len(conflicting_groups) >= 2:
                debug_log.append(f"  Checking {len(conflicting_groups)} conflicting groups for equivalency")
                # Try to resolve conflicts pairwise between groups
                remaining_groups = conflicting_groups.copy()
                resolved_groups = []
                
                while len(remaining_groups) >= 2:
                    group1 = remaining_groups[0]
                    resolved_with_others = False
                    
                    for i in range(1, len(remaining_groups)):
                        group2 = remaining_groups[i]
                        
                        # Check if direct definitions conflict
                        if _check_conflicting_direct_definitions(group1['direct'], group2['direct']):
                            # Try to resolve using AI
                            resolved_def = experimental_resolve_conflicting_definitions(
                                proc, group1['direct'], group2['direct'], logfile_path
                            )
                            
                            if resolved_def is not None:
                                # Definitions are effectively equivalent - use the resolved one
                                # Remove all definitions from both groups from document level
                                for def_entry in group1['defs']:
                                    _remove_definition_from_level(parsed_content, def_entry, [], debug_log)
                                for def_entry in group2['defs']:
                                    _remove_definition_from_level(parsed_content, def_entry, [], debug_log)
                                
                                # Add the resolved definition back
                                if 'document_information' in parsed_content.keys():
                                    if 'document_definitions' not in parsed_content['document_information'].keys():
                                        parsed_content['document_information']['document_definitions'] = []
                                    parsed_content['document_information']['document_definitions'].append(resolved_def)
                                
                                debug_log.append(f"  Resolved group1+group2 as equivalent, added resolved_def back")
                                moved_this_iteration += 1  # Count as processed (even though we're not moving down)
                                
                                remaining_groups.remove(group1)
                                remaining_groups.remove(group2)
                                resolved_with_others = True
                                break
                    
                    if not resolved_with_others:
                        # group1 couldn't be resolved with any other group - keep it for moving down
                        resolved_groups.append(remaining_groups.pop(0))
                    else:
                        # Continue with next iteration
                        continue
                
                # Add any remaining groups
                resolved_groups.extend(remaining_groups)
                
                # If all conflicts were resolved, skip moving definitions down
                if len(resolved_groups) <= 1:
                    debug_log.append(f"  All conflicts resolved, skipping move-down")
                    continue
                
                # Use resolved_groups (which may have fewer items if some were resolved)
                groups_to_move = resolved_groups
            else:
                # No equivalency checking or not enough groups - use conflicting groups
                groups_to_move = conflicting_groups
            
            # Filter groups to those with non-scoped direct definitions (exclude those with express scoping)
            groups_to_move = [g for g in groups_to_move if not _has_express_scoping(g['direct'])]
            debug_log.append(f"  Groups to move: {len(groups_to_move)}")
            
            if not groups_to_move:
                # All conflicting groups have express scoping - skip this term
                debug_log.append(f"  All groups have express scoping, skipping")
                continue
            
            # Move each group down one level (direct definition + its elaborative definition if any)
            for group in groups_to_move:
                direct_def = group['direct']
                elaborative_def = group.get('elaborative')
                
                debug_log.append(f"\n  Moving group: direct={direct_def.get('source_type', 'unknown')} {direct_def.get('source_number', 'unknown')}, elaborative={elaborative_def.get('source_type', 'unknown') if elaborative_def else 'none'} {elaborative_def.get('source_number', 'unknown') if elaborative_def else ''}")
                
                # Use the direct definition's source for determining where to move
                source_type = direct_def.get('source_type')
                source_number = direct_def.get('source_number')
                
                if not source_type or not source_number:
                    # Can't determine source - skip this group
                    if logfile_path:
                        log_document_issue(
                            logfile_path,
                            'conflict_resolution_skipped',
                            item_type_name='unknown',
                            item_number='unknown',
                            issue_description=f"Could not resolve conflict for term '{term}' - missing source information",
                            term=term
                        )
                    debug_log.append(f"    ERROR: Missing source information")
                    continue
                
                # Get source unit's organizational context (use direct definition's source)
                source_context = _get_source_context(parsed_content, source_type, source_number)
                
                if not source_context:
                    # Can't find source context - skip this group
                    if logfile_path:
                        log_document_issue(
                            logfile_path,
                            'conflict_resolution_skipped',
                            item_type_name=source_type,
                            item_number=source_number,
                            issue_description=f"Could not find organizational context for source unit of term '{term}'",
                            term=term
                        )
                    debug_log.append(f"    ERROR: Could not find source context")
                    continue
                
                debug_log.append(f"    Source context: {source_context}")
                
                # Move all definitions in the group together (they all move to the same location)
                all_moved = True
                for def_entry in group['defs']:
                    debug_log.append(f"    Moving definition: term='{def_entry.get('term', 'unknown')}', source={def_entry.get('source_type', 'unknown')} {def_entry.get('source_number', 'unknown')}")
                    
                    # We're at document level (empty org_context), so we can always move down
                    # if there's at least one level in source_context
                    # Move definition down one level (all in group move to same location)
                    current_level = []  # Document level
                    move_success = _move_definition_down_one_level(parsed_content, def_entry, source_context, current_level, logfile_path, proc, debug_log)
                    
                    if move_success:
                        # Verify definition was actually added to target level
                        # Target context is first level of source_context when moving from document level
                        target_context_for_verify = source_context[:1]  # First level when moving from document level
                        normalized_term = _normalize_term_for_comparison(def_entry.get('term', ''))

                        # Check whether the target is a sub-unit or an org-hierarchy node
                        first_entry = target_context_for_verify[0] if target_context_for_verify else {}
                        if _is_sub_unit_type(parsed_content, first_entry.get('name', '')):
                            # Target is a sub-unit: verify against defined_terms
                            plural = _get_sub_unit_plural(parsed_content, first_entry['name'])
                            sub_unit = lookup_item(parsed_content, plural, first_entry['number']) if plural else None
                            if sub_unit is not None:
                                found_in_org = False
                                for existing_def in sub_unit.get('defined_terms', []):
                                    if (_normalize_term_for_comparison(existing_def.get('term', '')) == normalized_term and
                                        existing_def.get('value', '').strip() == def_entry.get('value', '').strip()):
                                        found_in_org = True
                                        break
                                if not found_in_org:
                                    debug_log.append(f"      ERROR: Definition not found in sub-unit defined_terms after move!")
                                    all_moved = False
                                    continue
                        else:
                            org_pointer_check = get_org_pointer_from_context(parsed_content, target_context_for_verify)
                            if org_pointer_check and 'unit_definitions' in org_pointer_check.keys():
                                found_in_org = False
                                for existing_def in org_pointer_check['unit_definitions']:
                                    if (_normalize_term_for_comparison(existing_def.get('term', '')) == normalized_term and
                                        existing_def.get('value', '').strip() == def_entry.get('value', '').strip()):
                                        found_in_org = True
                                        break
                                if not found_in_org:
                                    debug_log.append(f"      ERROR: Definition not found in org level after move!")
                                    all_moved = False
                                    continue
                        
                        # Remove from document level
                        remove_success = _remove_definition_from_level(parsed_content, def_entry, [], debug_log)
                        if remove_success:
                            debug_log.append(f"      SUCCESS: Moved and removed")
                        else:
                            debug_log.append(f"      WARNING: Moved but failed to remove from document level!")
                            all_moved = False
                            # Log this as an error
                            if logfile_path:
                                log_document_issue(
                                    logfile_path,
                                    'conflict_resolution_removal_failed',
                                    item_type_name=def_entry.get('source_type', 'unknown'),
                                    item_number=def_entry.get('source_number', 'unknown'),
                                    issue_description=f"Definition for term '{term}' was moved to org level but could not be removed from document level",
                                    term=term,
                                    source_context=str(source_context)
                                )
                    else:
                        debug_log.append(f"      ERROR: Failed to move definition down")
                        all_moved = False
                
                if all_moved:
                    moved_this_iteration += len(group['defs'])
                    total_moved += len(group['defs'])
                    debug_log.append(f"    SUCCESS: All definitions in group moved")
                else:
                    debug_log.append(f"    WARNING: Some definitions in group failed to move")
        
        if moved_this_iteration == 0:
            # No definitions were moved this iteration - stop
            debug_log.append(f"\nNo definitions moved this iteration, stopping")
            break
        
        # Check for conflicts at the new locations and continue if needed
        # (This will be handled in the next iteration by checking document level again,
        # and conflicts at organizational levels will be handled separately if needed)
    
    if iteration >= max_iterations:
        if logfile_path:
            log_document_issue(
                logfile_path,
                'conflict_resolution_max_iterations',
                item_type_name='unknown',
                item_number='unknown',
                issue_description=f"Conflict resolution reached maximum iterations ({max_iterations})",
            )
        debug_log.append(f"\nWARNING: Reached max iterations")
    
    return total_moved


def resolve_conflicts_at_org_level(parsed_content: dict,
                                    org_context: List[dict],
                                    logfile_path: Optional[str] = None,
                                    proc=None,
                                    elaborative_client=None) -> int:
    """
    Resolve conflicts at a specific organizational level by moving non-scoped conflicting
    definitions down one level toward their source units.
    
    This is called recursively to handle conflicts that appear at organizational levels
    after definitions are moved down from document level.
    
    Args:
        parsed_content (dict): The document content
        org_context (List[dict]): Organizational context path for the level to check
        logfile_path (str, optional): Path to document issues logfile
        proc: Optional processor instance
        
    Returns:
        int: Number of definitions moved
    """
    total_moved = 0
    max_iterations = 100  # Safety limit
    iteration = 0
    while iteration < max_iterations:
        iteration += 1

        # Get definitions at this level (refresh each iteration as definitions may have moved)
        definitions = _get_definitions_at_org_level(parsed_content, org_context)
        if not definitions:
            break  # No definitions left at this level

        # Find conflicts
        conflicts = _find_conflicts_at_level(definitions)
        if not conflicts:
            break  # No conflicts remaining
        
        moved_this_iteration = 0
        
        # Process each conflicting term
        for term, conflicting_defs in conflicts.items():
            # First, try to resolve conflicts by checking if definitions are effectively equivalent
            # Only proceed with moving definitions down if they are NOT equivalent
            if proc is not None and len(conflicting_defs) >= 2:
                # Try to resolve conflicts pairwise
                resolved_defs = []
                remaining_defs = conflicting_defs.copy()
                while len(remaining_defs) >= 2:
                    def1 = remaining_defs[0]
                    resolved_with_others = False
                    
                    for i in range(1, len(remaining_defs)):
                        def2 = remaining_defs[i]
                        # Check if these are conflicting direct definitions
                        if _check_conflicting_direct_definitions(def1, def2):
                            # Try to resolve using AI
                            resolved_def = experimental_resolve_conflicting_definitions(
                                proc, def1, def2, logfile_path
                            )
                            if resolved_def is not None:
                                # Definitions are effectively equivalent - use the resolved one
                                resolved_defs.append(resolved_def)
                                remaining_defs.remove(def1)
                                remaining_defs.remove(def2)
                                resolved_with_others = True
                                
                                # Remove both original definitions from this level
                                _remove_definition_from_level(parsed_content, def1, org_context)
                                _remove_definition_from_level(parsed_content, def2, org_context)
                                # Add the resolved definition back to this level
                                org_pointer = get_org_pointer_from_context(parsed_content, org_context)
                                if org_pointer:
                                    if 'unit_definitions' not in org_pointer.keys():
                                        org_pointer['unit_definitions'] = []
                                    org_pointer['unit_definitions'].append(resolved_def)
                                
                                moved_this_iteration += 1  # Count as processed
                                break
                    
                    if not resolved_with_others:
                        # def1 couldn't be resolved with any other definition - keep it for moving down
                        resolved_defs.append(remaining_defs.pop(0))
                    else:
                        # Continue with next iteration
                        continue
                # Add any remaining definitions
                resolved_defs.extend(remaining_defs)
                
                # If all conflicts were resolved, skip moving definitions down
                if len(resolved_defs) <= 1:
                    continue
                
                # Use resolved_defs (which may have fewer items if some were resolved)
                conflicting_defs = resolved_defs
            # Filter to non-scoped definitions (exclude those with express scoping)
            non_scoped_defs = [d for d in conflicting_defs if not _has_express_scoping(d)]
            
            if not non_scoped_defs:
                # All conflicting definitions have express scoping - skip this term
                continue
            # Move each non-scoped definition down one level
            for def_entry in non_scoped_defs:
                source_type = def_entry.get('source_type')
                source_number = def_entry.get('source_number')
                if not source_type or not source_number:
                    continue
                # Get source unit's organizational context
                source_context = _get_source_context(parsed_content, source_type, source_number)
                if not source_context:
                    continue
                
                # Check if we've already reached the level above the source unit
                # If org_context is as deep as or deeper than source_context, we're at or past the source level
                # We want to stop when we reach the level above the source unit
                # The source unit is at the deepest level in source_context, so we stop when
                # org_context length equals source_context length (we're at the level above the source)
                if len(org_context) >= len(source_context):
                    # Already at or below the level we should be at - stop moving this definition
                    # This definition should remain at this level (or become unit-specific)
                    continue
                # Move definition down one level (from current org_context toward source)
                if _move_definition_down_one_level(parsed_content, def_entry, source_context, org_context, logfile_path, proc):
                    # Remove from current level
                    _remove_definition_from_level(parsed_content, def_entry, org_context)
                    moved_this_iteration += 1
                    total_moved += 1
                    
                    # Get the target context where definition was moved
                    if len(org_context) < len(source_context):
                        next_level_context = source_context[:len(org_context) + 1]
                        # Recursively check for conflicts at the new level
                        total_moved += resolve_conflicts_at_org_level(
                            parsed_content,
                            next_level_context,
                            logfile_path,
                            proc,
                            elaborative_client
                        )
        
        if moved_this_iteration == 0:
            # No definitions were moved this iteration - stop
            break
    
    # After resolving conflicts at this level, recursively check deeper levels
    # that might have received definitions
    from utils import get_organizational_item_name_set
    
    org_name_set = get_organizational_item_name_set(parsed_content)
    org_pointer = get_org_pointer_from_context(parsed_content, org_context)
    
    if org_pointer:
        for level_name in org_pointer.keys():
            if level_name in org_name_set:
                for num in org_pointer[level_name].keys():
                    next_level_context = org_context + [{"name": level_name, "number": num}]
                    total_moved += resolve_conflicts_at_org_level(
                        parsed_content,
                        next_level_context,
                        logfile_path,
                        proc,
                        elaborative_client
                    )
    
    return total_moved


def resolve_all_definition_conflicts(parsed_content: dict,
                                      logfile_path: Optional[str] = None,
                                      proc=None) -> int:
    """
    Resolve all definition conflicts throughout the document by iteratively moving
    non-scoped conflicting definitions down the organizational tree.

    This function:
    1. Resolves conflicts at document level (which may move definitions to org levels)
    2. Recursively resolves conflicts at all organizational levels (starting from top level)

    Args:
        parsed_content (dict): The document content
        logfile_path (str, optional): Path to document issues logfile
        proc: Optional processor instance

    Returns:
        int: Total number of definitions moved
    """
    total_moved = 0
    # Create task-specific clients if config is available
    if proc and proc.config:
        conflict_client = create_client_for_task(proc.config, 'stage2.definitions.resolve_conflict')
        elaborative_client = create_client_for_task(proc.config, 'stage2.definitions.detect_elaborative')
    else:
        # Fallback to None (functions will use proc.client)
        conflict_client = None
        elaborative_client = None
    # First, resolve conflicts at document level
    # This will move definitions down and may create conflicts at org levels
    total_moved += resolve_document_level_conflicts(parsed_content, logfile_path, proc, elaborative_client)
    # Then, resolve conflicts at organizational levels starting from the top level
    # We process top-level org units, and resolve_conflicts_at_org_level will
    # recursively handle deeper levels as needed
    from utils import get_organizational_item_name_set, get_org_pointer_from_context
    
    org_name_set = get_organizational_item_name_set(parsed_content)
    org_pointer = get_org_pointer_from_context(parsed_content, [])
    
    if org_pointer:
        # Process each top-level organizational unit
        for level_name in org_pointer.keys():
            if level_name in org_name_set:
                for num in org_pointer[level_name].keys():
                    org_context = [{"name": level_name, "number": num}]
                    # *** Bug occurs in next function call. ***
                    # This will recursively handle all deeper levels
                    moved = resolve_conflicts_at_org_level(parsed_content, org_context, logfile_path, proc, elaborative_client)
                    total_moved += moved

    return total_moved
