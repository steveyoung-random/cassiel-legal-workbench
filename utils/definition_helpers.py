"""
Definition processing helper functions.

This module provides utility functions for scope resolution, unit matching, and
organizational path finding used in definition processing.
"""
# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License

import re
from .text_processing import canonical_org_types
from .document_handling import get_full_item_name_set, get_organizational_item_name_set, get_item_numbers_for_type



def strip_sub_prefix(element_type):
    """
    Strip "sub" or "sub-" prefix from element type name.
    
    Args:
        element_type (str): Element type name (e.g., "subsection", "sub-section")
        
    Returns:
        str: Element type with "sub" prefix removed, or original if no prefix
    """
    # Use regex to match "sub" or "sub-" at the beginning (case-insensitive)
    pattern = r'^sub-?'
    result = re.sub(pattern, '', element_type, flags=re.IGNORECASE)
    return result if result != element_type else element_type


def find_substantive_unit_with_maximum_matching(parsed_content, element_type, designation):
    """
    Find a substantive unit by trying exact match first, then longest backward-prefix match.

    Supports both top-level content types and sub-unit types (v0.5 schema), using
    get_item_numbers_for_type so that sub-units nested inside parent containers are
    visible to this function.

    'Backward-prefix match' means: the given designation starts with a unit's designation,
    followed immediately by a non-alphanumeric separator character (e.g. '.', '(', '-').
    This handles scope phrases that reference a sub-paragraph of a unit, such as
    '9E003.a.8' resolving to ECCN '9E003', or '282(b)(7)(B)(i)' resolving to section '282'.
    The longest matching unit designation is preferred (most specific enclosing unit).

    Args:
        parsed_content (dict): The document content
        element_type (str): Type of substantive unit (e.g., "section", "article", "eccn")
        designation (str): Designation/number to match (e.g., "56(g)", "9E003.a.8")

    Returns:
        tuple: (matched_type, matched_designation) if found, (None, None) otherwise
    """
    element_type, element_type_plural = canonical_org_types(element_type)

    # Compute base type by stripping any "sub" prefix (e.g. "subsection" -> "section")
    stripped = strip_sub_prefix(element_type)
    if stripped != element_type:
        base_type, base_type_plural = canonical_org_types(stripped)
    else:
        base_type, base_type_plural = None, None

    # Get all unit numbers for each type (handles both top-level and sub-unit types).
    # Use a set for O(1) membership tests in the exact-match step.
    unit_numbers = set(get_item_numbers_for_type(parsed_content, element_type_plural))
    base_unit_numbers = set(get_item_numbers_for_type(parsed_content, base_type_plural)) if base_type_plural else set()

    designation_str = str(designation)

    # 1. Exact match with original type
    if designation_str in unit_numbers:
        return (element_type, designation_str)

    # 2. Exact match with base type (sub-prefix stripped)
    if base_type and designation_str in base_unit_numbers:
        return (base_type, designation_str)

    # 3. Backward-prefix match: find the longest unit designation U such that
    #    designation starts with U and designation[len(U)] is non-alphanumeric.
    #    This handles sub-paragraph references like '9E003.a.8' -> ECCN '9E003'
    #    and '282(b)(7)(B)(i)' -> section '282'.
    def best_backward_match(numbers, unit_type):
        best = None
        best_len = 0
        for num in numbers:
            num_str = str(num)
            n = len(num_str)
            if (n < len(designation_str)
                    and designation_str.startswith(num_str)
                    and not designation_str[n].isalnum()):
                if n > best_len:
                    best_len = n
                    best = num_str
        return (unit_type, best) if best else (None, None)

    result = best_backward_match(unit_numbers, element_type)
    if result[0]:
        return result

    if base_type:
        result = best_backward_match(base_unit_numbers, base_type)
        if result[0]:
            return result

    return (None, None)


def expand_element_range(parsed_content, element_type, first_designation, last_designation):
    """
    Expand a range of substantive units to include all intermediate units.

    Supports both top-level content types and sub-unit types (v0.5 schema), using
    get_item_numbers_for_type so that sub-units nested inside parent containers are
    visible to this function. Document order is preserved (not sorted).

    Args:
        parsed_content (dict): The document content
        element_type (str): Type of substantive unit (e.g., "section", "article")
        first_designation (str): First unit in range
        last_designation (str): Last unit in range

    Returns:
        list: List of all unit designations in the range (inclusive)
    """
    element_type, element_type_plural = canonical_org_types(element_type)

    # Get unit numbers in document order; handles both top-level and sub-unit types.
    unit_list = get_item_numbers_for_type(parsed_content, element_type_plural)
    if not unit_list:
        # Try stripping "sub" prefix
        stripped = strip_sub_prefix(element_type)
        if stripped != element_type:
            _, base_type_plural = canonical_org_types(stripped)
            unit_list = get_item_numbers_for_type(parsed_content, base_type_plural)
        if not unit_list:
            return []  # Type doesn't exist

    unit_set = set(unit_list)
    if first_designation not in unit_set or last_designation not in unit_set:
        return []  # Range endpoints not found

    return_list = []
    in_range = False
    for designation in unit_list:
        if designation == first_designation:
            in_range = True
            return_list.append(designation)
        elif designation == last_designation:
            in_range = False
            return_list.append(designation)
        elif in_range:
            return_list.append(designation)

    return return_list


def resolve_current_from_context(parsed_content, element_type, current_item_context):
    """
    Resolve a "current" reference to an actual designation and build its organizational path.
    
    Args:
        parsed_content (dict): The document content
        element_type (str): Type of element to resolve (e.g., "chapter", "part")
        current_item_context (list): List of dicts representing the current item's organizational path
                                     Each entry should have 'name' and 'number' keys
        
    Returns:
        tuple: (designation, path_list) where designation is the actual designation (str) 
               and path_list is the organizational path (list of dicts) up to that unit.
               Returns (None, None) if not found.
    """
    # Check if element_type is a substantive unit type
    substantive_unit_types = get_full_item_name_set(parsed_content)
    
    # Check if this is a substantive unit reference
    # For substantive units, we may need to strip "sub" prefix (e.g., "subsection" -> "section")
    base_type = strip_sub_prefix(element_type)
    is_substantive = (element_type in substantive_unit_types or base_type in substantive_unit_types)
    
    if is_substantive:
        # For substantive units, "current" means the current item itself
        # This should be handled by the caller, not here
        return (None, None)
    
    # For organizational units, do NOT strip "sub" prefix - "subchapter" is different from "chapter"
    # Look through the context from highest to lowest level
    # We want to find the first (highest) occurrence of this type in the context
    # and build the path up to that point
    # Note: current_item_context is a list of single-key dictionaries like [{"title": "42"}, {"chapter": "6A"}]
    path_list = []
    for context_entry in current_item_context:
        # Context entries are single-key dictionaries like {"title": "42"}
        if not isinstance(context_entry, dict) or len(context_entry) != 1:
            continue
        
        key = next(iter(context_entry))
        value = context_entry[key]
        
        # Add this level to the path
        path_list.append({key: value})
        
        # Try exact match only - do not strip "sub" prefix for organizational units
        if key == element_type:
            return (value, path_list)
    
    return (None, None)


def find_organizational_unit_path(parsed_content, element_type, element_designation, current_item_context):
    """
    Find an organizational unit in the document structure and build its full path from root.
    
    This function finds the instance of the named unit that is closest to the working unit by:
    1. Starting from the organizational unit directly above the working unit
    2. Doing a full tree search from that point (excluding any previously searched subtree)
    3. If not found, moving one level up the organizational tree and searching again
    4. Continuing until the unit is found or the root is reached
    
    This approach handles all cases:
    - Units at the same level (siblings)
    - Units higher in the hierarchy (parents, grandparents, etc.)
    - Units lower in the hierarchy (children, grandchildren, etc.)
    - Units unrelated to context (falls back to root search)
    
    Args:
        parsed_content (dict): The document content
        element_type (str): Type of organizational unit (e.g., "chapter", "part")
        element_designation (str): Designation/number of the unit
        current_item_context (list): Current item's organizational context (used as hint for search)
                                    List of dicts like [{"title": "42"}, {"chapter": "6A"}, {"subchapter": "I"}]
        
    Returns:
        list: Organizational path as list of dicts (e.g., [{"title": "42"}, {"chapter": "6A"}])
              Returns None if unit not found
    """
    if 'document_information' not in parsed_content:
        return None
    if 'organization' not in parsed_content['document_information']:
        return None
    if 'content' not in parsed_content['document_information']['organization']:
        return None
    
    org_content = parsed_content['document_information']['organization']['content']
    org_name_set = get_organizational_item_name_set(parsed_content)
    
    # Helper function to recursively search for the unit, excluding a specific subtree
    def search_org_structure(current_level, path_so_far, target_type, target_designation, exclude_path=None):
        """
        Search for target unit in the tree, excluding a specific subtree if provided.
        
        Args:
            current_level: Current level in org structure
            path_so_far: Path from root to current_level
            target_type: Type of unit to find
            target_designation: Designation of unit to find
            exclude_path: Path to exclude (list of dicts), or None to exclude nothing
        """
        # Check if current level has the target type (exact match only)
        if target_type in current_level:
            if target_designation in current_level[target_type]:
                # Found it! Return the path
                new_path = path_so_far + [{target_type: target_designation}]
                return new_path
        
        # Recursively search sub-levels
        for level_name in current_level.keys():
            if level_name in org_name_set:
                for level_number in current_level[level_name].keys():
                    new_path = path_so_far + [{level_name: level_number}]
                    
                    # Check if this path should be excluded
                    should_exclude = False
                    if exclude_path is not None and len(exclude_path) > 0:
                        # Check if this path matches the exclude_path (i.e., is a descendant of it)
                        if len(new_path) >= len(exclude_path):
                            matches_exclude = True
                            for i, exclude_entry in enumerate(exclude_path):
                                if i >= len(new_path):
                                    matches_exclude = False
                                    break
                                exclude_key = next(iter(exclude_entry))
                                exclude_value = exclude_entry[exclude_key]
                                path_key = next(iter(new_path[i]))
                                path_value = new_path[i][path_key]
                                if exclude_key != path_key or exclude_value != path_value:
                                    matches_exclude = False
                                    break
                            if matches_exclude:
                                should_exclude = True
                    
                    if not should_exclude:
                        result = search_org_structure(
                            current_level[level_name][level_number],
                            new_path,
                            target_type,
                            target_designation,
                            exclude_path
                        )
                        if result:
                            return result
        
        return None
    
    # If we have context, start from the organizational unit directly above the working unit
    if current_item_context and isinstance(current_item_context, list) and len(current_item_context) > 0:
        # Navigate to the current item's context location in the org structure
        context_pointer = org_content
        context_path = []
        context_valid = True
        
        # Try to navigate through the full context path
        for context_entry in current_item_context:
            if not isinstance(context_entry, dict) or len(context_entry) != 1:
                context_valid = False
                break
            
            context_key = next(iter(context_entry))
            context_value = context_entry[context_key]
            
            # Check if this level exists in the org structure
            if context_key in context_pointer and context_value in context_pointer[context_key]:
                context_path.append({context_key: context_value})
                context_pointer = context_pointer[context_key][context_value]
            else:
                # Either a sub-unit type (e.g. ccl_category, eccn) or an unexpected entry.
                # Stop here — context collected so far is still valid and usable.
                break
        
        if context_valid and len(context_path) > 0:
            # Start from the organizational unit directly above the working unit
            # (i.e., the last entry in context_path, which is the deepest org unit containing the working unit)
            search_start_path = context_path
            search_start_pointer = context_pointer
            exclude_path = None  # Initially, exclude nothing
            
            # Iteratively search, moving up one level each time if not found
            while search_start_path is not None:
                # Search the full tree from current search start point, excluding previously searched subtree
                result = search_org_structure(
                    search_start_pointer,
                    search_start_path,
                    element_type,
                    element_designation,
                    exclude_path
                )
                
                if result:
                    return result
                
                # Not found at this level - move up one level
                if len(search_start_path) > 0:
                    # Record the current search point as the exclusion for next iteration
                    exclude_path = search_start_path.copy()
                    
                    # Move up one level
                    search_start_path = search_start_path[:-1]
                    
                    # Navigate to the new search start point
                    search_start_pointer = org_content
                    for entry in search_start_path:
                        entry_key = next(iter(entry))
                        entry_value = entry[entry_key]
                        search_start_pointer = search_start_pointer[entry_key][entry_value]
                else:
                    # Already at root - no more levels to search
                    break
    
    # Fall back to searching from root (handles case where context wasn't available or invalid)
    # This is the same as the iterative approach but starting from root with no exclusions
    result = search_org_structure(org_content, [], element_type, element_designation, None)

    return result


def _find_organizational_unit_in_tree(org_content, org_name_set, target_type, target_designation):
    """
    Search the entire organizational tree for a specific unit.

    Args:
        org_content (dict): The organizational content structure
        org_name_set (set): Set of recognized organizational unit types
        target_type (str): Type of unit to find
        target_designation (str): Designation of unit to find

    Returns:
        list: Full path from root to the unit, or None if not found
    """
    def search(current_level, path_so_far):
        # Check if target is at this level
        if target_type in current_level:
            if target_designation in current_level[target_type]:
                return path_so_far + [{target_type: target_designation}]

        # Search recursively in sub-levels
        for level_name in current_level.keys():
            if level_name in org_name_set:
                for level_number in current_level[level_name].keys():
                    new_path = path_so_far + [{level_name: level_number}]
                    result = search(current_level[level_name][level_number], new_path)
                    if result:
                        return result

        return None

    return search(org_content, [])


def _find_organizational_unit_in_subtree(current_pointer, org_name_set, target_type, target_designation):
    """
    Search within a subtree for a specific organizational unit.

    Args:
        current_pointer (dict): Current position in organizational tree
        org_name_set (set): Set of recognized organizational unit types
        target_type (str): Type of unit to find
        target_designation (str): Designation of unit to find

    Returns:
        list: Path from current position to the unit (not including current position),
              or None if not found
    """
    def search(current_level, path_so_far):
        # Check if target is at this level
        if target_type in current_level:
            if target_designation in current_level[target_type]:
                return path_so_far + [{target_type: target_designation}]

        # Search recursively in sub-levels
        for level_name in current_level.keys():
            if level_name in org_name_set:
                for level_number in current_level[level_name].keys():
                    new_path = path_so_far + [{level_name: level_number}]
                    result = search(current_level[level_name][level_number], new_path)
                    if result:
                        return result

        return None

    return search(current_pointer, [])


def resolve_compound_organizational_path(parsed_content, compound_elements, current_item_context,
                                         document_issues_logfile, item_type_name, item_number, scope_phrase):
    """
    Resolve a compound organizational reference like "Chapter III, Section 2".

    The AI has already identified this as a compound path by returning a list.
    This function traverses the organizational structure following the specified path.

    Strategy:
    1. Find the highest-level element (anchor) in the current context
    2. If not in context, search entire document for the anchor
    3. Navigate down from anchor following the remaining path elements

    Examples:
    - "Chapter III, Section 2" from "Title 3, Part A, Chapter III, Section 2, Division 3"
      → Finds Chapter III in context, then finds Section 2 within that Chapter
    - "Chapter III, Section 3" from same location
      → Finds Chapter III in context, then finds Section 3 within that Chapter (different section)

    Args:
        parsed_content (dict): The document content
        compound_elements (list): List of {element_type, element_designation} objects
                                 Ordered from higher to lower organizational level
        current_item_context (list): Current item's organizational context
        document_issues_logfile (str): Path to logfile for issues
        item_type_name (str): Type of working unit (for logging)
        item_number (str): Number of working unit (for logging)
        scope_phrase (str): Original scope phrase (for logging)

    Returns:
        list: Organizational path from root to target, or None if not found
    """
    from .document_issues import log_document_issue

    if 'document_information' not in parsed_content:
        return None
    if 'organization' not in parsed_content['document_information']:
        return None
    if 'content' not in parsed_content['document_information']['organization']:
        return None

    org_content = parsed_content['document_information']['organization']['content']
    org_name_set = get_organizational_item_name_set(parsed_content)

    # Extract the path elements: [(type1, designation1), (type2, designation2), ...]
    path_elements = []
    for obj in compound_elements:
        if not isinstance(obj, dict):
            continue
        element_type = obj.get('element_type')
        element_designation = obj.get('element_designation')
        if element_type and element_designation and element_designation != "current":
            path_elements.append((element_type, element_designation))

    if len(path_elements) == 0:
        return None

    # Step 1: Find the highest-level element in the current context
    # We search UP the context from the working unit to find where this path starts
    anchor_type, anchor_designation = path_elements[0]
    anchor_found_at = None  # Index in current_item_context where anchor was found
    anchor_path = []  # Path from root to anchor

    # Search through the context to find the anchor
    for i, context_entry in enumerate(current_item_context):
        if not isinstance(context_entry, dict) or len(context_entry) != 1:
            continue

        key = next(iter(context_entry))
        value = context_entry[key]

        # Build path as we go
        anchor_path.append({key: value})

        # Check if this matches our anchor (case-insensitive comparison)
        if key.lower() == anchor_type.lower() and value == anchor_designation:
            anchor_found_at = i
            break

    # If anchor not found in context, search the entire document structure
    if anchor_found_at is None:
        # Search from root for the anchor element
        anchor_path = _find_organizational_unit_in_tree(
            org_content, org_name_set, anchor_type, anchor_designation
        )
        if anchor_path is None:
            log_document_issue(
                document_issues_logfile, 'scope_resolution', item_type_name, item_number,
                f'Could not find anchor organizational unit: {anchor_type} {anchor_designation}',
                scope_phrase=scope_phrase,
                anchor_type=anchor_type,
                anchor_designation=anchor_designation
            )
            return None

    # Step 2: Navigate down from the anchor following the remaining path elements
    current_path = anchor_path.copy()
    current_pointer = org_content

    # Navigate to the anchor position
    for entry in current_path:
        entry_key = next(iter(entry))
        entry_value = entry[entry_key]
        if entry_key in current_pointer and entry_value in current_pointer[entry_key]:
            current_pointer = current_pointer[entry_key][entry_value]
        else:
            log_document_issue(
                document_issues_logfile, 'scope_resolution', item_type_name, item_number,
                f'Could not navigate to anchor position in org structure',
                scope_phrase=scope_phrase,
                failed_at=f"{entry_key} {entry_value}"
            )
            return None

    # Step 3: Navigate through the remaining path elements (if any)
    for target_type, target_designation in path_elements[1:]:
        # Search within current_pointer for the target element
        found = False

        # Check if target_type exists at this level
        if target_type in current_pointer:
            if target_designation in current_pointer[target_type]:
                # Found it - add to path and navigate down
                current_path.append({target_type: target_designation})
                current_pointer = current_pointer[target_type][target_designation]
                found = True

        # If not found at this level, search recursively in sub-levels
        if not found:
            sub_path = _find_organizational_unit_in_subtree(
                current_pointer, org_name_set, target_type, target_designation
            )
            if sub_path:
                # Found in subtree - extend path
                current_path.extend(sub_path)
                # Navigate to new position
                for entry in sub_path:
                    entry_key = next(iter(entry))
                    entry_value = entry[entry_key]
                    if entry_key in current_pointer and entry_value in current_pointer[entry_key]:
                        current_pointer = current_pointer[entry_key][entry_value]
                    else:
                        log_document_issue(
                            document_issues_logfile, 'scope_resolution', item_type_name, item_number,
                            f'Could not navigate through compound path',
                            scope_phrase=scope_phrase,
                            failed_at=f"{target_type} {target_designation}"
                        )
                        return None
            else:
                # Not found - log and return None
                log_document_issue(
                    document_issues_logfile, 'scope_resolution', item_type_name, item_number,
                    f'Could not find organizational unit in compound path: {target_type} {target_designation}',
                    scope_phrase=scope_phrase,
                    partial_path=[str(e) for e in current_path]
                )
                return None

    return current_path

