# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License
from ast import parse
import re
import json
from xml.etree.ElementInclude import include
from typing import Dict, Iterable, Iterator, List, Tuple, Optional, Any
from .error_handling import InputError
from .text_processing import canonical_org_types

def _resolve_param_key(param_pointer, key):
    """
    Look up a parameter key, tolerating int/string mismatch.

    Parameter dict keys are ints during parsing but become strings after JSON round-trip.
    Values like sub_unit_type and parent_type may be ints in either context.
    This helper tries the key as-is, then as str(), then as int().
    Returns the parameter dict entry or None.
    """
    if key in param_pointer:
        return param_pointer[key]
    str_key = str(key)
    if str_key in param_pointer:
        return param_pointer[str_key]
    try:
        int_key = int(key)
        if int_key in param_pointer:
            return param_pointer[int_key]
    except (ValueError, TypeError):
        pass
    return None


def get_list_string(input_list, connecting_word):
    # Creates a string made up of the strings in the list, separated by commas and using the connecting_word.
    list_string = ''
    for list_iteration in range(len(input_list)):
        list_string += input_list[list_iteration]
        if list_iteration < len(input_list) - 2:
            list_string += ', '
        elif list_iteration == len(input_list) - 2:
            if len(input_list) > 2: # Account for Oxford comma.  Only use if there are three or more items.
                list_string += ','
            list_string += ' ' + connecting_word + ' '
    return list_string

def get_organizational_item_name_set(parsed_content):
    # Go through the 'content' section of the organizational structure and create a set of all unit types (e.g. chapter, part, subpart, etc.).
    item_name_set = set()
    full_item_name_set = get_full_item_name_set(parsed_content)
    org_keyword_set = {'unit_title', 'unit_definitions', 'summary_1', 'summary_2', 'summary_3'}
    for item_name in full_item_name_set:
        org_keyword_set.add('begin_' + item_name)
        org_keyword_set.add('stop_' + item_name)

    if ('document_information' in parsed_content.keys() 
        and 'organization' in parsed_content['document_information'].keys()
        and 'content' in parsed_content['document_information']['organization'].keys()):
        org_content_pointer = parsed_content['document_information']['organization']['content']
        item_name_set = org_name_set_subunit_iteration(org_content_pointer, item_name_set, org_keyword_set)
    return item_name_set

def org_name_set_subunit_iteration(limited_content, item_name_set, org_keyword_set):
    # Subroutine to be called recursively from get_organizational_item_name_set.
    for subunit_name in limited_content.keys():
        if not subunit_name in org_keyword_set:
            item_name_set.add(subunit_name)
            # Need to account for subunit numbers (or other identifiers) below the current subunit level.
            for subunit_number in limited_content[subunit_name].keys():
                item_name_set = org_name_set_subunit_iteration(limited_content[subunit_name][subunit_number], item_name_set, org_keyword_set)
    return item_name_set

def get_operational_item_name_set(parsed_content):
    item_name_set = set()
    if ('document_information' in parsed_content.keys()
        and 'parameters' in parsed_content['document_information'].keys()):
        param_pointer = parsed_content['document_information']['parameters']
        for item_type in param_pointer.keys():
            p = param_pointer[item_type]
            if p.get('is_sub_unit', False):
                continue
            if 1 == p['operational']:
                item_type_name = p['name']
                item_name_set.add(item_type_name)
    return item_name_set

def get_full_item_name_set(parsed_content):
    item_name_set = set()
    if ('document_information' in parsed_content.keys() 
        and 'parameters' in parsed_content['document_information'].keys()):
        param_pointer = parsed_content['document_information']['parameters']
        for item_type in param_pointer.keys():
            item_type_name = param_pointer[item_type]['name']                        
            item_name_set.add(item_type_name)
    return item_name_set

def _toc_sub_units(param_pointer, parent_item, indent_level):
    """
    Recursively generate TOC lines for sub-units within a container.

    For each sub-unit type:
    - Container sub-units (those with further sub_units) are listed individually on
      their own lines and recursively expanded.
    - Leaf sub-units (no further sub_units) are collected and formatted as a single
      compact comma-separated line: "Ccl_sections 0A001, 0A002, and 0A003".

    This mirrors the compact format used for leaf substantive units under org units.
    """
    lines = ''
    indent = '    ' * indent_level
    for sub_type_key, sub_type_items in parent_item['sub_units'].items():
        sub_p = _resolve_param_key(param_pointer, sub_type_key)
        sub_type_name = sub_p['name'] if sub_p else str(sub_type_key)
        sub_type_name_plural = sub_p['name_plural'] if sub_p else sub_type_name + 's'
        cap_name = sub_type_name[:1].upper() + sub_type_name[1:] if sub_type_name else ''
        cap_plural = sub_type_name_plural[:1].upper() + sub_type_name_plural[1:] if sub_type_name_plural else ''

        leaf_keys = []
        for sub_key, sub_item in sub_type_items.items():
            if has_sub_units(sub_item):
                # Flush any accumulated leaves before this container
                if leaf_keys:
                    if len(leaf_keys) == 1:
                        lines += indent + cap_name + ' ' + leaf_keys[0] + '\n'
                    else:
                        lines += indent + cap_plural + ' ' + get_list_string(leaf_keys, 'and') + '\n'
                    leaf_keys = []
                # Container sub-unit: list individually and recurse
                lines += indent + cap_name + ' ' + sub_key + '\n'
                lines += _toc_sub_units(param_pointer, sub_item, indent_level + 1)
            else:
                leaf_keys.append(sub_key)

        # Flush any remaining leaves
        if leaf_keys:
            if len(leaf_keys) == 1:
                lines += indent + cap_name + ' ' + leaf_keys[0] + '\n'
            else:
                lines += indent + cap_plural + ' ' + get_list_string(leaf_keys, 'and') + '\n'

    return lines


def _toc_substantive_items(parsed_content, item_name, item_name_plural, cap_item_name, first_item, last_item, indent_level):
    """
    Generate TOC lines for a range of substantive items, expanding sub-units when present.

    Container items (those with sub_units) are listed individually on their own lines
    and recursively expanded.  Leaf items (no sub_units) are collected and formatted as
    a single compact comma-separated line: "Sections 774.1, 774.2, and 774.3".
    """
    lines = ''
    content_pointer = parsed_content.get('content', {})
    items_dict = content_pointer.get(item_name_plural, {})
    param_pointer = parsed_content.get('document_information', {}).get('parameters', {})
    indent = '    ' * indent_level
    cap_singular = item_name[:1].upper() + item_name[1:] if item_name else ''
    cap_plural = item_name_plural[:1].upper() + item_name_plural[1:] if item_name_plural else ''

    include = False
    leaf_keys = []
    for item_key in items_dict:
        if item_key == first_item:
            include = True
        if include:
            unit = items_dict[item_key]
            if has_sub_units(unit):
                # Flush any accumulated leaves before this container
                if leaf_keys:
                    if len(leaf_keys) == 1:
                        lines += indent + cap_singular + ' ' + leaf_keys[0] + '\n'
                    else:
                        lines += indent + cap_plural + ' ' + get_list_string(leaf_keys, 'and') + '\n'
                    leaf_keys = []
                # Container: list individually and expand sub-units
                lines += indent + cap_singular + ' ' + item_key + '\n'
                lines += _toc_sub_units(param_pointer, unit, indent_level + 1)
            else:
                leaf_keys.append(item_key)
        if item_key == last_item:
            break

    # Flush any remaining leaves
    if leaf_keys:
        if len(leaf_keys) == 1:
            lines += indent + cap_singular + ' ' + leaf_keys[0] + '\n'
        else:
            lines += indent + cap_plural + ' ' + get_list_string(leaf_keys, 'and') + '\n'

    return lines


def create_table_of_contents(parsed_content, limited_content, level, summary_number=0, substantive_unit_details=0, filter_item_type=None):
    # Produce a table of contents that can be used in prompts to help match a substantive section to the organizational unit it is part of.
    # limited_content should be set to parsed_content['organization']['content'] and level should be set to 0 when called at the top level.
    # The function will call itself recursively, changing the values of limited_content and level as needed.
    # summary_number indicates the level of summary (if any) that should be included in the table of contents.
    # substantive_unit_details indicates whether each substantive unit (e.g. sections, articles, etc.) is listed, rather than specifying a range.
    # If substantive_unit_details is 1, then summary_number is set to 0, regardless of what is provided in arguments.
    # filter_item_type (optional): When set, only include this specific substantive unit type in the TOC.
    #                              When None (top-level call), process all item types in order at each organizational level,
    #                              then recurse down with each type as a filter.

    table_of_contents = ''
    substantive_flag = 0 # This flag will be set to 1 if there are operative parts of the regulation directly at this organizational level.
    lowest_level_flag = 1 # This flag will be set to 1 if there are no lower organizational levels.

    if 1 == substantive_unit_details:
        summary_number = 0
    if summary_number > 0:
        summary_string = 'summary_' + str(summary_number)
    else:
        summary_string = ''

    if not ('document_information' in parsed_content.keys()
            and 'organization' in parsed_content['document_information'].keys()
            and 'parameters' in parsed_content['document_information'].keys()):
        raise InputError('create_table_of_contents: Document information is not correct.')
        exit(1)

    param_pointer = parsed_content['document_information']['parameters']
    org_pointer = parsed_content['document_information']['organization']

    # Sort item types by their numeric keys to ensure correct order (recitals, articles, appendices, etc.)
    sorted_item_type_keys = sorted(param_pointer.keys(), key=lambda x: int(x))

    # Traverse organizational structure
    for level_name in limited_content.keys():
        if (not re.match('unit_title', level_name) and
            not re.match('unit_definitions', level_name) and
            not re.match('begin_', level_name) and
            not re.match('stop_', level_name) and
            not re.match('summary_', level_name)): # Anything other than these should be org unit types.
            level_name, level_name_plural = canonical_org_types(level_name)

            for org_item_number in limited_content[level_name].keys():  # This loops org_item_number over each instance of the organizational type at the current location.
                working_item = limited_content[level_name][org_item_number]
                cap_level_name = ''
                if len(level_name) > 1:
                    cap_level_name = level_name[0].upper() + level_name[1:]
                elif len(level_name) > 0:
                    cap_level_name = level_name[0].upper()
                else:
                    raise InputError('No level_name.\n')
                    exit(1)

                # Mode 1: No filter - process all item types in order at this organizational level
                if filter_item_type is None:
                    # Add organizational unit header (only once)
                    table_of_contents += '    '*level + cap_level_name + ' ' + org_item_number
                    if 0 == substantive_unit_details and 'unit_title' in working_item.keys():
                        table_of_contents += ': ' + working_item['unit_title']
                    table_of_contents += '\n'

                    # Process all item types in order at THIS organizational level
                    for item_type_key in sorted_item_type_keys:
                        if param_pointer[item_type_key].get('is_sub_unit', False):
                            continue
                        item_name = param_pointer[item_type_key]['name']
                        item_name_plural = param_pointer[item_type_key]['name_plural']
                        begin_tag = 'begin_' + item_name
                        stop_tag = 'stop_' + item_name

                        # Check if this item type exists at THIS organizational level
                        if begin_tag in working_item.keys() and working_item[begin_tag] != '':
                            substantive_flag = 1
                            first_item = working_item[begin_tag]
                            last_item = working_item.get(stop_tag, first_item)

                            if not first_item == last_item:
                                # Range of items
                                cap_item_name = ''
                                if len(item_name_plural) > 1:
                                    cap_item_name = item_name_plural[0].upper() + item_name_plural[1:]
                                elif len(item_name_plural) > 0:
                                    cap_item_name = item_name_plural[0].upper()
                                else:
                                    raise InputError('No item_name.\n')
                                    exit(1)
                                if 1 == substantive_unit_details:
                                    table_of_contents += _toc_substantive_items(
                                        parsed_content, item_name, item_name_plural, cap_item_name,
                                        first_item, last_item, level + 1)
                                else:
                                    table_of_contents += '    ' + '    '*level + cap_item_name +  ' ' + first_item + ' to ' + last_item + '\n'
                            else:
                                # Single item
                                cap_item_name = ''
                                if len(item_name) > 1:
                                    cap_item_name = item_name[0].upper() + item_name[1:]
                                elif len(item_name) > 0:
                                    cap_item_name = item_name[0].upper()
                                else:
                                    raise InputError('No item_name.\n')
                                    exit(1)
                                if 1 == substantive_unit_details:
                                    # Check for sub-units on the single item
                                    single_unit = parsed_content.get('content', {}).get(item_name_plural, {}).get(first_item)
                                    if single_unit and has_sub_units(single_unit):
                                        table_of_contents += _toc_substantive_items(
                                            parsed_content, item_name, item_name_plural, cap_item_name,
                                            first_item, first_item, level + 1)
                                    else:
                                        table_of_contents += '    ' + '    '*level + cap_item_name +  ' ' + first_item + '\n'
                                else:
                                    table_of_contents += '    ' + '    '*level + cap_item_name +  ' ' + first_item + '\n'

                        # Recurse to sub-organizational levels with this item type as filter
                        sub_content = create_table_of_contents(
                            parsed_content, working_item, level+1,
                            summary_number, substantive_unit_details,
                            filter_item_type=item_name
                        )
                        if sub_content:
                            lowest_level_flag = 0
                            table_of_contents += sub_content

                    # Add summary if at lowest level
                    if 1 == lowest_level_flag and not '' == summary_string and summary_string in working_item.keys():
                        table_of_contents += '\n' + working_item[summary_string] + '\n\n'

                # Mode 2: Filter is set - only process the specified item type
                else:
                    begin_tag = 'begin_' + filter_item_type
                    stop_tag = 'stop_' + filter_item_type
                    has_type_at_level = begin_tag in working_item.keys() and working_item[begin_tag] != ''

                    # Get sub-organizational content for this filtered type
                    sub_content = create_table_of_contents(
                        parsed_content, working_item, level+1,
                        summary_number, substantive_unit_details,
                        filter_item_type=filter_item_type
                    )

                    # Only add this organizational unit if it contains the filtered type (either locally or in sub-levels)
                    if has_type_at_level or sub_content:
                        # Add organizational unit header
                        table_of_contents += '    '*level + cap_level_name + ' ' + org_item_number
                        if 0 == substantive_unit_details and 'unit_title' in working_item.keys():
                            table_of_contents += ': ' + working_item['unit_title']
                        table_of_contents += '\n'

                        # Add the filtered item type if it exists at this level
                        if has_type_at_level:
                            substantive_flag = 1
                            lowest_level_flag = 1
                            first_item = working_item[begin_tag]
                            last_item = working_item.get(stop_tag, first_item)

                            # Find the item_name_plural for this item type
                            item_name_plural = ''
                            for item_type_key in param_pointer.keys():
                                if param_pointer[item_type_key]['name'] == filter_item_type:
                                    item_name_plural = param_pointer[item_type_key]['name_plural']
                                    break

                            if not first_item == last_item:
                                # Range of items
                                cap_item_name = ''
                                if len(item_name_plural) > 1:
                                    cap_item_name = item_name_plural[0].upper() + item_name_plural[1:]
                                elif len(item_name_plural) > 0:
                                    cap_item_name = item_name_plural[0].upper()
                                else:
                                    raise InputError('No item_name.\n')
                                    exit(1)
                                if 1 == substantive_unit_details:
                                    table_of_contents += _toc_substantive_items(
                                        parsed_content, filter_item_type, item_name_plural, cap_item_name,
                                        first_item, last_item, level + 1)
                                else:
                                    table_of_contents += '    ' + '    '*level + cap_item_name +  ' ' + first_item + ' to ' + last_item + '\n'
                            else:
                                # Single item
                                cap_item_name = ''
                                if len(filter_item_type) > 1:
                                    cap_item_name = filter_item_type[0].upper() + filter_item_type[1:]
                                elif len(filter_item_type) > 0:
                                    cap_item_name = filter_item_type[0].upper()
                                else:
                                    raise InputError('No item_name.\n')
                                    exit(1)
                                if 1 == substantive_unit_details:
                                    single_unit = parsed_content.get('content', {}).get(item_name_plural, {}).get(first_item)
                                    if single_unit and has_sub_units(single_unit):
                                        table_of_contents += _toc_substantive_items(
                                            parsed_content, filter_item_type, item_name_plural, cap_item_name,
                                            first_item, first_item, level + 1)
                                    else:
                                        table_of_contents += '    ' + '    '*level + cap_item_name +  ' ' + first_item + '\n'
                                else:
                                    table_of_contents += '    ' + '    '*level + cap_item_name +  ' ' + first_item + '\n'

                        # Add sub-organizational content
                        if '' == sub_content:
                            lowest_level_flag = 1
                        else:
                            lowest_level_flag = 0
                        if 1 == lowest_level_flag and not '' == summary_string and summary_string in working_item.keys():
                            table_of_contents += '\n' + working_item[summary_string] + '\n\n'
                        if not '' == sub_content:
                            table_of_contents += sub_content

    return table_of_contents

def get_org_pointer_from_scope(org_content, content_scope):
    """Navigate org tree following content_scope breadcrumb. Returns (type, id, node) or (None, None, None)."""
    current = org_content
    scope_type, scope_id, scope_node = None, None, None
    for entry in content_scope:
        for k, v in entry.items():
            if k not in current or v not in current[k]:
                return None, None, None
            scope_type, scope_id = k, v
            scope_node = current[k][v]
            current = scope_node
    return scope_type, scope_id, scope_node


def get_org_pointer(parsed_content, content_pointer):
    # Given the pointer to a location in the main content, return a pointer to where that shows up in the organizational content.
    retval = '' # Value if the location cannot be found.
    
    if ('document_information' in parsed_content.keys() 
        and 'organization' in parsed_content['document_information'].keys()
        and 'content' in parsed_content['document_information']['organization'].keys()):
        org_pointer = parsed_content['document_information']['organization']
        org_content_pointer = org_pointer['content']
        
        if 'context' in content_pointer.keys():
            org_context = content_pointer['context']
            for entry in org_context:
                for key, value in entry.items():
                    if key in org_content_pointer.keys() and value in org_content_pointer[key].keys():
                        org_content_pointer = org_content_pointer[key][value]
                        break
                    else:
                        return retval
        retval = org_content_pointer
    return retval

def get_org_pointer_from_context(parsed_content, org_context):
    # Given a list of dictionaries (each with a name and number key), follow it through the
    # organizational content to return a pointer to that entry.
    # For contexts that include substantive types (e.g., supplement, eccn) beyond the org hierarchy,
    # returns the last reachable org node; substantive types are not in the org tree.
    retval = None
    if ('document_information' in parsed_content.keys()
        and 'organization' in parsed_content['document_information'].keys()
        and 'content' in parsed_content['document_information']['organization'].keys()):
        org_name_set = get_organizational_item_name_set(parsed_content)
        retval = parsed_content['document_information']['organization']['content']
        for level in org_context:
            if 'name' not in level.keys() or 'number' not in level.keys():
                return None
            if level['name'] not in retval.keys():
                if level['name'] not in org_name_set:
                    # Substantive type (e.g., supplement) — org hierarchy stops here
                    return retval
                return None
            retval = retval[level['name']]
            if level['number'] not in retval.keys():
                return None
            retval = retval[level['number']]
    return retval

def iter_all_items(parsed_content) -> Iterator[Tuple[str, str, str, str, dict]]:

    # Yields (item_type_name, item_type_name_plural, cap_item_type_name, item_number, working_item)
    # across all item types, whether or not operational items.
    # For v0.4 documents with nested sub-units: skips is_sub_unit parameter types at top level,
    # and for items that have sub_units, yields each sub-unit instead of the parent.

    if ('document_information' not in parsed_content.keys()
        or 'parameters' not in parsed_content['document_information'].keys()
        or 'content' not in parsed_content.keys()):
        raise InputError('iter_all_items: invalid parsed_content structure.')

    param_pointer = parsed_content['document_information']['parameters']
    content_pointer = parsed_content['content']

    for item_type in param_pointer.keys():
        p = param_pointer[item_type]
        if p.get('is_sub_unit', False):
            continue

        item_type_name = p['name']
        item_type_name_plural = p['name_plural']
        if item_type_name_plural not in content_pointer.keys():
            raise InputError(f'Error: {item_type_name_plural} not present.')

        cap_item_type_name = item_type_name[:1].upper() + item_type_name[1:] if item_type_name else ''

        for item_number, working_item in content_pointer[item_type_name_plural].items():
            if has_sub_units(working_item):
                # Container: recursively yield leaf sub-units (any depth).
                yield from _iter_leaves(param_pointer, working_item, include_non_operational=True)
            else:
                yield (item_type_name, item_type_name_plural, cap_item_type_name, item_number, working_item)

def _iter_leaves(param_pointer, working_item, include_non_operational=False):
    """
    Recursively yield leaf sub-items from a container item.

    Descends into nested sub_units until items with no sub_units are found (leaves).
    Yields (type_name, type_name_plural, cap_type_name, item_key, sub_item) for each leaf.
    Used internally by iter_containers and other utilities that need only leaf nodes.
    """
    for sub_type_key, sub_type_items in working_item['sub_units'].items():
        sub_p = _resolve_param_key(param_pointer, sub_type_key)
        if not sub_p:
            continue
        if not include_non_operational and sub_p.get('operational', 0) != 1:
            continue
        sub_type_name = sub_p['name']
        sub_type_name_plural = sub_p['name_plural']
        cap_sub_type_name = sub_type_name[:1].upper() + sub_type_name[1:] if sub_type_name else ''
        for sub_key, sub_item in sub_type_items.items():
            if has_sub_units(sub_item):
                yield from _iter_leaves(param_pointer, sub_item, include_non_operational)
            else:
                yield (sub_type_name, sub_type_name_plural, cap_sub_type_name, sub_key, sub_item)


def _iter_all_nodes(param_pointer, item_type_name, item_type_name_plural, cap_item_type_name, item_number, working_item):
    """
    Pre-order recursive yield of a node and all its operational sub-unit descendants.

    Yields the node itself first, then recursively descends into sub_units,
    yielding each descendant node before its own children (pre-order traversal).
    Non-operational sub-unit types are skipped.
    """
    yield (item_type_name, item_type_name_plural, cap_item_type_name, item_number, working_item)
    for sub_type_key, sub_type_items in working_item.get('sub_units', {}).items():
        sub_p = _resolve_param_key(param_pointer, sub_type_key)
        if not sub_p or sub_p.get('operational', 0) != 1:
            continue
        sub_type_name = sub_p['name']
        sub_type_name_plural = sub_p['name_plural']
        cap_sub_type_name = sub_type_name[:1].upper() + sub_type_name[1:] if sub_type_name else ''
        for sub_key, sub_item in sub_type_items.items():
            yield from _iter_all_nodes(param_pointer, sub_type_name, sub_type_name_plural, cap_sub_type_name, sub_key, sub_item)


def iter_operational_items(parsed_content) -> Iterator[Tuple[str, str, str, str, dict]]:
    """
    Yield (item_type_name, item_type_name_plural, cap_item_type_name, item_number, working_item)
    for every node of an operational type, including container nodes that have nested sub-units.

    Pre-order traversal: a container is yielded before its sub-unit descendants.
    is_sub_unit types are not iterated at the top level (they appear only as descendants
    when their parent container is yielded).

    Callers that need only leaf nodes can filter with has_sub_units():
        if has_sub_units(working_item): continue
    Stage 3 summary generation does this because container_summaries() handles containers
    via aggregation after the leaf-summary pass.
    """
    if ('document_information' not in parsed_content.keys()
        or 'parameters' not in parsed_content['document_information'].keys()
        or 'content' not in parsed_content.keys()):
        raise InputError('iter_operational_items: invalid parsed_content structure.')

    param_pointer = parsed_content['document_information']['parameters']
    content_pointer = parsed_content['content']

    for item_type in param_pointer.keys():
        p = param_pointer[item_type]
        if p.get('is_sub_unit', False):
            continue
        if 'operational' not in p.keys() or p['operational'] != 1:
            continue

        item_type_name = p['name']
        item_type_name_plural = p['name_plural']
        if item_type_name_plural not in content_pointer.keys():
            raise InputError(f'Error: {item_type_name_plural} not present.')

        cap_item_type_name = item_type_name[:1].upper() + item_type_name[1:] if item_type_name else ''

        for item_number, working_item in content_pointer[item_type_name_plural].items():
            yield from _iter_all_nodes(param_pointer, item_type_name, item_type_name_plural, cap_item_type_name, item_number, working_item)

def has_sub_units(working_item) -> bool:
    """Check whether a substantive unit contains nested sub-units.

    Returns True only when there is at least one actual item in the sub_units
    dict.  A parser may produce a type-keyed skeleton like {"11": {}} for a
    section that ended up with no children; that is treated as a leaf (False)
    so that level_1_summaries processes it rather than container_summaries.
    """
    return any(bool(v) for v in working_item.get('sub_units', {}).values())


def lookup_item(parsed_content, item_type_name_plural, item_number) -> Optional[dict]:
    """
    Look up a substantive item by type and number.

    First checks the top-level content section (fast path). If not found and the type
    is a sub-unit type (has is_sub_unit in parameters), searches through parent containers'
    sub_units dicts.

    Returns the actual dict reference from the document tree (not a copy), so callers
    can mutate it in place (e.g., adding ext_definitions).

    Returns None if the item is not found.
    """
    content_pointer = parsed_content.get('content', {})

    # Fast path: top-level lookup
    if item_type_name_plural in content_pointer:
        if item_number in content_pointer[item_type_name_plural]:
            return content_pointer[item_type_name_plural][item_number]

    # Check if this is a sub-unit type; if so, search parent containers
    param_pointer = parsed_content.get('document_information', {}).get('parameters', {})
    target_param_key = None
    for k, p in param_pointer.items():
        if p.get('name_plural') == item_type_name_plural and p.get('is_sub_unit', False):
            target_param_key = str(k)
            break

    if target_param_key is None:
        return None

    # Fast path 2: index lookup (O(1) navigation for indexed sub-unit types)
    index = parsed_content.get('document_information', {}).get('sub_unit_index', {})
    if target_param_key in index and item_number in index[target_param_key]:
        entry = index[target_param_key][item_number]
        container_plural = entry.get('container_plural')
        container_id = entry.get('container_id')
        path = entry.get('path', [])
        if container_plural and container_id:
            container = content_pointer.get(container_plural, {}).get(container_id)
            if container is not None:
                current = container
                for j in range(0, len(path), 2):
                    type_key = path[j]
                    item_id = path[j + 1] if j + 1 < len(path) else None
                    if item_id is None:
                        current = None
                        break
                    sub_units = current.get('sub_units', {})
                    type_items = _resolve_param_key(sub_units, type_key) or {}
                    current = type_items.get(item_id)
                    if current is None:
                        break
                if current is not None:
                    final_sub_units = current.get('sub_units', {})
                    type_items = _resolve_param_key(final_sub_units, target_param_key) or {}
                    if item_number in type_items:
                        return type_items[item_number]

    # Slow path: scan all containers recursively (now recursive via updated iter_containers)
    for _, _, _, _, container_item in iter_containers(parsed_content):
        sub_units = container_item.get('sub_units', {})
        type_items = _resolve_param_key(sub_units, target_param_key) or {}
        if item_number in type_items:
            return type_items[item_number]

    return None


def get_item_numbers_for_type(parsed_content, item_type_name_plural) -> list:
    """
    Return all item numbers (keys) for a given type, including sub-units nested in parent containers.

    For top-level types, returns keys from parsed_content['content'][item_type_name_plural].
    For sub-unit types, searches parent containers' sub_units dicts.
    Returns an empty list if the type is not found or has no items.
    """
    if 'content' not in parsed_content or 'document_information' not in parsed_content:
        return []

    content_pointer = parsed_content['content']

    # Fast path: top-level type
    if item_type_name_plural in content_pointer:
        return list(content_pointer[item_type_name_plural].keys())

    # Slow path: sub-unit type — search parent containers
    param_pointer = parsed_content['document_information'].get('parameters', {})
    target_param_key = None
    for key, p in param_pointer.items():
        if isinstance(p, dict) and p.get('name_plural') == item_type_name_plural and p.get('is_sub_unit'):
            target_param_key = str(key)
            break

    if target_param_key is None:
        return []

    # v0.5: sub_units is type-keyed {param_key: {sub_num: sub_item, ...}, ...}
    result = []
    for _, _, _, _, container_item in iter_containers(parsed_content):
        sub_units = container_item.get('sub_units', {})
        type_items = _resolve_param_key(sub_units, target_param_key) or {}
        result.extend(type_items.keys())
    return result


def iter_containers(parsed_content) -> Iterator[Tuple[str, str, str, str, dict]]:
    """
    Iterate over operational items that contain nested sub-units.

    Yields (item_type_name, item_type_name_plural, cap_item_type_name, item_number, working_item)
    only for items where has_sub_units(working_item) is True.
    """
    if ('document_information' not in parsed_content
        or 'parameters' not in parsed_content['document_information']
        or 'content' not in parsed_content):
        raise InputError('iter_containers: invalid parsed_content structure.')

    param_pointer = parsed_content['document_information']['parameters']
    content_pointer = parsed_content['content']

    def _collect_post_order(item, tname, tplural, ctname, ikey):
        """Post-order recursive: yield deepest sub-containers before self."""
        for sub_type_key, sub_type_items in item.get('sub_units', {}).items():
            sub_p = _resolve_param_key(param_pointer, sub_type_key)
            if not sub_p:
                continue
            stn = sub_p['name']
            stp = sub_p['name_plural']
            cstn = stn[:1].upper() + stn[1:] if stn else ''
            for sub_key, sub_item in sub_type_items.items():
                if has_sub_units(sub_item):
                    yield from _collect_post_order(sub_item, stn, stp, cstn, sub_key)
        yield (tname, tplural, ctname, ikey, item)

    for item_type in param_pointer.keys():
        p = param_pointer[item_type]
        if p.get('is_sub_unit', False):
            continue
        if 'operational' not in p or p['operational'] != 1:
            continue

        item_type_name = p['name']
        item_type_name_plural = p['name_plural']
        if item_type_name_plural not in content_pointer:
            continue

        cap_item_type_name = item_type_name[:1].upper() + item_type_name[1:] if item_type_name else ''

        for item_number, working_item in content_pointer[item_type_name_plural].items():
            if has_sub_units(working_item):
                yield from _collect_post_order(
                    working_item, item_type_name, item_type_name_plural,
                    cap_item_type_name, item_number
                )


def iter_org_content(parsed_content) -> Iterator[Tuple[dict, list]]:
    # Iterate through all elements of the organization content.  Returns the organization pointer, then a context list.
    if ('document_information' not in parsed_content.keys() 
        or 'organization' not in parsed_content['document_information'].keys()
        or 'content' not in parsed_content['document_information']['organization'].keys()):
        raise InputError('iter_definitions: invalid parsed_content structure.')
    org_name_set = get_organizational_item_name_set(parsed_content)
    org_context = []

    yield from org_content_sub_iterator(parsed_content, org_context, org_name_set)


def org_content_sub_iterator(parsed_content, context, org_name_set) -> Iterator[Tuple[dict, list]]:
    level_pointer = get_org_pointer_from_context(parsed_content, context)
    yield(level_pointer, context)
    # Next, go through sub-levels.
    for level in level_pointer.keys():
        if level in org_name_set:
            for num in level_pointer[level].keys():
                context_copy = context.copy()
                context_copy.append({"name": level, "number": num})
                yield from org_content_sub_iterator(parsed_content, context_copy, org_name_set)    

def iter_definitions(parsed_content) -> Iterator[list]: # First item in list is a dictionary with the actual definition entry.
    # Second item is context as a list of dictionaries that leads to the organizational location of the dictionary with name and number (same as for org_context in parsers).
    # The second item is empty if this is not a definition in an organizational location.
    # Third item is a dictionary with item_type_name_plural and item_number for a substantive unit, if this definition is from a substantive unit.
    # the third item is empty if this is not a definition in a substantive unit.
    if ('document_information' not in parsed_content.keys() 
        or 'parameters' not in parsed_content['document_information'].keys()
        or 'content' not in parsed_content.keys()
        or 'organization' not in parsed_content['document_information'].keys()
        or 'content' not in parsed_content['document_information']['organization'].keys()):
        raise InputError('iter_definitions: invalid parsed_content structure.')
 
    org_name_set = get_organizational_item_name_set(parsed_content)
    operational_name_set = get_operational_item_name_set(parsed_content)

    #First, do all non-scope definitions.
    if 'document_definitions' in parsed_content['document_information'].keys():
        for def_entry in parsed_content['document_information']['document_definitions']:
            yield([def_entry, [], {}])

    # Next, go through organizational structure finding all definitions.
    for org_entry, context in iter_org_content(parsed_content):
        if 'unit_definitions' in org_entry.keys():
            for def_entry in org_entry['unit_definitions']:
                yield([def_entry, context, {}])

    # Last, go through the operational items and find all definitions.
    for item_type_name, item_type_name_plural, cap_item_type_name, item_number, working_item in iter_operational_items(parsed_content):
        if 'defined_terms' in working_item.keys():
            if len(working_item['defined_terms']) > 0:
                for def_entry in working_item['defined_terms']:
                    yield([def_entry, [], {"source_type": item_type_name, "source_number": item_number}])
        if 'ext_definitions' in working_item.keys():
            if len(working_item['ext_definitions']) > 0:
                for def_entry in working_item['ext_definitions']:
                    yield([def_entry, [], {"source_type": item_type_name, "source_number": item_number}])

def iter_indirect_definitions(parsed_content) -> Iterator[list]: # First item in list is a dictionary with the actual definition entry.
    # Iterates through all processed indirect_definitions (meaning that the indirect_loc_type and indirect_loc_number are known, and possibly a value).
    # Second item is context as a list of dictionaries that leads to the organizational location of the dictionary with name and number (same as for org_context in parsers).
    # The second item is empty if this is not a definition in an organizational location.
    # Third item is a dictionary with item_type_name_plural and item_number for a substantive unit, if this definition is from a substantive unit.
    # the third item is empty if this is not a definition in a substantive unit.
    if ('document_information' not in parsed_content.keys() 
        or 'parameters' not in parsed_content['document_information'].keys()
        or 'content' not in parsed_content.keys()
        or 'organization' not in parsed_content['document_information'].keys()
        or 'content' not in parsed_content['document_information']['organization'].keys()):
        raise InputError('iter_definitions: invalid parsed_content structure.')
 
    org_name_set = get_organizational_item_name_set(parsed_content)
    operational_name_set = get_operational_item_name_set(parsed_content)

    #First, do all non-scope definitions.
    if 'indirect_definitions' in parsed_content['document_information'].keys():
        for def_entry in parsed_content['document_information']['indirect_definitions']:
            yield([def_entry, [], {}])

    # Next, go through organizational structure finding all definitions.
    for org_entry, context in iter_org_content(parsed_content):
        if 'unit_indirect_definitions' in org_entry.keys():
            for def_entry in org_entry['unit_indirect_definitions']:
                yield([def_entry, context, {}])

    # Last, go through the operational items and find all external indirect definitions (meaning that a different unit said that the indirect definition applies here).
    for item_type_name, item_type_name_plural, cap_item_type_name, item_number, working_item in iter_operational_items(parsed_content):
        if 'ext_indirect_definitions' in working_item.keys():
            if len(working_item['ext_indirect_definitions']) > 0:
                for def_entry in working_item['ext_indirect_definitions']:
                    yield([def_entry, [], {"source_type": item_type_name, "source_number": item_number}])

def write_if_updated(parsed_content, out_path: str, dirty_flag: int) -> int:
    # Written by ChatGPT 5, modified by hand.
    if dirty_flag == 1:
        # Use UTF-8 encoding and ensure_ascii=False to handle Unicode properly
        # This matches how the UI reads the file
        with open(out_path, "w", encoding='utf-8') as outfile:
            outfile.write(json.dumps(parsed_content, indent=4, ensure_ascii=False))
        return 0
    return dirty_flag

class TextExtractionTools:
    """Tools for extracting and searching text within documents"""
    
    def __init__(self, source_text: str):
        self.source_text = source_text
    
    def extract_text_by_offset(self, start_offset: int, end_offset: int) -> Dict[str, Any]:
        """Extract exact text using character offsets"""
        if start_offset < 0 or end_offset > len(self.source_text):
            return {
                "error": "Offset out of bounds", 
                "text_length": len(self.source_text),
                "requested_start": start_offset,
                "requested_end": end_offset
            }
        
        extracted = self.source_text[start_offset:end_offset]
        return {
            "extracted_text": extracted,
            "start_offset": start_offset,
            "end_offset": end_offset,
            "character_count": end_offset - start_offset
        }
    
    def search_text_around_offset(self, search_text: str, approximate_offset: int, window_size: int = 500) -> Dict[str, Any]:
        """Find closest match to search text near given offset"""
        start_window = max(0, approximate_offset - window_size//2)
        end_window = min(len(self.source_text), approximate_offset + window_size//2)
        
        window_text = self.source_text[start_window:end_window]
        best_match_pos = window_text.find(search_text)
        
        if best_match_pos != -1:
            actual_start = start_window + best_match_pos
            actual_end = actual_start + len(search_text)
            return {
                "found": True,
                "actual_start_offset": actual_start,
                "actual_end_offset": actual_end,
                "matched_text": self.source_text[actual_start:actual_end]
            }
        else:
            return {
                "found": False, 
                "search_window": [start_window, end_window],
                "window_text_preview": window_text[:100] + "..." if len(window_text) > 100 else window_text
            }
    
    def get_text_length(self) -> Dict[str, Any]:
        """Get total length of source text"""
        return {"text_length": len(self.source_text)}
    
    def execute_tool(self, tool_name: str, **kwargs) -> Dict[str, Any]:
        """Execute a tool by name with given parameters"""
        try:
            if tool_name == "extract_text_by_offset":
                return self.extract_text_by_offset(**kwargs)
            elif tool_name == "search_text_around_offset":
                return self.search_text_around_offset(**kwargs)
            elif tool_name == "get_text_length":
                return self.get_text_length()
            else:
                return {"error": f"Unknown tool: {tool_name}"}
                
        except (TypeError, ValueError, KeyError) as e:
            return {
                "error": f"Invalid tool call parameters: {str(e)}",
                "tool_name": tool_name,
                "received_input": str(kwargs),
                "message": "Please check your parameter names and values and try again."
            }


def get_text_extraction_tools_schema() -> list:
    """Get the tool schema definition for text extraction tools"""
    return [
        {
            "name": "extract_text_by_offset",
            "description": "Extract exact text using character offsets from the source document",
            "input_schema": {
                "type": "object",
                "properties": {
                    "start_offset": {"type": "integer"},
                    "end_offset": {"type": "integer"}
                },
                "required": ["start_offset", "end_offset"]
            }
        },
        {
            "name": "search_text_around_offset", 
            "description": "Find closest match to text near a given offset",
            "input_schema": {
                "type": "object",
                "properties": {
                    "search_text": {"type": "string"},
                    "approximate_offset": {"type": "integer"},
                    "window_size": {"type": "integer", "default": 500}
                },
                "required": ["search_text", "approximate_offset"]
            }
        },
        {
            "name": "get_text_length",
            "description": "Get total character length of source text",
            "input_schema": {"type": "object", "properties": {}}
        }
    ]


def add_substantive_markers_org(parsed_content, org_context, item_type_name, item_num):
    """
    Add "begin_" and "stop_" markers for each substantive item type to the 
    organizational information for the document.
    
    This function is used during parsing to mark the range of substantive items
    (e.g., sections, articles) within each organizational unit of the document.
    
    Args:
        parsed_content: The parsed document structure
        org_context: List of dicts describing the organizational path 
                     (e.g., [{'title': '1'}, {'chapter': '2'}])
        item_type_name: The type of item (e.g., 'section', 'article')
        item_num: The number/identifier of the item
        
    Raises:
        ParseError: If org_context references organizational information not found
    """
    from .error_handling import ParseError
    
    if not [] == org_context:
        local_org_pointer = parsed_content['document_information']['organization']['content']  # Start at the root.
        begin_label = 'begin_' + item_type_name
        stop_label = 'stop_' + item_type_name
        for entry in org_context:
            name = list(entry.keys())[0]
            number = entry[name]
            if name in local_org_pointer.keys() and number in local_org_pointer[name]:
                local_org_pointer = local_org_pointer[name][number]
            else:
                raise ParseError('org_context includes information not found in organizational information.')
        if not begin_label in local_org_pointer.keys():
            local_org_pointer[begin_label] = ''    
        if '' == local_org_pointer[begin_label]:  # Only need this for the first one encountered.
            local_org_pointer[begin_label] = item_num
        local_org_pointer[stop_label] = item_num  # By always updating the stop item we will automatically have the right value.


def get_org_top_unit(parsed_content, org_keyword_set=None):
    """
    Return org context for the top organizational unit.
    
    This function identifies the topmost organizational unit in the document
    (e.g., directive, regulation, title) and returns it in org_context format.
    
    Args:
        parsed_content: The parsed document structure
        org_keyword_set: Optional set of keywords that appear in org structure
                        but aren't organizational types (e.g., 'unit_title', 
                        'begin_article'). If None, it will be generated automatically.
        
    Returns:
        list: A single-element list containing a dict with the top unit
              (e.g., [{'directive': '2023/1234'}])
    """
    org_type = ''
    org_num = ''
    
    # Get list of keywords that may show up in the organizational structure 
    # that are not organizational types for this document.
    if org_keyword_set is None:
        operational_item_name_set = get_operational_item_name_set(parsed_content)
        org_keyword_set = {'unit_title', 'unit_definitions'}
        for item_name in operational_item_name_set:
            org_keyword_set.add('begin_' + item_name)
            org_keyword_set.add('stop_' + item_name)

    for entry in parsed_content['document_information']['organization']['content'].keys():
        if not entry in org_keyword_set:
            org_type = entry
    org_num = list(parsed_content['document_information']['organization']['content'][org_type].keys())[0]
    return [{org_type: org_num}]

def chunk_text(text, breakpoints, preferred_length=15000):
    """
    Iterate through a long set of text, yielding chunks that are preferably
    not longer than the given preferred_length.

    The breaks in the text are defined in breakpoints, which is a list of lists,
    where each interior list is two numbers: the offset into the text to locate the
    breakpoint and the level of the breakpoint (higher numbers are less desirable
    places to break).

    Multi-level fallback: When a segment between breakpoints at the current level
    exceeds preferred_length, the segment is recursively sub-chunked using the next
    available breakpoint level within that segment.
    """
    if text == '':
        return

    if preferred_length <= 0 or len(breakpoints) < 1:
        yield text
        return

    # Get sorted unique breakpoint levels.
    levels = sorted(set(entry[1] for entry in breakpoints))
    if not levels:
        yield text
        return

    yield from _chunk_text_at_level(text, 0, len(text), breakpoints, levels, 0, preferred_length)


def _chunk_text_at_level(text, region_start, region_end, breakpoints, levels, level_index, preferred_length):
    """
    Chunk a region of text using breakpoints at levels[level_index].

    When a segment still exceeds preferred_length after splitting at this level,
    recursively sub-chunks it using the next level (level_index + 1).

    Args:
        text: The full text string.
        region_start: Start offset of the region to chunk (inclusive).
        region_end: End offset of the region to chunk (exclusive).
        breakpoints: Full list of [position, level] breakpoints.
        levels: Sorted list of unique breakpoint levels.
        level_index: Index into levels for the current splitting level.
        preferred_length: Target maximum chunk size in characters.
    """
    if region_start >= region_end:
        return

    if level_index >= len(levels):
        # No more levels available; yield the region as-is.
        yield text[region_start:region_end]
        return

    current_level = levels[level_index]

    # Collect breakpoint positions at this level within [region_start, region_end).
    split_positions = [
        entry[0] for entry in breakpoints
        if entry[1] == current_level and region_start < entry[0] < region_end
    ]

    if not split_positions:
        # No breakpoints at this level in this region; try next level.
        yield from _chunk_text_at_level(text, region_start, region_end, breakpoints, levels, level_index + 1, preferred_length)
        return

    # Walk through segments defined by these breakpoints.
    # Segments: [region_start..split_positions[0]], [split_positions[0]..split_positions[1]], ...
    # Accumulate adjacent segments as long as total stays under preferred_length.
    current_start = region_start
    current_end = region_start  # The last accepted breakpoint position.

    for pos in split_positions:
        segment_length = pos - current_start
        if segment_length < preferred_length:
            # Still within budget; extend the accumulated region.
            current_end = pos
        else:
            # This segment would exceed preferred_length.
            if current_start == current_end:
                # No accumulated text before this point; the single segment
                # from current_start to pos is oversized. Sub-chunk it.
                yield from _sub_chunk_segment(text, current_start, pos, breakpoints, levels, level_index + 1, preferred_length)
                current_start = pos
                current_end = pos
            else:
                # Yield the accumulated region up to current_end.
                yield text[current_start:current_end]
                current_start = current_end
                current_end = pos
                # Check if the new segment (current_start to pos) is also oversized.
                if (pos - current_start) >= preferred_length:
                    yield from _sub_chunk_segment(text, current_start, pos, breakpoints, levels, level_index + 1, preferred_length)
                    current_start = pos
                    current_end = pos

    # Yield remaining text from current_start to region_end.
    if current_start < region_end:
        remaining = region_end - current_start
        if remaining >= preferred_length and current_start != current_end:
            # Yield accumulated portion, then sub-chunk the rest.
            yield text[current_start:current_end]
            yield from _sub_chunk_segment(text, current_end, region_end, breakpoints, levels, level_index + 1, preferred_length)
        elif remaining >= preferred_length:
            # Single oversized tail; sub-chunk it.
            yield from _sub_chunk_segment(text, current_start, region_end, breakpoints, levels, level_index + 1, preferred_length)
        else:
            yield text[current_start:region_end]


def _sub_chunk_segment(text, seg_start, seg_end, breakpoints, levels, next_level_index, preferred_length):
    """
    Sub-chunk an oversized segment by trying the next breakpoint level.

    If no further levels are available, yields the segment as-is.
    """
    if next_level_index < len(levels):
        yield from _chunk_text_at_level(text, seg_start, seg_end, breakpoints, levels, next_level_index, preferred_length)
    else:
        yield text[seg_start:seg_end]



                    





    


    
def build_metadata_suffix(item_number, working_item, content_pointer=None, item_type_names=None):
    """
    Build annotation and notes suffix for sections that are part of duplicate sets.

    This function creates a formatted metadata suffix containing annotation and notes
    fields for ALL sections in a duplicate set (both the original and all _dup variants).
    The suffix is designed to be appended to text chunks during summarization and question
    answering to provide AI models with critical context about alternative versions,
    supersession dates, and conditional applicability.

    Args:
        item_number: The item identifier (e.g., "section_123" or "section_123_dup")
        working_item: Dictionary containing item data including 'annotation' and 'notes'
        content_pointer: Optional dictionary containing all items (used to check for duplicates)
        item_type_names: Optional string key for the item type (e.g., "sections", "articles")

    Returns:
        Formatted metadata suffix string with [ANNOTATION] and [NOTES] markers,
        or empty string if no metadata or not part of a duplicate set

    Example:
        >>> build_metadata_suffix("section_2050_dup", {"annotation": "Amended 2020", "notes": {}})
        '\n\n[ANNOTATION]: Amended 2020'
        >>> build_metadata_suffix("section_2050", {"annotation": "Original"}, content, "sections")
        '\n\n[ANNOTATION]: Original'  # If section_2050_dup exists
    """
    # Check if this section is part of a duplicate set
    is_duplicate_set = False

    # Case 1: This item has _dup in its identifier
    if '_dup' in str(item_number):
        is_duplicate_set = True
    # Case 2: Check if a duplicate of this item exists (item_number + '_dup')
    elif content_pointer is not None and item_type_names is not None:
        # Check if there's a section with this number + '_dup'
        if item_number + '_dup' in content_pointer.get(item_type_names, {}):
            is_duplicate_set = True

    if not is_duplicate_set:
        return ""  # No metadata for non-duplicate sections

    suffix_parts = []

    # Add annotation if present
    annotation = working_item.get('annotation', '')
    if annotation and annotation.strip():
        suffix_parts.append(f"[ANNOTATION]: {annotation}")

    # Add notes if present
    notes = working_item.get('notes', {})
    if notes and isinstance(notes, dict):
        notes_list = []
        for note_id, note_text in notes.items():
            notes_list.append(f"Note {note_id}: {note_text}")

        if notes_list:
            notes_formatted = ", ".join(notes_list)
            suffix_parts.append(f"[NOTES]: {notes_formatted}")

    if suffix_parts:
        return "\n\n" + "\n\n".join(suffix_parts)

    return ""


def augment_chunk_with_metadata(chunk_text, metadata_suffix):
    """
    Append metadata suffix to a text chunk.

    This function augments a text chunk with annotation and notes metadata by appending
    the metadata suffix. It's designed to be called for each chunk when processing
    sections with duplicate identifiers, ensuring that every chunk has access to the
    full metadata context.

    Args:
        chunk_text: The chunk text content
        metadata_suffix: The metadata suffix from build_metadata_suffix()

    Returns:
        Chunk text with metadata suffix appended (if suffix is non-empty),
        otherwise returns the original chunk text unchanged

    Example:
        >>> chunk = "Section text here..."
        >>> suffix = "\n\n[ANNOTATION]: Amended 2020"
        >>> augment_chunk_with_metadata(chunk, suffix)
        'Section text here...\n\n[ANNOTATION]: Amended 2020'
    """
    if metadata_suffix:
        return chunk_text + metadata_suffix
    return chunk_text
