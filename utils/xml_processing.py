"""
XML processing utilities for parsing legal documents.

This module provides common XML manipulation and traversal functions
that are used across different document parsers.
"""
# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License

from lxml import etree as ET
from typing import Iterator


def get_all_elements(tree, pattern) -> Iterator[ET.Element]:
    """
    Get all elements matching a pattern in an XML tree.
    
    Args:
        tree: The XML element tree to search
        pattern: The tag pattern to match (e.g., 'section', 'ARTICLE')
        
    Yields:
        ET.Element: Each matching element found in the tree
    """
    if pattern in tree.tag:
        yield(tree)
    matches = tree.findall(".//" + pattern)
    if matches:
        for m in matches:
            yield(m)


def get_first_element(tree, pattern):
    """
    Get the first element matching a pattern in an XML tree.
    
    Args:
        tree: The XML element tree to search
        pattern: The tag pattern to match (e.g., 'section', 'ARTICLE')
        
    Returns:
        ET.Element or None: The first matching element, or None if not found
    """
    result = None
    matches = tree.findall(".//" + pattern)
    if matches and len(matches) > 0:
        result = matches[0]
    return result


def drop_ns_and_prefix_to_underscore(root):
    """
    Convert an lxml tree to one without namespaces.
    
    - Elements that had an explicit prefix like <dc:term> become <dc_term>.
    - Elements that only inherited a default namespace keep their local tag (e.g., <title> stays <title>).
    - Works in-place. Accepts an Element or ElementTree and returns the root Element.
    
    Args:
        root: An lxml Element or ElementTree to process
        
    Returns:
        ET.Element: The root element with namespaces removed
        
    Notes:
        - Only element tags are changed, per the request. Attributes are left as-is.
    """
    # Normalize to an Element root
    if isinstance(root, ET._ElementTree):
        root = root.getroot()

    for el in root.iter():
        q = ET.QName(el)
        local = q.localname
        pref = el.prefix  # None if there was no explicit prefix in the source
        el.tag = f"{pref}_{local}" if pref else local

    return root

