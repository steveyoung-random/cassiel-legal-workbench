"""
Document issues logging utilities.

This module provides functions for logging document-level issues (separate from AI call logs),
such as scope resolution problems, missing context, definition removals, etc.
"""
# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License

import os
import json


def get_document_issues_logfile(dir_path=''):
    """
    Get a logfile path for document-level issues (separate from AI call logfiles).
    
    This logfile is used for issues encountered during document processing that are
    not AI call errors, such as scope resolution problems, missing context, etc.
    
    Args:
        dir_path (str): Directory path where logfile should be created
        
    Returns:
        str: Path to the logfile
    """
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


def log_document_issue(logfile_path, issue_type, item_type_name=None, item_number=None, issue_description='', **kwargs):
    """
    Log a document-level issue to the separate logfile.
    
    Args:
        logfile_path (str): Path to the logfile
        issue_type (str): Type of issue (e.g., 'scope_resolution', 'missing_context')
        item_type_name (str, optional): Type of the item where issue occurred
        item_number (str, optional): Number/identifier of the item
        issue_description (str): Description of the issue
        **kwargs: Additional fields to include in the log entry
    """
    log_entry = {
        'issue_type': issue_type,
        'issue': issue_description,
        'timestamp': str(os.path.getmtime(logfile_path) if os.path.exists(logfile_path) else '')
    }
    if item_type_name:
        log_entry['item_type'] = item_type_name
    if item_number:
        log_entry['item_number'] = item_number
    # Add any additional fields
    log_entry.update(kwargs)
    
    with open(logfile_path, "a", encoding='utf-8') as logfile_handle:
        logfile_handle.write(json.dumps(log_entry, indent=4) + '\n')

