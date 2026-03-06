"""
Processing Status Management Utilities

This module provides utilities for tracking processing status, progress, and
checkpoints in processed JSON files.
"""
# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License

from datetime import datetime, timezone
from typing import Dict, Any, Optional


def init_processing_status(parsed_content: Dict[str, Any]) -> None:
    """
    Initialize processing_status structure in parsed_content if it doesn't exist.

    Args:
        parsed_content: The parsed document content dictionary
    """
    if 'document_information' not in parsed_content:
        parsed_content['document_information'] = {}

    if 'processing_status' not in parsed_content['document_information']:
        parsed_content['document_information']['processing_status'] = {
            'parsed': True,
            'stage_2_complete': False,
            'stage_2_progress': {
                'total_items': 0,
                'processed_items': 0,
                'current_phase': 'initialization',
                'last_updated': None
            },
            'stage_3_complete': False,
            'stage_3_progress': {
                'operational': {
                    'total': 0,
                    'summary_1': 0,
                    'summary_2': 0
                },
                'organizational': {
                    'total': 0,
                    'summary_2': 0
                },
                'last_updated': None
            },
            'checkpoint_state': {
                'stage': None,
                'last_processed_item': None,
                'checkpoint_threshold': 30,
                'items_since_checkpoint': 0
            }
        }


def get_processing_status(parsed_content: Dict[str, Any]) -> Dict[str, Any]:
    """
    Get processing_status from parsed_content, initializing if needed.

    Args:
        parsed_content: The parsed document content dictionary

    Returns:
        The processing_status dictionary
    """
    init_processing_status(parsed_content)
    return parsed_content['document_information']['processing_status']


def update_stage_2_progress(parsed_content: Dict[str, Any],
                            total_items: int,
                            processed_items: int,
                            current_phase: str = 'processing',
                            set_complete: Optional[bool] = None) -> None:
    """
    Update Stage 2 processing progress.

    Args:
        parsed_content: The parsed document content dictionary
        total_items: Total number of items to process
        processed_items: Number of items processed so far
        current_phase: Current processing phase (e.g., 'extracting_definitions', 'resolving_scopes')
        set_complete: If provided, explicitly sets stage_2_complete to this value.
                      If None (default), derives it from processed_items >= total_items.
                      Pass set_complete=False from sub-functions to avoid prematurely marking
                      Stage 2 complete before all processing steps have run.
    """
    status = get_processing_status(parsed_content)
    status['stage_2_progress']['total_items'] = total_items
    status['stage_2_progress']['processed_items'] = processed_items
    status['stage_2_progress']['current_phase'] = current_phase
    status['stage_2_progress']['last_updated'] = datetime.now(timezone.utc).isoformat()

    # Update completion status
    if set_complete is not None:
        status['stage_2_complete'] = set_complete
    else:
        status['stage_2_complete'] = (processed_items >= total_items) if total_items > 0 else False


def update_stage_3_progress(parsed_content: Dict[str, Any],
                            operational_counts: Optional[Dict[str, int]] = None,
                            organizational_counts: Optional[Dict[str, int]] = None) -> None:
    """
    Update Stage 3 processing progress.

    Args:
        parsed_content: The parsed document content dictionary
        operational_counts: Dict with 'total', 'summary_1', 'summary_2' counts for operational items
        organizational_counts: Dict with 'total', 'summary_2' counts for organizational units
    """
    status = get_processing_status(parsed_content)

    if operational_counts:
        status['stage_3_progress']['operational'].update(operational_counts)

    if organizational_counts:
        status['stage_3_progress']['organizational'].update(organizational_counts)

    status['stage_3_progress']['last_updated'] = datetime.now(timezone.utc).isoformat()

    # Update completion status - both operational AND organizational must have summary_2
    op = status['stage_3_progress']['operational']
    org = status['stage_3_progress']['organizational']

    op_complete = (op['summary_2'] >= op['total']) if op['total'] > 0 else True
    org_complete = (org['summary_2'] >= org['total']) if org['total'] > 0 else True

    status['stage_3_complete'] = op_complete and org_complete


def _context_is_in_scope(context, scope_keys):
    """
    True if this org unit's context is at or below the content_scope root.

    context: list of {"name": type, "number": id} dicts from iter_org_content
    scope_keys: list of (type, id) tuples derived from content_scope
    """
    if len(context) < len(scope_keys):
        return False  # Above scope root
    for i, (scope_type, scope_id) in enumerate(scope_keys):
        if i >= len(context):
            return False
        ctx_type = context[i].get('name')
        ctx_id = context[i].get('number')
        if ctx_type != scope_type or ctx_id != scope_id:
            return False
    return True


def count_stage_3_progress(parsed_content: Dict[str, Any]) -> tuple:
    """
    Count items with summaries for Stage 3 progress tracking.

    Args:
        parsed_content: The parsed document content dictionary

    Returns:
        Tuple of (operational_counts, organizational_counts) dictionaries
    """
    from utils.document_handling import iter_operational_items, iter_org_content

    operational = {'total': 0, 'summary_1': 0, 'summary_2': 0}

    # Count operational items
    for _, _, _, _, working_item in iter_operational_items(parsed_content):
        operational['total'] += 1
        if 'summary_1' in working_item and working_item['summary_1']:
            operational['summary_1'] += 1
        if 'summary_2' in working_item and working_item['summary_2']:
            operational['summary_2'] += 1

    # Build scope path for org-unit filtering (if content_scope present)
    content_scope = parsed_content.get('document_information', {}).get('content_scope')
    scope_keys = None
    if content_scope:
        scope_keys = [(list(entry.keys())[0], list(entry.values())[0]) for entry in content_scope]

    # Count organizational units
    organizational = {'total': 0, 'summary_2': 0}
    if ('document_information' in parsed_content and
        'organization' in parsed_content['document_information'] and
        'content' in parsed_content['document_information']['organization']):

        for org_item, context in iter_org_content(parsed_content):
            # Skip the root container (empty context) - it's not an actual organizational unit
            # Only count units that have organizational unit fields like 'unit_title'
            if context or 'unit_title' in org_item:
                # Skip org units above content_scope root (only count in-scope units)
                if scope_keys and not _context_is_in_scope(context, scope_keys):
                    continue
                # Skip empty/reserved organizational units that have no content to summarize.
                # These have only unit_title (and possibly unit_definitions or summary_ fields)
                # but no begin_/stop_ tags and no child org units.
                has_content = any(
                    k.startswith('begin_') or k.startswith('stop_')
                    for k in org_item.keys()
                ) or any(
                    isinstance(org_item[k], dict)
                    for k in org_item.keys()
                    if not k.startswith(('unit_title', 'unit_definitions', 'begin_', 'stop_', 'summary_'))
                )
                if not has_content:
                    continue
                organizational['total'] += 1
                if 'summary_2' in org_item and org_item['summary_2']:
                    organizational['summary_2'] += 1

    return operational, organizational


def update_checkpoint_state(parsed_content: Dict[str, Any],
                            stage: str,
                            last_processed_item: str,
                            threshold: int = 30) -> None:
    """
    Update checkpoint state for resumable processing.

    Args:
        parsed_content: The parsed document content dictionary
        stage: Current processing stage (e.g., 'stage_2', 'stage_3')
        last_processed_item: Identifier of last processed item
        threshold: Number of items between checkpoints
    """
    status = get_processing_status(parsed_content)
    checkpoint = status['checkpoint_state']

    checkpoint['stage'] = stage
    checkpoint['last_processed_item'] = last_processed_item
    checkpoint['checkpoint_threshold'] = threshold
    checkpoint['items_since_checkpoint'] += 1


def should_checkpoint(parsed_content: Dict[str, Any]) -> bool:
    """
    Check if processing should checkpoint (save state).

    Args:
        parsed_content: The parsed document content dictionary

    Returns:
        True if checkpoint threshold reached, False otherwise
    """
    status = get_processing_status(parsed_content)
    checkpoint = status['checkpoint_state']

    return checkpoint['items_since_checkpoint'] >= checkpoint['checkpoint_threshold']


def reset_checkpoint_counter(parsed_content: Dict[str, Any]) -> None:
    """
    Reset the checkpoint item counter after saving.

    Args:
        parsed_content: The parsed document content dictionary
    """
    status = get_processing_status(parsed_content)
    status['checkpoint_state']['items_since_checkpoint'] = 0


def is_stage_2_complete(parsed_content: Dict[str, Any]) -> bool:
    """
    Check if Stage 2 is complete.

    Args:
        parsed_content: The parsed document content dictionary

    Returns:
        True if Stage 2 is complete, False otherwise
    """
    status = get_processing_status(parsed_content)
    return status.get('stage_2_complete', False)


def is_stage_3_complete(parsed_content: Dict[str, Any]) -> bool:
    """
    Check if Stage 3 is complete.

    Args:
        parsed_content: The parsed document content dictionary

    Returns:
        True if Stage 3 is complete, False otherwise
    """
    status = get_processing_status(parsed_content)
    return status.get('stage_3_complete', False)
