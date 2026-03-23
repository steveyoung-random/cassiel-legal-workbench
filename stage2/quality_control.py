"""
Definition quality control functions for evaluating and improving definitions.

This module provides functions for evaluating definition quality, retrying extraction
for poor definitions, and removing definitions that cannot be improved.
"""
# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License

from collections import defaultdict
from utils import (
    iter_definitions,
    query_json,
    canonical_org_types,
    get_document_issues_logfile,
    build_definition_quality_evaluation_prompt,
    build_definition_retry_prompt,
    build_external_reference_validation_prompt,
    build_high_conflict_review_prompt,
    lookup_item,
    chunk_text,
    InputError,
    ModelError,
)
from utils.config import create_client_for_task
from .processor import DefinitionsProcessor
from .indirect_resolution import remove_definition

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


def check_external_reference_validity(proc, def_entry, client=None):
    """
    Secondary quality check for definitions with external references.

    This function is called when a definition has an 'external_reference' field
    but failed the primary quality evaluation. It checks whether the definition
    text clearly relies on an external source for the actual definition.

    Args:
        proc (DefinitionsProcessor): The processor instance
        def_entry (dict): The definition entry with external_reference field
        client: Optional AI client for task-specific model routing

    Returns:
        bool: True if this is a valid indirect external reference, False otherwise

    Raises:
        ModelError: If AI model fails to respond
    """
    term = def_entry.get('term', '')
    definition = def_entry.get('value', '').strip()
    external_reference = def_entry.get('external_reference', '')

    if not external_reference:
        return False

    # Build external reference validation prompt
    prompt = build_external_reference_validation_prompt(term, definition, external_reference)

    # Use provided client or fall back to default
    active_client = client if client is not None else proc.client

    # Query AI model
    try:
        result = query_json(active_client, [], prompt, proc.logfile,
                           config=proc.config, task_name='stage2.definitions.validate_external')

        # Parse result (expect [1] or [0])
        if isinstance(result, list) and len(result) > 0:
            if result[0] == 1 or result[0] == '1':
                return True

        return False
    except Exception as e:
        # Fail fast on errors
        raise ModelError(f"Failed to validate external reference for term '{term}': {e}")


def evaluate_definition_quality(proc, def_entry, source_text, cache_prompt, client=None, conflict_count=0):
    """
    Evaluate whether a definition is of acceptable quality.

    Args:
        proc (DefinitionsProcessor): The processor instance
        def_entry (dict): The definition entry to evaluate
        source_text (str): The source text containing the definition
        cache_prompt (str): Cached prompt containing the source text
        client: Optional AI client for task-specific model routing
        conflict_count (int): Number of conflicting candidates reviewed for this term (triggers skeptical prompt)

    Returns:
        bool: True if definition is acceptable, False if needs retry/remove

    Raises:
        InputError: If definition entry is missing required fields
        ModelError: If AI model fails to respond
    """
    if 'term' not in def_entry:
        raise InputError(f"Definition entry missing 'term' field: {def_entry}")
    if 'value' not in def_entry:
        raise InputError(f"Definition entry missing 'value' field: {def_entry}")

    term = def_entry['term']
    definition = def_entry.get('value', '').strip()

    # Quick check: if definition is empty, it's not good
    if not definition:
        return False

    # Quick check: if definition is just the term itself, it's circular
    if definition.lower() == term.lower():
        return False

    # Get source item type for prompt
    source_type = def_entry.get('source_type', 'section')

    # Build evaluation prompt (account for elaborations); apply skeptical preamble for high-conflict terms
    def_kind = def_entry.get('def_kind', 'direct')
    prompt = build_definition_quality_evaluation_prompt(term, definition, source_type, def_kind, conflict_count=conflict_count)

    # Use provided client or fall back to default
    active_client = client if client is not None else proc.client

    # Query AI model (no cached source text to avoid unnecessary/contextual confusion)
    try:
        result = query_json(active_client, [], prompt, proc.logfile,
                           config=proc.config, task_name='stage2.definitions.evaluate_quality')
        
        # Parse result (expect [1] or [0])
        if isinstance(result, list) and len(result) > 0:
            if result[0] == 1 or result[0] == '1':
                return True
        
        return False
    except Exception as e:
        # Fail fast on errors
        raise ModelError(f"Failed to evaluate definition quality for term '{term}': {e}")


def improve_definition_from_text(proc, term, existing_definition, source_text, cache_prompt, item_type_name, def_kind: str = 'direct', client=None):
    """
    Retry definition extraction from source text.

    Args:
        proc (DefinitionsProcessor): The processor instance
        term (str): The term to find a definition for
        existing_definition (str): The existing definition (may be empty or poor)
        source_text (str): The source text to search
        cache_prompt (str): Cached prompt containing the source text (text is already in cache)
        item_type_name (str): Type of the item containing the text
        def_kind (str): 'direct' or 'elaboration'
        client: Optional AI client for task-specific model routing

    Returns:
        str or None: Improved definition if found, None otherwise

    Raises:
        ModelError: If AI model fails to respond
    """
    # Build retry prompt (source_text is already in cache_prompt, so we don't need to include it)
    prompt = build_definition_retry_prompt(term, existing_definition, item_type_name, def_kind)
    # Note: source_text is already in cache_prompt, so we just use the cache

    # Use provided client or fall back to default
    active_client = client if client is not None else proc.client

    try:
        result = query_json(active_client, [cache_prompt], prompt, proc.logfile,
                           config=proc.config, task_name='stage2.definitions.retry_extraction')
        
        # Parse result (expect {"term": "...", "value": "..."} or {})
        if isinstance(result, dict):
            if 'term' in result and 'value' in result:
                improved_def = result['value'].strip()
                if improved_def and improved_def.lower() != term.lower():
                    return improved_def
        
        return None
    except Exception as e:
        # Fail fast on errors
        raise ModelError(f"Failed to improve definition for term '{term}': {e}")


def _normalize_term_for_high_conflict(term):
    """Lowercase and strip leading articles for grouping conflicting definitions."""
    t = term.lower().strip()
    for article in ('the ', 'a ', 'an '):
        if t.startswith(article):
            t = t[len(article):]
            break
    return t


def review_high_conflict_terms(proc, threshold=3):
    """
    Review terms with many conflicting definition candidates for authenticity.

    For each term that appears as 'quality_checked: False' across N >= threshold
    different source units, sends a single AI call asking whether the candidates
    are genuine definitions. Non-genuine groups are removed. Genuine survivors
    receive a '_conflict_count' annotation so that the subsequent quality
    evaluation step applies a skeptical prompt.

    Args:
        proc (DefinitionsProcessor): The processor instance
        threshold (int): Minimum number of distinct source units to trigger review
    """
    print("Reviewing high-conflict definition terms...")

    # Create task-specific client
    review_client = None
    if proc.config:
        from utils.config import create_client_for_task
        review_client = create_client_for_task(proc.config, 'stage2.definitions.review_high_conflict')

    active_client = review_client if review_client is not None else proc.client
    document_issues_logfile = get_document_issues_logfile(proc.out_path)

    # Group un-evaluated definitions by normalized term
    # Each group maps normalized_term -> list of (def_entry, org_context, operational_context)
    term_groups = defaultdict(list)
    for def_entry, org_context, operational_context in iter_definitions(proc.parsed_content):
        if def_entry.get('quality_checked', False):
            continue
        # Skip indirect definitions awaiting resolution
        if 'indirect_loc_type' in def_entry and 'indirect_loc_number' in def_entry:
            continue
        norm = _normalize_term_for_high_conflict(def_entry.get('term', ''))
        term_groups[norm].append((def_entry, org_context, operational_context))

    # Filter to high-conflict groups: N distinct source units >= threshold
    reviewed = 0
    for norm_term, entries in term_groups.items():
        # Count distinct source (type, number) pairs
        sources = set()
        for def_entry, _, _ in entries:
            s_type = def_entry.get('source_type', '')
            s_num = def_entry.get('source_number', '')
            if s_type and s_num:
                sources.add((s_type, s_num))
        if len(sources) < threshold:
            continue

        # Build candidate list for the prompt
        term = entries[0][0].get('term', norm_term)
        candidates = []
        for def_entry, _, _ in entries:
            candidates.append({
                'value': def_entry.get('value', '').strip(),
                'source_type': def_entry.get('source_type', 'unknown'),
                'source_number': def_entry.get('source_number', 'unknown'),
            })

        print(f"  High-conflict term '{term}': {len(entries)} candidates from {len(sources)} source units — reviewing...")

        try:
            prompt = build_high_conflict_review_prompt(term, candidates)
            result = query_json(active_client, [], prompt, proc.logfile,
                               config=proc.config, task_name='stage2.definitions.review_high_conflict')

            verdict = result.get('verdict', '') if isinstance(result, dict) else ''
            reason = result.get('reason', '') if isinstance(result, dict) else ''

            if verdict == 'not_definitions':
                # Remove all candidates in this group
                for def_entry, org_context, operational_context in entries:
                    remove_definition(proc, def_entry, org_context, operational_context,
                                     logfile_path=document_issues_logfile,
                                     previous_definition=def_entry.get('value', ''),
                                     reason=f'high_conflict_review: not_definitions ({reason})')
                print(f"    Removed {len(entries)} candidates — verdict: not_definitions")
            elif verdict == 'genuine':
                # Annotate all surviving candidates with conflict count for skeptical QC
                n = len(entries)
                for def_entry, _, _ in entries:
                    def_entry['_conflict_count'] = n
                proc.dirty = 1
                print(f"    Kept {n} candidates — verdict: genuine (skeptical QC will apply)")
            else:
                # Unexpected response: treat conservatively as not_definitions
                for def_entry, org_context, operational_context in entries:
                    remove_definition(proc, def_entry, org_context, operational_context,
                                     logfile_path=document_issues_logfile,
                                     previous_definition=def_entry.get('value', ''),
                                     reason=f'high_conflict_review: unexpected verdict "{verdict}" — treated as not_definitions')
                print(f"    Removed {len(entries)} candidates — unexpected verdict '{verdict}'")

        except Exception as e:
            print(f"    WARNING: high-conflict review failed for '{term}': {e} — skipping")

        reviewed += 1

    if reviewed == 0:
        print("  No high-conflict terms found.")
    else:
        print(f"  Reviewed {reviewed} high-conflict term(s).")
    proc.flush()


def evaluate_and_improve_definitions(proc, count=30):
    """
    Evaluate and improve definitions after scope resolution.

    This function processes all definitions that have been moved to final locations,
    evaluates their quality, retries extraction for poor/blank definitions, and
    removes definitions that cannot be improved.

    Modified to evaluate each unique definition only once, then update all copies
    across different locations. This ensures consistency and efficiency when the
    same definition from a source appears in multiple substantive units.

    Args:
        proc (DefinitionsProcessor): The processor instance containing document context
        count (int): Maximum number of unique definitions to process in this run

    The function:
    1. Deduplicates definitions by (source_type, source_number, term)
    2. Tracks all locations where each unique definition appears
    3. Evaluates each unique definition once
    4. Updates all copies with the same result (improved value or removal)
    5. Marks all copies as evaluated
    """
    print("Evaluating and improving definitions...")

    # Create task-specific clients if config is available
    if proc.config:
        evaluate_client = create_client_for_task(proc.config, 'stage2.definitions.evaluate_quality')
        retry_client = create_client_for_task(proc.config, 'stage2.definitions.retry_extraction')
        validate_client = create_client_for_task(proc.config, 'stage2.definitions.validate_external')
    else:
        # Fallback to default client for backward compatibility
        evaluate_client = None
        retry_client = None
        validate_client = None

    # Get document issues logfile for logging definition removals
    document_issues_logfile = get_document_issues_logfile(proc.out_path)

    # Track unique definitions and all their locations
    # Key: (source_type, source_number, term) or (None, None, term, id) for definitions without source
    # Value: {
    #   'canonical_def': def_entry (the first copy found, used for evaluation),
    #   'all_copies': [(def_entry, org_context, operational_context), ...]
    # }
    unique_definitions = {}
    source_texts = {}
    source_items = {}  # (source_type, source_number) -> source item dict (for breakpoints/chunk_prefix)

    # Collect all definitions that need evaluation and group by unique key
    for def_entry, org_context, operational_context in iter_definitions(proc.parsed_content):
        # Skip already evaluated definitions
        if def_entry.get('quality_checked', False):
            continue

        # Skip indirect definitions that still need resolution (handled separately)
        if 'indirect_loc_type' in def_entry and 'indirect_loc_number' in def_entry:
            continue

        # Get source information
        source_type = def_entry.get('source_type', '')
        source_number = def_entry.get('source_number', '')
        term = def_entry.get('term', '')

        # Create unique key
        if not source_type or not source_number:
            # No source information - create unique key per instance
            # Use id(def_entry) to make each instance unique (can't deduplicate without source info)
            unique_key = (None, None, term, id(def_entry))
        else:
            # Normal case: unique key is (source_type, source_number, term)
            unique_key = (source_type, source_number, term)

        # Track this definition
        if unique_key not in unique_definitions:
            unique_definitions[unique_key] = {
                'canonical_def': def_entry,
                'all_copies': []
            }

        # Add this copy to the list
        unique_definitions[unique_key]['all_copies'].append((def_entry, org_context, operational_context))

        # Get source item if not already cached (and source info exists)
        if source_type and source_number:
            source_key = (source_type, source_number)
            if source_key not in source_items:
                source_type_singular, source_type_plural = canonical_org_types(source_type)
                source_item = lookup_item(proc.parsed_content, source_type_plural, source_number)
                source_items[source_key] = source_item or {}
                source_texts[source_key] = (source_item or {}).get('text', '')

    # Process unique definitions
    for unique_key, def_info in unique_definitions.items():
        if count < 1:
            break

        canonical_def = def_info['canonical_def']
        all_copies = def_info['all_copies']

        # Extract components from unique_key
        if len(unique_key) == 4:
            # Definitions without source info (has id as 4th element)
            source_type, source_number, term, _ = unique_key
        else:
            # Normal definitions with source info
            source_type, source_number, term = unique_key

        # Handle definitions without source information
        if source_type is None or source_number is None:
            # Quick evaluation: if empty or just term, remove from all locations
            definition = canonical_def.get('value', '').strip()
            if not definition or definition.lower() == term.lower():
                reason = 'empty or circular definition (no source information available for evaluation)'
                # Remove from all locations
                for def_copy, org_context, operational_context in all_copies:
                    remove_definition(proc, def_copy, org_context, operational_context,
                                     logfile_path=document_issues_logfile,
                                     previous_definition=definition,
                                     reason=reason)
                print(f"Removed poor definition for '{term}' from {len(all_copies)} location(s) (no source information)")
            else:
                # Mark all copies as checked (can't evaluate without source text)
                for def_copy, org_context, operational_context in all_copies:
                    def_copy['quality_checked'] = True
                proc.dirty = 1

            count -= 1
            continue

        # Get source text
        source_key = (source_type, source_number)
        source_text = source_texts.get(source_key, '')

        if not source_text:
            # No source text available, mark all copies as checked
            for def_copy, org_context, operational_context in all_copies:
                def_copy['quality_checked'] = True
            proc.dirty = 1
            count -= 1
            continue

        # Build cache prompt for source item, scoped to the chunk containing this term.
        # For short items (no breakpoints, or text < chunk size), chunk_text yields the
        # full text as a single chunk — behavior is identical to the pre-chunking path.
        source_type_singular, source_type_plural = canonical_org_types(source_type)
        cap_source_type = source_type[0].upper() + source_type[1:] if source_type else 'Section'
        src_item = source_items.get(source_key, {})
        breakpoints = src_item.get('breakpoints', [])
        chunk_prefix = src_item.get('chunk_prefix', '')
        chunks = list(chunk_text(source_text, breakpoints, preferred_length=_STAGE2_CHUNK_SIZE))
        # Locate-then-extract: find first chunk containing the term (case-insensitive).
        # Falls back to first chunk (= full text for single-chunk items) if not found.
        target_chunk = chunks[0] if chunks else source_text
        if len(chunks) > 1:
            term_lower = term.lower()
            for c in chunks:
                if term_lower in c.lower():
                    target_chunk = c
                    break
        cache_prompt = _build_item_cache_prompt(source_type, cap_source_type, source_number, chunk_prefix, target_chunk)

        # Evaluate the canonical definition (first copy found)
        existing_definition = canonical_def.get('value', '').strip()

        # Read conflict_count annotation set by review_high_conflict_terms (if any)
        conflict_count = canonical_def.get('_conflict_count', 0)

        # Evaluate definition quality (skeptical preamble applied if conflict_count > 0)
        is_good = evaluate_definition_quality(proc, canonical_def, source_text, cache_prompt, evaluate_client, conflict_count=conflict_count)

        if is_good:
            # Definition is good, mark all copies as checked and strip internal annotation
            for def_copy, org_context, operational_context in all_copies:
                def_copy['quality_checked'] = True
                def_copy.pop('_conflict_count', None)
            proc.dirty = 1
            print(f"Definition for '{term}' is acceptable (updated {len(all_copies)} location(s))")
        else:
            # Definition is poor or blank, try to improve
            improved_def = improve_definition_from_text(proc, term, existing_definition, source_text, cache_prompt, source_type, canonical_def.get('def_kind', 'direct'), retry_client)

            if improved_def:
                # Update all copies with improved version
                for def_copy, org_context, operational_context in all_copies:
                    def_copy['value'] = improved_def

                # Re-evaluate improved definition (using canonical_def which now has improved value)
                is_good_after_retry = evaluate_definition_quality(proc, canonical_def, source_text, cache_prompt, evaluate_client, conflict_count=conflict_count)

                if is_good_after_retry:
                    # Improved definition is good, mark all copies as checked and strip annotation
                    for def_copy, org_context, operational_context in all_copies:
                        def_copy['quality_checked'] = True
                        def_copy.pop('_conflict_count', None)
                    proc.dirty = 1
                    print(f"Improved definition for '{term}' (updated {len(all_copies)} location(s))")
                else:
                    # Improved definition is still not good
                    # STAGE 2: Check if this is a valid external reference before removing
                    if canonical_def.get('external_reference'):
                        is_valid_external_ref = check_external_reference_validity(proc, canonical_def, validate_client)
                        if is_valid_external_ref:
                            # This is a valid indirect external reference, keep it
                            for def_copy, org_context, operational_context in all_copies:
                                def_copy['quality_checked'] = True
                                def_copy.pop('_conflict_count', None)
                            proc.dirty = 1
                            print(f"Definition for '{term}' retained as valid external reference (updated {len(all_copies)} location(s))")
                        else:
                            # Not a valid external reference, remove from all locations
                            reason = 'improved definition still not acceptable after retry; external reference check failed'
                            previous_def_info = f"Original: '{existing_definition if existing_definition else '(empty)'}'; Improved: '{improved_def}'"
                            for def_copy, org_context, operational_context in all_copies:
                                remove_definition(proc, def_copy, org_context, operational_context,
                                                 logfile_path=document_issues_logfile,
                                                 previous_definition=previous_def_info,
                                                 reason=reason)
                            print(f"Removed definition for '{term}' from {len(all_copies)} location(s) (could not be improved; not a valid external reference)")
                    else:
                        # No external reference, remove from all locations
                        reason = 'improved definition still not acceptable after retry'
                        previous_def_info = f"Original: '{existing_definition if existing_definition else '(empty)'}'; Improved: '{improved_def}'"
                        for def_copy, org_context, operational_context in all_copies:
                            remove_definition(proc, def_copy, org_context, operational_context,
                                             logfile_path=document_issues_logfile,
                                             previous_definition=previous_def_info,
                                             reason=reason)
                        print(f"Removed definition for '{term}' from {len(all_copies)} location(s) (could not be improved)")
            else:
                # Could not improve definition
                # STAGE 2: Check if this is a valid external reference before removing
                if canonical_def.get('external_reference'):
                    is_valid_external_ref = check_external_reference_validity(proc, canonical_def, validate_client)
                    if is_valid_external_ref:
                        # This is a valid indirect external reference, keep it
                        for def_copy, org_context, operational_context in all_copies:
                            def_copy['quality_checked'] = True
                            def_copy.pop('_conflict_count', None)
                        proc.dirty = 1
                        print(f"Definition for '{term}' retained as valid external reference (updated {len(all_copies)} location(s))")
                    else:
                        # Not a valid external reference, remove from all locations
                        reason = 'could not extract improved definition from source text; external reference check failed'
                        for def_copy, org_context, operational_context in all_copies:
                            remove_definition(proc, def_copy, org_context, operational_context,
                                             logfile_path=document_issues_logfile,
                                             previous_definition=existing_definition,
                                             reason=reason)
                        print(f"Removed definition for '{term}' from {len(all_copies)} location(s) (could not be improved; not a valid external reference)")
                else:
                    # No external reference, remove from all locations
                    reason = 'could not extract improved definition from source text'
                    for def_copy, org_context, operational_context in all_copies:
                        remove_definition(proc, def_copy, org_context, operational_context,
                                         logfile_path=document_issues_logfile,
                                         previous_definition=existing_definition,
                                         reason=reason)
                    print(f"Removed definition for '{term}' from {len(all_copies)} location(s) (could not be improved)")

        count -= 1

    proc.flush()
    if count < 1:
        print('More definition evaluation needed.  Please run again.\n')
        exit(0)
    print("Definition evaluation complete.")

