"""
Chunking helpers for long text processing in legal document analysis.

This module provides utilities for handling text chunking in the document analysis
pipeline, specifically for summary generation in Process_Stage_3.py.

Key functions:
- create_chunk_summary_prompt: Creates prompts for chunk summarization
- synthesize_final_summary: Combines chunk summaries into final summary
- deduplicate_references: Removes duplicate references from chunk results
"""
# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License

from typing import List, Dict, Any, Optional
from utils.ai_client import QueryWithBaseClient, query_text_with_retry
from utils.text_processing import clean_text, clean_summary_text, strip_emphasis_marks


def create_chunk_summary_prompt(base_prompt: str, chunk_text: str, chunk_num: int,
                               total_chunks: int, item_type_name: str, item_number: str,
                               previous_summaries: List[str] = None,
                               unit_title: str = "") -> str:
    """
    Create prompt for summarizing a chunk of text.

    Handles both single-chunk (total_chunks=1) and multi-chunk cases intelligently:
    - Single chunk: Returns standard prompt with no chunk-specific instructions
    - Multi-chunk: Adds chunk context and instructions for proper handling

    Args:
        base_prompt: The base prompt template for summarization
        chunk_text: The text content of this chunk
        chunk_num: Which chunk this is (1-based)
        total_chunks: Total number of chunks for this item
        item_type_name: Type of item being summarized (e.g., 'section', 'article')
        item_number: Number/identifier of the item
        previous_summaries: List of summaries from earlier chunks (for context)
        unit_title: Optional human-readable title for the item (e.g. ECCN heading text)

    Returns:
        str: Formatted prompt for the AI model
    """
    # Build item label: "Section 42" or "Eccn 0A979: Police helmets..."
    item_label = f"{item_type_name.capitalize()} {item_number}"
    if unit_title:
        item_label += f": {unit_title}"

    if total_chunks == 1:
        # Single chunk case - use standard prompt with no modifications
        return base_prompt + f"\n\nHere is the {item_type_name} to summarize:\n\n" + \
               f"{item_label}:\n{chunk_text}"

    # Multi-chunk case - add chunk-specific context and instructions
    chunk_context = f"This is part {chunk_num} of {total_chunks} of {item_label}."
    
    # Add context from previous chunks if available
    context_instruction = ""
    if previous_summaries and len(previous_summaries) > 0:
        context_instruction = f"""
Context from earlier parts of this {item_type_name}:
"""
        for i, summary in enumerate(previous_summaries, 1):
            context_instruction += f"Part {i}: {summary}\n"
        
        context_instruction += f"""
IMPORTANT: The above context helps you understand the overall {item_type_name}. 
Summarize ONLY the text below (part {chunk_num}). Do not summarize the context.

"""
    
    # Add chunk-specific instruction
    chunk_instruction = f"""
You are viewing part {chunk_num} of {total_chunks}. Focus your summary on this specific portion."""
    
    # Combine all parts
    return base_prompt + "\n\n" + chunk_context + "\n" + context_instruction + \
           chunk_instruction + f"\n\nHere is part {chunk_num} of {item_label}:\n\n{chunk_text}"


def synthesize_final_summary(chunk_summaries: List[str], item_type_name: str,
                           item_number: str, client, logfile: str, config: Optional[dict] = None) -> str:
    """
    Synthesize final summary from chunk summaries.

    If only one summary exists, returns it as-is (no synthesis needed).
    If multiple summaries exist, uses AI to create a cohesive final summary.

    Args:
        chunk_summaries: List of summaries from each chunk
        item_type_name: Type of item being summarized
        item_number: Number/identifier of the item
        client: AI client for synthesis query
        logfile: Log file for tracking operations
        config: Optional configuration for fallback model support

    Returns:
        str: Final synthesized summary
    """
    if len(chunk_summaries) == 0:
        return ""

    if len(chunk_summaries) == 1:
        # No synthesis needed - return single summary as-is
        return chunk_summaries[0]

    # Multiple summaries: use AI to synthesize
    synthesis_prompt = f"""Please create a final, cohesive 3-sentence summary of {item_type_name} {item_number}
by synthesizing these summaries of its parts:

"""
    for i, summary in enumerate(chunk_summaries, 1):
        synthesis_prompt += f"Part {i}: {summary}\n\n"

    synthesis_prompt += """
Create a unified summary that captures the overall content without referencing part numbers.
Return only the summary text, no preamble."""

    # Use query_text_with_retry to benefit from retry mechanism for empty responses
    result = query_text_with_retry(client, [], synthesis_prompt, logfile, max_tokens=0, max_retries=3, config=config, task_name='stage3.summary.level1.synthesis')
    return clean_summary_text(result)


def deduplicate_references(all_references: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Remove duplicate references from a list of reference dictionaries.
    
    Duplicates are identified by matching both 'type' and 'value' fields.
    
    Args:
        all_references: List of reference dictionaries with 'type' and 'value' keys
        
    Returns:
        List[Dict]: Deduplicated list of references
    """
    if not all_references:
        return []
    
    seen = set()
    deduplicated = []
    
    for ref in all_references:
        if 'type' in ref and 'value' in ref:
            # Strip emphasis marks from Need_Definition values
            if ref['type'] == 'Need_Definition':
                ref = dict(ref)  # Don't mutate the original
                ref['value'] = strip_emphasis_marks(ref['value'])
            # Create a key for deduplication
            ref_key = (ref['type'], ref['value'])
            if ref_key not in seen:
                seen.add(ref_key)
                deduplicated.append(ref)
    
    return deduplicated
