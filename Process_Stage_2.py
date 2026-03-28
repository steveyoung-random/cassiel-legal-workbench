"""
Document Analysis Stage 2 Processor (Process_Stage_2.py)

This module performs the second stage of legal document analysis, processing JSON files created by
the parse_set modules to add AI-generated definitions and organizational context.

The processor uses AI models to:
- Extract and categorize defined terms with their scopes
- Resolve organizational scopes for definitions

Key Features:
- Definition extraction and scope resolution
- Organizational hierarchy processing
- Context-aware definition management
- Incremental processing with state preservation
- Manifest-based file discovery
- Processing status tracking
"""
# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License

import os
import sys
import json
import re
import argparse
from pathlib import Path
from utils import *
from utils.config import get_config, get_output_directory, get_checkpoint_threshold
from utils.processing_status import (
    init_processing_status,
    update_stage_2_progress,
    is_stage_2_complete
)
from utils.document_handling import iter_operational_items
from utils.manifest_utils import discover_parse_files, parse_filter_string
from stage2 import (
    DefinitionsProcessor,
    find_defined_terms,
    find_defined_terms_scopes,
    evaluate_and_improve_definitions,
    review_high_conflict_terms,
    process_indirect_definitions,
    enhance_resolve_indirect_definitions,
)
from stage2.conflict_resolution import resolve_all_definition_conflicts


def count_operational_items(parsed_content):
    """Count total operational items in the document, including container nodes."""
    count = 0
    for _ in iter_operational_items(parsed_content):
        count += 1
    return count


def discover_files_to_process(input_path, config, filter_str=None):
    """
    Discover files to process based on input path.

    Args:
        input_path: Path to directory, JSON file, or XML/HTML file
        config: Configuration dictionary
        filter_str: Optional filter string for manifest-based discovery

    Returns:
        List of (parse_file_path, output_file_path) tuples
    """
    files_to_process = []

    # If input is a JSON file directly
    if os.path.isfile(input_path) and input_path.endswith('.json'):
        if '_parse_output.json' in input_path:
            parse_file = input_path
            output_file = input_path.replace('_parse_output.json', '_processed.json')
        elif '_processed.json' in input_path:
            output_file = input_path
            parse_file = input_path.replace('_processed.json', '_parse_output.json')
        else:
            print(f'Warning: Unexpected JSON file format: {input_path}')
            return []

        files_to_process.append((parse_file, output_file))
        return files_to_process

    # If input is directory, try manifest-based discovery first
    if os.path.isdir(input_path):
        filter_criteria = parse_filter_string(filter_str) if filter_str else None
        discovered = discover_parse_files(input_path, filter_criteria)

        # If manifest files were found OR a filter was specified, use manifest-based results
        # (even if empty - an empty result with a filter means "no matches", not "fall back")
        if discovered or filter_str:
            for item in discovered:
                files_to_process.append((item['parse_file'], item['processed_file']))
            return files_to_process

        # Fall back: scan for parse output files (direct scan or derived from XML/HTML)
        dir_path = input_path
        seen = set()  # avoid duplicates

        # First: scan for *_parse_output.json directly (handles subdirs where manifest is in parent)
        for file_name in os.listdir(dir_path):
            if file_name.endswith('_parse_output.json'):
                parse_file = os.path.join(dir_path, file_name)
                output_file = parse_file.replace('_parse_output.json', '_processed.json')
                key = (parse_file, output_file)
                if key not in seen:
                    seen.add(key)
                    files_to_process.append(key)

        # Second: derive from XML/HTML (backward compatibility)
        for file_name in os.listdir(dir_path):
            if file_name.endswith('.xml') or file_name.endswith('.html'):
                file_stem = re.sub(r'\.\w+$', '', file_name)
                parse_file = os.path.join(dir_path, file_stem + '_parse_output.json')
                output_file = os.path.join(dir_path, file_stem + '_processed.json')

                if (os.path.exists(parse_file) or os.path.exists(output_file)):
                    key = (parse_file, output_file)
                    if key not in seen:
                        seen.add(key)
                        files_to_process.append(key)

        return files_to_process

    # If input is XML/HTML file (backward compatibility)
    if os.path.isfile(input_path) and (input_path.endswith('.xml') or input_path.endswith('.html')):
        dir_path = os.path.dirname(input_path)
        file_name = os.path.basename(input_path)
        file_stem = re.sub(r'\.\w+$', '', file_name)

        parse_file = os.path.join(dir_path, file_stem + '_parse_output.json')
        output_file = os.path.join(dir_path, file_stem + '_processed.json')

        files_to_process.append((parse_file, output_file))
        return files_to_process

    return []


def process_file_stage_2(parse_file_path, output_file_path, client, logfile, checkpoint_threshold, max_items=10000, config=None):
    """
    Process a single file through Stage 2.

    Args:
        parse_file_path: Path to parse output JSON file
        output_file_path: Path to processed output JSON file
        client: AI client for processing
        logfile: Log file path
        checkpoint_threshold: Number of items between checkpoints
        max_items: Maximum number of items to process in this run (default: 10000)
        config: Configuration dictionary for task-specific model routing (optional)

    Returns:
        True if processing completed, False if more processing needed
    """
    parsed_content = None

    # Load content from processed file if it exists, otherwise from parse file
    if os.path.exists(output_file_path):
        with open(output_file_path, encoding='utf-8') as json_file:
            parsed_content = json.load(json_file)
    elif os.path.exists(parse_file_path):
        with open(parse_file_path, encoding='utf-8') as json_file:
            parsed_content = json.load(json_file)
    else:
        print(f'Error: Neither parse output nor processed file found')
        print(f'  Parse file: {parse_file_path}')
        print(f'  Processed file: {output_file_path}')
        return False

    # Initialize processing status
    init_processing_status(parsed_content)

    # Check if already complete
    if is_stage_2_complete(parsed_content):
        print(f'  Stage 2 already complete, skipping')
        return True

    CheckVersion(parsed_content)

    # Count total items for progress tracking
    total_items = count_operational_items(parsed_content)
    print(f'  Total operational items: {total_items}')

    # Initialize progress (will be updated during processing)
    update_stage_2_progress(parsed_content, total_items, 0)

    # Get the table_of_contents with expanded substantive unit details
    table_of_contents = create_table_of_contents(
        parsed_content,
        parsed_content['document_information']['organization']['content'],
        0, 0, 1
    )

    # Construct processor object
    proc = DefinitionsProcessor(client, logfile, parsed_content, output_file_path, table_of_contents, config)

    # Run processing steps. Catch SystemExit(0) from sub-functions that need more runs
    # (they call exit(0) when they hit their item count limit).
    stage_2_functions_done = True
    try:
        find_defined_terms(proc, max_items)
        process_indirect_definitions(proc)
        find_defined_terms_scopes(proc, max_items)
        enhance_resolve_indirect_definitions(proc, max_items)
        review_high_conflict_terms(proc)
        evaluate_and_improve_definitions(proc, max_items)
    except SystemExit as e:
        if e.code == 0:
            # A sub-function needs more processing — its flush already wrote partial state.
            stage_2_functions_done = False
        else:
            raise
    
    if stage_2_functions_done:
        # Resolve conflicts among document-level definitions
        document_issues_logfile = get_document_issues_logfile(output_file_path)
        total_moved = resolve_all_definition_conflicts(parsed_content, document_issues_logfile, proc)
        if total_moved > 0:
            print(f'  Conflict resolution: Moved {total_moved} definitions')
            proc.dirty = 1

        # Mark Stage 2 complete and write the final state directly to disk.
        # Write unconditionally (not through proc.flush()) to guarantee the file
        # always reflects the completed state regardless of dirty-flag state.
        update_stage_2_progress(parsed_content, total_items, total_items)
        with open(output_file_path, 'w', encoding='utf-8') as outfile:
            outfile.write(json.dumps(parsed_content, indent=4, ensure_ascii=False))
        print(f'  Stage 2 complete. Output written: {output_file_path}')
        return True
    else:
        return False


def main():
    """Main entry point for Stage 2 processing."""
    parser = argparse.ArgumentParser(
        description='Process documents through Stage 2 (definition extraction).'
    )
    parser.add_argument(
        'input_path',
        help='Path to JSON file, directory, or source file (XML/HTML)'
    )
    parser.add_argument(
        '--filter',
        default='',
        help='Filter criteria for manifest-based discovery (e.g., "title=42,chapter=6A")'
    )
    parser.add_argument(
        '--config',
        default='config.json',
        help='Path to configuration file (default: config.json)'
    )
    parser.add_argument(
        '--checkpoint-threshold',
        type=int,
        help='Number of items between checkpoints (overrides config)'
    )
    parser.add_argument(
        '--max-items',
        type=int,
        default=None,
        help='Maximum number of items to process in this run before pausing (default: from config or 10000)'
    )

    args = parser.parse_args()

    # Load configuration
    config = get_config(args.config)

    # Determine checkpoint threshold
    if args.checkpoint_threshold:
        checkpoint_threshold = args.checkpoint_threshold
    else:
        checkpoint_threshold = get_checkpoint_threshold(config)
    
    # Get max items: explicit CLI wins, else config.processing.stage_2_max_items, else 10000
    if args.max_items is not None:
        max_items = args.max_items
    else:
        max_items = config.get('processing', {}).get('stage_2_max_items', 10000)

    # Discover files to process
    files_to_process = discover_files_to_process(args.input_path, config, args.filter)

    if not files_to_process:
        print(f'No files found to process in: {args.input_path}')
        if args.filter:
            print(f'Filter criteria: {args.filter}')
        sys.exit(1)

    print(f'Found {len(files_to_process)} file(s) to process')
    print(f'max_items: {max_items}')

    # Create AI client and logfile
    # Determine directory for logfile
    if os.path.isdir(args.input_path):
        log_dir = args.input_path
    else:
        log_dir = os.path.dirname(args.input_path)

    client = create_ai_client(config=config)
    logfile = GetLogfile(log_dir)

    # Process each file
    all_complete = True
    for parse_file, output_file in files_to_process:
        print(f'\nProcessing: {os.path.basename(parse_file)}')
        try:
            result = process_file_stage_2(parse_file, output_file, client, logfile, checkpoint_threshold, max_items, config)
            if not result:
                all_complete = False
                print(f'  [WARNING] Processing incomplete - more processing needed for this file')
        except Exception as e:
            print(f'  Error processing file: {e}')
            import traceback
            traceback.print_exc()
            all_complete = False
            continue
    
    if all_complete:
        print('\n[SUCCESS] Stage 2 processing complete')
    else:
        print('\n[WARNING] Stage 2 processing incomplete - some files need more processing')
        print('  Please run Stage 2 again to continue processing')
        sys.exit(1)


if __name__ == '__main__':
    main()
    