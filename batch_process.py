"""
Batch Processing CLI for Document Analyzer

This script processes multiple documents through Stage 2 and Stage 3 automatically,
enabling efficient batch processing of entire document collections.

Usage:
    python batch_process.py <directory> [options]

    # Process all parsed files in directory through both stages
    python batch_process.py C:/path/to/documents

    # Process only Stage 2
    python batch_process.py C:/path/to/documents --stages 2

    # Process with custom config
    python batch_process.py C:/path/to/documents --config custom_config.json

    # Force reprocessing of completed files
    python batch_process.py C:/path/to/documents --force

Features:
- Discovers all *_parse_output.json files in directory
- Idempotent processing (skips already-completed stages by default)
- Sequential processing (Stage 2 → Stage 3)
- Error handling (continues processing remaining files on error)
- Progress reporting (shows current file N of M)
- Summary report (successes, failures, skipped)
"""
# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License

import os
import sys
import json
import argparse
import time
from pathlib import Path
from typing import List, Dict, Tuple, Optional

# Import processing utilities
from utils.config import get_config, get_checkpoint_threshold
from utils.processing_status import is_stage_2_complete, is_stage_3_complete
from utils.ai_client import create_ai_client, GetLogfile
from Process_Stage_2 import process_file_stage_2
from Process_Stage_3 import process_file_stage_3
from registry.post_stage3 import extract_and_resolve


class BatchResults:
    """Track batch processing results."""

    def __init__(self):
        self.total_files = 0
        self.stage_2_success = 0
        self.stage_2_skipped = 0
        self.stage_2_failed = 0
        self.stage_3_success = 0
        self.stage_3_skipped = 0
        self.stage_3_failed = 0
        self.errors = []  # List of (file_path, stage, error_message) tuples
        self.start_time = time.time()

    def add_error(self, file_path: str, stage: str, error: str):
        """Record an error."""
        self.errors.append((file_path, stage, error))

    def get_duration(self) -> float:
        """Get elapsed time in seconds."""
        return time.time() - self.start_time

    def print_summary(self):
        """Print summary report."""
        duration = self.get_duration()

        print("\n" + "=" * 80)
        print("BATCH PROCESSING SUMMARY")
        print("=" * 80)
        print(f"Total files discovered: {self.total_files}")
        print(f"Total processing time: {duration:.1f} seconds ({duration/60:.1f} minutes)")
        print()

        print("Stage 2 (Definitions):")
        print(f"  ✓ Completed: {self.stage_2_success}")
        print(f"  ⊘ Skipped:   {self.stage_2_skipped}")
        print(f"  ✗ Failed:    {self.stage_2_failed}")
        print()

        print("Stage 3 (Summaries):")
        print(f"  ✓ Completed: {self.stage_3_success}")
        print(f"  ⊘ Skipped:   {self.stage_3_skipped}")
        print(f"  ✗ Failed:    {self.stage_3_failed}")
        print()

        if self.errors:
            print(f"ERRORS ({len(self.errors)} total):")
            print("-" * 80)
            for file_path, stage, error in self.errors:
                filename = os.path.basename(file_path)
                print(f"  {filename}")
                print(f"    Stage: {stage}")
                print(f"    Error: {error}")
                print()
        else:
            print("No errors encountered!")

        print("=" * 80)


def discover_parsed_files(directory: str) -> List[str]:
    """
    Discover all *_parse_output.json files in directory.

    Args:
        directory: Path to directory to scan

    Returns:
        List of paths to parse output JSON files
    """
    parsed_files = []

    if not os.path.isdir(directory):
        print(f"Error: '{directory}' is not a directory")
        return []

    # Look for files ending with _parse_output.json
    for filename in os.listdir(directory):
        if filename.endswith('_parse_output.json'):
            file_path = os.path.join(directory, filename)
            parsed_files.append(file_path)

    return sorted(parsed_files)


def check_file_status(parsed_file: str) -> Tuple[bool, bool, str]:
    """
    Check which stages are complete for a file.

    Args:
        parsed_file: Path to *_parse_output.json file

    Returns:
        Tuple of (stage_2_complete, stage_3_complete, processed_file_path)
    """
    # Determine processed file path
    processed_file = parsed_file.replace('_parse_output.json', '_processed.json')

    # Check if processed file exists
    if not os.path.exists(processed_file):
        return False, False, processed_file

    # Load processed file to check status
    try:
        with open(processed_file, 'r', encoding='utf-8') as f:
            content = json.load(f)

        stage_2_done = is_stage_2_complete(content)
        stage_3_done = is_stage_3_complete(content)

        return stage_2_done, stage_3_done, processed_file

    except Exception as e:
        print(f"Warning: Could not read processed file {processed_file}: {e}")
        return False, False, processed_file


def process_single_file(
    parsed_file: str,
    processed_file: str,
    stages: List[int],
    config: dict,
    checkpoint_threshold: int,
    force: bool,
    results: BatchResults
) -> None:
    """
    Process a single file through requested stages.

    Args:
        parsed_file: Path to *_parse_output.json file
        processed_file: Path to *_processed.json file
        stages: List of stages to process (e.g., [2, 3])
        config: Configuration dictionary
        checkpoint_threshold: Checkpoint threshold setting
        force: If True, reprocess even if already complete
        results: BatchResults object to track outcomes
    """
    filename = os.path.basename(parsed_file)

    # Check current status
    stage_2_done, stage_3_done, _ = check_file_status(parsed_file)

    # Process Stage 2 if requested
    if 2 in stages:
        if stage_2_done and not force:
            print(f"  Stage 2: Already complete (skipping)")
            results.stage_2_skipped += 1
        else:
            try:
                print(f"  Stage 2: Processing...")

                # Create AI client and logfile
                client = create_ai_client(config=config)
                logfile = GetLogfile(os.path.dirname(parsed_file))

                # Process Stage 2
                completed = process_file_stage_2(
                    parsed_file,
                    processed_file,
                    client,
                    logfile,
                    checkpoint_threshold,
                    max_items=10000,
                    config=config
                )

                if completed:
                    print(f"  Stage 2: ✓ Complete")
                    results.stage_2_success += 1
                    stage_2_done = True
                else:
                    print(f"  Stage 2: Incomplete (may need more processing)")
                    results.stage_2_success += 1  # Still count as success
                    stage_2_done = True

            except Exception as e:
                error_msg = str(e)
                print(f"  Stage 2: ✗ Failed - {error_msg}")
                results.stage_2_failed += 1
                results.add_error(filename, "Stage 2", error_msg)
                # Don't proceed to Stage 3 if Stage 2 failed
                return

    # Process Stage 3 if requested
    if 3 in stages:
        # Check if Stage 2 is complete (prerequisite for Stage 3)
        if not stage_2_done:
            print(f"  Stage 3: Skipping (Stage 2 not complete)")
            results.stage_3_skipped += 1
            return

        if stage_3_done and not force:
            print(f"  Stage 3: Already complete (skipping)")
            results.stage_3_skipped += 1
        else:
            try:
                print(f"  Stage 3: Processing...")

                # Create task-specific AI clients for flexible model system
                from utils.config import create_client_for_task
                clients = {
                    'organizational': create_client_for_task(config, 'stage3.summary.organizational'),
                    'level1': create_client_for_task(config, 'stage3.summary.level1'),
                    'level1_with_references': create_client_for_task(config, 'stage3.summary.level1_with_references'),
                    'level2': create_client_for_task(config, 'stage3.summary.level2'),
                    'topic_statement': create_client_for_task(config, 'stage3.summary.topic_statement')
                }

                # For backward compatibility, keep a default client
                client = clients['level1']
                logfile = GetLogfile(os.path.dirname(parsed_file))

                # Process Stage 3 with clients and config for fallback model support
                completed = process_file_stage_3(
                    parsed_file,
                    processed_file,
                    client,
                    logfile,
                    checkpoint_threshold,
                    max_items=300,
                    clients=clients,
                    config=config
                )

                if completed:
                    print(f"  Stage 3: ✓ Complete")
                    results.stage_3_success += 1
                else:
                    print(f"  Stage 3: Incomplete (may need more processing)")
                    results.stage_3_success += 1  # Still count as success

                # Post-Stage-3 hook: update cross-reference registry.
                try:
                    reg_stats = extract_and_resolve(processed_file, config=config)
                    if reg_stats['extracted'] or reg_stats['resolved']:
                        parts = []
                        if reg_stats['extracted']:
                            parts.append(f"{reg_stats['extracted']} refs extracted")
                        if reg_stats['resolved']:
                            parts.append(f"{reg_stats['resolved']} resolved")
                        print(f"  Registry: {', '.join(parts)}")
                except Exception as e:
                    print(f"  Registry: warning — {e}", file=sys.stderr)

            except Exception as e:
                error_msg = str(e)
                print(f"  Stage 3: ✗ Failed - {error_msg}")
                results.stage_3_failed += 1
                results.add_error(filename, "Stage 3", error_msg)


def main():
    """Main entry point for batch processing."""
    parser = argparse.ArgumentParser(
        description='Batch process documents through Stage 2 and/or Stage 3.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process all files through both stages
  python batch_process.py C:/path/to/documents

  # Process only Stage 2
  python batch_process.py C:/path/to/documents --stages 2

  # Force reprocessing of completed files
  python batch_process.py C:/path/to/documents --force

  # Use custom config
  python batch_process.py C:/path/to/documents --config custom_config.json
        """
    )

    parser.add_argument(
        'directory',
        help='Directory containing *_parse_output.json files to process'
    )

    parser.add_argument(
        '--stages',
        default='2,3',
        help='Comma-separated list of stages to process (default: "2,3")'
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
        '--force',
        action='store_true',
        help='Reprocess files even if stages are already complete'
    )

    parser.add_argument(
        '--fail-fast',
        action='store_true',
        help='Stop processing on first error instead of continuing'
    )

    args = parser.parse_args()

    # Validate directory
    if not os.path.isdir(args.directory):
        print(f"Error: Directory not found: {args.directory}")
        sys.exit(1)

    # Parse stages
    try:
        stages = [int(s.strip()) for s in args.stages.split(',')]
        if not all(s in [2, 3] for s in stages):
            print("Error: Stages must be 2 and/or 3")
            sys.exit(1)
    except ValueError:
        print(f"Error: Invalid stages format: {args.stages}")
        print("Expected comma-separated integers (e.g., '2,3' or '2' or '3')")
        sys.exit(1)

    # Load configuration
    try:
        config = get_config(args.config)
    except Exception as e:
        print(f"Error loading config: {e}")
        sys.exit(1)

    # Determine checkpoint threshold
    if args.checkpoint_threshold:
        checkpoint_threshold = args.checkpoint_threshold
    else:
        checkpoint_threshold = get_checkpoint_threshold(config)

    # Discover files
    print(f"Scanning directory: {args.directory}")
    parsed_files = discover_parsed_files(args.directory)

    if not parsed_files:
        print("No *_parse_output.json files found in directory")
        sys.exit(0)

    print(f"Found {len(parsed_files)} parse output files")
    print(f"Stages to process: {', '.join(f'Stage {s}' for s in stages)}")
    if args.force:
        print("Force mode: Will reprocess completed files")
    print()

    # Initialize results tracking
    results = BatchResults()
    results.total_files = len(parsed_files)

    # Process each file
    for i, parsed_file in enumerate(parsed_files, 1):
        filename = os.path.basename(parsed_file)
        processed_file = parsed_file.replace('_parse_output.json', '_processed.json')

        print(f"[{i}/{len(parsed_files)}] {filename}")

        try:
            process_single_file(
                parsed_file,
                processed_file,
                stages,
                config,
                checkpoint_threshold,
                args.force,
                results
            )
        except Exception as e:
            error_msg = f"Unexpected error: {str(e)}"
            print(f"  ✗ {error_msg}")
            results.add_error(filename, "Processing", error_msg)

            if args.fail_fast:
                print("\nStopping due to --fail-fast")
                break

        print()  # Blank line between files

    # Print summary report
    results.print_summary()

    # Exit with error code if there were failures
    if results.stage_2_failed > 0 or results.stage_3_failed > 0:
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == '__main__':
    main()
