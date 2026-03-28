"""
Job execution functions for document processing tasks.

This module contains the actual job execution logic, refactored from the
Process_Stage_2.py and Process_Stage_3.py scripts to work with the job queue system.

The key difference is that these functions:
1. Take a JobQueue instance and job_id as parameters
2. Call queue.update_progress() during execution
3. Return result dicts instead of printing to console
4. Don't use argparse (parameters come from job params)
"""
# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License

import json
import os
import re
import sys
import traceback
import threading
import time

# Add parent directory to path to import processing modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.ai_client import create_ai_client, GetLogfile
from utils.config import get_config
from utils.processing_status import (
    init_processing_status,
    update_stage_2_progress,
    update_stage_3_progress,
    count_stage_3_progress,
    count_stage_3_progress,
    is_stage_2_complete,
    is_stage_3_complete
)
from utils.error_handling import CheckVersion
from utils.document_handling import create_table_of_contents
from utils.document_issues import get_document_issues_logfile
from parsers.registry import get_parser, load_parsers_from_config


class ProgressMonitor:
    """
    Background thread that monitors processed.json file and updates job queue.

    This allows us to report progress during long-running operations without
    modifying the core processing logic.

    Supports both Stage 2 and Stage 3 progress monitoring.
    """

    def __init__(self, queue, job_id, file_path, stage, poll_interval=5):
        """
        Initialize progress monitor.

        Args:
            queue: JobQueue instance
            job_id: Job ID to update
            file_path: Path to processed.json file to monitor
            stage: Stage to monitor ('stage_2' or 'stage_3')
            poll_interval: Seconds between polling (default: 5)
        """
        self.queue = queue
        self.job_id = job_id
        self.file_path = file_path
        self.stage = stage
        self.poll_interval = poll_interval
        self.stop_flag = threading.Event()
        self.thread = None

    def start(self):
        """Start monitoring in background thread."""
        self.thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.thread.start()

    def stop(self):
        """Stop monitoring thread."""
        self.stop_flag.set()
        if self.thread:
            self.thread.join(timeout=10)

    def _monitor_loop(self):
        """Background thread loop that polls file and updates progress."""
        last_items_done = 0
        last_phase = 'initialization'

        while not self.stop_flag.is_set():
            try:
                # Read processed.json file
                if os.path.exists(self.file_path):
                    with open(self.file_path, 'r', encoding='utf-8') as f:
                        parsed_content = json.load(f)

                    # Extract processing_status
                    proc_status = parsed_content.get('document_information', {}).get('processing_status', {})

                    if proc_status:
                        if self.stage == 'stage_2':
                            # Monitor Stage 2 progress
                            stage_2_progress = proc_status.get('stage_2_progress', {})
                            total_items = stage_2_progress.get('total_items', 0)
                            processed_items = stage_2_progress.get('processed_items', 0)
                            current_phase = stage_2_progress.get('current_phase', 'processing')

                            items_done = processed_items
                            phase = current_phase

                            # Current item indicator
                            if total_items > 0:
                                current_item = f'{processed_items}/{total_items}'
                            else:
                                current_item = 'processing'

                            # Only update if progress changed
                            if items_done != last_items_done or phase != last_phase:
                                self.queue.update_progress(
                                    self.job_id,
                                    phase,
                                    current_item,
                                    items_done,
                                    total_items
                                )
                                last_items_done = items_done
                                last_phase = phase

                        elif self.stage == 'stage_3':
                            # Monitor Stage 3 progress
                            stage_3_progress = proc_status.get('stage_3_progress', {})
                            operational = stage_3_progress.get('operational', {})
                            organizational = stage_3_progress.get('organizational', {})

                            # Calculate total progress
                            op_total = operational.get('total', 0)
                            op_summary_1 = operational.get('summary_1', 0)
                            op_summary_2 = operational.get('summary_2', 0)
                            org_total = organizational.get('total', 0)
                            org_summary_2 = organizational.get('summary_2', 0)

                            # Determine current phase and calculate items_done/total_items based on phase
                            if op_summary_1 < op_total:
                                phase = 'summary_1'
                                current_item = f'op_{op_summary_1}/{op_total}'
                                # During summary_1, count summary_1 items done out of operational total
                                items_done = op_summary_1
                                total_items = op_total
                            elif op_summary_2 < op_total:
                                phase = 'summary_2'
                                current_item = f'op_{op_summary_2}/{op_total}'
                                # During summary_2, count summary_2 items done out of operational total
                                items_done = op_summary_2
                                total_items = op_total
                            elif org_summary_2 < org_total:
                                phase = 'org_summary_2'
                                current_item = f'org_{org_summary_2}/{org_total}'
                                # During org_summary_2, count both op_summary_2 and org_summary_2
                                # Total is op_total + org_total, items_done is op_summary_2 + org_summary_2
                                items_done = op_summary_2 + org_summary_2
                                total_items = op_total + org_total
                            else:
                                phase = 'finalizing'
                                current_item = 'complete'
                                # All done
                                items_done = op_summary_2 + org_summary_2
                                total_items = op_total + org_total

                            # Only update if progress changed
                            if items_done != last_items_done or phase != last_phase:
                                self.queue.update_progress(
                                    self.job_id,
                                    phase,
                                    current_item,
                                    items_done,
                                    total_items
                                )
                                last_items_done = items_done
                                last_phase = phase

            except Exception as e:
                # Don't crash on errors, just log and continue
                try:
                    self.queue.log_message(self.job_id, 'WARNING', f'Progress monitor error: {e}')
                except:
                    pass  # If even logging fails, just continue

            # Wait before next poll
            self.stop_flag.wait(self.poll_interval)


def process_stage_3_job(queue, job):
    """
    Execute Stage 3 processing for a job.

    This function wraps the logic from Process_Stage_3.py, adapted to work
    with the job queue system. It processes a document through all 4 phases
    of Stage 3 and updates progress via the job queue.

    Args:
        queue: JobQueue instance for progress updates
        job: Job dict with keys: id, file_path, params

    Returns:
        Result dict with keys:
            - operational_items: Number of operational items processed
            - organizational_units: Number of organizational units processed
            - phases_completed: List of phase names completed

    Raises:
        Exception: Any processing errors (will be caught by worker and logged)
    """
    job_id = job['id']
    file_path = job['file_path']
    params = job['params']

    # Extract parameters
    checkpoint_threshold = params.get('checkpoint_threshold', 30)
    max_items = params.get('max_items', 300)
    config_path = params.get('config') or 'config.json'  # Handle None explicitly

    # Load configuration
    config = get_config(config_path)

    # Determine output file path
    # If file_path ends with _parse_output.json, replace with _processed.json
    # If file_path ends with _processed.json, use as-is
    if file_path.endswith('_parse_output.json'):
        output_file_path = file_path.replace('_parse_output.json', '_processed.json')
    elif file_path.endswith('_processed.json'):
        output_file_path = file_path
    else:
        raise ValueError(f"Unexpected file path format: {file_path}")

    # Stage 3 MUST load from processed file only (Stage 2 must be complete first)
    if not os.path.exists(output_file_path):
        raise FileNotFoundError(
            f'Processed file not found (Stage 2 must be run first): {output_file_path}'
        )

    # Load parsed content
    with open(output_file_path, encoding='utf-8') as json_file:
        parsed_content = json.load(json_file)

    # Initialize processing status
    init_processing_status(parsed_content)

    # Check if Stage 2 is complete (prerequisite for Stage 3)
    if not is_stage_2_complete(parsed_content):
        raise ValueError(f'Stage 2 not complete (prerequisite for Stage 3): {output_file_path}')

    # Check if already complete
    if is_stage_3_complete(parsed_content):
        queue.log_message(job_id, 'INFO', 'Stage 3 already complete, skipping')
        return {
            'operational_items': 0,
            'organizational_units': 0,
            'phases_completed': [],
            'already_complete': True
        }

    CheckVersion(parsed_content)

    # Count progress for initial status
    operational_counts, organizational_counts = count_stage_3_progress(parsed_content)

    # Log initial status
    queue.log_message(
        job_id, 'INFO',
        f'Stage 3 started: {operational_counts["summary_1"]}/{operational_counts["total"]} ops with summary_1, '
        f'{operational_counts["summary_2"]}/{operational_counts["total"]} ops with summary_2, '
        f'{organizational_counts["summary_2"]}/{organizational_counts["total"]} org units with summary_2'
    )

    # Update initial progress
    update_stage_3_progress(parsed_content, operational_counts, organizational_counts)

    # Report initial progress to queue
    total_items = operational_counts['total'] + organizational_counts['total']
    items_done = operational_counts['summary_2'] + organizational_counts['summary_2']
    queue.update_progress(job_id, 'initialization', 'starting', items_done, total_items)

    # Start progress monitoring in background thread
    # This will periodically read the processed.json file and update the job queue
    progress_monitor = ProgressMonitor(queue, job_id, output_file_path, 'stage_3', poll_interval=5)
    progress_monitor.start()

    try:
        # Create AI client and logfile
        log_dir = os.path.dirname(output_file_path)
        logfile = GetLogfile(log_dir)

        # Import Process_Stage_3 functions
        # We need to import the actual processing functions from Process_Stage_3.py
        # For now, I'll import the whole module
        import Process_Stage_3
        from utils.config import create_client_for_task

        # Create task-specific AI clients for flexible model system
        clients = {
            'organizational': create_client_for_task(config, 'stage3.summary.organizational'),
            'level1': create_client_for_task(config, 'stage3.summary.level1'),
            'level1_with_references': create_client_for_task(config, 'stage3.summary.level1_with_references'),
            'level2': create_client_for_task(config, 'stage3.summary.level2'),
            'topic_statement': create_client_for_task(config, 'stage3.summary.topic_statement')
        }

        # For backward compatibility, keep a default client
        client = clients['level1']

        # Create SummaryProcessor with clients and config for fallback model support
        proc = Process_Stage_3.SummaryProcessor(client, logfile, parsed_content, output_file_path, clients=clients, config=config)
        proc.flush()

        phases_completed = []

        # Phase 1: Organizational Level 1 Summaries
        try:
            queue.update_progress(job_id, 'org_summary_1', 'processing', items_done, total_items)
            queue.log_message(job_id, 'INFO', 'Phase 1: Organizational Level 1 Summaries')
            Process_Stage_3.top_organization_summaries(proc, 1)
            phases_completed.append('org_summary_1')
        except Exception as e:
            queue.log_message(job_id, 'ERROR', f'Phase 1 failed: {e}')
            raise

        # Phase 2: Operational Level 1 Summaries
        try:
            queue.log_message(job_id, 'INFO', f'Phase 2: Operational Level 1 Summaries (up to {max_items} items)')

            # We need to modify level_1_summaries to report progress via queue
            # For now, we'll call it directly and update progress after
            level_1_complete = process_level_1_with_progress(proc, max_items, checkpoint_threshold, queue, job_id, total_items)

            # Update counts after level 1
            operational_counts, organizational_counts = count_stage_3_progress(parsed_content)
            update_stage_3_progress(parsed_content, operational_counts, organizational_counts)
            proc.flush()

            items_done = operational_counts['summary_2'] + organizational_counts['summary_2']
            queue.update_progress(job_id, 'summary_1_complete', 'completed', items_done, total_items)

            if not level_1_complete:
                queue.log_message(job_id, 'WARNING', 'Level 1 summaries not complete - max_items limit reached')
                # Mark job as partially complete but needing more work
                return {
                    'operational_items': operational_counts['summary_1'],
                    'organizational_units': organizational_counts['total'],
                    'phases_completed': phases_completed,
                    'incomplete': True,
                    'reason': 'max_items limit reached during level_1_summaries'
                }

            phases_completed.append('op_summary_1')
        except Exception as e:
            # Update progress before failing
            operational_counts, organizational_counts = count_stage_3_progress(parsed_content)
            update_stage_3_progress(parsed_content, operational_counts, organizational_counts)
            proc.flush()
            queue.log_message(job_id, 'ERROR', f'Phase 2 failed: {e}')
            raise

        # Phase 3: Organizational Level 2 Summaries (preliminary - before operational summaries)
        try:
            queue.update_progress(job_id, 'org_summary_2', 'processing', items_done, total_items)
            queue.log_message(job_id, 'INFO', 'Phase 3: Organizational Level 2 Summaries (preliminary)')
            Process_Stage_3.top_organization_summaries(proc, 2)
            # Update counts after Phase 3
            operational_counts, organizational_counts = count_stage_3_progress(parsed_content)
            update_stage_3_progress(parsed_content, operational_counts, organizational_counts)
            proc.dirty = 1  # update_stage_3_progress modifies parsed_content, so mark as dirty
            proc.flush()
            phases_completed.append('org_summary_2_preliminary')
        except Exception as e:
            queue.log_message(job_id, 'ERROR', f'Phase 3 failed: {e}')
            raise

        # Phase 4: Operational Level 2 Summaries
        try:
            queue.log_message(job_id, 'INFO', f'Phase 4: Operational Level 2 Summaries (up to {max_items} items)')

            # Call level_2_summaries with progress reporting
            level_2_complete = process_level_2_with_progress(proc, max_items, checkpoint_threshold, queue, job_id, total_items)

            # Update counts after level 2
            operational_counts, organizational_counts = count_stage_3_progress(parsed_content)
            update_stage_3_progress(parsed_content, operational_counts, organizational_counts)
            proc.flush()

            items_done = operational_counts['summary_2'] + organizational_counts['summary_2']

            if not level_2_complete:
                queue.log_message(job_id, 'WARNING', 'Level 2 summaries not complete - max_items limit reached')
                # Update organizational summaries before returning (based on completed operational items)
                try:
                    queue.log_message(job_id, 'INFO', 'Updating organizational summaries before pausing')
                    Process_Stage_3.top_organization_summaries(proc, 2)
                    # Re-count after updating org summaries
                    operational_counts, organizational_counts = count_stage_3_progress(parsed_content)
                    update_stage_3_progress(parsed_content, operational_counts, organizational_counts)
                    proc.flush()
                except Exception as e:
                    queue.log_message(job_id, 'WARNING', f'Failed to update org summaries: {e}')

                return {
                    'operational_items': operational_counts['summary_2'],
                    'organizational_units': organizational_counts['summary_2'],
                    'phases_completed': phases_completed,
                    'incomplete': True,
                    'reason': 'max_items limit reached during level_2_summaries'
                }

            phases_completed.append('op_summary_2')
        except Exception as e:
            # Update progress before failing
            operational_counts, organizational_counts = count_stage_3_progress(parsed_content)
            update_stage_3_progress(parsed_content, operational_counts, organizational_counts)
            proc.flush()
            queue.log_message(job_id, 'ERROR', f'Phase 4 failed: {e}')
            raise

        # Phase 5: Final organizational summaries (after all operational summaries are done)
        try:
            # CRITICAL: Reload from file to ensure we're working with saved data, not stale in-memory data
            # This ensures we see the actual state of the file, not in-memory modifications that weren't flushed
            with open(output_file_path, 'r', encoding='utf-8') as f:
                parsed_content = json.load(f)
            proc.parsed_content = parsed_content  # Update the processor's reference
            
            # Get counts before Phase 5 (from file, not memory)
            operational_counts_before, organizational_counts_before = count_stage_3_progress(parsed_content)
            queue.log_message(
                job_id, 'INFO',
                f'Phase 5: Final Organizational Summaries - Before: {organizational_counts_before["summary_2"]}/{organizational_counts_before["total"]} org units with summary_2'
            )
            
            items_done = operational_counts_before['summary_2'] + organizational_counts_before['summary_2']
            queue.update_progress(job_id, 'org_summary_2_final', 'processing', items_done, total_items)
            
            # Generate final organizational summaries
            Process_Stage_3.top_organization_summaries(proc, 2)
            
            # CRITICAL: Flush immediately after generating summaries
            proc.flush()
            
            # Reload from file to verify summaries were actually saved
            with open(output_file_path, 'r', encoding='utf-8') as f:
                parsed_content_after = json.load(f)
            
            # Update counts after Phase 5 (from file, not memory)
            operational_counts_after, organizational_counts_after = count_stage_3_progress(parsed_content_after)
            update_stage_3_progress(parsed_content_after, operational_counts_after, organizational_counts_after)
            proc.parsed_content = parsed_content_after  # Update processor reference
            proc.dirty = 1  # update_stage_3_progress modifies parsed_content, so mark as dirty
            proc.flush()  # Flush the updated status
            
            # Log the results
            org_added = organizational_counts_after['summary_2'] - organizational_counts_before['summary_2']
            queue.log_message(
                job_id, 'INFO',
                f'Phase 5: After - {organizational_counts_after["summary_2"]}/{organizational_counts_after["total"]} org units with summary_2 (added {org_added})'
            )
            
            # If no summaries were added, log a warning
            if org_added == 0 and organizational_counts_after['summary_2'] < organizational_counts_after['total']:
                # Find the most recent log file for reference
                log_dir = os.path.dirname(output_file_path)
                logfile_path = None
                import glob
                log_files = glob.glob(os.path.join(log_dir, 'log*.json'))
                if log_files:
                    log_files.sort(key=os.path.getmtime, reverse=True)
                    logfile_path = log_files[0]
                
                queue.log_message(
                    job_id, 'WARNING',
                    f'Phase 5: No organizational summaries were added ({organizational_counts_after["summary_2"]}/{organizational_counts_after["total"]} have summary_2). '
                    f'This may indicate that operational summaries are incomplete or organizational structure is missing required markers (begin_/stop_ tags). '
                    f'Check log file for details: {logfile_path if logfile_path else "log file not found"}.'
                )
            
            items_done = operational_counts_after['summary_2'] + organizational_counts_after['summary_2']
            queue.update_progress(job_id, 'org_summary_2_final', 'completed', items_done, total_items)
            phases_completed.append('org_summary_2_final')
            
            # Update parsed_content reference for final checks
            parsed_content = parsed_content_after
        except Exception as e:
            queue.log_message(job_id, 'ERROR', f'Phase 5 failed: {e}')
            import traceback
            queue.log_message(job_id, 'ERROR', f'Phase 5 traceback: {traceback.format_exc()}')
            # Still update counts even if Phase 5 fails
            operational_counts, organizational_counts = count_stage_3_progress(parsed_content)
            update_stage_3_progress(parsed_content, operational_counts, organizational_counts)
            proc.flush()
            raise

        # Phase 6: Topic Statement Generation (after all summaries complete)
        try:
            # Reload from file to ensure we have latest summaries
            with open(output_file_path, 'r', encoding='utf-8') as f:
                parsed_content = json.load(f)
            proc.parsed_content = parsed_content
            
            # Check if topic statement already exists
            existing_topic = parsed_content.get('document_information', {}).get('topic_statement', '')
            if existing_topic and existing_topic.strip():
                queue.log_message(job_id, 'INFO', f'Phase 6: Topic statement already exists, skipping generation')
            else:
                queue.log_message(job_id, 'INFO', 'Phase 6: Topic Statement Generation')
                items_done = operational_counts_after['summary_2'] + organizational_counts_after['summary_2']
                queue.update_progress(job_id, 'topic_statement', 'processing', items_done, total_items)
                
                # Generate topic statement
                from Process_Stage_3 import generate_topic_statement
                topic_statement = generate_topic_statement(proc)
                
                if topic_statement:
                    # Store in document_information
                    if 'document_information' not in parsed_content:
                        parsed_content['document_information'] = {}
                    parsed_content['document_information']['topic_statement'] = topic_statement
                    proc.parsed_content = parsed_content
                    proc.dirty = 1
                    
                    # Flush immediately
                    proc.flush()
                    
                    preview = topic_statement[:100] + '...' if len(topic_statement) > 100 else topic_statement
                    queue.log_message(job_id, 'INFO', f'Phase 6: Topic statement generated ({len(topic_statement)} chars): {preview}')
                    queue.update_progress(job_id, 'topic_statement', 'completed', items_done, total_items)
                    phases_completed.append('topic_statement')
                else:
                    queue.log_message(job_id, 'WARNING', 'Phase 6: Topic statement generation returned empty result')
                    
        except Exception as e:
            # Don't fail job if topic statement generation fails - it's optional
            queue.log_message(job_id, 'WARNING', f'Phase 6: Topic statement generation failed: {e}')
            import traceback
            queue.log_message(job_id, 'WARNING', f'Phase 6 traceback: {traceback.format_exc()}')

        # Final counts (re-count to ensure accuracy)
        operational_counts, organizational_counts = count_stage_3_progress(parsed_content)
        update_stage_3_progress(parsed_content, operational_counts, organizational_counts)
        proc.flush()

        # Check if fully complete
        if is_stage_3_complete(parsed_content):
            queue.log_message(job_id, 'INFO', 'Stage 3 fully complete')

            # Post-Stage-3 hook: update cross-reference registry.
            try:
                from registry.post_stage3 import extract_and_resolve
                reg_stats = extract_and_resolve(output_file_path, config=config)
                if reg_stats['extracted'] or reg_stats['resolved']:
                    queue.log_message(
                        job_id, 'INFO',
                        f"Registry updated: {reg_stats['extracted']} refs extracted, "
                        f"{reg_stats['resolved']} resolved"
                    )
            except Exception as e:
                queue.log_message(job_id, 'WARNING', f'Registry update failed (non-fatal): {e}')

            return {
                'operational_items': operational_counts['summary_2'],
                'organizational_units': organizational_counts['summary_2'],
                'phases_completed': phases_completed,
                'complete': True
            }
        else:
            queue.log_message(job_id, 'WARNING', 'Stage 3 processing complete but not all items have summaries')
            return {
                'operational_items': operational_counts['summary_2'],
                'organizational_units': organizational_counts['summary_2'],
                'phases_completed': phases_completed,
                'incomplete': True,
                'reason': 'Not all items have summaries yet'
            }

    finally:
        # Stop progress monitor when done (success or failure)
        progress_monitor.stop()


def process_level_1_with_progress(proc, max_items, checkpoint_threshold, queue, job_id, total_items):
    """
    Wrapper for level_1_summaries that reports progress to queue.

    This function monitors the processing and reports progress updates
    to the job queue every few items.
    """
    import Process_Stage_3
    from utils.processing_status import count_stage_3_progress

    # Track progress before
    operational_counts_before, _ = count_stage_3_progress(proc.parsed_content)
    summary_1_before = operational_counts_before['summary_1']

    # Call the actual function
    result = Process_Stage_3.level_1_summaries(proc, max_items, checkpoint_threshold)

    # Update progress after
    operational_counts_after, organizational_counts = count_stage_3_progress(proc.parsed_content)
    summary_1_after = operational_counts_after['summary_1']
    items_processed = summary_1_after - summary_1_before

    items_done = operational_counts_after['summary_2'] + organizational_counts['summary_2']
    queue.update_progress(
        job_id,
        'summary_1',
        f'processed_{items_processed}',
        items_done,
        total_items
    )

    return result


def process_level_2_with_progress(proc, max_items, checkpoint_threshold, queue, job_id, total_items):
    """
    Wrapper for level_2_summaries that reports progress to queue.

    This function monitors the processing and reports progress updates
    to the job queue every few items.
    """
    import Process_Stage_3
    from utils.processing_status import count_stage_3_progress

    # Track progress before
    operational_counts_before, organizational_counts_before = count_stage_3_progress(proc.parsed_content)
    summary_2_before = operational_counts_before['summary_2']

    # Call the actual function
    result = Process_Stage_3.level_2_summaries(proc, max_items, checkpoint_threshold)

    # Update progress after
    operational_counts_after, organizational_counts_after = count_stage_3_progress(proc.parsed_content)
    summary_2_after = operational_counts_after['summary_2']
    items_processed = summary_2_after - summary_2_before

    items_done = operational_counts_after['summary_2'] + organizational_counts_after['summary_2']
    queue.update_progress(
        job_id,
        'summary_2',
        f'processed_{items_processed}',
        items_done,
        total_items
    )

    return result


def process_stage_2_phase_with_progress(proc, phase_func, phase_name, phase_num, total_phases, *args):
    """
    Wrapper for Stage 2 phase functions that updates progress and flushes to disk.

    This function wraps a Stage 2 phase function to:
    1. Update the current_phase in the file's processing_status
    2. Call the phase function
    3. Flush changes to disk so ProgressMonitor can read them

    Args:
        proc: DefinitionsProcessor instance
        phase_func: The phase function to call
        phase_name: Name of the phase (e.g., 'extracting_definitions')
        phase_num: Current phase number (1-6)
        total_phases: Total number of phases (6)
        *args: Arguments to pass to phase_func

    Returns:
        Result from phase_func
    """
    from utils.processing_status import update_stage_2_progress

    # Get total items count
    proc_status = proc.parsed_content.get('document_information', {}).get('processing_status', {})
    total_items = proc_status.get('stage_2_progress', {}).get('total_items', 0)

    # Calculate progress as items completed (approximate based on phase)
    # We estimate each phase processes roughly equal portions
    items_done = int(total_items * (phase_num - 1) / total_phases)

    # Update progress to show we're starting this phase
    update_stage_2_progress(proc.parsed_content, total_items, items_done, phase_name)
    proc.dirty = 1
    proc.flush()

    # Call the actual phase function
    result = phase_func(*args)

    # After phase completes, update progress again
    items_done = int(total_items * phase_num / total_phases)
    update_stage_2_progress(proc.parsed_content, total_items, items_done, phase_name)
    proc.dirty = 1
    proc.flush()

    return result


def process_parse_job(queue, job):
    """
    Execute parsing job using appropriate parser adapter.

    This function uses the parser registry to get the correct parser adapter
    and execute parsing with progress updates.

    Args:
        queue: JobQueue instance for progress updates
        job: Job dict with keys: id, file_path, params, parser_type

    Returns:
        Result dict with keys:
            - files_created: List of file paths created
            - warnings: List of warning messages

    Raises:
        ValueError: If parser_type is missing or unknown
        Exception: Any parsing errors (will be caught by worker and logged)
    """
    job_id = job['id']
    file_path = job['file_path']
    parser_type = job.get('parser_type')
    params = job.get('params', {})

    # Validate parser_type
    if not parser_type:
        raise ValueError("Parse job missing parser_type")

    # Load parsers from config (ensure registry is populated)
    config = get_config()
    load_parsers_from_config(config)

    # Get parser adapter from registry
    adapter = get_parser(parser_type)
    if not adapter:
        raise ValueError(f"Unknown parser type: {parser_type}")

    # Parse params if it's a string (JSON)
    if isinstance(params, str):
        params = json.loads(params) if params else {}
    elif params is None:
        params = {}

    # Log job start
    queue.log_message(job_id, 'INFO', f'Starting parse job with {parser_type} parser')
    queue.log_message(job_id, 'INFO', f'File: {os.path.basename(file_path)}')

    # Update progress: Starting
    queue.update_progress(
        job_id,
        phase='parsing',
        current_item=os.path.basename(file_path),
        items_done=0,
        total_items=1
    )

    try:
        # Execute parsing
        result = adapter.parse_file(
            file_path=file_path,
            config=config,
            params=params
        )

        if not result.success:
            error_msg = result.error_message or "Parse failed with unknown error"
            raise Exception(f"Parse failed: {error_msg}")

        # Log warnings if any
        for warning in result.warnings:
            queue.log_message(job_id, 'WARNING', f'Parse warning: {warning}')

        # Update progress: Complete
        queue.update_progress(
            job_id,
            phase='parsing',
            current_item=os.path.basename(file_path),
            items_done=1,
            total_items=1
        )

        # Log completion
        files_count = len(result.files_created) if result.files_created else 0
        queue.log_message(job_id, 'INFO', f'Parse completed successfully ({files_count} files created)')

        return {
            'files_created': result.files_created or [],
            'warnings': result.warnings,
            'parser_type': parser_type
        }

    except Exception as e:
        # Log error and re-raise
        queue.log_message(job_id, 'ERROR', f'Parse job failed: {str(e)}')
        raise


def process_stage_2_job(queue, job):
    """
    Execute Stage 2 processing for a job.

    This function wraps the logic from Process_Stage_2.py, adapted to work
    with the job queue system. It processes a document through Stage 2 definition
    extraction and scope resolution.

    Args:
        queue: JobQueue instance for progress updates
        job: Job dict with keys: id, file_path, params

    Returns:
        Result dict with keys:
            - total_items: Total operational items in document
            - items_processed: Number of items processed
            - definitions_found: Number of definitions extracted
            - complete: True if Stage 2 is fully complete

    Raises:
        Exception: Any processing errors (will be caught by worker and logged)
    """
    job_id = job['id']
    file_path = job['file_path']
    params = job['params']

    # Extract parameters
    checkpoint_threshold = params.get('checkpoint_threshold', 30)
    max_items = params.get('max_items', 10000)
    config_path = params.get('config') or 'config.json'  # Handle None explicitly

    # Load configuration
    config = get_config(config_path)

    # Determine input and output file paths
    # If file_path ends with _parse_output.json, derive processed path
    # If file_path ends with _processed.json, derive parse path
    if file_path.endswith('_parse_output.json'):
        parse_file_path = file_path
        output_file_path = file_path.replace('_parse_output.json', '_processed.json')
    elif file_path.endswith('_processed.json'):
        output_file_path = file_path
        parse_file_path = file_path.replace('_processed.json', '_parse_output.json')
    else:
        raise ValueError(f"Unexpected file path format: {file_path}")

    # Load content from processed file if it exists, otherwise from parse file
    parsed_content = None
    if os.path.exists(output_file_path):
        with open(output_file_path, encoding='utf-8') as json_file:
            parsed_content = json.load(json_file)
    elif os.path.exists(parse_file_path):
        with open(parse_file_path, encoding='utf-8') as json_file:
            parsed_content = json.load(json_file)
    else:
        raise FileNotFoundError(
            f'Neither parse output nor processed file found:\n'
            f'  Parse file: {parse_file_path}\n'
            f'  Processed file: {output_file_path}'
        )

    # Initialize processing status
    init_processing_status(parsed_content)

    # Check if already complete
    if is_stage_2_complete(parsed_content):
        queue.log_message(job_id, 'INFO', 'Stage 2 already complete, skipping')
        return {
            'total_items': 0,
            'items_processed': 0,
            'definitions_found': 0,
            'already_complete': True,
            'complete': True
        }

    CheckVersion(parsed_content)

    # Count total items for progress tracking
    from utils.document_handling import iter_operational_items
    total_items = 0
    for _, _, _, _, _ in iter_operational_items(parsed_content):
        total_items += 1

    queue.log_message(job_id, 'INFO', f'Stage 2 started: {total_items} operational items')

    # Update initial progress
    update_stage_2_progress(parsed_content, total_items, 0)
    queue.update_progress(job_id, 'initialization', 'starting', 0, total_items)

    # Write initial progress to file BEFORE starting ProgressMonitor
    # This ensures the monitor can read the file immediately
    json_content = json.dumps(parsed_content, indent=4, ensure_ascii=False)
    with open(output_file_path, "w", encoding='utf-8') as outfile:
        outfile.write(json_content)
        outfile.flush()
        os.fsync(outfile.fileno())

    # Create AI client and logfile
    log_dir = os.path.dirname(output_file_path)
    client = create_ai_client(config=config)
    logfile = GetLogfile(log_dir)

    # Import Process_Stage_2 functions
    import Process_Stage_2
    from stage2 import (
        find_defined_terms,
        find_defined_terms_scopes,
        evaluate_and_improve_definitions,
        process_indirect_definitions,
        enhance_resolve_indirect_definitions,
    )
    from stage2.conflict_resolution import resolve_all_definition_conflicts

    # Get the table_of_contents with expanded substantive unit details
    table_of_contents = create_table_of_contents(
        parsed_content,
        parsed_content['document_information']['organization']['content'],
        0, 0, 1
    )

    # Construct processor object
    from stage2 import DefinitionsProcessor
    proc = DefinitionsProcessor(client, logfile, parsed_content, output_file_path, table_of_contents, config)

    # Start progress monitoring in background thread
    # This will periodically read the processed.json file and update the job queue
    progress_monitor = ProgressMonitor(queue, job_id, output_file_path, 'stage_2', poll_interval=5)
    progress_monitor.start()

    try:
        # Phase 1: Definition extraction
        queue.log_message(job_id, 'INFO', 'Phase 1: Extracting definitions')
        process_stage_2_phase_with_progress(
            proc, find_defined_terms, 'extracting_definitions', 1, 6,
            proc, max_items
        )

        # Phase 2: Process indirect definitions
        queue.log_message(job_id, 'INFO', 'Phase 2: Processing indirect definitions')
        process_stage_2_phase_with_progress(
            proc, process_indirect_definitions, 'indirect_definitions', 2, 6,
            proc
        )

        # Phase 3: Scope resolution
        queue.log_message(job_id, 'INFO', 'Phase 3: Resolving scopes')
        process_stage_2_phase_with_progress(
            proc, find_defined_terms_scopes, 'resolving_scopes', 3, 6,
            proc, max_items
        )

        # Phase 4: Quality control
        queue.log_message(job_id, 'INFO', 'Phase 4: Quality control')
        process_stage_2_phase_with_progress(
            proc, evaluate_and_improve_definitions, 'quality_control', 4, 6,
            proc, max_items
        )

        # Phase 5: Enhance indirect definitions
        queue.log_message(job_id, 'INFO', 'Phase 5: Enhancing indirect definitions')
        process_stage_2_phase_with_progress(
            proc, enhance_resolve_indirect_definitions, 'enhancing_indirect', 5, 6,
            proc, max_items
        )

        # Phase 6: Conflict resolution
        queue.log_message(job_id, 'INFO', 'Phase 6: Resolving conflicts')
        document_issues_logfile = get_document_issues_logfile(output_file_path)

        # For conflict resolution, we need a custom wrapper since it has different parameters
        update_stage_2_progress(parsed_content, total_items, int(total_items * 5 / 6), 'conflict_resolution')
        proc.dirty = 1
        proc.flush()

        total_moved = resolve_all_definition_conflicts(parsed_content, document_issues_logfile, proc)
        if total_moved > 0:
            queue.log_message(job_id, 'INFO', f'Moved {total_moved} definitions during conflict resolution')
            proc.dirty = 1

        # Update final progress status
        update_stage_2_progress(parsed_content, total_items, total_items, 'complete')
        proc.dirty = 1
        proc.flush()

        # Count definitions
        definitions_found = 0
        if 'document_level_definitions' in parsed_content:
            definitions_found += len(parsed_content['document_level_definitions'])
        # Could also count definitions in organizational and operational items if needed

        # Save output
        json_content = json.dumps(parsed_content, indent=4, ensure_ascii=False)
        with open(output_file_path, "w", encoding='utf-8') as outfile:
            outfile.write(json_content)
            # Ensure immediate visibility on disk (Windows compatibility)
            outfile.flush()
            os.fsync(outfile.fileno())

        queue.log_message(job_id, 'INFO', f'Stage 2 completed: {output_file_path}')

        return {
            'total_items': total_items,
            'items_processed': total_items,
            'definitions_found': definitions_found,
            'complete': True
        }

    except Exception as e:
        # Save partial progress before failing
        try:
            update_stage_2_progress(parsed_content, total_items, total_items, 'error')
            json_content = json.dumps(parsed_content, indent=4, ensure_ascii=False)
            with open(output_file_path, "w", encoding='utf-8') as outfile:
                outfile.write(json_content)
                outfile.flush()
                os.fsync(outfile.fileno())
        except:
            pass  # If save fails, just continue with the error
        raise

    finally:
        # Stop progress monitor when done (success or failure)
        progress_monitor.stop()


def process_question_job(queue, job):
    """
    Execute question-answering job for a processed document.

    This function wraps the logic from question_answering.py, adapted to work
    with the job queue system. It processes a question against a fully processed
    document (Stage 3 complete).

    Args:
        queue: JobQueue instance for progress updates
        job: Job dict with keys: id, file_path, params

    Returns:
        Result dict with keys:
            - question_text: The question that was answered
            - answer_text: The final answer
            - question_file: Path to the question file created/used
            - relevance_scores: Number of units scored
            - iterations: Number of analysis iterations performed

    Raises:
        Exception: Any processing errors (will be caught by worker and logged)
    """
    job_id = job['id']
    file_path = job['file_path']
    params = job['params']

    # Extract parameters
    question_text = params.get('question_text')
    question_file_path = params.get('question_file')  # Optional: pre-existing question file
    max_items = params.get('max_items', 300)
    max_tokens = params.get('max_tokens', 1000)
    max_iterations = params.get('max_iterations', 3)
    qa_mode = params.get('qa_mode')  # Optional: Q&A mode (quick_scan, standard, thorough, maximum_confidence)

    if not question_text and not question_file_path:
        raise ValueError("Either question_text or question_file must be provided in params")

    # Determine processed file path
    # If file_path ends with .xml or .html, derive processed path
    # If file_path ends with _processed.json, use as-is
    if file_path.endswith('.xml') or file_path.endswith('.html'):
        dir_path = os.path.dirname(file_path)
        file_stem = re.sub(r'\.\w+$', '', os.path.basename(file_path))
        processed_file_path = os.path.join(dir_path, file_stem + '_processed.json')
    elif file_path.endswith('_processed.json'):
        processed_file_path = file_path
        dir_path = os.path.dirname(file_path)
        file_stem = os.path.basename(file_path).replace('_processed.json', '')
    else:
        raise ValueError(f"Unexpected file path format: {file_path}")

    # Load processed content
    if not os.path.exists(processed_file_path):
        raise FileNotFoundError(f'Processed file not found (Stage 3 must be complete): {processed_file_path}')

    with open(processed_file_path, 'r', encoding='utf-8') as json_file:
        parsed_content = json.load(json_file)

    # Check if Stage 3 is complete (prerequisite for question answering)
    if not is_stage_3_complete(parsed_content):
        raise ValueError(f'Stage 3 not complete (prerequisite for question answering): {processed_file_path}')

    # Import question_answering module functions
    from utils.text_processing import clean_text
    import question_answering

    # Load or normalize question text
    if question_file_path and os.path.isfile(question_file_path):
        # Use existing question file
        with open(question_file_path, 'r', encoding='utf-8') as f:
            question_object = json.load(f)
        question_text = question_object.get('question', {}).get('text', question_text)
    else:
        # Clean question text
        question_text = clean_text(question_text)
        if not question_text:
            raise ValueError("Question text is empty after cleaning")

        # Find or create question file (mirroring question_answering.py logic)
        question_file_path = None
        files = os.listdir(dir_path)

        for fname in files:
            if not fname.startswith(file_stem + '_question_'):
                continue
            if not fname.endswith('.json'):
                continue

            fpath = os.path.join(dir_path, fname)
            try:
                with open(fpath, 'r', encoding='utf-8') as f:
                    obj = json.load(f)
                stored_q = obj.get('question', {}).get('text', '')
                if clean_text(stored_q) == question_text:
                    question_file_path = fpath
                    question_object = obj
                    break
            except:
                continue

        # If no matching file found, create new one
        if not question_file_path:
            # Find next available number
            used_numbers = set()
            for fname in files:
                match = re.match(rf'{re.escape(file_stem)}_question_(\d+)\.json', fname)
                if match:
                    used_numbers.add(int(match.group(1)))

            next_num = 1
            while next_num in used_numbers:
                next_num += 1

            question_file_path = os.path.join(dir_path, f'{file_stem}_question_{next_num}.json')
            question_object = {
                'question': {'text': question_text},
                'complete': False
            }

    # Check if already complete
    is_complete = question_object.get('complete', False)
    final_answer_text = question_object.get('working_answer', {}).get('text', '')

    if is_complete and final_answer_text:
        queue.log_message(job_id, 'INFO', 'Question already answered, skipping')
        return {
            'question_text': question_text,
            'answer_text': final_answer_text,
            'question_file': question_file_path,
            'already_complete': True
        }

    # Ensure question text is set
    if 'question' not in question_object:
        question_object['question'] = {}
    question_object['question']['text'] = question_text

    queue.log_message(job_id, 'INFO', f'Question: {question_text[:100]}...' if len(question_text) > 100 else f'Question: {question_text}')

    # Load configuration
    config_path = params.get('config') or 'config.json'
    config = get_config(config_path)

    # Set up AI client and logfile
    client = create_ai_client(config=config)
    logfile = GetLogfile(dir_path)

    # Define progress callback for granular progress reporting
    def progress_callback(phase, status, items_done, items_total):
        """Callback to report progress from QuestionProcessor methods."""
        queue.update_progress(job_id, phase, status, items_done, items_total)

    # Create QuestionProcessor with progress callback and mode
    qp = question_answering.QuestionProcessor(
        client, parsed_content, question_object, question_file_path, logfile,
        progress_callback=progress_callback, config=config, mode=qa_mode,
        processed_file_path=processed_file_path,
    )

    try:
        # Phase 1: Relevance scoring
        queue.log_message(job_id, 'INFO', 'Phase 1: Scoring relevance of units')
        queue.update_progress(job_id, 'relevance_scoring', 'starting', 0, 100)
        qp.score_relevance(max_items=max_items, max_tokens=max_tokens)

        # Count scored units
        relevance_scores = 0
        if 'relevance_scores' in question_object:
            relevance_scores = len(question_object['relevance_scores'])
        queue.log_message(job_id, 'INFO', f'Scored {relevance_scores} units')

        # Check if mode is configured to stop after scoring
        if qp.mode_config.get("stop_after_scoring", False):
            queue.log_message(job_id, 'INFO', 'Mode configured to stop after scoring - skipping analysis phases')
            queue.update_progress(job_id, 'complete', 'finished', 1, 1)
            return {
                'question_text': question_text,
                'answer_text': 'Quick scan mode: See scored sections in question file',
                'question_file': question_file_path,
                'mode': qa_mode or 'quick_scan',
                'quick_scan': True
            }
        
        # Phase 2: Iterative analysis
        queue.log_message(job_id, 'INFO', 'Phase 2: Running iterative analysis')
        queue.update_progress(job_id, 'iterative_analysis', 'starting', 0, 100)
        qp.run_to_stability(base_max_iterations=None)  # Uses mode_config["max_analysis_passes"]

        # Phase 3: Cleanup
        queue.log_message(job_id, 'INFO', 'Phase 3: Cleaning up scratch document')
        queue.update_progress(job_id, 'cleanup', 'processing', 0, 1)
        qp.cleanup_scratch_and_answer()
        queue.update_progress(job_id, 'cleanup', 'completed', 1, 1)

        # Phase 4: Final answer
        queue.log_message(job_id, 'INFO', 'Phase 4: Generating final answer')
        queue.update_progress(job_id, 'final_answer', 'processing', 0, 1)
        qp.generate_final_answer()
        queue.update_progress(job_id, 'final_answer', 'completed', 1, 1)

        # Phase 5: Quality check (optional) - runs AFTER final answer to validate it
        if qp.mode_config.get("quality_check_phase", False):
            queue.log_message(job_id, 'INFO', 'Phase 5: Running quality check on final answer')
            queue.update_progress(job_id, 'quality_check', 'processing', 0, 1)
            qp.quality_check_answer()
            queue.update_progress(job_id, 'quality_check', 'completed', 1, 1)

        # Mark as complete
        qp.question_object['complete'] = True
        qp._save_question_object()

        queue.update_progress(job_id, 'complete', 'finished', 4, 4)

        final_answer = qp.question_object['working_answer']['text']
        queue.log_message(job_id, 'INFO', f'Answer generated ({len(final_answer)} characters)')

        return {
            'question_text': question_text,
            'answer_text': final_answer,
            'question_file': question_file_path,
            'relevance_scores': relevance_scores,
            'complete': True
        }

    except Exception as e:
        # Save partial progress before failing
        try:
            qp._save_question_object()
        except:
            pass
        raise
