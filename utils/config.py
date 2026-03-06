"""
Configuration loader for Document Analyzer.

Loads settings from config.json (required). Provides accessor functions
for specific configuration sections.
"""
# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License

import json
import os
import sys
from pathlib import Path


# Cached configuration (populated by get_config on first call)
_config_cache = None


def get_config(config_path: str = 'config.json') -> dict:
    """
    Load configuration from config.json.

    The config file is required. If it does not exist, prints an error
    message and exits.

    Args:
        config_path: Path to config file (default: 'config.json' in current directory)

    Returns:
        Configuration dictionary
    """
    global _config_cache

    if _config_cache is not None:
        return _config_cache

    if not os.path.exists(config_path):
        print(f"Error: Configuration file '{config_path}' not found.", file=sys.stderr)
        print("This file is required to run Document Analyzer.", file=sys.stderr)
        print("See config.json in the project repository for the expected format.", file=sys.stderr)
        sys.exit(1)

    with open(config_path, 'r', encoding='utf-8') as f:
        _config_cache = json.load(f)

    return _config_cache


def get_output_directory(config: dict = None) -> str:
    """
    Get configured output directory, creating it if necessary.

    Args:
        config: Configuration dictionary (loads from file if None)

    Returns:
        Absolute path to output directory
    """
    if config is None:
        config = get_config()

    output_dir = config.get('output', {}).get('directory')
    if not output_dir:
        output_dir = os.path.join(os.path.expanduser('~'), 'document_analyzer_output')

    # Expand user path if needed
    output_dir = os.path.expanduser(output_dir)

    # Create directory if it doesn't exist
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    return output_dir


def get_output_structure(config: dict = None) -> str:
    """
    Get configured output directory structure.

    Args:
        config: Configuration dictionary (loads from file if None)

    Returns:
        'flat' or 'per_document'
    """
    if config is None:
        config = get_config()

    return config.get('output', {}).get('structure', 'per_document')


def get_parse_mode(config: dict = None) -> str:
    """
    Get default parsing mode for multi-chapter documents.

    Args:
        config: Configuration dictionary (loads from file if None)

    Returns:
        'split_chapters' or 'full_document'
    """
    if config is None:
        config = get_config()

    return config.get('processing', {}).get('default_parse_mode', 'split_chapters')


def get_checkpoint_threshold(config: dict = None) -> int:
    """
    Get default checkpoint threshold for processing stages.

    Args:
        config: Configuration dictionary (loads from file if None)

    Returns:
        Number of items to process before checkpoint
    """
    if config is None:
        config = get_config()

    return config.get('processing', {}).get('default_checkpoint_threshold', 30)


def get_document_roots_config(config: dict = None) -> dict:
    """
    Get document roots configuration.

    Args:
        config: Configuration dictionary (loads from file if None)

    Returns:
        Dict mapping document root name to root configuration
    """
    if config is None:
        config = get_config()

    return config.get('document_roots', {})


def get_job_queue_database(config: dict = None) -> str:
    """
    Get job queue database path.

    Args:
        config: Configuration dictionary (loads from file if None)

    Returns:
        Path to jobs.db file
    """
    if config is None:
        config = get_config()

    db_path = config.get('job_queue', {}).get('database')
    if not db_path:
        output_dir = get_output_directory(config)
        db_path = os.path.join(output_dir, 'jobs.db')

    return db_path


def get_model_config(config: dict, model_name: str) -> dict:
    """
    Get configuration for a specific model name.

    Args:
        config: Configuration dictionary
        model_name: Model name (e.g., 'gpt-5-nano', 'claude-sonnet-4-5')

    Returns:
        Dict with 'platform', 'model', 'max_tokens', etc.

    Raises:
        ValueError: If model name not found in configuration
    """
    models = config.get('models', {})

    if model_name not in models:
        raise ValueError(f"Model '{model_name}' not found in configuration. Available models: {list(models.keys())}")

    return models[model_name]


def get_model_for_task(config: dict, task_name: str) -> str:
    """
    Get model name for a specific processing task.

    Checks 'model_assignments' in config and falls back to 'current_engine'
    if the task is not explicitly configured.

    Args:
        config: Configuration dictionary
        task_name: Task name (e.g., 'stage3.summary.level1', 'qa.relevance.score')

    Returns:
        Model name string (e.g., 'gpt-5-nano', 'claude-sonnet-4-5')
    """
    model_assignments = config.get('model_assignments', {})

    if task_name in model_assignments:
        return model_assignments[task_name]

    # Fallback to default model
    return config.get('current_engine', 'gpt-5-nano')


def get_fallback_models(config: dict, task_name: str) -> list:
    """
    Get fallback model list for a specific task.

    Checks for task-specific fallback models first, then falls back to global
    fallback model list.

    Args:
        config: Configuration dictionary
        task_name: Task name (e.g., 'stage3.summary.topic_statement')

    Returns:
        List of fallback model names, or empty list
    """
    retry_config = config.get('retry', {})

    # Check for task-specific fallback
    task_fallback_key = f"{task_name}.fallback_models"
    if task_fallback_key in retry_config:
        return retry_config[task_fallback_key]

    # Use global fallback
    return retry_config.get('fallback_models', [])


def get_max_retries_per_model(config: dict) -> int:
    """
    Get maximum retry count per model.

    Args:
        config: Configuration dictionary

    Returns:
        Max retries per model (default: 3 if not configured)
    """
    return config.get('retry', {}).get('max_retries_per_model', 3)


def create_client_for_task(config: dict, task_name: str):
    """
    Create AI client for a specific processing task.

    Combines get_model_for_task() with create_ai_client() to create a client
    configured for a specific task.

    Args:
        config: Configuration dictionary
        task_name: Task name (e.g., 'stage3.summary.level1')

    Returns:
        BaseAIClient instance configured for the task
    """
    from .ai_client import create_ai_client
    model_name = get_model_for_task(config, task_name)
    return create_ai_client(model_name=model_name, config=config)


def get_qa_mode_config(mode_name: str = None, config: dict = None) -> dict:
    """
    Get configuration for a Q&A mode. Returns default if mode not found.

    Args:
        mode_name: Name of the mode (e.g., 'quick_scan', 'standard', 'thorough', 'maximum_confidence')
                  If None, uses default_qa_mode from config
        config: Configuration dictionary (loads from file if None)

    Returns:
        Dictionary with mode configuration, merged with defaults to ensure all keys present
    """
    if config is None:
        config = get_config()

    modes = config.get("question_answering_modes", {})
    default_mode = config.get("default_qa_mode", "standard")

    if mode_name is None:
        mode_name = default_mode

    mode_config = modes.get(mode_name, modes.get(default_mode, {}))

    # Merge with defaults to ensure all keys present
    defaults = {
        "scoring_summary_level": "summary_1",
        "org_summary_scoring": True,
        "stop_after_scoring": False,
        "max_analysis_passes": 3,
        "quality_check_phase": False,
        "analyze_zero_score_sections": False,
        "scoring_fallback_to_summary_2": False,
        "scoring_batch_max_chars": 10000,
        "scoring_batch_max_items": 10,
        "score_1_gate": False,
        "deduplicate_new_facts": False,
        "compact_after_additions": 0,
        "implicit_reference_detection": False,
    }

    return {**defaults, **mode_config}
