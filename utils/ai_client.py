# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License
from genericpath import isfile
from urllib.request import CacheFTPHandler
from api_keys import secrets
from datetime import datetime, UTC
import os
import json
import re
import anthropic
from openai import AzureOpenAI
import openai
from .config import get_config
from .error_handling import ConfigError, InputError
from .error_handling import ModelError
from .api_cache import get_cached_response, set_cached_response, remove_cached_response
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
import warnings
import time
import random


def make_api_call_with_retry(api_call_func, max_retries=5, base_delay=2):
    """
    Make API call with exponential backoff for rate limits and transient errors.

    This wrapper handles:
    - Rate limit errors (HTTP 429)
    - Connection errors (DNS failures, network issues)
    - Server errors (5xx)
    - Other transient API failures

    Args:
        api_call_func: Callable that makes the API call
        max_retries: Maximum number of retry attempts (default: 5)
        base_delay: Base delay in seconds for exponential backoff (default: 2)

    Returns:
        The result of api_call_func() if successful

    Raises:
        The original exception if max retries exceeded or non-retryable error
    """
    for attempt in range(max_retries):
        try:
            return api_call_func()

        except Exception as e:
            error_str = str(e).lower()
            error_type = type(e).__name__

            # Determine if error is retryable
            is_retryable = False
            error_category = "unknown"

            # Rate limit errors
            if "rate_limit" in error_str or "429" in error_str or "rate limit" in error_type.lower():
                is_retryable = True
                error_category = "rate limit"

            # Connection errors (including DNS failures like the user experienced)
            elif "connection" in error_str or "getaddrinfo failed" in error_str or \
                 "apiconnectionerror" in error_type.lower() or "connecterror" in error_type.lower():
                is_retryable = True
                error_category = "connection"

            # Server errors (5xx)
            elif "500" in error_str or "502" in error_str or "503" in error_str or \
                 "504" in error_str or "internal server error" in error_str:
                is_retryable = True
                error_category = "server"

            # Timeout errors
            elif "timeout" in error_str or "timed out" in error_str:
                is_retryable = True
                error_category = "timeout"

            # Retry logic
            if is_retryable and attempt < max_retries - 1:
                # Calculate delay with exponential backoff and jitter
                exponential_delay = base_delay * (2 ** attempt)
                jitter = random.uniform(0, exponential_delay * 0.1)  # Add up to 10% jitter
                delay = exponential_delay + jitter

                print(f"    {error_category.title()} error (attempt {attempt + 1}/{max_retries}): {error_type}")
                print(f"    Retrying in {delay:.1f} seconds...")
                time.sleep(delay)
                continue
            elif is_retryable:
                # Max retries exceeded
                print(f"    {error_category.title()} error persisted after {max_retries} attempts. Aborting.")
                raise e
            else:
                # Non-retryable error - re-raise immediately
                print(f"    Non-retryable error: {error_type}: {e}")
                raise e

    return None


def safe_int(d, key):
    if d is None:
        return 0
    # dict-like
    try:
        return int(d.get(key, 0))
    except Exception:
        pass
    # attribute-like
    try:
        return int(getattr(d, key, 0))
    except Exception:
        return 0

def GetLogfile(dir_path=''):
    if not os.path.isdir(dir_path):
        if os.path.isfile(dir_path):
            dir_path = os.path.dirname(dir_path)
        else:
            dir_path = os.path.abspath(os.path.curdir)
    count = 1
    config = get_config()
    log_stem = config.get('log_stem', 'log')
    while os.path.exists(os.path.join(dir_path, log_stem + str(count).zfill(4) + '.json')):
        count += 1
    logfile = str(os.path.join(dir_path, log_stem + str(count).zfill(4) + '.json'))
    return logfile


def QueryWithBaseClient(ai_client: 'BaseAIClient', cache_prompt_list: str, query_prompt: str, logfile: str = '', json_output: bool = True, max_tokens: int = 0, return_full_response: bool = False):
    """
    Refactored Query function that uses BaseAIClient and create_message.
    
    Args:
        ai_client: BaseAIClient instance for making queries
        cache_prompt_list (list): A list of static portions of the prompt, each a string
        query_prompt: String for the query
        logfile: Path to logfile for recording query and response (optional)
        json_output: Whether to return only JSON output (default: True)
        max_tokens: Maximum tokens for response (0 uses config default)
        
    Returns:
        str: AI response, optionally extracted as JSON
        
    Note:
        This function replaces the old Query function's functionality but uses
        the new BaseAIClient architecture. AI_engine selection is not supported
        as the client is pre-configured.
        
    Technical Note:
        The function automatically handles different message formats for each provider:
        - Claude: System message is passed as separate 'system' parameter
        - OpenAI: System message is included in the messages list
        This maintains compatibility with the old Query function behavior.
    """
    # Set system message
    system_message = "You are a legal expert that analyzes legal documents."

    # Create system message
    system_msg = AIMessage(
        role="system",
        cache='',
        content=system_message
    )
    messages = [system_msg]
    full_cache = ''

    for part in cache_prompt_list:
        full_cache += part
    # Create cache messages
        system_msg = AIMessage(
            role="system",
            cache=part,
            content=''
        )
        messages.append(system_msg)
    
    # Get model name from client
    model_name = getattr(ai_client, 'model', 'unknown')

    # Check cache before making API call
    cached_response = get_cached_response(full_cache, query_prompt, model_name, max_tokens)
    if cached_response is not None:
        # Use cached response
        result = cached_response
        
        # Log the cached query and response (marked as cached)
        log_entry = [str(datetime.now(UTC)), full_cache, query_prompt, result, 0, 0, 'CACHED']
        if not logfile:
            logfile = GetLogfile()
        
        with open(logfile, "a", encoding="utf-8") as logfile_handle:
            logfile_handle.write(json.dumps(log_entry, indent=4))

        # Extract JSON if requested
        if json_output:
            result = extract_json_from_response(result)

        if return_full_response:
            # For cached responses, create a minimal AIResponse object
            return (result, AIResponse(content=result, stop_reason='cached'))
        return result

    # Create user message with the query
    user_message = AIMessage(
        role="user",
        cache='',
        content=query_prompt
    )

    messages.append(user_message)

    # Create empty tools list (no tool calls for basic queries)
    tools = []

    # Make the query using the BaseAIClient
    response = ai_client.create_message(
        messages=messages,
        tools=tools,
        max_tokens=max_tokens
    )
    
    # Extract the content
    result = response.content
    
    # Store response in cache (only if non-empty - empty responses should not be cached
    # as they indicate failures that need retry)
    if result and str(result).strip():
        set_cached_response(full_cache, query_prompt, model_name, result, max_tokens)
    
    # Log the query and response
    log_entry = [str(datetime.now(UTC)), full_cache, query_prompt, result, response.cache_created, response.cache_read]
    if not logfile:
        logfile = GetLogfile()
    
    with open(logfile, "a", encoding="utf-8") as logfile_handle:
        logfile_handle.write(json.dumps(log_entry, indent=4))

    # Extract JSON if requested
    if json_output:
        result = extract_json_from_response(result)

    if return_full_response:
        return (result, response)
    return result


def Query(client, query_prompt, logfile, json_output=1, max_tokens=0, AI_engine=''):
    """
    DEPRECATED: Use QueryWithBaseClient instead.
    
    This function will be removed in a future version.
    Use QueryWithBaseClient with a BaseAIClient instance for new code.
    """
    warnings.warn(
        "Query function is deprecated. Use QueryWithBaseClient with BaseAIClient instead.",
        DeprecationWarning,
        stacklevel=2
    )
    
    # client is the object used for making queries to the AI engine.
    # query_prompt is the string for the query.
    # logfile is a string with the path to the logfile used for recording the query and response from the AI engine.
    # If json_output is set to 1, then return only JSON output.
    # If no positive value given for max_tokens, then use what is in the config.json file.
    config = get_config()
    if '' == AI_engine:
        # AI_engine is a platform name like 'Claude' or 'OpenAI'
        # Map current_engine (a model name) back to its platform
        current_model = config.get('current_engine', '')
        model_config = config.get('models', {}).get(current_model, {})
        AI_engine = model_config.get('platform', '')
        if not AI_engine:
            raise ConfigError("current_engine not found in models configuration.")
    # Find the model entry matching this platform
    models = config.get('models', {})
    model_entry = None
    for m_name, m_config in models.items():
        if m_config.get('platform') == AI_engine:
            model_entry = m_config
            break
    if model_entry is None:
        raise ConfigError(f"No model configured for platform '{AI_engine}'.")
    result = ''
    model = model_entry['model']
    if max_tokens <= 0:
        max_tokens = model_entry.get('max_tokens', 8000)
    if 'Claude' == AI_engine:
        def _make_api_call():
            return client.messages.create(
                model=model,
                max_tokens=max_tokens,
                system="You are a legal expert that analyzes legal documents.",
                messages=[
                    {
                        "role": "user",
                        "content": query_prompt
                    }
                ]
            )
        response = make_api_call_with_retry(_make_api_call)
        result = response.content[0].text
    elif 'Azure' == AI_engine:
        chat_prompt = [
            {
                "role": "system",
                "content": [
                    {
                        "type": "text",
                        "text": "You are a legal expert that analyzes legal documents."
                    }
                ]
            },
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": query_prompt
                    }
                ]
            }
        ]
        def _make_api_call():
            return client.chat.completions.create(model=model, messages=chat_prompt, max_tokens=max_tokens)
        response = make_api_call_with_retry(_make_api_call)
        result = response.choices[0].message.content
    elif 'OpenAI' == AI_engine:
        def _make_api_call():
            return client.chat.completions.create(
                model=model,
                messages=[
                    {
                        "role": "system",
                        "content": "You are a helpful assistant."
                    },
                    {
                        "role": "user",
                        "content": query_prompt
                    }
                ],
                max_completion_tokens=max_tokens
            )
        response = make_api_call_with_retry(_make_api_call)
        result = response.choices[0].message.content
    log_entry = [str(datetime.now(UTC)), query_prompt, result]
    if '' == logfile:
        logfile = GetLogfile()
    with open(logfile, "a", encoding="utf-8") as logfile_handle:
        logfile_handle.write(json.dumps(log_entry, indent=4))
    if 1 == json_output:
        # Enhanced JSON extraction to handle cases where AI includes explanatory text
        result = extract_json_from_response(result)
    return result

def query_json(ai_client, cache_prompt_list: list, prompt: str, logfile: str, max_tokens: int = 0, max_retries: int = 3, expected_keys: list = None, config: dict = None, task_name: str = None) -> list:
    """
    Send a prompt to the AI model and return parsed JSON response.

    Includes automatic retry with cache-busting for malformed responses and optional
    fallback to alternative models if the primary model fails.

    Args:
        ai_client: BaseAIClient instance for making queries
        cache_prompt_list (list): A list of static portions of the prompt, each a string
        prompt (str): The prompt to send to the AI model
        logfile(str): The logfile to write logging information to
        max_tokens (int): Maximum tokens for response (0 uses config default)
        max_retries (int): Maximum number of retry attempts for malformed responses (default: 3)
                          NOTE: If config and task_name provided, this uses get_max_retries_per_model(config) instead
        expected_keys (list): Optional list of required keys that must be present in the parsed JSON.
                             If specified, responses missing these keys will trigger retry.
                             If None, any valid JSON is accepted (default: None)
        config (dict): Optional configuration dictionary (enables fallback model support)
        task_name (str): Optional task name (e.g., 'stage3.summary.level2')
                        Required for fallback model support

    Returns:
        list: Parsed JSON response from the model

    Raises:
        ModelError: If the model fails to respond or returns invalid JSON after all retries
                   (including fallback models if configured)

    Notes:
        - If config and task_name are provided, fallback models will be tried after primary model exhausts retries
        - Fallback models are determined by get_fallback_models(config, task_name)
        - Each model (primary and fallbacks) gets max_retries_per_model attempts
        - Logs which model succeeded for quality tracking
    """
    # Need to re-construct cache_prompt by combining potentially smaller parts into ones large enough to cache.
    new_cache_prompt_list = []
    for part in cache_prompt_list:
        if not isinstance(part, str):
            raise InputError('Call to query_json with malformed cache_prompt_list.')
            exit(1)
        if 0 == len(new_cache_prompt_list):
            new_cache_prompt_list.append(part)
        else:
            last_item = new_cache_prompt_list[-1]
            if len(last_item) > 4500: # Based on estimate for 1024 tokens.
                new_cache_prompt_list.append(part)
            else: # Last item is too short for cache, so add new item to it.
                new_cache_prompt_list.pop()
                new_cache_prompt_list.append(last_item + part)

    # Anthropic API allows maximum of 4 cache_control blocks
    # If we have more than 4 blocks, combine the FIRST blocks to get down to 4
    # This optimizes for cumulative growth patterns where:
    # - Early blocks (instructions, initial summaries) are stable and reused
    # - Later blocks (recent summaries) are added incrementally
    MAX_CACHE_BLOCKS = 4
    if len(new_cache_prompt_list) > MAX_CACHE_BLOCKS:
        # Calculate how many blocks need to be combined
        excess_blocks = len(new_cache_prompt_list) - MAX_CACHE_BLOCKS
        # Combine the first (excess_blocks + 1) blocks into a superblock
        # This leaves the last 3 blocks separate for individual caching
        num_to_combine = excess_blocks + 1

        # Create superblock from first num_to_combine blocks
        superblock = ''.join(new_cache_prompt_list[:num_to_combine])

        # Rebuild list: [superblock, remaining blocks...]
        new_cache_prompt_list = [superblock] + new_cache_prompt_list[num_to_combine:]

    # Determine effective max_retries (use config value if provided)
    if config and task_name:
        from .config import get_max_retries_per_model, get_fallback_models
        effective_max_retries = get_max_retries_per_model(config)
        # Check if fallback models are configured - if so, primary model only gets 1 attempt
        fallback_models = get_fallback_models(config, task_name)
        primary_max_attempts = 1 if fallback_models else effective_max_retries
        # Warn if no fallback models configured (potential config issue)
        if not fallback_models:
            print(f"    WARNING: No fallback models for task={task_name}. Config retry section: {config.get('retry', 'MISSING')}")
    else:
        effective_max_retries = max_retries
        primary_max_attempts = max_retries
        fallback_models = []
        # Warn when config/task_name not provided (fallbacks disabled)
        if task_name:
            print(f"    WARNING: Fallbacks disabled - config not provided for task={task_name}")

    # Track failed attempts for cleanup later
    failed_attempts = []
    full_cache = ''.join(new_cache_prompt_list)
    model_name = getattr(ai_client, 'model', 'unknown')

    # Retry loop for handling malformed responses (primary model)
    # If fallback models are configured, primary only gets 1 attempt
    for attempt in range(primary_max_attempts):
        # For retry attempts, create a cache-busting variation of the prompt
        if attempt == 0:
            current_prompt = prompt
        else:
            # Add cache-busting variations with randomness AT THE BEGINNING
            # This maximizes the chance of avoiding platform-side caching
            cache_bust_prefix = f"[Request ID: {random.randint(100000, 999999)} - Please ensure your response is properly formatted JSON]\n\n"
            current_prompt = cache_bust_prefix + prompt
            print(f"    WARNING: Retrying query (attempt {attempt + 1}/{effective_max_retries}) with cache-busting variation...")

        try:
            response_obj = QueryWithBaseClient(ai_client, new_cache_prompt_list, current_prompt, logfile, True, max_tokens, return_full_response=True)

            # Handle both old (string) and new (tuple) return formats
            if isinstance(response_obj, tuple):
                result, ai_response = response_obj
            else:
                result = response_obj
                ai_response = None

            # Check if response is empty or has problematic stop_reason
            is_problematic = False
            error_reason = None
            parsed_result = None

            if not result:
                is_problematic = True
                error_reason = "empty_response"
            else:
                # Try to parse JSON
                try:
                    parsed_result = json.loads(result)

                    # Successfully parsed - now check if it has required keys (if specified)
                    if expected_keys is not None:
                        # Validate that required keys are present
                        missing_keys = []

                        # Handle both dict and list responses
                        if isinstance(parsed_result, dict):
                            missing_keys = [key for key in expected_keys if key not in parsed_result]
                        elif isinstance(parsed_result, list):
                            # For list responses, expected_keys should be empty or None
                            # If expected_keys is specified for a list, that's a mismatch
                            if expected_keys:
                                is_problematic = True
                                error_reason = f"expected_dict_with_keys_{expected_keys}_but_got_list"
                        else:
                            # For other types (null, primitives), any expected_keys is a problem
                            if expected_keys:
                                is_problematic = True
                                error_reason = f"expected_dict_with_keys_{expected_keys}_but_got_{type(parsed_result).__name__}"

                        # If we found missing keys, flag as problematic
                        if missing_keys and not is_problematic:
                            is_problematic = True
                            error_reason = f"missing_required_keys: {missing_keys}"

                    # If JSON parsed successfully and passes validation (or no validation required),
                    # accept it - even if it's short like [] or {}
                    # Short valid JSON is legitimate (e.g., empty lists, empty objects)

                except Exception as e:
                    # Failed to parse JSON - this is always problematic
                    is_problematic = True
                    error_reason = f"json_parse_error: {e}"

            # If this attempt succeeded, clean up failed cache entries and return
            if not is_problematic and parsed_result is not None:
                # Always save successful response under the original prompt key
                # This works with the earlier fix that saves successful retries under original key:
                # - If attempt 0 succeeded: QueryWithBaseClient() already cached it under original key,
                #   and set_cached_response() will skip (key exists) - this is fine, value is correct
                # - If attempt > 0 succeeded: We remove the bad attempt 0 entry (if any), then save
                #   the successful retry response under original key for future cache hits
                
                # If this was a retry (attempt > 0), we need to ensure the original key has the correct value
                # First, remove any bad cache entry from attempt 0 (if it was cached before we detected it was problematic)
                if attempt > 0:
                    # Remove the bad attempt 0 entry (if it exists) so we can save the good retry response
                    remove_cached_response(full_cache, prompt, model_name, max_tokens)
                    # Also remove the retry key entry (with cache-busting prefix) since it's not useful
                    remove_cached_response(full_cache, current_prompt, model_name, max_tokens)
                
                # Save successful response under the original prompt key
                # (This will write because we removed it above if attempt > 0, or skip if attempt 0 and already cached)
                set_cached_response(full_cache, prompt, model_name, result, max_tokens)
                
                if failed_attempts:
                    # We had failures but eventually succeeded - clean up bad cache entries
                    error_log_path = logfile.replace('.json', '_error.log') if logfile else 'error.log'
                    with open(error_log_path, 'a', encoding='utf-8') as error_log:
                        error_log.write(f"\n{'='*70}\n")
                        error_log.write(f"RETRY SUCCESS: Query succeeded after {attempt} failed attempt(s)\n")
                        error_log.write(f"Timestamp: {datetime.now(UTC)}\n")
                        error_log.write(f"Model: {model_name}\n")
                        error_log.write(f"\nCleaning up {len(failed_attempts)} bad cache entries...\n")

                        for i, failed in enumerate(failed_attempts):
                            removed = remove_cached_response(full_cache, failed['prompt'], model_name, max_tokens)
                            error_log.write(f"  Attempt {i+1}: {'Removed' if removed else 'Not found in cache'} - Reason: {failed['reason']}\n")

                        error_log.write(f"\nSuccessful prompt variation: {current_prompt[:200]}...\n")
                        if attempt > 0:
                            error_log.write(f"Response saved under original prompt key for future cache hits.\n")
                        error_log.write(f"{'='*70}\n")

                    print(f"    SUCCESS: Query succeeded after {attempt} retry attempt(s). Bad cache entries cleaned.")
                    if attempt > 0:
                        print(f"    Response saved under original prompt key for future cache hits.")

                return parsed_result

            # This attempt failed - record it
            failed_attempts.append({
                'prompt': current_prompt,
                'result': result if result else '',
                'reason': error_reason,
                'stop_reason': ai_response.stop_reason if ai_response else 'unknown'
            })
            
            # CRITICAL: If this was attempt 0 (first attempt) and it failed, we need to clean up
            # the cache entry that QueryWithBaseClient() created before we detected it was problematic.
            # QueryWithBaseClient() caches responses immediately, so an empty/problematic response from attempt 0
            # would already be cached under the original prompt key.
            if attempt == 0 and is_problematic:
                # Remove the bad cache entry from attempt 0 so retries don't hit it
                remove_cached_response(full_cache, current_prompt, model_name, max_tokens)
                print(f"    [Cache cleanup] Removed problematic response from attempt 0 before retrying...")

            # If this is not the last attempt, continue to retry
            if attempt < primary_max_attempts - 1:
                continue

            # This was the last attempt with primary model and it failed
            # If fallback models are configured, break immediately to try them
            break

        except ModelError as e:
            # ModelError from QueryWithBaseClient - record it and continue with retries or fallback
            # Don't re-raise immediately - we want to try fallback models if configured
            failed_attempts.append({
                'prompt': current_prompt,
                'result': '',
                'reason': f"model_error: {e}",
                'stop_reason': 'error'
            })

            if attempt < primary_max_attempts - 1:
                print(f"    WARNING: ModelError on attempt {attempt + 1}: {e}. Retrying...")
                continue
            else:
                # Last attempt with primary model - break out to try fallback models
                break
        except Exception as e:
            # Unexpected error - log and potentially retry
            failed_attempts.append({
                'prompt': current_prompt,
                'result': '',
                'reason': f"unexpected_error: {e}",
                'stop_reason': 'error'
            })

            if attempt < primary_max_attempts - 1:
                print(f"    WARNING: Unexpected error on attempt {attempt + 1}: {e}. Retrying...")
                continue
            else:
                # Last attempt with primary model - break out to try fallback models
                break

    # Primary model exhausted attempts - try fallback models if configured
    if config and task_name:
        # create_ai_client is defined in this module (utils/ai_client.py), not in utils/config.py
        # fallback_models already retrieved above if config/task_name provided

        if fallback_models:
            print(f"    Primary model '{model_name}' failed after {primary_max_attempts} attempt(s). Immediately trying fallback models: {fallback_models}")

            for fallback_model_name in fallback_models:
                print(f"    Attempting fallback model: {fallback_model_name}")

                try:
                    # Create client for fallback model
                    fallback_client = create_ai_client(model_name=fallback_model_name, config=config)
                    fallback_model_actual = getattr(fallback_client, 'model', fallback_model_name)

                    # Try fallback model with same retry logic
                    for attempt in range(effective_max_retries):
                        if attempt == 0:
                            current_prompt = prompt
                        else:
                            cache_bust_prefix = f"[Request ID: {random.randint(100000, 999999)} - Please ensure your response is properly formatted JSON]\n\n"
                            current_prompt = cache_bust_prefix + prompt
                            print(f"        Fallback retry (attempt {attempt + 1}/{effective_max_retries})...")

                        try:
                            response_obj = QueryWithBaseClient(fallback_client, new_cache_prompt_list, current_prompt, logfile, True, max_tokens, return_full_response=True)

                            if isinstance(response_obj, tuple):
                                result, ai_response = response_obj
                            else:
                                result = response_obj
                                ai_response = None

                            # Check if response is valid
                            is_problematic = False
                            error_reason = None
                            parsed_result = None

                            if not result:
                                is_problematic = True
                                error_reason = "empty_response"
                            else:
                                try:
                                    parsed_result = json.loads(result)

                                    if expected_keys is not None:
                                        missing_keys = []
                                        if isinstance(parsed_result, dict):
                                            missing_keys = [key for key in expected_keys if key not in parsed_result]
                                        elif isinstance(parsed_result, list):
                                            if expected_keys:
                                                is_problematic = True
                                                error_reason = f"expected_dict_with_keys_{expected_keys}_but_got_list"
                                        else:
                                            if expected_keys:
                                                is_problematic = True
                                                error_reason = f"expected_dict_with_keys_{expected_keys}_but_got_{type(parsed_result).__name__}"

                                        if missing_keys and not is_problematic:
                                            is_problematic = True
                                            error_reason = f"missing_required_keys: {missing_keys}"

                                except Exception as e:
                                    is_problematic = True
                                    error_reason = f"json_parse_error: {e}"

                            # If fallback succeeded, log and return
                            if not is_problematic and parsed_result is not None:
                                print(f"    SUCCESS with fallback model '{fallback_model_name}' after {attempt + 1} attempt(s)")

                                # Log fallback success
                                error_log_path = logfile.replace('.json', '_fallback.log') if logfile else 'fallback.log'
                                with open(error_log_path, 'a', encoding='utf-8') as fallback_log:
                                    fallback_log.write(f"\n{'='*70}\n")
                                    fallback_log.write(f"FALLBACK SUCCESS: {fallback_model_name}\n")
                                    fallback_log.write(f"Timestamp: {datetime.now(UTC)}\n")
                                    fallback_log.write(f"Primary model '{model_name}' failed after {primary_max_attempts} attempt(s)\n")
                                    fallback_log.write(f"Fallback model '{fallback_model_name}' succeeded after {attempt + 1} attempt(s)\n")
                                    fallback_log.write(f"Task: {task_name}\n")
                                    fallback_log.write(f"{'='*70}\n")

                                # Clean up failed cache entries
                                for i, failed in enumerate(failed_attempts):
                                    remove_cached_response(full_cache, failed['prompt'], model_name, max_tokens)

                                # Cache successful response under BOTH primary and fallback model keys.
                                # Primary key ensures re-runs hit cache immediately without retrying the failed primary request.
                                set_cached_response(full_cache, prompt, model_name, result, max_tokens)  # primary model key
                                set_cached_response(full_cache, prompt, fallback_model_actual, result, max_tokens)  # fallback key

                                return parsed_result

                            # Fallback attempt failed - continue retrying with this fallback model
                            if attempt < effective_max_retries - 1:
                                continue
                            else:
                                # Exhausted retries with this fallback - try next fallback model
                                print(f"        Fallback model '{fallback_model_name}' exhausted retries")
                                break

                        except Exception as e:
                            if attempt < effective_max_retries - 1:
                                print(f"        Fallback error on attempt {attempt + 1}: {e}. Retrying...")
                                continue
                            else:
                                print(f"        Fallback model '{fallback_model_name}' failed: {e}")
                                break

                except Exception as e:
                    print(f"    Failed to create fallback client for '{fallback_model_name}': {e}")
                    continue

    # All models (primary + fallbacks) exhausted - write comprehensive error log and raise
    error_log_path = logfile.replace('.json', '_error.log') if logfile else 'error.log'
    with open(error_log_path, 'a', encoding='utf-8') as error_log:
        error_log.write(f"\n{'='*70}\n")
        error_log.write(f"ERROR: Failed to get valid response after trying all models\n")
        error_log.write(f"Timestamp: {datetime.now(UTC)}\n")
        error_log.write(f"Primary model: {model_name} ({primary_max_attempts} attempt(s))\n")
        if config and task_name:
            fallback_models = get_fallback_models(config, task_name)
            if fallback_models:
                error_log.write(f"Fallback models tried: {fallback_models}\n")

        error_log.write(f"\n--- Primary Model Attempts ---\n")
        for i, failed in enumerate(failed_attempts):
            error_log.write(f"\nAttempt {i+1}:\n")
            error_log.write(f"  Reason: {failed['reason']}\n")
            error_log.write(f"  Stop reason: {failed['stop_reason']}\n")
            error_log.write(f"  Result length: {len(failed['result'])} chars\n")
            if failed['result']:
                error_log.write(f"  Result preview: {failed['result'][:200]}...\n")

        error_log.write(f"\n--- Prompt Information ---\n")
        error_log.write(f"Task: {task_name if task_name else 'unknown'}\n")
        error_log.write(f"Original query prompt length: {len(prompt)} characters\n")
        error_log.write(f"Cache prompt parts: {len(new_cache_prompt_list)}\n")

        # Calculate total cache size
        total_cache_size = sum(len(part) for part in new_cache_prompt_list)
        error_log.write(f"Total cache size: {total_cache_size} characters\n")
        error_log.write(f"Total prompt size: {total_cache_size + len(prompt)} characters\n")

        # Estimate token count (rough: 1 token ≈ 4 characters)
        estimated_tokens = (total_cache_size + len(prompt)) // 4
        error_log.write(f"Estimated tokens: ~{estimated_tokens}\n")

        error_log.write(f"\n--- Original Query Prompt (first 2000 chars) ---\n")
        error_log.write(f"{prompt[:2000]}\n")
        if len(prompt) > 2000:
            error_log.write(f"... (truncated, {len(prompt) - 2000} more characters)\n")

        error_log.write(f"\n--- Cache Cleanup ---\n")
        error_log.write(f"Removing {len(failed_attempts)} failed cache entries...\n")
        error_log.write(f"{'='*70}\n")

    # Clean up ALL failed cache entries
    for i, failed in enumerate(failed_attempts):
        removed = remove_cached_response(full_cache, failed['prompt'], model_name, max_tokens)
        print(f"    [Cache cleanup] Removed failed attempt {i+1}: {'Success' if removed else 'Not found'}")

    # Raise error with information about the failure
    last_failure = failed_attempts[-1] if failed_attempts else {'reason': 'unknown'}
    raise ModelError(
        f"Failed to get valid JSON response after trying all models. "
        f"Primary model: {model_name}. "
        f"Last error: {last_failure['reason']}. "
        f"Details written to {error_log_path}"
    )

def query_text_with_retry(ai_client, cache_prompt_list: list, prompt: str, logfile: str, max_tokens: int = 0, max_retries: int = 3, config: dict = None, task_name: str = None) -> str:
    """
    Send a prompt to the AI model and return plain text response with retry logic.

    Similar to query_json() but returns plain text instead of parsed JSON.
    Includes automatic retry with cache-busting for empty responses and optional
    fallback to alternative models if the primary model fails.

    Args:
        ai_client: BaseAIClient instance for making queries
        cache_prompt_list (list): A list of static portions of the prompt, each a string
        prompt (str): The prompt to send to the AI model
        logfile (str): The logfile to write logging information to
        max_tokens (int): Maximum tokens for response (0 uses config default)
        max_retries (int): Maximum number of retry attempts for empty responses (default: 3)
                          NOTE: If config and task_name provided, this uses get_max_retries_per_model(config) instead
        config (dict): Optional configuration dictionary (enables fallback model support)
        task_name (str): Optional task name (e.g., 'stage3.summary.organizational')
                        Required for fallback model support

    Returns:
        str: Plain text response from the model

    Raises:
        ModelError: If the model fails to respond or returns empty response after all retries
                   (including fallback models if configured)

    Notes:
        - If config and task_name are provided, fallback models will be tried after primary model exhausts retries
        - Fallback models are determined by get_fallback_models(config, task_name)
        - Each model (primary and fallbacks) gets max_retries_per_model attempts
        - Logs which model succeeded for quality tracking
    """
    # Reconstruct cache_prompt by combining potentially smaller parts (same as query_json)
    new_cache_prompt_list = []
    for part in cache_prompt_list:
        if not isinstance(part, str):
            raise InputError('Call to query_text_with_retry with malformed cache_prompt_list.')
        if 0 == len(new_cache_prompt_list):
            new_cache_prompt_list.append(part)
        else:
            last_item = new_cache_prompt_list[-1]
            if len(last_item) > 4500:  # Based on estimate for 1024 tokens
                new_cache_prompt_list.append(part)
            else:  # Last item is too short for cache, so add new item to it
                new_cache_prompt_list.pop()
                new_cache_prompt_list.append(last_item + part)
    
    # Anthropic API allows maximum of 4 cache_control blocks
    MAX_CACHE_BLOCKS = 4
    if len(new_cache_prompt_list) > MAX_CACHE_BLOCKS:
        excess_blocks = len(new_cache_prompt_list) - MAX_CACHE_BLOCKS
        num_to_combine = excess_blocks + 1
        superblock = ''.join(new_cache_prompt_list[:num_to_combine])
        new_cache_prompt_list = [superblock] + new_cache_prompt_list[num_to_combine:]

    # Determine effective max_retries (use config value if provided)
    if config and task_name:
        from .config import get_max_retries_per_model, get_fallback_models
        effective_max_retries = get_max_retries_per_model(config)
        # Check if fallback models are configured - if so, primary model only gets 1 attempt
        fallback_models = get_fallback_models(config, task_name)
        primary_max_attempts = 1 if fallback_models else effective_max_retries
        # Warn if no fallback models configured (potential config issue)
        if not fallback_models:
            print(f"    WARNING: No fallback models for task={task_name}. Config retry section: {config.get('retry', 'MISSING')}")
    else:
        effective_max_retries = max_retries
        primary_max_attempts = max_retries
        fallback_models = []
        # Warn when config/task_name not provided (fallbacks disabled)
        if task_name:
            print(f"    WARNING: Fallbacks disabled - config not provided for task={task_name}")

    # Track failed attempts for cleanup later
    failed_attempts = []
    full_cache = ''.join(new_cache_prompt_list)
    model_name = getattr(ai_client, 'model', 'unknown')

    # Retry loop for handling empty responses (primary model)
    # If fallback models are configured, primary only gets 1 attempt
    for attempt in range(primary_max_attempts):
        # For retry attempts, create a cache-busting variation of the prompt
        if attempt == 0:
            current_prompt = prompt
        else:
            # Add cache-busting variations with randomness AT THE BEGINNING
            cache_bust_prefix = f"[Request ID: {random.randint(100000, 999999)} - Please provide a response]\n\n"
            current_prompt = cache_bust_prefix + prompt
            print(f"    WARNING: Retrying query (attempt {attempt + 1}/{effective_max_retries}) with cache-busting variation...")
        
        try:
            response_obj = QueryWithBaseClient(ai_client, new_cache_prompt_list, current_prompt, logfile, False, max_tokens, return_full_response=True)
            
            # Handle both old (string) and new (tuple) return formats
            if isinstance(response_obj, tuple):
                result, ai_response = response_obj
            else:
                result = response_obj
                ai_response = None
            
            # Check if response is empty
            if result and str(result).strip():
                # Always save successful response under the original prompt key
                # This works with the earlier fix that saves successful retries under original key:
                # - If attempt 0 succeeded: QueryWithBaseClient() already cached it under original key,
                #   and set_cached_response() will skip (key exists) - this is fine, value is correct
                # - If attempt > 0 succeeded: We remove the bad attempt 0 entry (if any), then save
                #   the successful retry response under original key for future cache hits
                
                # If this was a retry (attempt > 0), we need to ensure the original key has the correct value
                # First, remove any bad cache entry from attempt 0 (if it was cached before we detected it was problematic)
                if attempt > 0:
                    # Remove the bad attempt 0 entry (if it exists) so we can save the good retry response
                    remove_cached_response(full_cache, prompt, model_name, max_tokens)
                    # Also remove the retry key entry (with cache-busting prefix) since it's not useful
                    remove_cached_response(full_cache, current_prompt, model_name, max_tokens)
                
                # Save successful response under the original prompt key
                # (This will write because we removed it above if attempt > 0, or skip if attempt 0 and already cached)
                set_cached_response(full_cache, prompt, model_name, result, max_tokens)
                
                if failed_attempts:
                    # We had failures but eventually succeeded - clean up bad cache entries
                    error_log_path = logfile.replace('.json', '_error.log') if logfile else 'error.log'
                    with open(error_log_path, 'a', encoding='utf-8') as error_log:
                        error_log.write(f"\n{'='*70}\n")
                        error_log.write(f"RETRY SUCCESS: Query succeeded after {attempt} failed attempt(s)\n")
                        error_log.write(f"Timestamp: {datetime.now(UTC)}\n")
                        error_log.write(f"Model: {model_name}\n")
                        error_log.write(f"\nCleaning up {len(failed_attempts)} bad cache entries...\n")
                        
                        for i, failed in enumerate(failed_attempts):
                            removed = remove_cached_response(full_cache, failed['prompt'], model_name, max_tokens)
                            error_log.write(f"  Attempt {i+1}: {'Removed' if removed else 'Not found'} - Reason: {failed['reason']}\n")
                        
                        error_log.write(f"\nSuccessful prompt variation: {current_prompt[:200]}...\n")
                        if attempt > 0:
                            error_log.write(f"Response saved under original prompt key for future cache hits.\n")
                        error_log.write(f"{'='*70}\n")
                    
                    print(f"    SUCCESS: Query succeeded after {attempt} retry attempt(s). Bad cache entries cleaned.")
                    if attempt > 0:
                        print(f"    Response saved under original prompt key for future cache hits.")
                
                return str(result).strip()
            
            # This attempt failed - record it
            failed_attempts.append({
                'prompt': current_prompt,
                'result': str(result) if result else '',
                'reason': 'empty_response',
                'stop_reason': ai_response.stop_reason if ai_response else 'unknown'
            })
            
            # CRITICAL: If this was attempt 0 (first attempt) and it failed, we need to clean up
            # the cache entry that QueryWithBaseClient() created before we detected it was problematic.
            # QueryWithBaseClient() caches responses immediately, so an empty response from attempt 0
            # would already be cached under the original prompt key.
            if attempt == 0:
                # Remove the bad cache entry from attempt 0 so retries don't hit it
                remove_cached_response(full_cache, current_prompt, model_name, max_tokens)
                print(f"    [Cache cleanup] Removed empty response from attempt 0 before retrying...")
            
            # If this is not the last attempt, continue to retry
            if attempt < primary_max_attempts - 1:
                continue

            # This was the last attempt with primary model and it failed
            # If fallback models are configured, break immediately to try them
            break

        except ModelError as e:
            # ModelError from QueryWithBaseClient - record it and continue with retries or fallback
            # Don't re-raise immediately - we want to try fallback models if configured
            failed_attempts.append({
                'prompt': current_prompt,
                'result': '',
                'reason': f"model_error: {e}",
                'stop_reason': 'error'
            })

            if attempt < primary_max_attempts - 1:
                print(f"    WARNING: ModelError (attempt {attempt + 1}/{primary_max_attempts}): {e}")
                continue
            else:
                # Last attempt with primary model - break out to try fallback models
                break
        except Exception as e:
            # Unexpected error - log and potentially retry
            failed_attempts.append({
                'prompt': current_prompt,
                'result': '',
                'reason': f"unexpected_error: {e}",
                'stop_reason': 'error'
            })

            if attempt < primary_max_attempts - 1:
                print(f"    WARNING: Unexpected error (attempt {attempt + 1}/{primary_max_attempts}): {e}")
                continue
            else:
                # Last attempt with primary model - break out to try fallback models
                break

    # Primary model exhausted attempts - try fallback models if configured
    if config and task_name:
        # create_ai_client is defined in this module (utils/ai_client.py), not in utils/config.py
        # fallback_models already retrieved above if config/task_name provided

        if fallback_models:
            print(f"    Primary model '{model_name}' failed after {primary_max_attempts} attempt(s). Immediately trying fallback models: {fallback_models}")

            for fallback_model_name in fallback_models:
                print(f"    Attempting fallback model: {fallback_model_name}")

                try:
                    # Create client for fallback model
                    fallback_client = create_ai_client(model_name=fallback_model_name, config=config)
                    fallback_model_actual = getattr(fallback_client, 'model', fallback_model_name)

                    # Try fallback model with same retry logic
                    for attempt in range(effective_max_retries):
                        if attempt == 0:
                            current_prompt = prompt
                        else:
                            cache_bust_prefix = f"[Request ID: {random.randint(100000, 999999)} - Please provide a response]\n\n"
                            current_prompt = cache_bust_prefix + prompt
                            print(f"        Fallback retry (attempt {attempt + 1}/{effective_max_retries})...")

                        try:
                            response_obj = QueryWithBaseClient(fallback_client, new_cache_prompt_list, current_prompt, logfile, False, max_tokens, return_full_response=True)

                            if isinstance(response_obj, tuple):
                                result, ai_response = response_obj
                            else:
                                result = response_obj
                                ai_response = None

                            # Check if response is valid (not empty)
                            if result and str(result).strip():
                                print(f"    SUCCESS with fallback model '{fallback_model_name}' after {attempt + 1} attempt(s)")

                                # Log fallback success
                                error_log_path = logfile.replace('.json', '_fallback.log') if logfile else 'fallback.log'
                                with open(error_log_path, 'a', encoding='utf-8') as fallback_log:
                                    fallback_log.write(f"\n{'='*70}\n")
                                    fallback_log.write(f"FALLBACK SUCCESS: {fallback_model_name}\n")
                                    fallback_log.write(f"Timestamp: {datetime.now(UTC)}\n")
                                    fallback_log.write(f"Primary model '{model_name}' failed after {primary_max_attempts} attempt(s)\n")
                                    fallback_log.write(f"Fallback model '{fallback_model_name}' succeeded after {attempt + 1} attempt(s)\n")
                                    fallback_log.write(f"Task: {task_name}\n")
                                    fallback_log.write(f"{'='*70}\n")

                                # Clean up failed cache entries
                                for i, failed in enumerate(failed_attempts):
                                    remove_cached_response(full_cache, failed['prompt'], model_name, max_tokens)

                                # Cache successful response under BOTH primary and fallback model keys.
                                # Primary key ensures re-runs hit cache immediately without retrying the failed primary request.
                                set_cached_response(full_cache, prompt, model_name, str(result).strip(), max_tokens)  # primary model key
                                set_cached_response(full_cache, prompt, fallback_model_actual, str(result).strip(), max_tokens)  # fallback key

                                return str(result).strip()

                            # Fallback attempt failed - continue retrying with this fallback model
                            if attempt < effective_max_retries - 1:
                                continue
                            else:
                                # Exhausted retries with this fallback - try next fallback model
                                print(f"        Fallback model '{fallback_model_name}' exhausted retries")
                                break

                        except Exception as e:
                            if attempt < effective_max_retries - 1:
                                print(f"        Fallback error on attempt {attempt + 1}: {e}. Retrying...")
                                continue
                            else:
                                print(f"        Fallback model '{fallback_model_name}' failed: {e}")
                                break

                except Exception as e:
                    print(f"    Failed to create fallback client for '{fallback_model_name}': {e}")
                    continue

    # All models (primary + fallbacks) exhausted - write comprehensive error log and raise
    error_log_path = logfile.replace('.json', '_error.log') if logfile else 'error.log'
    with open(error_log_path, 'a', encoding='utf-8') as error_log:
        error_log.write(f"\n{'='*70}\n")
        error_log.write(f"ERROR: Failed to get valid text response after trying all models\n")
        error_log.write(f"Timestamp: {datetime.now(UTC)}\n")
        error_log.write(f"Primary model: {model_name} ({primary_max_attempts} attempt(s))\n")
        if config and task_name:
            fallback_models = get_fallback_models(config, task_name)
            if fallback_models:
                error_log.write(f"Fallback models tried: {fallback_models}\n")

        error_log.write(f"\n--- Primary Model Attempts ---\n")
        for i, failed in enumerate(failed_attempts):
            error_log.write(f"\nAttempt {i+1}:\n")
            error_log.write(f"  Reason: {failed['reason']}\n")
            error_log.write(f"  Stop reason: {failed['stop_reason']}\n")
            error_log.write(f"  Result length: {len(failed['result'])} chars\n")

        error_log.write(f"\n--- Prompt Information ---\n")
        error_log.write(f"Task: {task_name if task_name else 'unknown'}\n")
        error_log.write(f"Original query prompt length: {len(prompt)} characters\n")
        error_log.write(f"Cache prompt parts: {len(new_cache_prompt_list)}\n")

        total_cache_size = sum(len(part) for part in new_cache_prompt_list)
        error_log.write(f"Total cache size: {total_cache_size} characters\n")

        error_log.write(f"\n--- Cache Cleanup ---\n")
        error_log.write(f"Removing {len(failed_attempts)} failed cache entries...\n")
        error_log.write(f"{'='*70}\n")

    # Clean up ALL failed cache entries
    for i, failed in enumerate(failed_attempts):
        removed = remove_cached_response(full_cache, failed['prompt'], model_name, max_tokens)
        print(f"    [Cache cleanup] Removed failed attempt {i+1}: {'Success' if removed else 'Not found'}")

    # Raise error with information about the failure
    last_failure = failed_attempts[-1] if failed_attempts else {'reason': 'unknown'}
    raise ModelError(
        f"Failed to get valid text response after trying all models. "
        f"Primary model: {model_name}. "
        f"Last error: {last_failure['reason']}. "
        f"Details written to {error_log_path}"
    )


def extract_json_from_response(response_text):
    """
    Extract JSON from AI response, handling cases where explanatory text is included.
    
    This function tries multiple strategies to extract valid JSON from the response:
    1. Look for JSON code blocks (```json ... ```)
    2. Find the last occurrence of valid JSON in the text
    3. Use regex to extract JSON with proper quote handling
    
    Args:
        response_text (str): The full response from the AI model
        
    Returns:
        str: Extracted JSON string, or empty string if no valid JSON found
    """
    if not response_text:
        return ''
    
    # Strategy 1: Look for JSON code blocks
    json_block_match = re.search(r'```(?:json)?\s*(\[.*?\]|{.*?})\s*```', response_text, re.DOTALL)
    if json_block_match:
        return json_block_match.group(1)
    
    # Strategy 2: Find the last occurrence of what looks like JSON
    # Look for arrays or objects at the end of the text
    json_patterns = [
        r'(\[.*?\])\s*$',  # Array at end
        r'(\{.*?\})\s*$',  # Object at end
        r'(\[.*?\])',      # Any array
        r'(\{.*?\})',      # Any object
    ]
    
    for pattern in json_patterns:
        matches = list(re.finditer(pattern, response_text, re.DOTALL))
        if matches:
            # Try the last match first, then work backwards
            for match in reversed(matches):
                candidate = match.group(1)
                try:
                    # Test if it's valid JSON
                    json.loads(candidate)
                    return candidate
                except json.JSONDecodeError:
                    continue
    
    # Strategy 3: Try to extract JSON using the original logic but with better quote handling
    square_position = response_text.find(r'[')
    curly_position = response_text.find(r'{')
    
    if square_position > -1 and (curly_position < 0 or square_position < curly_position):
        # Look for array
        regex_out = re.search(r'^[^\[\]]*(\[.*?\])\s*$', response_text, re.DOTALL)
    elif curly_position > -1 and (square_position < 0 or curly_position < square_position):
        # Look for object
        regex_out = re.search(r'^[^{}]*(\{.*?\})\s*$', response_text, re.DOTALL)
    else:
        return ''
    
    if regex_out and regex_out.group(1):
        extracted = regex_out.group(1)
        # Clean up common issues
        extracted = extracted.replace('\\"', '"')  # Fix escaped quotes
        extracted = extracted.replace('\\n', '')   # Remove escaped newlines
        extracted = extracted.replace('\\t', '')   # Remove escaped tabs
        try:
            # Test if it's valid JSON after cleanup
            json.loads(extracted)
            return extracted
        except json.JSONDecodeError:
            pass
    
    return ''


class AIMessage:
    """Standardized message format"""
    def __init__(self, role: str, cache: Any, content: Any):
        self.role = role
        self.cache = cache
        self.content = content


class ToolCall:
    """Standardized tool call format"""
    def __init__(self, id: str, name: str, input: Dict[str, Any]):
        self.id = id
        self.name = name
        self.input = input


class AIResponse:
    """Standardized AI response format"""
    def __init__(self, content: str, tool_calls: List[ToolCall] = None, cache_created: int=0, cache_read: int=0, stop_reason: str = None):
        self.content = content
        self.tool_calls = tool_calls or []
        self.cache_created = cache_created
        self.cache_read = cache_read
        self.stop_reason = stop_reason


class BaseAIClient(ABC):
    """Abstract base class for AI clients"""
    
    @abstractmethod
    def create_message(self, messages: List[AIMessage], tools: List[Dict], max_tokens: int = 1000) -> AIResponse:
        pass
    
    @abstractmethod
    def format_tool_result(self, tool_call_id: str, result: Dict[str, Any]) -> AIMessage:
        pass


class AnthropicClient(BaseAIClient):
    """Anthropic Claude client implementation"""
    
    def __init__(self, api_key: str, model: str):
        self.client = anthropic.Anthropic(api_key=api_key)
        self.model = model
    
    def create_message(self, messages: List[AIMessage], tools: List[Dict], max_tokens: int = 0) -> AIResponse:
        # Convert to Anthropic format
        if max_tokens <= 0:
            config = get_config()
            model_cfg = config.get('models', {}).get(self.model, {})
            max_tokens = model_cfg.get('max_tokens', 8000)

        # Extract system message and user messages separately for Claude
        system_messages = ''
        msg_content = []

        # Anthropic API allows maximum of 4 cache_control blocks
        MAX_CACHE_BLOCKS = 4
        cache_blocks_added = 0

        for msg in messages:
            if msg.role == "system":
                if not '' == msg.cache:
                    if len(msg.cache) > 4500: # Based on measurements that show 4.3 - 4.8 characters per token, shooting for 1024 tokens.
                        # Only add cache_control if we haven't reached the limit
                        if cache_blocks_added < MAX_CACHE_BLOCKS:
                            msg_content.append({
                                "type": "text",
                                "text": msg.cache,
                                "cache_control": {"type": "ephemeral"}
                                })
                            cache_blocks_added += 1
                        else:
                            # Don't add cache_control to blocks beyond the 4th
                            msg_content.append({
                                "type": "text",
                                "text": msg.cache
                                })
                    else:
                        msg_content.append({
                            "type": "text",
                            "text": msg.cache
                            })
                else:
                    system_messages = msg.content
        for msg in messages:
            if msg.role == "user":
                msg_content.append({
                    "type": "text",
                    "text": msg.content
                })
        user_messages = [
                    {
                        "role": "user",
                        "content": msg_content
                    }
                ]

        # Determine if streaming is required
        # Anthropic requires streaming for requests that may take longer than 10 minutes
        # This typically happens when max_tokens >= 4096, so we use a conservative threshold
        config = get_config()
        default_max = config.get('models', {}).get(self.model, {}).get('max_tokens', 8000)
        use_streaming = (max_tokens >=10000) or (max_tokens > default_max)

        if use_streaming:
            # Use streaming for large requests
            def _make_api_call():
                stream = self.client.messages.create(
                    model=self.model,
                    max_tokens=max_tokens,
                    system=system_messages,
                    tools=tools,
                    messages=user_messages,
                    stream=True
                )
                
                # Collect streaming chunks
                content_parts = []
                tool_calls_dict = {}  # Track tool calls by ID
                stop_reason = None
                usage = None
                current_tool_id = None
                current_tool_input_parts = {}
                
                for event in stream:
                    # Handle different event types
                    if event.type == 'content_block_start':
                        if hasattr(event, 'content_block'):
                            if event.content_block.type == 'tool_use':
                                tool_use = event.content_block
                                current_tool_id = tool_use.id
                                tool_calls_dict[current_tool_id] = {
                                    'id': tool_use.id,
                                    'name': tool_use.name,
                                    'input': tool_use.input if hasattr(tool_use, 'input') else {}
                                }
                                current_tool_input_parts[current_tool_id] = {}
                    elif event.type == 'content_block_delta':
                        if hasattr(event, 'delta'):
                            if event.delta.type == 'text_delta' and hasattr(event.delta, 'text'):
                                content_parts.append(event.delta.text)
                            elif event.delta.type == 'input_json_delta' and hasattr(event.delta, 'partial_json'):
                                # Handle partial JSON for tool inputs (if needed)
                                if current_tool_id:
                                    if current_tool_id not in current_tool_input_parts:
                                        current_tool_input_parts[current_tool_id] = ""
                                    current_tool_input_parts[current_tool_id] += event.delta.partial_json
                    elif event.type == 'content_block_stop':
                        current_tool_id = None
                    elif event.type == 'message_delta':
                        # Update stop reason if provided
                        if hasattr(event, 'delta') and hasattr(event.delta, 'stop_reason'):
                            stop_reason = event.delta.stop_reason
                    elif event.type == 'message_stop':
                        # Final event, extract usage if available
                        if hasattr(event, 'usage'):
                            usage = event.usage
                    elif event.type == 'message':
                        # Complete message event (contains usage and stop_reason)
                        if hasattr(event, 'usage'):
                            usage = event.usage
                        if hasattr(event, 'stop_reason'):
                            stop_reason = event.stop_reason
                
                # Reconstruct response object-like structure
                content = ''.join(content_parts)
                
                # Convert tool_calls_dict to list of ToolCall objects
                tool_calls = []
                for tool_call_data in tool_calls_dict.values():
                    tool_calls.append(ToolCall(
                        id=tool_call_data['id'],
                        name=tool_call_data['name'],
                        input=tool_call_data['input']
                    ))
                
                # Extract usage information
                cache_created = 0
                cache_read = 0
                if usage:
                    cache_created = getattr(usage, 'cache_creation_input_tokens', 0)
                    cache_read = getattr(usage, 'cache_read_input_tokens', 0)
                
                # Create a response-like object for compatibility
                class StreamingResponse:
                    def __init__(self, content, tool_calls, cache_created, cache_read, stop_reason):
                        self.content = [type('Block', (), {'type': 'text', 'text': content})()]
                        self.usage = type('Usage', (), {
                            'cache_creation_input_tokens': cache_created,
                            'cache_read_input_tokens': cache_read
                        })()
                        self.stop_reason = stop_reason
                        self._tool_calls = tool_calls
                
                return StreamingResponse(content, tool_calls, cache_created, cache_read, stop_reason)
            
            response = make_api_call_with_retry(_make_api_call)
        else:
            # Use non-streaming for smaller requests
            # But catch ValueError in case SDK still requires streaming
            def _make_api_call():
                try:
                    return self.client.messages.create(
                        model=self.model,
                        max_tokens=max_tokens,
                        system=system_messages,
                        tools=tools,
                        messages=user_messages
                    )
                except ValueError as e:
                    # If SDK requires streaming, retry with streaming
                    if "streaming is required" in str(e).lower():
                        # Fall back to streaming
                        stream = self.client.messages.create(
                            model=self.model,
                            max_tokens=max_tokens,
                            system=system_messages,
                            tools=tools,
                            messages=user_messages,
                            stream=True
                        )
                        
                        # Collect streaming chunks (same logic as above)
                        content_parts = []
                        tool_calls_dict = {}
                        stop_reason = None
                        usage = None
                        current_tool_id = None
                        current_tool_input_parts = {}
                        
                        for event in stream:
                            if event.type == 'content_block_start':
                                if hasattr(event, 'content_block'):
                                    if event.content_block.type == 'tool_use':
                                        tool_use = event.content_block
                                        current_tool_id = tool_use.id
                                        tool_calls_dict[current_tool_id] = {
                                            'id': tool_use.id,
                                            'name': tool_use.name,
                                            'input': tool_use.input if hasattr(tool_use, 'input') else {}
                                        }
                                        current_tool_input_parts[current_tool_id] = {}
                            elif event.type == 'content_block_delta':
                                if hasattr(event, 'delta'):
                                    if event.delta.type == 'text_delta' and hasattr(event.delta, 'text'):
                                        content_parts.append(event.delta.text)
                                    elif event.delta.type == 'input_json_delta' and hasattr(event.delta, 'partial_json'):
                                        if current_tool_id:
                                            if current_tool_id not in current_tool_input_parts:
                                                current_tool_input_parts[current_tool_id] = ""
                                            current_tool_input_parts[current_tool_id] += event.delta.partial_json
                            elif event.type == 'content_block_stop':
                                current_tool_id = None
                            elif event.type == 'message_delta':
                                if hasattr(event, 'delta') and hasattr(event.delta, 'stop_reason'):
                                    stop_reason = event.delta.stop_reason
                            elif event.type == 'message_stop':
                                if hasattr(event, 'usage'):
                                    usage = event.usage
                            elif event.type == 'message':
                                if hasattr(event, 'usage'):
                                    usage = event.usage
                                if hasattr(event, 'stop_reason'):
                                    stop_reason = event.stop_reason
                        
                        content = ''.join(content_parts)
                        tool_calls = []
                        for tool_call_data in tool_calls_dict.values():
                            tool_calls.append(ToolCall(
                                id=tool_call_data['id'],
                                name=tool_call_data['name'],
                                input=tool_call_data['input']
                            ))
                        
                        cache_created = 0
                        cache_read = 0
                        if usage:
                            cache_created = getattr(usage, 'cache_creation_input_tokens', 0)
                            cache_read = getattr(usage, 'cache_read_input_tokens', 0)
                        
                        class StreamingResponse:
                            def __init__(self, content, tool_calls, cache_created, cache_read, stop_reason):
                                self.content = [type('Block', (), {'type': 'text', 'text': content})()]
                                self.usage = type('Usage', (), {
                                    'cache_creation_input_tokens': cache_created,
                                    'cache_read_input_tokens': cache_read
                                })()
                                self.stop_reason = stop_reason
                                self._tool_calls = tool_calls
                        
                        return StreamingResponse(content, tool_calls, cache_created, cache_read, stop_reason)
                    else:
                        # Re-raise if it's a different ValueError
                        raise

            response = make_api_call_with_retry(_make_api_call)

        # Extract text content
        text_blocks = [block for block in response.content if block.type == 'text']
        content = text_blocks[0].text if text_blocks else ""

        # Extract usage information
        usage = response.usage
        cache_created = getattr(usage, 'cache_creation_input_tokens', 0)
        cache_read = getattr(usage, 'cache_read_input_tokens', 0)

        # Extract stop reason
        stop_reason = getattr(response, 'stop_reason', None)

        # Extract tool calls
        tool_calls = []
        # Check if response has _tool_calls attribute (streaming response)
        if hasattr(response, '_tool_calls'):
            tool_calls = response._tool_calls
        else:
            # For non-streaming, extract from response content
            for block in response.content:
                if block.type == 'tool_use':
                    tool_calls.append(ToolCall(
                        id=block.id,
                        name=block.name,
                        input=block.input
                    ))

        return AIResponse(content=content, tool_calls=tool_calls, cache_created=cache_created, cache_read=cache_read, stop_reason=stop_reason)
    
    def format_tool_result(self, tool_call_id: str, result: Dict[str, Any]) -> AIMessage:
        return AIMessage(
            role="user",
            content=[{
                "type": "tool_result",
                "tool_use_id": tool_call_id,
                "content": json.dumps(result)
            }]
        )

def get_cached_tokens(usage):
    """Safely extract cached tokens from usage object"""
    try:
        prompt_tokens_details = getattr(usage, 'prompt_tokens_details', None)
        if prompt_tokens_details is None:
            return 0
        
        # Try different possible attribute names
        if hasattr(prompt_tokens_details, 'cached_tokens'):
            return prompt_tokens_details.cached_tokens
        elif hasattr(prompt_tokens_details, 'cached'):
            return prompt_tokens_details.cached
        else:
            print(f"DEBUG: Full object: {prompt_tokens_details}")
            return 0
            
    except Exception as e:
        print(f"DEBUG: Error getting cached tokens: {e}")
        return 0

class OpenAIClient(BaseAIClient):
    """OpenAI client implementation"""
    
    def __init__(self, api_key: str, model: str):
        self.client = openai.OpenAI(api_key=api_key)
        self.model = model
          
    def create_message(self, messages: List[AIMessage], tools: List[Dict], max_tokens: int = 0) -> AIResponse:
        # Convert to OpenAI format
        if max_tokens <= 0:
            config = get_config()
            model_cfg = config.get('models', {}).get(self.model, {})
            max_tokens = model_cfg.get('max_tokens', 8000)
        openai_messages = []
        # Get full cache and system message.
        full_cache = ''
        system_message = ''
        user_prompt = ''
        for msg in messages:
            if msg.role == "system" and isinstance(msg.content, str):
                if not '' == msg.cache:
                    full_cache += msg.cache
                else:
                    system_message = msg.content
            elif msg.role == "user" and isinstance(msg.content, str):
                user_prompt += msg.content
        if not '' == system_message:
            openai_messages.append({
                "role": "system",
                "content": system_message
            })
        if not '' == full_cache:
            openai_messages.append({
                "role": "system",
                "content": full_cache
            })
        if not '' == user_prompt:
            openai_messages.append({
                "role": "user",
                "content": user_prompt
            })

        # Convert tools to OpenAI format
        openai_tools = []
        for tool in tools:
            openai_tools.append({
                "type": "function",
                "function": {
                    "name": tool["name"],
                    "description": tool["description"],
                    "parameters": tool["input_schema"]
                }
            })
               
        # Use max_completion_tokens for OpenAI (matching old Query function behavior)
        # Wrap API call with retry logic
        def _make_api_call():
            return self.client.chat.completions.create(
                model=self.model,
                max_completion_tokens=max_tokens,
                tools=openai_tools,
                messages=openai_messages
            )

        response = make_api_call_with_retry(_make_api_call)

        message = response.choices[0].message
        content = message.content or ""

        # Extract stop reason (OpenAI calls it finish_reason)
        stop_reason = getattr(response.choices[0], 'finish_reason', None)

        # Extract tool calls
        tool_calls = []
        if message.tool_calls:
            for tc in message.tool_calls:
                tool_calls.append(ToolCall(
                    id=tc.id,
                    name=tc.function.name,
                    input=json.loads(tc.function.arguments)
                ))

        # Extract usage information
        usage = response.usage
        cached = get_cached_tokens(usage)

        return AIResponse(content=content, tool_calls=tool_calls, cache_created=0, cache_read=cached, stop_reason=stop_reason)
    
    def format_tool_result(self, tool_call_id: str, result: Dict[str, Any]) -> AIMessage:
        return AIMessage(
            role="tool",
            content=json.dumps(result)
        )


def GetClient(AI_engine=''):
    """
    DEPRECATED: Use create_ai_client() instead.

    Creates a raw API client for the given platform.
    AI_engine options: Claude, Azure, OpenAI
    """
    warnings.warn(
        "GetClient is deprecated. Use create_ai_client() instead.",
        DeprecationWarning,
        stacklevel=2
    )
    config = get_config()
    if '' == AI_engine:
        # Map current_engine (model name) to platform
        current_model = config.get('current_engine', '')
        model_cfg = config.get('models', {}).get(current_model, {})
        AI_engine = model_cfg.get('platform', '')
        if not AI_engine:
            print('Error: current_engine not found in models configuration.')
            exit(1)

    client = 0
    if 'Claude' == AI_engine:
        api_key = secrets['anthropic_api_key']
        client = anthropic.Anthropic(api_key=api_key)
    elif 'Azure' == AI_engine:
        api_key = secrets['azure_openai_api_key']
        # Find Azure model config
        azure_config = None
        for m_config in config.get('models', {}).values():
            if m_config.get('platform') == 'Azure':
                azure_config = m_config
                break
        if not azure_config:
            print('Error: No Azure model configured.')
            exit(1)
        api_version = azure_config.get('api_version', '2024-05-01-preview')
        azure_endpoint = secrets['azure_openai_endpoint']
        client = AzureOpenAI(api_key=api_key, api_version=api_version, azure_endpoint=azure_endpoint)
    elif 'OpenAI' == AI_engine:
        api_key = secrets['openai_api_key']
        client = openai.OpenAI(api_key=api_key)
    else:
        print('Invalid engine.')
        exit(1)
    print("Client setup complete.\n")
    return client


def create_ai_client(model_name='', config: Optional[Dict[str, Any]] = None) -> 'BaseAIClient':
    """
    Factory function to create AI clients using model names.

    Args:
        model_name: Model name (e.g., 'gpt-5-nano', 'claude-sonnet-4-5').
                    If empty, uses current_engine from config.
        config: Optional configuration dictionary. If not provided, loads from config.json.

    Returns:
        BaseAIClient instance
    """
    from .config import get_model_config

    active_config = config if config is not None else get_config()

    if not model_name:
        model_name = active_config.get('current_engine', '')
        if not model_name:
            raise ValueError("No model_name provided and no current_engine in configuration")

    model_config = get_model_config(active_config, model_name)
    
    # Extract configuration values
    platform = model_config['platform']
    api_model_name = model_config['model']
    max_tokens = model_config.get('max_tokens', 8000)
    
    # Create client based on platform
    platform_lower = platform.lower()
    
    if platform_lower == "claude":
        api_key = secrets['anthropic_api_key']
        return AnthropicClient(api_key, api_model_name)
    elif platform_lower == "openai":
        api_key = secrets['openai_api_key']
        return OpenAIClient(api_key, api_model_name)
    elif platform_lower == "azure":
        # Azure still uses the old AzureOpenAI client (not BaseAIClient)
        # This is a known limitation - Azure support may need to be added later
        api_key = secrets['azure_openai_api_key']
        api_version = model_config.get('api_version', '2024-05-01-preview')
        azure_endpoint = secrets['azure_openai_endpoint']
        # Note: AzureOpenAI is not a BaseAIClient, so this will cause type issues
        # For now, we'll return it but the type checker will complain
        return AzureOpenAI(api_key=api_key, api_version=api_version, azure_endpoint=azure_endpoint)  # type: ignore
    else:
        raise ValueError(f"Unsupported platform: {platform}")