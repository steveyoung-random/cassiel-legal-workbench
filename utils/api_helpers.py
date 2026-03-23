# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License
"""
Higher-level AI query helpers for Document Analyzer.

Provides query_with_tools() — a multi-turn tool-call loop that supports both
OpenAI and Anthropic clients.  The caller supplies a list of tool definitions
(in Anthropic format) and a resolver callable; this module handles the provider-
specific wire format and the back-and-forth until the model produces a final
text answer.
"""

import json
import time
import random
from datetime import datetime, UTC
from typing import Callable, Dict, List, Optional


# Maximum tool-call rounds before giving up and returning whatever text is available
_MAX_TOOL_ROUNDS = 10


def query_with_tools(
    ai_client,
    cache_prompt_list: list,
    prompt: str,
    tools: list,
    tool_resolver: Callable[[str, dict], str],
    logfile: str,
    max_tokens: int = 0,
    config: dict = None,
    task_name: str = None,
) -> str:
    """
    Send a prompt to the AI model with callable tools and return the final text response.

    Handles the multi-turn tool-call loop: after receiving a response that contains
    tool calls, each call is resolved locally via tool_resolver, the results are fed
    back to the model, and the process repeats until the model produces a text answer
    (stop_reason != 'tool_use' / 'tool_calls') or _MAX_TOOL_ROUNDS is reached.

    Results are NOT cached (tool calls are stateful/interactive).

    Args:
        ai_client: BaseAIClient instance (AnthropicClient or OpenAIClient)
        cache_prompt_list: Static context strings (same as query_json / query_text_with_retry)
        prompt: The user query
        tools: List of tool definitions in Anthropic format:
               [{"name": str, "description": str, "input_schema": {"type": "object", "properties": {...}}}]
        tool_resolver: Callable(tool_name: str, tool_input: dict) -> str
                       Returns the tool result as a plain string.
        logfile: Path to log file
        max_tokens: Maximum tokens for each response (0 = use config default)
        config: Configuration dictionary (optional)
        task_name: Task name for model routing (optional)

    Returns:
        str: Final text response from the model after all tool calls resolved.
             Returns '' if no text was produced.
    """
    from .ai_client import make_api_call_with_retry, AnthropicClient, OpenAIClient
    from .config import get_config

    if config is None:
        config = get_config()

    if max_tokens <= 0:
        model_name = getattr(ai_client, 'model', '')
        max_tokens = config.get('models', {}).get(model_name, {}).get('max_tokens', 8000)

    # Detect provider type
    is_anthropic = isinstance(ai_client, AnthropicClient)

    if is_anthropic:
        result = _query_with_tools_anthropic(
            ai_client, cache_prompt_list, prompt, tools, tool_resolver,
            logfile, max_tokens, config
        )
    else:
        result = _query_with_tools_openai(
            ai_client, cache_prompt_list, prompt, tools, tool_resolver,
            logfile, max_tokens, config
        )

    # Log the final interaction
    _log_entry(logfile, ''.join(cache_prompt_list), prompt, result)
    return result


def _log_entry(logfile, cache_text, prompt, result):
    """Append a JSON log entry."""
    if not logfile:
        return
    log_entry = [str(datetime.now(UTC)), cache_text, prompt, result, 0, 0, 'TOOL_USE']
    try:
        with open(logfile, 'a', encoding='utf-8') as f:
            f.write(json.dumps(log_entry, indent=4))
    except Exception:
        pass


def _query_with_tools_anthropic(ai_client, cache_prompt_list, prompt, tools, tool_resolver,
                                 logfile, max_tokens, config):
    """Anthropic implementation of the tool-call loop."""
    raw_client = ai_client.client
    model = ai_client.model

    # Build system message and cached context blocks
    system_message = "You are a legal expert that analyzes legal documents."

    # Build initial content blocks (cache + user prompt)
    initial_content = []
    for part in cache_prompt_list:
        if len(part) > 4500:
            initial_content.append({
                "type": "text",
                "text": part,
                "cache_control": {"type": "ephemeral"},
            })
        else:
            initial_content.append({"type": "text", "text": part})
    initial_content.append({"type": "text", "text": prompt})

    messages = [{"role": "user", "content": initial_content}]

    final_text = ''
    for _ in range(_MAX_TOOL_ROUNDS):
        def _call(messages=messages):
            return raw_client.messages.create(
                model=model,
                max_tokens=max_tokens,
                system=system_message,
                tools=tools,
                messages=messages,
            )

        from .ai_client import make_api_call_with_retry
        response = make_api_call_with_retry(_call)

        # Extract text and tool-use blocks
        text_parts = []
        tool_use_blocks = []
        for block in response.content:
            if block.type == 'text':
                text_parts.append(block.text)
            elif block.type == 'tool_use':
                tool_use_blocks.append(block)

        final_text = ''.join(text_parts)

        if response.stop_reason != 'tool_use' or not tool_use_blocks:
            break

        # Resolve tool calls
        tool_results = []
        for block in tool_use_blocks:
            result_text = tool_resolver(block.name, block.input)
            tool_results.append({
                "type": "tool_result",
                "tool_use_id": block.id,
                "content": result_text,
            })

        # Add assistant turn + tool results to conversation
        messages.append({"role": "assistant", "content": response.content})
        messages.append({"role": "user", "content": tool_results})

    return final_text


def _query_with_tools_openai(ai_client, cache_prompt_list, prompt, tools, tool_resolver,
                              logfile, max_tokens, config):
    """OpenAI implementation of the tool-call loop."""
    raw_client = ai_client.client
    model = ai_client.model

    # Build OpenAI-format tools
    openai_tools = []
    for tool in tools:
        openai_tools.append({
            "type": "function",
            "function": {
                "name": tool["name"],
                "description": tool["description"],
                "parameters": tool["input_schema"],
            },
        })

    # Build initial messages
    full_cache = ''.join(cache_prompt_list)
    messages = [{"role": "system", "content": "You are a legal expert that analyzes legal documents."}]
    if full_cache:
        messages.append({"role": "system", "content": full_cache})
    messages.append({"role": "user", "content": prompt})

    final_text = ''
    for _ in range(_MAX_TOOL_ROUNDS):
        def _call(messages=messages):
            return raw_client.chat.completions.create(
                model=model,
                max_completion_tokens=max_tokens,
                tools=openai_tools,
                messages=messages,
            )

        from .ai_client import make_api_call_with_retry
        response = make_api_call_with_retry(_call)
        message = response.choices[0].message
        finish_reason = response.choices[0].finish_reason

        final_text = message.content or ''

        if finish_reason != 'tool_calls' or not message.tool_calls:
            break

        # Resolve tool calls
        messages.append(message)
        for tc in message.tool_calls:
            tool_input = json.loads(tc.function.arguments)
            result_text = tool_resolver(tc.function.name, tool_input)
            messages.append({
                "role": "tool",
                "tool_call_id": tc.id,
                "content": result_text,
            })

    return final_text
