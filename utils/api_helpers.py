# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License
"""
Higher-level AI query helpers for Document Analyzer.

Provides query_with_tools() — a multi-turn tool-call loop that supports both
OpenAI and Anthropic clients.  The caller supplies a list of tool definitions
(in Anthropic format) and a resolver callable; this module handles the provider-
specific wire format and the back-and-forth until the model produces a final
text answer.

Caching strategy
----------------
Each individual API round in the tool-call loop is cached separately.  The cache
key for round N is built from:

  * The full serialized messages list at the point the API call is made —
    i.e. everything the model has seen so far, including prior tool-call requests
    and tool results.
  * The serialized tools list (so a schema change invalidates existing entries).
  * The system prompt string.
  * Model name and max_tokens (folded in by APICache._generate_cache_key).

So the effective fingerprint is:

  Round 1 key = hash(system + tools + initial_prompt + model + max_tokens)
  Round 2 key = hash(system + tools + initial_prompt + round-1 response
                     + tool-result-1 + model + max_tokens)
  ...and so on.

Because the key for round N+1 includes the tool results that were fed back (which
come from our local resolver code), a bug fix in the resolver produces a different
tool result → different round N+1 key → cache miss → fresh API call.  Stale cached
responses from before the fix are therefore never silently replayed.

Including the tools list in the key means that changing a tool's schema (description,
parameter names, etc.) also invalidates the cache, even when the prompt text is
unchanged.

Response objects from the provider SDK are normalized to plain dicts before being
added to the messages list.  This keeps the list JSON-serializable at all times,
which is required for deterministic key generation.
"""

import json
from datetime import datetime, UTC
from types import SimpleNamespace
from typing import Callable

from .api_cache import get_cached_response, set_cached_response


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

    Each individual API round is cached keyed on the full serialized conversation
    state at that point.  See module docstring for the full caching strategy.

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
    from .ai_client import AnthropicClient, OpenAIClient
    from .config import get_config

    if config is None:
        config = get_config()

    model_name = getattr(ai_client, 'model', '')

    if max_tokens <= 0:
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


# ---------------------------------------------------------------------------
# Cache helpers
# ---------------------------------------------------------------------------

def _messages_cache_key(messages: list, tools: list = None, system: str = '') -> str:
    """
    Return a stable JSON string suitable for use as the query_prompt cache key.

    Encodes the full messages list, the tools schema, and the system prompt so that
    changes to any of these invalidate existing cache entries.  All structures are
    serialized with sorted keys to be independent of dict insertion order.

    Note: model name and max_tokens are folded into the final SHA-256 hash by
    APICache._generate_cache_key; they do not need to appear here.
    """
    key_data: dict = {"messages": messages}
    if tools:
        key_data["tools"] = tools
    if system:
        key_data["system"] = system
    return json.dumps(key_data, sort_keys=True, ensure_ascii=False)


def _get_round_cached(messages: list, model: str, max_tokens: int,
                      tools: list = None, system: str = ''):
    """Return cached round response (JSON string), or None on miss."""
    return get_cached_response('', _messages_cache_key(messages, tools, system),
                               model, max_tokens)


def _set_round_cached(messages: list, model: str, max_tokens: int, response_dict: dict,
                      tools: list = None, system: str = ''):
    """Cache a normalized round response dict (stored as a JSON string)."""
    if response_dict:
        set_cached_response('', _messages_cache_key(messages, tools, system),
                            model, max_tokens,
                            json.dumps(response_dict, ensure_ascii=False))


# ---------------------------------------------------------------------------
# Anthropic path
# ---------------------------------------------------------------------------

def _query_with_tools_anthropic(ai_client, cache_prompt_list, prompt, tools, tool_resolver,
                                 logfile, max_tokens, config):
    """Anthropic implementation of the tool-call loop with per-round caching."""
    from .ai_client import make_api_call_with_retry

    raw_client = ai_client.client
    model = ai_client.model
    system_message = "You are a legal expert that analyzes legal documents."

    # Build initial content blocks (cache + user prompt) — plain dicts throughout
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
        # --- cache check (keyed on messages + tools schema + system prompt) ---
        cached_str = _get_round_cached(messages, model, max_tokens,
                                       tools=tools, system=system_message)
        if cached_str is not None:
            round_data = json.loads(cached_str)
        else:
            # Live API call
            def _call(messages=messages):
                return raw_client.messages.create(
                    model=model,
                    max_tokens=max_tokens,
                    system=system_message,
                    tools=tools,
                    messages=messages,
                )
            response = make_api_call_with_retry(_call)

            # Normalize response to plain dicts (makes it JSON-serializable and
            # keeps the messages list stable for subsequent round keys)
            normalized_content = []
            for block in response.content:
                if block.type == 'text':
                    normalized_content.append({"type": "text", "text": block.text})
                elif block.type == 'tool_use':
                    normalized_content.append({
                        "type": "tool_use",
                        "id": block.id,
                        "name": block.name,
                        "input": block.input,
                    })

            round_data = {
                "stop_reason": response.stop_reason,
                "content": normalized_content,
            }
            _set_round_cached(messages, model, max_tokens, round_data,
                              tools=tools, system=system_message)

        # Reconstruct lightweight response-like objects from the (possibly cached) dicts
        stop_reason = round_data["stop_reason"]
        content_blocks = [SimpleNamespace(**b) for b in round_data["content"]]

        # Extract text and tool-use blocks
        text_parts = []
        tool_use_blocks = []
        for block in content_blocks:
            if block.type == 'text':
                text_parts.append(block.text)
            elif block.type == 'tool_use':
                tool_use_blocks.append(block)

        final_text = ''.join(text_parts)

        if stop_reason != 'tool_use' or not tool_use_blocks:
            break

        # Resolve tool calls (runs current resolver code — intentionally not cached)
        tool_results = []
        for block in tool_use_blocks:
            result_text = tool_resolver(block.name, block.input)
            tool_results.append({
                "type": "tool_result",
                "tool_use_id": block.id,
                "content": result_text,
            })

        # Append normalized dicts so the messages list stays JSON-serializable
        messages.append({"role": "assistant", "content": round_data["content"]})
        messages.append({"role": "user", "content": tool_results})

    return final_text


# ---------------------------------------------------------------------------
# OpenAI path
# ---------------------------------------------------------------------------

def _query_with_tools_openai(ai_client, cache_prompt_list, prompt, tools, tool_resolver,
                              logfile, max_tokens, config):
    """OpenAI implementation of the tool-call loop with per-round caching."""
    from .ai_client import make_api_call_with_retry

    raw_client = ai_client.client
    model = ai_client.model

    # Build OpenAI-format tools
    openai_tools = [
        {
            "type": "function",
            "function": {
                "name": tool["name"],
                "description": tool["description"],
                "parameters": tool["input_schema"],
            },
        }
        for tool in tools
    ]

    # Build initial messages (plain dicts throughout)
    full_cache = ''.join(cache_prompt_list)
    system_message = "You are a legal expert that analyzes legal documents."
    messages = [{"role": "system", "content": system_message}]
    if full_cache:
        messages.append({"role": "system", "content": full_cache})
    messages.append({"role": "user", "content": prompt})

    final_text = ''
    for _ in range(_MAX_TOOL_ROUNDS):
        # --- cache check (keyed on messages + tools schema + system prompt) ---
        # Use the canonical Anthropic-format tools list as the key source so that
        # the OpenAI-derived openai_tools (a mechanical transformation) does not
        # add a redundant dimension to the key.
        cached_str = _get_round_cached(messages, model, max_tokens,
                                       tools=tools, system=system_message)
        if cached_str is not None:
            round_data = json.loads(cached_str)
        else:
            # Live API call
            def _call(messages=messages):
                return raw_client.chat.completions.create(
                    model=model,
                    max_completion_tokens=max_tokens,
                    tools=openai_tools,
                    messages=messages,
                )
            response = make_api_call_with_retry(_call)
            message = response.choices[0].message
            finish_reason = response.choices[0].finish_reason

            # Normalize to plain dicts
            round_data = {
                "finish_reason": finish_reason,
                "content": message.content,
                "tool_calls": [
                    {
                        "id": tc.id,
                        "type": "function",
                        "function": {
                            "name": tc.function.name,
                            "arguments": tc.function.arguments,
                        },
                    }
                    for tc in (message.tool_calls or [])
                ] if message.tool_calls else None,
            }
            _set_round_cached(messages, model, max_tokens, round_data,
                              tools=tools, system=system_message)

        finish_reason = round_data["finish_reason"]
        final_text = round_data["content"] or ''

        if finish_reason != 'tool_calls' or not round_data.get("tool_calls"):
            break

        # Reconstruct tool_calls as SimpleNamespace so tool_resolver can be called
        tool_calls = [
            SimpleNamespace(
                id=tc["id"],
                function=SimpleNamespace(
                    name=tc["function"]["name"],
                    arguments=tc["function"]["arguments"],
                ),
            )
            for tc in round_data["tool_calls"]
        ]

        # Append normalized assistant turn (plain dict, not SDK object)
        messages.append({
            "role": "assistant",
            "content": round_data["content"],
            "tool_calls": round_data["tool_calls"],
        })

        # Resolve tool calls and append results
        for tc in tool_calls:
            tool_input = json.loads(tc.function.arguments)
            result_text = tool_resolver(tc.function.name, tool_input)
            messages.append({
                "role": "tool",
                "tool_call_id": tc.id,
                "content": result_text,
            })

    return final_text


# ---------------------------------------------------------------------------
# Logging helpers
# ---------------------------------------------------------------------------

def _write_log_entry(logfile, log_entry):
    """Append a pre-built JSON log entry to logfile."""
    if not logfile:
        return
    try:
        with open(logfile, 'a', encoding='utf-8') as f:
            f.write(json.dumps(log_entry, indent=4))
    except Exception:
        pass


def _log_entry(logfile, cache_text, prompt, result):
    """Append a TOOL_USE JSON log entry."""
    log_entry = [str(datetime.now(UTC)), cache_text, prompt, result, 0, 0, 'TOOL_USE']
    _write_log_entry(logfile, log_entry)
