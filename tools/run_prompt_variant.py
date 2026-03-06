#!/usr/bin/env python3
"""
Run analyst calls for a small set of test cases with optional prompt overrides.

Supports comparing baseline behaviour vs. a prompt variant without running a
full Stage 4 pipeline.  Each run produces one prompt file and one response file
per chunk, plus a summary.json with aggregate action counts.

Usage:
    # Baseline (no variant)
    python tools/run_prompt_variant.py <processed_json> "<question>" \\
        --units eccn:0A001 eccn:3A090 \\
        --output results/baseline/

    # With a variant
    python tools/run_prompt_variant.py <processed_json> "<question>" \\
        --units eccn:0A001 eccn:3A090 \\
        --variant variants/v1_concise_schema.py \\
        --output results/v1/

Variant file contract:
    A Python file with zero or more of the following module-level functions.
    Any section not overridden falls back to the baseline text unchanged.

    def role_instructions() -> str:
        # Replace the analyst role + task description (cache part 1)

    def action_schema(score_level: int) -> str:
        # Replace the full action schema block (cache part 3)
        # score_level is 1, 2, or 3

    def unit_context_header() -> str:
        # Text prepended to the unit-specific context block (query prompt)

Unit spec format:
    TYPE:NUMBER where TYPE is a case-insensitive prefix match against the
    document's parameter type names (e.g. "eccn" matches "eccns").
"""
# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import sys
import types

# Allow running from the tools/ directory or the project root
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from utils import lookup_item, chunk_text
from utils.config import get_config, create_client_for_task, get_model_for_task
from utils import ModelError, query_json
from question_answering import ContextBuilder, ChunkAnalyzer


# ---------------------------------------------------------------------------
# Unit spec resolution (shared with dump_analyst_prompt)
# ---------------------------------------------------------------------------


def resolve_unit_spec(
    parsed_content: dict, spec: str
) -> tuple[str, str, str]:
    """
    Resolve 'TYPE:NUMBER' to (item_type_name, item_type_name_plural, item_number).
    """
    if ":" not in spec:
        raise ValueError(f"Unit spec must be 'TYPE:NUMBER', got: {spec!r}")
    type_prefix, item_number = spec.split(":", 1)
    type_prefix_lower = type_prefix.lower()

    param_pointer = (
        parsed_content.get("document_information", {}).get("parameters", {})
    )
    for _key, p in param_pointer.items():
        name = p.get("name", "")
        name_plural = p.get("name_plural", "")
        if name.lower().startswith(type_prefix_lower) or name_plural.lower().startswith(
            type_prefix_lower
        ):
            return name, name_plural, item_number

    all_names = sorted(
        p.get("name", "") for p in param_pointer.values() if p.get("name")
    )
    raise ValueError(
        f"No type matching prefix '{type_prefix}'. "
        f"Available types: {all_names}"
    )


# ---------------------------------------------------------------------------
# Variant loading and application
# ---------------------------------------------------------------------------


def load_variant(variant_path: str) -> types.ModuleType:
    """Load a variant Python file as a module."""
    spec = importlib.util.spec_from_file_location("variant", variant_path)
    if spec is None or spec.loader is None:
        raise ValueError(f"Cannot load variant file: {variant_path!r}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # type: ignore[union-attr]
    return module


def apply_variant(
    static_cache: list[str],
    unit_context: str,
    variant: types.ModuleType | None,
    score_level: int,
) -> tuple[list[str], str]:
    """
    Apply variant overrides to static_cache and unit_context.

    static_cache layout (produced by build_cache_components_for_item):
        [0] role_instructions
        [1] question_block
        [2] action_instructions  (selectivity guidance + action schema)

    Returns (modified_static_cache, modified_unit_context).
    """
    if variant is None:
        return static_cache, unit_context

    modified = list(static_cache)

    if hasattr(variant, "role_instructions"):
        modified[0] = variant.role_instructions()

    if hasattr(variant, "action_schema"):
        modified[2] = variant.action_schema(score_level)

    if hasattr(variant, "unit_context_header"):
        header = variant.unit_context_header()
        unit_context = header + unit_context

    return modified, unit_context


# ---------------------------------------------------------------------------
# Simple scratch update between chunks
# ---------------------------------------------------------------------------


def apply_actions_to_scratch(
    actions: list, scratch: dict, unit_type: str, unit_number: str
) -> dict:
    """
    Apply add_entry and add_answer actions to scratch so later chunks can
    see what was already added.  Other action types are counted but not
    reflected in the scratch document.
    """
    # Work on a shallow copy of the top-level dict; inner dicts are mutated
    scratch = dict(scratch)
    for action in actions:
        if not isinstance(action, dict):
            continue
        action_type = action.get("action", "")

        if action_type == "add_entry":
            entry = action.get("entry", {})
            etype = entry.get("type", "")   # 'fact' or 'question'
            eid = entry.get("id", "")
            if etype and eid:
                if etype not in scratch:
                    scratch[etype] = {}
                scratch[etype][eid] = {
                    "content": entry.get("content", ""),
                    "source": [f"{unit_type} {unit_number}"],
                }

        elif action_type == "add_answer":
            q_id = action.get("question_id", "")
            answer = action.get("answer", {})
            if q_id and scratch.get("question", {}).get(q_id) is not None:
                q_entry = scratch["question"][q_id]
                if "answers" not in q_entry:
                    q_entry["answers"] = {}
                # Generate a safe ID if not provided
                raw_id = answer.get("id", "")
                a_id = raw_id if raw_id else f"a_{len(q_entry['answers']) + 1:03d}"
                q_entry["answers"][a_id] = {
                    "content": answer.get("content", ""),
                    "source": [f"{unit_type} {unit_number}"],
                }

    return scratch


# ---------------------------------------------------------------------------
# Action counting helpers
# ---------------------------------------------------------------------------


def count_actions(
    all_actions: list[list],
    unit_type: str,
    unit_number: str,
) -> dict:
    """Return per-unit summary counts from all chunks' action lists."""
    facts_added = 0
    questions_added = 0
    section_requests = 0
    detail_requests = 0
    self_requests = 0

    for actions in all_actions:
        for a in actions:
            if not isinstance(a, dict):
                continue
            atype = a.get("action", "")
            if atype == "add_entry":
                etype = a.get("entry", {}).get("type", "")
                if etype == "fact":
                    facts_added += 1
                elif etype == "question":
                    questions_added += 1
            elif atype == "add_section":
                section_requests += 1
            elif atype == "request_detail":
                detail_requests += 1
            elif atype == "request_relevant_section":
                self_requests += _is_self_request(a, unit_type, unit_number)

    return {
        "facts_added": facts_added,
        "questions_added": questions_added,
        "section_requests": section_requests,
        "detail_requests": detail_requests,
        "self_requests_detected": self_requests,
    }


def _is_self_request(action: dict, unit_type: str, unit_number: str) -> int:
    """Return 1 if a request_relevant_section targets the current unit."""
    target_type = str(action.get("target_type", "")).strip().lower()
    target_number = str(action.get("target_number", "")).strip().lower()
    return int(
        target_type == unit_type.lower() and target_number == unit_number.lower()
    )


# ---------------------------------------------------------------------------
# Per-unit run
# ---------------------------------------------------------------------------


def run_unit(
    parsed_content: dict,
    question_text: str,
    spec: str,
    score_level: int,
    scratch_initial: dict,
    variant: types.ModuleType | None,
    client,
    logfile: str,
    output_dir: str,
) -> dict | None:
    """Run all chunks for one unit and return summary counts, or None on error."""
    try:
        item_type_name, item_type_name_plural, item_number = resolve_unit_spec(
            parsed_content, spec
        )
    except ValueError as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        return None

    working_item = lookup_item(parsed_content, item_type_name_plural, item_number)
    if working_item is None:
        print(
            f"[ERROR] Unit not found: {spec} "
            f"(type_plural={item_type_name_plural}, number={item_number})",
            file=sys.stderr,
        )
        return None

    unit_title = working_item.get("unit_title", "")
    safe_id = f"{item_type_name}_{item_number.replace('/', '_').replace(chr(92), '_')}"

    # Build baseline cache components
    context_builder = ContextBuilder(parsed_content, question_text)
    static_cache, unit_context = context_builder.build_cache_components_for_item(
        working_item,
        item_type_name,
        item_number,
        scratch_initial,
        score_level=score_level,
    )

    # Apply variant overrides
    static_cache, unit_context = apply_variant(
        static_cache, unit_context, variant, score_level
    )

    # Determine chunks
    text = working_item.get("text", "")
    breakpoints = working_item.get("breakpoints", [])
    if breakpoints and text:
        chunks = list(chunk_text(text, breakpoints, preferred_length=15000))
    else:
        chunks = [text] if text else ["[No text content]"]

    n_chunks = len(chunks)
    print(f"  {spec}: {n_chunks} chunk(s)", end="", flush=True)

    # Run each chunk
    scratch = dict(scratch_initial)
    all_actions: list[list] = []

    for idx, chunk_str in enumerate(chunks):
        # Build prompt with current scratch snapshot
        analyzer = ChunkAnalyzer.__new__(ChunkAnalyzer)
        analyzer.scratch_snapshot = scratch

        full_cache, query_prompt = analyzer._build_chunk_prompt(
            static_cache,
            unit_context,
            chunk_str,
            None,   # prev_chunk_summary
            item_type_name,
            item_number,
            False,  # refine
            unit_title=unit_title,
        )

        # Save prompt
        prompt_text = "\n".join(full_cache) + "\n\n--- QUERY PROMPT ---\n\n" + query_prompt
        prompt_file = os.path.join(output_dir, f"{safe_id}_prompt_chunk{idx + 1}.txt")
        with open(prompt_file, "w", encoding="utf-8") as f:
            f.write(prompt_text)

        # Call LLM
        try:
            result_obj = query_json(client, full_cache, query_prompt, logfile)
        except ModelError as exc:
            print(f"\n  [ERROR] LLM call failed for {spec} chunk {idx + 1}: {exc}",
                  file=sys.stderr)
            result_obj = {"actions": []}

        # Normalise response (same logic as ChunkAnalyzer.analyze_chunks)
        if isinstance(result_obj, dict):
            if "actions" in result_obj and isinstance(result_obj["actions"], list):
                actions = result_obj["actions"]
            else:
                actions = [result_obj]
        elif isinstance(result_obj, list):
            actions = result_obj
        else:
            actions = []

        # Save response
        response_file = os.path.join(
            output_dir, f"{safe_id}_response_chunk{idx + 1}.json"
        )
        with open(response_file, "w", encoding="utf-8") as f:
            json.dump(result_obj, f, indent=2)

        all_actions.append(actions)

        # Update local scratch so subsequent chunks see prior additions
        scratch = apply_actions_to_scratch(actions, scratch, item_type_name, item_number)

        print(".", end="", flush=True)

    print()  # newline after progress dots

    return count_actions(all_actions, item_type_name, item_number)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    # Ensure UTF-8 output on Windows where the console may default to cp1252
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    parser = argparse.ArgumentParser(
        description=(
            "Run analyst LLM calls for a small set of units with optional "
            "prompt variant overrides."
        )
    )
    parser.add_argument("processed_json", help="Path to processed JSON file")
    parser.add_argument("question", help="Question text")
    parser.add_argument(
        "--units",
        nargs="+",
        required=True,
        metavar="TYPE:NUMBER",
        help="Unit specs (e.g. eccn:0A001 eccn:3A090)",
    )
    parser.add_argument(
        "--variant",
        metavar="FILE",
        help="Python file with optional override functions (omit for baseline)",
    )
    parser.add_argument(
        "--score-level",
        type=int,
        default=2,
        choices=[1, 2, 3],
        help="Score level passed to build_cache_components_for_item (default: 2)",
    )
    parser.add_argument(
        "--output",
        metavar="DIR",
        required=True,
        help="Output directory for prompts, responses, and summary.json",
    )
    parser.add_argument(
        "--scratch",
        metavar="FILE",
        help="Path to pre-loaded scratch JSON file (default: empty scratch)",
    )
    args = parser.parse_args()

    # Load inputs
    with open(args.processed_json, "r", encoding="utf-8") as f:
        parsed_content = json.load(f)

    scratch_initial: dict = {}
    if args.scratch:
        with open(args.scratch, "r", encoding="utf-8") as f:
            scratch_initial = json.load(f)

    variant: types.ModuleType | None = None
    if args.variant:
        variant = load_variant(args.variant)
        print(f"Loaded variant: {args.variant}")
    else:
        print("Running baseline (no variant)")

    # Set up output directory and logfile
    os.makedirs(args.output, exist_ok=True)
    logfile = os.path.join(args.output, "qa_variant.log")

    # Create LLM client
    config = get_config()
    task_name = "qa.analysis.analyze_chunk"
    model_name = get_model_for_task(config, task_name)
    client = create_client_for_task(config, task_name)
    print(f"Using model: {model_name}  (task: {task_name})")
    print(f"Output: {args.output}")
    print()

    # Run each unit
    summary: dict[str, dict] = {}
    for spec in args.units:
        print(f"Processing {spec}...")
        counts = run_unit(
            parsed_content,
            args.question,
            spec,
            args.score_level,
            scratch_initial,
            variant,
            client,
            logfile,
            args.output,
        )
        if counts is not None:
            summary[spec] = counts

    # Write summary
    summary_path = os.path.join(args.output, "summary.json")
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    # Print summary table
    print()
    print("Summary")
    print("-" * 60)
    col_w = max((len(s) for s in summary), default=10)
    header = f"{'Unit':<{col_w}}  {'facts':>5}  {'qs':>3}  {'sects':>5}  {'dtail':>5}  {'self?':>5}"
    print(header)
    print("-" * len(header))
    for spec, counts in summary.items():
        self_flag = "YES" if counts["self_requests_detected"] else "-"
        print(
            f"{spec:<{col_w}}  "
            f"{counts['facts_added']:>5}  "
            f"{counts['questions_added']:>3}  "
            f"{counts['section_requests']:>5}  "
            f"{counts['detail_requests']:>5}  "
            f"{self_flag:>5}"
        )
    print(f"\nSummary written to: {summary_path}")


if __name__ == "__main__":
    main()
