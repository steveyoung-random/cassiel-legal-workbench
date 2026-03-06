#!/usr/bin/env python3
"""
Dump the full assembled analyst prompt for a given unit + question.

Writes one file per analyst call (one per chunk for multi-chunk units).
Each file contains the complete prompt exactly as the model receives it:
all four cache sections plus the query prompt for that specific chunk.

Usage:
    python tools/dump_analyst_prompt.py <processed_json> "<question>" \\
        --units eccn:0A001 eccn:3A090 \\
        --output dir/ \\
        [--score-level 2]        # 2=default, 3=high-relevance path
        [--scratch scratch.json] # pre-loaded scratch (default: empty)

Unit spec format:
    TYPE:NUMBER where TYPE is a case-insensitive prefix match against the
    document's parameter type names (e.g. "eccn" matches "eccns").
"""
# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License

from __future__ import annotations

import argparse
import json
import os
import sys

# Allow running from the tools/ directory or the project root
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from utils import lookup_item, chunk_text
from question_answering import ContextBuilder, ChunkAnalyzer


# ---------------------------------------------------------------------------
# Unit spec resolution
# ---------------------------------------------------------------------------


def resolve_unit_spec(
    parsed_content: dict, spec: str
) -> tuple[str, str, str]:
    """
    Resolve 'TYPE:NUMBER' to (item_type_name, item_type_name_plural, item_number).

    TYPE is matched case-insensitively as a prefix against the document's
    parameter type names and plural type names.
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
# Token estimation
# ---------------------------------------------------------------------------


def estimate_tokens(text: str) -> int:
    """Rough estimate: ~4 characters per token."""
    return max(1, len(text) // 4)


# ---------------------------------------------------------------------------
# Core dump logic
# ---------------------------------------------------------------------------


def dump_unit(
    parsed_content: dict,
    question_text: str,
    spec: str,
    score_level: int,
    scratch: dict,
    output_dir: str,
) -> None:
    """Write one prompt file per chunk for the given unit."""
    try:
        item_type_name, item_type_name_plural, item_number = resolve_unit_spec(
            parsed_content, spec
        )
    except ValueError as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        return

    working_item = lookup_item(parsed_content, item_type_name_plural, item_number)
    if working_item is None:
        print(
            f"[ERROR] Unit not found: {spec} "
            f"(type_plural={item_type_name_plural}, number={item_number})",
            file=sys.stderr,
        )
        return

    unit_title = working_item.get("unit_title", "")

    # Build static cache + unit context (no LLM calls)
    context_builder = ContextBuilder(parsed_content, question_text)
    static_cache, unit_context = context_builder.build_cache_components_for_item(
        working_item,
        item_type_name,
        item_number,
        scratch,
        score_level=score_level,
        scratch_snapshot=scratch,
    )

    # Minimal ChunkAnalyzer — only scratch_snapshot is accessed by _build_chunk_prompt
    analyzer = ChunkAnalyzer.__new__(ChunkAnalyzer)
    analyzer.scratch_snapshot = scratch

    # Determine chunks
    text = working_item.get("text", "")
    breakpoints = working_item.get("breakpoints", [])
    if breakpoints and text:
        chunks = list(chunk_text(text, breakpoints, preferred_length=15000))
    else:
        chunks = [text] if text else ["[No text content]"]

    n_chunks = len(chunks)
    safe_number = item_number.replace("/", "_").replace("\\", "_")
    os.makedirs(output_dir, exist_ok=True)

    for chunk_idx, chunk_str in enumerate(chunks):
        full_cache, query_prompt = analyzer._build_chunk_prompt(
            static_cache,
            unit_context,
            chunk_str,
            None,   # prev_chunk_summary
            item_type_name,
            item_number,
            False,  # refine
            unit_title=unit_title,
            chunk_idx=chunk_idx,
            n_chunks=n_chunks,
        )

        # -------------------------------------------------------------------
        # Format one file = one complete analyst call
        # -------------------------------------------------------------------
        lines: list[str] = []

        # File header
        unit_label = f"{item_type_name.upper()} {item_number}"
        if unit_title:
            unit_label += f" — {unit_title}"
        chunk_label = f"Chunk {chunk_idx + 1} of {n_chunks}"
        lines += [f"=== {unit_label} | {chunk_label} ===", ""]

        # _build_chunk_prompt builds full_cache as:
        #   [0] role_instructions
        #   [1] question_block
        #   [2] action_instructions  (selectivity guidance + action schema)
        #   [3..] scratch document separators and JSON
        for idx, label in [
            (0, "CACHE PART 1: ROLE INSTRUCTIONS"),
            (1, "CACHE PART 2: QUESTION"),
            (2, "CACHE PART 3: ACTION SCHEMA"),
        ]:
            lines.append(f"=== {label} ===")
            lines.append(full_cache[idx].rstrip())
            lines.append("")

        lines.append("=== CACHE PART 4: SCRATCH DOCUMENT ===")
        lines.append("".join(full_cache[3:]).rstrip())
        lines.append("")

        lines.append("=== QUERY PROMPT ===")
        lines.append(query_prompt.rstrip())
        lines.append("")

        # Per-chunk token summary
        cache_chars = sum(len(p) for p in full_cache)
        lines += [
            "=== SUMMARY ===",
            f"Chunk: {chunk_idx + 1} of {n_chunks}",
            f"Cache tokens (estimated): {estimate_tokens('x' * cache_chars):,}",
            f"Query tokens (estimated): {estimate_tokens(query_prompt):,}",
            "",
        ]

        # Write file
        fname = f"{item_type_name}_{safe_number}_chunk{chunk_idx + 1}.txt"
        fpath = os.path.join(output_dir, fname)
        with open(fpath, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))

        print(f"  Written: {fname}  ({chunk_label})")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    # Ensure UTF-8 output on Windows where the console may default to cp1252
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    parser = argparse.ArgumentParser(
        description=(
            "Dump assembled analyst prompts for given units without making LLM calls. "
            "Writes one file per analyst call (one per chunk for multi-chunk units)."
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
        "--output",
        metavar="DIR",
        required=True,
        help="Output directory for .txt files",
    )
    parser.add_argument(
        "--score-level",
        type=int,
        default=2,
        choices=[1, 2, 3],
        help="Score level passed to build_cache_components_for_item (default: 2)",
    )
    parser.add_argument(
        "--scratch",
        metavar="FILE",
        help="Path to pre-loaded scratch JSON file (default: empty scratch)",
    )
    args = parser.parse_args()

    with open(args.processed_json, "r", encoding="utf-8") as f:
        parsed_content = json.load(f)

    scratch: dict = {}
    if args.scratch:
        with open(args.scratch, "r", encoding="utf-8") as f:
            scratch = json.load(f)

    for spec in args.units:
        print(f"{spec}:")
        dump_unit(
            parsed_content,
            args.question,
            spec,
            args.score_level,
            scratch,
            args.output,
        )


if __name__ == "__main__":
    main()
