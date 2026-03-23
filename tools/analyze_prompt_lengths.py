#!/usr/bin/env python3
"""Analyze Stage 2 log entries to find the longest prompts."""

import os
import json
import sys
from collections import Counter

LOG_ROOT = r"C:/Users/syoun/document_analyzer_output"


def classify_task(query_prompt):
    """Classify a query prompt as a Stage 2 task type, or None if not Stage 2."""
    snippet = query_prompt[:800]

    if "extract **defined terms**" in snippet:
        return "s2.extract_terms"
    if "explicitly created, usable definition** for the term" in snippet:
        return "s2.extract_definition"
    if "extract the scope of applicability for a definition" in snippet:
        return "s2.extract_scope"
    if ("Evaluate whether the following would make sense as a usable definition" in snippet or
            ("earlier analysis" in snippet and "was found to be defined as" in snippet)):
        return "s2.evaluate_quality"
    if "find a definition for the term:" in snippet and "trying" in snippet:
        return "s2.retry_extraction"
    return None


def parse_log_file(filepath):
    """Parse a log file and return list of entries."""
    entries = []
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read().strip()
        if not content:
            return entries

        decoder = json.JSONDecoder()
        pos = 0
        while pos < len(content):
            while pos < len(content) and content[pos] in ' \t\n\r':
                pos += 1
            if pos >= len(content):
                break
            try:
                obj, end_pos = decoder.raw_decode(content, pos)
                if isinstance(obj, list):
                    # Could be a single entry [ts, cache, query, resp, ...] or a list of entries
                    if len(obj) > 0 and isinstance(obj[0], list):
                        entries.extend(obj)
                    else:
                        entries.append(obj)
                pos = end_pos
            except json.JSONDecodeError:
                break
    except Exception as e:
        print(f"  ERROR reading {filepath}: {e}", file=sys.stderr)
    return entries


def main():
    results = []
    files_checked = 0
    entries_checked = 0
    stage2_found = 0

    print(f"Walking {LOG_ROOT} for log*.json files...", flush=True)

    for root, dirs, files in os.walk(LOG_ROOT):
        for fname in sorted(files):
            if fname.startswith("log") and fname.endswith(".json"):
                filepath = os.path.join(root, fname)
                files_checked += 1

                entries = parse_log_file(filepath)
                for entry in entries:
                    entries_checked += 1

                    if not isinstance(entry, list) or len(entry) < 3:
                        continue

                    cache_text = entry[1] if len(entry) > 1 else ""
                    query_prompt = entry[2] if len(entry) > 2 else ""
                    timestamp = entry[0] if len(entry) > 0 else ""

                    if not isinstance(cache_text, str):
                        cache_text = ""
                    if not isinstance(query_prompt, str):
                        query_prompt = ""

                    task_type = classify_task(query_prompt)
                    if task_type is None:
                        continue

                    stage2_found += 1
                    total_len = len(cache_text) + len(query_prompt)

                    results.append({
                        "file": filepath,
                        "task_type": task_type,
                        "total_len": total_len,
                        "cache_len": len(cache_text),
                        "query_len": len(query_prompt),
                        "cache_text": cache_text,
                        "query_prompt": query_prompt,
                        "timestamp": timestamp,
                    })

    print(f"Files checked: {files_checked}")
    print(f"Total entries parsed: {entries_checked}")
    print(f"Stage 2 entries found: {stage2_found}")

    if not results:
        print("No Stage 2 entries found.")
        return

    results.sort(key=lambda x: x["total_len"], reverse=True)
    top = results[:10]

    print()
    print("=" * 80)
    print(f"TOP {len(top)} LONGEST STAGE 2 PROMPTS")
    print("=" * 80)

    for rank, r in enumerate(top, 1):
        print(f"\n{'='*80}")
        print(f"RANK #{rank}")
        print(f"  Task type   : {r['task_type']}")
        print(f"  File        : {r['file']}")
        print(f"  Timestamp   : {r['timestamp']}")
        print(f"  Total length: {r['total_len']:,} chars")
        print(f"  Cache length: {r['cache_len']:,} chars")
        print(f"  Query length: {r['query_len']:,} chars")
        print()
        print("--- QUERY PROMPT (first 2000 chars) ---")
        print(r['query_prompt'][:2000])
        if len(r['query_prompt']) > 2000:
            print(f"\n... [{len(r['query_prompt']) - 2000:,} more chars] ...")
        print()
        if r['cache_len'] > 0:
            print("--- CACHE TEXT (first 500 chars) ---")
            print(r['cache_text'][:500])
            if len(r['cache_text']) > 500:
                print(f"\n... [{len(r['cache_text']) - 500:,} more chars] ...")
        print()

    print("\n" + "="*80)
    print("DISTRIBUTION BY TASK TYPE (all Stage 2 entries):")
    task_counts = Counter(r['task_type'] for r in results)
    for task, count in sorted(task_counts.items()):
        avg_len = sum(r['total_len'] for r in results if r['task_type'] == task) / count
        max_len = max(r['total_len'] for r in results if r['task_type'] == task)
        print(f"  {task:<30} count={count:5d}  avg={avg_len:>10,.0f}  max={max_len:>10,}")


if __name__ == "__main__":
    main()
