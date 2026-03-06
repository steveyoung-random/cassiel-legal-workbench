"""
Q&A Log Analyzer

Parses a question_answering.py log file and answers two types of queries:

  unit <unit_id>
      Show, for each analysis pass, every fact and question added by analysts
      working on that specific unit (matched by unit_id appearing in the source
      or in the prompt).

  compaction
      For each compaction call, show the facts BEFORE (from the prompt) and
      AFTER (from the response) side-by-side to understand what was lost or
      merged.

Usage:
  python tools/analyze_qa_log.py <log_file> unit 3A001
  python tools/analyze_qa_log.py <log_file> compaction
  python tools/analyze_qa_log.py <log_file> unit 3A001 --show-prompts

Log format: the log file is a sequence of concatenated JSON arrays, each with
the structure:
  [timestamp, cache_text, query_prompt, response_text, cache_created, cache_read]
  or (cached hits):
  [timestamp, cache_text, query_prompt, response_text, 0, 0, "CACHED"]
"""
# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License

import argparse
import json
import re
import sys
from typing import Iterator, List, Tuple


# ---------------------------------------------------------------------------
# Log file parser
# ---------------------------------------------------------------------------

def iter_log_entries(path: str) -> Iterator[List]:
    """Yield each JSON array entry from a concatenated-JSON log file."""
    with open(path, 'r', encoding='utf-8') as f:
        text = f.read()

    decoder = json.JSONDecoder()
    pos = 0
    n = len(text)
    while pos < n:
        # Skip whitespace between entries
        while pos < n and text[pos] in ' \t\r\n':
            pos += 1
        if pos >= n:
            break
        try:
            obj, end = decoder.raw_decode(text, pos)
            yield obj
            pos = end
        except json.JSONDecodeError:
            # Skip one character and try again (handles minor corruption)
            pos += 1


def get_full_prompt(entry: List) -> str:
    """Reconstruct the full prompt text: cache + query."""
    cache = entry[1] if len(entry) > 1 else ''
    query = entry[2] if len(entry) > 2 else ''
    return (cache or '') + (query or '')


def get_response(entry: List) -> str:
    return str(entry[3]) if len(entry) > 3 else ''


def get_timestamp(entry: List) -> str:
    return str(entry[0]) if entry else ''


def is_cached(entry: List) -> bool:
    return len(entry) > 6 and entry[6] == 'CACHED'


# ---------------------------------------------------------------------------
# Entry classifiers
# ---------------------------------------------------------------------------

def is_analyst_call(entry: List) -> bool:
    """True if this entry is an analyst call (adds facts/questions to scratch)."""
    prompt = get_full_prompt(entry)
    return 'Scratch Document' in prompt and 'Your Portion' in prompt


def is_compaction_call(entry: List) -> bool:
    """True if this entry is a compaction call."""
    prompt = get_full_prompt(entry)
    return 'Consolidate them by removing redundant' in prompt


def is_implicit_detection_call(entry: List) -> bool:
    """True if this entry is an implicit reference detection call."""
    query = entry[2] if len(entry) > 2 else ''
    return 'meaningfully referenced' in (query or '')


def get_unit_from_analyst_prompt(entry: List) -> Tuple[str, str]:
    """
    Extract the unit type and number from an analyst call prompt.
    Returns (type_name, unit_number) or ('', '').

    The analyst prompt contains a section like:
        YOUR PORTION TO ANALYZE
        ====
        Eccn 3A001: ...
    or
        Section 774.1: ...
    """
    query = entry[2] if len(entry) > 2 else ''
    if not query:
        return '', ''
    # Find the "YOUR PORTION TO ANALYZE" header then parse the first content line
    m = re.search(
        r'YOUR PORTION TO ANALYZE\s*\n[=\-]+\n(\w[\w .]*?)\s+([\w.]+)\s*:',
        query
    )
    if m:
        return m.group(1).strip(), m.group(2).strip()
    return '', ''


def unit_is_being_analyzed(entry: List, unit_id: str) -> bool:
    """
    True if this analyst call is FOR the given unit_id (i.e., unit_id appears
    in the 'YOUR PORTION TO ANALYZE' header line, not just anywhere in the prompt).
    """
    query = entry[2] if len(entry) > 2 else ''
    if not query:
        return False
    m = re.search(
        r'YOUR PORTION TO ANALYZE\s*\n[=\-]+\n(\w[\w .]*?)\s+([\w.]+)\s*:',
        query
    )
    if not m:
        return False
    # The unit_id in the header line
    header_line = m.group(0)
    return unit_id in header_line


def parse_actions_from_response(response_text: str) -> dict:
    """
    Parse the JSON actions list from an analyst response.

    Action schema (from question_answering.py):
      add_entry:              entry.{id, type, content, importance}
      add_answer:             question_id, answer.{id, content}
      add_section:            question_id
      request_detail:         id, target_type, target_number, detail_level
      request_relevant_section: id, target_type, target_number

    Returns dict with keys 'facts', 'questions', 'requests', 'answers'.
    """
    result = {'facts': [], 'questions': [], 'requests': [], 'answers': [], 'raw': []}
    try:
        m = re.search(r'\{.*\}', response_text, re.DOTALL)
        if not m:
            return result
        data = json.loads(m.group(0))
        actions = data.get('actions', [])
        if not isinstance(actions, list):
            return result
        for action in actions:
            if not isinstance(action, dict):
                continue
            atype = action.get('action', '')
            result['raw'].append(action)
            if atype == 'add_entry':
                entry = action.get('entry', {})
                etype = entry.get('type', '')
                item = {
                    'id': str(entry.get('id', '')),
                    'content': entry.get('content', ''),
                    'importance': entry.get('importance', ''),
                }
                if etype == 'fact':
                    result['facts'].append(item)
                elif etype == 'question':
                    result['questions'].append(item)
                else:
                    # unknown entry type — put in facts for visibility
                    item['entry_type'] = etype
                    result['facts'].append(item)
            elif atype == 'add_answer':
                answer = action.get('answer', {})
                result['answers'].append({
                    'question_id': str(action.get('question_id', '')),
                    'id': str(answer.get('id', '')),
                    'content': answer.get('content', ''),
                })
            elif atype in ('request_detail', 'request_relevant_section'):
                result['requests'].append({
                    'action': atype,
                    'id': str(action.get('id', '')),
                    'target_type': action.get('target_type', ''),
                    'target_number': action.get('target_number', ''),
                    'detail_level': action.get('detail_level', ''),
                })
    except (json.JSONDecodeError, AttributeError):
        pass
    return result


# ---------------------------------------------------------------------------
# Compaction analysis
# ---------------------------------------------------------------------------

def parse_facts_from_compaction_prompt(prompt_text: str) -> dict:
    """Extract the 'Current facts' JSON dict from a compaction prompt."""
    m = re.search(r'Current facts:\s*(\{.*?\})\s*\nReturn', prompt_text, re.DOTALL)
    if m:
        try:
            return json.loads(m.group(1))
        except json.JSONDecodeError:
            pass
    # Fallback: find any JSON object after "Current facts:"
    idx = prompt_text.find('Current facts:')
    if idx >= 0:
        sub = prompt_text[idx + len('Current facts:'):].strip()
        try:
            decoder = json.JSONDecoder()
            obj, _ = decoder.raw_decode(sub)
            return obj
        except json.JSONDecodeError:
            pass
    return {}


def parse_facts_from_compaction_response(response_text: str) -> dict:
    """Extract the 'fact' dict from a compaction response."""
    try:
        m = re.search(r'\{.*\}', response_text, re.DOTALL)
        if m:
            data = json.loads(m.group(0))
            return data.get('fact', {})
    except json.JSONDecodeError:
        pass
    return {}


def print_compaction_analysis(entries: List[List]) -> None:
    compaction_entries = [e for e in entries if is_compaction_call(e)]
    if not compaction_entries:
        print("No compaction calls found in log.")
        return

    print(f"Found {len(compaction_entries)} compaction call(s).\n")
    for i, entry in enumerate(compaction_entries, 1):
        ts = get_timestamp(entry)
        cached = ' [CACHED]' if is_cached(entry) else ''
        print(f"{'='*70}")
        print(f"Compaction #{i}  {ts}{cached}")
        print(f"{'='*70}")

        prompt = get_full_prompt(entry)
        response = get_response(entry)

        before = parse_facts_from_compaction_prompt(prompt)
        after = parse_facts_from_compaction_response(response)

        print(f"\nBEFORE ({len(before)} facts):")
        for fid, fdata in before.items():
            if isinstance(fdata, dict):
                content = fdata.get('content', str(fdata))
                sources = fdata.get('source', [])
                src = f"  [{', '.join(sources)}]" if sources else ''
                print(f"  [{fid}] {content}{src}")
            else:
                print(f"  [{fid}] {fdata}")

        print(f"\nAFTER ({len(after)} facts):")
        if after:
            for fid, fdata in after.items():
                if isinstance(fdata, dict):
                    content = fdata.get('content', str(fdata))
                    sources = fdata.get('source', [])
                    src = f"  [{', '.join(sources)}]" if sources else ''
                    print(f"  [{fid}] {content}{src}")
                else:
                    print(f"  [{fid}] {fdata}")
        else:
            print("  (could not parse response)")

        removed = len(before) - len(after)
        print(f"\nResult: {len(before)} -> {len(after)} facts ({removed:+d})")
        print()


# ---------------------------------------------------------------------------
# Unit analysis
# ---------------------------------------------------------------------------

def unit_mentioned_in_prompt(entry: List, unit_id: str) -> bool:
    """True if unit_id appears anywhere in the full prompt."""
    return unit_id in get_full_prompt(entry)


def print_unit_analysis(entries: List[List], unit_id: str, show_prompts: bool = False) -> None:
    analyst_entries = [
        e for e in entries
        if is_analyst_call(e) and unit_is_being_analyzed(e, unit_id)
    ]

    if not analyst_entries:
        print(f"No analyst calls found mentioning unit '{unit_id}'.")
        return

    print(f"Found {len(analyst_entries)} analyst call(s) mentioning unit '{unit_id}'.\n")

    for i, entry in enumerate(analyst_entries, 1):
        ts = get_timestamp(entry)
        cached = ' [CACHED]' if is_cached(entry) else ''
        unit_type, unit_num = get_unit_from_analyst_prompt(entry)
        analyst_label = f"{unit_type} {unit_num}" if unit_type else "(unit unknown)"

        print(f"{'='*70}")
        print(f"Call #{i}  Analyst: {analyst_label}  {ts}{cached}")
        print(f"{'='*70}")

        if show_prompts:
            print("\n--- PROMPT (query portion) ---")
            query = entry[2] if len(entry) > 2 else ''
            # Print last 2000 chars to keep output manageable
            if len(query) > 2000:
                print(f"  [... first {len(query)-2000} chars omitted ...]\n")
                print(query[-2000:])
            else:
                print(query)

        response = get_response(entry)
        actions = parse_actions_from_response(response)

        if actions['facts']:
            print(f"\n  Facts added ({len(actions['facts'])}):")
            for f in actions['facts']:
                src = f"  [{', '.join(f['source'])}]" if f.get('source') else ''
                print(f"    [{f['id']}] {f['content']}{src}")
        else:
            print("\n  Facts added: (none)")

        if actions['questions']:
            print(f"\n  Questions added ({len(actions['questions'])}):")
            for q in actions['questions']:
                src = f"  [{', '.join(q['source'])}]" if q.get('source') else ''
                print(f"    [{q['id']}] {q['content']}{src}")

        if actions['answers']:
            print(f"\n  Answers added ({len(actions['answers'])}):")
            for a in actions['answers']:
                qid = a.get('question_id', '')
                content = a.get('content', '')
                print(f"    [{a['id']}] -> Q:{qid}: {content[:120]}")

        if actions['requests']:
            print(f"\n  Requests ({len(actions['requests'])}):")
            for r in actions['requests']:
                label = r.get('action', 'request')
                detail = f" ({r['detail_level']})" if r.get('detail_level') else ''
                print(f"    [{r['id']}] {label}: {r['target_type']} {r['target_number']}{detail}")

        if not any([actions['facts'], actions['questions'], actions['answers'], actions['requests']]):
            print("\n  (no actions parsed from response)")
            if show_prompts:
                print("\n--- RAW RESPONSE ---")
                print(response[:500])

        print()

    # Also show implicit detection calls that include this unit_id
    detection_entries = [
        e for e in entries
        if is_implicit_detection_call(e) and unit_id in get_full_prompt(e)
    ]
    if detection_entries:
        print(f"\n{'='*70}")
        print(f"Implicit reference detection calls mentioning '{unit_id}': {len(detection_entries)}")
        print(f"{'='*70}")
        for i, entry in enumerate(detection_entries, 1):
            ts = get_timestamp(entry)
            resp = get_response(entry)
            print(f"\nDetection call #{i}  {ts}")
            try:
                m = re.search(r'\{.*\}', resp, re.DOTALL)
                if m:
                    data = json.loads(m.group(0))
                    print(f"  referenced_units: {data.get('referenced_units', [])}")
                else:
                    print(f"  response: {resp[:200]}")
            except json.JSONDecodeError:
                print(f"  response: {resp[:200]}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description='Analyze a question_answering.py log file.'
    )
    parser.add_argument('log_file', help='Path to the log JSON file')
    subparsers = parser.add_subparsers(dest='command', required=True)

    unit_parser = subparsers.add_parser(
        'unit',
        help='Show facts/questions added per pass for a specific unit'
    )
    unit_parser.add_argument('unit_id', help='Unit identifier (e.g., 3A001, 5A001.b)')
    unit_parser.add_argument(
        '--show-prompts', action='store_true',
        help='Also print the query portion of each analyst prompt'
    )

    subparsers.add_parser(
        'compaction',
        help='Show before/after state at each compaction call'
    )

    args = parser.parse_args()

    print(f"Loading log: {args.log_file}")
    entries = list(iter_log_entries(args.log_file))
    print(f"Loaded {len(entries)} log entries.\n")

    if args.command == 'unit':
        print_unit_analysis(entries, args.unit_id, getattr(args, 'show_prompts', False))
    elif args.command == 'compaction':
        print_compaction_analysis(entries)


if __name__ == '__main__':
    main()
