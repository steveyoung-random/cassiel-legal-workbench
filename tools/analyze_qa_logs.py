"""
Analyze Question-Answering Log Files

This tool parses log files from question_answering.py and provides a simplified,
human-readable overview of how the AI analysts are performing their work.

For each AI analyst call, it displays:
- The substantive unit being analyzed (e.g., Section 5, Article 12)
- Number of new facts being added
- Number of new questions being asked
- Specifics of detail requests
- Specifics of questions asked

By default, shows all analyst calls (including those with no actions).
Scoring calls are always excluded as they are not analyst work.

Usage:
    python analyze_qa_logs.py <logfile.json>
    python analyze_qa_logs.py <logfile.json> --verbose
    python analyze_qa_logs.py <logfile.json> --show-all
"""
# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License

import json
import sys
import re
from typing import Dict, List, Any, Optional, Tuple


def extract_unit_designation(prompt_text: str) -> Optional[str]:
    """
    Extract the substantive unit designation from the prompt.

    Looks for patterns like:
    - "Section 5:"
    - "Article 12:"
    - Other organizational unit types

    Returns the designation or None if not found.
    """
    # Look for the "YOUR PORTION TO ANALYZE" section
    portion_match = re.search(
        r'YOUR PORTION TO ANALYZE.*?={70}.*?(\w+)\s+(\d+):',
        prompt_text,
        re.DOTALL
    )
    if portion_match:
        unit_type = portion_match.group(1)
        unit_number = portion_match.group(2)
        return f"{unit_type} {unit_number}"

    # Fallback: look for any pattern like "Section 5:" or "Article 12:"
    fallback_match = re.search(r'((?:Section|Article|Chapter|Part|Title|Subsection)\s+\d+):', prompt_text)
    if fallback_match:
        return fallback_match.group(1)

    return None


def parse_actions(response_text: str) -> Tuple[List[Dict[str, Any]], bool]:
    """
    Parse the AI response to extract actions.

    Returns:
        Tuple of (list of action objects, is_valid_json)
    """
    if not response_text or not response_text.strip():
        return [], False

    # Try to extract JSON from response
    try:
        # First try direct parsing
        data = json.loads(response_text)

        # Handle different response formats
        if isinstance(data, dict) and "actions" in data:
            return data["actions"], True
        elif isinstance(data, list):
            return data, True
        elif isinstance(data, dict):
            # Single action object
            return [data], True

        return [], False

    except json.JSONDecodeError:
        # Try to extract JSON from code blocks or mixed text
        json_match = re.search(r'```(?:json)?\s*(\{.*?\})\s*```', response_text, re.DOTALL)
        if json_match:
            try:
                data = json.loads(json_match.group(1))
                if isinstance(data, dict) and "actions" in data:
                    return data["actions"], True
                return [data], True
            except json.JSONDecodeError:
                pass

        # Try to find JSON object in the text
        json_match = re.search(r'\{.*\}', response_text, re.DOTALL)
        if json_match:
            try:
                data = json.loads(json_match.group(0))
                if isinstance(data, dict) and "actions" in data:
                    return data["actions"], True
                return [data], True
            except json.JSONDecodeError:
                pass

    return [], False


def analyze_actions(actions: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Analyze a list of actions and extract summary information.

    Returns a dictionary with:
    - fact_count: number of facts added
    - question_count: number of questions added
    - answer_count: number of answers added
    - detail_request_count: number of detail requests
    - section_reference_count: number of section references added
    - facts: list of fact contents
    - questions: list of question contents
    - detail_requests: list of detail request descriptions
    """
    summary = {
        'fact_count': 0,
        'question_count': 0,
        'answer_count': 0,
        'detail_request_count': 0,
        'section_reference_count': 0,
        'facts': [],
        'questions': [],
        'answers': [],
        'detail_requests': [],
        'section_references': []
    }

    for action in actions:
        if not isinstance(action, dict) or 'action' not in action:
            continue

        action_type = action['action']

        if action_type == 'add_entry':
            entry = action.get('entry', {})
            entry_type = entry.get('type')
            content = entry.get('content', '')

            if entry_type == 'fact':
                summary['fact_count'] += 1
                summary['facts'].append(content)
            elif entry_type == 'question':
                summary['question_count'] += 1
                importance = entry.get('importance', '')
                importance_str = f" [importance: {importance}]" if importance else ""
                summary['questions'].append(f"{content}{importance_str}")

        elif action_type == 'add_answer':
            summary['answer_count'] += 1
            question_id = action.get('question_id', '')
            answer = action.get('answer', {})
            content = answer.get('content', '')
            summary['answers'].append(f"To {question_id}: {content}")

        elif action_type == 'add_section':
            summary['section_reference_count'] += 1
            question_id = action.get('question_id', '')
            summary['section_references'].append(f"Question {question_id}")

        elif action_type == 'request_detail':
            summary['detail_request_count'] += 1
            target_type = action.get('target_type', '')
            target_number = action.get('target_number', '')
            detail_level = action.get('detail_level', 'summary_2')
            summary['detail_requests'].append(
                f"{target_type} {target_number} ({detail_level})"
            )

    return summary


def format_analyst_report(
    entry_num: int,
    unit_designation: Optional[str],
    summary: Dict[str, Any],
    is_refine: bool,
    is_cached: bool,
    verbose: bool = False
) -> str:
    """
    Format a readable report for a single analyst call.
    """
    lines = []

    # Header
    header = f"Call #{entry_num}"
    if unit_designation:
        header += f" - {unit_designation}"
    if is_refine:
        header += " [REFINEMENT PASS]"
    if is_cached:
        header += " [CACHED]"

    lines.append("=" * 70)
    lines.append(header)
    lines.append("=" * 70)

    # Summary counts
    counts = []
    if summary['fact_count'] > 0:
        counts.append(f"{summary['fact_count']} fact(s)")
    if summary['question_count'] > 0:
        counts.append(f"{summary['question_count']} question(s)")
    if summary['answer_count'] > 0:
        counts.append(f"{summary['answer_count']} answer(s)")
    if summary['detail_request_count'] > 0:
        counts.append(f"{summary['detail_request_count']} detail request(s)")
    if summary['section_reference_count'] > 0:
        counts.append(f"{summary['section_reference_count']} section reference(s)")

    if counts:
        lines.append("Actions: " + ", ".join(counts))
    else:
        lines.append("Actions: No actions taken")

    # Details (only in verbose mode or if there are interesting actions)
    if verbose or summary['question_count'] > 0 or summary['detail_request_count'] > 0:

        if summary['facts'] and verbose:
            lines.append("\nFacts added:")
            for i, fact in enumerate(summary['facts'], 1):
                # Truncate long facts
                truncated = fact[:200] + "..." if len(fact) > 200 else fact
                lines.append(f"  {i}. {truncated}")

        if summary['questions']:
            lines.append("\nQuestions asked:")
            for i, question in enumerate(summary['questions'], 1):
                lines.append(f"  {i}. {question}")

        if summary['detail_requests']:
            lines.append("\nDetail requests:")
            for i, req in enumerate(summary['detail_requests'], 1):
                lines.append(f"  {i}. {req}")

        if summary['answers'] and verbose:
            lines.append("\nAnswers provided:")
            for i, answer in enumerate(summary['answers'], 1):
                # Truncate long answers
                truncated = answer[:200] + "..." if len(answer) > 200 else answer
                lines.append(f"  {i}. {truncated}")

        if summary['section_references'] and verbose:
            lines.append("\nSection references added:")
            for i, ref in enumerate(summary['section_references'], 1):
                lines.append(f"  {i}. {ref}")

    lines.append("")  # Empty line for spacing

    return "\n".join(lines)


def is_refinement_pass(query_prompt: str) -> bool:
    """Check if this is a refinement pass based on the query prompt."""
    return "REFINEMENT PASS" in query_prompt


def is_scoring_call(full_cache: str, query_prompt: str, response: str) -> bool:
    """
    Check if this is a relevance scoring call.

    Scoring calls have distinct characteristics:
    - Prompt asks to "evaluate the likely relevance"
    - Prompt mentions scoring options [0], [1], [2], [3]
    - Response is typically a single digit in brackets
    """
    combined_prompt = full_cache + query_prompt

    # Check for scoring-specific language in the prompt
    scoring_indicators = [
        "evaluate the likely relevance",
        "Please evaluate",
        "Your response must be one of these four options",
        "[0] if the portion appears NOT relevant",
        "[1] if the portion has a LOW probability",
        "[2] if the portion is LIKELY relevant",
        "[3] if the portion is CLEARLY IMPORTANT"
    ]

    for indicator in scoring_indicators:
        if indicator in combined_prompt:
            return True

    # Also check response for scoring pattern
    if '[0]' in response or '[1]' in response or '[2]' in response or '[3]' in response:
        return True

    return False


def analyze_log_file(logfile_path: str, verbose: bool = False, show_all: bool = False) -> None:
    """
    Analyze a log file and print a summary of AI analyst activities.

    Args:
        logfile_path: Path to the log file
        verbose: Show full details of facts and answers
        show_all: Show all calls including scoring and no-action calls
    """
    print(f"\nAnalyzing log file: {logfile_path}\n")

    try:
        with open(logfile_path, 'r', encoding='utf-8') as f:
            content = f.read()
    except FileNotFoundError:
        print(f"Error: Log file not found: {logfile_path}")
        return
    except Exception as e:
        print(f"Error reading log file: {e}")
        return

    # Parse log entries
    # Each entry is a JSON array, but multiple entries may be concatenated
    # We need to split them carefully
    entries = []

    # Try to parse as a single JSON array first
    try:
        entries = [json.loads(content)]
    except json.JSONDecodeError:
        # If that fails, try to find individual JSON arrays
        # Log entries are written as separate JSON arrays, not in a master array
        bracket_count = 0
        start_idx = -1

        for i, char in enumerate(content):
            if char == '[':
                if bracket_count == 0:
                    start_idx = i
                bracket_count += 1
            elif char == ']':
                bracket_count -= 1
                if bracket_count == 0 and start_idx >= 0:
                    try:
                        entry = json.loads(content[start_idx:i+1])
                        entries.append(entry)
                    except json.JSONDecodeError:
                        pass
                    start_idx = -1

    if not entries:
        print("No valid log entries found in the file.")
        return

    print(f"Found {len(entries)} total log entries.")

    # Analyze each entry
    total_facts = 0
    total_questions = 0
    total_answers = 0
    total_detail_requests = 0
    scoring_calls = 0
    cleanup_calls = 0
    no_action_calls = 0
    analyst_calls = 0

    print("Filtering and analyzing entries...\n")

    for entry_num, entry in enumerate(entries, 1):
        if not isinstance(entry, list) or len(entry) < 4:
            continue

        timestamp = entry[0]
        full_cache = entry[1]
        query_prompt = entry[2]
        response = entry[3]
        is_cached = len(entry) > 6 and entry[6] == 'CACHED'

        # Check if this is a scoring call first - skip entirely if so
        if is_scoring_call(full_cache, query_prompt, response):
            scoring_calls += 1
            continue

        # Check if it's a cleanup or final answer call
        # Cleanup calls have top-level "scratch" or "working_answer" keys (not "actions")
        # We need to be careful not to match analyst calls that mention "fact" in actions
        try:
            response_obj = json.loads(response)
            if isinstance(response_obj, dict):
                # Cleanup calls have "scratch" and/or "working_answer" at top level
                # Analyst calls have "actions" at top level
                if ('scratch' in response_obj or 'working_answer' in response_obj) and 'actions' not in response_obj:
                    cleanup_calls += 1
                    if show_all:
                        print(f"Call #{entry_num} - Cleanup/Answer Generation Pass")
                        print("=" * 70)
                        print("")
                    continue
        except json.JSONDecodeError:
            pass

        # Extract unit designation
        unit_designation = extract_unit_designation(full_cache + query_prompt)

        # Check if refinement pass
        is_refine = is_refinement_pass(query_prompt)

        # Parse actions from response
        actions, is_valid = parse_actions(response)

        # Analyze actions
        summary = analyze_actions(actions)

        # Check if this is a no-action analyst call
        has_actions = (summary['fact_count'] > 0 or
                       summary['question_count'] > 0 or
                       summary['answer_count'] > 0 or
                       summary['detail_request_count'] > 0 or
                       summary['section_reference_count'] > 0)

        if not has_actions:
            no_action_calls += 1
            # Don't filter out no-action calls - show them by default

        if has_actions:
            analyst_calls += 1

        # Update totals
        total_facts += summary['fact_count']
        total_questions += summary['question_count']
        total_answers += summary['answer_count']
        total_detail_requests += summary['detail_request_count']

        # Print report
        report = format_analyst_report(
            entry_num,
            unit_designation,
            summary,
            is_refine,
            is_cached,
            verbose
        )
        print(report)

    # Print summary
    print("=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"Total log entries: {len(entries)}")
    print(f"  - Scoring calls (excluded): {scoring_calls}")
    print(f"  - Cleanup calls (excluded): {cleanup_calls}")
    print(f"  - Analyst calls with actions: {analyst_calls}")
    print(f"  - Analyst calls with no actions: {no_action_calls}")
    print()
    print(f"Total facts added: {total_facts}")
    print(f"Total questions asked: {total_questions}")
    print(f"Total answers provided: {total_answers}")
    print(f"Total detail requests: {total_detail_requests}")
    print("")


def main():
    if len(sys.argv) < 2:
        print("Usage: python analyze_qa_logs.py <logfile.json> [options]")
        print("\nOptions:")
        print("  --verbose, -v     Show full text of facts and answers")
        print("  --show-all, -a    Show cleanup/answer generation passes")
        return 1

    logfile_path = sys.argv[1]
    verbose = '--verbose' in sys.argv or '-v' in sys.argv
    show_all = '--show-all' in sys.argv or '-a' in sys.argv

    analyze_log_file(logfile_path, verbose, show_all)
    return 0


if __name__ == '__main__':
    sys.exit(main())
