"""
Stage 4 Question-Answering (question_answering.py)

This module implements the Stage 4 pipeline for iteratively answering questions
about a single processed legal document (v0.3 JSON), using the summaries and
definition/scope information produced by Process_Stage_2.py and Process_Stage_3.py.

High-level responsibilities:
- Load a processed document and a natural-language question.
- Score each operational unit (section, article, etc.) for relevance.
- Run iterative, per-unit/per-chunk AI "analyst" passes that:
  - View a keyhole slice of the document (their own unit/chunk + narrow context).
  - Read and append to a shared scratch document (facts, questions, answers, requests).
- After iterations stabilize, run a separate cleanup / answer-synthesis phase
  to produce a final answer and a pruned scratch document.

CLI usage (mirrors Ask_Question.py):
    python question_answering.py path/to/file.html "What is the question?" 
    python question_answering.py path/to/file.html path/to/question.txt
"""
# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License

from __future__ import annotations

import argparse
import copy
import json
import os
import re
import sys
from typing import Any, Dict, List, Tuple, Optional

from utils import (
    clean_text,
    canonical_org_types,
    iter_operational_items,
    chunk_text,
    build_metadata_suffix,
    augment_chunk_with_metadata,
    create_ai_client,
    QueryWithBaseClient,
    query_json,
    query_text_with_retry,
    InputError,
    ModelError,
    GetLogfile,
    lookup_item,
    has_sub_units,
    get_organizational_item_name_set,
    find_substantive_unit_with_maximum_matching,
)
from utils.text_processing import strip_emphasis_marks
from utils.document_handling import _resolve_param_key


# ---------------------------------------------------------------------------
# Scratch document management
# ---------------------------------------------------------------------------


class ScratchDocumentManager:
    """
    Manage the shared scratch document used by all analyst passes.

    Structure (stored under question_object['scratch']):
      {
        "fact": {
          "fact_001": {
            "content": "Text of fact.",
            "source": ["Section 5"],
            "importance": 2
          },
          ...
        },
        "question": {
          "q_001": {
            "content": "Clarifying question.",
            "source": ["Section 5"],
            "answers": {
              "a_001": {
                "content": "Partial answer.",
                "source": ["Article 12"]
              }
            }
          },
          ...
        },
        "requests": {
          "r_001": {
            "action": "request_detail",
            "target_type": "Section",
            "target_number": "215",
            "detail_level": "summary_2",
            "source": ["Section 5"]
          },
          ...
        }
      }

    This manager enforces an append-only discipline for analyst passes:
    analysts may add entries and answers, but never delete or overwrite
    existing content.
    """

    def __init__(self, question_object: Dict[str, Any], source_doc_label: Optional[str] = None):
        self.question_object = question_object
        self.source_doc_label = source_doc_label
        if "scratch" not in self.question_object:
            self.question_object["scratch"] = {}
        scratch = self.question_object["scratch"]
        if "fact" not in scratch:
            scratch["fact"] = {}
        if "question" not in scratch:
            scratch["question"] = {}
        if "requests" not in scratch:
            scratch["requests"] = {}

    # Convenience property
    @property
    def scratch(self) -> Dict[str, Any]:
        return self.question_object["scratch"]

    # ------------------------------------------------------------------
    # ID helpers
    # ------------------------------------------------------------------

    def _next_id(self, category: str, prefix: str) -> str:
        """
        Generate the next sequential ID for a given scratch category.
        E.g. prefix='fact' in category='fact' -> 'fact_001', 'fact_002', ...
        """
        bucket = self.scratch.get(category, {})
        n = len(bucket) + 1
        candidate = f"{prefix}_{n:03d}"
        while candidate in bucket:
            n += 1
            candidate = f"{prefix}_{n:03d}"
        return candidate

    # ------------------------------------------------------------------
    # Core operations (append-only)
    # ------------------------------------------------------------------

    def add_entry(
        self,
        entry_type: str,
        content: str,
        source_label: str,
        importance: Optional[int] = None,
    ) -> Tuple[bool, Optional[str]]:
        """
        Add a new fact or question entry. ID is auto-generated.

        Returns (True, question_id) if a question was added, (True, None) if a fact was added, (False, None) otherwise.
        """
        if entry_type not in ("fact", "question"):
            return False, None
        if not content.strip():
            return False, None

        if entry_type not in self.scratch:
            self.scratch[entry_type] = {}

        bucket = self.scratch[entry_type]
        new_id = self._next_id(entry_type, entry_type)
        bucket[new_id] = {
            "content": content.strip(),
            "source": [source_label],
        }
        if importance is not None and entry_type == "question":
            # Only questions get importance in this simplified schema.
            bucket[new_id]["importance"] = int(importance)

        # Return question_id if a question was added
        question_id = new_id if entry_type == "question" else None
        return True, question_id

    def add_answer(
        self,
        question_id: str,
        content: str,
        source_label: str,
    ) -> bool:
        """
        Add an answer under an existing question. ID is auto-generated.

        Returns True if an answer was added.
        """
        if "question" not in self.scratch:
            return False
        questions = self.scratch["question"]
        if question_id not in questions:
            # Unknown question id; ignore but do not error.
            return False

        q = questions[question_id]
        if "answers" not in q:
            q["answers"] = {}
        answers = q["answers"]

        n = len(answers) + 1
        new_id = f"answer_{n:03d}"
        while new_id in answers:
            n += 1
            new_id = f"answer_{n:03d}"

        answers[new_id] = {
            "content": content.strip(),
            "source": [source_label],
        }
        return True

    def add_section_reference(self, question_id: str, source_label: str) -> bool:
        """
        Record that a question is also relevant to another section/article.
        Mirrors the 'add_section' action from Ask_Question.py.
        """
        if "question" not in self.scratch:
            return False
        questions = self.scratch["question"]
        if question_id not in questions:
            return False
        q = questions[question_id]
        if "source" not in q:
            q["source"] = []
        if source_label not in q["source"]:
            q["source"].append(source_label)
            return True
        return False

    def add_detail_request(
        self,
        target_type: str,
        target_number: str,
        detail_level: str,
        source_label: str,
    ) -> bool:
        """
        Add a request_detail entry asking for more detail about another unit. ID is auto-generated.
        """
        if not target_type or not target_number or not detail_level:
            return False
        if "requests" not in self.scratch:
            self.scratch["requests"] = {}
        bucket = self.scratch["requests"]
        new_id = self._next_id("requests", "request")
        bucket[new_id] = {
            "action": "request_detail",
            "target_type": target_type,
            "target_number": target_number,
            "detail_level": detail_level,
            "source": [source_label],
        }
        return True

    def add_relevant_section_request(
        self,
        target_type: str,
        target_number: str,
        source_label: str,
    ) -> bool:
        """
        Add a request_relevant_section entry asking that another section be added
        to the analysis because it is highly likely to affect the answer. ID is auto-generated.
        """
        if not target_type or not target_number:
            return False
        if "requests" not in self.scratch:
            self.scratch["requests"] = {}
        bucket = self.scratch["requests"]
        new_id = self._next_id("requests", "request")
        bucket[new_id] = {
            "action": "request_relevant_section",
            "target_type": target_type,
            "target_number": target_number,
            "source": [source_label],
        }
        return True

    # ------------------------------------------------------------------
    # Applying analyst actions
    # ------------------------------------------------------------------

    def apply_action(
        self,
        item_type_name: str,
        item_num: str,
        action_object: Dict[str, Any],
    ) -> Tuple[bool, Optional[str], Optional[str]]:
        """
        Apply a single action object returned by an analyst.

        Supported actions:
          - add_entry
          - add_answer
          - add_section
          - request_detail
          - request_relevant_section

        Returns (changed, question_id_added, question_id_answered):
          - changed: True if the scratch document was modified
          - question_id_added: ID of question added (if any), None otherwise
          - question_id_answered: ID of question answered (if any), None otherwise
        """
        changed = False
        question_id_added = None
        question_id_answered = None
        source_label = f"{item_type_name.capitalize()} {item_num}"
        if self.source_doc_label:
            source_label += f" ({self.source_doc_label})"

        if "action" not in action_object:
            return False, None, None

        action = action_object["action"]

        if action == "add_entry":
            entry = action_object.get("entry", {})
            entry_type = entry.get("type")
            content = entry.get("content", "")
            importance = entry.get("importance")
            added, q_id = self.add_entry(entry_type, content, source_label, importance)
            if added:
                changed = True
                question_id_added = q_id

        elif action == "add_answer":
            question_id = str(action_object.get("question_id", ""))
            answer = action_object.get("answer", {})
            content = answer.get("content", "")
            if self.add_answer(question_id, content, source_label):
                changed = True
                question_id_answered = question_id

        elif action == "add_section":
            question_id = str(action_object.get("question_id", ""))
            if self.add_section_reference(question_id, source_label):
                changed = True

        elif action == "request_detail":
            target_type = str(action_object.get("target_type", "")).strip()
            target_number = str(action_object.get("target_number", "")).strip()
            detail_level = str(action_object.get("detail_level", "")).strip()
            if self.add_detail_request(
                target_type,
                target_number,
                detail_level,
                source_label,
            ):
                changed = True

        elif action == "request_relevant_section":
            target_type = str(action_object.get("target_type", "")).strip()
            target_number = str(action_object.get("target_number", "")).strip()
            if self.add_relevant_section_request(
                target_type,
                target_number,
                source_label,
            ):
                changed = True

        # Unknown actions are ignored (append-only discipline).
        return changed, question_id_added, question_id_answered

    def apply_actions(
        self,
        item_type_name: str,
        item_num: str,
        actions: List[Dict[str, Any]],
    ) -> Tuple[bool, List[str], List[str]]:
        """
        Apply a list of action objects.
        
        Returns (changed, question_ids_added, question_ids_answered):
          - changed: True if any change occurred
          - question_ids_added: List of question IDs that were added
          - question_ids_answered: List of question IDs that were answered
        """
        changed = False
        question_ids_added = []
        question_ids_answered = []
        for action in actions:
            if isinstance(action, dict):
                action_changed, q_id_added, q_id_answered = self.apply_action(item_type_name, item_num, action)
                if action_changed:
                    changed = True
                if q_id_added:
                    question_ids_added.append(q_id_added)
                if q_id_answered:
                    question_ids_answered.append(q_id_answered)
        return changed, question_ids_added, question_ids_answered


# ---------------------------------------------------------------------------
# Context building helpers
# ---------------------------------------------------------------------------


def collect_scoped_definitions_for_qa(
    parsed_content: Dict[str, Any],
    working_item: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """
    Collect definitions that are in scope for the given operational item.

    This is a QA-specific reimplementation (to avoid importing Process_Stage_3.py),
    mirroring the behavior of collect_scoped_definitions() used for level_2_summaries.
    """
    definitions: List[Dict[str, Any]] = []

    if "context" not in working_item:
        raise InputError("collect_scoped_definitions_for_qa: working_item missing context")

    org_context = working_item["context"]

    # Document-wide definitions
    doc_info = parsed_content.get("document_information", {})
    for def_entry in doc_info.get("document_definitions", []):
        if "term" in def_entry and "value" in def_entry:
            entry = dict(def_entry)
            entry["source_kind"] = "document"
            entry["org_depth"] = -1
            definitions.append(entry)

    # Organizational unit definitions along the context path
    if (
        "organization" in doc_info
        and "content" in doc_info["organization"]
    ):
        org_content_pointer = doc_info["organization"]["content"]
        org_name_set = get_organizational_item_name_set(parsed_content)
        depth = 0
        for entry in org_context:
            for org_type, org_number in entry.items():
                if org_type in org_name_set:
                    # Organizational type: walk down the org tree
                    if (
                        org_type in org_content_pointer
                        and org_number in org_content_pointer[org_type]
                    ):
                        org_content_pointer = org_content_pointer[org_type][org_number]
                        for def_entry in org_content_pointer.get("unit_definitions", []):
                            if "term" in def_entry and "value" in def_entry:
                                d = dict(def_entry)
                                d["source_kind"] = "organization"
                                d["org_depth"] = depth
                                definitions.append(d)
                        depth += 1
                else:
                    # Substantive parent type (e.g., "supplement"): look up parent
                    # container and include its defined_terms and ext_definitions
                    _, type_plural = canonical_org_types(org_type)
                    parent_item = lookup_item(parsed_content, type_plural, org_number)
                    if parent_item:
                        for field in ("defined_terms", "ext_definitions"):
                            for def_entry in parent_item.get(field, []):
                                if "term" in def_entry and "value" in def_entry:
                                    d = dict(def_entry)
                                    d["source_kind"] = "organization"
                                    d["org_depth"] = depth
                                    definitions.append(d)
                        depth += 1

    # Item-specific ext_definitions and defined_terms
    for key, source_kind, depth in (
        ("ext_definitions", "item", 999),
        ("defined_terms", "item", 999),
    ):
        for def_entry in working_item.get(key, []):
            if "term" in def_entry and "value" in def_entry:
                d = dict(def_entry)
                d["source_kind"] = source_kind
                d["org_depth"] = depth
                definitions.append(d)

    return definitions


def collect_referenced_summaries_for_qa(
    parsed_content: Dict[str, Any],
    working_item: Dict[str, Any],
    item_type_name: str,
    item_number: str,
    detail_requests: Dict[Tuple[str, str], str] | None = None,
) -> str:
    """
    Collect summary text from referenced sections/articles for QA context.

    - Starts with level 1 summaries.
    - If a detail request exists for a unit with detail_level >= 'summary_2',
      prefer summary_2 when available.
    """
    if "content" not in parsed_content:
        return ""

    content_pointer = parsed_content["content"]
    need_ref = working_item.get("need_ref", [])
    entries: List[Tuple[str, str, str]] = []

    for ref in need_ref:
        if not isinstance(ref, dict):
            continue
        ref_type = ref.get("type")
        ref_value = ref.get("value")
        if not ref_type or not ref_value:
            continue
        if ref_type in ("Need_Definition", "External"):
            continue

        # Skip self references
        if ref_type.lower() == item_type_name.lower() and ref_value == item_number:
            continue

        try:
            type_singular, type_plural = canonical_org_types(ref_type.lower())
        except Exception:
            type_plural = ref_type.lower() + "s"

        ref_item = lookup_item(parsed_content, type_plural, ref_value)
        if ref_item is None:
            continue

        # Determine which summary field to use
        key = (ref_type.lower(), ref_value)
        requested_level = (detail_requests or {}).get(key, "summary_1")
        summary_text = ""
        if requested_level == "summary_2" and ref_item.get("summary_2"):
            summary_text = ref_item["summary_2"]
        elif ref_item.get("summary_1"):
            summary_text = ref_item["summary_1"]

        if summary_text:
            cap_type = ref_type[0].upper() + ref_type[1:] if ref_type else ref_type
            entries.append((cap_type, ref_value, summary_text))

    if not entries:
        return ""

    # Group label by first type
    first_type = entries[0][0]
    section_label = first_type.upper() + "S" if not first_type.endswith("s") else first_type.upper()

    out = [section_label + ":\n"]
    for t, num, text in entries:
        out.append(f"{t} {num}:\n{text}\n")
    return "\n".join(out)


def build_definition_context_for_qa(
    parsed_content: Dict[str, Any],
    working_item: Dict[str, Any],
    item_type_name: str,
    item_number: str,
    include_all_item_definitions: bool = False,
) -> str:
    """
    Build a human-readable block of definitions that are likely relevant to this item.

    We:
      - Collect scoped definitions.
      - Filter to only those whose term appears in need_ref with type == 'Need_Definition'.
      - If include_all_item_definitions is True, include all item-specific definitions
        (ext_definitions and defined_terms) without filtering.
      - Group by term and display their values (with source labels when available).
    """
    definitions = collect_scoped_definitions_for_qa(parsed_content, working_item)
    
    # Collect terms from need_ref for filtering (except item-specific if include_all_item_definitions is True)
    need_terms: List[str] = []
    for ref in working_item.get("need_ref", []):
        if isinstance(ref, dict) and ref.get("type") == "Need_Definition":
            val = strip_emphasis_marks(str(ref.get("value", "")).strip())
            if val:
                need_terms.append(val)

    # Group definitions by lowercased term and source_kind
    term_groups: Dict[str, List[Dict[str, Any]]] = {}
    item_definitions: List[Dict[str, Any]] = []  # Item-specific definitions

    for d in definitions:
        term = d.get("term")
        if not term:
            continue
        key = strip_emphasis_marks(term).lower()
        
        # If chunking is used, collect item-specific definitions separately
        if include_all_item_definitions and d.get("source_kind") == "item":
            item_definitions.append(d)
        else:
            term_groups.setdefault(key, []).append(d)

    # If no definitions at all, return empty
    # But if chunking is used and we have item definitions, include them even without need_terms
    if not need_terms and not item_definitions:
        return ""

    def format_source(def_entry: Dict[str, Any]) -> str:
        value = str(def_entry.get("value", "")).strip()
        if not value:
            return value
        st = def_entry.get("source_type")
        sn = def_entry.get("source_number")
        if st and sn:
            if st.lower() != item_type_name.lower() or sn != item_number:
                cap_type = st[0].upper() + st[1:] if st else st
                return f"[This definition is from {cap_type} {sn}] {value}"
        return value

    lines: List[str] = []
    lines.append("Here are definitions that may be useful context for this request:\n")

    # First, include all item-specific definitions if chunking is used
    if include_all_item_definitions and item_definitions:
        # Group item definitions by term
        item_term_groups: Dict[str, List[Dict[str, Any]]] = {}
        for d in item_definitions:
            term = d.get("term")
            if term:
                key = term.lower()
                item_term_groups.setdefault(key, []).append(d)
        
        # Add all item definitions
        for term_key, group in item_term_groups.items():
            # Use the first term's original casing for display
            display_term = group[0].get("term", "")
            lines.append(f"\"{display_term}\":\n")
            for d in group:
                formatted = format_source(d)
                if formatted:
                    lines.append(f"  - {formatted}\n")
            lines.append("\n")

    # Then, include filtered definitions from need_ref (document-wide and organizational)
    for term in need_terms:
        key = strip_emphasis_marks(term).lower()
        group = term_groups.get(key)
        if not group:
            continue
        lines.append(f"\"{term}\":\n")
        for d in group:
            formatted = format_source(d)
            if formatted:
                lines.append(f"  - {formatted}\n")
        lines.append("\n")

    return "".join(lines)


def _build_parent_definition_block(
    parent_parsed_content: Dict[str, Any],
    child_parsed_content: Dict[str, Any],
    working_item: Dict[str, Any],
) -> str:
    """
    Build a definitions block from the parent document's document_definitions
    for terms needed by working_item that are not covered in the child document.

    Only propagates document-level definitions (those the parent's Stage 2 found
    to have document-wide scope). These correspond to definitions from the
    organizational unit at which the document was split (e.g., title-level or
    chapter-level), and are the most broadly applicable to child documents.

    Returns an empty string if there are no applicable supplemental definitions.
    """
    # Collect need_ref terms from the item
    need_terms: List[str] = []
    for ref in working_item.get("need_ref", []):
        if isinstance(ref, dict) and ref.get("type") == "Need_Definition":
            val = strip_emphasis_marks(str(ref.get("value", "")).strip())
            if val:
                need_terms.append(val)
    if not need_terms:
        return ""

    # Collect terms already covered by the child document's own definitions
    child_definitions = collect_scoped_definitions_for_qa(child_parsed_content, working_item)
    covered_terms: set = {
        strip_emphasis_marks(d.get("term", "")).lower()
        for d in child_definitions
        if d.get("term")
    }

    # Collect parent document-wide definitions for uncovered terms
    parent_doc_info = parent_parsed_content.get("document_information", {})
    parent_doc_defs: List[Dict[str, Any]] = parent_doc_info.get("document_definitions", [])

    # Index parent defs by normalised term
    parent_by_term: Dict[str, List[Dict[str, Any]]] = {}
    for d in parent_doc_defs:
        term = d.get("term")
        if not term or not d.get("value"):
            continue
        key = strip_emphasis_marks(term).lower()
        parent_by_term.setdefault(key, []).append(d)

    lines: List[str] = []
    for need_term in need_terms:
        key = strip_emphasis_marks(need_term).lower()
        if key in covered_terms:
            continue  # already provided by the child document
        group = parent_by_term.get(key)
        if not group:
            continue
        lines.append(f'"{need_term}":\n')
        for d in group:
            value = str(d.get("value", "")).strip()
            if value:
                lines.append(f"  - [Parent document definition] {value}\n")
        lines.append("\n")

    if not lines:
        return ""

    header = "Parent Document Definitions (from the document this was split from):\n"
    return header + "".join(lines)


class ContextBuilder:
    """
    Build staged, keyhole context for each analysis call.

    Responsibilities:
      - Attach primary question text.
      - Attach the item's own text or chunk text (and optional rolling window).
      - Attach scoped definitions filtered by need_ref.
      - Attach summaries of referenced units (level 1 by default, level 2 when requested).
      - Produce cache_prompt_list (static) and leave scratch + iteration hints
        for the dynamic query_prompt.
    """

    def __init__(self, parsed_content: Dict[str, Any], question_text: str,
                 external_doc_label: Optional[str] = None,
                 parent_parsed_content: Optional[Dict[str, Any]] = None):
        self.parsed_content = parsed_content
        self.question_text = question_text
        self.external_doc_label = external_doc_label
        self.parent_parsed_content = parent_parsed_content

    def build_detail_request_map(
        self,
        scratch: Dict[str, Any],
    ) -> Dict[Tuple[str, str], str]:
        """
        Examine scratch['requests'] and determine which units have requested
        additional detail (e.g., summary_2).
        Returns a mapping (type_lower, number) -> detail_level.
        """
        detail_map: Dict[Tuple[str, str], str] = {}
        for req in scratch.get("requests", {}).values():
            if not isinstance(req, dict):
                continue
            if req.get("action") != "request_detail":
                continue
            t = str(req.get("target_type", "")).strip()
            n = str(req.get("target_number", "")).strip()
            level = str(req.get("detail_level", "")).strip() or "summary_2"
            if not t or not n:
                continue
            key = (t.lower(), n)
            # Store the requested detail level (currently only summary_2 is supported)
            detail_map[key] = level
        return detail_map

    def build_cache_components_for_item(
        self,
        working_item: Dict[str, Any],
        item_type_name: str,
        item_number: str,
        scratch: Dict[str, Any],
        score_level: int = 2,
        scratch_snapshot: Optional[Dict[str, Any]] = None,
    ) -> Tuple[List[str], str]:
        """
        Build static cache components and unit-specific context.

        Returns:
            Tuple[List[str], str]: (static_cache_parts, unit_context)
            - static_cache_parts: Truly static content (role, question, action schema)
            - unit_context: Unit-specific context (definitions, summaries) for query prompt

        The caller will append the scratch document to static_cache_parts, then
        put unit_context and chunk text in the query prompt.

        scratch_snapshot is the frozen iteration-start snapshot used for emptiness
        detection. If None, falls back to scratch.
        """
        static_parts: List[str] = []

        # Determine effective snapshot for emptiness check
        eff_snap = scratch_snapshot if scratch_snapshot is not None else scratch
        scratch_is_empty = not eff_snap.get("fact") and not eff_snap.get("question")

        # Role instructions — simplified when scratch is empty (no existing content to reference)
        if scratch_is_empty:
            role_instructions = (
                "You are a legal analyst helping with an iterative analysis of a large "
                'legal document ("Main Document").\n\n'
                "You are given a primary question, some context (definitions and "
                "summaries of referenced units), and a portion of the Main Document "
                'to analyze ("Your Portion").\n\n'
                "Your task is to populate a shared Scratch Document with facts, questions, "
                "and research requests grounded in Your Portion. The Scratch Document is "
                "currently empty — you are among the first analysts to contribute.\n\n"
            )
        else:
            role_instructions = (
                "You are a legal analyst helping with an iterative analysis of a large "
                'legal document ("Main Document").\n\n'
                "You are given a primary question, some context (definitions and "
                "summaries of referenced units), and a portion of the Main Document "
                'to analyze ("Your Portion"). You are also given a shared "Scratch Document" '
                "that other analysts are updating.\n\n"
                "Your task on each iteration is to update the Scratch Document only. "
                "You must NOT remove or overwrite any existing content; you may only "
                "add new facts, questions, answers, section references, or detail "
                "requests that are grounded in Your Portion.\n\n"
            )
        static_parts.append(role_instructions)

        # Question (shared across all items)
        question_block = f"Primary Question:\n{self.question_text}\n\n"
        static_parts.append(question_block)

        # Score-adjusted selectivity guidance
        if score_level >= 3:
            fact_selectivity = (
                "1. Before adding each fact, ask yourself: 'If this fact were absent from my "
                "notes, would the answer to the Primary Question be meaningfully less accurate "
                "or complete?' Only add a fact if the answer is clearly yes. Do not add facts "
                "that are already represented in the scratch document, unless the connection to "
                "Your Portion is particularly strong, such that the final answer will need to "
                "point to Your Portion.\n"
            )
            fact_quantity_note = (
                "IMPORTANT: Be selective — before adding each fact ask yourself whether its "
                "absence would make the answer meaningfully less accurate or complete. Only add "
                "it if the answer is clearly yes.\n"
            )
            question_guidance = (
                "Add a 'question' ONLY when you need to ask other AI analysts about OTHER "
                "substantive units in the document (not Your Portion). Questions are for "
                "learning about content in other sections/articles that may help answer the "
                "Primary Question. Questions should be narrowly tailored to elicit specific "
                "information that is expected to help you contribute to answering the Primary "
                "Question.\n"
                "Do NOT add questions that are substantially restatements of the Primary "
                "Question itself. Such questions lead to redundant answers that duplicate the "
                "facts being collected.\n\n"
            )
        else:
            fact_selectivity = (
                "1. Before adding each fact, ask yourself: 'If this fact were absent from my "
                "notes, would the answer to the Primary Question be meaningfully less accurate "
                "or complete?' Only add a fact if the answer is clearly yes. Limit yourself to "
                "at most 3 new facts unless additional ones are unambiguously essential. Do not "
                "add facts that are only tangentially related or already represented in the "
                "scratch document.\n"
            )
            fact_quantity_note = (
                "IMPORTANT: Be selective — before adding each fact ask yourself whether its "
                "absence would make the answer meaningfully less accurate or complete. Only add "
                "it if the answer is clearly yes, and limit yourself to at most 3 new facts "
                "unless more are unambiguously essential.\n"
            )
            question_guidance = (
                "Add a 'question' ONLY when you need to ask other AI analysts about OTHER "
                "substantive units in the document (not Your Portion), and only when there is "
                "a specific, clear information gap that another unit is likely to fill. You "
                "need not ask any questions if your unit does not present such a gap.\n"
                "Questions should be narrowly tailored to elicit specific information that is "
                "expected to help you contribute to answering the Primary Question.\n"
                "Do NOT add questions that are substantially restatements of the Primary "
                "Question itself. Such questions lead to redundant answers that duplicate the "
                "facts being collected.\n\n"
            )

        # Action schema — two variants: empty scratch (no add_answer/add_section/PRIORITY)
        # and non-empty scratch (full schema with all actions).
        add_entry_block = (
            'To add a new fact or question:\n'
            '{\n'
            '  "action": "add_entry",\n'
            '  "entry": {\n'
            '    "type": "fact" | "question",\n'
            '    "content": "Text of the fact or question.",\n'
            '    "importance": 1 | 2 | 3  // optional, for questions only\n'
            "  }\n"
            "}\n"
        )
        request_detail_block = (
            "To request a more detailed summary of another substantive unit:\n"
            "{\n"
            '  "action": "request_detail",\n'
            '  "target_type": "Section" | "Article" | "...",\n'
            '  "target_number": "201",\n'
            '  "detail_level": "summary_2"\n'
            "}\n"
            "Use this action when you need a general understanding of another substantive unit. "
            "A basic summary (summary_1) may already be provided in the context above. If you need "
            "more detail than has been provided, request summary_2. The requested summary will be added to your context "
            "in future iterations.\n"
            "If you need specific information from another unit, use a 'question' instead of "
            "requesting a summary.\n\n"
        )
        request_relevant_section_block = (
            "To add a new substantive unit to the analysis (one not currently being analyzed):\n"
            "{\n"
            '  "action": "request_relevant_section",\n'
            '  "target_type": "Section" | "Article" | "...",\n'
            '  "target_number": "201"\n'
            "}\n"
            "Use this action when the Primary Question cannot be fully answered without knowing the specific "
            "content of another unit — its actual parameters, thresholds, or listed items. A separate analyst "
            "will be assigned to that unit. Do not request a unit simply because Your Portion mentions it; "
            "only request it when that unit's actual content is necessary to answer the question.\n\n"
        )

        if scratch_is_empty:
            action_instructions = (
                "You will respond with a single JSON object with a top-level key "
                + '"actions" containing a list of zero or more action objects. Each '
                + "action object must be one of the following forms:\n\n"
                + add_entry_block
                + fact_selectivity
                + "2. Do NOT add facts that merely state Your Portion lacks relevant information (e.g., \"Section X does not mention Y\"). Add only positive information that helps answer the question.\n\n"
                + fact_quantity_note
                + question_guidance
                + request_detail_block
                + request_relevant_section_block
                + "Rules:\n"
                + "- Base all new facts, questions, and requests ONLY on Your Portion.\n"
                + "- Use the context (definitions and summaries) only to help you interpret "
                + "Your Portion, not as an independent source.\n"
                + "- Do NOT ask questions about Your Portion. Questions should ONLY be about OTHER "
                + "substantive units in the document.\n"
                + "- To learn about other substantive units, you have two options:\n"
                + "  1. Ask a specific question if you need targeted information\n"
                + "  2. Request summary_2 if you need a general understanding of another unit\n"
                + "- Do NOT request detail (summary_2) for Your Portion - you already have the full text.\n"
                + "- After adding a fact that references another unit by name or number and states that "
                + "the answer depends on that unit's specific parameters, thresholds, or listed items "
                + "(e.g., 'applies to items meeting the parameters of Section X', 'see Section X for "
                + "applicable thresholds'), immediately check: does the Primary Question require knowing "
                + "that unit's actual content? If yes, use 'request_relevant_section' to add it.\n"
                + "- If you have no useful actions, return: {\"actions\": []}\n\n"
            )
        else:
            add_answer_block = (
                "To add an answer to an existing question:\n"
                "{\n"
                '  "action": "add_answer",\n'
                '  "question_id": "existing_question_id",\n'
                '  "answer": {\n'
                '    "content": "Text of the answer."\n'
                "  }\n"
                "}\n"
                "IMPORTANT: Each answer gets its own unique ID. Multiple analysts can answer the same question.\n"
                "- By default, provide ONLY NEW information - do not repeat what previous answers already stated.\n"
                "- Do NOT answer just to state that Your Portion does not address the question, UNLESS the question "
                "specifically asks about Your Portion by name (e.g., \"Does Section 123 mention X?\" when you are analyzing Section 123). "
                "In such cases, a negative answer is appropriate.\n"
                "- If a previous answer is WRONG based on Your Portion, create a new answer that:\n"
                "  1. States the correct information from Your Portion\n"
                "  2. Explicitly notes the conflict with the earlier answer (e.g., \"Note: This contradicts "
                "the earlier answer regarding X because...\")\n"
                "- If a previous answer is INCOMPLETE but correct, just add the missing information without "
                "restating what was already said.\n"
                "- Later analysts will see all answers in sequence, so the back-and-forth dialogue is preserved.\n\n"
            )
            add_section_block = (
                "To indicate that an existing question also arises from this portion:\n"
                "{\n"
                '  "action": "add_section",\n'
                '  "question_id": "existing_question_id"\n'
                "}\n\n"
            )
            action_instructions = (
                "PRIORITY: Before adding new facts, carefully review the Scratch Document:\n"
                + "1. FIRST, check if Your Portion can answer any existing questions in the scratch document. If so, use 'add_answer' actions BEFORE adding any new facts.\n"
                + fact_selectivity.replace("1. ", "2. ", 1)
                + "3. Do NOT add facts that merely state Your Portion lacks relevant information (e.g., \"Section X does not mention Y\"). Add only positive information that helps answer the question.\n"
                + "4. Prefer answering existing questions over generating new facts whenever Your Portion contains relevant information.\n\n"
                + "You will respond with a single JSON object with a top-level key "
                + '"actions" containing a list of zero or more action objects. Each '
                + "action object must be one of the following forms:\n\n"
                + add_entry_block
                + fact_quantity_note
                + question_guidance
                + add_answer_block
                + add_section_block
                + request_detail_block
                + request_relevant_section_block
                + "Rules:\n"
                + "- ALWAYS review the Scratch Document first to identify opportunities to answer existing questions.\n"
                + "- Base all new facts, questions, answers, and requests ONLY on Your Portion.\n"
                + "- Use the context (definitions and summaries) only to help you interpret "
                + "Your Portion, not as an independent source.\n"
                + "- Do NOT ask questions about Your Portion. Questions should ONLY be about OTHER "
                + "substantive units in the document.\n"
                + "- To learn about other substantive units, you have two options:\n"
                + "  1. Ask a specific question if you need targeted information\n"
                + "  2. Request summary_2 if you need a general understanding of another unit\n"
                + "- Do NOT request detail (summary_2) for Your Portion - you already have the full text.\n"
                + "- Do not restate existing scratch content unless a significantly clearer "
                + "or more precise formulation is needed.\n"
                + "- After adding a fact that references another unit by name or number and states that "
                + "the answer depends on that unit's specific parameters, thresholds, or listed items "
                + "(e.g., 'applies to items meeting the parameters of Section X', 'see Section X for "
                + "applicable thresholds'), immediately check: does the Primary Question require knowing "
                + "that unit's actual content? If yes, use 'request_relevant_section' to add it.\n"
                + "- If you have no useful actions, return: {\"actions\": []}\n\n"
            )
        static_parts.append(action_instructions)

        # Build unit-specific context (definitions and summaries) for query prompt
        unit_context_parts = []

        # Cross-document unit header (shown instead of primary-doc framing)
        if self.external_doc_label:
            unit_context_parts.append(
                f"CROSS-DOCUMENT UNIT\n"
                f"This unit is from an external document ({self.external_doc_label}). "
                f"Analyze it in the context of the primary question and contribute "
                f"findings to the shared scratch document.\n\n"
            )

        # Definitions
        # Check if chunking will be used: if item has breakpoints and text is long enough
        text = working_item.get("text", "")
        breakpoints = working_item.get("breakpoints", [])
        will_be_chunked = False
        if len(breakpoints) > 0 and len(text) > 0:
            # Check if chunking would produce multiple chunks
            chunks = list(chunk_text(text, breakpoints, preferred_length=15000))
            will_be_chunked = len(chunks) > 1

        # Unit overview for multi-chunk items (orientation only — before definitions)
        if will_be_chunked:
            unit_title = working_item.get("unit_title", "")
            unit_summary = working_item.get("summary_1", "")
            if unit_summary:
                overview_label = f"{item_type_name.capitalize()} {item_number}"
                if unit_title:
                    overview_label += f": {unit_title}"
                unit_context_parts.append(
                    "UNIT OVERVIEW (for orientation only — do not extract facts from this summary):\n"
                    + overview_label + "\n"
                    + unit_summary + "\n\n"
                )

        def_block = build_definition_context_for_qa(
            self.parsed_content,
            working_item,
            item_type_name,
            item_number,
            include_all_item_definitions=will_be_chunked,
        )
        if def_block:
            unit_context_parts.append(def_block + "\n")

        # Parent document definitions (Task 3.10): supplement with document-wide
        # definitions from the parent document (the split source) for need_ref terms
        # not already covered by the child document's own definitions.
        if self.parent_parsed_content and not self.external_doc_label:
            parent_def_block = _build_parent_definition_block(
                self.parent_parsed_content,
                self.parsed_content,
                working_item,
            )
            if parent_def_block:
                unit_context_parts.append(parent_def_block + "\n")

        # Parent container framing context for sub-units
        # Walk working_item["context"]; entries that are substantive types (not org types)
        # identify parent containers whose text frames the current item.
        # Include all such ancestors' text; fall back to summary_1 if total is too long.
        parent_org_name_set = get_organizational_item_name_set(self.parsed_content)
        parent_containers = []  # list of (ctx_type, ctx_id, parent_item)
        for ctx_entry in working_item.get("context", []):
            for ctx_type, ctx_id in ctx_entry.items():
                if ctx_type not in parent_org_name_set:
                    _, type_plural = canonical_org_types(ctx_type)
                    parent_item = lookup_item(self.parsed_content, type_plural, str(ctx_id))
                    if parent_item:
                        parent_containers.append((ctx_type, str(ctx_id), parent_item))

        if parent_containers:
            _PARENT_TEXT_LIMIT = 3000
            raw_texts = [p[2].get("text", "").strip() for p in parent_containers]
            use_summaries = sum(len(t) for t in raw_texts) > _PARENT_TEXT_LIMIT

            parent_blocks = []
            for (ctx_type, ctx_id, parent_item), raw_text in zip(parent_containers, raw_texts):
                if use_summaries:
                    content = parent_item.get("summary_1", "").strip() or raw_text
                else:
                    content = raw_text
                if content:
                    parent_blocks.append(
                        f"Parent container ({ctx_type} {ctx_id}):\n{content}"
                    )

            if parent_blocks:
                content_desc = "summaries" if use_summaries else "introductory text"
                unit_context_parts.append(
                    "PARENT CONTAINER CONTEXT\n"
                    f"The following {content_desc} from parent containers frames Your Portion:\n\n"
                    + "\n\n".join(parent_blocks) + "\n\n"
                )

        # Referenced summaries (level 1 by default, level 2 when requested)
        detail_map = self.build_detail_request_map(scratch)
        ref_block = collect_referenced_summaries_for_qa(
            self.parsed_content,
            working_item,
            item_type_name,
            item_number,
            detail_requests=detail_map,
        )
        if ref_block:
            context_header = (
                "Here are summaries of substantive units that may be useful context "
                "for this request:\n\n"
            )
            unit_context_parts.append(context_header + ref_block + "\n")

        unit_context = "".join(unit_context_parts)
        return static_parts, unit_context


# ---------------------------------------------------------------------------
# Item and chunk analysis
# ---------------------------------------------------------------------------


class ChunkAnalyzer:
    """
    Handle analysis of chunked items, including rolling-window context.
    """

    def __init__(
        self,
        client,
        logfile: str,
        context_builder: ContextBuilder,
        scratch_manager: ScratchDocumentManager,
        question_object: Dict[str, Any],
        parsed_content: Dict[str, Any],
        scratch_snapshot: Dict[str, Any],
    ):
        self.client = client
        self.logfile = logfile
        self.context_builder = context_builder
        self.scratch_manager = scratch_manager
        self.question_object = question_object
        self.parsed_content = parsed_content
        self.scratch_snapshot = scratch_snapshot

    def _build_chunk_prompt(
        self,
        static_cache: List[str],
        unit_context: str,
        chunk_text_str: str,
        prev_chunk_summary: Optional[str],
        item_type_name: str,
        item_number: str,
        refine: bool,
        unit_title: str = "",
        chunk_idx: int = 0,
        n_chunks: int = 1,
        prior_chunk_additions: Optional[Dict[str, Any]] = None,
    ) -> Tuple[List[str], str]:
        """
        Build cache_prompt_list and query_prompt for a single chunk.

        Optimized cache strategy for Anthropic prompt caching:
        - Cache list (multiple cache breakpoints):
          1. Static instructions (role, question, action schema)
          2. Scratch document (grows over time, but prefix stays stable)
        - Query prompt (not cached, changes every call):
          1. Chunk framing note (when n_chunks > 1)
          2. Unit-specific context (definitions, summaries)
          3. Prior chunk findings (when chunk_idx > 0 and earlier chunks contributed)
          4. Chunk-specific context and text to analyze
          5. Refine instructions (if applicable)
          6. Final response instruction

        This structure maximizes cache hits because:
        - Static instructions never change
        - Scratch document grows by appending, so prefix remains cacheable
        - Unit-specific and chunk-specific content in non-cached portion
        """
        # Start with static cache (role, question, action schema)
        full_cache = list(static_cache)

        # Add scratch document to cache (as separate strings for cache breakpoints)
        # This goes at the end of cache so it can grow while keeping prefix cached
        # Exclude 'requests' bucket: it's pipeline infrastructure (detail/section requests)
        # that analysts cannot act on; showing it wastes tokens and may cause confusion.
        scratch_is_empty = not self.scratch_snapshot.get("fact") and not self.scratch_snapshot.get("question")

        full_cache.append("=" * 70 + "\n")
        full_cache.append("CURRENT SCRATCH DOCUMENT\n")
        full_cache.append("=" * 70 + "\n")
        if scratch_is_empty:
            full_cache.append("(Empty — no entries yet.)\n")
        else:
            scratch_for_display = {k: v for k, v in self.scratch_snapshot.items() if k != "requests"}
            scratch_json = json.dumps(scratch_for_display, indent=4)
            full_cache.append(scratch_json + "\n")
        full_cache.append("=" * 70 + "\n")
        if not scratch_is_empty:
            full_cache.append("END OF SCRATCH DOCUMENT\n")
            full_cache.append("=" * 70 + "\n")
        full_cache.append("\n")

        # Build query prompt with unit-specific and chunk-specific content (not cached)
        prompt = []

        # Chunk framing note (when multi-chunk unit)
        if n_chunks > 1:
            unit_label = f"{item_type_name.capitalize()} {item_number}"
            prompt.append(
                f"CONTEXT: {unit_label} is a long unit that has been divided into {n_chunks} portions "
                f"for parallel analysis. You are examining portion {chunk_idx + 1} of {n_chunks}. "
                f"Each portion is being assigned to a separate analyst; the other {n_chunks - 1} portion"
                f"{'s are' if n_chunks - 1 != 1 else ' is'} being handled by other analysts in parallel.\n\n"
                f"A high-level summary of the full {unit_label} is provided below under UNIT OVERVIEW "
                f"so you can orient yourself within the larger unit. Use it only for orientation — "
                f"extract facts and questions solely from \"Your Portion\" at the end of this prompt.\n\n"
                f"Because the other portions of {unit_label} are already being covered, do not use "
                f"request_detail or request_relevant_section for {unit_label} itself.\n\n"
            )

        # Unit-specific context (definitions and summaries - changes per unit)
        if unit_context:
            prompt.append(unit_context)

        # Prior chunk findings (when later chunks of a multi-chunk unit)
        if chunk_idx > 0 and prior_chunk_additions:
            has_facts = bool(prior_chunk_additions.get("fact"))
            has_questions = bool(prior_chunk_additions.get("question"))
            if has_facts or has_questions:
                has_prior_questions = has_questions
                answer_reminder = (
                    "\nIf you can answer any question below, use:\n"
                    '  {"action": "add_answer", "question_id": "<id>", "answer": {"content": "..."}}\n'
                ) if has_prior_questions else ""

                filtered = {k: v for k, v in prior_chunk_additions.items() if v}
                prompt.append("=" * 70 + "\n")
                prompt.append(
                    f"EARLIER PORTIONS OF {item_type_name.upper()} {item_number} — ANALYST FINDINGS\n"
                )
                prompt.append("=" * 70 + "\n")
                prompt.append(
                    f"The analysts who examined the earlier portions of "
                    f"{item_type_name.capitalize()} {item_number} identified the following. "
                    f"Do not duplicate them.{answer_reminder}\n"
                )
                # Keep serialized ordering deterministic across runs to maximize local cache hits.
                prompt.append(json.dumps(filtered, indent=2, sort_keys=True) + "\n")
                prompt.append("=" * 70 + "\n")
                prompt.append(
                    f"END OF EARLIER PORTIONS FINDINGS\n"
                )
                prompt.append("=" * 70 + "\n\n")

        # Refine instructions (dynamic - only appears on refine passes)
        if refine:
            prompt.append(
                "REFINEMENT PASS: The Scratch Document may already contain "
                "most useful content for this portion. Your priority should be:\n"
                "1. Answer any existing questions that Your Portion can address (use 'add_answer' actions).\n"
                "2. Only add new facts if they are critical and clearly missing from the scratch document.\n"
                "3. Be biased toward returning an empty list of actions if nothing important is missing.\n\n"
            )

        # Previous chunk context (changes per chunk)
        if prev_chunk_summary:
            prompt.append(
                "Context from earlier parts of this unit (for understanding only; "
                "do NOT extract facts/questions from this summary):\n"
            )
            prompt.append(prev_chunk_summary + "\n\n")

        # The text to analyze (changes every call)
        # Use clear, prominent labeling to indicate this is THE text to analyze
        prompt.append("=" * 70 + "\n")
        prompt.append("YOUR PORTION TO ANALYZE\n")
        prompt.append("=" * 70 + "\n")
        item_label = f"{item_type_name.capitalize()} {item_number}"
        if unit_title:
            item_label += f": {unit_title}"
        prompt.append(f"{item_label}:\n\n")
        prompt.append(chunk_text_str.strip() + "\n")
        prompt.append("=" * 70 + "\n")
        prompt.append("END OF YOUR PORTION\n")
        prompt.append("=" * 70 + "\n\n")

        prompt.append(
            "Respond with your JSON object containing the list of actions.\n\n"
            "Before responding, remember that if you cannot explain why an element "
            "that you are returning will help answer the Primary Question or another "
            "question in the Scratch Document, then you should not include that element "
            "in your response.\n"
  
        )

        query_prompt = "".join(prompt)
        return full_cache, query_prompt

    def analyze_chunks(
        self,
        working_item: Dict[str, Any],
        item_type_name: str,
        item_number: str,
        refine: bool,
        score_level: int = 2,
    ) -> Tuple[bool, bool, List[str], List[str]]:
        """
        Analyze the item's text in chunks.
        
        Returns:
            Tuple[bool, bool, List[str], List[str]]: (changed, had_actions, question_ids_added, question_ids_answered)
            - changed: True if the scratch document was updated
            - had_actions: True if any non-empty actions were returned
            - question_ids_added: List of question IDs that were added
            - question_ids_answered: List of question IDs that were answered
        """
        text = working_item.get("text", "")
        if not text:
            # Check whether this is a data-table sub-unit (data_table: 1 flag in parameters).
            # These sub-units have text="" but summary_1 set by Stage 3.  Use the summary so
            # the analyst produces a pointer-style response rather than receiving an empty prompt.
            _params = self.parsed_content.get('document_information', {}).get('parameters', {})
            _is_data_table = any(p.get('name') == item_type_name and p.get('data_table') and p.get('is_sub_unit')
                                 for p in _params.values())
            if _is_data_table:
                text = working_item.get("summary_1", "")
        breakpoints = working_item.get("breakpoints", [])

        # Get plural form of item type for duplicate checking
        item_type_names = None
        content_pointer = None
        if 'document_information' in self.parsed_content and 'parameters' in self.parsed_content['document_information']:
            param_pointer = self.parsed_content['document_information']['parameters']
            for item_type_key, params in param_pointer.items():
                if params.get('name') == item_type_name:
                    item_type_names = params.get('name_plural')
                    break
        if 'content' in self.parsed_content:
            content_pointer = self.parsed_content['content']

        # Build metadata suffix for sections in duplicate sets
        metadata_suffix = build_metadata_suffix(item_number, working_item, content_pointer, item_type_names)

        # Build cache components once per item.
        # Returns: (static_cache, unit_context)
        static_cache, unit_context = self.context_builder.build_cache_components_for_item(
            working_item,
            item_type_name,
            item_number,
            self.question_object.get("scratch", {}),
            score_level=score_level,
            scratch_snapshot=self.scratch_snapshot,
        )

        chunks = list(chunk_text(text, breakpoints, preferred_length=15000))
        n_chunks = len(chunks)
        if not chunks:
            return False, False

        changed_any = False
        had_actions_any = False
        question_ids_added = []
        question_ids_answered = []
        prev_summary = None  # We could synthesize or reuse summaries in future.

        # Track facts and questions added by earlier chunks of this item (for later chunks)
        prior_chunk_additions: Dict[str, Any] = {"fact": {}, "question": {}}

        for idx, chunk in enumerate(chunks):
            # Augment chunk with metadata (for _dup sections)
            augmented_chunk = augment_chunk_with_metadata(chunk, metadata_suffix)

            cache_prompt_list, query_prompt = self._build_chunk_prompt(
                static_cache,
                unit_context,
                augmented_chunk,  # Use augmented chunk instead of plain chunk
                prev_summary,
                item_type_name,
                item_number,
                refine,
                unit_title=working_item.get("unit_title", ""),
                chunk_idx=idx,
                n_chunks=n_chunks,
                prior_chunk_additions=prior_chunk_additions,
            )

            # Snapshot keys before apply_actions to detect new additions
            if n_chunks > 1:
                facts_before = set(self.scratch_manager.scratch.get("fact", {}).keys())
                questions_before = set(self.scratch_manager.scratch.get("question", {}).keys())

            # For now, rely on API-level caching; we do our own high-level
            # grouping in QueryWithBaseClient.
            try:
                result_obj = query_json(self.client, cache_prompt_list, query_prompt, self.logfile,
                                        max_tokens=16000)
            except ModelError as e:
                raise ModelError(
                    f"ChunkAnalyzer: invalid JSON response for {item_type_name} "
                    f"{item_number}, chunk {idx+1}: {e}"
                )

            # Expect top-level {"actions": [...]} but be forgiving.
            actions = []
            if isinstance(result_obj, dict):
                if "actions" in result_obj and isinstance(result_obj["actions"], list):
                    actions = result_obj["actions"]
                else:
                    # Single action object
                    actions = [result_obj]
            elif isinstance(result_obj, list):
                actions = result_obj

            # Check if we had any non-empty actions
            if actions and len(actions) > 0:
                had_actions_any = True

            changed, q_ids_added, q_ids_answered = self.scratch_manager.apply_actions(
                item_type_name, item_number, actions
            )
            if changed:
                changed_any = True
                question_ids_added.extend(q_ids_added)
                question_ids_answered.extend(q_ids_answered)

            # Track newly added facts and questions for subsequent chunks
            if n_chunks > 1:
                new_fact_ids = sorted(set(self.scratch_manager.scratch.get("fact", {}).keys()) - facts_before)
                for fid in new_fact_ids:
                    prior_chunk_additions["fact"][fid] = self.scratch_manager.scratch["fact"][fid]
                new_question_ids = sorted(
                    set(self.scratch_manager.scratch.get("question", {}).keys()) - questions_before
                )
                for qid in new_question_ids:
                    prior_chunk_additions["question"][qid] = self.scratch_manager.scratch["question"][qid]

        return changed_any, had_actions_any, question_ids_added, question_ids_answered


class ItemAnalyzer:
    """
    Analyze a single operational item, delegating to ChunkAnalyzer when needed.
    """

    def __init__(
        self,
        client,
        logfile: str,
        parsed_content: Dict[str, Any],
        question_object: Dict[str, Any],
        scratch_snapshot: Dict[str, Any],
        external_doc_label: Optional[str] = None,
        parent_parsed_content: Optional[Dict[str, Any]] = None,
    ):
        self.client = client
        self.logfile = logfile
        self.parsed_content = parsed_content
        self.question_object = question_object
        self.scratch_snapshot = scratch_snapshot
        self.external_doc_label = external_doc_label
        self.parent_parsed_content = parent_parsed_content

    def analyze_item(
        self,
        item_type_name: str,
        item_type_name_plural: str,
        item_number: str,
        working_item: Dict[str, Any],
        question_text: str,
        refine: bool,
        score_level: int = 2,
    ) -> Tuple[bool, bool, List[str], List[str]]:
        """
        Analyze a single item.
        
        Returns:
            Tuple[bool, bool, List[str], List[str]]: (changed, had_actions, question_ids_added, question_ids_answered)
            - changed: True if scratch was updated
            - had_actions: True if any non-empty actions were returned
            - question_ids_added: List of question IDs that were added
            - question_ids_answered: List of question IDs that were answered
        """
        scratch_manager = ScratchDocumentManager(self.question_object,
                                                   source_doc_label=self.external_doc_label)
        context_builder = ContextBuilder(self.parsed_content, question_text,
                                         external_doc_label=self.external_doc_label,
                                         parent_parsed_content=self.parent_parsed_content)
        chunk_analyzer = ChunkAnalyzer(
            self.client,
            self.logfile,
            context_builder,
            scratch_manager,
            self.question_object,
            self.parsed_content,
            self.scratch_snapshot,
        )

        # Currently, we always route through ChunkAnalyzer; it will treat
        # single-chunk items as a trivial case.
        return chunk_analyzer.analyze_chunks(
            working_item,
            item_type_name,
            item_number,
            refine,
            score_level=score_level,
        )


# ---------------------------------------------------------------------------
# Sub-unit scoring helpers
# ---------------------------------------------------------------------------


def _iter_text_bearing_sub_units(param_pointer, item_data):
    """
    Recursively yield all sub-units that have non-empty text at any nesting depth.

    Yields (type_key_str, type_name, sub_num, sub_data) for each text-bearing sub-unit.
    Unlike the old _iter_leaf_scores, this also yields non-leaf sub-units that have
    text (e.g., a method_section that contains nested table sub-units).  Data-table
    and other text-less sub-units are skipped — they are not scored or analyzed as
    prose; their summary_1 is available to parent/sibling analysts via placeholders
    in the parent's text field.
    """
    for sub_type_key, sub_type_items in item_data.get("sub_units", {}).items():
        sub_p = _resolve_param_key(param_pointer, sub_type_key)
        if not sub_p:
            continue
        sub_type_key_str = str(sub_type_key)
        sub_type_name = sub_p.get("name", "")
        for sub_num, sub_data in sub_type_items.items():
            if sub_data.get("text"):
                yield (sub_type_key_str, sub_type_name, sub_num, sub_data)
            if has_sub_units(sub_data):
                yield from _iter_text_bearing_sub_units(param_pointer, sub_data)


# ---------------------------------------------------------------------------
# Question processor and top-level orchestration
# ---------------------------------------------------------------------------


class QuestionProcessor:
    """
    Top-level controller for a single (document, question) pair.
    """

    def __init__(
        self,
        client,
        parsed_content: Dict[str, Any],
        question_object: Dict[str, Any],
        question_file: str,
        logfile: str,
        progress_callback=None,
        config: Optional[Dict[str, Any]] = None,
        mode: Optional[str] = None,
        processed_file_path: Optional[str] = None,
    ):
        self.client = client
        self.parsed_content = parsed_content
        self.question_object = question_object
        self.question_file = question_file
        self.logfile = logfile
        self.progress_callback = progress_callback  # Optional callback for progress reporting
        self._config = config  # Store config for phase-specific client selection

        # Load Q&A mode configuration
        from utils.config import get_qa_mode_config
        self.mode_config = get_qa_mode_config(mode_name=mode, config=config)
        self.mode_name = mode  # Store mode name for reference

        if "question" not in self.question_object or "text" not in self.question_object["question"]:
            raise InputError("QuestionProcessor: question text missing in question_object.")
        if "scores" not in self.question_object:
            self.question_object["scores"] = {}
        if "scratch" not in self.question_object:
            self.question_object["scratch"] = {}
        if "working_answer" not in self.question_object:
            self.question_object["working_answer"] = {"text": ""}

        # Initialize skip list for optimization (units that can be skipped)
        if "skip_list" not in self.question_object:
            self.question_object["skip_list"] = []
        
        # Initialize progress tracking for idempotency
        if "progress" not in self.question_object:
            self.question_object["progress"] = {
                "scoring_complete": False,
                "analysis_iterations_completed": 0,
                "cleanup_complete": False,
                "quality_check_complete": False,
                "final_answer_complete": False,
            }
        
        # Cache for task-specific clients (created on demand)
        self._task_clients = {}

        # Cross-document unit analysis (Task 3.7)
        self._processed_file_path = processed_file_path
        self._external_documents: Dict[str, Dict] = {}        # file_path → parsed_content cache
        self._item_level_refs: List[Dict] = []                # item-level registry refs, loaded once
        self._ext_doc_refs: Dict[str, List[Dict]] = {}        # per-external-doc registry refs cache
        if "cross_doc_scores" not in self.question_object:
            self.question_object["cross_doc_scores"] = {}
        try:
            from registry.stage4_integration import (
                load_item_level_refs_for_document,
                load_parent_document_for_qa,
            )
            self._item_level_refs = load_item_level_refs_for_document(
                processed_file_path or "", self._config or {}
            )
            # Parent document definitions (Task 3.10)
            self._parent_parsed_content: Optional[Dict[str, Any]] = (
                load_parent_document_for_qa(processed_file_path or "", self._config or {})
            )
        except Exception:
            self._item_level_refs = []
            self._parent_parsed_content = None

        # Tracks fact IDs that have been through at least one compaction in the current
        # run_analysis_iteration call. Used by _detect_implicit_references(pending_only=True)
        # to restrict scanning to facts added since the last compaction.
        self._compacted_fact_ids: set = set()

    # Convenience
    @property
    def question_text(self) -> str:
        return self.question_object["question"]["text"]

    # ------------------------------------------------------------------
    # Cross-document unit helpers (Task 3.7)
    # ------------------------------------------------------------------

    def _load_external_document(self, file_path: str) -> Optional[Dict]:
        """Load and cache an external processed JSON. Returns None on error."""
        if file_path in self._external_documents:
            return self._external_documents[file_path]
        try:
            import json as _json
            with open(file_path, 'r', encoding='utf-8') as f:
                doc = _json.load(f)
            self._external_documents[file_path] = doc
            return doc
        except Exception as e:
            print(f"  Warning: could not load external document {file_path}: {e}")
            return None

    def _add_cross_doc_unit(self, file_path: str, item_type_name: str,
                            item_number: str, score: int = 2) -> bool:
        """
        Add a cross-doc unit to cross_doc_scores.
        Returns True if newly added (False if already present).
        item_type_name is the singular type name (e.g., 'section').
        """
        cds = self.question_object.setdefault("cross_doc_scores", {})
        file_scores = cds.setdefault(file_path, {})
        type_scores = file_scores.setdefault(item_type_name, {})
        if item_number in type_scores:
            return False
        type_scores[item_number] = score
        return True

    def _initialize_cross_document_units(self) -> None:
        """
        Pre-load cross-doc units from the registry for all scored primary items.
        Called once after score_relevance() completes, before the first iteration.
        Idempotent: existing cross_doc_scores entries are not duplicated.
        """
        if not self._item_level_refs:
            return

        # Build a reverse map: type name (lowercase) → param key, for score lookups.
        # scores uses param keys (e.g. "1"); registry stores type names (e.g. "section").
        param_pointer = self.parsed_content.get("document_information", {}).get("parameters", {})
        name_to_param_key = {
            pd.get("name", "").lower(): str(pk)
            for pk, pd in param_pointer.items()
            if isinstance(pd, dict) and pd.get("name")
        }

        added = 0
        for ref in self._item_level_refs:
            src_type_name = ref['source_item_type']   # e.g., 'section' (name, not param key)
            src_num = ref['source_item_number']

            # Translate type name to param key used in scores
            src_param_key = name_to_param_key.get(src_type_name.lower())
            if src_param_key is None:
                continue

            # Only load if the source unit was actually scored
            src_score = (
                self.question_object.get("scores", {})
                .get(src_param_key, {})
                .get(src_num)
            )
            if not src_score:
                continue

            tgt_file = ref['target_file_path']
            tgt_type = ref['target_item_type']   # e.g., 'section'
            tgt_num = ref['target_item_number']

            ext_doc = self._load_external_document(tgt_file)
            if ext_doc is None:
                continue

            # Verify the target item exists in the external document
            try:
                _, tgt_type_plural = canonical_org_types(tgt_type.lower())
            except Exception:
                tgt_type_plural = tgt_type + 's'
            if lookup_item(ext_doc, tgt_type_plural, tgt_num) is None:
                continue

            if self._add_cross_doc_unit(tgt_file, tgt_type, tgt_num):
                added += 1
                ext_name = os.path.basename(tgt_file).replace('_processed.json', '')
                print(f"  Cross-doc: queued {tgt_type} {tgt_num} from "
                      f"{ext_name} (referenced by {src_type_name} {src_num})")

        if added:
            print(f"  Added {added} cross-document unit(s) to analysis queue.")
            self._save_question_object()

    def _find_registry_target_for_ref(
        self, ext_file: str, src_type: str, src_num: str, ref_text: str
    ) -> Optional[Dict]:
        """
        Look up an item-level-resolved registry entry for a ref in an external document.
        Used by _refresh_cross_document_units() to follow chains recursively.
        Results are cached per external file path.
        """
        if ext_file not in self._ext_doc_refs:
            try:
                from registry.stage4_integration import load_item_level_refs_for_document
                self._ext_doc_refs[ext_file] = load_item_level_refs_for_document(
                    ext_file, self._config or {}
                )
            except Exception:
                self._ext_doc_refs[ext_file] = []

        for r in self._ext_doc_refs[ext_file]:
            if (r['source_item_type'] == src_type
                    and r['source_item_number'] == src_num
                    and r['ref_text'] == ref_text
                    and r['target_item_type'] is not None):
                return r
        return None

    def _refresh_cross_document_units(self) -> int:
        """
        Scan cross-doc units that were analyzed this round for their own External
        need_ref entries, check the registry for item-level resolution, and queue
        any newly discovered cross-doc units. Returns count of newly added units.
        """
        added = 0
        cds = self.question_object.get("cross_doc_scores", {})

        for ext_file, ext_scores in cds.items():
            ext_doc = self._load_external_document(ext_file)
            if ext_doc is None:
                continue

            for item_type_name, type_scores in ext_scores.items():
                try:
                    _, type_plural = canonical_org_types(item_type_name.lower())
                except Exception:
                    type_plural = item_type_name + 's'

                for item_num in list(type_scores.keys()):
                    unit = lookup_item(ext_doc, type_plural, item_num)
                    if unit is None:
                        continue

                    for ref_entry in unit.get("need_ref", []):
                        if not isinstance(ref_entry, dict):
                            continue
                        if ref_entry.get("type") != "External":
                            continue
                        ref_text = ref_entry.get("value", "")

                        tgt = self._find_registry_target_for_ref(
                            ext_file, item_type_name, item_num, ref_text
                        )
                        if tgt is None:
                            continue

                        tgt_file = tgt['target_file_path']
                        tgt_type = tgt['target_item_type']
                        tgt_num = tgt['target_item_number']

                        ext2 = self._load_external_document(tgt_file)
                        if ext2 is None:
                            continue

                        if self._add_cross_doc_unit(tgt_file, tgt_type, tgt_num):
                            added += 1
                            ext2_name = os.path.basename(tgt_file).replace('_processed.json', '')
                            print(f"  Cross-doc (recursive): queued {tgt_type} {tgt_num} "
                                  f"from {ext2_name}")

        if added:
            self._save_question_object()
        return added

    # ------------------------------------------------------------------
    # Client selection helpers
    # ------------------------------------------------------------------

    def _get_client_for_phase(self, phase: str):
        """
        Get the appropriate AI client for a given phase.

        Uses model_assignments from config to determine which model to use.
        Clients are cached by model name so each model is only instantiated once.

        Args:
            phase: One of 'relevance_scoring', 'iterative_analysis', 'cleanup', 'final_answer'

        Returns:
            BaseAIClient instance
        """
        from utils.config import get_model_for_task

        config = self._config
        if not config:
            return self.client

        # Map phase names to task names
        phase_to_task = {
            'relevance_scoring': 'qa.relevance.score',
            'iterative_analysis': 'qa.analysis.analyze_chunk',
            'cleanup': 'qa.synthesis.cleanup_scratch',
            'final_answer': 'qa.synthesis.final_answer'
        }

        task_name = phase_to_task.get(phase, 'qa.relevance.score')
        model_name = get_model_for_task(config, task_name)

        # If it's the same model as the default client, reuse it
        default_model = config.get('current_engine', '')
        if model_name == default_model:
            return self.client

        # Create and cache task-specific client
        if model_name not in self._task_clients:
            self._task_clients[model_name] = create_ai_client(model_name=model_name, config=config)
        return self._task_clients[model_name]
    
    def _get_task_name_for_phase(self, phase: str) -> str:
        """
        Get the task name for a given phase.
        
        Args:
            phase: One of 'relevance_scoring', 'iterative_analysis', 'cleanup', 'final_answer'
        
        Returns:
            Task name string (e.g., 'qa.relevance.score')
        """
        phase_to_task = {
            'relevance_scoring': 'qa.relevance.score',
            'iterative_analysis': 'qa.analysis.analyze_chunk',
            'cleanup': 'qa.synthesis.cleanup_scratch',
            'final_answer': 'qa.synthesis.final_answer'
        }
        return phase_to_task.get(phase, 'qa.relevance.score')

    # ------------------------------------------------------------------
    # Skip optimization helpers
    # ------------------------------------------------------------------

    def _get_unit_key(self, item_type_name: str, item_number: str) -> str:
        """Generate a unique key for a unit."""
        return f"{item_type_name.lower()}_{item_number}"

    def _should_skip_unit(
        self, item_type_name: str, item_number: str, refine: bool
    ) -> bool:
        """
        Check if this unit should be skipped.
        
        Skip if:
        - This is a refinement pass (refine=True)
        - The unit is on the skip list
        """
        if not refine:
            # Never skip on initial pass
            return False
        
        unit_key = self._get_unit_key(item_type_name, item_number)
        skip_list = self.question_object.get("skip_list", [])
        return unit_key in skip_list

    def _add_to_skip_list(self, item_type_name: str, item_number: str) -> None:
        """Add a unit to the skip list."""
        unit_key = self._get_unit_key(item_type_name, item_number)
        skip_list = self.question_object.get("skip_list", [])
        if unit_key not in skip_list:
            skip_list.append(unit_key)
            self.question_object["skip_list"] = skip_list

    def _clear_skip_list(self) -> None:
        """Clear the entire skip list (called when new question is added)."""
        self.question_object["skip_list"] = []

    def _remove_from_skip_list(self, item_type_name: str, item_number: str) -> None:
        """Remove a unit from the skip list."""
        unit_key = self._get_unit_key(item_type_name, item_number)
        skip_list = self.question_object.get("skip_list", [])
        if unit_key in skip_list:
            skip_list.remove(unit_key)
            self.question_object["skip_list"] = skip_list

    # ------------------------------------------------------------------
    # Scoring phase
    # ------------------------------------------------------------------

    def _score_organizational_units(self, question: str, max_tokens: int = 1000) -> Dict[str, int]:
        """
        Score organizational units for relevance to the question.

        Returns a dictionary mapping org_unit keys (e.g., "title_42", "chapter_6A") to scores (0-3).
        """
        org_scores = {}

        if "document_information" not in self.parsed_content:
            return org_scores

        org_content = self.parsed_content["document_information"].get("organization", {}).get("content", {})
        if not org_content:
            return org_scores

        # Build scope path list for above-scope shortcut (avoids AI calls on ancestors)
        content_scope = self.parsed_content["document_information"].get("content_scope")
        scope_path = None
        if content_scope:
            scope_path = [f"{list(e.keys())[0]}_{list(e.values())[0]}" for e in content_scope]

        def score_org_unit_recursive(org_type: str, org_id: str, org_data: Dict[str, Any], path: List[str]) -> None:
            """Recursively score organizational units."""
            # Create unique key using full hierarchical path
            org_key = "/".join(path)

            # Check if we should skip this unit based on parent scores
            # path includes current unit as last element, so ancestors are path[:-1]
            ancestors = path[:-1]
            if len(ancestors) >= 2:
                # Check if the last 2 ancestors both scored 0
                ancestor_keys = ["/".join(path[:i+1]) for i in range(len(path)-2, len(path))]
                if all(org_scores.get(ancestor_key, 1) == 0 for ancestor_key in ancestor_keys[-2:]):
                    # Skip this unit and its descendants - both immediate parents scored 0
                    return

            # Check if this unit is above the content_scope root.
            # Units above scope have no useful summary (they cover more than the slice),
            # so we skip the AI call: assign score 1 and recurse into children.
            if scope_path and len(path) < len(scope_path):
                path_str = "/".join(path)
                scope_prefix = "/".join(scope_path[:len(path)])
                if path_str == scope_prefix:
                    # Above scope, on the correct branch — assign 1, recurse without AI call
                    org_scores[org_key] = 1
                    for key, value in org_data.items():
                        if key not in ["unit_title", "summary_1", "summary_2", "unit_definitions",
                                      "begin_section", "stop_section", "begin_article", "stop_article"]:
                            if isinstance(value, dict):
                                for child_id, child_data in value.items():
                                    if isinstance(child_data, dict):
                                        new_path = path + [f"{key}_{child_id}"]
                                        score_org_unit_recursive(key, child_id, child_data, new_path)
                    return
                else:
                    # Above scope but on the wrong branch — assign 0, do not recurse
                    org_scores[org_key] = 0
                    return

            # Get summary (prefer summary_1 for organizational units)
            summary = ""
            if "summary_1" in org_data and org_data["summary_1"]:
                summary = org_data["summary_1"]
            elif "summary_2" in org_data and org_data["summary_2"]:
                summary = org_data["summary_2"]

            if not summary:
                # If no summary, default to score 1 (possibly relevant) to avoid filtering out children
                org_scores[org_key] = 1
            else:
                # Score this organizational unit
                score = self._score_unit_relevance(
                    org_type, org_id, summary, question, max_tokens, is_org_unit=True
                )
                org_scores[org_key] = score

            # Recursively score child organizational units
            for key, value in org_data.items():
                if key not in ["unit_title", "summary_1", "summary_2", "unit_definitions",
                              "begin_section", "stop_section", "begin_article", "stop_article"]:
                    # This is likely a nested organizational unit type
                    if isinstance(value, dict):
                        for child_id, child_data in value.items():
                            if isinstance(child_data, dict):
                                new_path = path + [f"{key}_{child_id}"]
                                score_org_unit_recursive(key, child_id, child_data, new_path)

        # Score all top-level organizational units
        for org_type, org_units in org_content.items():
            for org_id, org_data in org_units.items():
                if isinstance(org_data, dict):
                    score_org_unit_recursive(org_type, org_id, org_data, [f"{org_type}_{org_id}"])

        return org_scores

    def _score_unit_relevance(
        self, 
        unit_type: str, 
        unit_id: str, 
        summary: str, 
        question: str, 
        max_tokens: int = 1000,
        is_org_unit: bool = False
    ) -> int:
        """
        Score a single unit's relevance to the question.
        
        Args:
            unit_type: Type of unit (e.g., "section", "title", "chapter")
            unit_id: Identifier of the unit (e.g., "201", "42")
            summary: Summary text to evaluate
            question: Question text
            max_tokens: Maximum tokens for the API call
            is_org_unit: Whether this is an organizational unit (for error messages)
        
        Returns:
            Score (0-3)
        """
        prompt = []
        prompt.append("Here is a summary of a portion of a longer document:\n\n")
        prompt.append("***start of summary***\n\n")
        prompt.append(summary)
        prompt.append("\n\n***end of summary***\n\n")
        prompt.append(
            "Please evaluate the likely relevance of that portion to answering "
            "the following question:\n\n"
        )
        prompt.append("***start of question***\n\n")
        prompt.append(question)
        prompt.append("\n\n***end of question***\n\n")
        prompt.append(
            "IMPORTANT: You MUST respond with a single digit enclosed in square brackets.\n\n"
            "Your response must be one of these four options:\n"
            "[0] if the portion appears NOT relevant to answering the question\n"
            "[1] if the portion has a LOW probability of being relevant\n"
            "[2] if the portion is LIKELY relevant\n"
            "[3] if the portion is CLEARLY IMPORTANT for answering the question\n\n"
            "Respond now with only the single digit enclosed in square brackets (e.g., [0], [1], [2], or [3]). "
            "Do not include any explanation, reasoning, or other text.\n"
        )
        full_prompt = "".join(prompt)

        # For scoring we do not need JSON extraction; just capture the digit.
        # Use query_text_with_retry for automatic retry and fallback model support
        client = self._get_client_for_phase('relevance_scoring')
        task_name = self._get_task_name_for_phase('relevance_scoring')
        config = self._config
        
        try:
            result_text = query_text_with_retry(
                client,
                [],
                full_prompt,
                self.logfile,
                max_tokens=max_tokens,
                config=config,
                task_name=task_name
            )
        except ModelError as e:
            # Re-raise with more context about which unit failed
            raise ModelError(
                f"score_relevance: failed to get response for {unit_type} {unit_id} after retries. "
                f"Original error: {str(e)}"
            )
        
        # Parse response - expect digit in square brackets, but accept bare digit too
        result_str = str(result_text).strip()
        if not result_str:
            raise ModelError(
                f"score_relevance: model returned a blank response for "
                f"{unit_type} {unit_id} after retries. Expected a digit in square brackets."
            )

        # First try to match digit in square brackets like [0], [1], [2], [3]
        match = re.search(r"\[([0-3])\]", result_str)
        if match:
            score = int(match.group(1))
        else:
            # Fall back to matching just a bare digit
            match = re.search(r"[0-3]", result_str)
            if not match:
                raise ModelError(
                    f"score_relevance: model did not return a valid digit for "
                    f"{unit_type} {unit_id} after retries. Response: {result_text}"
                )
            score = int(match.group(0))

        return score

    def _score_units_batch(
        self,
        units: List[Tuple[str, str, str, str]],  # (type_key, type_name, num, summary)
    ) -> Dict[Tuple[str, str], int]:
        """
        Score a batch of units for relevance to the question in a single API call.

        For single-unit batches, delegates to _score_unit_relevance to use its
        simpler plain-text prompt format.

        Args:
            units: List of (type_key, type_name, num, summary) tuples.
                   type_key is the parameter key (e.g. "eccn_type");
                   type_name is the display name (e.g. "Eccn").
                   Batches are pre-sized by the caller using scoring_batch_max_chars.

        Returns:
            Dict mapping (type_key, num) -> score (0–3).
            Any unit missing from the response defaults to score 1.
        """
        if not units:
            return {}

        # Single unit: use the existing individual scorer (simpler prompt)
        if len(units) == 1:
            type_key, type_name, num, summary = units[0]
            try:
                score = self._score_unit_relevance(type_name, num, summary, self.question_text)
            except ModelError:
                score = 1
            return {(type_key, num): score}

        # Multi-unit batch: question + instructions cached; summaries in query prompt.
        cache_prefix = (
            "You are a relevance classifier for legal document analysis. "
            "I will provide numbered summaries of portions of a legal document "
            "and a question. For each numbered summary, assign a relevance score.\n\n"
            "Score scale:\n"
            "0 = NOT relevant to answering the question\n"
            "1 = LOW probability of being relevant\n"
            "2 = LIKELY relevant\n"
            "3 = CLEARLY IMPORTANT for answering the question\n\n"
            f"Question:\n{self.question_text}\n\n"
        )

        query_parts = ["Summaries to score:\n\n"]
        for idx, (type_key, type_name, num, summary) in enumerate(units, start=1):
            label = f"{type_name.capitalize()} {num}" if type_name else str(num)
            query_parts.append(f"[{idx}] {label}:\n{summary}\n\n")
        query_parts.append(
            'Respond with a JSON object mapping each item number (string key) to its '
            'score (integer 0-3). Include ALL items. '
            'Example for 3 items: {"1": 2, "2": 0, "3": 3}\n'
            "No explanation, only the JSON object.\n"
        )
        query_prompt = "".join(query_parts)

        client = self._get_client_for_phase('relevance_scoring')
        task_name = self._get_task_name_for_phase('relevance_scoring')
        # Budget for visible output (~15 tokens/item for compact JSON) plus a large
        # fixed overhead for reasoning models (e.g. gpt-5-mini).  Reasoning models
        # consume internal "thinking" tokens before producing any visible content;
        # empirically ~920–1420 thinking tokens are used for 10-item batches, so a
        # minimum of 2000 ensures the reasoning budget is not exhausted before output.
        max_tokens = max(len(units) * 15 + 2000, 2000)

        try:
            result_obj = query_json(
                client,
                [cache_prefix],
                query_prompt,
                self.logfile,
                max_tokens=max_tokens,
                config=self._config,
                task_name=task_name,
            )
        except ModelError:
            # Batch failed — fall back to individual scoring
            print(f"    Batch scoring failed for {len(units)} units, falling back to individual calls")
            results = {}
            for type_key, type_name, num, summary in units:
                try:
                    score = self._score_unit_relevance(type_name, num, summary, self.question_text)
                except ModelError:
                    score = 1
                results[(type_key, num)] = score
            return results

        # Parse response: expect {"1": 2, "2": 0, ...}
        results = {}
        if isinstance(result_obj, dict):
            for idx, (type_key, type_name, num, _) in enumerate(units, start=1):
                raw = result_obj.get(str(idx))
                if raw is not None:
                    try:
                        score = max(0, min(3, int(raw)))
                    except (ValueError, TypeError):
                        score = 1
                else:
                    score = 1  # Default for missing entries
                results[(type_key, num)] = score
        else:
            # Unexpected format — default all to 1
            for type_key, type_name, num, _ in units:
                results[(type_key, num)] = 1

        return results

    def _apply_group_promotion(self, scores: dict) -> bool:
        """
        Promote all text-bearing members of a substantive unit family to the group max score.

        A "family" is a top-level substantive unit together with all of its text-bearing
        sub-units at any nesting depth.  If any member scores above 0, every other member
        of the same family is elevated to that same max score.  This ensures that when any
        part of a logically unified unit is relevant, analysts receive all of its prose
        components — not just the piece whose summary happened to score highest.

        Data-table and other text-less sub-units are not part of any family (they are
        never handed to prose analysts regardless of score).

        Returns True if any scores were changed.
        """
        param_pointer = self.parsed_content["document_information"]["parameters"]
        content_pointer = self.parsed_content["content"]
        changed = False

        for item_type in param_pointer:
            p = param_pointer[item_type]
            if not (p.get("operational") == 1 and "name" in p and "name_plural" in p):
                continue
            if p.get("is_sub_unit", False):
                continue
            item_type_names = p["name_plural"]
            if item_type_names not in content_pointer:
                continue

            for item_num, item_data in content_pointer[item_type_names].items():
                if not has_sub_units(item_data):
                    continue

                # Collect all text-bearing members of this family.
                members: List[Tuple[str, str]] = []  # (type_key, num)

                if item_data.get("text"):
                    members.append((item_type, item_num))

                for sub_type_key_str, _, sub_num, _ in _iter_text_bearing_sub_units(param_pointer, item_data):
                    members.append((sub_type_key_str, sub_num))

                if len(members) <= 1:
                    continue  # nothing to promote

                # Find the highest score any family member has received.
                max_score = max(
                    scores.get(type_key, {}).get(num, 0)
                    for type_key, num in members
                )

                if max_score == 0:
                    continue  # no member is relevant — nothing to promote

                # Elevate every under-scored (or unscored) member to max_score.
                for type_key, num in members:
                    current = scores.get(type_key, {}).get(num, 0)
                    if current < max_score:
                        scores.setdefault(type_key, {})[num] = max_score
                        changed = True

        return changed

    def score_relevance(self, max_tokens: int = 1000) -> None:
        """
        Score each operational item for relevance to the question.

        Scores:
          0 = not relevant
          1 = possibly relevant (unlikely)
          2 = likely relevant
          3 = clearly important
        """
        # Check if scoring is already complete (idempotency)
        progress = self.question_object.get("progress", {})
        if progress.get("scoring_complete", False):
            print("  Relevance scoring already complete, skipping...")
            return

        if (
            "document_information" not in self.parsed_content
            or "parameters" not in self.parsed_content["document_information"]
            or "content" not in self.parsed_content
        ):
            raise InputError("score_relevance: invalid parsed_content structure.")

        param_pointer = self.parsed_content["document_information"]["parameters"]
        content_pointer = self.parsed_content["content"]

        question = self.question_text
        scores = self.question_object.get("scores", {})
        updated = False

        # Get mode configuration
        scoring_summary_level = self.mode_config.get("scoring_summary_level", "summary_1")
        org_summary_scoring = self.mode_config.get("org_summary_scoring", True)
        scoring_fallback_to_summary_2 = self.mode_config.get("scoring_fallback_to_summary_2", False)

        # Step 1: Score organizational units if enabled
        org_scores = {}
        if org_summary_scoring:
            org_scores = self._score_organizational_units(question, max_tokens)
            print(f"  Scored {len(org_scores)} organizational units")

        # Step 2: Score substantive units in batches grouped by total summary length.
        # Two-pass approach: first collect all units that need scoring (applying all
        # filtering logic), then batch-score them with _score_units_batch.
        org_name_set = get_organizational_item_name_set(self.parsed_content)
        scoring_batch_max_chars = self.mode_config.get("scoring_batch_max_chars", 10000)
        scoring_batch_max_items = self.mode_config.get("scoring_batch_max_items", 10)

        # Pass 1: collect units to score
        to_score: List[Tuple[str, str, str, str]] = []  # (type_key, type_name, num, summary)

        for item_type in param_pointer:
            p = param_pointer[item_type]
            if not (p.get("operational") == 1 and "name" in p and "name_plural" in p):
                continue
            # Skip sub-unit types — they are scored via container expansion below
            if p.get("is_sub_unit", False):
                continue
            item_type_name = p["name"]
            item_type_names = p["name_plural"]
            if item_type_names not in content_pointer:
                continue
            if item_type not in scores:
                scores[item_type] = {}

            for item_num, item_data in content_pointer[item_type_names].items():
                # Container expansion: collect all text-bearing sub-units for scoring,
                # then also score the parent itself if it has text.  Data-table and
                # other text-less sub-units are intentionally excluded — they are never
                # handed to analysts as prose and scoring them accomplishes nothing.
                if has_sub_units(item_data):
                    for sub_type_key_str, sub_type_name, sub_num, sub_data in _iter_text_bearing_sub_units(param_pointer, item_data):
                        if sub_type_key_str not in scores:
                            scores[sub_type_key_str] = {}
                        if sub_num in scores[sub_type_key_str]:
                            continue  # already scored

                        # Org_scores filtering for sub-units: use only org-type context entries
                        if org_summary_scoring and org_scores:
                            context = sub_data.get("context", [])
                            should_score = True
                            if context:
                                full_path = []
                                for context_item in context:
                                    for ctx_type, ctx_id in context_item.items():
                                        if ctx_type in org_name_set:
                                            full_path.append(f"{ctx_type}_{ctx_id}")
                                if full_path:
                                    parent_org_key = "/".join(full_path)
                                    should_score = org_scores.get(parent_org_key, 0) > 0
                            if not should_score:
                                continue

                        summary = ""
                        if scoring_summary_level == "summary_2" and sub_data.get("summary_2"):
                            summary = sub_data["summary_2"]
                        elif sub_data.get("summary_1"):
                            summary = sub_data["summary_1"]
                        elif sub_data.get("summary_2"):
                            summary = sub_data["summary_2"]
                        else:
                            continue

                        to_score.append((sub_type_key_str, sub_type_name, sub_num, summary))

                    # Also score the parent unit itself when it carries prose text.
                    # (Previously the parent was unconditionally skipped.)
                    if item_data.get("text") and item_num not in scores[item_type]:
                        should_score = True
                        if org_summary_scoring and org_scores:
                            context = item_data.get("context", [])
                            if context:
                                full_path = []
                                for context_item in context:
                                    for ctx_type, ctx_id in context_item.items():
                                        if ctx_type in org_name_set:
                                            full_path.append(f"{ctx_type}_{ctx_id}")
                                if full_path:
                                    parent_org_key = "/".join(full_path)
                                    should_score = org_scores.get(parent_org_key, 0) > 0
                        if should_score:
                            summary = ""
                            if scoring_summary_level == "summary_2" and item_data.get("summary_2"):
                                summary = item_data["summary_2"]
                            elif item_data.get("summary_1"):
                                summary = item_data["summary_1"]
                            elif item_data.get("summary_2"):
                                summary = item_data["summary_2"]
                            if summary:
                                to_score.append((item_type, item_type_name, item_num, summary))

                    continue  # done with container; non-container path below handles simple items

                if item_num in scores[item_type]:
                    continue  # already scored

                # Filter by organizational scores if org_summary_scoring is enabled
                if org_summary_scoring and org_scores:
                    context = item_data.get("context", [])
                    should_score = True  # Default to True if no parent organizational unit

                    if context:
                        full_path = []
                        for context_item in context:
                            for org_type, org_id in context_item.items():
                                if org_type in org_name_set:
                                    full_path.append(f"{org_type}_{org_id}")
                        parent_org_key = "/".join(full_path)
                        should_score = org_scores.get(parent_org_key, 0) > 0

                    if not should_score:
                        continue

                # Get summary based on mode configuration
                summary = ""
                if scoring_summary_level == "summary_2" and "summary_2" in item_data and item_data["summary_2"]:
                    summary = item_data["summary_2"]
                elif "summary_1" in item_data and item_data["summary_1"]:
                    summary = item_data["summary_1"]
                elif "summary_2" in item_data and item_data["summary_2"]:
                    summary = item_data["summary_2"]
                else:
                    continue

                to_score.append((item_type, item_type_name, item_num, summary))

        # Pass 2: batch-score collected units, flushing when total summary chars exceeds limit
        total_to_score = len(to_score)
        scored_count = 0
        print(f"  Scoring {total_to_score} substantive units "
              f"(batch limits: {scoring_batch_max_items} items, {scoring_batch_max_chars} chars)")

        current_batch: List[Tuple[str, str, str, str]] = []
        current_batch_chars = 0

        def _flush_batch() -> None:
            nonlocal scored_count, current_batch, current_batch_chars
            if not current_batch:
                return
            batch_scores = self._score_units_batch(current_batch)
            for (type_key, num), score in batch_scores.items():
                scores.setdefault(type_key, {})[num] = score
                scored_count += 1
                if self.progress_callback and total_to_score > 0:
                    self.progress_callback('relevance_scoring', 'processing', scored_count, total_to_score)
            current_batch = []
            current_batch_chars = 0

        for type_key, type_name, num, summary in to_score:
            summary_len = len(summary)
            if current_batch and (
                len(current_batch) >= scoring_batch_max_items
                or current_batch_chars + summary_len > scoring_batch_max_chars
            ):
                _flush_batch()
            current_batch.append((type_key, type_name, num, summary))
            current_batch_chars += summary_len

        _flush_batch()

        if to_score:
            updated = True

        # Step 3: Fallback re-scoring if enabled and no high-relevance sections found
        if scoring_fallback_to_summary_2 and scoring_summary_level == "summary_1":
            # Check if we found any high-relevance sections (score >= 2)
            has_high_relevance = False
            for item_type in scores:
                for item_num, score in scores[item_type].items():
                    if score >= 2:
                        has_high_relevance = True
                        break
                if has_high_relevance:
                    break
            
            if not has_high_relevance:
                print("  No high-relevance sections found with summary_1, re-scoring with summary_2...")
                # Collect items with summary_2 for batch re-scoring
                to_rescore: List[Tuple[str, str, str, str]] = []
                for item_type in list(scores.keys()):
                    p = _resolve_param_key(param_pointer, item_type)
                    if not p:
                        continue
                    if not (p.get("operational") == 1 and "name" in p and "name_plural" in p):
                        continue
                    item_type_name = p["name"]
                    item_type_names = p["name_plural"]

                    for item_num in list(scores[item_type].keys()):
                        item_data = lookup_item(self.parsed_content, item_type_names, item_num)
                        if item_data is None:
                            continue
                        if not item_data.get("summary_2"):
                            continue
                        to_rescore.append((item_type, item_type_name, item_num, item_data["summary_2"]))

                # Batch re-score
                rescore_batch: List[Tuple[str, str, str, str]] = []
                rescore_batch_chars = 0

                def _flush_rescore() -> None:
                    nonlocal rescore_batch, rescore_batch_chars
                    if not rescore_batch:
                        return
                    batch_scores = self._score_units_batch(rescore_batch)
                    for (type_key, num), score in batch_scores.items():
                        scores.setdefault(type_key, {})[num] = score
                    rescore_batch = []
                    rescore_batch_chars = 0

                for type_key, type_name, num, summary in to_rescore:
                    summary_len = len(summary)
                    if rescore_batch and (
                        len(rescore_batch) >= scoring_batch_max_items
                        or rescore_batch_chars + summary_len > scoring_batch_max_chars
                    ):
                        _flush_rescore()
                    rescore_batch.append((type_key, type_name, num, summary))
                    rescore_batch_chars += summary_len

                _flush_rescore()

                if to_rescore:
                    updated = True

        # Step 4: Group promotion — if any text-bearing member of a substantive unit
        # family scored above 0, elevate all other text-bearing members to the same score.
        if self._apply_group_promotion(scores):
            updated = True

        self.question_object["scores"] = scores

        # Mark scoring as complete (idempotency)
        progress = self.question_object.get("progress", {})
        progress["scoring_complete"] = True
        self.question_object["progress"] = progress
        
        if updated:
            self._save_question_object()
        else:
            # Still save to persist the progress marker
            self._save_question_object()
        
        # Check if we should stop after scoring
        if self.mode_config.get("stop_after_scoring", False):
            print("  Mode configured to stop after scoring - skipping analysis phases")
            return

    def process_relevant_section_requests(self) -> int:
        """
        Process request_relevant_section actions from the scratch document:
        1. Add requested sections to the scores with a default score of 2 (likely relevant)
        2. Add requested sections to the requesting unit's need_ref list (for summary_2)
        3. Create detail requests for summary_2 of the newly referenced units
        4. Remove fulfilled requests from scratch (unfulfilled ones remain as warnings)

        Returns the number of new sections added to the scores.
        """
        if (
            "document_information" not in self.parsed_content
            or "parameters" not in self.parsed_content["document_information"]
            or "content" not in self.parsed_content
        ):
            raise InputError("process_relevant_section_requests: invalid parsed_content structure.")

        scratch = self.question_object.get("scratch", {})
        requests = scratch.get("requests", {})
        scores = self.question_object.get("scores", {})
        param_pointer = self.parsed_content["document_information"]["parameters"]
        content_pointer = self.parsed_content["content"]

        new_sections_added = 0
        requests_to_remove = []  # Track fulfilled requests to remove

        # Process all request_relevant_section actions
        # Iterate over a list copy to avoid RuntimeError if we modify during iteration
        for req_id, req_data in list(requests.items()):
            if not isinstance(req_data, dict):
                continue
            if req_data.get("action") != "request_relevant_section":
                continue

            target_type = str(req_data.get("target_type", "")).strip()
            target_number = str(req_data.get("target_number", "")).strip()
            source_list = req_data.get("source", [])

            if not target_type or not target_number:
                continue

            # Find the parameter type that matches this target_type
            item_type_key = None
            item_type_name = None
            for param_key, param_data in param_pointer.items():
                if not isinstance(param_data, dict):
                    continue
                if not (param_data.get("operational") == 1 and "name" in param_data):
                    continue
                param_name = param_data["name"].lower()
                if param_name == target_type.lower():
                    item_type_key = param_key
                    item_type_name = param_data["name"]
                    break

            if not item_type_key:
                # Try to find by checking if target_type matches any parameter name
                # This handles variations in capitalization
                for param_key, param_data in param_pointer.items():
                    if not isinstance(param_data, dict):
                        continue
                    if not (param_data.get("operational") == 1 and "name" in param_data):
                        continue
                    param_name = param_data["name"]
                    if param_name.lower() == target_type.lower() or param_name.lower().strip("s") == target_type.lower().strip("s"):
                        item_type_key = param_key
                        item_type_name = param_data["name"]
                        break

            if not item_type_key:
                # Could not find matching item type - leave request in scratch as warning
                continue

            # Verify the section exists in content
            param_data = param_pointer[item_type_key]
            item_type_names = param_data.get("name_plural", "")
            if not item_type_names:
                continue
            if lookup_item(self.parsed_content, item_type_names, target_number) is None:
                # Not in primary doc — try external documents already loaded
                for ext_file, ext_doc in self._external_documents.items():
                    if lookup_item(ext_doc, item_type_names, target_number) is not None:
                        if self._add_cross_doc_unit(ext_file, item_type_name.lower(), target_number):
                            new_sections_added += 1
                            ext_name = os.path.basename(ext_file).replace('_processed.json', '')
                            print(f"  Cross-doc: queued {item_type_name} {target_number} from "
                                  f"{ext_name} (via section request)")
                        requests_to_remove.append(req_id)
                        break
                # Skip primary-doc processing: either we queued it externally, or it's not
                # found anywhere (leave request in scratch as a warning in the latter case).
                continue

            # Request can be fulfilled - process it

            # 1. Add to scores if not already there
            if item_type_key not in scores:
                scores[item_type_key] = {}

            if target_number not in scores[item_type_key]:
                scores[item_type_key][target_number] = 2
                new_sections_added += 1
                # Remove from skip list if it was there
                self._remove_from_skip_list(target_type.lower(), target_number)
            elif scores[item_type_key][target_number] < 2:
                # Bump score-1 units to 2 when explicitly requested
                scores[item_type_key][target_number] = 2
                new_sections_added += 1
                self._remove_from_skip_list(target_type.lower(), target_number)
                print(f"  Bumped {item_type_name} {target_number} from score 1 to score 2 (explicit request)")

            # 2. Add to requesting unit's need_ref list and create detail request
            # Parse source to identify requesting unit(s)
            for source_label in source_list:
                if not isinstance(source_label, str):
                    continue
                # source_label format: "Section 5", "Article 12", etc.
                parts = source_label.split()
                if len(parts) < 2:
                    continue
                requesting_type_name = parts[0].lower()
                requesting_number = parts[1]

                # Find the requesting unit in content
                requesting_type_plural = None
                for param_key, param_data in param_pointer.items():
                    if not isinstance(param_data, dict):
                        continue
                    if param_data.get("name", "").lower() == requesting_type_name:
                        requesting_type_plural = param_data.get("name_plural", "")
                        break

                if not requesting_type_plural:
                    continue
                requesting_unit = lookup_item(self.parsed_content, requesting_type_plural, requesting_number)
                if requesting_unit is None:
                    continue

                # Add to need_ref if not already there
                if "need_ref" not in requesting_unit:
                    requesting_unit["need_ref"] = []

                # Check if already in need_ref
                already_referenced = False
                for ref in requesting_unit["need_ref"]:
                    if isinstance(ref, dict) and ref.get("type", "").lower() == target_type.lower() and ref.get("value") == target_number:
                        already_referenced = True
                        break

                if not already_referenced:
                    requesting_unit["need_ref"].append({
                        "type": item_type_name,
                        "value": target_number
                    })

            # 3. Add detail request for summary_2
            # Use ScratchDocumentManager to add the detail request
            scratch_manager = ScratchDocumentManager(self.question_object)
            scratch_manager.add_detail_request(
                target_type,
                target_number,
                "summary_2",
                "System"  # Source label for auto-generated requests
            )

            # 4. Mark this request for removal (it's been fulfilled)
            requests_to_remove.append(req_id)

        # Remove fulfilled requests from scratch
        if requests_to_remove:
            for req_id in requests_to_remove:
                if req_id in requests:
                    del requests[req_id]

        if new_sections_added > 0:
            # Apply group promotion so that requesting one member of a substantive unit
            # family automatically includes all other text-bearing members at the same score.
            self._apply_group_promotion(scores)

        if new_sections_added > 0 or requests_to_remove:
            self.question_object["scores"] = scores
            self._save_question_object()
            if new_sections_added > 0:
                print(f"  Added {new_sections_added} new section(s) to analysis based on relevant section requests")
            if requests_to_remove:
                print(f"  Removed {len(requests_to_remove)} fulfilled request(s) from scratch document")

        return new_sections_added

    # ------------------------------------------------------------------
    # Iterative analysis
    # ------------------------------------------------------------------

    def run_analysis_iteration(self, refine: bool) -> Tuple[bool, int]:
        """
        Perform a single analysis iteration over items with score > 0.
        
        Returns:
            Tuple[bool, int]: (changed, new_sections_added)
            - changed: True if the scratch or working_answer was updated
            - new_sections_added: Number of new sections added to scores via relevant section requests
        """
        if (
            "document_information" not in self.parsed_content
            or "parameters" not in self.parsed_content["document_information"]
            or "content" not in self.parsed_content
        ):
            raise InputError("run_analysis_iteration: invalid parsed_content structure.")

        param_pointer = self.parsed_content["document_information"]["parameters"]
        content_pointer = self.parsed_content["content"]
        scores = self.question_object.get("scores", {})

        # Capture scratch snapshot at beginning of iteration for fact deduplication
        # This ensures facts are compared only against facts from prior passes
        scratch_snapshot = copy.deepcopy(self.question_object.get("scratch", {}))

        changed_any = False
        client = self._get_client_for_phase('iterative_analysis')
        item_analyzer = ItemAnalyzer(
            client,
            self.logfile,
            self.parsed_content,
            self.question_object,
            scratch_snapshot,
            parent_parsed_content=getattr(self, '_parent_parsed_content', None),
        )

        # Process items in order of decreasing score (3 → 1).
        # Score-1 gate: always skip score-1 on the first pass (refine=False).
        # If score_1_gate config is True, also skip score-1 on all subsequent passes.
        score_1_gate = self.mode_config.get("score_1_gate", False)
        compact_after_additions = self.mode_config.get("compact_after_additions", 0)
        additions_since_compact = 0

        # Treat all facts already on disk as "compacted" so that _detect_implicit_references
        # with pending_only=True only scans facts added during this call.
        self._compacted_fact_ids = set(
            self.question_object.get("scratch", {}).get("fact", {}).keys()
        )

        for score_level in (3, 2, 1):
            if score_level == 1 and (not refine or score_1_gate):
                continue  # always gate on first pass; also gate on refine passes if config says so

            for item_type, type_scores in scores.items():
                p = _resolve_param_key(param_pointer, item_type)
                if not p:
                    continue
                if not (p.get("operational") == 1 and "name" in p and "name_plural" in p):
                    continue
                item_type_name = p["name"]
                item_type_names = p["name_plural"]

                for item_num, score in list(type_scores.items()):
                    if score != score_level:
                        continue
                    working_item = lookup_item(self.parsed_content, item_type_names, item_num)
                    if working_item is None:
                        continue
                    if not working_item.get("text"):
                        continue

                    # Check if we should skip this unit (optimization)
                    if self._should_skip_unit(item_type_name, item_num, refine):
                        print(f"  Skipping {item_type_name} {item_num} (relevance: {score}) - no changes since last empty analysis")
                        continue

                    # Track scratch document before analysis
                    scratch_before = self.question_object.get("scratch", {})
                    facts_before = len(scratch_before.get("fact", {}))
                    questions_before = len(scratch_before.get("question", {}))
                    requests_before = len(scratch_before.get("requests", {}))

                    print(f"  Analyzing {item_type_name} {item_num} (relevance: {score})...", end="")

                    changed, had_actions, question_ids_added, question_ids_answered = item_analyzer.analyze_item(
                        item_type_name,
                        item_type_names,
                        item_num,
                        working_item,
                        self.question_text,
                        refine,
                        score_level=score_level,
                    )

                    # Manage skip list based on results
                    # If new question was added, clear the entire skip list
                    if question_ids_added:
                        self._clear_skip_list()

                    # If answer was added to a question, remove all source units from skip list
                    for q_id in question_ids_answered:
                        scratch = self.question_object.get("scratch", {})
                        questions = scratch.get("question", {})
                        q_data = questions.get(q_id, {})
                        if isinstance(q_data, dict):
                            sources = q_data.get("source", [])
                            for source_label in sources:
                                # Parse source label (e.g., "Section 201" -> item_type_name="section", item_number="201")
                                # Source labels are formatted as "{Type} {Number}" with a single space
                                parts = source_label.split(None, 1)  # Split on first space only
                                if len(parts) == 2:
                                    source_type = parts[0].lower()
                                    source_num = parts[1]
                                    self._remove_from_skip_list(source_type, source_num)

                    # If analysis returned empty actions, add unit to skip list
                    if not had_actions:
                        self._add_to_skip_list(item_type_name, item_num)

                    if changed:
                        changed_any = True
                        self._save_question_object()

                        # Show what was added
                        scratch_after = self.question_object.get("scratch", {})
                        facts_added = len(scratch_after.get("fact", {})) - facts_before
                        questions_added_count = len(scratch_after.get("question", {})) - questions_before
                        requests_added = len(scratch_after.get("requests", {})) - requests_before

                        results = []
                        if facts_added > 0:
                            results.append(f"{facts_added} fact(s)")
                        if questions_added_count > 0:
                            results.append(f"{questions_added_count} question(s)")
                        if requests_added > 0:
                            results.append(f"{requests_added} detail request(s)")

                        if results:
                            print(f" Added: {', '.join(results)}")
                        else:
                            print(f" Updated")

                        # Interval compaction: compact after every compact_after_additions additions
                        if compact_after_additions > 0:
                            additions_since_compact += facts_added + questions_added_count
                            if additions_since_compact >= compact_after_additions:
                                # Scan pending (uncompacted) facts for cross-references before
                                # compaction abstracts specific unit identifiers.
                                self._detect_implicit_references(pending_only=True)
                                self._compact_scratch()
                                additions_since_compact = 0
                    else:
                        print(f" No changes")

            # End of score group: scan pending facts for cross-references, then compact residual.
            if compact_after_additions > 0 and additions_since_compact > 0:
                self._detect_implicit_references(pending_only=True)
                self._compact_scratch()
                additions_since_compact = 0
            elif compact_after_additions == 0:
                # No interval compaction — scan all facts added this pass.
                self._detect_implicit_references(pending_only=True)

        # Cross-document unit analysis — units queued in cross_doc_scores
        cds = self.question_object.get("cross_doc_scores", {})
        for ext_file_path, ext_scores_for_doc in cds.items():
            ext_parsed_content = self._load_external_document(ext_file_path)
            if ext_parsed_content is None:
                continue

            ext_param_pointer = ext_parsed_content.get("document_information", {}).get("parameters", {})
            ext_doc_label = os.path.basename(ext_file_path).replace('_processed.json', '')

            for item_type_stored, type_scores in ext_scores_for_doc.items():
                # Find param entry by name in the external document
                item_type_name = item_type_stored
                item_type_name_plural = None
                for pk, pd in ext_param_pointer.items():
                    if not isinstance(pd, dict):
                        continue
                    if pd.get("name", "").lower() == item_type_stored.lower():
                        item_type_name = pd["name"]
                        item_type_name_plural = pd.get("name_plural", item_type_name + "s")
                        break
                if item_type_name_plural is None:
                    try:
                        _, item_type_name_plural = canonical_org_types(item_type_stored)
                    except Exception:
                        item_type_name_plural = item_type_stored + "s"

                for item_num, score in type_scores.items():
                    if score not in (2, 3):
                        continue

                    working_item = lookup_item(ext_parsed_content, item_type_name_plural, item_num)
                    if working_item is None or not working_item.get("text"):
                        continue

                    print(f"  Analyzing [external:{ext_doc_label}] {item_type_name} {item_num} "
                          f"(score: {score})...", end="")

                    scratch_before = self.question_object.get("scratch", {})
                    facts_before = len(scratch_before.get("fact", {}))
                    questions_before = len(scratch_before.get("question", {}))
                    requests_before = len(scratch_before.get("requests", {}))

                    cross_doc_analyzer = ItemAnalyzer(
                        client,
                        self.logfile,
                        ext_parsed_content,
                        self.question_object,
                        scratch_snapshot,
                        external_doc_label=ext_doc_label,
                    )
                    changed, had_actions, question_ids_added, question_ids_answered = (
                        cross_doc_analyzer.analyze_item(
                            item_type_name,
                            item_type_name_plural,
                            item_num,
                            working_item,
                            self.question_text,
                            refine,
                            score_level=score,
                        )
                    )

                    if changed:
                        changed_any = True
                        self._save_question_object()
                        scratch_after = self.question_object.get("scratch", {})
                        facts_added = len(scratch_after.get("fact", {})) - facts_before
                        questions_added_count = len(scratch_after.get("question", {})) - questions_before
                        requests_added = len(scratch_after.get("requests", {})) - requests_before
                        results = []
                        if facts_added > 0:
                            results.append(f"{facts_added} fact(s)")
                        if questions_added_count > 0:
                            results.append(f"{questions_added_count} question(s)")
                        if requests_added > 0:
                            results.append(f"{requests_added} detail request(s)")
                        if results:
                            print(f" Added: {', '.join(results)}")
                        else:
                            print(f" Updated")
                    else:
                        print(f" No changes")

        # Process any relevant section requests that were made during this iteration
        new_sections_added = self.process_relevant_section_requests()

        # Scan newly-analyzed cross-doc units for registry refs to further external units
        new_cross_doc = self._refresh_cross_document_units()
        if new_cross_doc > 0:
            new_sections_added += new_cross_doc

        # End-of-round dedup (only when not using interval compaction)
        if self.mode_config.get("deduplicate_new_facts", False) and compact_after_additions == 0:
            self._deduplicate_new_facts(scratch_snapshot)

        # Final compaction: pick up any residual additions since last interval fire
        if compact_after_additions > 0 and additions_since_compact > 0:
            self._compact_scratch()

        return changed_any, new_sections_added

    def _deduplicate_new_facts(self, scratch_before: Dict) -> int:
        """
        Remove facts added in the just-completed round that are redundant with
        facts from the round-start snapshot.  Single batch AI call.

        Args:
            scratch_before: frozen snapshot taken at round start

        Returns:
            Number of facts removed
        """
        live_facts = self.question_object.get("scratch", {}).get("fact", {})
        prior_facts = scratch_before.get("fact", {})

        new_fact_ids = [fid for fid in live_facts if fid not in prior_facts]
        if not new_fact_ids or not prior_facts:
            return 0

        prompt_parts = [
            "You are reviewing facts extracted from a legal document analysis.\n\n"
            f"Primary Question: {self.question_text}\n\n"
            "Facts established in prior rounds:\n\n"
        ]
        for i, (fid, fdata) in enumerate(prior_facts.items(), 1):
            prompt_parts.append(f"{i}. {fdata.get('content', '')}\n")

        prompt_parts.append("\nNew candidate facts from this round:\n\n")
        for fid in new_fact_ids:
            content = live_facts[fid].get("content", "")
            prompt_parts.append(f"  \"{fid}\": {content}\n")

        prompt_parts.append(
            "\nFor each candidate fact ID, decide: does it provide materially new "
            "information not already covered by the established facts?\n"
            "Return a JSON object: {\"fact_id\": true|false, ...}\n"
            "true = keep (is new), false = remove (redundant).\n"
            "When uncertain, keep (true).\n"
        )

        try:
            client = self._get_client_for_phase('iterative_analysis')
            max_tokens = max(len(new_fact_ids) * 15 + 300, 300)
            result = query_json(
                client, [], "".join(prompt_parts), self.logfile,
                max_tokens=max_tokens, config=self._config,
                task_name='qa.analysis.analyze_chunk'
            )
        except Exception:
            return 0

        removed = 0
        if isinstance(result, dict):
            for fid in new_fact_ids:
                keep = result.get(fid, True)  # default keep if missing
                if keep is False or keep == 0:
                    live_facts.pop(fid, None)
                    removed += 1

        if removed > 0:
            print(f"  End-of-round dedup: removed {removed} redundant fact(s)")
            self._save_question_object()

        return removed

    def _detect_implicit_references(self, pending_only: bool = False) -> int:
        """
        Use an LLM to identify unit identifiers meaningfully referenced in scratch facts,
        then match those identifiers to scored units using backward-prefix matching.

        When pending_only=True, only facts added since the last compaction are scanned.
        This is used to detect cross-references before compaction can abstract specific
        unit identifiers (e.g. "3A090") into generic text. The caller is responsible for
        invoking this before each _compact_scratch() call.

        When pending_only=False (the historical default), all current facts are scanned.

        The candidate list is NOT sent to the LLM (it may be very large). Instead the LLM
        reads the fact text and returns identifiers as it sees them; matching logic then
        resolves each to a known scored unit, handling sub-paragraph references like
        "3A090.a" → ECCN "3A090".

        Only runs when implicit_reference_detection is True in mode config.

        Returns:
            Number of units promoted.
        """
        if not self.mode_config.get("implicit_reference_detection", False):
            return 0

        scratch = self.question_object.get("scratch", {})
        all_facts = scratch.get("fact", {})
        scores = self.question_object.get("scores", {})
        param_pointer = self.parsed_content["document_information"]["parameters"]

        if pending_only:
            facts = {k: v for k, v in all_facts.items() if k not in self._compacted_fact_ids}
        else:
            facts = all_facts

        if not facts:
            return 0

        # Skip the LLM call if there are no score-0/1 units to promote.
        has_promotable = any(
            score < 2
            for type_scores in scores.values()
            for score in (type_scores.values() if isinstance(type_scores, dict) else [])
        )
        if not has_promotable:
            return 0

        facts_text = "\n".join(
            f"- {fdata.get('content', '')}" for fdata in facts.values()
        )

        # Ask the LLM to identify referenced unit identifiers from the fact text.
        # No candidate list is provided — we match the response ourselves.
        prompt = (
            f"Primary Question: {self.question_text}\n\n"
            "The following facts were extracted from a legal document section. "
            "Identify any other units (sections, articles, ECCNs, etc.) whose content the "
            "facts suggest is needed to answer the Primary Question — for example, because "
            "the answer requires knowing a threshold, parameter, list, or requirement defined "
            "in that unit.\n\n"
            f"Facts:\n{facts_text}\n\n"
            "Do not include a unit merely because it is mentioned, cross-referenced, or listed "
            "as related. Include a unit only when its absence from the analysis would leave the "
            "Primary Question materially incomplete or unanswerable.\n\n"
            "Return a JSON object with key 'referenced_units' containing a list of unit "
            "identifiers exactly as they appear in the facts (e.g., section numbers, "
            "ECCN numbers, article numbers).\n"
            "Example: {\"referenced_units\": [\"3A090\", \"5A001\"]}\n"
            "If none qualify: {\"referenced_units\": []}"
        )

        try:
            client = self._get_client_for_phase('iterative_analysis')
            result = query_json(
                client, [], prompt, self.logfile,
                max_tokens=500, config=self._config,
                task_name='qa.analysis.analyze_chunk'
            )
        except Exception as e:
            print(f"  Implicit reference detection failed: {e}")
            return 0

        if not isinstance(result, dict) or "referenced_units" not in result:
            return 0

        referenced = result.get("referenced_units", [])
        if not isinstance(referenced, list):
            return 0

        # Build type_name → param_key map for all operational types.
        type_to_param_key: Dict[str, str] = {}
        for pk, pd in param_pointer.items():
            if isinstance(pd, dict) and pd.get("operational") == 1 and "name" in pd:
                type_to_param_key[pd["name"]] = pk
        operational_types = list(type_to_param_key.keys())

        # For each LLM-returned identifier, resolve it to a scored unit using
        # find_substantive_unit_with_maximum_matching (shared with Process_Stage_3.py).
        # This handles exact matches and backward-prefix matches (e.g., "3A090.a" → "3A090").
        promoted = 0
        promoted_ids: set = set()
        for ref_id in referenced:
            if not isinstance(ref_id, str):
                continue

            matched_type = None
            matched_id = None
            for element_type in operational_types:
                mt, mid = find_substantive_unit_with_maximum_matching(
                    self.parsed_content, element_type, ref_id
                )
                if mid is not None:
                    matched_type = mt
                    matched_id = mid
                    break  # unit IDs are typically distinct across types

            if matched_id is None or matched_id in promoted_ids:
                continue

            param_key = type_to_param_key.get(matched_type)
            if not param_key:
                continue

            current_score = scores.get(param_key, {}).get(matched_id)
            if current_score is not None and current_score >= 2:
                continue  # already at/above 2, no promotion needed

            if param_key not in scores:
                scores[param_key] = {}
            prev_score = scores[param_key].get(matched_id, 0)
            scores[param_key][matched_id] = 2
            self._remove_from_skip_list(matched_type, matched_id)
            promoted_ids.add(matched_id)
            promoted += 1
            print(f"  Implicit reference: promoted {matched_type} {matched_id} from score {prev_score} to 2")

        if promoted > 0:
            self.question_object["scores"] = scores
            self._save_question_object()

        return promoted

    def _compact_scratch(self) -> int:
        """
        Consolidate facts by removing redundant and useless entries. Fires unconditionally
        when called (no count threshold). Called on the interval set by compact_after_additions.

        Returns:
            Number of facts removed (positive), or 0 if nothing to compact or call failed.
        """
        scratch = self.question_object.get("scratch", {})
        facts = scratch.get("fact", {})
        if not facts:
            return 0

        count_before = len(facts)
        print(f"  Scratch compaction: consolidating {count_before} facts...")

        facts_json = json.dumps(facts, indent=2)

        prompt = (
            f"Primary Question: {self.question_text}\n\n"
            f"The following {count_before} facts were collected during document analysis. "
            "Consolidate them by removing redundant, duplicate, or marginally useful entries, "
            "and merging facts that convey essentially the same information. "
            "Keep only facts that are useful and non-redundant for answering the Primary Question.\n\n"
            "Rules:\n"
            "- Merge facts that convey the same information; combine their source lists.\n"
            "- Remove facts that are fully covered by another retained fact.\n"
            "- Keep facts that add distinct, useful details even if they seem related to others.\n"
            "- Do not discard any fact that contains unique information relevant to the Primary Question.\n"
            "- Do not fabricate new information.\n"
            "- Preserve the JSON schema: each fact has 'content' and 'source' fields.\n\n"
            f"Current facts:\n{facts_json}\n\n"
            "Return a JSON object with a single key 'fact' containing the consolidated facts dict. "
            "Use simple sequential IDs (fact_001, fact_002, ...). "
            "Example: {\"fact\": {\"fact_001\": {\"content\": \"...\", \"source\": [\"Section 5\"]}, ...}}\n"
        )

        try:
            client = self._get_client_for_phase('cleanup')
            max_tokens = min(count_before * 300 + 500, 16000)
            result = query_json(
                client, [], prompt, self.logfile,
                max_tokens=max_tokens, config=self._config,
                task_name='qa.synthesis.cleanup_scratch'
            )
        except Exception as e:
            print(f"  Scratch compaction failed: {e}")
            return 0

        if not isinstance(result, dict) or "fact" not in result:
            print("  Scratch compaction: unexpected response shape, skipping")
            return 0

        new_facts = result["fact"]
        if not isinstance(new_facts, dict):
            return 0

        scratch["fact"] = new_facts
        self.question_object["scratch"] = scratch
        removed = count_before - len(new_facts)
        print(f"  Scratch compaction: {count_before} → {len(new_facts)} facts ({removed} removed)")
        self._save_question_object()
        # Update the baseline so subsequent pending_only detection only scans new additions.
        self._compacted_fact_ids = set(new_facts.keys())
        return removed

    def run_to_stability(self, base_max_iterations: int = None) -> None:
        """
        Run analysis iterations until no further changes are made or the
        maximum number of iterations is reached.
        
        The maximum iterations is dynamically extended: if new sections are added
        during iteration N, the max_iterations is extended by base_max_iterations
        (default 3) to allow those new sections to complete their full analysis cycle.
        For example, if a section is added during iteration 3, max_iterations becomes 6.
        
        Args:
            base_max_iterations: Maximum iterations (if None, uses mode_config["max_analysis_passes"])
        """
        # Use mode configuration if base_max_iterations not provided
        if base_max_iterations is None:
            base_max_iterations = self.mode_config.get("max_analysis_passes", 3)
        
        # Check progress and resume from where we left off (idempotency)
        progress = self.question_object.get("progress", {})
        iterations_completed = progress.get("analysis_iterations_completed", 0)
        
        # Start with base_max_iterations, but this will be dynamically extended
        current_max_iterations = base_max_iterations
        
        # If we've already completed all iterations, skip
        if iterations_completed >= current_max_iterations:
            print(f"\nAnalysis already completed {iterations_completed} iteration(s), skipping...")
            return
        
        # Start from the next iteration
        iteration_num = iterations_completed + 1

        # Report that iterative analysis phase has started (even before first iteration completes)
        if self.progress_callback:
            self.progress_callback('iterative_analysis', 'starting', iterations_completed, current_max_iterations)

        # Flag to track whether the next iteration should be a refinement pass
        # Start with False for first iteration, True after that
        # Reset to False when new sections are added
        should_refine = (iteration_num > 1)

        # Initialize cross-document units from registry (idempotent; safe on resume)
        self._initialize_cross_document_units()

        # First iteration (or resume)
        if iteration_num == 1:
            iteration_type = "Initial Analysis"
        else:
            iteration_type = "Refinement Pass" if should_refine else "Initial Analysis"

        print(f"\n{'='*70}")
        print(f"ITERATION {iteration_num} ({iteration_type})")
        print(f"{'='*70}")
        changed, new_sections_added = self.run_analysis_iteration(refine=should_refine)
        
        # If new sections were added, reset refinement flag for next iteration
        # so new sections get an initial analysis pass
        if new_sections_added > 0:
            should_refine = False
            current_max_iterations = iteration_num + base_max_iterations
            print(f"  Extended max_iterations to {current_max_iterations} to accommodate {new_sections_added} new section(s)")
            print(f"  Next iteration will be Initial Analysis for newly added sections")
        else:
            # After processing current iteration, next should be refinement
            should_refine = True
        
        # Update progress after first/resumed iteration
        progress["analysis_iterations_completed"] = iteration_num
        self.question_object["progress"] = progress
        self._save_question_object()

        # Report progress if callback provided
        if self.progress_callback:
            self.progress_callback('iterative_analysis', 'processing', iteration_num, current_max_iterations)

        # Continue with remaining iterations
        # Loop continues while we have changes OR new sections were added, and iterations remain
        while (changed or new_sections_added > 0) and iteration_num < current_max_iterations:
            iteration_num += 1
            
            # Determine iteration type based on refinement flag
            iteration_type = "Refinement Pass" if should_refine else "Initial Analysis"
            
            print(f"\n{'='*70}")
            print(f"ITERATION {iteration_num} ({iteration_type})")
            print(f"{'='*70}")
            changed, new_sections_added = self.run_analysis_iteration(refine=should_refine)
            
            # If new sections were added, reset refinement flag for next iteration
            # so new sections get an initial analysis pass
            if new_sections_added > 0:
                should_refine = False
                current_max_iterations = iteration_num + base_max_iterations
                print(f"  Extended max_iterations to {current_max_iterations} to accommodate {new_sections_added} new section(s)")
                print(f"  Next iteration will be Initial Analysis for newly added sections")
            else:
                # After processing current iteration, next should be refinement
                should_refine = True
            
            # Update progress after each iteration
            progress["analysis_iterations_completed"] = iteration_num
            self.question_object["progress"] = progress
            self._save_question_object()

            # Report progress if callback provided
            if self.progress_callback:
                self.progress_callback('iterative_analysis', 'processing', iteration_num, current_max_iterations)

        if not changed and new_sections_added == 0:
            print(f"\nAnalysis converged after {iteration_num} iteration(s) - no new changes detected.")
        elif iteration_num >= current_max_iterations:
            print(f"\nCompleted maximum {current_max_iterations} iteration(s).")

        # Step 3: Analyze zero-score sections if enabled and no high-relevance sections found
        if self.mode_config.get("analyze_zero_score_sections", False):
            zero_score_added = self._analyze_zero_score_sections()

            # If zero-score analysis added sections, run additional iterations to analyze them
            if zero_score_added > 0 and iteration_num < current_max_iterations:
                print(f"Running additional iteration(s) to analyze {zero_score_added} fallback section(s)...")
                # Extend max iterations to allow for analysis of newly added sections
                current_max_iterations = iteration_num + base_max_iterations
                should_refine = False  # Start with initial analysis for new sections

                while iteration_num < current_max_iterations:
                    iteration_num += 1

                    iteration_type = "Refinement Pass" if should_refine else "Initial Analysis"
                    print(f"\n{'='*70}")
                    print(f"ITERATION {iteration_num} ({iteration_type})")
                    print(f"{'='*70}")
                    changed, new_sections_added = self.run_analysis_iteration(refine=should_refine)

                    # Update progress after each iteration
                    progress["analysis_iterations_completed"] = iteration_num
                    self.question_object["progress"] = progress
                    self._save_question_object()

                    # Report progress if callback provided
                    if self.progress_callback:
                        self.progress_callback('iterative_analysis', 'processing', iteration_num, current_max_iterations)

                    # Check convergence
                    if not changed and new_sections_added == 0:
                        print(f"\nAnalysis converged after {iteration_num} iteration(s).")
                        break

                    # After processing current iteration, next should be refinement
                    should_refine = True

    def _analyze_zero_score_sections(self) -> int:
        """
        Analyze sections that scored 0 if no high-relevance sections were found.
        This is a fallback mechanism for when initial scoring may have missed relevant content.

        Returns:
            Number of zero-score sections added for analysis
        """
        scores = self.question_object.get("scores", {})

        # Check if we have any high-relevance sections (score >= 2)
        has_high_relevance = False
        for item_type in scores:
            for item_num, score in scores[item_type].items():
                if score >= 2:
                    has_high_relevance = True
                    break
            if has_high_relevance:
                break

        if has_high_relevance:
            # We have high-relevance sections, no need to analyze zero-score sections
            return 0

        print(f"\n{'='*70}")
        print("ZERO-SCORE SECTION ANALYSIS (Fallback)")
        print(f"{'='*70}")
        print("No high-relevance sections found. Analyzing zero-score sections as fallback...")

        if (
            "document_information" not in self.parsed_content
            or "parameters" not in self.parsed_content["document_information"]
            or "content" not in self.parsed_content
        ):
            return 0
        
        param_pointer = self.parsed_content["document_information"]["parameters"]
        content_pointer = self.parsed_content["content"]
        
        # Find all zero-score sections and add them with score 1 (possibly relevant)
        # so they get analyzed in the next iteration
        zero_score_count = 0
        for item_type in param_pointer:
            p = param_pointer[item_type]
            if not (p.get("operational") == 1 and "name" in p and "name_plural" in p):
                continue
            # Skip sub-unit types — they are added via container expansion below
            if p.get("is_sub_unit", False):
                continue
            item_type_names = p["name_plural"]
            if item_type_names not in content_pointer:
                continue
            if item_type not in scores:
                scores[item_type] = {}

            for item_num, item_data in content_pointer[item_type_names].items():
                # Container expansion: add all text-bearing sub-units and, when the
                # parent itself has prose text, the parent too.
                if has_sub_units(item_data):
                    for sub_type_key_str, _, sub_num, sub_data in _iter_text_bearing_sub_units(param_pointer, item_data):
                        if sub_type_key_str not in scores:
                            scores[sub_type_key_str] = {}
                        if sub_num in scores[sub_type_key_str] and scores[sub_type_key_str][sub_num] != 0:
                            continue
                        if "summary_1" not in sub_data and "summary_2" not in sub_data:
                            continue
                        scores[sub_type_key_str][sub_num] = 1
                        zero_score_count += 1

                    # Also add the parent when it has prose text of its own.
                    if item_data.get("text"):
                        if item_num not in scores[item_type] or scores[item_type][item_num] == 0:
                            if "summary_1" in item_data or "summary_2" in item_data:
                                scores[item_type][item_num] = 1
                                zero_score_count += 1

                    continue  # done with container

                # Only process sections that scored 0
                if item_num in scores[item_type] and scores[item_type][item_num] != 0:
                    continue

                # Check if section has summaries (required for analysis)
                if "summary_1" not in item_data and "summary_2" not in item_data:
                    continue

                # Add with score 1 to trigger analysis
                scores[item_type][item_num] = 1
                zero_score_count += 1
        
        if zero_score_count > 0:
            self.question_object["scores"] = scores
            self._save_question_object()
            print(f"  Added {zero_score_count} zero-score section(s) for fallback analysis")
        else:
            print("  No zero-score sections found to analyze")

        return zero_score_count

    # ------------------------------------------------------------------
    # Cleanup and final answer
    # ------------------------------------------------------------------

    def cleanup_scratch_and_answer(self) -> None:
        """
        Run a dedicated cleanup pass over the full scratch document to
        propose a pruned scratch and optionally a refined working answer.
        """
        # Check if cleanup is already complete (idempotency)
        progress = self.question_object.get("progress", {})
        if progress.get("cleanup_complete", False):
            print(f"\n{'='*70}")
            print("CLEANUP PHASE")
            print(f"{'='*70}")
            print("Cleanup already complete, skipping...")
            return
        
        print(f"\n{'='*70}")
        print("CLEANUP PHASE")
        print(f"{'='*70}")

        scratch = self.question_object.get("scratch", {})
        facts_before = len(scratch.get("fact", {}))
        questions_before = len(scratch.get("question", {}))
        requests_before = len(scratch.get("requests", {}))

        print(f"Before cleanup: {facts_before} facts, {questions_before} questions, {requests_before} detail requests")
        print("Running cleanup and consolidation...")

        scratch_json = json.dumps(scratch, indent=4)

        prompt = []
        prompt.append(
            "You are a senior legal analyst reviewing a collaborative Scratch "
            "Document that multiple analysts have contributed to while analyzing "
            "a long legal document.\n\n"
        )
        prompt.append(
            "The goal is to clean up the Scratch Document so that it only contains "
            "useful, non-duplicative, and reasonably concise facts, questions, "
            "answers, and detail requests that are important for answering the "
            "Primary Question.\n\n"
        )
        prompt.append(
            "Context: This Scratch Document was built collaboratively by multiple AI analysts, "
            "each analyzing different substantive units (sections, articles, etc.) of the legal "
            "document. Each analyst contributed facts, questions, and answers based on their "
            "assigned portion. As a result, you may find:\n"
            "- Duplicate or highly similar facts from different analysts who found the same information\n"
            "- Related facts that could be consolidated\n"
            "- Questions that have been answered or are no longer relevant\n\n"
        )
        prompt.append("Primary Question:\n")
        prompt.append(self.question_text + "\n\n")
        prompt.append("Current Scratch Document (JSON):\n")
        prompt.append(scratch_json + "\n\n")
        prompt.append(
            "Return a single JSON object with the following structure:\n"
            "{\n"
            '  "scratch": { ... cleaned scratch JSON ... },\n'
            '  "working_answer": {\n'
            '    "text": "Optional draft answer text, or empty string if you do not '
            'wish to propose one yet."\n'
            "  }\n"
            "}\n\n"
        )
        prompt.append(
            "Rules for cleanup:\n"
            "- Preserve clearly relevant, non-duplicative items.\n"
            "- CONSOLIDATE duplicative or highly similar entries: When you find facts, questions, "
            "or answers that are substantially the same, merge them into a single entry. When "
            "consolidating, combine the source lists from all merged entries so that the consolidated "
            "entry's source field contains all substantive units that contributed to it.\n"
            "  Example: If fact_001 (source: [\"Section 5\"]) and fact_003 (source: [\"Article 12\"]) "
            "convey the same information, merge them into one fact with source: [\"Section 5\", \"Article 12\"].\n"
            "- Merge or drop only entries that are irrelevant to the Primary Question, or are obviously redundant or trivial.\n"
            "- Do not introduce new facts that are not already in the scratch.\n"
            "- If you modify the working_answer.text, base it only on content already "
            "in the scratch.\n"
        )

        full_prompt = "".join(prompt)

        # Cleanup needs a large token limit because it must return the entire scratch document
        # Estimate needed: ~150 tokens per fact/question + overhead
        # For safety, use 32000 tokens (supports ~200 facts comfortably)
        cleanup_max_tokens = 32000

        client = self._get_client_for_phase('cleanup')
        result = query_json(client, [], full_prompt, self.logfile, max_tokens=cleanup_max_tokens)

        if not isinstance(result, dict):
            raise ModelError("cleanup_scratch_and_answer: model did not return an object.")

        new_scratch = result.get("scratch")
        if isinstance(new_scratch, dict):
            self.question_object["scratch"] = new_scratch

        wa = result.get("working_answer", {})
        if isinstance(wa, dict) and "text" in wa:
            self.question_object["working_answer"]["text"] = str(wa["text"])

        # Mark cleanup as complete (idempotency)
        progress = self.question_object.get("progress", {})
        progress["cleanup_complete"] = True
        self.question_object["progress"] = progress
        
        self._save_question_object()

        # Show cleanup results
        facts_after = len(new_scratch.get("fact", {})) if isinstance(new_scratch, dict) else facts_before
        questions_after = len(new_scratch.get("question", {})) if isinstance(new_scratch, dict) else questions_before
        requests_after = len(new_scratch.get("requests", {})) if isinstance(new_scratch, dict) else requests_before

        print(f"After cleanup: {facts_after} facts, {questions_after} questions, {requests_after} detail requests")
        if facts_after < facts_before or questions_after < questions_before or requests_after < requests_before:
            print(f"Removed: {facts_before - facts_after} facts, {questions_before - questions_after} questions, {requests_before - requests_after} detail requests")

    def generate_final_answer(self) -> None:
        """
        Generate a final narrative answer based on the cleaned scratch
        and (optionally) selected summaries/texts.
        """
        # Check if final answer is already complete (idempotency)
        progress = self.question_object.get("progress", {})
        if progress.get("final_answer_complete", False):
            print(f"\n{'='*70}")
            print("FINAL ANSWER GENERATION")
            print(f"{'='*70}")
            print("Final answer already generated, skipping...")
            return
        
        print(f"\n{'='*70}")
        print("FINAL ANSWER GENERATION")
        print(f"{'='*70}")
        print("Synthesizing final answer from scratch document...")

        scratch = self.question_object.get("scratch", {})
        scratch_text = json.dumps(scratch, indent=4)

        prompt = []
        prompt.append(
            "You are a legal expert providing analysis based on a comprehensive review of one or more "
            "legal documents. Using only the information provided below, answer the question.\n\n"
        )
        prompt.append(
            "Background: Multiple AI analysts have reviewed different portions (sections, articles, etc.) "
            "of one or more legal documents to extract relevant information. The findings below represent "
            "their collective analysis, with each entry including source information indicating which portion "
            "of which document contributed that information. Sources from the primary document appear as "
            "\"Section 5\" or \"Article 12\". Sources from cross-referenced external documents appear with "
            "a parenthetical document identifier, e.g., \"Section 744.17 (Part744)\" or "
            "\"Eccn 3A090 (Part774)\".\n\n"
        )
        prompt.append("Question:\n")
        prompt.append(self.question_text + "\n\n")
        prompt.append("Findings from Document Analysis:\n")
        prompt.append(scratch_text + "\n\n")
        prompt.append(
            "Instructions:\n"
            "- Provide a clear, comprehensive answer suitable for a legal practitioner.\n"
            "- CRITICAL - CITE DOCUMENT SOURCES: When making claims or statements, you MUST reference "
            "the specific substantive units (e.g., \"Section 5\", \"Article 12\", \"Chapter 3\") where "
            "that information is found. Each fact or answer entry in the findings above includes a \"source\" "
            "field showing which document units contributed that information. Use these source labels in your "
            "answer. You may reference multiple units when information comes from multiple sources.\n"
            "- CROSS-DOCUMENT CITATIONS: When a source label includes a parenthetical document name "
            "(e.g., \"Section 3A090 (Part774)\"), include the document name in your citation "
            "(e.g., \"Part 774, ECCN 3A090\"). This indicates the information comes from a "
            "cross-referenced external document rather than the primary document being analyzed.\n"
            "- DO NOT reference internal working document identifiers like \"fact_1\", \"answer_2\", or "
            "\"question_3\". These are internal tracking IDs. ONLY reference the substantive units "
            "(Sections, Articles, Chapters, etc.) from the \"source\" fields.\n"
            "- DO NOT mention the analysis process, analysts, or working documents in your answer. "
            "Write as if you are directly interpreting the legal documents.\n"
            "- Address uncertainties: If the analysis includes unresolved questions that could materially "
            "affect the answer, acknowledge these limitations. Explain what information is uncertain and "
            "why it matters, so the reader understands the scope of the analysis.\n"
            "- If the available information is insufficient to fully answer the question, explain "
            "what is known and what remains uncertain.\n"
        )

        full_prompt = "".join(prompt)

        # Final answer generation needs sufficient tokens for comprehensive legal analysis
        # Using 16000 tokens to allow for detailed, thorough responses
        final_answer_max_tokens = 16000

        client = self._get_client_for_phase('final_answer')

        # Use query_text_with_retry to handle empty responses and enable fallback models
        try:
            answer_text = query_text_with_retry(
                client,
                [],
                full_prompt,
                self.logfile,
                max_tokens=final_answer_max_tokens,
                max_retries=3,
                config=self._config,
                task_name='qa.final_answer'
            )
        except ModelError as e:
            raise ModelError(f"generate_final_answer: Failed to generate final answer after retries: {e}")

        # Before overwriting, preserve existing answer if switching modes
        existing_answer = self.question_object.get("working_answer", {}).get("text", "").strip()
        previous_mode = self.question_object.get("qa_mode")
        current_mode = self.mode_name or "standard"
        
        # If there's an existing answer from a different mode, preserve it
        if existing_answer and previous_mode and previous_mode != current_mode and previous_mode != "quick_scan":
            # Initialize previous_answers structure if it doesn't exist
            if "previous_answers" not in self.question_object:
                self.question_object["previous_answers"] = []
            
            # Check if this answer is already in history (avoid duplicates)
            already_preserved = any(
                entry.get("answer_text") == existing_answer and entry.get("mode") == previous_mode
                for entry in self.question_object["previous_answers"]
            )
            
            if not already_preserved:
                # Save the previous answer with metadata
                from datetime import datetime, UTC
                previous_answer_entry = {
                    "mode": previous_mode,
                    "answer_text": existing_answer,
                    "timestamp": datetime.now(UTC).isoformat().replace('+00:00', 'Z'),
                    "saved_when": "answer_overwritten"
                }
                self.question_object["previous_answers"].append(previous_answer_entry)
                print(f"  Preserved previous answer from {previous_mode} mode in previous_answers history.")
        
        # Now set the new answer
        self.question_object["working_answer"]["text"] = str(answer_text).strip()
        
        # Mark final answer as complete (idempotency)
        progress = self.question_object.get("progress", {})
        progress["final_answer_complete"] = True
        self.question_object["progress"] = progress
        
        self._save_question_object()

        print(f"Final answer generated ({len(answer_text)} characters)")

    def quality_check_answer(self) -> None:
        """
        Quality check phase: Validate final answer accuracy by reviewing full text of relevant sections.

        This phase runs AFTER final answer generation and:
        (i) If no issues found → accept answer as-is
        (ii) If minor concerns found → append concerns to answer
        (iii) If significant issues found → regenerate answer with concerns as feedback

        This phase is only run when quality_check_phase is enabled in mode configuration.
        """
        # Check if quality check is already complete (idempotency)
        progress = self.question_object.get("progress", {})
        if progress.get("quality_check_complete", False):
            print(f"\n{'='*70}")
            print("QUALITY CHECK PHASE")
            print(f"{'='*70}")
            print("Quality check already complete, skipping...")
            return

        print(f"\n{'='*70}")
        print("QUALITY CHECK PHASE")
        print(f"{'='*70}")

        # Get the FINAL answer (generated by generate_final_answer())
        final_answer = self.question_object.get("working_answer", {}).get("text", "")
        if not final_answer or not final_answer.strip():
            print("No answer to validate, skipping quality check...")
            # Mark as complete even if skipped
            progress["quality_check_complete"] = True
            self.question_object["progress"] = progress
            self._save_question_object()
            return

        # Extract all source units from scratch document
        scratch = self.question_object.get("scratch", {})
        source_units = set()

        # Extract sources from facts
        facts = scratch.get("fact", {})
        for fact_id, fact_data in facts.items():
            sources = fact_data.get("source", [])
            if isinstance(sources, list):
                for source_str in sources:
                    # Parse source strings like "Section 5", "Article 12", etc.
                    source_units.add(source_str)

        # Extract sources from question answers
        questions = scratch.get("question", {})
        for q_id, q_data in questions.items():
            answers = q_data.get("answers", {})
            for a_id, a_data in answers.items():
                sources = a_data.get("source", [])
                if isinstance(sources, list):
                    for source_str in sources:
                        source_units.add(source_str)

        if not source_units:
            print("No source units found in scratch document, skipping quality check...")
            progress["quality_check_complete"] = True
            self.question_object["progress"] = progress
            self._save_question_object()
            return

        print(f"Validating answer against {len(source_units)} source units...")

        # Initialize quality concerns structure
        if "quality_concerns" not in self.question_object:
            self.question_object["quality_concerns"] = {}

        # Get parameters for looking up item types
        if ("document_information" not in self.parsed_content
            or "parameters" not in self.parsed_content["document_information"]
            or "content" not in self.parsed_content):
            print("Warning: Cannot perform quality check - invalid document structure")
            progress["quality_check_complete"] = True
            self.question_object["progress"] = progress
            self._save_question_object()
            return

        param_pointer = self.parsed_content["document_information"]["parameters"]
        content_pointer = self.parsed_content["content"]

        # Build a mapping from item_type_name to plural
        type_name_to_plural = {}
        for item_type_key, params in param_pointer.items():
            if params.get("operational") == 1 and "name" in params and "name_plural" in params:
                type_name_to_plural[params["name"]] = params["name_plural"]

        # Process each source unit
        units_checked = 0
        units_with_concerns = 0

        for source_str in sorted(source_units):
            # Parse source string like "Section 5" or "Article 12"
            parts = source_str.strip().split(None, 1)
            if len(parts) != 2:
                continue

            item_type_name = parts[0].lower()
            item_number = parts[1]

            # Look up the plural form
            if item_type_name not in type_name_to_plural:
                continue

            item_type_plural = type_name_to_plural[item_type_name]

            # Get the item data
            item_data = lookup_item(self.parsed_content, item_type_plural, item_number)
            if item_data is None:
                continue

            # Get full text of the unit
            unit_text = item_data.get("text", "")
            if not unit_text or not unit_text.strip():
                continue

            # Build context for this unit
            context_parts = []

            # Add definitions in scope
            if "defined_terms" in item_data and item_data["defined_terms"]:
                context_parts.append("Definitions in this unit:")
                for def_entry in item_data["defined_terms"]:
                    term = def_entry.get("term", "")
                    definition = def_entry.get("definition", "")
                    if term and definition:
                        context_parts.append(f"  - {term}: {definition}")

            # Add external definitions if any
            if "ext_definitions" in item_data and item_data["ext_definitions"]:
                context_parts.append("\nExternal definitions referenced by this unit:")
                for def_entry in item_data["ext_definitions"]:
                    term = def_entry.get("term", "")
                    definition = def_entry.get("definition", "")
                    if term and definition:
                        context_parts.append(f"  - {term}: {definition}")

            # Add organizational context
            if "context" in item_data and item_data["context"]:
                context_parts.append("\nOrganizational context:")
                for ctx in item_data["context"]:
                    for org_type, org_num in ctx.items():
                        context_parts.append(f"  - {org_type.capitalize()} {org_num}")

            context_str = "\n".join(context_parts) if context_parts else "No additional context available."

            # Build quality check prompt
            prompt = []
            prompt.append(
                "You are one of several analysts performing a parallel quality review of a final\n"
                "answer about a legal document. Each analyst reviews a different substantive unit.\n"
                "The answer you are reviewing was synthesized from information across multiple parts\n"
                "of the document, most of which you cannot see.\n\n"
            )

            prompt.append("=" * 70 + "\n")
            prompt.append("YOUR ASSIGNED SUBSTANTIVE UNIT\n")
            prompt.append("=" * 70 + "\n")
            prompt.append(f"{item_type_name.capitalize()} {item_number}:\n\n")
            prompt.append(unit_text.strip() + "\n")
            prompt.append("=" * 70 + "\n\n")

            prompt.append("=" * 70 + "\n")
            prompt.append("FINAL ANSWER\n")
            prompt.append("=" * 70 + "\n")
            prompt.append(final_answer.strip() + "\n")
            prompt.append("=" * 70 + "\n\n")

            prompt.append("CONTEXT FOR YOUR UNIT:\n")
            prompt.append(context_str + "\n\n")

            prompt.append(
                "TASK:\n"
                "Your role is to validate the answer's treatment of YOUR assigned unit only. Do NOT\n"
                "compare the entire answer to your unit's text—the answer correctly draws from many\n"
                "units you cannot see. Instead, focus on:\n\n"
                "1. **Logic Consistency**: Is the answer's logic consistent with what your unit says?\n"
                "   For example, if the answer states \"Section X requires Y,\" and your unit is Section X,\n"
                "   does it actually require Y? Does the answer's reasoning align with your unit's provisions?\n\n"
                "2. **Misstatements About This Unit**: Does the answer make any incorrect statements\n"
                "   specifically about your unit? For example, misquoting text, misstating requirements,\n"
                "   or mischaracterizing provisions from your unit.\n\n"
                "IMPORTANT: Do NOT flag as errors:\n"
                "- Information in the answer that comes from other units (this is expected)\n"
                "- The answer not mentioning everything in your unit (the answer synthesizes across units)\n"
                "- The answer being incomplete from your unit's perspective (other analysts review other units)\n\n"
                "Only flag issues where the answer's treatment of YOUR unit is logically inconsistent\n"
                "or factually incorrect.\n\n"
            )

            prompt.append(
                "Respond with a JSON object:\n"
                "{\n"
                '  "concerns": [\n'
                "    {\n"
                '      "type": "logic_inconsistency" | "misstatement" | "other",\n'
                '      "severity": "high" | "medium" | "low",\n'
                '      "description": "Specific issue with how answer treats this unit",\n'
                '      "relevant_text_from_unit": "Quote from unit that contradicts or is misstated"\n'
                "    }\n"
                "  ],\n"
                '  "overall_assessment": "consistent" | "mostly_consistent" | "needs_review" | "inconsistent",\n'
                '  "notes": "Any additional observations about answer\'s treatment of this unit"\n'
                "}\n\n"
                "If no concerns are found, return empty concerns array with overall_assessment: \"consistent\".\n"
            )

            full_prompt = "".join(prompt)

            # Query the AI for quality check
            try:
                client = self._get_client_for_phase('final_answer')  # Use same client as final answer
                quality_result = query_json(client, [], full_prompt, self.logfile, max_tokens=2000)

                if isinstance(quality_result, dict):
                    # Store the quality check result for this unit
                    unit_key = f"{item_type_name}_{item_number}"
                    self.question_object["quality_concerns"][unit_key] = {
                        "unit": source_str,
                        "concerns": quality_result.get("concerns", []),
                        "overall_assessment": quality_result.get("overall_assessment", "unknown"),
                        "notes": quality_result.get("notes", "")
                    }

                    # Count units with concerns
                    if quality_result.get("concerns") and len(quality_result.get("concerns", [])) > 0:
                        units_with_concerns += 1

                    units_checked += 1

                    # Print results (in separate try/except so print errors don't affect data processing)
                    try:
                        if quality_result.get("concerns") and len(quality_result.get("concerns", [])) > 0:
                            print(f"  ✗ {source_str}: {len(quality_result.get('concerns', []))} concern(s) - {quality_result.get('overall_assessment', 'unknown')}")
                        else:
                            print(f"  ✓ {source_str}: {quality_result.get('overall_assessment', 'consistent')}")
                    except UnicodeEncodeError:
                        # Fallback to ASCII-safe output if Unicode characters fail
                        if quality_result.get("concerns") and len(quality_result.get("concerns", [])) > 0:
                            print(f"  [!] {source_str}: {len(quality_result.get('concerns', []))} concern(s) - {quality_result.get('overall_assessment', 'unknown')}")
                        else:
                            print(f"  [OK] {source_str}: {quality_result.get('overall_assessment', 'consistent')}")

            except Exception as e:
                print(f"  Warning: Quality check failed for {source_str}: {e}")
                continue

        # Analyze quality check results and categorize concerns
        print(f"\nQuality check results:")
        print(f"  - Units checked: {units_checked}/{len(source_units)}")
        print(f"  - Units with concerns: {units_with_concerns}")

        # Categorize concerns as minor vs significant
        has_significant_issues = False
        has_minor_concerns = False
        all_concerns = []

        for unit_key, qc_data in self.question_object["quality_concerns"].items():
            assessment = qc_data.get("overall_assessment", "unknown")
            concerns = qc_data.get("concerns", [])
            unit_name = qc_data.get("unit", "Unknown unit")

            # Check for significant issues
            if assessment in ("inconsistent", "needs_review"):
                has_significant_issues = True
                print(f"  - {unit_name}: SIGNIFICANT ISSUE ({assessment})")
            elif concerns:
                # Check concern severity
                for concern in concerns:
                    severity = concern.get("severity", "medium")
                    if severity == "high":
                        has_significant_issues = True
                        print(f"  - {unit_name}: HIGH severity concern")
                        break
                else:
                    # Only low/medium concerns
                    has_minor_concerns = True
                    print(f"  - {unit_name}: Minor concerns ({assessment})")

            # Collect all non-consistent concerns for reporting
            if concerns or assessment not in ("consistent", "unknown"):
                all_concerns.append({
                    "unit": unit_name,
                    "assessment": assessment,
                    "concerns": concerns,
                    "notes": qc_data.get("notes", "")
                })

        # Take action based on concern severity
        if not has_significant_issues and not has_minor_concerns:
            # (i) No issues → accept answer as-is
            try:
                print("\n  ✓ All units validated successfully - answer accepted")
            except UnicodeEncodeError:
                print("\n  [OK] All units validated successfully - answer accepted")
            progress["quality_check_complete"] = True
            self.question_object["progress"] = progress
            self._save_question_object()

        elif has_significant_issues:
            # (iv) Significant issues → regenerate answer with concerns
            print("\n  ! Significant issues found - regenerating answer with quality concerns...")
            self._regenerate_answer_with_concerns(all_concerns)
            progress["quality_check_complete"] = True
            self.question_object["progress"] = progress
            self._save_question_object()

        else:
            # (iii) Minor concerns only → append to answer
            try:
                print("\n  ~ Minor concerns found - appending to answer...")
            except UnicodeEncodeError:
                print("\n  [~] Minor concerns found - appending to answer...")
            self._append_concerns_to_answer(all_concerns)
            progress["quality_check_complete"] = True
            self.question_object["progress"] = progress
            self._save_question_object()

    def _append_concerns_to_answer(self, all_concerns: List[Dict[str, Any]]) -> None:
        """
        Append quality check concerns to the existing answer.

        Args:
            all_concerns: List of concern dictionaries with unit, assessment, concerns, notes
        """
        current_answer = self.question_object["working_answer"]["text"]

        # Build concerns section
        concerns_text = []
        concerns_text.append("\n\n---\n\n")
        concerns_text.append("## Quality Review Notes\n\n")
        concerns_text.append("During validation against source document units, the following concerns were identified:\n\n")

        for concern_data in all_concerns:
            unit = concern_data["unit"]
            assessment = concern_data["assessment"]
            concerns = concern_data.get("concerns", [])
            notes = concern_data.get("notes", "")

            concerns_text.append(f"**{unit}** (Assessment: {assessment}):\n")

            if concerns:
                for concern in concerns:
                    severity = concern.get("severity", "medium")
                    description = concern.get("description", "")
                    concerns_text.append(f"- [{severity.upper()}] {description}\n")

            if notes:
                concerns_text.append(f"  *Note: {notes}*\n")

            concerns_text.append("\n")

        # Append to answer
        updated_answer = current_answer + "".join(concerns_text)
        self.question_object["working_answer"]["text"] = updated_answer
        print(f"  Appended {len(all_concerns)} quality concern(s) to answer")

    def _regenerate_answer_with_concerns(self, all_concerns: List[Dict[str, Any]]) -> None:
        """
        Regenerate the final answer incorporating quality check concerns as feedback.

        Args:
            all_concerns: List of concern dictionaries with unit, assessment, concerns, notes
        """
        scratch = self.question_object.get("scratch", {})
        scratch_text = json.dumps(scratch, indent=4)

        # Build concerns summary for the prompt
        concerns_summary = []
        concerns_summary.append("QUALITY REVIEW FINDINGS:\n")
        concerns_summary.append("The initial answer was reviewed against the full text of source units. ")
        concerns_summary.append("The following issues were identified that you must address:\n\n")

        for concern_data in all_concerns:
            unit = concern_data["unit"]
            assessment = concern_data["assessment"]
            concerns = concern_data.get("concerns", [])

            if assessment in ("inconsistent", "needs_review") or any(c.get("severity") == "high" for c in concerns):
                concerns_summary.append(f"**{unit}** (Assessment: {assessment}):\n")
                for concern in concerns:
                    description = concern.get("description", "")
                    relevant_text = concern.get("relevant_text_from_unit", "")
                    concerns_summary.append(f"  - Issue: {description}\n")
                    if relevant_text:
                        concerns_summary.append(f"    Relevant text: \"{relevant_text}\"\n")
                concerns_summary.append("\n")

        concerns_text = "".join(concerns_summary)

        # Build prompt for regeneration
        prompt = []
        prompt.append(
            "You are a legal expert providing analysis based on a comprehensive review of one or more "
            "legal documents. Using only the information provided below, answer the question.\n\n"
        )
        prompt.append(
            "Background: Multiple AI analysts have reviewed different portions (sections, articles, etc.) "
            "of one or more legal documents to extract relevant information. The findings below represent "
            "their collective analysis, with each entry including source information indicating which portion "
            "of which document contributed that information. Sources from the primary document appear as "
            "\"Section 5\" or \"Article 12\". Sources from cross-referenced external documents appear with "
            "a parenthetical document identifier, e.g., \"Section 744.17 (Part744)\" or "
            "\"Eccn 3A090 (Part774)\".\n\n"
        )
        prompt.append(
            "IMPORTANT: An initial answer was generated but quality review identified issues when "
            "cross-checked against the full text of source units. You MUST address these concerns "
            "in your answer.\n\n"
        )
        prompt.append(concerns_text)
        prompt.append("\n")
        prompt.append("Question:\n")
        prompt.append(self.question_text + "\n\n")
        prompt.append("Findings from Document Analysis:\n")
        prompt.append(scratch_text + "\n\n")
        prompt.append(
            "Instructions:\n"
            "- Provide a clear, comprehensive answer suitable for a legal practitioner.\n"
            "- CRITICAL - ADDRESS QUALITY CONCERNS: Your answer must correct the issues identified above. "
            "Ensure your statements are consistent with the source units and do not contain the errors flagged.\n"
            "- CRITICAL - CITE DOCUMENT SOURCES: When making claims or statements, you MUST reference "
            "the specific substantive units (e.g., \"Section 5\", \"Article 12\", \"Chapter 3\") where "
            "that information is found. Each fact or answer entry in the findings above includes a \"source\" "
            "field showing which document units contributed that information. Use these source labels in your "
            "answer. You may reference multiple units when information comes from multiple sources.\n"
            "- CROSS-DOCUMENT CITATIONS: When a source label includes a parenthetical document name "
            "(e.g., \"Section 3A090 (Part774)\"), include the document name in your citation "
            "(e.g., \"Part 774, ECCN 3A090\"). This indicates the information comes from a "
            "cross-referenced external document rather than the primary document being analyzed.\n"
            "- DO NOT reference internal working document identifiers like \"fact_1\", \"answer_2\", or "
            "\"question_3\". These are internal tracking IDs. ONLY reference the substantive units "
            "(Sections, Articles, Chapters, etc.) from the \"source\" fields.\n"
            "- DO NOT mention the quality review, concerns, or this regeneration process in your answer. "
            "Write as if you are directly interpreting the legal documents.\n"
            "- Address uncertainties: If the analysis includes unresolved questions that could materially "
            "affect the answer, acknowledge these limitations. Explain what information is uncertain and "
            "why it matters, so the reader understands the scope of the analysis.\n"
            "- If the available information is insufficient to fully answer the question, explain "
            "what is known and what remains uncertain.\n"
        )

        full_prompt = "".join(prompt)

        # Regenerate answer
        final_answer_max_tokens = 16000
        client = self._get_client_for_phase('final_answer')

        # Use query_text_with_retry to handle empty responses and enable fallback models
        try:
            new_answer_text = query_text_with_retry(
                client,
                [],
                full_prompt,
                self.logfile,
                max_tokens=final_answer_max_tokens,
                max_retries=3,
                config=self._config,
                task_name='qa.regenerate_answer'
            )
        except ModelError as e:
            raise ModelError(f"regenerate_answer_with_concerns: Failed to regenerate answer after retries: {e}")

        # Update answer
        self.question_object["working_answer"]["text"] = str(new_answer_text).strip()
        print(f"  Regenerated answer ({len(new_answer_text)} characters) addressing {len(all_concerns)} concern(s)")

    # ------------------------------------------------------------------
    # Utilities
    # ------------------------------------------------------------------

    def _save_question_object(self) -> None:
        with open(self.question_file, "w", encoding="utf-8") as f:
            json.dump(self.question_object, f, indent=4, ensure_ascii=False)


# ---------------------------------------------------------------------------
# CLI entry point (mirrors Ask_Question.py)
# ---------------------------------------------------------------------------


def main(argv: List[str]) -> int:
    parser = argparse.ArgumentParser(
        description="Question-answering system for legal documents",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Use preset mode
  python question_answering.py document.html "Question?" --mode thorough

  # Custom overrides
  python question_answering.py document.html "Question?" \\
    --scoring-summary summary_2 --max-iterations 5 --quality-check

  # Quick scan mode (scoring only)
  python question_answering.py document.html "Question?" --mode quick_scan
        """
    )
    
    parser.add_argument("file_path", help="Path to processed JSON file or source HTML/XML file")
    parser.add_argument("question", help="Question text or path to question.txt file")
    
    # Mode selection
    parser.add_argument(
        "--mode",
        choices=["quick_scan", "standard", "thorough", "maximum_confidence"],
        help="Q&A analysis mode (default: standard)"
    )
    
    # Individual overrides
    parser.add_argument(
        "--scoring-summary",
        choices=["summary_1", "summary_2"],
        help="Summary level to use for scoring (overrides mode default)"
    )
    parser.add_argument(
        "--org-summary-scoring",
        action="store_true",
        help="Enable organizational summary scoring (overrides mode default)"
    )
    parser.add_argument(
        "--no-org-summary-scoring",
        action="store_true",
        help="Disable organizational summary scoring (overrides mode default)"
    )
    parser.add_argument(
        "--stop-after-scoring",
        action="store_true",
        help="Stop after scoring phase, return scored sections only (overrides mode default)"
    )
    parser.add_argument(
        "--max-iterations",
        type=int,
        help="Maximum analysis iterations (overrides mode default)"
    )
    parser.add_argument(
        "--quality-check",
        action="store_true",
        help="Enable quality check phase (overrides mode default)"
    )
    parser.add_argument(
        "--analyze-zero-scores",
        action="store_true",
        help="Analyze zero-score sections if no high-relevance found (overrides mode default)"
    )
    parser.add_argument(
        "--scoring-fallback",
        action="store_true",
        help="Re-score with summary_2 if Level 1 finds nothing (overrides mode default)"
    )
    
    # Legacy positional arguments support
    if len(argv) >= 3 and not argv[1].startswith("--"):
        # Old-style positional arguments
        args = parser.parse_args([argv[1], argv[2]] + argv[3:])
    else:
        args = parser.parse_args(argv[1:])
    
    file_path = args.file_path
    question_input = args.question

    if not os.path.isfile(file_path):
        print("Input not a file.")
        return 1

    # Determine processed file path
    # If file_path ends with .xml or .html, derive processed path
    # If file_path ends with _processed.json, use as-is
    if file_path.endswith('.xml') or file_path.endswith('.html'):
        print("Working with: " + file_path)
        dir_path = os.path.dirname(file_path)
        file_stem = re.sub(r'\.\w+$', '', os.path.basename(file_path))
        processed_file_path = os.path.join(dir_path, file_stem + "_processed.json")
    elif file_path.endswith('_processed.json'):
        processed_file_path = file_path
        dir_path = os.path.dirname(file_path)
        file_stem = os.path.basename(file_path).replace('_processed.json', '')
        print("Working with processed file: " + file_path)
    else:
        print("Need html, xml, or _processed.json input file.")
        return 1

    if os.path.exists(processed_file_path):
        with open(processed_file_path, "r", encoding="utf-8") as json_file:
            parsed_content = json.load(json_file)
    else:
        print("Processed JSON file not found. Run Process_Stage_3.py first.")
        return 1

    # Load or normalize question text
    if os.path.isfile(question_input):
        if question_input.endswith(".txt"):
            with open(question_input, "r", encoding="utf-8") as q_file:
                question_text = clean_text(q_file.read())
        else:
            print("Need text input file for question.")
            return 1
    else:
        question_text = clean_text(str(question_input))

    if not question_text:
        print("No question found.")
        return 1

    # Find or create question file, mirroring Ask_Question.py naming scheme.
    # Simply find a file with matching question text - that's the one to use
    question_file = ""
    files = os.listdir(dir_path)
    
    for item_path in files:
        item_file = os.path.join(dir_path, item_path)
        if (
            os.path.isfile(item_file)
            and os.path.basename(item_file).startswith(file_stem + "_question_")
            and item_file.endswith(".json")
        ):
            with open(item_file, "r", encoding="utf-8") as item_q_file:
                try:
                    item_content = json.load(item_q_file)
                except Exception:
                    continue
                if (
                    "question" in item_content
                    and "text" in item_content["question"]
                    and question_text == item_content["question"]["text"]
                ):
                    question_file = item_file
                    break

    if not question_file:
        # Create new question file
        question_content: Dict[str, Any] = {"question": {"text": question_text}}
        count = 1
        while os.path.exists(
            os.path.join(dir_path, file_stem + "_question_" + str(count).zfill(4) + ".json")
        ):
            count += 1
        question_file = os.path.join(
            dir_path,
            file_stem + "_question_" + str(count).zfill(4) + ".json",
        )
        with open(question_file, "w", encoding="utf-8") as q_file_handle:
            json.dump(question_content, q_file_handle, indent=4, ensure_ascii=False)

    with open(question_file, "r", encoding="utf-8") as q_file_handle:
        question_object = json.load(q_file_handle)

    # Ensure question text matches
    if "question" not in question_object:
        question_object["question"] = {}
    question_object["question"]["text"] = question_text

    # Set up AI client and logfile
    from utils.config import get_config, get_qa_mode_config
    config = get_config()
    client = create_ai_client(config=config)
    logfile = GetLogfile(dir_path)

    # Build mode configuration from arguments (needed for completion check)
    mode_name = args.mode
    current_mode_config = get_qa_mode_config(mode_name=mode_name, config=config)
    current_stop_after_scoring = current_mode_config.get("stop_after_scoring", False)

    # Check if question has already been fully answered (BEFORE any processing)
    # The "complete" flag in the question file indicates the question was run to completion
    # Also check for working_answer as a fallback (for older question files)
    is_complete = question_object.get("complete", False)
    working_answer = question_object.get("working_answer", {})
    final_answer_text = working_answer.get("text", "").strip()
    previous_mode = question_object.get("qa_mode")  # Mode used in previous run

    # Allow re-running if:
    # 1. Question is complete but has no answer (e.g., quick_scan mode)
    # 2. Previous mode was quick_scan and current mode generates answers
    # 3. Mode changed and new mode generates answers
    should_rerun = False
    if is_complete:
        if not final_answer_text:
            # Completed but no answer - likely quick_scan, allow re-running
            should_rerun = True
            print("Question was previously run in quick_scan mode (no answer generated).")
            print("Re-running with current mode to generate full answer...")
        elif previous_mode == "quick_scan" and not current_stop_after_scoring:
            # Previous was quick_scan, current mode generates answers - allow upgrade
            should_rerun = True
            print(f"Question was previously run in quick_scan mode.")
            print(f"Re-running with {mode_name or 'standard'} mode to generate full answer...")
        elif previous_mode and previous_mode != (mode_name or "standard") and not current_stop_after_scoring:
            # Mode changed to one that generates answers - allow re-running
            should_rerun = True
            print(f"Question was previously run in {previous_mode} mode.")
            print(f"Re-running with {mode_name or 'standard'} mode...")
        else:
            # Question is complete with answer - display it
            print("Question has already been answered. Displaying existing answer...")
            if final_answer_text:
                # Get the question text from the file (may differ slightly from input due to cleaning)
                stored_question_text = question_object.get("question", {}).get("text", question_text)
                print(f"\nQuestion: {stored_question_text}\n")
                print("Answer:\n")
                print(final_answer_text)
            else:
                print("(No answer text found in question file)")
            return 0
    
    if should_rerun:
        # Preserve previous answer if switching between answer-generating modes
        if final_answer_text and previous_mode and previous_mode != "quick_scan" and not current_stop_after_scoring:
            # Initialize previous_answers structure if it doesn't exist
            if "previous_answers" not in question_object:
                question_object["previous_answers"] = []
            
            # Save the previous answer with metadata
            from datetime import datetime, UTC
            previous_answer_entry = {
                "mode": previous_mode,
                "answer_text": final_answer_text,
                "timestamp": datetime.now(UTC).isoformat().replace('+00:00', 'Z'),
                "saved_when": "mode_switched"
            }
            question_object["previous_answers"].append(previous_answer_entry)
            print(f"  Preserved previous answer from {previous_mode} mode in previous_answers history.")
        
        # Reset completion status to allow re-processing
        question_object["complete"] = False
        # Clear progress markers for phases that need to be re-run
        if "progress" not in question_object:
            question_object["progress"] = {}
        progress = question_object["progress"]
        # Keep scoring if we're upgrading from quick_scan (scores are still valid)
        if previous_mode == "quick_scan" and not current_stop_after_scoring:
            # Keep scoring_complete, but reset analysis phases
            progress["analysis_iterations_completed"] = 0
            progress["cleanup_complete"] = False
            progress["final_answer_complete"] = False
            print("  Keeping existing relevance scores from quick_scan mode.")
        else:
            # Different mode - may need to re-score if summary level changed
            # For now, reset everything to be safe
            progress["scoring_complete"] = False
            progress["analysis_iterations_completed"] = 0
            progress["cleanup_complete"] = False
            progress["final_answer_complete"] = False
            print("  Resetting all progress markers for new mode.")
        
        # Save the updated question object (with preserved answer)
        with open(question_file, "w", encoding="utf-8") as q_file_handle:
            json.dump(question_object, q_file_handle, indent=4, ensure_ascii=False)

    # Build mode configuration from arguments (already done above, but get full config with overrides)
    mode_config = get_qa_mode_config(mode_name=mode_name, config=config)
    
    # Apply individual overrides if provided
    if args.scoring_summary:
        mode_config["scoring_summary_level"] = args.scoring_summary
    if args.org_summary_scoring:
        mode_config["org_summary_scoring"] = True
    if args.no_org_summary_scoring:
        mode_config["org_summary_scoring"] = False
    if args.stop_after_scoring:
        mode_config["stop_after_scoring"] = True
    if args.max_iterations is not None:
        mode_config["max_analysis_passes"] = args.max_iterations
    if args.quality_check:
        mode_config["quality_check_phase"] = True
    if args.analyze_zero_scores:
        mode_config["analyze_zero_score_sections"] = True
    if args.scoring_fallback:
        mode_config["scoring_fallback_to_summary_2"] = True
    
    # Create QuestionProcessor with mode
    qp = QuestionProcessor(
        client, parsed_content, question_object, question_file, logfile,
        config=config, mode=mode_name,
        processed_file_path=processed_file_path,
    )
    
    # Override mode_config with any custom overrides
    qp.mode_config = mode_config
    
    if mode_name:
        print(f"Using Q&A mode: {mode_name}")
    else:
        print(f"Using default Q&A mode: standard")

    # Phase 1: relevance scoring (idempotent; skips already-scored items)
    print("Scoring relevance of substantive units...")
    qp.score_relevance(max_tokens=1000)

    # Check if mode is configured to stop after scoring
    if qp.mode_config.get("stop_after_scoring", False):
        print("\n" + "="*70)
        print("QUICK SCAN MODE - Showing scored sections only")
        print("="*70)
        print(f"\nQuestion: {question_text}\n")
        print("Relevant Sections (sorted by relevance score):\n")
        
        # Format and display scored sections
        scored_sections = []
        scores = qp.question_object.get("scores", {})
        param_pointer = parsed_content["document_information"]["parameters"]
        content_pointer = parsed_content["content"]
        
        for item_type in scores:
            p = _resolve_param_key(param_pointer, item_type)
            if not p:
                continue
            if not (p.get("operational") == 1 and "name" in p and "name_plural" in p):
                continue
            item_type_name = p["name"]
            item_type_names = p["name_plural"]

            for item_num, score in scores[item_type].items():
                item_data = lookup_item(parsed_content, item_type_names, item_num)
                if item_data is None:
                    continue
                unit_title = item_data.get("unit_title", "")
                scored_sections.append({
                    "type": item_type_name,
                    "number": item_num,
                    "title": unit_title,
                    "score": score
                })
        
        # Sort by score (descending), then by type and number
        scored_sections.sort(key=lambda x: (-x["score"], x["type"], x["number"]))
        
        # Display sections grouped by score
        for score_value in [3, 2, 1, 0]:
            sections_at_score = [s for s in scored_sections if s["score"] == score_value]
            if not sections_at_score:
                continue
            
            score_label = {
                3: "CLEARLY IMPORTANT",
                2: "LIKELY RELEVANT",
                1: "POSSIBLY RELEVANT",
                0: "NOT RELEVANT"
            }.get(score_value, f"Score {score_value}")
            
            print(f"\n{score_label} (Score {score_value}):")
            print("-" * 70)
            for section in sections_at_score:
                title_str = f" - {section['title']}" if section['title'] else ""
                print(f"  {section['type'].title()} {section['number']}{title_str}")
        
        print("\n" + "="*70)
        print("Quick scan complete. Use a different mode (standard, thorough, maximum_confidence)")
        print("to generate a full answer.")
        print("="*70)
        
        # Mark as complete for quick scan mode and store mode used
        qp.question_object["complete"] = True
        qp.question_object["qa_mode"] = mode_name or "quick_scan"
        qp._save_question_object()
        
        return 0

    # Phase 2: iterative analysis to fill scratch
    print("Running iterative analysis to populate scratch document...")
    qp.run_to_stability(base_max_iterations=None)  # Uses mode_config["max_analysis_passes"]

    # Phase 3: cleanup scratch and optional working answer
    print("Running scratch cleanup and answer proposal...")
    qp.cleanup_scratch_and_answer()

    # Phase 4: final answer
    print("Generating final answer...")
    qp.generate_final_answer()

    # Phase 5: quality check (optional) - runs AFTER final answer to validate it
    if qp.mode_config.get("quality_check_phase", False):
        print("Running quality check on final answer...")
        qp.quality_check_answer()

    # Mark as complete and store mode used
    qp.question_object["complete"] = True
    qp.question_object["qa_mode"] = mode_name or "standard"
    qp._save_question_object()

    print(f"\nQuestion: {question_text}\n")
    print("Answer:\n")
    print(qp.question_object["working_answer"]["text"])

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))


