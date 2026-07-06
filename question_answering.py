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
  - Propose facts, questions, and section requests for gatekeeper review.
- After WS8 rounds stabilize, synthesize a final answer from the gated scratch document.

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
from difflib import SequenceMatcher
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
    expand_element_range,
)
from utils.text_processing import strip_emphasis_marks
from utils.document_handling import _resolve_param_key


# ---------------------------------------------------------------------------
# Scratch document management
# ---------------------------------------------------------------------------


class ScratchDocumentManager:
    """
    Manage the shared scratch document used by all analyst passes (WS8 schema).

    Structure (stored under question_object['scratch']):
      {
        "primary_question": "...",
        "facts": [
          {"id": "f001", "content": "...", "source_units": ["Section 5", "Article 12"]}
        ],
        "questions": [
          {
            "id": "q001",
            "text": "...",
            "status": "open",          // "open" | "closed"
            "proposed_by": ["Section 5"],
            "seen_by": ["Section 5"],  // units that have been offered this question
            "answers": [
              {"unit_id": "Section 5", "text": "...", "substantive": true}
            ],
            "synthesis": null,         // set when closed
            "derivation_depth": 0
          }
        ],
        "hypotheses": [],              // Phase 8B placeholder
        "final_answer": null,
        "fact_pool_summary": "",       // running compressed summary of all facts
        "unresolved_questions": []
      }

    Requests are not stored in scratch — they are consumed inline by the
    orchestrator and never shown to analysts.
    """

    def __init__(self, question_object: Dict[str, Any], source_doc_label: Optional[str] = None):
        self.question_object = question_object
        self.source_doc_label = source_doc_label
        if "scratch" not in self.question_object:
            self.question_object["scratch"] = {}
        scratch = self.question_object["scratch"]
        scratch.setdefault("primary_question",
                           question_object.get("question", {}).get("text", ""))
        scratch.setdefault("facts", [])
        scratch.setdefault("questions", [])
        scratch.setdefault("hypotheses", [])
        if "final_answer" not in scratch:
            scratch["final_answer"] = None
        scratch.setdefault("fact_pool_summary", "")
        scratch.setdefault("unresolved_questions", [])
        scratch.setdefault("rejected_inquiries", [])
        scratch.setdefault("bridge_rejections", [])

    @property
    def scratch(self) -> Dict[str, Any]:
        return self.question_object["scratch"]

    @property
    def facts(self) -> List[Dict[str, Any]]:
        return self.scratch["facts"]

    @property
    def questions(self) -> List[Dict[str, Any]]:
        return self.scratch["questions"]

    # ------------------------------------------------------------------
    # ID helpers
    # ------------------------------------------------------------------

    def _next_fact_id(self) -> str:
        existing = {f["id"] for f in self.facts}
        n = len(self.facts) + 1
        while f"f{n:03d}" in existing:
            n += 1
        return f"f{n:03d}"

    def _next_question_id(self) -> str:
        existing = {q["id"] for q in self.questions}
        n = len(self.questions) + 1
        while f"q{n:03d}" in existing:
            n += 1
        return f"q{n:03d}"

    def _label(self, unit_id: str) -> str:
        """Append source_doc_label when present."""
        if self.source_doc_label:
            return f"{unit_id} ({self.source_doc_label})"
        return unit_id

    # ------------------------------------------------------------------
    # Fact operations
    # ------------------------------------------------------------------

    def add_fact(self, content: str, source_unit: str) -> str:
        """Add a new fact; return its ID. Gating is caller's responsibility."""
        fact_id = self._next_fact_id()
        self.facts.append({
            "id": fact_id,
            "content": content.strip(),
            "source_units": [self._label(source_unit)],
        })
        return fact_id

    def merge_fact(self, fact_id: str, source_unit: str,
                   updated_content: Optional[str] = None) -> bool:
        """Add a source to an existing fact; optionally replace content."""
        label = self._label(source_unit)
        for fact in self.facts:
            if fact["id"] == fact_id:
                if label not in fact["source_units"]:
                    fact["source_units"].append(label)
                if updated_content:
                    fact["content"] = updated_content.strip()
                return True
        return False

    def get_fact_by_id(self, fact_id: str) -> Optional[Dict[str, Any]]:
        for fact in self.facts:
            if fact["id"] == fact_id:
                return fact
        return None

    @property
    def fact_count(self) -> int:
        return len(self.facts)

    # ------------------------------------------------------------------
    # Question operations
    # ------------------------------------------------------------------

    def add_question(
        self,
        text: str,
        proposed_by_unit: str,
        rationale: str = "",
        derivation_depth: int = 0,
    ) -> str:
        """Add a new open question; return its ID."""
        q_id = self._next_question_id()
        self.questions.append({
            "id": q_id,
            "text": text.strip(),
            "rationale": rationale.strip(),
            "status": "open",
            "proposed_by": [self._label(proposed_by_unit)],
            "seen_by": [],
            "answers": [],
            "synthesis": None,
            "derivation_depth": derivation_depth,
            "supporting_fact_ids": [],
        })
        return q_id

    def merge_question(self, question_id: str, additional_proposer: str) -> bool:
        """Record that another unit also proposed this question."""
        label = self._label(additional_proposer)
        for q in self.questions:
            if q["id"] == question_id:
                if label not in q["proposed_by"]:
                    q["proposed_by"].append(label)
                return True
        return False

    def add_rejected_inquiry(
        self,
        kind: str,
        text: str,
        source_unit: str,
        analyst_reason: str = "",
        gate_reason: str = "",
        depth: int = 0,
    ) -> None:
        """
        Record a rejected section request or sub-question for post-run visibility.

        Dedupes on (kind, text, source_unit, gate_reason): the same proposer hitting
        the same gate verdict for the same target repeatedly produces only one entry,
        which keeps the diagnostic section bounded on noisy runs.
        """
        text_norm = text.strip()
        source_norm = self._label(source_unit)
        gate_norm = (gate_reason or "").strip()
        for existing in self.scratch["rejected_inquiries"]:
            if (
                existing.get("kind") == kind
                and existing.get("text") == text_norm
                and existing.get("source_unit") == source_norm
                and existing.get("gate_reason") == gate_norm
            ):
                return
        self.scratch["rejected_inquiries"].append({
            "kind": kind,
            "text": text_norm,
            "source_unit": source_norm,
            "analyst_reason": (analyst_reason or "").strip(),
            "gate_reason": gate_norm,
            "depth": depth,
        })

    def add_bridge_rejection(
        self,
        fact_content: str,
        source_unit: str,
        sub_question: str,
        rationale: str = "",
    ) -> None:
        """Record a sub-question-context fact rejection for later tuning."""
        self.scratch["bridge_rejections"].append({
            "fact_content": fact_content.strip(),
            "source_unit": self._label(source_unit),
            "sub_question": sub_question.strip(),
            "rationale": (rationale or "").strip(),
        })

    def mark_question_seen(self, question_id: str, unit_id: str) -> bool:
        """Record that a unit has been offered this question."""
        label = self._label(unit_id)
        for q in self.questions:
            if q["id"] == question_id:
                if label not in q["seen_by"]:
                    q["seen_by"].append(label)
                return True
        return False

    def add_answer(self, question_id: str, unit_id: str, text: str,
                   substantive: bool = True) -> bool:
        """Record an answer; also marks the unit as having seen the question."""
        label = self._label(unit_id)
        for q in self.questions:
            if q["id"] == question_id:
                q["answers"].append({
                    "unit_id": label,
                    "text": text.strip(),
                    "substantive": substantive,
                })
                if label not in q["seen_by"]:
                    q["seen_by"].append(label)
                return True
        return False

    def close_question(self, question_id: str, synthesis: str) -> bool:
        """Mark a question closed with a synthesized answer."""
        for q in self.questions:
            if q["id"] == question_id:
                q["status"] = "closed"
                q["synthesis"] = synthesis.strip() if synthesis else ""
                return True
        return False

    def get_open_questions(self) -> List[Dict[str, Any]]:
        return [q for q in self.questions if q["status"] == "open"]

    def is_question_exhausted(self, question: Dict[str, Any],
                               active_unit_labels: set) -> bool:
        """True if every active unit has been offered this question."""
        return active_unit_labels.issubset(set(question["seen_by"]))

    def get_workable_open_questions(self, active_unit_labels: set) -> List[Dict[str, Any]]:
        """Open questions that have not yet been offered to all active units."""
        return [q for q in self.questions
                if q["status"] == "open"
                and not self.is_question_exhausted(q, active_unit_labels)]

    def get_unseen_questions_for_unit(self, unit_id: str) -> List[Dict[str, Any]]:
        """Open questions the given unit has not yet been offered."""
        label = self._label(unit_id)
        return [q for q in self.questions
                if q["status"] == "open" and label not in q["seen_by"]]

    def get_substantive_responders(self, question_id: str) -> List[str]:
        """Return unit labels that gave substantive answers to this question."""
        for q in self.questions:
            if q["id"] == question_id:
                return [a["unit_id"] for a in q["answers"] if a.get("substantive", True)]
        return []

    @property
    def open_question_count(self) -> int:
        return sum(1 for q in self.questions if q["status"] == "open")

    # ------------------------------------------------------------------
    # Fact pool context for gatekeepers
    # ------------------------------------------------------------------

    def get_fact_pool_context(self, threshold_chars: int = 15000) -> str:
        """
        Return a context string describing the current fact pool.

        If total fact text is under threshold: full list.
        If over: running summary as header, then the most-recent facts in full
        until the threshold is reached (from the newest end backward).
        """
        if not self.facts:
            return "(No facts collected yet.)"

        lines = [
            f"[{f['id']}] {f['content']}  (sources: {', '.join(f['source_units'])})"
            for f in self.facts
        ]
        full_text = "\n".join(lines)

        if len(full_text) <= threshold_chars:
            return f"Established facts:\n{full_text}"

        # Over threshold: summary header + most-recent facts in full
        summary = self.scratch.get("fact_pool_summary", "")
        header = (f"Summary of established facts:\n{summary}\n\nMost recent facts (full detail):\n"
                  if summary else "Most recent facts (full detail):\n")
        remaining = threshold_chars - len(header)
        recent: List[str] = []
        for line in reversed(lines):
            if len(line) + 1 > remaining:
                break
            recent.insert(0, line)
            remaining -= len(line) + 1
        return header + "\n".join(recent)

    def update_fact_pool_summary(self, new_summary: str) -> None:
        self.scratch["fact_pool_summary"] = new_summary


# ---------------------------------------------------------------------------
# Gatekeeper functions (Tasks 8.3 / 8.5 / 8.R6)
# ---------------------------------------------------------------------------

# Close-match fact gate (task 8.R6) — see WORKSTREAM_8_PLAN.md.
# Facts whose normalized text matches an existing fact exactly are auto-merged
# (no LLM call). Facts whose similarity to any existing fact is at or above the
# configured threshold are routed to a specialized close-match gate prompt that
# names the top neighbors and asks whether the difference is material.
# Tunables live in config.json (processing.fact_gate_close_match.threshold and
# .max_neighbors), accessed via utils/config.py.


def _normalize_fact_text(text: str) -> str:
    """Normalize whitespace and curly-quote variants for exact-equivalence checks."""
    if not text:
        return ""
    t = re.sub(r"[“”‘’`]", '"', text)
    t = re.sub(r"\s+", " ", t).strip().lower()
    return t


def _texts_equivalent(a: str, b: str) -> bool:
    return _normalize_fact_text(a) == _normalize_fact_text(b)


def _find_close_match_neighbors(
    proposed: str,
    items: List[Dict[str, Any]],
    threshold: float,
    max_neighbors: int,
    content_key: str = "content",
) -> List[Tuple[float, Dict[str, Any]]]:
    """Return top-N pool items with similarity >= threshold to the proposed text,
    sorted by similarity descending. The content_key controls which dict field
    holds the text to compare — 'content' for facts, 'text' for questions."""
    p_norm = _normalize_fact_text(proposed)
    if not p_norm:
        return []
    scored: List[Tuple[float, Dict[str, Any]]] = []
    for item in items:
        i_norm = _normalize_fact_text(item.get(content_key, ""))
        if not i_norm:
            continue
        sim = SequenceMatcher(None, p_norm, i_norm).ratio()
        if sim >= threshold:
            scored.append((sim, item))
    scored.sort(key=lambda x: -x[0])
    return scored[:max_neighbors]


def _gate_close_match_fact(
    client,
    config: Optional[Dict],
    logfile: str,
    primary_question: str,
    proposed_content: str,
    source_unit: str,
    neighbors: List[Tuple[float, Dict[str, Any]]],
    question_context: Optional[str] = None,
    question_rationale: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Close-match variant of the fact gate (task qa.gate.fact_close_match).

    Invoked when the proposed fact has at least one pool neighbor above the
    similarity threshold. The prompt cites the named neighbors and frames the
    decision narrowly: accept only if the difference is material enough that
    merging would mislead. Verdict options: accept or merge (no reject — by
    construction the fact is closely related to established material).

    When question_context (a sub-question) and question_rationale (the bridge
    justification produced by the question proposer) are both present, the
    prompt instructs the gate to evaluate the proposed fact against both the
    sub-question AND the primary question via the rationale-bridge.
    """
    if question_context:
        rationale_clause = (
            f"\nRationale connecting this sub-question to the primary question: "
            f"\"{question_rationale}\""
        ) if question_rationale else ""
        question_being_evaluated = (
            f"Sub-question being evaluated: {question_context}{rationale_clause}\n"
            f"(Primary question for context: {primary_question})"
        )
    else:
        question_being_evaluated = f"Question being evaluated: {primary_question}"

    neighbor_lines = [
        f"[{fact['id']}] \"{fact['content']}\"  (similarity: {sim:.2f})"
        for sim, fact in neighbors
    ]
    neighbor_block = "\n".join(neighbor_lines)

    prompt = (
        f"{question_being_evaluated}\n\n"
        f"The following existing pool fact(s) are very similar to a newly proposed fact:\n"
        f"{neighbor_block}\n\n"
        f"Proposed new fact from {source_unit}:\n\"{proposed_content}\"\n\n"
        "Accept the proposed fact as a NEW fact only if its difference from the closest "
        "existing fact above is MATERIAL — that is, merging the two would lose important "
        "information or mislead a reader about the rule, scope, condition, or exception "
        "the fact describes. Differences in wording, citation form, level of paraphrase, "
        "or the addition of inconsequential detail are NOT material; in those cases, "
        "merge into the closest matching existing fact.\n\n"
        "If you merge, cite the existing fact's ID. You may optionally supply an improved "
        "merged statement that preserves wording or citations worth keeping from either fact.\n\n"
        "Respond with ONLY a JSON object in one of these exact forms:\n"
        '{"verdict": "accept"}\n'
        '{"verdict": "merge", "merge_fact_id": "f001"}\n'
        '{"verdict": "merge", "merge_fact_id": "f001", "merged_content": "Combined statement."}\n'
    )

    try:
        result = query_json(client, [], prompt, logfile, max_tokens=500,
                            config=config, task_name="qa.gate.fact_close_match")
    except Exception:
        # Fail-conservative: merge with closest neighbor (avoid leaking a duplicate).
        return {"verdict": "merge", "merge_fact_id": neighbors[0][1]["id"]}

    if not isinstance(result, dict):
        return {"verdict": "merge", "merge_fact_id": neighbors[0][1]["id"]}

    verdict = str(result.get("verdict", "merge")).strip().lower()
    if verdict == "accept":
        return {"verdict": "accept"}

    out: Dict[str, Any] = {"verdict": "merge"}
    fid = str(result.get("merge_fact_id", "")).strip()
    neighbor_ids = {fact["id"] for _, fact in neighbors}
    if fid in neighbor_ids:
        out["merge_fact_id"] = fid
        mc = str(result.get("merged_content", "")).strip()
        if mc:
            out["merged_content"] = mc
    else:
        # Cited ID wasn't one of the neighbors — fall back to closest match.
        out["merge_fact_id"] = neighbors[0][1]["id"]
    return out


def _gate_and_merge_fact(
    scratch_manager: "ScratchDocumentManager",
    client,
    config: Optional[Dict],
    logfile: str,
    primary_question: str,
    proposed_content: str,
    source_unit: str,
    question_context: Optional[str] = None,
    close_match_client=None,
    question_rationale: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Gate a proposed fact against the current fact pool (task qa.gate.fact).

    Returns one of:
      {"verdict": "accept"}
      {"verdict": "reject"}
      {"verdict": "merge", "merge_fact_id": "f001"}
      {"verdict": "merge", "merge_fact_id": "f001", "merged_content": "..."}

    Pre-gate similarity check (task 8.R6):
    - If the proposed fact is text-equivalent to an existing fact (after whitespace
      and quote normalization), auto-merge with no AI call.
    - If the proposed fact has any pool neighbor at or above the close-match
      threshold, route to _gate_close_match_fact (the specialized prompt that
      names the neighbors) using close_match_client (which may be bound to a
      different model than the general gate client). Falls back to client if
      close_match_client is None.
    - Otherwise, fall through to the general gate prompt below.

    Rationale-bridging (task 8.R8): when gating against a sub-question (i.e.,
    question_context is provided), question_rationale carries the proposer's
    stated reason the sub-question's answer would matter for the primary
    question. The prompt requires the fact to (a) help answer the sub-question
    AND (b) plausibly affect the primary answer via that rationale.

    No empty-pool shortcut: even the first fact ever proposed must pass the
    relevance criteria (and the rationale-bridge criterion when applicable).
    The general gate prompt handles the empty-pool case gracefully — the
    fact_pool_ctx becomes "(No facts collected yet.)" and only accept/reject
    are reachable (merge requires a pool entry).
    """
    # Pre-gate similarity check (thresholds from config; see processing.fact_gate_close_match)
    from utils.config import (
        get_fact_gate_close_match_threshold,
        get_fact_gate_close_match_max_neighbors,
    )
    neighbors = _find_close_match_neighbors(
        proposed_content,
        scratch_manager.facts,
        threshold=get_fact_gate_close_match_threshold(config),
        max_neighbors=get_fact_gate_close_match_max_neighbors(config),
    )
    if neighbors:
        top_sim, top_fact = neighbors[0]
        # Exact match: auto-merge, no AI call
        if _texts_equivalent(proposed_content, top_fact.get("content", "")):
            return {"verdict": "merge", "merge_fact_id": top_fact["id"]}
        # Close match: specialized prompt decides the dedup question. On `merge`,
        # return immediately. On `accept` (= materially distinct from named
        # neighbors), FALL THROUGH to the general gate below so the proposed fact
        # is still subject to the relevance criteria and the rationale-bridge
        # criterion when gating against a sub-question. Material distinctness
        # from existing facts is necessary but not sufficient.
        cm_result = _gate_close_match_fact(
            close_match_client or client, config, logfile,
            primary_question, proposed_content, source_unit, neighbors,
            question_context=question_context,
            question_rationale=question_rationale,
        )
        if cm_result.get("verdict") == "merge":
            return cm_result
        # Else (accept): fall through to general gate below.

    fact_pool_ctx = scratch_manager.get_fact_pool_context()

    if question_context:
        rationale_clause = (
            f"\nRationale connecting this sub-question to the primary question: "
            f"\"{question_rationale}\""
        ) if question_rationale else ""
        question_being_evaluated = (
            f"Sub-question being evaluated: {question_context}{rationale_clause}\n"
            f"(Primary question for context: {primary_question})"
        )
    else:
        question_being_evaluated = f"Question being evaluated: {primary_question}"

    # When gating against a sub-question with a rationale, add the rationale-bridge
    # criterion that requires the fact to plausibly affect the primary answer.
    bridge_criterion = ""
    if question_context and question_rationale:
        bridge_criterion = (
            "\n\nIMPORTANT — RATIONALE BRIDGE: When gating against a sub-question, the "
            "fact must satisfy BOTH of the following:\n"
            "(a) The fact materially helps answer the sub-question on its own terms; AND\n"
            "(b) In light of the rationale above (which states why the sub-question's "
            "answer would matter to the primary question), the fact must plausibly affect "
            "or sharpen the answer to the PRIMARY question. Reject if (b) fails: a fact "
            "that answers the sub-question but whose content would not change the answer "
            "to the primary question does not belong in this pool. Patterns that commonly "
            "fail (b): procedural mechanics of unrelated provisions; enforcement remedies "
            "that only matter once a violation is established; rules governing a different "
            "actor than the primary question concerns; definitions used elsewhere in the "
            "same article that do not bear on the primary subject."
        )

    prompt = (
        f"{question_being_evaluated}\n\n"
        f"{fact_pool_ctx}\n\n"
        f"Proposed new fact from {source_unit}:\n\"{proposed_content}\"\n\n"
        "Decide whether to accept, reject, or merge this proposed fact.\n\n"
        "ACCEPT if the fact provides a specific piece of information that would appear in — "
        "or be directly cited by — a correct, complete answer to the question. This includes:\n"
        "- Substantive rules, requirements, obligations, or permissions that specifically "
        "name or govern the subject of the question\n"
        "- Definitions of terms that appear in the question or in a governing rule, where "
        "the definition could affect who or what is covered by that rule\n"
        "- Exceptions, conditions, or qualifications that bear directly on the subject of "
        "the question\n"
        "If you are uncertain whether to accept, apply this tiebreaker: would a reader who "
        "had all other established facts but not this one notice its absence in a correct "
        "answer? If yes, accept. If the answer would be equally complete and accurate "
        "without it, reject.\n\n"
        "REJECT if any of the following apply:\n"
        "- The fact addresses the broader topic or legal area of the question but does not "
        "specifically address what the question asks — being in the same legal instrument or "
        "subject area is not sufficient; the fact must bear on the specific matter the "
        "question asks about\n"
        "- The fact's connection to the question subject depends on an inferential chain not "
        "stated in the fact itself or in the existing fact pool — the fact is about topic X, "
        "but establishing that X is relevant to the question requires reasoning such as "
        "'X applies to Y, and Y sometimes involves the question subject'\n"
        "- The fact is general background, document scope, or structural information that "
        "does not state a substantive rule bearing on the question subject\n"
        "- The same information is already captured at sufficient generality by an existing fact\n\n"
        "MERGE if the proposed fact and an existing fact convey the same essential information "
        "— a reader of both would learn nothing from the second that they did not already know "
        "from the first — but the proposed fact adds wording, specificity, or a source "
        "citation worth preserving. Provide the existing fact's ID and an optional improved "
        "combined statement. Do not merge facts that make genuinely distinct points even if "
        "they concern the same section or rule."
        f"{bridge_criterion}\n\n"
        "Respond with ONLY a JSON object in one of these exact forms:\n"
        '{"verdict": "accept"}\n'
        '{"verdict": "reject"}\n'
        '{"verdict": "merge", "merge_fact_id": "f001"}\n'
        '{"verdict": "merge", "merge_fact_id": "f001", "merged_content": "Combined statement."}\n'
    )

    try:
        result = query_json(client, [], prompt, logfile, max_tokens=500,
                            config=config, task_name="qa.gate.fact")
    except Exception:
        return {"verdict": "accept"}  # fail-open

    if not isinstance(result, dict):
        return {"verdict": "accept"}

    verdict = str(result.get("verdict", "accept")).strip().lower()
    if verdict not in ("accept", "reject", "merge"):
        verdict = "accept"

    out: Dict[str, Any] = {"verdict": verdict}
    if verdict == "merge":
        fid = str(result.get("merge_fact_id", "")).strip()
        if fid and scratch_manager.get_fact_by_id(fid):
            out["merge_fact_id"] = fid
            mc = str(result.get("merged_content", "")).strip()
            if mc:
                out["merged_content"] = mc
        else:
            out["verdict"] = "accept"  # referenced ID not found — just accept
    elif verdict == "reject" and question_context:
        # Record bridge-context rejections for post-run tuning of the rationale-bridge
        # criterion. We only capture rejections that occurred while gating against a
        # sub-question, since those are the ones whose acceptance/rejection turns on
        # the bridge to the primary question rather than on local relevance.
        scratch_manager.add_bridge_rejection(
            fact_content=proposed_content,
            source_unit=source_unit,
            sub_question=question_context,
            rationale=question_rationale or "",
        )

    return out


def _update_fact_pool_summary(
    scratch_manager: "ScratchDocumentManager",
    client,
    config: Optional[Dict],
    logfile: str,
    primary_question: str,
) -> None:
    """
    Update the running fact pool summary (task qa.gate.fact_summary).

    No-op when fewer than 5 facts (the full pool is always shown below the threshold).
    """
    if scratch_manager.fact_count < 5:
        return

    facts_text = "\n".join(
        f"[{f['id']}] {f['content']}"
        for f in scratch_manager.facts
    )
    current_summary = scratch_manager.scratch.get("fact_pool_summary", "")

    prompt = (
        f"Primary question: {primary_question}\n\n"
        f"Current summary: {current_summary or '(none yet)'}\n\n"
        f"All current facts:\n{facts_text}\n\n"
        "Write a concise 3–5 sentence summary describing what has been established "
        "so far about the primary question. Focus on key findings, patterns, "
        "and any notable gaps.\n"
        "Respond with only the summary text, no JSON wrapper.\n"
    )

    try:
        new_summary = query_text_with_retry(
            client, [], prompt, logfile, max_tokens=500,
            config=config, task_name="qa.gate.fact_summary",
        )
        if new_summary and new_summary.strip():
            scratch_manager.update_fact_pool_summary(new_summary.strip())
    except Exception:
        pass


def _gate_close_match_question(
    client,
    config: Optional[Dict],
    logfile: str,
    primary_question: str,
    proposed_question: str,
    proposed_by_unit: str,
    rationale: str,
    neighbors: List[Tuple[float, Dict[str, Any]]],
) -> Dict[str, Any]:
    """
    Close-match variant of the question gate (task qa.gate.question_close_match).

    Invoked when the proposed sub-question has at least one open-question neighbor
    above the similarity threshold. The prompt cites the named neighbors and frames
    the decision narrowly: accept only if the proposed question would extract
    substantively different facts or shed light on a different aspect of the
    primary question. Verdict options: accept or merge (no reject — by construction
    the proposed question is closely related to an existing one).
    """
    neighbor_lines = [
        f"[{q['id']}] \"{q['text']}\"  (similarity: {sim:.2f})"
        for sim, q in neighbors
    ]
    neighbor_block = "\n".join(neighbor_lines)
    rationale_block = (
        f"Analyst's rationale: \"{rationale}\"\n\n" if rationale else ""
    )

    prompt = (
        f"Primary question: {primary_question}\n\n"
        f"The following existing open question(s) are very similar to a newly proposed "
        f"sub-question:\n{neighbor_block}\n\n"
        f"Proposed new sub-question from {proposed_by_unit}:\n\"{proposed_question}\"\n"
        f"{rationale_block}"
        "Accept the proposed sub-question as a NEW question only if its difference from "
        "the closest existing question is MATERIAL — that is, the proposed question would "
        "elicit substantively different facts, target a different aspect of the primary "
        "question, or apply a different theory. Differences in wording, level of generality, "
        "or paraphrase alone are NOT material; in those cases, merge into the closest "
        "existing question.\n\n"
        "If you merge, cite the existing question's ID.\n\n"
        "Respond with ONLY a JSON object in one of these exact forms:\n"
        '{"verdict": "accept"}\n'
        '{"verdict": "merge", "merge_question_id": "q001"}\n'
    )

    try:
        result = query_json(client, [], prompt, logfile, max_tokens=300,
                            config=config, task_name="qa.gate.question_close_match")
    except Exception:
        # Fail-conservative: merge with closest neighbor (avoid duplicate question).
        return {"verdict": "merge", "merge_question_id": neighbors[0][1]["id"]}

    if not isinstance(result, dict):
        return {"verdict": "merge", "merge_question_id": neighbors[0][1]["id"]}

    verdict = str(result.get("verdict", "merge")).strip().lower()
    if verdict == "accept":
        return {"verdict": "accept"}

    out: Dict[str, Any] = {"verdict": "merge"}
    qid = str(result.get("merge_question_id", "")).strip()
    neighbor_ids = {q["id"] for _, q in neighbors}
    if qid in neighbor_ids:
        out["merge_question_id"] = qid
    else:
        out["merge_question_id"] = neighbors[0][1]["id"]
    return out


def _gate_question(
    scratch_manager: "ScratchDocumentManager",
    client,
    config: Optional[Dict],
    logfile: str,
    primary_question: str,
    proposed_question: str,
    proposed_by_unit: str,
    rationale: str = "",
    close_match_client=None,
) -> Dict[str, Any]:
    """
    Gate a proposed question (task qa.gate.question).

    Returns one of:
      {"verdict": "accept"}
      {"verdict": "reject"}
      {"verdict": "merge", "merge_question_id": "q001"}

    Pre-gate similarity check (task 8.R8): if the proposed question has any
    open-question neighbor at or above the question close-match threshold,
    route to _gate_close_match_question (the specialized prompt that names the
    neighbors). Otherwise fall through to the general gate prompt below.

    The general gate prompt now leans on the rationale: a sub-question whose
    rationale states only generic topical-relatedness (e.g., 'same legal area',
    'might be relevant') is rejected.
    """
    # Cheap heuristic first: is it basically a restatement of the primary question?
    if _is_near_duplicate_of_primary(proposed_question, primary_question):
        return {"verdict": "reject", "reason": "near-duplicate of the primary question"}

    existing_open = [q for q in scratch_manager.questions if q["status"] == "open"]
    has_facts = bool(scratch_manager.facts)

    # No empty-context shortcut: even the first proposed sub-question must pass
    # the rationale-specificity criterion. The general gate prompt handles the
    # empty case gracefully (fact_block becomes empty; existing_lines == "(none)").

    # Pre-gate similarity check against open questions. The close-match path's
    # only job is the dedup decision. On `merge`, return immediately. On
    # `accept` (= "materially distinct from named neighbors"), FALL THROUGH to
    # the general gate so the proposed question is still subject to the
    # rationale-specificity and primary-relevance criteria. Material
    # distinctness from existing questions is necessary but not sufficient.
    if existing_open:
        from utils.config import (
            get_question_gate_close_match_threshold,
            get_question_gate_close_match_max_neighbors,
        )
        q_neighbors = _find_close_match_neighbors(
            proposed_question,
            existing_open,
            threshold=get_question_gate_close_match_threshold(config),
            max_neighbors=get_question_gate_close_match_max_neighbors(config),
            content_key="text",
        )
        if q_neighbors:
            top_sim, top_q = q_neighbors[0]
            # Exact-text duplicate: auto-merge with no AI call.
            if _texts_equivalent(proposed_question, top_q.get("text", "")):
                return {"verdict": "merge", "merge_question_id": top_q["id"]}
            cm_result = _gate_close_match_question(
                close_match_client or client, config, logfile,
                primary_question, proposed_question, proposed_by_unit, rationale,
                q_neighbors,
            )
            if cm_result.get("verdict") == "merge":
                return cm_result
            # Else (accept): fall through to general gate below.

    rationale_block = (
        f"Analyst's rationale: \"{rationale}\"\n\n" if rationale else ""
    )

    fact_block = ""
    if has_facts:
        fact_pool_ctx = scratch_manager.get_fact_pool_context()
        fact_block = f"{fact_pool_ctx}\n\n"

    existing_lines = "\n".join(
        f"[{q['id']}] {q['text']}" for q in existing_open
    ) if existing_open else "(none)"

    prompt = (
        f"Primary question: {primary_question}\n\n"
        f"{fact_block}"
        f"Proposed question from {proposed_by_unit}:\n\"{proposed_question}\"\n"
        f"{rationale_block}"
        f"Open questions already in the research queue:\n{existing_lines}\n\n"
        "Decide:\n"
        "- 'accept': the question is specific, substantive, directly advances the primary "
        "question, and is not already answered by the established facts above. The "
        "analyst's rationale must describe a SPECIFIC way the sub-question's answer "
        "would change or sharpen the answer to the primary question.\n"
        "- 'reject': any of the following apply:\n"
        "    * Restatement of the primary question, or merely a generalization/specialization "
        "of it that would not extract additional information.\n"
        "    * Too vague to drive useful fact extraction.\n"
        "    * Substantially answered by the established facts above.\n"
        "    * Already covered by an open question.\n"
        "    * The rationale shows only GENERIC topical-relatedness — 'same legal area', "
        "'might be relevant', 'related to attendance', 'could matter for X'. The rationale "
        "must name a concrete mechanism by which the sub-question's answer would influence "
        "the primary answer. If the rationale would apply equally to many unrelated "
        "sub-questions about the same statute, it is too generic.\n"
        "    * The likely answers would be enforcement remedies, procedural mechanics, or "
        "rules about a different actor — i.e., content that wouldn't actually change the "
        "primary answer even when fully answered.\n"
        "- 'merge': substantially duplicates an existing open question (provide its ID).\n\n"
        "Important: if the established facts already give a clear answer to the proposed "
        "question, reject it — there is no value in researching a question the facts "
        "already resolve.\n\n"
        "Respond with ONLY JSON in one of these forms:\n"
        '{"verdict": "accept"}\n'
        '{"verdict": "reject", "reason": "Brief reason for rejection."}\n'
        '{"verdict": "merge", "merge_question_id": "q001"}\n'
    )

    try:
        result = query_json(client, [], prompt, logfile, max_tokens=300,
                            config=config, task_name="qa.gate.question")
    except Exception:
        return {"verdict": "accept"}

    if not isinstance(result, dict):
        return {"verdict": "accept"}

    verdict = str(result.get("verdict", "accept")).strip().lower()
    if verdict not in ("accept", "reject", "merge"):
        verdict = "accept"

    out: Dict[str, Any] = {"verdict": verdict}
    if verdict == "merge":
        qid = str(result.get("merge_question_id", "")).strip()
        existing_ids = {q["id"] for q in scratch_manager.questions}
        if qid and qid in existing_ids:
            out["merge_question_id"] = qid
        else:
            out["verdict"] = "accept"
    elif verdict == "reject":
        reason = str(result.get("reason", "")).strip()
        if reason:
            out["reason"] = reason

    return out


# Materiality-based section request gate (task 8.R7) — see WORKSTREAM_8_PLAN.md.
# The gate sees the requested target's summary and a truncated excerpt of its
# actual text, and decides against a high materiality bar rather than from the
# requestor's justification alone. The excerpt cap is tunable via config.json
# (processing.request_gate_text_excerpt_max_chars).


# ---------------------------------------------------------------------------
# Section-request target resolution (8.R9)
# ---------------------------------------------------------------------------
#
# Stage 4 section requests come from analysts as strings that the resolver
# turns into actual unit dicts. Surface forms include:
#
#   - "11121"                       — bare canonical number (literal match)
#   - "11121(e)"                    — paragraph subscript appended
#   - "11123 (full text)"           — explanatory parenthetical
#   - "11130 et seq."               — Latin range marker
#   - {"first": ..., "last": ...}   — analyst-emitted explicit range
#
# Resolution strategy:
#   1. Literal lookup_item in the primary doc.
#   2. If miss, fall back to Stage 2's find_substantive_unit_with_maximum_matching,
#      which does longest-backward-prefix match (handles paragraph subscripts,
#      parentheticals, trailing markers — anything where the next char after the
#      real unit number is non-alphanumeric).
#   3. If still miss, try each external doc with the same two-step pattern.
#   4. For ranges, expand via expand_element_range up to range_cap; if exceeded,
#      fall back to first endpoint only and record truncation.
#
# Why pre-resolve before gating: the gate decision needs the target's actual
# summary and text excerpt, and the enqueue path needs the canonical number to
# match a real dict key. Doing the resolution once and threading the result
# through preserves the 8.R7 invariant that the gate and the enqueue agree on
# what target is being judged — extended now to "target set" for ranges.


def _find_type_in_parameters(
    parsed_content: Optional[Dict[str, Any]], target_type: str
) -> Optional[Tuple[str, str, str]]:
    """Find the operational parameter entry matching target_type (case-insensitive).

    Returns (canonical_name, canonical_name_plural, param_key) or None.
    """
    if not parsed_content:
        return None
    param_pointer = parsed_content.get("document_information", {}).get("parameters", {})
    for pk, pd in param_pointer.items():
        if not isinstance(pd, dict):
            continue
        if not (pd.get("operational") == 1 and pd.get("name")):
            continue
        if pd["name"].lower() == target_type.lower():
            return pd["name"], pd.get("name_plural", ""), str(pk)
    return None


def _lookup_with_max_match(
    parsed_content: Dict[str, Any], target_type: str, designation: str
) -> Optional[Tuple[str, str, Dict[str, Any]]]:
    """Look up a unit by literal match first, then backward-prefix max-match fallback.

    Returns (canonical_type, canonical_number, working_item) or None. The canonical
    type/number are taken from the document's own parameters table (not the
    analyst's surface form), so downstream callers get clean state.
    """
    type_info = _find_type_in_parameters(parsed_content, target_type)
    if type_info is None:
        return None
    canonical_type, type_plural, _ = type_info

    # Literal match — exact-key membership in the doc's actual unit dict.
    wi = lookup_item(parsed_content, type_plural, designation)
    if wi is not None:
        return canonical_type, designation, wi

    # Backward-prefix fallback (Stage 2 primitive). Only fires when literal misses,
    # so a real sibling section cannot be silently shadowed by a parent.
    matched_type, matched_num = find_substantive_unit_with_maximum_matching(
        parsed_content, target_type, designation
    )
    if matched_type and matched_num:
        # The max-match helper may return a different type (e.g., "section" when
        # the analyst said "subsection"). Re-derive plural from the doc's params.
        matched_type_info = _find_type_in_parameters(parsed_content, matched_type)
        if matched_type_info is not None:
            canonical_type, matched_plural, _ = matched_type_info
        else:
            _, matched_plural = canonical_org_types(matched_type)
        wi = lookup_item(parsed_content, matched_plural, matched_num)
        if wi is not None:
            return canonical_type, matched_num, wi
    return None


def _build_target_info(
    parsed_content: Dict[str, Any],
    canonical_type: str,
    canonical_number: str,
    working_item: Dict[str, Any],
    source_doc_label: Optional[str],
    source_doc_file: Optional[str],
    text_excerpt_max_chars: int,
) -> Dict[str, Any]:
    """Bundle a resolved unit into the dict shape passed through gate + enqueue."""
    type_info = _find_type_in_parameters(parsed_content, canonical_type)
    if type_info is not None:
        _, type_plural, type_param_key = type_info
    else:
        _, type_plural = canonical_org_types(canonical_type)
        type_param_key = None

    summary = (
        working_item.get("summary_2")
        or working_item.get("summary_1")
        or working_item.get("unit_title")
        or ""
    )
    text = working_item.get("text") or ""
    if len(text) > text_excerpt_max_chars:
        text_excerpt = text[:text_excerpt_max_chars] + "\n[... truncated ...]"
    else:
        text_excerpt = text

    return {
        "parsed_content": parsed_content,
        "canonical_type": canonical_type,
        "canonical_number": canonical_number,
        "canonical_type_plural": type_plural,
        "type_param_key": type_param_key,
        "working_item": working_item,
        "source_doc_label": source_doc_label,
        "source_doc_file": source_doc_file,
        "summary": summary.strip() if isinstance(summary, str) else "",
        "text_excerpt": text_excerpt,
        "found_label": f"{canonical_type.capitalize()} {canonical_number}",
    }


def _resolve_target(
    target_type: str,
    primary_parsed_content: Optional[Dict[str, Any]],
    external_documents: Optional[Dict[str, Any]],
    *,
    number: Optional[str] = None,
    first: Optional[str] = None,
    last: Optional[str] = None,
    text_excerpt_max_chars: int = 6000,
    primary_doc_label: Optional[str] = None,
    range_cap: int = 15,
) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """Resolve a section-request target — single or range — to canonical unit(s).

    Shape selection:
      - `number` alone     → single-target resolution
      - `first` and `last` → range resolution via expand_element_range
      - neither            → ([], "missing_inputs")

    Single-target resolution: literal lookup, then max-match fallback, in primary
    first then each external doc.

    Range resolution: both endpoints must resolve in the SAME document (primary
    preferred, then each external). The expanded list is capped at range_cap; if
    exceeded, the first endpoint is returned with error_kind "range_cap_exceeded"
    so the caller can log truncation.

    Returns (targets, error_kind). On success error_kind is None.
    """
    if not primary_parsed_content:
        return [], "type_not_resolvable"

    is_range = bool(first) and bool(last)
    is_single = bool(number)
    if not is_range and not is_single:
        return [], "missing_inputs"

    # target_type must be a real operational type in primary; if not, no resolution
    # path can work (and the enqueue would skip).
    if _find_type_in_parameters(primary_parsed_content, target_type) is None:
        return [], "type_not_resolvable"

    if is_single:
        resolved = _lookup_with_max_match(primary_parsed_content, target_type, number)
        used_content = primary_parsed_content
        source_label = primary_doc_label
        source_file = None
        if resolved is None and external_documents:
            for ext_file, ext_doc in external_documents.items():
                ext_resolved = _lookup_with_max_match(ext_doc, target_type, number)
                if ext_resolved is not None:
                    resolved = ext_resolved
                    used_content = ext_doc
                    source_label = os.path.basename(ext_file).replace("_processed.json", "")
                    source_file = ext_file
                    break
        if resolved is None:
            return [], "not_present"
        ct, cn, wi = resolved
        return ([
            _build_target_info(
                used_content, ct, cn, wi,
                source_label, source_file, text_excerpt_max_chars,
            )
        ], None)

    # Range: both endpoints must resolve in the same document.
    first_in_primary = _lookup_with_max_match(primary_parsed_content, target_type, first)
    last_in_primary = _lookup_with_max_match(primary_parsed_content, target_type, last)
    used_content = primary_parsed_content
    source_label = primary_doc_label
    source_file = None
    first_resolved: Optional[Tuple[str, str, Dict[str, Any]]] = None
    last_resolved: Optional[Tuple[str, str, Dict[str, Any]]] = None
    if first_in_primary is not None and last_in_primary is not None:
        first_resolved, last_resolved = first_in_primary, last_in_primary
    elif external_documents:
        for ext_file, ext_doc in external_documents.items():
            ef = _lookup_with_max_match(ext_doc, target_type, first)
            el = _lookup_with_max_match(ext_doc, target_type, last)
            if ef is not None and el is not None:
                first_resolved, last_resolved = ef, el
                used_content = ext_doc
                source_label = os.path.basename(ext_file).replace("_processed.json", "")
                source_file = ext_file
                break
    if first_resolved is None or last_resolved is None:
        return [], "not_present"

    canonical_first_type, canonical_first_num, first_wi = first_resolved
    _, canonical_last_num, _ = last_resolved

    # Use first endpoint's canonical type as the range type; canonical_org_types
    # gives us the matching plural for lookup_item calls below.
    range_type = canonical_first_type
    _, range_plural = canonical_org_types(range_type)
    expanded = expand_element_range(
        used_content, range_type, canonical_first_num, canonical_last_num
    )
    if not expanded:
        return [], "not_present"
    if len(expanded) > range_cap:
        # Truncation: keep first endpoint only so the gate can still judge it.
        return ([
            _build_target_info(
                used_content, canonical_first_type, canonical_first_num, first_wi,
                source_label, source_file, text_excerpt_max_chars,
            )
        ], "range_cap_exceeded")

    targets: List[Dict[str, Any]] = []
    missing: List[str] = []
    for num in expanded:
        wi = lookup_item(used_content, range_plural, num)
        if wi is None:
            # expand_element_range walks the same parsed_content as lookup_item,
            # so a miss here means the two views of the document disagree about
            # which units exist. Project policy is to fail loudly on structural
            # inconsistencies rather than silently shorten the result list.
            missing.append(num)
            continue
        targets.append(_build_target_info(
            used_content, range_type, num, wi,
            source_label, source_file, text_excerpt_max_chars,
        ))
    if missing:
        raise InputError(
            "Range expansion structural inconsistency: expand_element_range "
            f"yielded {len(expanded)} {range_plural} numbers but "
            f"{len(missing)} could not be looked up "
            f"({', '.join(missing[:5])}{'...' if len(missing) > 5 else ''}). "
            "expand_element_range and lookup_item should agree on the set of "
            "units present in the document."
        )
    if not targets:
        # Reached only if expanded was empty after filtering — defensive.
        return [], "not_present"
    return targets, None


def _gate_section_request(
    scratch_manager: "ScratchDocumentManager",
    client,
    config: Optional[Dict],
    logfile: str,
    primary_question: str,
    target_info: Dict[str, Any],
    reason: str,
    proposed_by_unit: str,
) -> Dict[str, Any]:
    """
    Materiality gate for a pre-resolved section/unit request (task
    qa.gate.request_materiality).

    Takes a target_info dict (produced by _resolve_target) containing the
    target's summary + text excerpt and decides against a high materiality bar:
    accept only if the section's actual content is reasonably likely to change
    the final answer.

    Returns: {"verdict": "accept"} or {"verdict": "reject", "reason": "..."}.
    """
    summary_block = target_info["summary"] or "(no summary available)"
    excerpt_block = target_info["text_excerpt"] or "(no text available)"
    external_clause = ""
    if target_info["source_doc_label"]:
        external_clause = f" (in external document {target_info['source_doc_label']})"

    prompt = (
        f"Primary question: {primary_question}\n\n"
        f"An analyst working on {proposed_by_unit} is requesting that "
        f"{target_info['found_label']}{external_clause} be added to the analysis pool.\n\n"
        f"Analyst's stated justification: {reason or '(no reason provided)'}\n\n"
        f"Summary of {target_info['found_label']}:\n{summary_block}\n\n"
        f"Text of {target_info['found_label']}:\n{excerpt_block}\n\n"
        "Decide whether to add this section to the analysis pool. The bar is HIGH: accept "
        "only if, after reviewing the section's actual content above, the section contains "
        "specific material reasonably likely to CHANGE the final answer to the primary "
        "question — not merely that it is topically related to the same legal area.\n\n"
        "REJECT if any of the following apply:\n"
        "- The section discusses the same general topic but does not address the specific "
        "matter the primary question asks about.\n"
        "- The analyst's justification depends on an inferential chain not supported by the "
        "section's actual content.\n"
        "- The section is procedural, structural, or definitional in a way that would not "
        "alter the answer.\n"
        "- The section's substantive content is already captured by existing material "
        "available to the analysis.\n\n"
        "ACCEPT only when you can name a specific rule, exception, definition, or condition "
        "in the section's actual text that bears on the primary question. Provide that "
        "anchor in the rationale field.\n\n"
        "Respond with ONLY JSON in one of these forms:\n"
        '{"verdict": "accept", "rationale": "Brief, content-anchored reason."}\n'
        '{"verdict": "reject", "reason": "Brief reason for rejection."}\n'
    )

    try:
        result = query_json(client, [], prompt, logfile, max_tokens=400,
                            config=config, task_name="qa.gate.request_materiality")
    except Exception:
        # Fail-conservative: reject on error to protect against runaway re-analysis.
        return {"verdict": "reject", "reason": "gate call failed (fail-conservative)"}

    if not isinstance(result, dict):
        return {"verdict": "reject", "reason": "gate returned malformed response"}

    verdict = str(result.get("verdict", "reject")).strip().lower()
    if verdict not in ("accept", "reject"):
        verdict = "reject"
    out: Dict[str, Any] = {"verdict": verdict}
    if verdict == "reject":
        reason = str(result.get("reason", "")).strip()
        if reason:
            out["reason"] = reason
    return out


def _is_near_duplicate_of_primary(proposed: str, primary: str) -> bool:
    """Heuristic: is the proposed question essentially the primary question?"""
    p = proposed.lower().strip().rstrip("?").strip()
    q = primary.lower().strip().rstrip("?").strip()
    if p in q or q in p:
        return True
    if len(p) > 20 and len(q) > 20:
        words_p = set(p.split())
        words_q = set(q.split())
        overlap = len(words_p & words_q) / max(len(words_p), len(words_q), 1)
        return overlap > 0.75
    return False


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
        Return a mapping (type_lower, number) -> detail_level for requested summaries.

        In WS8, requests are consumed inline and never stored in scratch, so this
        always returns an empty dict.  The method is retained to avoid breaking
        callers inside build_cache_components_for_item.
        """
        return {}

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
        config: Dict[str, Any] = None,
    ):
        self.client = client
        self.logfile = logfile
        self.context_builder = context_builder
        self.scratch_manager = scratch_manager
        self.question_object = question_object
        self.parsed_content = parsed_content
        self.scratch_snapshot = scratch_snapshot
        self._config = config or {}

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

    # ------------------------------------------------------------------
    # WS8 Phase-specific prompt builders
    # ------------------------------------------------------------------

    def _build_phase1_prompt(
        self,
        unit_context: str,
        chunk_text_str: str,
        item_type_name: str,
        item_number: str,
        unit_title: str,
        chunk_idx: int,
        n_chunks: int,
        prior_facts: List[str],
        sub_question: Optional[str] = None,
        circleback_context: Optional[str] = None,
    ) -> Tuple[List[str], str]:
        """Cache + query prompt for Phase 1 (fact extraction).

        When sub_question is provided (Phase 3 calls), it replaces the main question.
        When circleback_context is provided, the analyst is told about responsive facts
        that were found for a question they proposed, so they can extract follow-up facts.
        The analyst never sees the shared fact pool or other analysts' outputs.
        """
        item_label = f"{item_type_name.capitalize()} {item_number}"
        if unit_title:
            item_label += f": {unit_title}"

        question_text = sub_question if sub_question else self.context_builder.question_text

        if circleback_context:
            role = (
                "You are a legal analyst performing fact extraction from a portion of a legal document.\n\n"
                "A sub-question you previously proposed has been answered by other sections. "
                "The answered question and the facts found in response are shown below. "
                "Your task: given this new information, extract any additional facts from YOUR PORTION "
                "that are now relevant to the primary question. Focus on facts your unit contributes "
                "that connect to or build on the answered sub-question.\n\n"
            )
        else:
            role = (
                "You are a legal analyst performing fact extraction from a portion of a legal document.\n\n"
                "Your sole task is to identify facts from YOUR PORTION that directly help answer "
                "the question below. Include specific rules, thresholds, definitions, "
                "requirements, and exceptions. Omit tangential information.\n\n"
            )
        question_block = f"Primary question:\n{question_text}\n\n"
        schema_block = (
            'Respond with a JSON object:\n'
            '{"facts": ["...", "..."], "section_requests": [...]}\n\n'
            "Each fact must be:\n"
            "- A complete, self-contained statement\n"
            "- Directly relevant to the question\n"
            "- Grounded solely in YOUR PORTION below — report what the text states, "
            "not your reasoning about why it may be relevant\n"
            "- Specific: include actual thresholds, numbers, or rule text when present\n"
            "Do not add parenthetical commentary, bridge reasoning, or characterizations "
            "asserting relevance that is not explicit in the source text. If a provision's "
            "connection to the question is not stated in the text itself, omit it rather "
            "than constructing the link.\n\n"
            "section_requests: specific units whose actual content is needed to answer the question "
            "but is not present in your portion. Only request a unit when knowing its content "
            "is necessary — not merely because your portion mentions it. Each entry takes ONE of "
            "two shapes:\n"
            '  Single section: {"type": "Section", "number": "<id>", "reason": "..."}\n'
            '  Explicit range: {"type": "Section", "first": "<id>", "last": "<id>", "reason": "..."}\n'
            "Rules for section_requests:\n"
            "- `number` must be the bare section identifier (e.g., \"11121\"), not decorated with "
            "paragraph subscripts, parentheticals, or commentary.\n"
            "- Use the range form ONLY when the source text invokes a range with two explicit "
            "endpoints (e.g., \"Sections 11122 to 11124, inclusive\", \"Sections 2258A through 2258E\", "
            "\"Sections 84504-84504.3\"). The resolver will expand the range to all sections within it.\n"
            "- When the source text invokes such a range, emit ONE range entry — do NOT enumerate "
            "the intermediate sections yourself. Example: source text \"sections 4201 through 4204\" "
            "→ {\"type\": \"Section\", \"first\": \"4201\", \"last\": \"4204\", \"reason\": \"...\"} "
            "(one entry, not four).\n"
            "- If the source text enumerates multiple sections explicitly (e.g., \"Sections X and Y\", "
            "\"Sections X, Y, and Z\"), emit ONE request entry per section using the single-section "
            "form. Do not combine them in `number`.\n"
            "- Do NOT use the range form for \"et seq.\" — that is a citation device for naming "
            "an Act, not a content request. If you believe the starting section is relevant, emit "
            "a single-section request for it.\n"
            "- Include a concrete reason tied to the primary question.\n"
            'If you find no relevant facts and have no requests, respond: {"facts": [], "section_requests": []}\n\n'
        )

        cache_parts = [role, question_block, schema_block]

        prompt: List[str] = []

        if unit_context:
            prompt.append(unit_context)

        if circleback_context:
            prompt.append(circleback_context)
            prompt.append("\n")

        if n_chunks > 1:
            prompt.append(
                f"NOTE: {item_label} is a long unit divided into {n_chunks} portions. "
                f"You are examining portion {chunk_idx + 1} of {n_chunks}.\n\n"
            )

        if prior_facts:
            prompt.append(
                "Facts already identified in earlier portions of this unit "
                "(do NOT re-extract):\n"
            )
            for pf in prior_facts:
                prompt.append(f"- {pf}\n")
            prompt.append("\n")

        prompt.append("=" * 70 + "\n")
        prompt.append("YOUR PORTION TO ANALYZE\n")
        prompt.append("=" * 70 + "\n")
        prompt.append(f"{item_label}:\n\n")
        prompt.append(chunk_text_str.strip() + "\n")
        prompt.append("=" * 70 + "\n\n")
        prompt.append("Respond with the JSON object listing extracted facts.\n")

        return cache_parts, "".join(prompt)

    def _build_phase2_prompt(
        self,
        unit_context: str,
        item_type_name: str,
        item_number: str,
        unit_title: str,
        unit_summary: str,
    ) -> Tuple[List[str], str]:
        """Cache + query prompt for Phase 2 (question/request generation).

        The analyst sees only their unit text and the primary question — never the
        shared fact pool or existing question list. Deduplication is the question
        gatekeeper's responsibility, not the analyst's.
        """
        unit_label = f"{item_type_name.capitalize()} {item_number}"
        if unit_title:
            unit_label += f": {unit_title}"

        role = (
            "You are a legal analyst identifying research gaps after reviewing a portion of a legal document.\n\n"
            "Based solely on the content of YOUR PORTION, you may propose:\n"
            "1. Substantive sub-questions about what the document requires, permits, or says — "
            "questions whose answers would come from other sections you have not seen.\n"
            "2. Section requests: specific units whose actual content is needed to answer "
            "the primary question but is not present in your portion.\n\n"
        )
        question_block = (
            f"Primary Question:\n{self.context_builder.question_text}\n\n"
        )
        schema_block = (
            'Respond with a JSON object:\n'
            '{"questions": [{"text": "...", "rationale": "..."}], "requests": [...]}\n\n'
            "Rules for sub-questions:\n"
            "- Ask about substance, not location. Frame questions as 'What does the document require for X?' "
            "or 'What restrictions apply to Y?' — NOT 'Which section covers X?' or 'Is there a section that...'.\n"
            "- Each sub-question must be something another section is specifically likely to answer.\n"
            "- Do NOT propose questions that restate or paraphrase the primary question.\n"
            "- Do NOT ask questions already answered by your own unit's content.\n"
            "- rationale: explain in one sentence why the answer to this sub-question would help answer "
            "the primary question — what the implication of a positive or negative answer would be.\n"
            "Rules for requests (section requests). Each entry takes ONE of two shapes:\n"
            '  Single section: {"type": "Section", "number": "<id>", "reason": "..."}\n'
            '  Explicit range: {"type": "Section", "first": "<id>", "last": "<id>", "reason": "..."}\n'
            "- `number` must be the bare section identifier (e.g., \"11121\"), not decorated with "
            "paragraph subscripts, parentheticals, or commentary.\n"
            "- Use the range form ONLY when the source text invokes a range with two explicit "
            "endpoints (e.g., \"Sections 11122 to 11124, inclusive\", \"Sections 2258A through 2258E\"). "
            "The resolver will expand it.\n"
            "- When the source text invokes such a range, emit ONE range entry — do NOT enumerate "
            "the intermediate sections yourself. Example: source text \"sections 4201 through 4204\" "
            "→ {\"type\": \"Section\", \"first\": \"4201\", \"last\": \"4204\", \"reason\": \"...\"} "
            "(one entry, not four).\n"
            "- For explicit enumerations like \"Sections X and Y\", emit ONE request per section using "
            "the single form. Do not combine them in `number`.\n"
            "- Do NOT use the range form for \"et seq.\" — that names an Act, not a content request.\n"
            "- Include a concrete reason tied to the primary question.\n"
            'If nothing to propose: {"questions": [], "requests": []}\n\n'
        )

        cache_parts = [role, question_block, schema_block]

        prompt: List[str] = []

        if unit_context:
            prompt.append(unit_context)

        prompt.append(f"Your unit: {unit_label}\n")
        if unit_summary:
            prompt.append(f"Summary: {unit_summary}\n")
        prompt.append(
            "\nBased on the content of your portion, what sub-questions or section requests "
            "would help answer the primary question?\n"
        )

        return cache_parts, "".join(prompt)

    # ------------------------------------------------------------------
    # WS8 Phase-specific analyze methods
    # ------------------------------------------------------------------

    def analyze_phase1(
        self,
        working_item: Dict[str, Any],
        item_type_name: str,
        item_number: str,
        score_level: int = 2,
        sub_question: Optional[str] = None,
        circleback_context: Optional[str] = None,
    ) -> Tuple[List[str], List[Dict[str, Any]]]:
        """
        Phase 1 (and Phase 3): extract proposed facts and section requests from this unit's chunks.

        When sub_question is provided, it replaces the main question in the prompt —
        this is how Phase 3 (targeted sub-question analysis) is implemented.
        When circleback_context is provided, the analyst is shown responsive facts for a
        question they proposed and asked to extract additional facts in light of them.
        Returns (proposed_facts, proposed_section_requests). Neither list is gated;
        the caller is responsible for gating before acting on results.
        """
        text = working_item.get("text", "")
        if not text:
            _params = self.parsed_content.get("document_information", {}).get("parameters", {})
            _is_data_table = any(
                p.get("name") == item_type_name and p.get("data_table") and p.get("is_sub_unit")
                for p in _params.values()
            )
            if _is_data_table:
                text = working_item.get("summary_1", "")

        if not text:
            return [], []

        breakpoints = working_item.get("breakpoints", [])
        chunks = list(chunk_text(text, breakpoints, preferred_length=15000))
        if not chunks:
            return [], []

        # Build unit context (definitions, summaries) — static per unit
        _, unit_context = self.context_builder.build_cache_components_for_item(
            working_item, item_type_name, item_number,
            self.scratch_manager.scratch,
            score_level=score_level,
        )

        # Metadata suffix for duplicate sections
        _params = self.parsed_content.get("document_information", {}).get("parameters", {})
        _item_type_names = None
        for _pk, _pd in _params.items():
            if isinstance(_pd, dict) and _pd.get("name") == item_type_name:
                _item_type_names = _pd.get("name_plural")
                break
        _content = self.parsed_content.get("content")
        metadata_suffix = build_metadata_suffix(item_number, working_item, _content, _item_type_names)

        all_proposed_facts: List[str] = []
        all_proposed_requests: List[Dict[str, Any]] = []
        prior_facts: List[str] = []
        unit_title = working_item.get("unit_title", "")

        for idx, chunk in enumerate(chunks):
            augmented = augment_chunk_with_metadata(chunk, metadata_suffix)
            cache_parts, query_prompt = self._build_phase1_prompt(
                unit_context,
                augmented,
                item_type_name,
                item_number,
                unit_title,
                chunk_idx=idx,
                n_chunks=len(chunks),
                prior_facts=prior_facts,
                sub_question=sub_question,
                circleback_context=circleback_context,
            )

            try:
                result_obj = query_json(
                    self.client, cache_parts, query_prompt, self.logfile,
                    max_tokens=8000, config=self._config,
                    task_name="qa.analysis.phase1",
                )
            except ModelError:
                continue

            if not isinstance(result_obj, dict):
                continue

            chunk_facts = [str(f).strip() for f in result_obj.get("facts", []) if f]
            all_proposed_facts.extend(chunk_facts)
            prior_facts.extend(chunk_facts)

            # Accept either single-section ({number}) or range ({first, last}) form.
            chunk_requests = [
                r for r in result_obj.get("section_requests", [])
                if isinstance(r, dict)
                and r.get("type")
                and (r.get("number") or (r.get("first") and r.get("last")))
            ]
            all_proposed_requests.extend(chunk_requests)

        return all_proposed_facts, all_proposed_requests

    def analyze_phase2(
        self,
        working_item: Dict[str, Any],
        item_type_name: str,
        item_number: str,
    ) -> Tuple[List[Tuple[str, str]], List[Dict[str, Any]]]:
        """
        Phase 2: propose questions and section requests for this unit.
        Returns (proposed_questions, proposed_requests) where each proposed question
        is a (text, rationale) tuple.

        The analyst sees only their unit and the primary question — not the fact pool
        or existing question list. Deduplication is the question gatekeeper's job.
        """
        _, unit_context = self.context_builder.build_cache_components_for_item(
            working_item, item_type_name, item_number,
            self.scratch_manager.scratch,
            score_level=2,
        )

        unit_summary = working_item.get("summary_1", "")
        unit_title = working_item.get("unit_title", "")

        cache_parts, query_prompt = self._build_phase2_prompt(
            unit_context, item_type_name, item_number, unit_title, unit_summary,
        )

        try:
            result_obj = query_json(
                self.client, cache_parts, query_prompt, self.logfile, max_tokens=4000,
                config=self._config, task_name="qa.analysis.phase2",
            )
        except ModelError:
            return [], []

        if not isinstance(result_obj, dict):
            return [], []

        raw_questions = result_obj.get("questions", [])
        questions: List[Tuple[str, str]] = []
        for q in raw_questions:
            if isinstance(q, dict):
                text = str(q.get("text", "")).strip()
                rationale = str(q.get("rationale", "")).strip()
            else:
                text = str(q).strip()
                rationale = ""
            if text:
                questions.append((text, rationale))

        # Phase 2 filter mirrors Phase 1: require `type` and either `number`
        # (single) or both `first` and `last` (range). Malformed entries are
        # dropped here rather than reaching _gate_and_enqueue_requests where
        # they would be silently skipped without a rejected_inquiry record.
        requests = [
            r for r in result_obj.get("requests", [])
            if isinstance(r, dict)
            and r.get("type")
            and (r.get("number") or (r.get("first") and r.get("last")))
        ]
        return questions, requests

    # ------------------------------------------------------------------
    # Retired: analyze_chunks (WS8 replacement — do not call)
    # ------------------------------------------------------------------

    def analyze_chunks(
        self,
        working_item,
        item_type_name,
        item_number,
        refine,
        score_level=2,
    ):
        """Retired (WS8). Use analyze_phase1/2/3 via make_chunk_analyzer() instead."""
        raise NotImplementedError("analyze_chunks is retired in WS8")


class ItemAnalyzer:
    """
    Build a ChunkAnalyzer for a single unit.

    analyze_item() is retired in WS8. Use make_chunk_analyzer() instead and call
    analyze_phase1 (Phase 1 and Phase 3 via sub_question param) or analyze_phase2.
    """

    def __init__(
        self,
        client,
        logfile: str,
        parsed_content: Dict[str, Any],
        question_object: Dict[str, Any],
        scratch_manager: ScratchDocumentManager,
        external_doc_label: Optional[str] = None,
        parent_parsed_content: Optional[Dict[str, Any]] = None,
    ):
        self.client = client
        self.logfile = logfile
        self.parsed_content = parsed_content
        self.question_object = question_object
        self.scratch_manager = scratch_manager
        self.external_doc_label = external_doc_label
        self.parent_parsed_content = parent_parsed_content

    def make_chunk_analyzer(self, question_text: str) -> "ChunkAnalyzer":
        """Return a ChunkAnalyzer instance wired to this unit's document and scratch."""
        context_builder = ContextBuilder(
            self.parsed_content, question_text,
            external_doc_label=self.external_doc_label,
            parent_parsed_content=self.parent_parsed_content,
        )
        return ChunkAnalyzer(
            self.client,
            self.logfile,
            context_builder,
            self.scratch_manager,
            self.question_object,
            self.parsed_content,
            {},  # scratch_snapshot unused in WS8 phase methods
            config=self._config,
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
        """Retired (WS8). Cross-doc units are added via gatekeeper-approved section requests."""
        raise NotImplementedError("_refresh_cross_document_units is retired in WS8")

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
            'iterative_analysis': 'qa.analysis.analyze_chunk',  # retired; kept for fallback
            'ws8_phase1': 'qa.analysis.phase1',
            'ws8_phase2': 'qa.analysis.phase2',
            'ws8_phase3': 'qa.analysis.answer_question',
            'ws8_gate': 'qa.gate.fact',
            'ws8_fact_close_match_gate': 'qa.gate.fact_close_match',
            'ws8_question_gate': 'qa.gate.question',
            'ws8_question_close_match_gate': 'qa.gate.question_close_match',
            'ws8_request_gate': 'qa.gate.request_materiality',
            'cleanup': 'qa.synthesis.cleanup_scratch',
            'final_answer': 'qa.synthesis.final_answer',
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
            'iterative_analysis': 'qa.analysis.analyze_chunk',  # retired; kept for fallback
            'ws8_phase1': 'qa.analysis.phase1',
            'ws8_phase2': 'qa.analysis.phase2',
            'ws8_phase3': 'qa.analysis.answer_question',
            'ws8_gate': 'qa.gate.fact',
            'ws8_fact_close_match_gate': 'qa.gate.fact_close_match',
            'ws8_question_gate': 'qa.gate.question',
            'ws8_question_close_match_gate': 'qa.gate.question_close_match',
            'ws8_request_gate': 'qa.gate.request_materiality',
            'cleanup': 'qa.synthesis.cleanup_scratch',
            'final_answer': 'qa.synthesis.final_answer',
        }
        return phase_to_task.get(phase, 'qa.relevance.score')

    # ------------------------------------------------------------------
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
            "[0] if the portion clearly addresses a different subject and is NOT relevant\n"
            "[1] if there is a LOW probability of relevance — any connection to the question "
            "is indirect and would require several inferential steps to establish\n"
            "[2] if the portion is LIKELY relevant — it plausibly addresses the question "
            "subject, though the connection may not be explicit in the summary\n"
            "[3] if the portion is CLEARLY IMPORTANT — the summary explicitly addresses "
            "the specific subject of the question, not merely a related domain\n\n"
            "Base your score on what the summary explicitly states. Do not assign a high "
            "score because you can imagine a possible connection; assign [3] only when "
            "the summary itself makes the relevance apparent.\n\n"
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
            "0 = NOT relevant — the summary clearly addresses a different subject\n"
            "1 = LOW probability — any connection to the question is indirect and "
            "would require several inferential steps to establish\n"
            "2 = LIKELY relevant — the summary plausibly addresses the question "
            "subject, though the connection may not be explicit\n"
            "3 = CLEARLY IMPORTANT — the summary explicitly addresses the specific "
            "subject of the question, not merely a related domain\n\n"
            "Base each score on what the summary explicitly states. Do not assign a "
            "high score because you can imagine a possible connection; assign 3 only "
            "when the summary itself makes the relevance apparent.\n\n"
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
            pct = int(scored_count * 100 / total_to_score) if total_to_score else 100
            print(f"\r    {scored_count}/{total_to_score} ({pct}%)", end="", flush=True)
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
        print()  # end the progress line

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

    # ------------------------------------------------------------------
    # WS8 orchestration helpers
    # ------------------------------------------------------------------

    def _get_active_unit_labels(self) -> set:
        """Return the set of labeled unit IDs across primary and cross-doc pools."""
        labels: set = set()
        for unit_info in self._build_active_pool():
            if unit_info["external_doc_label"]:
                sm = ScratchDocumentManager(
                    self.question_object,
                    source_doc_label=unit_info["external_doc_label"],
                )
                labels.add(sm._label(unit_info["unit_label"]))
            else:
                labels.add(unit_info["unit_label"])
        return labels

    def _build_active_pool(self) -> List[Dict[str, Any]]:
        """
        Build the list of all scoreable units for Phase 1/2/3.

        Each entry:
          unit_id, unit_label, labeled_unit_id, type_name, type_names, type_key,
          score, working_item, parsed_content, external_doc_label, parent_parsed_content
        """
        pool: List[Dict[str, Any]] = []
        param_pointer = self.parsed_content.get("document_information", {}).get("parameters", {})
        scores = self.question_object.get("scores", {})

        def _is_data_table_type(parsed_doc: Dict[str, Any], type_name: str) -> bool:
            params = parsed_doc.get("document_information", {}).get("parameters", {})
            return any(
                isinstance(p, dict)
                and p.get("name") == type_name
                and p.get("data_table")
                and p.get("is_sub_unit")
                for p in params.values()
            )

        for item_type, type_scores in scores.items():
            p = _resolve_param_key(param_pointer, item_type)
            if not p:
                continue
            if not (p.get("operational") == 1 and p.get("name") and p.get("name_plural")):
                continue
            item_type_name = p["name"]
            item_type_names = p["name_plural"]

            for item_num, score in type_scores.items():
                if score < 2:
                    continue
                working_item = lookup_item(self.parsed_content, item_type_names, item_num)
                if not working_item:
                    continue
                if not working_item.get("text") and not (
                    _is_data_table_type(self.parsed_content, item_type_name)
                    and working_item.get("summary_1")
                ):
                    continue
                pool.append({
                    "unit_id": item_num,
                    "unit_label": f"{item_type_name.capitalize()} {item_num}",
                    "type_name": item_type_name,
                    "type_names": item_type_names,
                    "type_key": str(item_type),
                    "score": score,
                    "working_item": working_item,
                    "parsed_content": self.parsed_content,
                    "external_doc_label": None,
                    "parent_parsed_content": getattr(self, "_parent_parsed_content", None),
                    "derivation_depth": 0,
                })

        # Cross-document units
        cds = self.question_object.get("cross_doc_scores", {})
        for ext_file, ext_scores in cds.items():
            ext_doc = self._load_external_document(ext_file)
            if not ext_doc:
                continue
            ext_label = os.path.basename(ext_file).replace("_processed.json", "")
            ext_params = ext_doc.get("document_information", {}).get("parameters", {})

            for type_name, type_scores_for_type in ext_scores.items():
                item_type_name = type_name
                item_type_names = None
                for _pk, _pd in ext_params.items():
                    if isinstance(_pd, dict) and _pd.get("name", "").lower() == type_name.lower():
                        item_type_name = _pd["name"]
                        item_type_names = _pd.get("name_plural", type_name + "s")
                        break
                if not item_type_names:
                    try:
                        _, item_type_names = canonical_org_types(type_name.lower())
                    except Exception:
                        item_type_names = type_name + "s"

                for item_num, score in type_scores_for_type.items():
                    if score < 2:
                        continue
                    working_item = lookup_item(ext_doc, item_type_names, item_num)
                    if not working_item:
                        continue
                    if not working_item.get("text") and not (
                        _is_data_table_type(ext_doc, item_type_name)
                        and working_item.get("summary_1")
                    ):
                        continue
                    pool.append({
                        "unit_id": item_num,
                        "unit_label": f"{item_type_name.capitalize()} {item_num}",
                        "type_name": item_type_name,
                        "type_names": item_type_names,
                        "type_key": item_type_name.lower(),
                        "score": score,
                        "working_item": working_item,
                        "parsed_content": ext_doc,
                        "external_doc_label": ext_label,
                        "parent_parsed_content": None,
                        "derivation_depth": 0,
                    })

        pool.sort(key=lambda u: -u["score"])
        return pool

    def _make_chunk_analyzer_for_unit(
        self,
        unit_info: Dict[str, Any],
        scratch_manager: ScratchDocumentManager,
        client,
    ) -> "ChunkAnalyzer":
        """Create a ChunkAnalyzer wired to the given unit's document and scratch."""
        context_builder = ContextBuilder(
            unit_info["parsed_content"],
            self.question_text,
            external_doc_label=unit_info["external_doc_label"],
            parent_parsed_content=unit_info["parent_parsed_content"],
        )
        return ChunkAnalyzer(
            client,
            self.logfile,
            context_builder,
            scratch_manager,
            self.question_object,
            unit_info["parsed_content"],
            {},
            config=self._config,
        )

    def _run_unit_phase1(
        self,
        unit_info: Dict[str, Any],
        scratch_manager: ScratchDocumentManager,
        phase_client,
        gate_client,
    ) -> int:
        """
        Phase 1 for one unit: extract, gate, and add facts.
        Returns count of facts accepted/merged.
        """
        unit_label = unit_info["unit_label"]
        item_type_name = unit_info["type_name"]
        item_num = unit_info["unit_id"]
        external_doc_label = unit_info["external_doc_label"]

        chunk_analyzer = self._make_chunk_analyzer_for_unit(unit_info, scratch_manager, phase_client)
        proposed_facts, _proposed_requests = chunk_analyzer.analyze_phase1(
            unit_info["working_item"],
            item_type_name,
            item_num,
            score_level=unit_info["score"],
        )

        if not proposed_facts:
            return 0

        # Source label is the unit label, possibly qualified by external doc
        if external_doc_label:
            sm_for_label = ScratchDocumentManager(
                self.question_object, source_doc_label=external_doc_label
            )
            source_label = sm_for_label._label(unit_label)
        else:
            source_label = unit_label

        accepted = 0
        for fact_content in proposed_facts:
            verdict = _gate_and_merge_fact(
                scratch_manager, gate_client, self._config, self.logfile,
                self.question_text, fact_content, source_label,
            )
            if verdict["verdict"] == "accept":
                scratch_manager.add_fact(fact_content, source_label)
                accepted += 1
            elif verdict["verdict"] == "merge" and verdict.get("merge_fact_id"):
                scratch_manager.merge_fact(
                    verdict["merge_fact_id"],
                    source_label,
                    updated_content=verdict.get("merged_content"),
                )
                accepted += 1

        return accepted

    def _process_ws8_section_requests(
        self,
        pending_requests: List[Dict[str, Any]],
    ) -> int:
        """
        OBSOLETE: dead code preserved temporarily for reference.

        This method belongs to the pre-queue-driven (round-based) Phase 2 path
        and is not called anywhere in the WS8 queue-driven flow. Section requests
        are now handled by `_gate_and_enqueue_requests` and `_enqueue_section_requests`
        (nested closures inside `run_analysis`), which go through `_resolve_target`
        (8.R9) for canonical resolution and range expansion. Do not call this method;
        do not extend it. Slated for removal.

        Process section requests collected during Phase 2.
        Adds validated units to scores for next round.
        Returns count of new units added.
        """
        if not pending_requests:
            return 0

        param_pointer = self.parsed_content.get("document_information", {}).get("parameters", {})
        scores = self.question_object.setdefault("scores", {})
        new_units = 0

        for req in pending_requests:
            target_type = req["target_type"].strip()
            target_number = req["target_number"].strip()
            if not target_type or not target_number:
                continue

            # Find parameter entry by name
            item_type_key = None
            item_type_names = None
            for pk, pd in param_pointer.items():
                if not isinstance(pd, dict):
                    continue
                if not (pd.get("operational") == 1 and pd.get("name")):
                    continue
                if pd["name"].lower() == target_type.lower():
                    item_type_key = pk
                    item_type_names = pd.get("name_plural", "")
                    break

            if not item_type_key or not item_type_names:
                continue

            # Verify unit exists in primary document
            if lookup_item(self.parsed_content, item_type_names, target_number) is None:
                # Try cross-doc external documents
                for ext_file, ext_doc in self._external_documents.items():
                    if lookup_item(ext_doc, item_type_names, target_number) is not None:
                        ttype = target_type.lower()
                        if self._add_cross_doc_unit(ext_file, ttype, target_number):
                            new_units += 1
                            ext_name = os.path.basename(ext_file).replace("_processed.json", "")
                            print(f"  Cross-doc (request): added {target_type} {target_number}"
                                  f" from {ext_name}")
                        break
                continue

            # Add to primary-doc scores if not already present at score 2+
            type_scores = scores.setdefault(str(item_type_key), {})
            if target_number not in type_scores or type_scores[target_number] < 2:
                type_scores[target_number] = 2
                new_units += 1
                print(f"  Section request: added {target_type} {target_number} (score 2)")

        if new_units:
            self._save_question_object()
        return new_units

    def _close_questions(
        self,
        scratch_manager: ScratchDocumentManager,
        active_unit_labels: set,
        unit_queue_empty: bool = True,
    ) -> int:
        """
        Close questions that are exhausted (all active units have been asked and
        unit queue is empty). A question is 'answered' if any facts were accepted
        in response to it; otherwise 'unanswerable'. No per-question synthesis.

        Returns count of questions newly closed.
        """
        closed = 0
        for q in scratch_manager.get_open_questions():
            if not unit_queue_empty:
                continue
            seen_by = set(q.get("seen_by", []))
            if not active_unit_labels.issubset(seen_by):
                continue
            has_facts = bool(q.get("supporting_fact_ids"))
            q["status"] = "answered" if has_facts else "unanswerable"
            closed += 1
        return closed

    def run_analysis(self) -> None:
        """
        Queue-driven WS8 analysis (tasks 8.R1–8.R5).

        Replaces the old round-based run_round / run_to_stability loop.

        State:
          unit_queue   — FIFO of unit_info dicts pending Phase 1+2
          active_units — ordered list of unit_info dicts that completed Phase 1
          question_queue — FIFO of question IDs pending Phase 3 against already-active units

        Each unit is processed exactly once through Phase 1+2.  After Phase 1+2, the
        new unit is immediately asked about every open question (Phase 3 inline).
        When the unit queue drains, the question queue is drained: each queued question
        is asked of every active unit that has not yet seen it.  A question closes
        (answered / unanswerable) when all active units have been asked and the unit
        queue is empty.
        """
        from collections import deque

        scratch_manager = ScratchDocumentManager(self.question_object)
        self._initialize_cross_document_units()

        phase1_client = self._get_client_for_phase("ws8_phase1")
        phase2_client = self._get_client_for_phase("ws8_phase2")
        fact_gate_client = self._get_client_for_phase("ws8_gate")
        fact_close_match_client = self._get_client_for_phase("ws8_fact_close_match_gate")
        question_gate_client = self._get_client_for_phase("ws8_question_gate")
        question_close_match_client = self._get_client_for_phase("ws8_question_close_match_gate")
        request_gate_client = self._get_client_for_phase("ws8_request_gate")

        # --- State ---
        initial_pool = self._build_active_pool()
        unit_queue: deque = deque(initial_pool)
        active_units: List[Dict[str, Any]] = []
        active_unit_labels: set = set()
        question_queue: deque = deque()
        circleback_queue: deque = deque()  # (unit_info, question_dict) pairs

        print(f"\n{'='*70}")
        print("ITERATIVE ANALYSIS (WS8 Queue-Driven)")
        print(f"{'='*70}")
        print(f"Initial unit queue: {len(unit_queue)} unit(s)")

        # --- Helper: resolve a unit's display label ---
        def _unit_label(unit_info: Dict[str, Any]) -> str:
            if unit_info["external_doc_label"]:
                sm_tmp = ScratchDocumentManager(
                    self.question_object,
                    source_doc_label=unit_info["external_doc_label"],
                )
                return sm_tmp._label(unit_info["unit_label"])
            return unit_info["unit_label"]

        # --- Helper: gate and add facts, return list of contributed fact IDs ---
        # Accept → new fact ID; merge → existing fact ID. Empty list if nothing accepted.
        # question_context: sub-question or circle-back context under which facts were extracted;
        # passed to the gate so facts can be evaluated against the specific question that
        # prompted their extraction rather than always against the primary question alone.
        # question_rationale: when question_context is a real sub-question (not a circle-back),
        # this is the proposer's stated reason the sub-question's answer would matter to the
        # primary question. The gate uses it as a bridge: facts must help answer the sub-question
        # AND, via the rationale, plausibly affect the primary answer.
        def _gate_facts(
            proposed: List[str],
            source_label: str,
            question_context: Optional[str] = None,
            question_rationale: Optional[str] = None,
        ) -> List[str]:
            contributed: List[str] = []
            for fact_content in proposed:
                verdict = _gate_and_merge_fact(
                    scratch_manager, fact_gate_client, self._config, self.logfile,
                    self.question_text, fact_content, source_label,
                    question_context=question_context,
                    close_match_client=fact_close_match_client,
                    question_rationale=question_rationale,
                )
                if verdict["verdict"] == "accept":
                    fact_id = scratch_manager.add_fact(fact_content, source_label)
                    contributed.append(fact_id)
                elif verdict["verdict"] == "merge" and verdict.get("merge_fact_id"):
                    scratch_manager.merge_fact(
                        verdict["merge_fact_id"],
                        source_label,
                        updated_content=verdict.get("merged_content"),
                    )
                    contributed.append(verdict["merge_fact_id"])
            return contributed

        # --- Helper: add approved section requests to unit_queue ---
        def _enqueue_section_requests(pending_requests: List[Dict[str, Any]]) -> int:
            if not pending_requests:
                return 0

            param_pointer = self.parsed_content.get("document_information", {}).get("parameters", {})
            scores = self.question_object.setdefault("scores", {})
            new_count = 0

            for req in pending_requests:
                target_type = req["target_type"].strip()
                target_number = req["target_number"].strip()
                if not target_type or not target_number:
                    continue
                child_depth = int(req.get("source_depth", 0)) + 1

                # 8.R9 short-circuit: if the request carries a pre-resolved
                # target_info (produced upstream by _resolve_target), use it
                # directly. target_type/target_number are already canonical and
                # the working_item is already known, so we skip the parameter
                # walk and the literal lookup_item.
                resolved_target = req.get("resolved_target")
                if resolved_target is not None:
                    item_type_name_found = resolved_target["canonical_type"]
                    item_type_names = resolved_target["canonical_type_plural"]
                    item_type_key = resolved_target.get("type_param_key")
                    working_item = resolved_target["working_item"]
                    rt_source_file = resolved_target.get("source_doc_file")
                    rt_source_label = resolved_target.get("source_doc_label")
                    rt_parsed_content = resolved_target.get("parsed_content")

                    if rt_source_file is not None:
                        # Cross-doc target
                        if self._add_cross_doc_unit(
                            rt_source_file, target_type.lower(), target_number
                        ):
                            ext_label = rt_source_label or os.path.basename(
                                rt_source_file
                            ).replace("_processed.json", "")
                            candidate_label = (
                                f"{item_type_name_found.capitalize()} {target_number}"
                            )
                            full_label_ext = ScratchDocumentManager(
                                self.question_object, source_doc_label=ext_label
                            )._label(candidate_label)
                            if (
                                full_label_ext not in active_unit_labels
                                and not any(
                                    _unit_label(u) == full_label_ext for u in unit_queue
                                )
                            ):
                                new_ui = {
                                    "unit_id": target_number,
                                    "unit_label": candidate_label,
                                    "type_name": item_type_name_found,
                                    "type_names": item_type_names,
                                    "type_key": target_type.lower(),
                                    "score": 2,
                                    "working_item": working_item,
                                    "parsed_content": rt_parsed_content,
                                    "external_doc_label": ext_label,
                                    "parent_parsed_content": None,
                                    "derivation_depth": child_depth,
                                }
                                unit_queue.append(new_ui)
                                new_count += 1
                                print(
                                    f"  Section request (cross-doc): queued "
                                    f"{item_type_name_found} {target_number} from {ext_label}"
                                )
                        continue

                    # Primary-doc target
                    candidate_label = f"{item_type_name_found.capitalize()} {target_number}"
                    if candidate_label in active_unit_labels:
                        continue
                    if any(
                        u["unit_label"] == candidate_label and u["external_doc_label"] is None
                        for u in unit_queue
                    ):
                        continue
                    type_scores = scores.setdefault(str(item_type_key), {})
                    if target_number not in type_scores or type_scores[target_number] < 2:
                        type_scores[target_number] = 2
                    new_ui = {
                        "unit_id": target_number,
                        "unit_label": candidate_label,
                        "type_name": item_type_name_found,
                        "type_names": item_type_names,
                        "type_key": str(item_type_key) if item_type_key is not None else target_type.lower(),
                        "score": 2,
                        "working_item": working_item,
                        "parsed_content": self.parsed_content,
                        "external_doc_label": None,
                        "parent_parsed_content": getattr(self, "_parent_parsed_content", None),
                        "derivation_depth": child_depth,
                    }
                    unit_queue.append(new_ui)
                    new_count += 1
                    print(f"  Section request: queued {item_type_name_found} {target_number}")
                    continue

                # Legacy path (no resolved_target attached) — preserved for any
                # caller that doesn't go through _gate_and_enqueue_requests.
                item_type_key = None
                item_type_name_found = None
                item_type_names = None
                for pk, pd in param_pointer.items():
                    if not isinstance(pd, dict):
                        continue
                    if not (pd.get("operational") == 1 and pd.get("name")):
                        continue
                    if pd["name"].lower() == target_type.lower():
                        item_type_key = pk
                        item_type_name_found = pd["name"]
                        item_type_names = pd.get("name_plural", "")
                        break

                if not item_type_key or not item_type_names:
                    continue

                working_item = lookup_item(self.parsed_content, item_type_names, target_number)
                if working_item is None:
                    # Try cross-doc
                    for ext_file, ext_doc in self._external_documents.items():
                        if lookup_item(ext_doc, item_type_names, target_number) is not None:
                            if self._add_cross_doc_unit(ext_file, target_type.lower(), target_number):
                                # Rebuild cross-doc unit_info and enqueue
                                ext_label = os.path.basename(ext_file).replace("_processed.json", "")
                                ext_params = ext_doc.get("document_information", {}).get("parameters", {})
                                for _pk, _pd in ext_params.items():
                                    if isinstance(_pd, dict) and _pd.get("name", "").lower() == target_type.lower():
                                        item_type_names = _pd.get("name_plural", target_type + "s")
                                        break
                                wi = lookup_item(ext_doc, item_type_names, target_number)
                                if wi:
                                    candidate_label = f"{target_type.capitalize()} {target_number}"
                                    full_label_ext = ScratchDocumentManager(
                                        self.question_object, source_doc_label=ext_label
                                    )._label(candidate_label)
                                    if full_label_ext not in active_unit_labels and not any(
                                        _unit_label(u) == full_label_ext for u in unit_queue
                                    ):
                                        new_ui = {
                                            "unit_id": target_number,
                                            "unit_label": candidate_label,
                                            "type_name": target_type,
                                            "type_names": item_type_names,
                                            "type_key": target_type.lower(),
                                            "score": 2,
                                            "working_item": wi,
                                            "parsed_content": ext_doc,
                                            "external_doc_label": ext_label,
                                            "parent_parsed_content": None,
                                            "derivation_depth": child_depth,
                                        }
                                        unit_queue.append(new_ui)
                                        new_count += 1
                                        print(f"  Section request (cross-doc): queued {target_type} {target_number} from {ext_label}")
                            break
                    continue

                # Check not already active or already queued
                candidate_label = f"{item_type_name_found.capitalize()} {target_number}"
                if candidate_label in active_unit_labels:
                    continue
                if any(u["unit_label"] == candidate_label and u["external_doc_label"] is None
                       for u in unit_queue):
                    continue

                # Update scores so the unit is visible in future _build_active_pool calls
                type_scores = scores.setdefault(str(item_type_key), {})
                if target_number not in type_scores or type_scores[target_number] < 2:
                    type_scores[target_number] = 2

                new_ui = {
                    "unit_id": target_number,
                    "unit_label": candidate_label,
                    "type_name": item_type_name_found,
                    "type_names": item_type_names,
                    "type_key": str(item_type_key),
                    "score": 2,
                    "working_item": working_item,
                    "parsed_content": self.parsed_content,
                    "external_doc_label": None,
                    "parent_parsed_content": getattr(self, "_parent_parsed_content", None),
                    "derivation_depth": child_depth,
                }
                unit_queue.append(new_ui)
                new_count += 1
                print(f"  Section request: queued {target_type} {target_number}")

            if new_count:
                self._save_question_object()
            return new_count

        # --- Helper: gate proposed section requests and enqueue approved ones ---
        def _gate_and_enqueue_requests(
            proposed_requests: List[Dict[str, Any]],
            source_label: str,
            source_parsed_content: Dict[str, Any],
            source_depth: int = 0,
        ) -> None:
            """Resolve each proposed request (single or range), gate per resolved
            target, and forward accepted ones to _enqueue_section_requests.

            Accepts two analyst output shapes:
              - {"type", "number", "reason"}              (single section)
              - {"type", "first", "last", "reason"}       (explicit range)

            Single targets pass through one gate call. Range targets are expanded
            via _resolve_target's expand_element_range branch (cap 15); each
            expanded target gets its own gate call carrying the same analyst
            reason. Canonical numbers from resolution — not the analyst's raw
            surface form — are what flow into pending and the rejected_inquiries
            appendix.

            Cross-doc resolution order limitation: resolution searches primary
            first, then externals. When the requester is on an external unit and
            the target identifier collides between primary and the requester's
            own doc, primary wins. This matches the legacy enqueue order so the
            8.R9 change is no worse than before, but a future cross-doc-aware
            fix should search the requester's own document (source_parsed_content)
            first.
            """
            from utils.config import get_request_gate_text_excerpt_max_chars
            text_excerpt_max_chars = get_request_gate_text_excerpt_max_chars(self._config)
            pending = []
            for req in proposed_requests:
                target_type = str(req.get("type", "")).strip()
                if not target_type:
                    continue
                reason = str(req.get("reason", "")).strip()
                number = str(req.get("number", "")).strip() or None
                first = str(req.get("first", "")).strip() or None
                last = str(req.get("last", "")).strip() or None

                if first and last:
                    request_text = f"{target_type} {first}-{last}"
                elif number:
                    request_text = f"{target_type} {number}"
                else:
                    # Malformed: analyst gave us neither shape. Skip silently —
                    # nothing to gate or enqueue.
                    continue

                targets, err = _resolve_target(
                    target_type,
                    primary_parsed_content=self.parsed_content,
                    external_documents=self._external_documents,
                    number=number, first=first, last=last,
                    text_excerpt_max_chars=text_excerpt_max_chars,
                    primary_doc_label=None,
                )

                if err == "type_not_resolvable":
                    scratch_manager.add_rejected_inquiry(
                        kind="section_request",
                        text=request_text,
                        source_unit=source_label,
                        analyst_reason=reason,
                        gate_reason=(
                            f"target type '{target_type}' is not an operational "
                            f"type in this document"
                        ),
                        depth=source_depth,
                    )
                    continue
                if err == "missing_inputs":
                    # Same shape as the silent-skip above; defensive.
                    continue
                if not targets:
                    # err is "not_present" (or unexpected): record sharpened reason.
                    scratch_manager.add_rejected_inquiry(
                        kind="section_request",
                        text=request_text,
                        source_unit=source_label,
                        analyst_reason=reason,
                        gate_reason=(
                            "target not present in this document or any loaded "
                            "external document"
                        ),
                        depth=source_depth,
                    )
                    continue

                if err == "range_cap_exceeded":
                    # Truncation note. The first endpoint is still in `targets`
                    # below and will be gated normally; the diagnostic entry
                    # records only the SECTIONS BEYOND the first endpoint that
                    # were NOT pursued, so the "Lines of inquiry not pursued"
                    # appendix doesn't mis-attribute the first endpoint's
                    # outcome to this truncation.
                    first_target = targets[0]
                    first_label = first_target["found_label"]
                    scratch_manager.add_rejected_inquiry(
                        kind="section_request",
                        text=f"{request_text} (range beyond {first_label} not pursued)",
                        source_unit=source_label,
                        analyst_reason=reason,
                        gate_reason=(
                            f"range exceeded cap; {first_label} was retained for "
                            f"materiality judgment but the remainder of the range "
                            f"was not analyzed"
                        ),
                        depth=source_depth,
                    )

                # Per-target materiality gate. Each expanded target gets its own
                # decision carrying the same analyst reason — the analyst was
                # talking about the whole range when it emitted the entry.
                for target_info in targets:
                    canonical_type = target_info["canonical_type"]
                    canonical_number = target_info["canonical_number"]
                    verdict = _gate_section_request(
                        scratch_manager, request_gate_client, self._config, self.logfile,
                        self.question_text, target_info, reason, source_label,
                    )
                    if verdict["verdict"] == "accept":
                        pending.append({
                            "target_type": canonical_type,
                            "target_number": canonical_number,
                            "reason": reason,
                            "source_unit": source_label,
                            "source_parsed_content": source_parsed_content,
                            "source_depth": source_depth,
                            "resolved_target": target_info,
                        })
                    else:
                        scratch_manager.add_rejected_inquiry(
                            kind="section_request",
                            text=f"{canonical_type} {canonical_number}",
                            source_unit=source_label,
                            analyst_reason=reason,
                            gate_reason=verdict.get("reason", ""),
                            depth=source_depth,
                        )
            _enqueue_section_requests(pending)

        # --- Helper: run Phase 1 for one unit ---
        def _do_phase1(unit_info: Dict[str, Any]) -> int:
            label = _unit_label(unit_info)
            chunk_analyzer = self._make_chunk_analyzer_for_unit(
                unit_info, scratch_manager, phase1_client
            )
            proposed_facts, proposed_requests = chunk_analyzer.analyze_phase1(
                unit_info["working_item"],
                unit_info["type_name"],
                unit_info["unit_id"],
                score_level=unit_info["score"],
            )
            contributed = _gate_facts(proposed_facts, label)
            if contributed:
                print(f"    [P1] {label}: {len(contributed)} fact(s) accepted")
                self._save_question_object()
            _gate_and_enqueue_requests(
                proposed_requests, label, unit_info["parsed_content"],
                source_depth=unit_info.get("derivation_depth", 0),
            )
            return len(contributed)

        # --- Helper: run Phase 2 for one unit ---
        def _do_phase2(unit_info: Dict[str, Any]) -> None:
            label = _unit_label(unit_info)
            chunk_analyzer = self._make_chunk_analyzer_for_unit(
                unit_info, scratch_manager, phase2_client
            )
            proposed_questions, proposed_requests = chunk_analyzer.analyze_phase2(
                unit_info["working_item"],
                unit_info["type_name"],
                unit_info["unit_id"],
            )

            unit_depth = int(unit_info.get("derivation_depth", 0))
            accepted_q = 0
            for q_text, q_rationale in proposed_questions:
                verdict = _gate_question(
                    scratch_manager, question_gate_client, self._config, self.logfile,
                    self.question_text, q_text, label, rationale=q_rationale,
                    close_match_client=question_close_match_client,
                )
                if verdict["verdict"] == "accept":
                    q_id = scratch_manager.add_question(
                        q_text, label, rationale=q_rationale,
                        derivation_depth=unit_depth,
                    )
                    question_queue.append(q_id)
                    accepted_q += 1
                elif verdict["verdict"] == "merge" and verdict.get("merge_question_id"):
                    scratch_manager.merge_question(verdict["merge_question_id"], label)
                else:
                    scratch_manager.add_rejected_inquiry(
                        kind="sub_question",
                        text=q_text,
                        source_unit=label,
                        analyst_reason=q_rationale,
                        gate_reason=verdict.get("reason", ""),
                        depth=unit_depth,
                    )

            if accepted_q:
                print(f"    [P2] {label}: {accepted_q} question(s) accepted")
                self._save_question_object()

            _gate_and_enqueue_requests(
                proposed_requests, label, unit_info["parsed_content"],
                source_depth=unit_depth,
            )

        # --- Helper: run Phase 3 for one unit × one question ---
        def _do_phase3(unit_info: Dict[str, Any], q_id: str, q_text: str) -> int:
            label = _unit_label(unit_info)
            scratch_manager.mark_question_seen(q_id, label)
            # Fetch the sub-question's stored rationale so the fact gate can apply
            # the rationale-bridge criterion (task 8.R8).
            q_obj = next((x for x in scratch_manager.questions if x["id"] == q_id), None)
            q_rationale = q_obj.get("rationale") if q_obj else None
            chunk_analyzer = self._make_chunk_analyzer_for_unit(
                unit_info, scratch_manager, phase1_client  # Phase 3 reuses phase1 client
            )
            proposed_facts, proposed_requests = chunk_analyzer.analyze_phase1(
                unit_info["working_item"],
                unit_info["type_name"],
                unit_info["unit_id"],
                score_level=unit_info["score"],
                sub_question=q_text,
            )
            contributed = _gate_facts(
                proposed_facts, label,
                question_context=q_text,
                question_rationale=q_rationale,
            )
            if contributed:
                q = next((x for x in scratch_manager.questions if x["id"] == q_id), None)
                if q is not None:
                    q.setdefault("supporting_fact_ids", []).extend(contributed)
                print(f"    [P3] {label} × {q_id}: {len(contributed)} fact(s)")
                self._save_question_object()
            _gate_and_enqueue_requests(
                proposed_requests, label, unit_info["parsed_content"],
                source_depth=unit_info.get("derivation_depth", 0),
            )
            return len(contributed)

        # --- Helper: run circle-back for one unit × one answered question ---
        def _do_circleback(unit_info: Dict[str, Any], q: Dict[str, Any]) -> int:
            """Feed responsive facts back to a question's proposer for follow-up extraction."""
            label = _unit_label(unit_info)
            q_id = q["id"]
            unit_info.setdefault("circleback_done", set()).add(q_id)

            # Build the circleback context block shown to the analyst
            responsive_facts = [
                f for f in scratch_manager.facts
                if f["id"] in q.get("supporting_fact_ids", [])
            ]
            if not responsive_facts:
                return 0

            fact_lines = "\n".join(
                f"  [{f['id']}] {f['content']}" for f in responsive_facts
            )
            rationale_line = (
                f"Your rationale for asking: {q['rationale']}\n" if q.get("rationale") else ""
            )
            circleback_context = (
                f"ANSWERED SUB-QUESTION\n"
                f"{'=' * 50}\n"
                f"Sub-question: {q['text']}\n"
                f"{rationale_line}"
                f"Facts found in response:\n{fact_lines}\n"
                f"{'=' * 50}\n\n"
                f"Given the above answered sub-question, extract any additional facts from "
                f"your portion that are now relevant to the primary question.\n"
            )

            chunk_analyzer = self._make_chunk_analyzer_for_unit(
                unit_info, scratch_manager, phase1_client
            )
            proposed_facts, proposed_requests = chunk_analyzer.analyze_phase1(
                unit_info["working_item"],
                unit_info["type_name"],
                unit_info["unit_id"],
                score_level=unit_info["score"],
                circleback_context=circleback_context,
            )
            cb_gate_context = f"Follow-up after answered sub-question: {q['text']}"
            contributed = _gate_facts(
                proposed_facts, label,
                question_context=cb_gate_context,
                question_rationale=q.get("rationale"),
            )
            if contributed:
                print(f"    [CB] {label} ← {q_id}: {len(contributed)} new fact(s)")
                self._save_question_object()
            _gate_and_enqueue_requests(
                proposed_requests, label, unit_info["parsed_content"],
                source_depth=unit_info.get("derivation_depth", 0),
            )
            return len(contributed)

        # --- Helper: close a question if conditions are met ---
        def _maybe_close_question(q_id: str) -> None:
            if unit_queue:
                return  # New units may still appear
            q = next((x for x in scratch_manager.questions if x["id"] == q_id), None)
            if not q or q["status"] != "open":
                return
            seen_by = set(q.get("seen_by", []))
            if not active_unit_labels.issubset(seen_by):
                return
            has_facts = bool(q.get("supporting_fact_ids"))
            q["status"] = "answered" if has_facts else "unanswerable"
            print(f"  Question {q_id} closed: {q['status']}")
            self._save_question_object()

            # Enqueue circle-backs for original proposers if the question was answered
            # and is not a derived question (depth 0 only, to prevent cascades)
            if q["status"] == "answered" and q.get("derivation_depth", 0) == 0:
                for unit_info in active_units:
                    label = _unit_label(unit_info)
                    if label in q.get("proposed_by", []):
                        done = unit_info.get("circleback_done", set())
                        if q_id not in done:
                            circleback_queue.append((unit_info, q))

        # --- Main loop ---
        while (unit_queue
               or any(q["status"] == "open" for q in scratch_manager.questions)
               or circleback_queue):
            if unit_queue:
                unit_info = unit_queue.popleft()
                label = _unit_label(unit_info)
                print(f"\n  Processing: {label}")

                # Phase 1: fact extraction
                _do_phase1(unit_info)

                # Phase 2: question + request generation
                _do_phase2(unit_info)

                # Register unit as active
                active_units.append(unit_info)
                active_unit_labels.add(label)

                # Phase 3 inline: ask new unit about all currently-open questions
                open_qs = scratch_manager.get_open_questions()
                if open_qs:
                    print(f"  [P3 inline] {label}: {len(open_qs)} open question(s)...")
                    for q in open_qs:
                        if label not in q.get("seen_by", []):
                            _do_phase3(unit_info, q["id"], q["text"])

            elif question_queue:
                # Unit queue empty — drain question queue
                q_id = question_queue.popleft()
                q = next((x for x in scratch_manager.questions if x["id"] == q_id), None)
                if not q or q["status"] != "open":
                    continue

                print(f"\n  [P3 queue] {q_id}: {q['text'][:60]}...")
                seen_by = set(q.get("seen_by", []))
                unseen = [u for u in active_units if _unit_label(u) not in seen_by]
                for unit_info in unseen:
                    _do_phase3(unit_info, q_id, q["text"])

                _maybe_close_question(q_id)

            elif circleback_queue:
                # Question and unit queues empty — drain circle-back queue
                unit_info, q = circleback_queue.popleft()
                label = _unit_label(unit_info)
                print(f"\n  [CB] {label} ← {q['id']} (circle-back)")
                _do_circleback(unit_info, q)

            else:
                break

        # Close any remaining open questions
        for q in scratch_manager.questions:
            if q["status"] == "open":
                has_facts = bool(q.get("supporting_fact_ids"))
                q["status"] = "answered" if has_facts else "unanswerable"

        unanswerable = [q for q in scratch_manager.questions if q["status"] == "unanswerable"]
        scratch_manager.scratch["unresolved_questions"] = [
            {"id": q["id"], "text": q["text"]} for q in unanswerable
        ]

        if unanswerable:
            print(f"\n  {len(unanswerable)} question(s) unanswerable — flagged for final answer.")

        print(f"\n  Analysis complete: {scratch_manager.fact_count} fact(s), "
              f"{len(scratch_manager.questions)} question(s)")
        self._save_question_object()

    # Alias for backwards compatibility with callers
    def run_to_stability(self, base_max_iterations: int = None) -> None:
        """Deprecated: use run_analysis(). Retained for caller compatibility."""
        self.run_analysis()

    def _build_substantive_scratch_blocks(self) -> Tuple[str, str, str]:
        """
        Return (facts_block, questions_block, unresolved_block) for substantive
        answer prompts.

        Deliberately excludes diagnostic scratch fields (`rejected_inquiries`,
        `bridge_rejections`) so they cannot contaminate a prompt whose answer is
        supposed to read as a direct interpretation of the source documents.
        Diagnostic content is surfaced separately via
        `_generate_lines_not_pursued_section`.
        """
        scratch = self.question_object.get("scratch", {})
        facts = scratch.get("facts", [])
        questions = scratch.get("questions", [])
        unresolved = scratch.get("unresolved_questions", [])

        fact_lines = []
        for f in facts:
            sources = ", ".join(f.get("source_units", []))
            fact_lines.append(f"- {f['content']}  [sources: {sources}]")
        facts_block = "\n".join(fact_lines) if fact_lines else "(No facts collected.)"

        closed_qs = [q for q in questions if q["status"] in ("answered", "unanswerable")]
        q_lines = []
        for q in closed_qs:
            status_str = "ANSWERED" if q["status"] == "answered" else "UNANSWERABLE"
            q_lines.append(f"- [{status_str}] {q['text']}")
        questions_block = "\n".join(q_lines) if q_lines else "(No sub-questions were generated.)"

        unresolved_block = ""
        if unresolved:
            ur_lines = [f"- {u['text']}" for u in unresolved]
            unresolved_block = (
                "\nUnanswerable Questions (asked but no information found):\n"
                + "\n".join(ur_lines) + "\n"
            )

        return facts_block, questions_block, unresolved_block

    def _generate_lines_not_pursued_section(self) -> str:
        """
        Generate the "Lines of inquiry not pursued" diagnostic section via a
        separate, focused prompt.  Returns "" if there are no rejected inquiries.

        This is intentionally a second LLM call rather than part of the main
        answer prompt: the main answer prompt instructs the synthesizer not to
        mention analysts or analysis processes, which directly conflicts with
        the diagnostic purpose of this section.  Splitting the prompts lets each
        carry the instructions appropriate to its output without contradiction.
        """
        scratch = self.question_object.get("scratch", {})
        rejected = scratch.get("rejected_inquiries", [])
        if not rejected:
            return ""

        # Build a compact, structured input listing.  We feed the model the
        # already-deduped entries (dedup happens at add time) and let it
        # produce light prose rather than a raw bullet dump.
        item_lines = []
        for r in rejected:
            kind_label = (
                "Section request"
                if r.get("kind") == "section_request"
                else "Sub-question"
            )
            target = r.get("text", "")
            src = r.get("source_unit", "") or "(unknown)"
            analyst = r.get("analyst_reason", "")
            gate = r.get("gate_reason", "")
            depth = r.get("depth", 0)
            line = f"- [{kind_label}] {target}  | proposed by {src} (depth {depth})"
            if analyst:
                line += f"\n    Analyst reason: {analyst}"
            if gate:
                line += f"\n    Gate reason: {gate}"
            item_lines.append(line)
        items_block = "\n".join(item_lines)

        prompt = (
            "You are producing a short DIAGNOSTIC appendix to a legal-analysis "
            "answer. This section makes the research process visible to the "
            "reader by listing lines of inquiry that were proposed during "
            "analysis but rejected by a gatekeeper before being pursued.\n\n"
            f"Primary question being researched:\n{self.question_text}\n\n"
            f"Rejected inquiries (already deduplicated):\n{items_block}\n\n"
            "Write a section titled exactly:\n"
            "    Lines of inquiry not pursued\n\n"
            "Format:\n"
            "- One bullet per item above.\n"
            "- Each bullet should name the kind (section request or sub-question), "
            "the target/text, the proposing unit, and the gatekeeper's stated "
            "reason verbatim (or 'no reason recorded' if absent).\n"
            "- Group identical or near-identical gate reasons together if it "
            "improves readability, but do not invent reasons.\n"
            "- Do not editorialize about whether the gatekeeper was right or "
            "wrong, and do not speculate about what those provisions might have "
            "contained.\n"
            "- It is appropriate and expected for this section to refer to "
            "gatekeepers, analysts, and the analysis process — that is the "
            "section's purpose.\n"
            "- Output ONLY the section heading and the bullets. No preamble, "
            "no closing remarks.\n"
        )

        client = self._get_client_for_phase('final_answer')
        try:
            section_text = query_text_with_retry(
                client,
                [],
                prompt,
                self.logfile,
                max_tokens=4000,
                max_retries=3,
                config=self._config,
                task_name='qa.synthesis.final_answer',
            )
        except ModelError:
            # Diagnostic section is optional — failure should not block the answer.
            return ""

        section_text = (section_text or "").strip()
        if not section_text:
            return ""
        # Separate clearly from the main answer.
        return "\n\n---\n\n" + section_text

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

        # Build a clean, structured prompt from the fact pool and question statuses.
        # Diagnostic scratch fields (rejected_inquiries, bridge_rejections) are
        # excluded by _build_substantive_scratch_blocks and surfaced separately
        # via _generate_lines_not_pursued_section, so they cannot contaminate an
        # answer that is supposed to read as a direct interpretation of the
        # source documents.
        facts_block, questions_block, unresolved_block = self._build_substantive_scratch_blocks()

        prompt = []
        prompt.append(
            "You are a legal expert providing analysis based on a comprehensive review of one or more "
            "legal documents. Using only the information provided below, answer the question.\n\n"
        )
        prompt.append(
            "Background: Multiple AI analysts reviewed different sections of one or more legal documents "
            "and extracted the facts below. Each fact includes source information (the section or article "
            "it came from). Sources from the primary document appear as \"Section 5\" or \"Article 12\". "
            "Sources from cross-referenced external documents appear with a parenthetical document name, "
            "e.g., \"Section 744.17 (Part744)\".\n\n"
        )
        prompt.append(f"Question:\n{self.question_text}\n\n")
        prompt.append(f"Collected Facts:\n{facts_block}\n\n")
        prompt.append(f"Sub-Questions Explored:\n{questions_block}\n")
        if unresolved_block:
            prompt.append(unresolved_block)
        prompt.append("\n")
        prompt.append(
            "Instructions:\n"
            "- ANSWER THE QUESTION AS POSED: The opening of your answer must directly "
            "address the question as written, not a broader or related one. For yes/no "
            "questions, lead with a yes or no that is consistent with the substance that "
            "follows.\n"
            "- Provide a clear, comprehensive answer suitable for a legal practitioner.\n"
            "- CITE DOCUMENT SOURCES: When making claims, reference the specific section or article "
            "(from the source labels in the facts list). You may cite multiple sources when relevant.\n"
            "- CROSS-DOCUMENT CITATIONS: When a source label includes a parenthetical document name "
            "(e.g., \"Section 3A090 (Part774)\"), include the document name in your citation.\n"
            "- Do NOT mention internal IDs (f001, q001, etc.). Only cite substantive units "
            "(Sections, Articles, Chapters, etc.).\n"
            "- If there are unanswerable questions that materially affect the answer, acknowledge "
            "those limitations explicitly.\n"
            "- Do NOT mention analysts, analysis processes, or working documents. "
            "Write as if you are directly interpreting the legal documents.\n"
            "- If the information is insufficient to fully answer the question, explain what is "
            "known and what remains uncertain.\n"
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
                task_name='qa.synthesis.final_answer'
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
        
        # Append the diagnostic "Lines of inquiry not pursued" section via a
        # separate prompt that does not carry the "do not mention process"
        # instruction.  Empty string if no rejections were recorded.
        diagnostic = self._generate_lines_not_pursued_section()

        # Now set the new answer
        combined = str(answer_text).strip() + diagnostic
        self.question_object["working_answer"]["text"] = combined

        # Mark final answer as complete (idempotency)
        progress = self.question_object.get("progress", {})
        progress["final_answer_complete"] = True
        self.question_object["progress"] = progress

        self._save_question_object()

        print(f"Final answer generated ({len(combined)} characters)")

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

        # Extract sources from facts (WS8 schema: facts is a list)
        for fact_data in scratch.get("facts", []):
            for source_str in fact_data.get("source_units", []):
                source_units.add(source_str)

        # Extract sources from question answers (WS8 schema: questions is a list)
        for q_data in scratch.get("questions", []):
            for a_data in q_data.get("answers", []):
                source_units.add(a_data.get("unit_id", ""))

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
        # Use the same substantive-scratch summary as generate_final_answer.
        # Dumping raw scratch JSON here would feed diagnostic fields
        # (rejected_inquiries, bridge_rejections) into a prompt that instructs
        # the synthesizer not to mention process — a contamination risk.  The
        # diagnostic content is appended afterward via the separate
        # _generate_lines_not_pursued_section prompt.
        facts_block, questions_block, unresolved_block = self._build_substantive_scratch_blocks()

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
        prompt.append(f"Collected Facts:\n{facts_block}\n\n")
        prompt.append(f"Sub-Questions Explored:\n{questions_block}\n")
        if unresolved_block:
            prompt.append(unresolved_block)
        prompt.append("\n")
        prompt.append(
            "Instructions:\n"
            "- ANSWER THE QUESTION AS POSED: The opening of your answer must directly "
            "address the question as written, not a broader or related one. For yes/no "
            "questions, lead with a yes or no that is consistent with the substance that "
            "follows.\n"
            "- Provide a clear, comprehensive answer suitable for a legal practitioner.\n"
            "- CRITICAL - ADDRESS QUALITY CONCERNS: Your answer must correct the issues identified above. "
            "Ensure your statements are consistent with the source units and do not contain the errors flagged.\n"
            "- CRITICAL - CITE DOCUMENT SOURCES: When making claims or statements, you MUST reference "
            "the specific substantive units (e.g., \"Section 5\", \"Article 12\", \"Chapter 3\") where "
            "that information is found. Each fact in the Collected Facts list above includes source labels "
            "in square brackets — use these labels in your answer. You may reference multiple units when "
            "information comes from multiple sources.\n"
            "- CROSS-DOCUMENT CITATIONS: When a source label includes a parenthetical document name "
            "(e.g., \"Section 3A090 (Part774)\"), include the document name in your citation "
            "(e.g., \"Part 774, ECCN 3A090\"). This indicates the information comes from a "
            "cross-referenced external document rather than the primary document being analyzed.\n"
            "- DO NOT reference internal working document identifiers like \"f001\" or \"q001\". "
            "These are internal tracking IDs. ONLY reference the substantive units "
            "(Sections, Articles, Chapters, etc.) from the source labels.\n"
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

        # Re-append the diagnostic section so a regenerated answer carries the
        # same "Lines of inquiry not pursued" appendix as the initial answer.
        diagnostic = self._generate_lines_not_pursued_section()

        combined = str(new_answer_text).strip() + diagnostic
        self.question_object["working_answer"]["text"] = combined
        print(f"  Regenerated answer ({len(combined)} characters) addressing {len(all_concerns)} concern(s)")

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
    # Ensure stdout/stderr can emit non-ASCII diagnostics (e.g. the "←" used in
    # circle-back trace lines) when output is redirected or captured. On Windows,
    # a redirected stream defaults to the legacy cp1252 codec, which raises
    # UnicodeEncodeError on those characters and crashes the run. reconfigure()
    # exists on Python 3.7+; guard for older/oddball streams that lack it.
    for _stream in (sys.stdout, sys.stderr):
        _reconfigure = getattr(_stream, "reconfigure", None)
        if _reconfigure is not None:
            try:
                _reconfigure(encoding="utf-8", errors="replace")
            except (ValueError, OSError):
                pass

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

    # Phase 2: WS8 queue-driven analysis (Phase 1 → 2 → Phase 3 inline)
    print("Running iterative analysis to populate scratch document...")
    qp.run_analysis()

    # Phase 3: final answer synthesis
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


