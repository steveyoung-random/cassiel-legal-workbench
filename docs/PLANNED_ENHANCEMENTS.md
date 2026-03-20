# Cassiel Legal Workbench - Planned Enhancements

This document describes planned enhancements and known areas for improvement.
For completed work, see `archive/COMPLETED_ENHANCEMENTS.md`.

---

## Processing Enhancements

### Model-Agnostic Cache Mode

**Priority**: Medium

The current API call cache keys results on both prompt content and the model
name. When switching to a newer model, the cache goes cold and every call is
treated as new — even for documents and units whose content has not changed.

The planned enhancement adds a second cache mode that keys on prompt content
only, ignoring the model. In this mode, a re-run after a model upgrade reuses
cached results for unchanged units (from the previous model) and generates
new results only for changed units (using the new model). The document is
processed efficiently, though units from different processing runs may have
used different models — an accepted trade-off when the goal is cost-efficient
corpus maintenance rather than uniform model coverage.

The model-aware mode (current behavior) is preserved and remains the default.
Users who want uniform model coverage across a document can always do a full
model-aware rerun.

---

### Image Processing in Documents

**Priority**: Low

Legal documents sometimes contain diagrams, tables as images, or scanned
inserts. Current parsers skip non-text content. A future enhancement could
use OCR or vision-model extraction for embedded images. This requires a
survey of how common this is across the four supported document types before
committing to implementation.

---

## Question-Answering Enhancements

### Stage 4 Architecture Overhaul (Role-Separated Analysis)

**Priority**: Medium

The current Stage 4 pipeline uses a broadcast-and-compact model in which
each analyst both extracts facts and asks questions in the same call, with a
shared scratch document that grows and is periodically compacted. Two
persistent quality issues have been observed with long documents: fact churn
(facts generated and then lost to compaction, including specific
cross-references) and question-list growth that dilutes analyst focus.

The planned redesign separates analyst roles into three phases per round:

1. **Fact extraction** — each analyst extracts only facts directly relevant
   to the primary question. A gatekeeper LLM reviews each proposed fact and
   accepts or rejects it before it enters the shared scratch document.
2. **Question/request generation** — each analyst proposes questions about
   other units. A gatekeeper filters semantic duplicates and off-topic
   questions.
3. **Answer collection** — for each open question, units that have not yet
   been asked are queried with a single question. A unit is offered each
   question only once. After first-pass answers are collected, a cleanup
   sub-pass allows units that contributed answers to clarify or correct based
   on what others said.

Questions carry lifecycle state (open / answered / closed) and are never
closed as unanswered — they remain open until answered, since units added
later via section requests may hold the answer. Old mechanisms (interval
compaction, scratch freeze, implicit reference detection, end-of-round
dedup) are retired. Chunking mechanics are preserved intact.

---

### Multi-Document Question Answering

**Priority**: Medium

**What is implemented**: Cross-document analysis via the reference registry
is already in place. When a document cites sections in other corpus
documents, those sections are automatically resolved and pulled into Stage 4
as additional analysts. Citations are tracked through to the final answer.

**What is not yet implemented**: Running a Stage 4 Q&A session against an
arbitrary set of documents that are not cross-referenced by a primary
document — for example, asking a question across all processed CFR titles
simultaneously, or across both a USC title and a CFR part that share a
subject area. This would require a document-selection mechanism and a way to
synthesize findings across independently-scored document sets.

---

### Stage 4: Analyst Notes Request Support

**Priority**: Low

Substantive units may have footnotes (`notes` field) stored separately from
main text (e.g., CFR footnote elements, USLM footnotes). Currently, notes
are included in analyst prompts only for duplicate sections (`_dup`); other
units see `(note: X)` placeholders but not the note content. The planned
enhancement allows an analyst to request note content for any unit — either
the one they are analyzing or another — analogous to how
`request_relevant_section` works for full text. The resolved note content
would be included in context so it can inform the answer.

---

### Q&A Relevant Provisions Export

**Priority**: Low

After a Q&A run, export the set of substantive units that contributed to
the answer (score ≥ 2, or cited in the scratch document) as a standalone
JSON or readable document. Useful for audit trails and for surfacing source
material without re-running the full pipeline.

---

## UI Enhancements

### Pagination UX

**Priority**: Low

Improve navigation for large document collections: paged views, scroll
position preservation, better handling of long section lists in the document
viewer.

### Accessibility

**Priority**: Low

Keyboard navigation for the main UI and document viewer; screen-reader-friendly
labels and contrast improvements.

---

## Analysis Tools

### Definition Review Tool

**Priority**: Low

A UI or CLI tool for reviewing extracted definitions across a processed
document. Filters by quality score, conflict count, and scope type; allows
manual accept/reject/edit. Useful for validating Stage 2 output on a new
document type before relying on it downstream.

---

### AI Model Comparison Tool

**Priority**: Low

A partial implementation exists in `testing/model_comparison/`. The goal is
a tool that takes the same documents processed by different model
configurations, extracts definitions, summaries, and scope resolutions
side-by-side, and uses an evaluator model to produce a comparative quality
assessment. This supports data-driven model selection and cost/quality
tradeoff analysis.

---

## Longer-Term Enhancements

### Advanced Definition Management

**Priority**: Low

- Automatic conflict resolution for duplicate definitions across documents
- Definition versioning for documents that change over time

### California Parser Improvements

**Priority**: Low

The California parser uses HTML source; XML would be preferable.
Breakpoints and footnotes are not currently extracted.

### Formex Parser Standardization

**Priority**: Low

`formex_set_parse.py` uses Python's standard library XML parser while the
other parsers use `lxml`. Standardizing to `lxml` would eliminate local
override functions but requires thorough testing with EU documents.

---

*Last Updated: 2026-03-19*
