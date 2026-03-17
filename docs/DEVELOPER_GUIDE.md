# Cassiel Legal Workbench - Developer Guide

This guide provides essential information for developers working on the Cassiel Legal Workbench. For usage instructions, see [`USAGE_GUIDE.md`](USAGE_GUIDE.md). For JSON format specifications, see [`JSON_SPECIFICATION.md`](JSON_SPECIFICATION.md).

---

## Quick Start

**Key Documentation:**
- [`USAGE_GUIDE.md`](USAGE_GUIDE.md) - How to use all tools
- [`JSON_SPECIFICATION.md`](JSON_SPECIFICATION.md) - Data format specification (v0.3 / v0.5)
- [`ADDING_NEW_PARSER.md`](ADDING_NEW_PARSER.md) - Guide for adding new parsers
- [`PLANNED_ENHANCEMENTS.md`](PLANNED_ENHANCEMENTS.md) - Future features and improvements
- [`VIEWER_GUIDE.md`](VIEWER_GUIDE.md) - Document viewer user guide

---

## System Overview

Cassiel Legal Workbench transforms legal documents (XML/HTML) into AI-enhanced JSON format with definitions, summaries, and question-answering capabilities. It handles documents from multiple jurisdictions (US Code, EU Regulations, California statutes).

### Processing Pipeline

**Stage 1: Parsing** (`*_parse_set.py`)
- Converts XML/HTML to standardized JSON (v0.3 for most parsers, v0.5 for CFR)
- Four parsers: USLM (USC), Formex (EU), CA HTML, CFR eCFR
- Extracts organizational structure, text, notes, breakpoints
- CFR parser v0.5: very long substantive units are automatically subdivided into nested sub-units (e.g., 638 ECCNs in the Commerce Control List become individually-processable sub-units)
- **Large HTML tables** (≥50 `<TR>` rows) are extracted as `table` sub-units (dynamic param key; `is_sub_unit: True`, `data_table: 1`). Name is `"table"` unless a non-sub-unit parameter already uses that name, in which case `"large_table"` is used. Keys are numeric (`"1"`, `"2"`, …) matching the natural table numbers in prose. `text = ""`; full HTML stored in `table_html`; `summary_1` set by Stage 3. Downstream stages identify data-table types by the `data_table: 1` flag, never by name. Parser-specific subdivision (CCL) takes precedence; the `table`/`large_table` sub-unit is the generic fallback. See `LARGE_TABLE_HANDLING.md` for full design.

**Stage 2: Definition Processing** (`Process_Stage_2.py`, `stage2/`)
- AI-powered definition extraction from legal text
- Scope resolution (AI extracts data, code handles logic)
- Quality control with retry mechanism
- Indirect definition resolution
- Sub-unit aware: iterates sub-units individually; `lookup_item()` resolves sub-unit references

**Stage 3: Summary Processing** (`Process_Stage_3.py`)
- Level 1 summaries (concise, 3 sentences)
- Level 2 summaries (detailed, 5+ paragraphs with context)
- Organizational summaries (hierarchical)
- Text chunking for long documents using breakpoints
- **Duplicate section handling**: Sections with `_dup` suffix automatically include `annotation` and `notes` fields in prompts, providing context about alternative versions, supersession dates, and conditional applicability
- Sub-unit aware: cumulative context resets at parent container boundaries; `container_summaries()` aggregates sub-unit summaries into a parent-level summary after each level

**Stage 4: Question-Answering** (`question_answering.py`)
- Relevance scoring (0-3 per unit)
- Iterative analysis with keyhole context
- Scratch document management (append-only)
- Detail escalation (level 1 → level 2 summaries)
- Sub-unit aware: scores each sub-unit individually; zero-score fallback expands containers to sub-units; analysis resolves sub-units via `lookup_item()`

### Architecture Features

✅ **Parser Plugin System** - Extensible to new document types (see `ADDING_NEW_PARSER.md`)
✅ **Flexible AI Models** - Per-task model configuration with fallback support
✅ **Three-Tier Caching** - API responses, embeddings, computed values
✅ **Job Queue System** - Async processing with worker process
✅ **Comprehensive UI** - Main UI for job management + Document Viewer for exploration

---

## File Structure

```
cassiel-legal-workbench/
├── *_parse_set.py           # Stage 1: Parsers (CA, CFR, Formex, USLM)
├── Process_Stage_2.py       # Stage 2: Definition processing
├── Process_Stage_3.py       # Stage 3: Summary generation
├── question_answering.py    # Stage 4: Q&A pipeline
├── batch_process.py         # CLI batch processing (Stage 2/3)
├── stage2/                  # Stage 2 modules
│   ├── processor.py         # DefinitionsProcessor class
│   ├── definition_extraction.py
│   ├── scope_resolution.py
│   ├── quality_control.py
│   ├── indirect_resolution.py
│   └── definition_management.py
├── parsers/                 # Parser plugin architecture
│   ├── adapter.py           # ParserAdapter base class
│   ├── registry.py          # Parser discovery and registration
│   ├── discovery.py         # Organizational unit discovery
│   └── *_adapter.py         # Parser adapters (USLM, Formex, CA)
├── worker/                  # Job queue system
│   ├── queue.py             # SQLite job queue manager
│   ├── jobs.py              # Job execution (all stages)
│   ├── run_worker.py        # Worker process launcher
│   └── schema.sql           # Queue database schema
├── ui/                      # Streamlit interfaces
│   ├── app.py               # Main UI (job management)
│   ├── view_document.py     # Document viewer
│   ├── column_layout.py     # View orchestration
│   ├── components/          # UI components (columns, jobs, questions)
│   └── viewer_components/   # Viewer components (library, navigation)
├── utils/                   # Utility modules
│   ├── ai_client.py         # AI model integration
│   ├── config.py            # Configuration management
│   ├── document_handling.py # Document structure utilities
│   ├── definition_helpers.py # Scope resolution helpers
│   ├── text_processing.py   # Text manipulation
│   ├── xml_processing.py    # XML parsing
│   ├── manifest_utils.py    # Manifest file management
│   └── ...
└── tools/                   # Developer utility scripts
    ├── analyze_qa_log.py    # Q&A log: unit trace and compaction analysis
    ├── analyze_qa_logs.py   # Q&A log: session overview (analyst calls, facts, questions)
    ├── log_viewer.py        # Interactive prompt/response viewer for any stage (Stage 2, 3, or 4 log files)
    ├── analyze_cfr_elements.py  # CFR XML element analysis
    ├── detect_long_units.py # Scan parsed output for long units; reports patterns and subdivision candidates
    └── ...                  # Other utilities
```

---

## Stage 1: Parsing

**Goal:** Convert source documents to standardized JSON format (v0.3 for USLM/Formex/CA; v0.5 for CFR).

**Parsers:**
- `uslm_set_parse.py` - United States Code (USLM XML) → v0.3
- `formex_set_parse.py` - European Union regulations (Formex XML) → v0.3
- `CA_parse_set.py` - California statutes (HTML) → v0.3
- `cfr_set_parse.py` - Code of Federal Regulations (eCFR XML) → v0.5, see `CFR_ECFR_PARSER_PLAN.md`

**v0.5 Schema (Nested Sub-Units):**

The CFR parser automatically subdivides very long supplements into nested sub-units. For example, the Commerce Control List (Supplement No. 1 to Part 774, 1.68M chars) becomes 649 sub-units (638 ECCNs + 10 EAR99 variants + 1 preamble), each independently processable through Stages 2-4.

Key schema in v0.5:
- `parameters[M]["is_sub_unit"]` — marks types that live inside parent containers (no top-level content section)
- Container items have `sub_units` as a type-keyed dict: `{param_key: {sub_id: sub_item, ...}, ...}` (e.g. `{"11": {"0A001": {...}, ...}}`), allowing multiple sub-unit types per container
- Each sub-unit item has the same fields as a substantive unit (`text`, `context`, `breakpoints`, etc.)

All processing stages handle both v0.3 and v0.5 documents transparently. See `JSON_SPECIFICATION.md` for full schema details and `NESTED_SUBSTANTIVE_UNITS_PLAN.md` for design rationale.

**Key Tasks:**
- Extract organizational hierarchy (titles, chapters, sections)
- Build `organization.content` nested structure
- Generate breakpoints for text chunking (`[[position, priority], ...]`)
- Extract footnotes/notes where available
- Output to configured directory with manifest files

**Command-Line Usage:**
```bash
python uslm_set_parse.py <input_path> [--config config.json]
python formex_set_parse.py <input_path>
python CA_parse_set.py <input_path>
python cfr_set_parse.py <input_path> [--mode full|split|auto] [--specific "part=17"]
```

**Output Structure:**
- All files of same type go to common folder (e.g., `output_dir/USC/`, `output_dir/CA/`)
- Each file gets `*_parse_output.json` + `*_manifest.json`
- Manifests track source file, parser type, organizational units

**Adding New Parsers:** See `ADDING_NEW_PARSER.md` for complete guide.

---

## Stage 2: Definitions

**Entry Point:** `Process_Stage_2.py`
**Modules:** `stage2/` package

**Processing Flow:**
1. **Definition Extraction** - AI extracts defined terms from each substantive unit
2. **Scope Resolution** (v2) - AI extracts structured data, code handles resolution logic
3. **Quality Control** - Binary evaluation (good/bad), retry poor definitions
4. **Indirect Resolution** - Copy from target locations or construct from text

**Key Principles:**
- Locally scoped definitions stay in `defined_terms` for chunked analysis
- Scope resolution uses minimal prompts (title + org types only)
- Code handles deterministic logic (ranges, "current", sub-units)
- Incremental processing with `quality_checked` markers

**Command-Line Usage:**
```bash
python Process_Stage_2.py <input_json> [--config config.json]
python batch_process.py <directory> --stages 2  # Batch processing
```

**Documentation:**
- [`DEFINITIONS_SYSTEM_DOCUMENTATION.md`](DEFINITIONS_SYSTEM_DOCUMENTATION.md) - How definitions work
- [`SCOPE_RESOLUTION_LOGIC.md`](SCOPE_RESOLUTION_LOGIC.md) - Scope resolution details

---

## Stage 3: Summaries

**Entry Point:** `Process_Stage_3.py`

**Processing Flow:**
1. Organization summaries (recursive hierarchy)
2. Level 1 summaries (concise with references)
3. Level 2 summaries (detailed with context)
4. Topic statement generation

**Key Features:**
- **Text Chunking** - Handles long documents using parser breakpoints
- **Context Building** - Collects scope-aware definitions from hierarchy
- **Caching** - Three-tier strategy (API, embeddings, computed)

**Key Functions:**
- `collect_scoped_definitions()` - Walks hierarchy for definitions in scope
- `build_level_2_context()` - Builds context with definitions + referenced sections
- `fuzzy_match_term()` - Resolves term names (threshold > 87)

**Command-Line Usage:**
```bash
python Process_Stage_3.py <input_json> [--config config.json]
python batch_process.py <directory> --stages 3  # Batch processing
python batch_process.py <directory> --stages 2,3  # Both stages
```

---

## Stage 4: Question-Answering

**Entry Point:** `question_answering.py`

**Key Components:**
- **ScratchDocumentManager** - Shared scratch document (append-only)
- **ContextBuilder** - Keyhole context with three-tier caching
- **QuestionProcessor** - Orchestrates scoring, analysis, cleanup, answer

**Processing Flow:**
1. **Relevance Scoring** - Score each unit 0-3 based on summaries
2. **Iterative Analysis** - Per-unit analysts with keyhole context
3. **Cleanup** - Deduplicate and merge scratch entries
4. **Final Answer** - Synthesize narrative from cleaned scratch

**Design Principles:**
- **Keyhole Context** - Each analyst sees minimal info (unit + needed definitions + refs)
- **Append-Only** - Analysts never delete/overwrite, only add
- **Staged Detail** - Start with level 1, escalate to level 2 or full text
- **Iterative Refinement** - Multiple passes until stability

**Configurable Q&A Modes**:
- **Quick Scan** - Scoring only, no answer generation (fastest)
- **Standard** - Balanced speed/quality (default, backward compatible)
- **Thorough** - Higher quality with more iterations
- **Maximum Confidence** - Highest quality with quality check phase

Modes can be selected via CLI (`--mode`), worker parameters (`qa_mode`), or UI dropdown.

**Command-Line Usage:**
```bash
# Basic usage
python question_answering.py <processed_json> "Your question here?"

# With mode selection
python question_answering.py <processed_json> "Your question?" --mode thorough

# With custom overrides
python question_answering.py <processed_json> "Your question?" --mode standard --scoring-summary summary_2 --max-iterations 5
```

---

## AI Model Configuration

Models are configured in `config.json` with per-task model selection and fallback support.

**Configuration Structure:**
```json
{
  "models": {
    "gpt-5-nano": {
      "platform": "OpenAI",
      "model": "gpt-5-nano",
      "max_tokens": 8000
    },
    "claude-sonnet-4-5": {
      "platform": "Claude",
      "model": "claude-3-5-sonnet-20241022",
      "max_tokens": 8000
    }
  },
  "current_engine": "gpt-5-nano",
  "retry": {
    "max_retries_per_model": 3,
    "fallback_models": ["gpt-5", "claude-sonnet-4-5"]
  },
  "tasks": {
    "stage3.summary.level2": {
      "model": "gpt-5",
      "fallback_models": ["claude-sonnet-4-5"]
    }
  }
}
```

**Key Features:**
- Per-task model selection (e.g., lightweight for scoring, heavy for analysis)
- Automatic fallback on failures
- Multiple models per platform
- Cache compatibility across model names

**Helper Functions** (in `utils/config.py`):
- `get_model_config(model_name)` - Get model configuration
- `get_task_config(task_name)` - Get task-specific configuration
- `get_client_for_task(task_name)` - Create client for specific task

---

## Parser Plugin Architecture

**Core Principle:** Organizational structure is discovered from parsed output, not predefined.

**Key Benefits:**
- **True Parser Independence** - No hardcoded organizational structures
- **Complete Flexibility** - Variable structure within single document
- **Discovery-Based** - Org units discovered at parse time
- **Extensibility** - Add parsers that discover any structure

**Adding a Parser:**
1. Create adapter file (`parsers/your_parser_adapter.py`)
2. Implement `ParserAdapter` interface (see `parsers/adapter.py`)
3. Register in `parsers/registry.py`
4. No changes to core system, UI, or job queue

See `ADDING_NEW_PARSER.md` for detailed instructions and code examples.

---

## Utility Modules (`utils/`)

**Core Utilities:**
- `ai_client.py` - AI model integration (OpenAI/Anthropic), query functions, logging
- `config.py` - Configuration management, model selection, per-task configuration
- `document_handling.py` - Document structure navigation, TOC generation, sub-unit iteration and lookup (`iter_operational_items`, `iter_all_items`, `lookup_item`, `has_sub_units`, `iter_containers`, `get_item_numbers_for_type`)
- `text_processing.py` - Text normalization, breakpoint management, HTML table conversion
- `xml_processing.py` - XML element finding, namespace handling
- `manifest_utils.py` - Manifest file management for parsed documents
- `chunking_helpers.py` - Text chunking utilities for long documents

**HTML Table Conversion (`text_processing.py`):**

The `html_table_to_plaintext()` function converts HTML tables to formatted plain text. This utility is available for future use in presenting human-readable output but is **not currently used in parsing** because it does not yet produce accurate enough output for all table types.

```python
from utils.text_processing import html_table_to_plaintext

plain_text = html_table_to_plaintext(html_table_string)
```

Features:
- Word-wrapping within fixed-width columns (no truncation)
- Row separators between each data row (`-+-`)
- Distinct header/footer separators (`=+=`)
- Colspan/rowspan handling
- Fallback to original HTML for overly complex tables (nested tables, etc.)

Helper functions (prefixed with `_`):
- `_parse_table_to_grid()` - Parse HTML to 2D grid with header/footer detection
- `_calculate_column_widths()` - Optimal column sizing respecting longest words
- `_wrap_text()` - Word-wrap text avoiding mid-word breaks
- `_assess_table_complexity()` - Detect tables that can't be reliably converted

**Note:** Parsers currently preserve tables as processed HTML/XML with text transformations applied (fractions, superscripts, etc.). See `CFR_ECFR_PARSER_PLAN.md` for details on table handling in the CFR parser.

**Definition Processing:**
- `definition_helpers.py` - Scope resolution (unit matching, ranges, "current")
- `definition_prompts.py` - Prompt building for definition tasks

**Error Handling:**
- `error_handling.py` - Custom exceptions (ParseError, ModelError, etc.)
- `document_issues.py` - Document-level issue logging

See individual module docstrings for detailed function documentation.

---

## Design Notes

### Parser Plugin Architecture

**Discovery-Based Design:**

Organizational structure is discovered from parsed output rather than predefined. Parser capabilities declare only splitting information — not organizational structure, which varies even within a single document type. The processing stages work entirely from what the parser places in the JSON output; they have no built-in assumptions about any particular document's structure.

Each parser adapter declares:
- Whether splitting is supported and the split unit names (e.g., "chapter" for a given parser)
- File extensions and display name

It does not declare predefined organizational unit names — those are discovered at parse time and encoded in the JSON.

### Scope Resolution

**Code-Heavy Approach:**

Scope resolution uses a deliberately code-heavy design: the AI extracts structured data (element type and designation), and deterministic code handles the resolution logic (ranges, "current" references, sub-unit targets). This keeps prompts small and resolution behavior predictable.

One subtlety worth noting: simpler models can have difficulty distinguishing scope language from indirection language when extracting scopes from indirect definitions. A common error is extracting "of this title" from the phrase "as defined in section 123 of this title" as if it were a scope indicator, when it is actually part of the cross-reference locator. The solution is to assign a more capable model specifically to the `stage2.definitions.extract_scope_with_indirect` task (when the definition has a non-empty `indirect` field), while using a lighter model for straightforward scope extraction. This keeps cost reasonable while ensuring accuracy where it matters.

### Definition Quality Control

**Design Principles:**

Binary evaluation (good/bad) is sufficient for driving the retry decision. Definitions are grouped by source and target unit for caching efficiency. Processing markers (`quality_checked`) make the pipeline idempotent — re-running will not re-evaluate already-checked definitions. When a definition cannot be repaired after retry, the system fails rather than passing a known-bad definition downstream.

### Locally Scoped Definitions

When units are chunked using breakpoints, locally scoped definitions must remain in `defined_terms` to ensure availability during chunked analysis.

### Question-Answering Architecture

**Key Principles:**
- **Keyhole context** reduces hallucination, improves focus
- **Append-only** prevents discarding important information
- **Separate cleanup phase** enables effective deduplication
- **Staged detail escalation** balances richness with efficiency
- **Cache optimization** maximizes API cache hits

**Processing Phases:**

1. **Relevance Scoring** (`score_relevance()`)
   - Scores organizational units (chapters, titles) using summary_1
   - Filters substantive units by organizational relevance
   - Scores substantive units using configurable summary level
   - Fallback re-scoring with summary_2 if no high-relevance units found

2. **Iterative Analysis** (`run_to_stability()`)
   - Analyzes units in priority order (score 3 → 2 → 1)
   - Uses ChunkAnalyzer with escalation (summary_1 → summary_2)
   - Skip list optimization avoids re-analyzing empty units
   - Dynamic iteration extension when new sections added
   - Zero-score fallback analysis when no high-relevance units found

3. **Cleanup** (`cleanup_scratch_and_answer()`)
   - Deduplicates facts and consolidates sources
   - Drops irrelevant/trivial entries
   - Optionally generates working answer proposal

4. **Final Answer** (`generate_final_answer()`)
   - Synthesizes answer from cleaned scratch document
   - Includes source citations to substantive units
   - Preserves previous mode answers in history

5. **Quality Check** (`quality_check_answer()`, `maximum_confidence` mode only)
   - Validates final answer against full text of source units
   - Parallel review: one analyst per substantive unit
   - Categorizes concerns: minor (appended) vs significant (regenerates)
   - Addresses "worst of both worlds" problem: validates actual final answer

**Q&A Modes** (see `config.json` for configuration):

- **quick_scan**: Scoring only, no analysis (stops after phase 1)
- **standard**: Balanced cost/quality (default)
  - Uses summary_1 for scoring
  - 3 max iterations
  - No organizational summary scoring
  - No quality check

- **thorough**: Higher quality analysis
  - Uses summary_2 for scoring
  - 6 max iterations
  - Organizational summary scoring enabled
  - Fallback to summary_2 if no high-relevance units
  - Zero-score section analysis enabled

- **maximum_confidence**: Highest quality with validation
  - Same as thorough, plus:
  - Quality check phase validates final answer
  - Reviews full text (not summaries) of source units
  - Regenerates or appends concerns based on severity

**Quality Check Implementation Details:**

The quality check phase addresses a critical flaw: validating a working answer then generating a new final answer without considering validation results.

**Current Design:**
1. Runs AFTER final answer generation (Phase 5)
2. Extracts source units from scratch document
3. For each source unit, queries AI analyst with:
   - Full text of the unit (not summary)
   - Final answer to validate
   - Context (definitions, organizational location)
4. Categorizes results:
   - **No issues** → Accept answer as-is
   - **Minor concerns** (mostly_consistent, low/medium severity) → Append concerns to answer
   - **Significant issues** (inconsistent, needs_review, high severity) → Regenerate answer with concerns as feedback
5. Saves quality_concerns in question file for review

**Key Design Decision:** Quality check validates the FINAL answer, not a preliminary working answer, ensuring validation results directly inform the delivered answer.

**A few specific bugs worth noting for anyone extending the Q&A pipeline:**
- Organizational filtering that sets unit scores to 0 breaks the zero-score fallback path; units should be skipped rather than zeroed out when excluded by organizational filtering.
- Zero-score analysis only has an effect if it triggers additional iterations; make sure the continuation loop is in place.
- Save data to disk before printing output; Unicode encoding errors in the print path should not interrupt processing.

### Formex Parser XML Library

`formex_set_parse.py` uses Python's standard library `xml.etree.ElementTree` while the other parsers use `lxml.etree`. This reflects the order in which the parsers were written. Local versions of `get_all_elements()` and `get_first_element()` override the lxml-based utility functions for this parser. Standardizing the Formex parser to use lxml would eliminate this duplication but would require thorough testing with EU documents.

### AI Response Robustness

**Multi-Model Fallback:**

Different AI models have different failure modes for difficult content. Automatic fallback to an alternative model is generally more effective than increasing retry counts on the same model — the system tries alternative models before giving up on a call entirely.

**Cache Management:**

The local cache is keyed on the full call signature. When all retries are exhausted, the cache entry is cleared so that a future run gets a fresh attempt. Empty JSON responses (e.g., `[]`) can be valid, so semantic validation using `expected_keys` is preferable to treating all empty responses as failures.

**Defensive Data Handling:**

AI models occasionally return unexpected formats — for example, a list where a string summary is expected. The system applies defensive format checks and auto-corrects where possible, logging a warning but continuing rather than failing. This should be applied consistently across similar fields (e.g., both `summary_1` and `summary_2`).

**Task-Specific Model Assignment:**

Because different tasks have different reasoning demands, models are assigned per task in `config.json`. Lighter models handle high-volume, straightforward tasks (scoring, extraction); more capable models handle complex tasks (indirect scope resolution, synthesis). Task-specific config variants — such as `stage2.definitions.extract_scope` vs. `stage2.definitions.extract_scope_with_indirect` — allow fine-grained control without changing the pipeline logic.

### Batch Processing

Batch processing is implemented as a standalone script (`batch_process.py`) rather than as flags on the individual stage scripts. This keeps each stage script simple and focused, and makes it easier to add batch-specific behavior such as error collection and progress reporting across many files.

---

## Testing & Debugging

**Tools:**
- **Stage 4 (Q&A) logs:** `tools/analyze_qa_log.py` (unit trace, compaction) and `tools/analyze_qa_logs.py` (session overview). See [`tools/README_analyze_qa_logs.md`](../tools/README_analyze_qa_logs.md).
- **Stage 2 and Stage 3 logs:** `tools/log_viewer.py` — interactive split-screen viewer for any AI call log (`log0001.json`, etc.). Run e.g. `python tools/log_viewer.py <path/to/log0001.json>`. Use next/prev, goto entry, and scroll each side; optional `rich` for better UI.
- **Document Issues Logs** (`document_issues*.json`) - Scope resolution and processing issues (no dedicated viewer; inspect JSON in output directory).
- **Parsing corrections** - Logged via `log_parsing_correction()` to document-issues-style logs when provided.

**Debugging Tips:**
1. Check log files for detailed error information
2. Use incremental processing to isolate issues
3. Validate JSON output against specification (`JSON_SPECIFICATION.md`)
4. Test with smaller document samples first
5. Review parsing correction logs for patterns

---

## Current Limitations

1. **California Parser** - HTML source; XML preferred (optional improvement)
2. California Parser Features** - Notes/breakpoints not implemented (optional)
3. **Question-Answering** - Single-document only (multi-document planned)
4. **Detail Escalation** - Limited to summary_1 → summary_2 (full text escalation not yet implemented)
5. **Testing** - Comprehensive end-to-end validation needed, especially for quality check phase

See [`PLANNED_ENHANCEMENTS.md`](PLANNED_ENHANCEMENTS.md) for planned improvements.

---

## Contributing

This project is not currently accepting external contributions. If you have found a bug or have a suggestion, please open a GitHub issue.

If you are extending the system — for example, adding a new parser — see [`ADDING_NEW_PARSER.md`](ADDING_NEW_PARSER.md) for the plugin interface and [`PLANNED_ENHANCEMENTS.md`](PLANNED_ENHANCEMENTS.md) for planned work that may overlap with what you have in mind.

Key principles to follow if you do work on the code:

1. **Follow JSON Specification** - Maintain compatibility with v0.3; see [`JSON_SPECIFICATION.md`](JSON_SPECIFICATION.md)
2. **Maintain document-agnosticism** - Stages 2–4 must know nothing about specific document types; all such knowledge belongs in parsers
3. **Fail loudly** - If input has structural problems, raise an error rather than silently dropping information
4. **Keep parsers independent** - Parser plugins should not depend on each other
5. **Test with multiple document types** - Changes to shared utilities can affect all four parsers

---

Cassiel Legal Workbench provides a complete pipeline from document parsing through question answering. All four processing stages are operational. The job queue system and UI provide convenient interfaces for document management and exploration.
For usage instructions, see [USAGE_GUIDE.md](USAGE_GUIDE.md)
