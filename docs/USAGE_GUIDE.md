# Cassiel Legal Workbench - Usage Guide

This guide provides practical examples for using the Cassiel Legal Workbench system.

## Running Parsers

### USLM Parser (United States Code)

```bash
# Default: Automatically splits multi-chapter documents into separate files
python uslm_set_parse.py /path/to/xml/file.xml

# Override: Parse entire document as single file
python uslm_set_parse.py /path/to/xml/file.xml --full-document

# Parse specific title and chapter
python uslm_set_parse.py /path/to/xml/file.xml --title 42 --chapter 6A

# Custom configuration
python uslm_set_parse.py /path/to/xml/file.xml --config custom_config.json
```

**USLM Parser Behavior**:
- **Default**: Splits multi-chapter USC Title documents into separate parse output files
- **Output**: Creates manifest file (`{file_stem}_manifest.json`) tracking all parsed files
- **Location**: All outputs written to configured output directory (default: `~/document_analyzer_output/`)
- **Structure**: Per-document subdirectories (e.g., `usc42/usc42_title42_chapter1_parse_output.json`)
- **Idempotency**: Re-running skips already-parsed chapters

### Formex Parser (European Union)

```bash
# Parse EU documents (Formex XML)
python formex_set_parse.py /path/to/xml/files/
```

### CA Parser (California)

```bash
# Parse California documents (HTML)
python CA_parse_set.py /path/to/html/files/
```

## Running Processing Stages

### Stage 2: Definition Processing

```bash
# Process all files in output directory (discovers via manifests)
python Process_Stage_2.py /path/to/output/directory

# Process specific chapter using manifest filter
python Process_Stage_2.py /path/to/output/directory --filter "chapter=43"

# Process all chapters in a title
python Process_Stage_2.py /path/to/output/directory --filter "title=42"

# Process specific JSON file directly
python Process_Stage_2.py /path/to/output/usc42/usc42_title42_chapter6A_parse_output.json

# Custom checkpoint threshold
python Process_Stage_2.py /path/to/output --filter "chapter=1" --checkpoint-threshold 50

# Old-style invocation (still works - backward compatible)
python Process_Stage_2.py /path/to/source/file.xml
```

**Features**:
- **Manifest Discovery**: Automatically finds parsed files via manifest files
- **Filtering**: Filter by any metadata field (title, chapter, etc.)
- **Direct JSON**: Can process `_parse_output.json` or `_processed.json` files directly
- **Status Tracking**: Adds `processing_status` to processed files with progress info
- **Idempotency**: Skips files where `stage_2_complete == true`
- **Backward Compatible**: Old-style `python Process_Stage_2.py <path>` still works

### Stage 3: Summary Processing

```bash
# Process all files in output directory (discovers via manifests)
python Process_Stage_3.py /path/to/output/directory

# Process specific chapter using manifest filter
python Process_Stage_3.py /path/to/output/directory --filter "chapter=43"

# Process all chapters in a title
python Process_Stage_3.py /path/to/output/directory --filter "title=42"

# Process specific JSON file directly (must be _processed.json, not _parse_output.json)
python Process_Stage_3.py /path/to/output/usc42/usc42_title42_chapter6A_processed.json

# Custom checkpoint threshold
python Process_Stage_3.py /path/to/output --filter "chapter=1" --checkpoint-threshold 50

# Old-style invocation (still works - backward compatible)
python Process_Stage_3.py /path/to/processed/files/
```

**Features** (Same as Stage 2):
- **Manifest Discovery**: Automatically finds processed files via manifest files
- **Filtering**: Filter by any metadata field (title, chapter, etc.)
- **Direct JSON**: Can process `_processed.json` files directly (NOT `_parse_output.json`)
- **Prerequisite Checking**: Verifies Stage 2 completion before processing
- **Status Tracking**: Updates `processing_status` with operational and organizational progress
- **Idempotency**: Skips files where `stage_3_complete == true`
- **Backward Compatible**: Old-style `python Process_Stage_3.py <path>` still works

**IMPORTANT**: Stage 3 requires Stage 2 to be complete first. It only reads from `_processed.json` files (produced by Stage 2), not from `_parse_output.json` files.

### Stage 4: Question-Answering

```bash
# Answer questions about a processed document (standard mode)
python question_answering.py /path/to/file.html "What is the question?"

# Or use a processed JSON file directly:
python question_answering.py /path/to/file_processed.json "What is the question?"

# Select a mode (quick_scan, standard, thorough, maximum_confidence)
python question_answering.py /path/to/file_processed.json "Question?" --mode thorough

# Or use a question from a text file:
python question_answering.py /path/to/file.html /path/to/question.txt
```

**Q&A Modes**:
- **quick_scan**: Fast scoring only, returns relevant sections without generating answer
- **standard**: Balanced speed/quality (default)
- **thorough**: Higher quality with more iterations and fallback analysis
- **maximum_confidence**: Highest quality with quality check phase that validates answer accuracy

**Quality Check Phase** (`maximum_confidence` mode only):
- Validates final answer by reviewing full text of source units (not summaries)
- Categorizes concerns as minor (appended to answer) or significant (triggers regeneration)
- Ensures answer accuracy without relying solely on summarized content

See `config.json` for mode configuration options and `DEVELOPER_GUIDE.md` for technical details.

## Keeping the Corpus Up to Date

When source documents are periodically updated, the system supports efficient corpus refresh: re-parse everything (fast, no AI calls), detect which documents changed, archive old processed files for changed documents, and re-run Stages 2/3. The API call cache returns cached results for unchanged units instantly, so only genuinely changed content generates new AI calls. See [CORPUS_MAINTENANCE.md](CORPUS_MAINTENANCE.md) for the full workflow.

## Processing Pipeline Overview

All four processing stages are fully working:

1. **Parsing** (`*_parse_set.py`): Converts XML/HTML legal documents to structured JSON
2. **Stage 2** (`Process_Stage_2.py`): Definition extraction, scope resolution, quality control, manifest discovery, status tracking
3. **Stage 3** (`Process_Stage_3.py`): Level 1 and level 2 summaries with chunking support, manifest discovery, status tracking, prerequisite checking
4. **Stage 4** (`question_answering.py`): Relevance scoring, iterative analysis, scratch document management, cleanup, and final answer synthesis. Supports configurable Q&A modes (quick_scan, standard, thorough, maximum_confidence) for different cost/quality tradeoffs.

## Cross-Reference Registry

The cross-reference registry tracks external citations found in processed documents and resolves them to specific corpus documents. It is stored as a SQLite database (`cross_reference_registry.db`) in the output directory alongside `jobs.db`.

Three tools work together:

| Tool | Purpose |
|------|---------|
| `tools/extract_references.py` | Scan processed JSON files and populate the registry |
| `tools/resolve_references.py` | Automatically match citations to corpus documents |
| `tools/registry_cli.py` | View registry state, manually review and resolve references |

### Typical Workflows

**Initial population (after parsing and processing Stage 2/3 for a batch of documents):**

```bash
# Step 1: Scan all processed files and load references into the registry
python tools/extract_references.py --all

# Step 2: Run automatic resolution (regex + metadata scoring)
python tools/resolve_references.py

# Step 3: Run AI resolution pass on citations that couldn't be parsed by regex
python tools/resolve_references.py --ai

# Step 4: Review the results
python tools/registry_cli.py report
```

**Ongoing (after processing new documents):** The post-Stage-3 hook in `batch_process.py` and the job worker automatically runs extract + resolve whenever Stage 3 completes. You can still run the tools manually if needed.

**Manual review of unresolved/ambiguous references:**

```bash
# See what needs attention
python tools/registry_cli.py report

# List ambiguous references
python tools/registry_cli.py refs --status ambiguous

# Inspect a specific reference
python tools/registry_cli.py show 42

# Find the target document ID
python tools/registry_cli.py docs

# Resolve it
python tools/registry_cli.py resolve 42 7 --notes "15 CFR Part 774 is in Title 15 corpus file"

# Or mark it as not in corpus
python tools/registry_cli.py not-in-corpus 42 --notes "References EAR preamble, not parsed"
```

---

### `extract_references.py` — Populate Registry

Scans processed document JSON files and registers all External cross-references (from `need_ref` and `defined_terms[].external_reference`). Safe to re-run; already-scanned documents are skipped by default.

```bash
python tools/extract_references.py <path> [<path> ...]
python tools/extract_references.py --all
python tools/extract_references.py --all --force
python tools/extract_references.py --db /path/to/registry.db <path>
```

**Arguments:**

| Argument | Description |
|----------|-------------|
| `path` | One or more JSON files or directories. Directories are searched recursively for `*_processed.json` files; `old/` and `archive/` subdirectories are skipped. Explicit file paths are accepted regardless of name. |
| `--all` | Use the output directory from `config.json` as the source (equivalent to passing the output directory as a path). |
| `--force` | Re-scan documents that were already scanned, picking up any changes. |
| `--db PATH` | Path to the registry SQLite database. Defaults to `{output_dir}/cross_reference_registry.db`. |

**Output:** Prints one line per file with status (`ok`, `skipped`, `ERROR`) and reference counts. Ends with a summary and the current registry state.

---

### `resolve_references.py` — Automatic Resolution

Runs a regex-based resolution pass against all unresolved references in the registry, matching citation strings to corpus documents by title, chapter, part, and section. Idempotent: already-resolved references are skipped unless `--force` is given.

```bash
python tools/resolve_references.py
python tools/resolve_references.py --force
python tools/resolve_references.py --verbose
python tools/resolve_references.py --ai
python tools/resolve_references.py --ai --verbose
python tools/resolve_references.py --db /path/to/registry.db
```

**Arguments:**

| Argument | Description |
|----------|-------------|
| `--force` | Re-resolve references that already have a resolution status (resolved, ambiguous, not_in_corpus). Useful after adding new corpus documents. |
| `--verbose` | Print one line per reference showing the resolution outcome during the pass. |
| `--ai` | After the automatic regex pass, run an AI-assisted pass targeting references whose citation strings could not be parsed (e.g., named acts like "the Bank Secrecy Act"). Requires a model configured for task `registry.resolution.ai` in `config.json` (falls back to `current_engine`). |
| `--db PATH` | Path to the registry database. |

**Resolution outcomes:**

| Status | Meaning |
|--------|---------|
| `resolved` | Citation matched exactly one corpus document. |
| `ambiguous` | Citation matched multiple corpus documents at the same score. Needs manual review. |
| `not_in_corpus` | Citation was parsed but no matching corpus document was found. |
| `unresolved` | Citation string could not be parsed by regex. Use `--ai` or manual resolution. |

See `registry/citation_patterns.md` for details on which citation forms are recognized and how scoring works.

---

### `registry_cli.py` — View and Manage

Interactive CLI for inspecting the registry and manually resolving references. All subcommands accept `--db PATH` to override the default database path.

```bash
python tools/registry_cli.py [--db PATH] <subcommand> [args]
```

#### `report` — Comprehensive resolution report

The primary review tool. Prints a structured report with all unresolved, ambiguous, and not-in-corpus references, plus actionable next steps.

```bash
python tools/registry_cli.py report
python tools/registry_cli.py report --unresolved-limit 0
python tools/registry_cli.py report --nic-limit 50
```

| Argument | Default | Description |
|----------|---------|-------------|
| `--unresolved-limit N` | 10 | Max unique unparseable citation strings to list. `0` = show all. |
| `--nic-limit N` | 20 | Max unique not-in-corpus citation strings to list. `0` = show all. |

#### `stats` — Registry statistics

```bash
python tools/registry_cli.py stats
```

Prints corpus document count, document split count, and cross-reference counts by status with a bar chart.

#### `docs` — List corpus documents

```bash
python tools/registry_cli.py docs
python tools/registry_cli.py docs --parser-type cfr
```

| Argument | Description |
|----------|-------------|
| `--parser-type TYPE` | Filter by parser type: `uslm`, `cfr`, `ca_html`, `formex`. |

Prints document ID, parser type, highest stage reached, and title. The **document ID** shown here is used as `target_doc_id` in the `resolve` command.

#### `refs` — List cross-references

```bash
python tools/registry_cli.py refs
python tools/registry_cli.py refs --status ambiguous
python tools/registry_cli.py refs --status unresolved --limit 0
python tools/registry_cli.py refs --doc 7 --context need_ref
```

| Argument | Default | Description |
|----------|---------|-------------|
| `--status STATUS` | (all) | Filter by status: `unresolved`, `ambiguous`, `resolved`, `not_in_corpus`. |
| `--doc DOC_ID` | (all) | Filter to references from the specified source document ID. |
| `--context TYPE` | (all) | Filter by reference context: `need_ref` (section-level dependency) or `definition` (term defined by reference). |
| `--limit N` | 50 | Maximum references to show. `0` = no limit. |

#### `show` — Full detail for one reference

```bash
python tools/registry_cli.py show 42
```

Shows all fields: citation text, reference context, source document + item, current target document, resolution method, resolution timestamp, and notes. Use this to understand an ambiguous or unresolved reference before deciding how to resolve it.

#### `resolve` — Manually resolve a reference

```bash
python tools/registry_cli.py resolve <ref_id> <target_doc_id>
python tools/registry_cli.py resolve 42 7 --notes "EAR is in Title 15 CFR file"
```

| Argument | Description |
|----------|-------------|
| `ref_id` | The reference ID (from `refs` or `report`). |
| `target_doc_id` | The corpus document ID to link to (from `docs`). |
| `--notes TEXT` | Optional explanation of why this resolution is correct. Stored in the registry. |

Marks the reference as `resolved` with method `manual`. Resolution is document-level (not section-level); section-level linking is handled at Stage 4 Q&A time (task 3.7).

#### `not-in-corpus` — Mark as not in corpus

```bash
python tools/registry_cli.py not-in-corpus 42
python tools/registry_cli.py not-in-corpus 42 --notes "References OMB Circular A-130, not parsed"
```

| Argument | Description |
|----------|-------------|
| `ref_id` | The reference ID. |
| `--notes TEXT` | Optional explanation. |

Use this for references that genuinely point to documents you do not have in the corpus and do not intend to add. The reference will be retried automatically if new documents are added and `resolve_references.py` is re-run.

#### `reset` — Reset a reference to unresolved

```bash
python tools/registry_cli.py reset 42
```

Resets any reference back to `unresolved` status, clearing the target document. Use this to correct a wrong manual resolution or to force re-resolution after corpus changes. After resetting, run `resolve_references.py` to attempt automatic resolution again.

---

## Manifest Files

Manifest files (`{file_stem}_manifest.json`) track parsed documents and their output files. They enable:
- Automatic file discovery across document roots
- Filtering by organizational units (title, chapter, etc.)
- Portability through relative paths
- Discovery of available filter keys

## Configuration

Configuration is managed through `config.json`. Key settings include:
- Document roots and parser assignments
- Output directory
- AI model settings (model-name-based configuration)
- Processing defaults
- Per-phase model selection for question answering

### AI Model Configuration

The system uses a model-name-based configuration that supports multiple models from the same or different platforms. Example configuration:

```json
{
  "models": {
    "gpt-5-nano": {
      "platform": "OpenAI",
      "model": "gpt-5-nano",
      "max_tokens": 8000
    },
    "gpt-5": {
      "platform": "OpenAI",
      "model": "gpt-5",
      "max_tokens": 8000
    },
    "claude-3-5-sonnet": {
      "platform": "Claude",
      "model": "claude-3-5-sonnet-20241022",
      "max_tokens": 8000
    }
  },
  "current_engine": "gpt-5-nano",
  "model_assignments": {
    "qa.analysis.analyze_chunk": "gpt-5",
    "qa.synthesis.cleanup_scratch": "gpt-5",
    "qa.synthesis.final_answer": "gpt-5"
  }
}
```

**Model Configuration**:
- Each model has a unique name (e.g., `"gpt-5-nano"`)
- `platform` specifies the provider (OpenAI, Claude, Azure)
- `model` specifies the actual API model name
- Multiple models from the same platform are supported

**Per-Task Model Assignment**:
Use `model_assignments` to assign specific models to specific tasks. Any task not listed uses `current_engine` as the default. See the comments in `config.json` for the full list of available task names.

See `config.json` for the complete configuration structure.

