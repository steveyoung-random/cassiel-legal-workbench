# Corpus Maintenance

This document describes how to keep a document corpus up to date as source documents
are revised over time.

## The Problem

Legal documents — CFR parts, USC titles, state statutes — are periodically updated.
When a new version is released, three questions arise:

1. Which documents actually changed?
2. Within a changed document, which units changed?
3. How do we avoid re-running expensive AI processing stages on things that haven't changed?

## The Pipeline Cost Structure

The pipeline has two kinds of steps:

- **Parsing** (Stage 1): fast, no AI calls. Converts XML or HTML source to JSON.
  Re-parsing the entire corpus takes minutes.
- **Definition extraction and summarization** (Stages 2 and 3): slow, AI calls per unit.
  Re-running Stage 2/3 on a large document can take hours and cost meaningful money.
- **Q&A** (Stage 4): user-driven, run on demand. Not part of routine maintenance.

The goal of corpus maintenance is therefore: **re-parse freely; avoid re-running Stages
2/3 on documents and units that haven't changed.**

The system achieves this through **API call caching**: each AI call is keyed on its
prompt content, so if a unit's text is unchanged the cached result is returned
immediately — no AI call, no cost. When a changed document is re-run from scratch,
only the changed units generate new AI calls; all unchanged units are served from cache.

## The Maintenance Workflow

The following workflow applies when a new bulk release of source documents is available.

### Step 1: Download the new release

Download the updated source files and overwrite the previous versions in place — same
filenames, same directory. The system tracks a hash of each source file in its manifest
so it can detect what changed; overwriting in place keeps this tracking accurate.

> If you need to retain old source files for archival purposes, keep them elsewhere.
> The working copy of each source file should always be the current version.

### Step 2: Re-parse all documents in the set

Run the appropriate parser on all source files in the set:

```
python cfr_set_parse.py <source_directory>
python uslm_set_parse.py <source_directory>
# etc.
```

Parsing is fast (no AI calls), so it is practical to re-parse the entire set even when
most documents are unchanged. Each parser run records a hash of the source file in the
document manifest.

This overwrites each document's `*_parse_output.json` with fresh output.

### Step 3: Detect changes and archive changed processed files

For each document, compare the new parse output to the existing processed file to
identify which units actually changed. For changed documents, archive the old processed
file so Stage 2/3 re-runs from scratch:

```
python tools/diff_document.py <path_to_parse_output.json> --archive
```

For each document, this tool:

- Compares the new parse output to the existing `*_processed.json`
- If nothing changed: does nothing (prints "No changes")
- If units changed: moves the processed file to an `archive/YYYYMMDD/` subdirectory,
  clears the document's cross-reference registry entries, and prints re-run instructions

For a typical periodic update where most documents are stable, the vast majority of
documents will report "No changes" and complete in milliseconds.

To run across an entire corpus directory in one command:

```
python tools/diff_document.py <output_directory> --all --archive
```

To preview what changed without archiving anything, omit `--archive`:

```
python tools/diff_document.py <output_directory> --all
```

### Step 4: Re-run Stage 2/3 batch processing

```
python batch_process.py <directory> --stages 2,3
```

Stage 2/3 automatically skip documents that are fully processed. After Step 3, only
archived documents are missing their processed files, so Stage 2/3 re-run only on those.
Within a re-run document, the API call cache returns cached results for unchanged units
instantly — effectively skipping them at near-zero cost.

The result: AI processing runs only on what actually changed.

---

## Edge Cases and Limitations

**Stage 4 results are not automatically invalidated.** If you have previously run Q&A
on a document and the document is subsequently updated, the Q&A answers may be stale.
The diff tool will flag changed documents in its report, but refreshing Stage 4 output
is a manual decision.

**New documents in the set** (a new CFR Part is added, for example) will have no
existing processed file. The diff tool will note this; you then run Stage 2/3 on the
new document from scratch. Your batch workflow should flag parse outputs that have no
corresponding processed file.

**Split documents** (large CFR titles parsed into per-part files) are handled as
independent documents. Parse and diff each split file separately.

**Structural changes** (e.g., a Part is reorganized into different sections) are
handled correctly — the entire document is re-run from scratch, so new units are
processed fresh, removed units are simply absent, and all Stage 2/3 results are
regenerated from the new parse output.
