# Cassiel Legal Workbench: Worked Examples

This directory contains three worked examples illustrating the Cassiel pipeline on real legal documents. Each example walks through all four processing stages, with curated excerpts showing the input, intermediate data, and final output at each step.

These examples accompany the technical paper and are intended to make the pipeline concrete.

---

## The Pipeline (Quick Reference)

```
Stage 1: Parse source XML/HTML → JSON (document structure, text, breakpoints)
Stage 2: Extract definitions → JSON (term definitions with scope)
Stage 3: Generate summaries → JSON (two-level summaries per unit)
Stage 4: Answer questions → JSON (scratch document + final answer)
```

---

## Examples

### Example 1: USC Title 1, Chapter 1 (Rules of Construction)

**Parser**: USLM
**Document**: U.S. Code Title 1, Chapter 1 - Rules of Construction. A short chapter (11 sections) that defines how terms like "person", "county", "vessel", and "officer" are used throughout the entire U.S. Code.
**Question posed**: "If a company subject to a federal reporting obligation merges into another company, does the surviving company inherit that obligation?"
**Key features illustrated**:
- Core pipeline walkthrough on a small, self-contained document
- Multiple definition types: direct definitions ("the word 'person' ... includes"), elaboration definitions, and scoped definitions
- Two-level summaries (section-level and chapter-level)
- Efficient Q&A: small document finishes quickly with a well-grounded answer

See `01_usc_title1_ch1/README.md`.

---

### Example 2: California GOV § 11120 (Bagley-Keene Open Meeting Act)

**Parser**: CA HTML
**Document**: California Government Code § 11120, the statement of legislative intent for the Bagley-Keene Open Meeting Act. Governs open meetings of state bodies.
**Question posed**: TBD
**Key features illustrated**:
- HTML source (not XML), showing parser flexibility
- Scoring: how the system decides which sections to analyze in depth
- Scratch document evolution across Q&A rounds
- A clean, well-sourced final answer on a substantive policy question

See `02_ca_gov_11120/README.md`.

---

### Example 3: EU AI Act (Formex XML)

**Parser**: Formex
**Document**: Regulation (EU) 2024/1689, the EU Artificial Intelligence Act. A complex multi-article regulation governing AI systems in the European Union.
**Question posed**: TBD
**Key features illustrated**:
- Non-US jurisdiction and non-US XML format (Formex)
- Complex multi-article Q&A requiring synthesis across many sections
- Cross-reference handling in a dense regulatory text

See `03_eu_ai_act/README.md`.

---

## Directory Layout

```
examples/
  README.md                    ← this file
  01_usc_title1_ch1/
    README.md                  ← narrative walkthrough
    snippets/
      stage1_source.xml        ← excerpt of source XML
      stage1_parsed.json       ← excerpt of parse output
      stage2_definitions.json  ← excerpt of definition data
      stage3_summaries.json    ← excerpt of summary data
      stage4_answer.txt        ← final Q&A answer
      stage4_scratch.json      ← excerpt of scratch document
  02_ca_gov_11120/
    README.md
    snippets/
      ...
  03_eu_ai_act/
    README.md
    snippets/
      ...
```
