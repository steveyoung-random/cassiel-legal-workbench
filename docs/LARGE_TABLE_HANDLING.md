# Large Table Handling (Workstream 9)

This document describes the design and implementation of large HTML table handling in the Cassiel Legal Workbench pipeline.

---

## 1. Motivation

The pipeline's substantive unit processing (Stages 2–4) is designed for legal prose. Large HTML tables embedded in parsed units are a fundamentally different content type — they are data records, not text to be comprehended. Forcing them through the current pipeline produces poor results:

- **Stage 2**: Definitions don't live in table rows; extraction would produce noise.
- **Stage 3**: No automated heuristic can reliably describe an arbitrary regulatory table's purpose and structure across the diversity of CFR content (entity lists, tariff codes, boundary coordinates, fee schedules, etc.).
- **Stage 4**: Feeding 2M chars of HTML to an analyst is impractical and wasteful.

The correct treatment — modeled on how figures are handled — is to extract large tables as first-class sub-units of type `table`, give them minimal but useful Stage 3 summaries (via a targeted AI call on the early portion), and produce a pointer-style response in Stage 4 rather than attempting to answer from table content directly.

---

## 2. Detection

`tools/detect_long_units.py` reports `<TR>` count for large HTML tables.

The pattern `HTML table rows` uses regex `<TR[\s>]` (case-insensitive). The threshold for this pattern is **50 rows** (not the default 5 used for other patterns). Units with ≥50 `<TR>` matches are flagged as candidates for extraction.

---

## 3. Schema

A `table` sub-unit has the following structure:

```json
{
  "text": "",
  "table_html": "<TABLE>...</TABLE>",
  "table_row_count": 3421,
  "table_column_headers": ["Country", "Entity", "License requirement"],
  "table_caption": "",
  "unit_title": "Table 1",
  "context": [{"part": "744"}, {"supplement": "No. 4 to Part 744"}],
  "breakpoints": []
}
```

Field notes:
- `text = ""` is intentional — mirrors container nodes. Stages use `summary_1` (set by Stage 3) or `table_html` for access to actual data.
- `table_row_count` is an approximate count of row elements (includes header/footer rows).
- `table_column_headers`: for HTML TABLE elements, extracted from `<TH>` in `<THEAD>`; for GPOTABLE elements, from `<CHED>` in `<BOXHD>`. Empty list if none found.
- `table_caption`: for HTML TABLE, from `<CAPTION>`; for GPOTABLE, from `<TTITLE>`. Empty string if absent.
- `table_html`: for HTML TABLE elements, contains serialized HTML. For CFR GPOTABLE elements (e.g., the Entity List), contains raw GPOTABLE XML. Stage 3 AI handles both formats.

Parameter entry (key assigned dynamically — the next available integer key in `document_information.parameters`):
```json
{
  "name": "table",
  "name_plural": "tables",
  "operational": 1,
  "is_sub_unit": true,
  "data_table": 1
}
```

The name is `"table"` in most documents. If `"table"` is already taken by a regular non-sub-unit operational parameter (e.g., a CFR title that uses appendix-style "Table" items), the name falls back to `"large_table"` / `"large_tables"` to avoid the collision. The parameter key is **not hardcoded** — it is assigned dynamically by `find_or_create_table_param_key()` in `utils/large_table_common.py`.

Sub-unit keys are derived from the local table counter within the parent item: `"1"`, `"2"`, etc. These match the natural labels used in document prose ("Table 1", "Table 2") and in Stage 4 prompts, so analysts can directly connect a prose reference like "refer to Table 1 below" to the extracted sub-unit. If a key would collide with a key already registered in the document-level index (two different parent items both producing "Table 1"), a short suffix derived from the parent ID is appended (e.g., `"1_rt744"`).

Index entry:
```json
{
  "1": {
    "container_plural": "supplements",
    "container_id": "No. 4 to Part 744",
    "path": ["No. 4 to Part 744"]
  }
}
```

---

## 4. Parser Integration

### XML-Level Interception (CFR)

Large tables in CFR XML exist as `GPOTABLE` or `TABLE` elements. The CFR parser normally calls `extract_table_text()` on these elements, converting them to plaintext or HTML before any post-hoc extraction could run. To avoid this, `parse_appendix()` in `cfr_set_parse.py` intercepts large table elements **at the XML level**, before `extract_table_text()` is called.

During the text-assembly loop, when a `GPOTABLE` or `TABLE` element is encountered:
1. Row count is checked (`_count_xml_table_rows()`): `ROW` descendants for GPOTABLE, `TR` descendants for TABLE.
2. If count ≥ `LARGE_TABLE_ROW_THRESHOLD` (50): the element is added to `pending_large_tables`; a placeholder `[Table N pending sub-unit extraction]` is added to `text_parts` instead.
3. If count < threshold: `extract_table_text()` is called as normal (table stays in parent text).

After text assembly and after the CCL subdivision check, `pending_large_tables` is processed:
- `_build_xml_table_sub_unit()` serializes each element and extracts metadata.
- `_assign_table_key()` computes globally unique keys (same algorithm as `utils/table_handling.py`).
- The pending placeholder is replaced with the final key-based marker: `[Table N extracted as sub-unit table KEY]`.
- `_find_or_create_table_param_key()` finds or allocates the `table` parameter entry dynamically.
- Sub-units and index entries are registered in `document_information`.

### HTML Text Scanning (generic fallback)

`utils/table_handling.py` exports `extract_large_tables(text, context, parent_type_name, parent_id)` for parsers that produce HTML text with `<TABLE>` elements. This is a post-hoc text scanner — it searches the assembled `text` field for `<TABLE>` HTML. It is **not** used for the CFR parser (where the XML-level path applies), but is available for future parsers that produce HTML-containing text.

### Tables Inside Nested DIVs

`extract_nested_div_content()` accepts an optional `pending_large_tables` parameter. When `parse_appendix()` passes this list, large GPOTABLE/TABLE elements encountered inside nested DIVs are appended to it and replaced with placeholders, so they are extracted as sub-units along with direct appendix children. The recursive DIV handler passes `pending_large_tables` through to nested DIVs.

### Guard Against Double-Subdivision

The `if pending_large_tables and not item_entry.get("sub_units"):` guard in `parse_appendix()` prevents table extraction from running if CCL subdivision has already produced sub-units. CCL subdivision produces semantic sub-units (`eccn`, `ccl_category`, `ccl_section`) that already handle the large-table problem better than the generic `table` path.

### XML-Level Interception (USLM)

In `uslm_set_parse.py`, `get_element_text2()` accepts optional `pending_large_tables`. When processing a child with tag `table`, if row count (`.//tr`) ≥ 50, the element is appended to `pending_large_tables` and a placeholder is returned. `process_section_element()` passes the list, then registers sub-units after text assembly. Tables are serialized to HTML via lxml.

### XML-Level Interception (Formex)

In `formex_set_parse.py`, `get_element_text2()` accepts optional `pending_large_tables`. When processing a child with tag `TGROUP`, if row count (`.//ROW`) ≥ 50, the element is appended and a placeholder returned. `add_text_brkpts_notes()` passes the list; `extract_articles()` and `extract_annexes()` call `_register_pending_large_tables()` after building text. TGROUP is serialized as raw XML (like GPOTABLE).

### DOM-Level Interception (CA / BeautifulSoup)

In `CA_parse_set.py`, the section content loop iterates over `current_div.find_all(['p', 'table'])` in document order. For each `table` element, if `len(table.find_all('tr'))` ≥ 50, the table is added to `pending_large_tables` and a placeholder is appended. `_register_ca_large_tables()` serializes the table via `str(table_tag)` (HTML) and registers sub-units. Small tables use `html_table_to_plaintext()` inline.

### Shared Helpers

`utils/large_table_common.py` provides:
- `assign_table_key(local_num, parent_id, taken)` — unique sub-unit keys
- `find_or_create_table_param_key(param_pointer)` — dynamic `table` parameter
- `build_table_sub_unit_from_html(html_str, row_count, headers, caption, ...)` — generic sub-unit builder

---

## 5. Stage 2

`table` sub-units are skipped entirely. In `find_defined_terms()`, a set of data-table type names is built from `data_table: 1` parameter entries. If `item_type_name` is in this set, `defined_terms = []` is set and the item is skipped. No AI calls are made.

---

## 6. Stage 3

`table` sub-units receive a targeted AI summary of the early portion of the table content.

**Function**: `_generate_table_structure_summary(working_item, proc)` in `Process_Stage_3.py`.

1. Uses `working_item.get("table_html", "")[:12000]` — enough to see headers + first ~20–30 rows (for GPOTABLE XML, this covers the BOXHD and initial ROW elements).
2. Sends to AI with `stage3.summary.table` task (configured to `claude-sonnet-4-6`).
3. Prompt: describe (1) what the table contains and its likely purpose, (2) what each column represents, (3) how rows are organized. If unclear, respond `CANNOT_DETERMINE`.
4. If AI returns `CANNOT_DETERMINE` or call fails: fallback to generic text — `"Large table (N rows). Columns: A, B, C."` or `"Large table (N rows)."` if no headers.
5. Result is stored in `working_item['summary_1']`.

The leaf summary loop in `level_1_summaries()` builds a set of data-table type names from `data_table: 1` parameter entries. Items in this set get `_generate_table_structure_summary()`, bypassing the normal prose summarization path.

---

## 7. Stage 4

**Scoring**: `table` sub-units have `text = ""` but `summary_1` set. The existing scoring path in `score_relevance()` already fetches `summary_1` for scoring when `text` is empty — no change needed for scoring.

**Analysis**: In `ChunkAnalyzer.analyze_chunks()`, if `text` is empty, the code checks whether the item type has `data_table: 1` in `document_information.parameters`. If so, `summary_1` is used as the text. The analyst receives a description of the table's structure and purpose. Because the "text" is a description rather than actual data, the LLM naturally produces a pointer-style response: "this question may be answerable by consulting this table, looking for [X]".

No special analyst instruction is needed for the first pass — the pointer-style response emerges naturally from the summarized content.

---

## 8. Escape Hatch

Parser-level specific subdivision (like CCL for Part 774 Supplement 1) produces non-`table` sub-units (e.g., `eccn`, `ccl_category`, `ccl_section`) that bypass this entire path. The guard in `parse_appendix()` ensures table extraction only fires when no subdivision has already occurred.

Over time, parsers for specific document types can produce semantically structured sub-units in place of the generic `table` sub-unit. The large-table generic path is the fallback for everything not yet given specific treatment.

---

## 9. Type Name, Key, and Collision Safety

The type is normally named `"table"` to match the natural language used in document prose — Stage 4 prompt headers read `"Table 1: Table 1"`, matching cross-references like "refer to Table 1 below".

**Name collision avoidance**: Some CFR titles already have a regular non-sub-unit operational parameter named `"table"` (e.g., appendix-style "Table to Part N" items). In that case, `find_or_create_table_param_key()` detects the conflict and uses `"large_table"` / `"large_tables"` instead. Downstream stages never check for a specific name — they identify data-table sub-units by the `data_table: 1` + `is_sub_unit: True` combination, so the name change is transparent.

**No hardcoded parameter key**: The parameter key is assigned by `find_or_create_table_param_key()` in `utils/large_table_common.py`, which searches `document_information.parameters` for an existing `data_table: 1` + `is_sub_unit: True` entry and returns its key, or allocates the next available integer key if none exists.

---

## 10. Known Limitations

- **Pointer-only Stage 4 answers**: Stage 4 can tell a user "the answer is probably in the Entity List table, look for China in the Country column" but cannot query specific rows. Row-level Q&A would require a different architecture (e.g., structured table parsing + SQL or embedding-based row lookup).
- **No row-level Q&A**: The table content is stored in `table_html` but is not indexed. Retrieving specific rows requires a separate lookup mechanism not yet implemented.
- **Approximate row count**: `table_row_count` counts all row elements including header and footer rows, so the count may slightly exceed the number of data rows.
- **`parse_appendix` only**: XML-level interception is implemented in `parse_appendix()` (direct appendix children and tables inside nested DIVs via `extract_nested_div_content()`). Large tables in regular CFR sections (`parse_section()`) are not yet extracted as sub-units.
