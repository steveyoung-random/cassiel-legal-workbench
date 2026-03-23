# JSON File Specification (Version 0.5)

The Cassiel Legal Workbench uses a standardized JSON format to represent legal documents. The structure is hierarchical and supports both organizational units (chapters, parts, etc.) and substantive units (sections, articles, etc.).

Version 0.5 adds support for **nested substantive units** (sub-units) and **large table sub-units**. Documents without these features are fully compatible — all new fields are optional and the system handles v0.3 documents without modification.

## Complete Structure

```json
{
  "document_information": {
    "version": "0.5",
    "title": "Document Title",
    "long_title": "Full Document Title",
    "parameters": {
      "1": {
        "name": "section",
        "name_plural": "sections", 
        "operational": 1
      },
      "2": {
        "name": "recital",
        "name_plural": "recitals",
        "operational": 0
      }
    },
    "document_definitions": [
      {
        "term": "provider",
        "value": "definition text",
        "source_type": "section",
        "source_number": "201",
        "def_kind": "direct",
        "quality_checked": true
      }
    ],
    "organization": {
      "item_types": [1],
      "content": {
        "title": {
          "42": {
            "unit_title": "Title 42",
            "unit_definitions": [
              {
                "term": "provider",
                "value": "definition text",
                "source_type": "section",
                "source_number": "201",
                "def_kind": "direct",
                "quality_checked": true
              }
            ],
            "summary_1": "Summary of title...",
            "summary_2": "Detailed summary...",
            "begin_section": "201",
            "stop_section": "299",
            "chapter": {
              "6A": {
                "unit_title": "Chapter 6A",
                "unit_definitions": [],
                "summary_1": "Summary of chapter...",
                "begin_section": "201",
                "stop_section": "299"
              }
            }
          }
        }
      }
    }
  },
  "content": {
    "sections": {
      "201": {
        "text": "Section content...",
        "unit_title": "Section Title",
        "breakpoints": [
          [245, 1],
          [512, 2]
        ],
        "notes": {
          "1": "Footnote text here",
          "2": "Another footnote text"
        },
        "annotation": "Effective Date Note: At 74 FR 26008...",
        "context": [
          {"title": "42"},
          {"chapter": "6A"}
        ],
        "summary_1": "AI-generated concise summary (3 sentences)",
        "summary_2": "AI-generated detailed summary (5+ paragraphs)",
        "defined_terms": [
          {
            "term": "provider",
            "value": "definition text",
            "scope": "in this chapter",
            "indirect": "",
            "def_kind": "direct",
            "quality_checked": true
          }
        ],
        "ext_definitions": [
          {
            "term": "provider",
            "value": "definition text",
            "source_type": "section",
            "source_number": "215",
            "def_kind": "direct",
            "quality_checked": true
          }
        ],
        "need_ref": [
          {
            "type": "Section",
            "value": "215"
          },
          {
            "type": "Need_Definition",
            "value": "Provider"
          },
          {
            "type": "External",
            "value": "Article 5 of Regulation ABC"
          }
        ]
      }
    }
  }
}
```

## Field Specifications

### document_information

- **version** (string, required): Document format version (currently "0.5"; "0.3" documents are fully supported)
- **title** (string, optional): Short document title
- **long_title** (string, optional): Full document title
- **topic_statement** (string, optional): AI-generated phrase-based topic statement capturing key subject areas covered in the document
  - **Format**: Phrase-based (not full sentences), 2-4 lines when displayed, comma-separated topics
  - **Purpose**: Designed for quick scanning and document identification in library views
  - **Example**: "Library governance and funding, Congressional Research Service operations, film preservation and registry, trust fund management, Inspector General oversight"
  - **Generation**: Created during Stage 3 processing after all summaries are complete. May be regenerated if missing or empty.
- **content_scope** (array, optional, v0.5): When a parser processes only a subset of a source document (e.g., a specific title or part), this field records the path from the root to the scope root as a list of `{"type": "id"}` dicts. Downstream stages use it to restrict processing and progress-counting to the in-scope subtree. Absent when the full document is parsed.
  - Example: `[{"title": "15"}, {"part": "774"}]`
- **parameters** (object, required): Defines legal unit types
  - Each key is a string number (e.g., "1", "2")
  - Each value contains:
    - **name** (string): Singular name of unit type (e.g., "section", "article")
    - **name_plural** (string): Plural name (e.g., "sections", "articles")
    - **operational** (integer): 1 if unit has legal effect, 0 if not (e.g., recitals)
    - **reference_instruction** (string, optional): Instruction text included in AI prompts
      to guide reference formatting for this substantive type. Used when reference IDs
      require context-dependent formatting (e.g., appendix references that must include
      part numbers).
    - **is_sub_unit** (boolean, optional, v0.5): When `true`, indicates this parameter type represents sub-units that live inside parent containers' `sub_units` dict rather than having their own top-level content section. Defaults to `false`. Used by iterators to skip this type at the top level and instead yield it via container expansion.
    - **data_table** (integer, optional, v0.5): When `1`, indicates this parameter type represents a large data table sub-unit. Stages 2, 3, and 4 treat data-table types specially: Stage 2 skips definition extraction; Stage 3 generates a table-structure summary rather than a prose summary; Stage 4 uses the Stage 3 summary as analyst content rather than attempting to analyze raw table content.
    - **definition_list** (integer, optional, v0.5): When `1`, indicates this parameter type represents a definition-list sub-unit extracted from a long `<LIST>` element at parse time. The sub-unit's `text` contains the list items; `chunk_prefix` carries the scope preamble; `breakpoints` are set at each item boundary. Stage 2 and Stage 3 prepend the `chunk_prefix` to every model call. Name is `"definition_list"` / `"definition_lists"` unless that name conflicts with a pre-existing parameter.
- **document_definitions** (array, optional): Document-wide definitions (no scope limitations)
  - Each definition entry (see Definition Entry Structure below)
- **organization** (object, required): Hierarchical organizational structure
  - **item_types** (array): List of parameter keys that are organized by this structure
  - **content** (object): Nested structure of organizational units

### Organizational Units (within organization.content)

Organizational units can be nested (e.g., title → chapter → part). Each unit can contain:

- **unit_title** (string, optional): Title/heading of the organizational unit
- **unit_definitions** (array, optional): Definitions scoped to this organizational unit
  - Each definition entry (see Definition Entry Structure below)
- **summary_1** (string, optional): Concise summary of the organizational unit
- **summary_2** (string, optional): Detailed summary of the organizational unit
- **begin_<unit_type>** (string, optional): First substantive unit in this organizational unit
  - Example: `begin_section: "201"` indicates section 201 is the first section in this unit
- **stop_<unit_type>** (string, optional): Last substantive unit in this organizational unit
  - Example: `stop_section: "299"` indicates section 299 is the last section in this unit
- **<org_unit_type>** (object, optional): Nested organizational units (e.g., `chapter`, `part`)

### Substantive Units (within content.<unit_type_plural>)

Substantive units (sections, articles, etc.) can contain:

- **text** (string, required): The main text content of the legal unit
- **unit_title** (string, optional): Title/heading of the substantive unit
- **breakpoints** (array, optional): List of `[position, priority]` pairs for text chunking
  - `position` (integer): Character position in text
  - `priority` (integer): Lower numbers indicate preferred break points (1 = most preferred)
  - Used for AI processing when text exceeds context limits
- **notes** (object, optional): Dictionary mapping note identifiers to footnote text
  - Keys are note identifiers (strings), values are footnote text
- **annotation** (string, optional): Trailing parenthetical annotation text
  - Example: "Amended 2020", "Repealed 2019", "Effective 2021"
  - Currently only populated by CA parser
  - Extracted from trailing parentheticals in section text
  - Provides critical context for duplicate sections (see Duplicate Section Handling below)
- **context** (array, required): Organizational path showing where this unit appears
  - List of single-key dictionaries, e.g., `[{"title": "42"}, {"chapter": "6A"}]`
  - Ordered from highest to lowest organizational level
- **summary_1** (string, optional): Concise summary (typically 3 sentences) generated by AI
- **summary_2** (string, optional): Detailed summary (typically 5+ paragraphs) generated by AI
- **defined_terms** (array, optional): Definitions defined in this unit and scoped to this unit
  - Definitions scoped to the same unit where they are defined remain here for chunked analysis
  - Each definition entry (see Definition Entry Structure below)
- **ext_definitions** (array, optional): Definitions from elsewhere that apply to this unit
  - Definitions scoped to this unit but defined in another unit
  - Each definition entry (see Definition Entry Structure below)
- **need_ref** (array, optional): References extracted during summary generation
  - List of reference objects with:
    - **type** (string): Reference type - substantive unit type (e.g., "Section", "Article"), "Need_Definition", or "External"
    - **value** (string): Reference value - unit number, term name, or external reference text
- **sub_units** (object, optional, v0.5): Type-keyed dictionary of nested sub-units. Present only on container items that have been subdivided. When present, the parent's `text` field is empty (all text has moved into sub-units). Structure: `{param_key: {sub_unit_id: sub_unit_item, ...}, ...}`. See Nested Sub-Units below.
- **table_html** (string, optional, v0.5): Raw HTML or GPOTABLE XML for large table sub-units (those with `data_table: 1` in their parameter entry). Present only on table sub-units; `text` is empty on these items. Stored as-is from the source document; AI models read the HTML/XML directly.
- **placeholder** (string, optional, v0.5): Exact string used in the parent unit's `text` in place of this sub-unit's content. Enables deterministic text reconstruction: replace each placeholder with the sub-unit's `text`. Present on table and definition-list sub-units; absent from ECCN sub-units (which replace the parent text with an empty string rather than an inline placeholder).
- **chunk_prefix** (string, optional, v0.5): Text to prepend verbatim to every chunk when this sub-unit is sent to AI models. Applied unconditionally to all chunks including the first. Used on definition-list sub-units where context that belongs to the parent unit (e.g., a scope preamble) does not appear anywhere in the sub-unit's own text. The parser decides the format and meaning of the prefix — Stages 2 and 3 prepend it as-is without adding additional labels. For Formex definition lists, the parser stores `[Scope: "..."]` with the preamble text embedded. Not present on sub-units where the sub-unit text is self-contained.

### Nested Sub-Units (v0.5)

Some substantive units are extremely long (e.g., the Commerce Control List in CFR Title 15 Part 774, 1.68M characters). Version 0.5 allows these to be decomposed into independently processable sub-units at any nesting depth.

**Container items** are substantive units that have a `sub_units` field. The container's `text` may be empty, where all text content lives in the sub-units. The container retains its organizational context, summary fields, and definition fields (which aggregate sub-unit results).

**Sub-unit structure (v0.5):** `sub_units` is a type-keyed outer dict where each key is the parameter key (as a string) for that sub-unit type. The value is an inner dict mapping sub-unit identifier to sub-unit item. This allows a single container to have multiple sub-unit types and enables multi-level nesting.

**Multi-level nesting:** Sub-unit items can themselves have `sub_units`, enabling arbitrary nesting depth. For the CCL, the hierarchy is: Supplement → CCL Category (param 12) → CCL Section (param 13) → ECCN (param 11). Intermediate containers (categories, sections) have `text` fields for preamble content and `sub_units` for their children.

**CCL-specific parameter types:**
- `"12"`: `{"name": "ccl_category", "name_plural": "ccl_categories", "operational": 1, "is_sub_unit": true}` — e.g., "Category 0—Nuclear Materials"
- `"13"`: `{"name": "ccl_section", "name_plural": "ccl_sections", "operational": 1, "is_sub_unit": true}` — e.g., "A. 'End Items'"

**Definition-list sub-units:** Long definition lists (containing "definition" or "definitions" in the enclosing element's title, with at least 5 items totaling at least 4,000 characters) are extracted as `definition_list` sub-units during parsing. The parameter entry has `"is_sub_unit": true` and `"definition_list": 1`. The sub-unit `text` contains the list items only; the parent unit retains any preamble text (e.g., "For the purposes of this Regulation, the following definitions apply:") plus the `placeholder` string. The sub-unit carries a `chunk_prefix` with the preamble text so every AI call sees the scope context; `breakpoints` mark item boundaries for chunked processing. Currently supported by the Formex parser; other parsers may be extended in future.

**Large table sub-units:** HTML or XML tables with 50 or more rows are extracted as `table` sub-units during parsing. The parameter key is assigned dynamically by the parser (no hardcoded value). Keys within the sub-unit dict are numeric strings (`"1"`, `"2"`, …) matching natural table numbers in prose references. The parameter entry has `"is_sub_unit": true` and `"data_table": 1`. The sub-unit item has `text: ""` and stores the raw table markup in `table_html`. Stage 3 generates a `summary_1` describing the table structure. Parser-specific subdivision (e.g., CCL ECCN subdivision) takes precedence; `table` sub-units are the generic fallback for any large table not otherwise handled.

**`sub_unit_index` field (optional, at `document_information` level):** When nested sub-units are present, an optional index provides O(1) lookup by ECCN ID without scanning the tree.

```json
"sub_unit_index": {
  "11": {
    "0A001": {
      "container_plural": "supplements",
      "container_id": "No. 1 to Part 774",
      "path": ["12", "0", "13", "A"]
    }
  }
}
```

Navigation: starting from `content[container_plural][container_id]`, follow each `[param_key, item_id]` pair in `path` by traversing `sub_units[param_key][item_id]`. The final item's `sub_units[target_param_key][item_number]` is the target.

**Sub-unit context:** Each level's context extends the parent's context. For a 3-level CCL:
- Category context: `[..., {"supplement": "No. 1 to Part 774"}]`
- Section context: `[..., {"supplement": "..."}, {"ccl_category": "0"}]`
- ECCN context: `[..., {"supplement": "..."}, {"ccl_category": "0"}, {"ccl_section": "A"}]`

**`_preamble` sub-unit:** Text before the first detected sub-unit boundary is stored as a sub-unit with the key `"_preamble"`. This preserves introductory text that belongs to the parent but precedes any sub-unit. Each nesting level can have its own `_preamble`.

**Iterator behavior:**
- `iter_operational_items()` and `iter_all_items()` recursively descend to leaf sub-units (any depth). Callers transparently process ECCNs without knowing the intermediate structure.
- `iter_containers()` yields all items that contain `sub_units`, at any depth, in **post-order** (deepest first). This ensures sub-unit summaries are available when the parent container is processed.
- `lookup_item()` uses the `sub_unit_index` for O(1) lookup when available, falling back to recursive scan via `iter_containers()`.

**Example (3-level CCL hierarchy):**

```json
{
  "document_information": {
    "parameters": {
      "3":  {"name": "supplement",    "name_plural": "supplements",     "operational": 1},
      "4":  {"name": "table",         "name_plural": "tables",          "operational": 1, "is_sub_unit": true, "data_table": 1},
      "11": {"name": "eccn",          "name_plural": "eccns",           "operational": 1, "is_sub_unit": true},
      "12": {"name": "ccl_category",  "name_plural": "ccl_categories",  "operational": 1, "is_sub_unit": true},
      "13": {"name": "ccl_section",   "name_plural": "ccl_sections",    "operational": 1, "is_sub_unit": true}
    },
    "sub_unit_index": {
      "11": {
        "0A001": {"container_plural": "supplements", "container_id": "No. 1 to Part 774",
                  "path": ["12", "0", "13", "A"]}
      }
    }
  },
  "content": {
    "supplements": {
      "No. 1 to Part 774": {
        "text": "",
        "context": [{"title": "15"}, {"part": "774"}],
        "sub_units": {
          "12": {
            "_preamble": {"text": "CCL preamble...", "context": [...], "sub_units": {}},
            "0": {
              "text": "Category 0 preamble text.",
              "unit_title": "Category 0—Nuclear Materials...",
              "context": [..., {"supplement": "No. 1 to Part 774"}],
              "sub_units": {
                "13": {
                  "A": {
                    "text": "",
                    "unit_title": "A. 'End Items'",
                    "context": [..., {"ccl_category": "0"}],
                    "sub_units": {
                      "11": {
                        "0A001": {
                          "text": "ECCN 0A001 content...",
                          "unit_title": "0A001 ...",
                          "context": [..., {"ccl_category": "0"}, {"ccl_section": "A"}],
                          "breakpoints": [[100, 1]]
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
```

**Multiple sub-unit types:** Because `sub_units` is type-keyed, a single container can hold different sub-unit types simultaneously.

**Backward compatibility:** All sub-unit fields are optional. Documents with version "0.3" (no `sub_units` at all) work identically to before. The iterators check for `sub_units` via field-existence checks and fall back to normal behavior when absent.

### Duplicate Section Handling

When a parser encounters duplicate section identifiers (sections with the same number appearing multiple times in the source document at the same organizational location), it appends `_dup` to create unique identifiers:

- **First occurrence**: Uses the original identifier (e.g., `"2050"`)
- **Second occurrence**: Appends `_dup` suffix (e.g., `"2050_dup"`)
- **Third occurrence**: Appends additional `_dup` suffix (e.g., `"2050_dup_dup"`)

**Summarization and Question Answering Behavior:**

During Stage 3 (summarization) and Stage 4 (question answering), ALL sections that are part of a duplicate set automatically include content from `annotation` and `notes` fields in the text provided to AI models. This includes both the original section (without `_dup`) and all duplicate variants (with `_dup` suffixes). This ensures that:

- Summaries for duplicate sections reference annotation context (e.g., amendment dates, repeal status, effective dates)
- Question answers about alternative versions include critical metadata
- Each chunk of a duplicate section includes the full annotation/notes context

**Example:**

```json
{
  "sections": {
    "2050": {
      "text": "Original version text...",
      "annotation": "Repealed 2020"
    },
    "2050_dup": {
      "text": "Alternative version text...",
      "annotation": "Amended 2020, operative 2021"
    }
  }
}
```

Since both sections are part of a duplicate set, the system automatically appends their respective annotations:

**For section `2050`:**
```
[ANNOTATION]: Repealed 2020
```

**For section `2050_dup`:**
```
[ANNOTATION]: Amended 2020, operative 2021
```

This metadata appears in all text chunks during summarization and question answering, providing critical context for AI models to understand which version applies when.

### Definition Entry Structure

Definition entries can appear in `document_definitions`, `unit_definitions`, `defined_terms`, or `ext_definitions`:

- **term** (string, required): The term being defined
- **value** (string, required): The definition text (may be empty for indirect definitions)
- **scope** (string, optional): Original scope phrase (e.g., "in this chapter") - typically only in `defined_terms` before scope resolution
- **indirect** (string, optional): Original indirect reference string (e.g., "as defined in section 23") - may be present before resolution
- **indirect_loc_type** (string, optional): Resolved indirect location type (e.g., "section", "article")
- **indirect_loc_number** (string, optional): Resolved indirect location number/identifier
- **def_kind** (string, optional): Type of definition - "direct" (standalone definition) or "elaboration" (augments/limits existing definition)
- **source_type** (string, optional): Type of unit where definition was originally found (e.g., "section", "article")
- **source_number** (string, optional): Number/identifier of unit where definition was originally found
- **external_reference** (string, optional): External reference text if definition points to another document
- **quality_checked** (boolean, optional): Flag indicating definition has been evaluated for quality (prevents reprocessing)

