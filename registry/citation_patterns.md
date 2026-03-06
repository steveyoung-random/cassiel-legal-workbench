# Cross-Reference Citation Patterns

This document describes how cross-references are recognized and resolved for
each parser type in the corpus. It is a living reference: update it when
manual resolution (task 3.4) surfaces new patterns.

---

## 1. USC / USLM (`parser_type = 'uslm'`)

### Citation String Patterns

The regex resolver recognizes these forms (in order of specificity):

| Pattern | Example |
|---|---|
| `section {N} of title {T}, United States Code` | `section 1813 of title 12, United States Code` |
| `section {N} of title {T}` | `section 1813 of title 12` |
| `{T} U.S.C. § {N}` | `12 U.S.C. § 1813` |
| `{T} U.S.C. {N}` | `12 U.S.C. 1813` |
| `{T} U.S.C.` (title only) | `12 U.S.C.` |
| `title {T}, United States Code` | `title 12, United States Code` |

Section numbers (`{N}`) may include letter suffixes and hyphens:
`1813`, `2000a`, `2000a-4`, `2000e-2(k)`. The resolver extracts the raw
string and uses boundary-aware prefix matching against known sections.

**Unrecognized patterns** (left as `unresolved` for AI or manual review):
- Named acts: `the Equal Credit Opportunity Act`, `Dodd-Frank Act`
- Statute citations without title/section numbers
- State law citations embedded in USC text

### Corpus File Structure

One JSON file per chapter per USC title:
```
usc{NN}_title{T}_chapter{C}_{stage}.json
```

`document_information.organization.content` has keys `"title"` and/or
`"chapter"`, each mapping to a dict of value → metadata. Example:
```json
{
  "title": {"12": {"name": "Banks and Banking", ...}},
  "chapter": {"1": {"name": "...", "begin_section": "1", "stop_section": "8"}}
}
```
→ `org_top_keys = {"title": ["12"], "chapter": ["1"]}`

`content_items["section"]` contains the actual section numbers present in
that chapter file (authoritative; used for section-level resolution).

### Designator Format

USC section designators may be:
- Pure numeric: `1`, `1234`
- Numeric with letter suffix: `1752a`, `2000e`
- Numeric with hyphen: `2000a-4`, `1752a-1`
- Rarely: named supplements or appendices (not section-type items)

Boundary-aware prefix rule applies: `"2000"` is a valid prefix of `"2000a"`
(digit→letter class change) but `"200"` is NOT a valid prefix of `"2000"`
(digit→digit, same class).

---

## 2. CFR (`parser_type = 'cfr'`)

### Citation String Patterns

| Pattern | Example |
|---|---|
| `{T} C.F.R. Part {P}` | `14 C.F.R. Part 39` |
| `{T} C.F.R. § {P}.{S}` | `14 C.F.R. § 39.5` |
| `{T} C.F.R.` (title only) | `14 C.F.R.` |
| `Part {P}` (bare, low confidence) | `Part 39` |

CFR section numbers include the part as prefix: `"39.5"` is section 5 of
part 39. When resolving a part-level citation (`cfr_part = 39`), the
resolver looks for items in `content_items` where `"39"` is a valid prefix
(e.g., section `"39"`, `"39.1"`, `"39.5"` all match).

**Unrecognized patterns**:
- Part cross-references without title: `Part 774 of the EAR`
- Full title of the chapter: `the Export Administration Regulations`

### Corpus File Structure

One JSON file per full CFR title:
```
title-{T}__date-{YYYY-MM-DD}_{stage}.json
```

`document_information.organization.content` has key `"title"` mapping to the
title number. Example:
```json
{"title": {"15": {"name": "Commerce and Foreign Trade", ...}}}
```
→ `org_top_keys = {"title": ["15"]}`

Since each file covers the full title, a title-level citation is typically
unambiguous (one file per title). Part-level citations can be confirmed
via `content_items`.

### Designator Format

CFR section numbers use part-prefixed dotted notation: `39.5`, `774.1`,
`200.302`. Appendix designators: `Appendix A to Part 39`,
`Supplement No. 1 to Part 774`.

Part-level citations (the most common CFR citation form) are matched against
`org_keys["part"]`, which is populated from the organizational hierarchy.
The resolver does not use section numbers to infer part membership.

---

## 3. California Codes (`parser_type = 'ca_html'`)

**Status: AI/manual resolution only.** No regex patterns are currently
implemented. Corpus files contain California code sections.

### Known citation forms (for future implementation):
- `Section {N} of the {Code Name} Code` — e.g., `Section 1234 of the Civil Code`
- `{Code Abbrev.} § {N}` — e.g., `Cal. Civ. Code § 1234`
- `{Code Abbrev.} {N}` — e.g., `Bus. & Prof. Code 17200`

### Corpus File Structure

One file per code section group. The file naming and org structure vary by
code type. Update this section when CA citation resolution is implemented.

---

## 4. Formex / EU Regulations (`parser_type = 'formex'`)

**Status: AI/manual resolution only.** No regex patterns currently implemented.

### Known citation forms (for future implementation):
- `Regulation (EU) No {N}/{YYYY}` — e.g., `Regulation (EU) No 575/2013`
- `Article {N} of Directive {YYYY}/{N}/EU`
- `Directive {YYYY}/{N}/EU`

### Corpus File Structure

One file per EU regulation document. Small corpus (currently 3 files).
Update this section when Formex citation resolution is implemented.

---

## 5. Resolution Algorithm Summary

1. **Parse** the citation string using regex (recognized patterns above).
2. **Score** each corpus document using metadata stored in `corpus_documents.metadata`:
   - `org_keys` — the full organizational hierarchy of this document file,
     collected recursively (e.g. `{"title": ["15"], "chapter": ["VII"],
     "subchapter": ["A"], "part": ["772", "774", ...]}`).
     Used for all org-unit-level matching.
   - `content_items` — the actual substantive unit numbers present in this file
     (e.g. `{"section": ["774.1", "774.2", ...], "appendix": ["A"]}`).
     Used for section-level matching (USC).
   - `parser_type` — used to confirm the citation type matches the document type.
3. **Matching by citation type**:
   - USC title → `org_keys["title"]`; USC section → `content_items["section"]`
     with boundary-aware prefix matching (see section 1 above).
   - CFR title → `org_keys["title"]`; CFR part → `org_keys["part"]` (exact).
     A part is an organizational unit; it lives in the org structure, not in
     the substantive unit list.
4. **Scores**: 5 = exact match at finest cited level; 4 = USC section prefix;
   3 = title + parser type match only; 2 = title match, type mismatch;
   1 = bare part hint; 0 = no match.
5. **Narrowing**: if multiple documents score equally, they are `ambiguous`.
6. **AI pass** (optional `--ai` flag): for citations that could not be parsed
   by regex, the AI model receives the citation string and a compact list of
   corpus documents, and identifies the matching document.
7. **Manual review** via CLI tool: for `ambiguous` and `not_in_corpus`
   references that the AI cannot resolve.

---

## 6. Notes and Known Limitations

- **Bare `Part N` citations** are treated as low-confidence (score 1) because
  "Part 39" could refer to CFR part 39, a USC chapter, or an internal appendix.
  These require AI or manual confirmation.
- **Named act citations** (e.g., "the Bank Secrecy Act") are common in USC
  documents but cannot be resolved by title/section number alone. These remain
  `unresolved` until manually linked or resolved via AI.
- **Suffix ambiguity**: `2000e` and `2000e-2` are both section numbers that
  start with `2000`. The boundary rule correctly rejects `2000e` as a match
  for `2000e-2` because after `2000e`, the next character is `-` (separator,
  valid prefix), so `2000e` IS a valid prefix of `2000e-2`. An exact match
  for `2000e-2` would score higher and win.
- **Intra-document cross-references** (references to sections within the same
  document) should NOT appear in the registry. The extraction tool only
  captures `need_ref[type="External"]` entries and `external_reference` in
  `defined_terms`, which are tagged by Stage 2 as pointing outside the document.
