# Example 2: CA Government Code - Bagley-Keene Open Meeting Act

**Document**: California Government Code §§ 11120-11132
**Parser**: CA HTML
**Source**: `source.html`
**Processed output**: `processed.json`
**Q&A output**: `question_0001.json`

## Purpose

HTML parsing, article-level scoped definitions, and Q&A on a 43-section document
where the scoring step does meaningful filtering. The scratch document includes an
open question referencing a section outside the analyzed file.

## Stage 1: parsing

The CA parser handles HTML from leginfo.legislature.ca.gov. Section boundaries are
identified by `<h6>` elements containing linked section numbers. The organizational
hierarchy (code, title, division, part, chapter, article) is inferred from heading
structure and URL metadata in the anchor links. Navigation chrome, stylesheets, and
JavaScript are stripped.

The parsed document places organizational metadata in `document_information.organization`
and section content in `content.sections`. Article 9 has 43 sections keyed by number,
including decimal variants like `11126.4.5`.

Section 11120 (legislative purpose) has no subsections and no breakpoints.

Source HTML for Section 11120: `snippets/stage1_source_s11120.html`

Section 11124 (the sign-in prohibition) has three lettered subsections, producing two
breakpoints at character offsets 244 and 675. The `context` field records the full
organizational ancestry for all 43 sections in Article 9.

Source HTML for Section 11124: `snippets/stage1_source_s11124.html`
Parsed JSON for Section 11124: `snippets/stage1_parsed_s11124.json`

## Stage 2: definitions

"State body" is defined at the article level by Sections 11121 (positive definition)
and 11121.1 (carve-outs). Stage 2 records this as an article-scoped definition and
provides it as context to all 43 sections during Stage 3 summarization and Stage 4
analysis. The State Bar of California is expressly included as a state body, operative
April 1, 2016, a fact that bears directly on the Q&A question.

Several sections define terms locally. Section 11123.2 defines "Teleconference",
"Teleconference location", and "Remote location", all scoped to that section. The same
term "Teleconference" is independently defined in Sections 11123.5 and 11123.5_dup
with slightly different language. The conflict detection system treats these as
compatible: each is scoped to its own section and the definitions are functionally
parallel.

Definitions: `snippets/stage2_definitions.json`

## Stage 3: summaries

All 43 sections receive first- and second-level summaries. Section 11120's summaries
are close in length because the section is short and fully self-contained. Section
11121 is longer (five breakpoints, 1,318 characters) and its second-level summary
names the State Bar specifically, drawing on the cross-reference to Section 11121.1
provided as context.

Section 11124 is the section that directly answers the question. Both summary levels
accurately capture the prohibition and the voluntary-list exception; the second-level
summary adds the pseudonym provision from subsection (c) more explicitly.

The article-level summary, generated from all 43 section summaries, abstracts across
the full framework without retaining procedural detail from any individual section.

Summaries: `snippets/stage3_summaries.json`

## Stage 4: question answering

**Question**: "Can the State Bar of California hold public meetings that require
attendees to sign in?"

**Mode**: standard | **Iterations**: 3 | **Sections**: 43

### Scoring

| Score | Count | Notable sections |
|-------|-------|-----------------|
| 3 | 6 | 11121 (state body), 11123 (open meetings), 11124 (sign-in), 11131 (venue rules) |
| 2 | 6 | 11120 (purpose), 11125 (notice), 11130.3, 11132 |
| 1 | 15 | Teleconference subprovisions, closed-session mechanics |
| 0 | 16 | Specialized procedures, granular carve-outs |

The spread across all four score levels contrasts with Example 1, where only 2 of 8
sections scored above 0. Here the question is about open-meeting rules in a document
whose sole subject is open-meeting rules, so a broader relevance distribution is expected.

Scores: `snippets/stage4_scores.json`

### Scratch document

Three iterations produce three facts, one open question, and one section request:

Facts from Sections 11121, 11123, and 11124 establish that the State Bar is a state
body, that its meetings must be open and public, and that mandatory sign-in as a
condition of attendance is prohibited (with the voluntary-list and pseudonym exceptions).

The analyst for Section 11131 noted that the nondiscrimination rule incorporates
"any characteristic listed or defined in Section 11135," but Section 11135 is not
included in the analyzed document. A `request_relevant_section` was filed for
Section 11135. The system could not retrieve it; the question remains open and the
final answer notes the limitation.

Scratch document: `snippets/stage4_scratch.json`

### Answer

No, mandatory sign-in as a condition of attendance violates Section 11124. The State
Bar is a state body under Section 11121, so the prohibition applies to it. Attendance
lists may be offered only with a clear statement that participation is voluntary.
Teleconference login requirements are permitted as long as participants may use
pseudonyms.

The specific protected characteristics referenced by Section 11131 (from Section 11135,
outside scope) could not be confirmed, but this does not affect the answer to the
question asked.

Full answer: `snippets/stage4_answer.md`

## Features illustrated

| Feature | Location |
|---------|----------|
| HTML source parsing | Stage 1 |
| Article-scoped definition ("state body") | Stage 2 |
| Same term defined in multiple sections with compatible scopes | Stage 2 |
| Broader scoring spread (43 sections) | Stage 4, scores |
| Open question referencing an out-of-scope section | Stage 4, scratch |
| Section request filed and unresolvable | Stage 4, scratch |
