# Example 1: USC Title 1, Chapter 1

**Document**: U.S. Code Title 1, Chapter 1: Rules of Construction
**Parser**: USLM
**Source**: `source.xml`
**Processed output**: `processed.json`
**Q&A output**: `question_0001.json`

## Purpose

Core pipeline walkthrough on a small, self-contained document. Covers definition
extraction (elaborative, direct, scoped), two-level summaries, and Q&A scoring on a
document where only two of eight sections are relevant.

## Stage 1: parsing

The USLM parser reads `source.xml` and produces a JSON document with two top-level
sections: organizational metadata (title, chapter) in `document_information`, and
section content in `content.sections`. Sections are keyed by number.

Chapter 1 has 8 sections. Each record includes `unit_title`, `text` (plain text with
USLM tags stripped), `context` (organizational ancestry), `breakpoints` (character
offsets for chunking), and `defined_terms` (populated in Stage 2).

Section 2 is a single sentence. The USLM `identifier` attribute (`/us/usc/t1/s2`)
gives the section number; `<heading>` becomes `unit_title`; `<content>` becomes
`text`; `<sourceCredit>` is discarded.

Source XML for Section 2: `snippets/stage1_source_s2.xml`
Parsed JSON for Section 2: `snippets/stage1_parsed_s2.json`

Section 7 has three lettered subsections, recorded as two breakpoints at character
offsets 467 and 642 (both level 1). If Stage 3 needed to chunk this section it would
split at one of these points. In practice Section 7 is short enough that chunking
does not trigger.

Source XML for Section 5: `snippets/stage1_source_s5.xml`
Parsed JSON for Section 7 (with breakpoints): `snippets/stage1_parsed_s7.json`

## Stage 2: definitions

Sections 1 through 6 contain elaborative rules embedded in their text: "the words
'person' and 'whoever' include corporations," "the word 'county' includes a parish."
These are `def_kind: "elaboration"` entries: the term's existing meaning is broadened
rather than replaced. Because the section text itself is the definition, they do not
produce standalone `defined_terms` records at the section level.

Sections 7 and 8 contain traditional definition blocks with explicit scope language.
Section 7 defines "State" with "In this section": the definition applies only within
Section 7. Section 8 defines "born alive" with "As used in this section." Both carry
`scope_processed: true` confirming the scope was resolved during Stage 2.

When Stage 3 summarizes Section 7, it receives the "State" definition as context. No
other section receives it.

Definitions: `snippets/stage2_definitions.json`

## Stage 3: summaries

All 8 sections receive first-level (`summary_1`) and second-level (`summary_2`)
summaries. For most sections in this chapter the two levels are similar in length
because the sections are short and self-contained; the gap is more pronounced in
sections with many definitions or heavy cross-references.

Section 5 ("Company or association as including successors and assigns") is the section
that answers the Q&A question:

```
Text (72 words):
  The word "company" or "association", when used in reference to a corporation, shall
  be deemed to embrace the words "successors and assigns of such company or
  association"...

summary_1:
  This provision treats 'company' or 'association' as including its successors and
  assigns, so that contracts and duties bind the entity's legal continuations as if
  they were explicitly named...

summary_2:
  This provision interprets the terms "company" or "association" to include the
  organization's successors and assigns. Consequently, all rights and obligations
  that apply to the company or association also bind and pass to its successors and
  assigns, as if the words "successors and assigns" were expressly stated...
```

After all 8 section summaries are produced, a chapter-level summary is generated from
them. That summary is what gets scored during Q&A to determine whether the chapter is
relevant to a question.

Summaries: `snippets/stage3_summaries.json`

## Stage 4: question answering

**Question**: "If a company subject to a federal reporting obligation merges into
another company, does the surviving company inherit that obligation?"

**Mode**: standard | **Iterations**: 2 | **Sections analyzed**: 2 of 8

### Scoring

| Section | Title | Score |
|---------|-------|-------|
| 1 | Words denoting number, gender... | 2 |
| 2 | County as including parish | 0 |
| 3 | Vessel as including all watercraft | 0 |
| 4 | Vehicle as including land transport | 0 |
| 5 | Company/association includes successors | 3 |
| 6 | Products of American fisheries | 0 |
| 7 | Marriage | 0 |
| 8 | Born-alive infant | 0 |

Six of eight sections score 0. Section 5 scores 3 (directly answers the merger
question). Section 1 scores 2 (establishes that entity-level obligations apply to
corporations generally).

Scores: `snippets/stage4_scores.json`

### Scratch document

Two analysis iterations produce two facts and no questions or section requests:

```json
{
  "fact_001": {
    "content": "When the terms 'company' or 'association' are used in reference to
                a corporation, they are deemed to include the 'successors and assigns'
                of that company/association as if those words were expressly included.",
    "source": ["Section 5"]
  },
  "fact_002": {
    "content": "Interpretive rule: the terms 'person' and 'whoever' include
                corporations, companies, associations, firms, partnerships, societies,
                and joint stock companies.",
    "source": ["Section 1"]
  }
}
```

Scratch document: `snippets/stage4_scratch.json`

### Answer

Yes, the surviving company inherits the obligation, to the extent the obligation is
imposed on a "company" or "association." Section 5 deems those terms to include
successors and assigns. The answer hedges on one point: whether the specific reporting
obligation uses the word "company" or "association" (vs. "person" or some other
framing) cannot be determined from Chapter 1 alone.

Full answer: `snippets/stage4_answer.md`

## Features illustrated

| Feature | Location |
|---------|----------|
| USLM XML structure and parsing | Stage 1 |
| Breakpoints in parsed JSON | Stage 1, Section 7 |
| Elaborative definitions (text-embedded) | Stage 2, Sections 1-6 |
| Direct scoped definitions | Stage 2, Sections 7-8 |
| Two-level summaries | Stage 3, Section 5 |
| Chapter-level organizational summary | Stage 3 |
| Focused scoring (2 of 8 relevant) | Stage 4, scores |
| Minimal scratch document | Stage 4, scratch |
