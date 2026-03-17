# Example 3: EU AI Act

**Document**: Regulation (EU) 2024/1689 of the European Parliament and of the Council
**Parser**: Formex 4
**Source**: `source.fmx.xml`
**Processed output**: `processed.json`
**Q&A output**: `question_0001.json`

## Purpose

Non-US jurisdiction and non-US XML format. Multi-article Q&A synthesis across a
113-article regulation, with regulation-wide definitions and a question requiring
interpretation of several interlocking provisions.

## Stage 1: parsing

Formex 4 is the official XML format for EU legislation published in the Official
Journal. Its tag vocabulary differs from USLM: `<ARTICLE>` with `<TI.ART>` (title)
and `<STI.ART>` (subtitle), `<PARAG>` with `<NO.PARAG>` for numbered paragraphs,
`<ALINEA>` for text blocks, and `<LIST TYPE="alpha">` or `<LIST TYPE="ARAB">` for
lettered or numbered lists. Defined terms are wrapped in `<QUOT.START>` and
`<QUOT.END>` elements with Unicode code-point values for the quotation marks.

The source file `source.fmx.xml` (the main document file, 651 KB) contains all 113
articles. Annexes are in separate sibling files. The parser assembles the full document
from these components.

The parsed JSON has three parallel content sections: `content.recitals` (180 recitals,
not operational), `content.articles` (113 articles keyed by number), and
`content.annexes` (13 annexes keyed by Roman numeral).

Article 3 (Definitions) illustrates the Formex structure. Its 65 numbered definitions
occupy a single `<LIST TYPE="ARAB">` element. The parser strips `<QUOT.START>` and
`<QUOT.END>` markers and produces a plain text field with all 65 definitions. At
17,135 characters, Article 3 is among the longest articles in the document; no
paragraph-level breakpoints are recorded because the structure is a flat numbered list.

Source XML (Articles 1 and 3, excerpt): `snippets/stage1_source_art3.xml`
Parsed JSON for Article 3: `snippets/stage1_parsed_art3.json`

## Stage 2: definitions

Article 3 contains 65 definitions, all introduced with "For the purposes of this
Regulation, the following definitions apply." Every definition is regulation-wide in
scope. This contrasts with Example 1 (section-scoped definitions) and Example 2
(article-scoped). Every analyst in Stage 4 receives the same definitional context.

Definitions central to the Q&A question:

"provider": any entity that develops a GPAI model and places it on the market under
its own name, whether for payment or free of charge. An entity that fine-tunes and
then publishes under its own name falls within this definition.

"general-purpose AI model": defined by generality and capability across tasks,
not by training method. Fine-tuned models are not excluded.

"substantial modification": a post-market change that affects compliance with
Chapter III Section 2 requirements or alters intended purpose. Relevant to whether
a fine-tuner of a high-risk system takes on provider obligations under Article 25.

"placing on the market": the first making available of an AI system or GPAI model
in the Union. Applies equally to fine-tuned models released under the fine-tuner's name.

Definitions: `snippets/stage2_definitions.json`

## Stage 3: summaries

113 articles and 13 annexes each receive first- and second-level summaries. For a
document of this size, the first-level summaries are the primary routing tool for
Q&A scoring. The second-level summaries are more precise but the critical distinction
the system needs to make, between, for example, Article 11 (technical documentation for
high-risk AI systems) and Article 53 (documentation for GPAI model providers), is
already visible at the first level.

Summaries for selected articles: `snippets/stage3_summaries.json`

## Stage 4: question answering

**Question**: "Are the obligations of a provider of a general purpose model limited
if they only fine-tune it?"

**Mode**: standard | **Iterations**: 4 | **Articles**: 113 + 13 annexes

### Scoring

| Score | Articles | Annexes |
|-------|----------|---------|
| 3 | 27 | 2 (XI, XII) |
| 2 | 37 | 6 |
| 1 | 18 | 2 |
| 0 | 17 | 3 |

27 of 113 articles score 3. GPAI provider obligations are distributed across many
chapters: scope (Article 2), definitions (Article 3), documentation (Articles 11,
53), obligations upon substantial modification (Article 25), systemic-risk rules
(Articles 51, 55), so a broad scoring distribution is expected.

Scores: `snippets/stage4_scores.json`

### Scratch document

Four iterations produced 14 facts from 12 articles and 3 annexes. The facts cover:
applicability triggers (Articles 1, 2); the "provider" definition and fine-tuner
scope (Article 3); data requirements when fine-tuning is used in high-risk systems
(Article 10); technical documentation obligations (Article 11, Annex IV); high-risk
provider obligations generally (Article 16); substantial modification and provider
status transfer (Article 25); transparency obligations independent of training method
(Article 50); systemic-risk classification and the 10^25 FLOP threshold (Article 51,
Annex XIII); core GPAI provider obligations (Article 53, Annexes XI, XII); open-source
carve-outs (Article 54); and additional systemic-risk obligations (Article 55).

No open questions remained. The one acknowledged uncertainty (whether fine-tuning
compute counts toward the 10^25 FLOP threshold for systemic-risk classification) is
a gap in the regulatory text, not in the analysis.

### Answer

Fine-tuning does not create a reduced obligation track. If a fine-tuner qualifies as
a "provider" and places the resulting model on the market under its own name, the
standard GPAI provider obligations under Article 53 apply in full. Obligations may
expand if the model has systemic risk (Article 55). The only statutory limitation
mechanisms are licensing-based (Article 54, open-source carve-outs) and scope-based
(R&D exclusions in Article 2), not training-method-based.

For high-risk AI systems, fine-tuning that constitutes a "substantial modification"
under Article 25 can shift provider status to the fine-tuner, which would then carry
the full high-risk provider obligations under Article 16.

Full answer: `snippets/stage4_answer.md`

## Features illustrated

| Feature | Location |
|---------|----------|
| Formex 4 XML structure | Stage 1 |
| Recitals, articles, and annexes as distinct content types | Stage 1 |
| Regulation-wide definitions (uniform scope) | Stage 2 |
| Scoring across 126 items | Stage 4, scores |
| 14 facts from 12 articles and 3 annexes | Stage 4, scratch |
| Acknowledged gap in regulatory text | Stage 4, answer |
