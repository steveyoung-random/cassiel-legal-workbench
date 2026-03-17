# A processing pipeline for AI-assisted legal document analysis

## How we got here

While working through the requirements of the then-new EU AI Act almost two years ago, I decided to test whether feeding the full 140+ pages of the law into ChatGPT would yield a reliable answer to a specific question.  It did not.  ChatGPT pointed me to sections that had nothing to do with the issue at hand, and persisted even after I pointed out the problem.  A fuller account of that experiment, and what it suggests about using AI tools in legal work, is at [Getting AI to reliably analyze statutes and regulations](AI_AND_LEGAL_DOCUMENTS.md).

The interesting question was why.  Testing texts of different lengths, I found that long before the context window for an LLM is filled, it is likely to lose focus and fail to answer accurately.[^1] This resembles what happens when a human reads a very long document: we do not retain every word, and a specific question about content read earlier will often require going back to look.  If an LLM can only keep a limited amount of text in focus at one time, it faces a familiar limitation.  The question, then, was whether an LLM could be stepped through a lengthy legal document in a way that more closely reflects how a careful reader works through one.  This project is the result.

## What the tool does

I realized that, rather than using the chat interface for these models, I would need to use the APIs made available by the AI platforms.  An LLM API is simply a mechanism that allows people to write programs that make calls into the LLM and get a response.  Using the APIs (currently for OpenAI's GPT models and Anthropic's Claude models), my program is able to take portions of a legal document, along with some instructions, and get a response that is, in practice, very reliable.  By working through a long legal document (it currently handles laws and regulations, not contracts or other types of documents), the system is able to build up a set of summaries of the document, at different levels of specificity and based on different portions of the overall document.  Because the amount of input provided to the LLM is fairly limited, and focused on limited topics, the system is designed to constrain the conditions under which hallucination can occur.  Based on working through a substantial amount of output and judging the results, I have not observed hallucinations in its output.  Where the system lacks enough information to answer a question, it records that limitation explicitly rather than fabricating an answer, as illustrated in the question-answering section below.

Unlike general text, laws and regulations are more easily divided into smaller pieces that are understandable, to some degree, on their own.  A statute may be made up of dozens or hundreds of sections or articles, each addressing a topic (or building on the topics that came before).  I refer to these as "substantive units" of the document.  The structures of laws and regulations are usually easily parsed into a format that allows for them to be managed independently, while maintaining information about their overall context within the larger document and its organizational structure.  After starting with a fairly limited summary of each of these substantive units of the document (similar to the high level sense of a section you might retain in your memory after the first time you read it), the system goes back through and uses what it had learned in the first pass to build up more meaningful context that is relevant to a substantive unit before asking an LLM to produce a more in-depth summary of that section.  This context may include definitions that the section relies on, as well as summaries already produced of other sections that the section being examined refers to.

The in-depth summaries of the sections in a particular organizational unit of the document, such as a chapter, can then be used to produce a summary for that chapter.  The chapters may be grouped together in a "part" or a "title" or other organizational structure, and the same process can be used to produce summaries at each such level.

These tiers of summaries of the document are then useful in guiding us towards an answer when we have a question about the document.  For example, an LLM can be given the question that you have, and go through each summary at each organizational level to find the sections of the document that are most likely to be relevant to the question.  In one version of the project, the end user can be provided with these sections, each drawn directly from the text of the original document.  Because the output is directly from the text of the law or regulation itself, and not output from an LLM, we don't encounter hallucinations.  The system uses the summaries to guide selection of the most relevant portions of the document, not to answer questions about the document.

A more ambitious version, which is already working reasonably well, uses the summaries to guide a set of LLM "analysts", each tasked with reviewing a single section of the document against the question at hand.  Each analyst contributes to a shared scratch document: they add factual statements drawn from their section, generate questions for other analysts to answer, and answer questions others have asked.  After several rounds of this, a separate LLM pass synthesizes the accumulated facts into a proposed answer, with citations to the relevant portions of the source text.  The detailed mechanics are described in the question-answering section below.

## The details of the project

The system is built around four sequential processing stages: parsing the source document into a common structured format; extracting and resolving the defined terms in the document; generating two-level summaries for each substantive unit and each organizational level; and, given a user question, iteratively assembling a proposed answer through multi-analyst analysis.  Each stage's output is the next stage's input.  Throughout, the design errs on the side of explicit failure: if a document has structural problems that a stage cannot handle, processing stops with an error rather than continuing and producing output that may be silently incomplete.  The remainder of this section describes each stage in turn.

This project is built on Python.  For any tool that is processing large amounts of text, and interfacing with LLM APIs, Python is a natural technology to use.  Along with Python, the project makes significant use of JSON, which is a good way of storing text-based information in a way that is easily viewable but also reflects the data structures of the program.

### Parsing

The first step in processing any particular document is parsing a version of that document into a common JSON schema.  Wherever possible, I look for XML versions of the statutes and regulations that I am working with, but sometimes I have to use HTML.

To give a sense of the transformation, here is Section 2 of USC Title 1 as it appears in the source XML.  It is a short, self-contained section that gives an elaborative definition:

```xml
<section identifier="/us/usc/t1/s2">
  <num value="2">§ 2. </num>
  <heading>"County" as including "parish", and so forth</heading>
  <content>The word "county" includes a parish, or any other equivalent subdivision
  of a State or Territory of the United States.</content>
  <sourceCredit>(July 30, 1947, ch. 388, 61 Stat. 633.)</sourceCredit>
</section>
```

*Full file: [`stage1_source_s2.xml`](../examples/01_usc_title1_ch1/snippets/stage1_source_s2.xml)*

The parser strips the source-specific XML tags and produces a JSON record that all subsequent stages work from:

```json
{
  "2": {
    "unit_title": "\"County\" as including \"parish\", and so forth",
    "text": "The word \"county\" includes a parish, or any other equivalent subdivision
             of a State or Territory of the United States.\n",
    "context": [
      { "title": "1" },
      { "chapter": "1" }
    ],
    "breakpoints": [],
    "notes": {},
    "defined_terms": []
  }
}
```

*Full file: [`stage1_parsed_s2.json`](../examples/01_usc_title1_ch1/snippets/stage1_parsed_s2.json)*

The `notes` field holds end notes or footnotes that accompany a substantive unit's body text in the source document.  The `context` field records the location of the substantive unit within the organizational hierarchy (here, Section 2 sits within Chapter 1 of Title 1).

By researching the details of the XML or HTML tags being used for an entire corpus of documents, I can create a specific parser for the entire corpus, which transforms documents into the project's common JSON format.  Parsers follow a common `ParserAdapter` interface, so adding support for a new document type requires only implementing that interface and registering the new parser (the rest of the system is unaffected).  The interface and registration process are documented in [`ADDING_NEW_PARSER.md`](ADDING_NEW_PARSER.md).  Because the task of creating each parser is time consuming, I have looked for bodies of laws and regulations that are very large, so that the work I have done can be leveraged as much as possible.  To date, I have created parsers for the EU Directives and Regulations, the US Code (USC), the US Code of Federal Regulations (CFR), and California statutes.  The parsing is based on specific types of operational units, which might be called sections, articles, paragraphs, appendices, or other things.  These are often grouped together into organizational frameworks (parts, subparts, chapters, subchapters, titles, subtitles, etc.).  These details, which vary greatly from one document type to another, are specified in the JSON document so that later code, which knows nothing of the specifics of any document type, can effectively process it.

### Definitions

The second stage in processing a document is focused on finding and refining definitions in the document.  The first version of the project did not process definitions at an early stage, and it led to problems in properly summarizing documents.  As any lawyer understands, the actual meaning of any law or regulation often turns on the exact definition of the terms used in the text.  This stage of the document processing involves passing each substantive portion of the document (assume here a section) to an LLM to have it give back a set of terms that it believes are defined in the section.  Then, for each term that has been provided, there is a call to the LLM to provide the definition of that term from the provided text.

As this project moved forward, it became clear that definitions in a legal document come in many forms, and we need to be able to handle each type.  For instance, there are direct, straightforward definitions of the form, "The term 'risk' means the combination of the probability of an occurrence of harm and the severity of that harm."  Sometimes a definition is indirect, and it is of the form, "'Clinic' has the same meaning as defined in Section 1200."  The processed record for that kind of definition looks like this:

```json
{
  "term": "Clinic",
  "value": "\"Clinic\" has the same meaning as defined in Section 1200.",
  "indirect": "Section 1200",
  "scope": "For purposes of this section",
  "def_kind": "elaboration",
  "source_type": "section",
  "source_number": "1339.75",
  "scope_processed": true,
  "quality_checked": true
}
```

The `indirect` field names the target; the Stage 2 resolver follows that reference and imports the full text of the definition from Section 1200 when building context for § 1339.75.

A note on field naming: `def_kind` records whether a definition is "direct" (which states the meaning in full) or an "elaboration" (which adds to or qualifies an existing definition).  The `indirect` field is a separate, orthogonal attribute that records whether the definition refers the reader elsewhere for its content.  The two are independent: a definition can be an elaboration while being fully stated in the local text, or it can be a direct definition expressed entirely by reference to another section.  This naming overlap is a known issue and will be addressed in a future version.

A definition may be elaborative in nature, such as by saying, "The word 'county' includes a parish, or any other equivalent subdivision of a State or Territory of the United States," which is the formulation used in Section 2 of USC Title 1.  A definition may come with a scope of applicability, such as "in this chapter", or "for purposes of this Directive."  Here is a scoped definition from Section 7 of USC Title 1, where the term "State" is defined only within that section:

```json
{
  "term": "State",
  "value": "In this section, the term \"State\" means a State, the District of
            Columbia, the Commonwealth of Puerto Rico, or any other territory
            or possession of the United States.",
  "indirect": "",
  "scope": "In this section",
  "def_kind": "direct",
  "source_type": "section",
  "source_number": "7",
  "scope_processed": true,
  "quality_checked": true
}
```

*Full file: [`stage2_definitions.json`](../examples/01_usc_title1_ch1/snippets/stage2_definitions.json)*

The definition system finds definitions, categorizes them, and records the meaning and scope of each.  As a result, it is not unusual for a particular word to have, for purposes of a particular section, a meaning that is given in another section with applicability to the entire statute, plus an elaboration on that definition given in yet another section with applicability to a particular subchapter of the statute.  This system can provide, for a particular portion of a document, the list of all defined terms applicable to it.

One mechanism added over time is a process that iterates through all defined terms and has the LLM assess each one: is this a good definition?  Reviewing a definition in isolation, without the surrounding text, often makes it easier to evaluate clearly.  This also enables a cost structure where a less capable model handles the initial extraction pass, and a more capable model is only engaged when a quality problem is detected.  The quality check asks the LLM to assess whether the definition text genuinely defines the given term (sometimes text that refers prominently to a term can be mistakenly extracted as though it were defining that term, when it is not).  The result is that most definitions are handled cheaply and well, with the more expensive model applied selectively.

There is also a system in this stage that detects potential conflicts of definitions.  The term "teleconference" in California Government Code § 11120 illustrates how these arise: the same term appears with different language in different subdivisions of the section.

```
Term: "teleconference"

Definition 1 (section-wide):
  scope: "For purposes of this section"
  kind: direct
  value: '"Teleconference" means a meeting of a state body, the members of which
          are at different locations, connected by electronic means...'

Definition 2 (subdivision-specific):
  scope: "For the purposes of this subdivision"
  kind: elaboration
  value: 'For the purposes of this subdivision, "teleconference" means a meeting
          of a state body, the members of which are at different locations...'
```

*Full file: [`stage2_definitions.json`](../examples/02_ca_gov_11120/snippets/stage2_definitions.json)*

When the Stage 3 summarizer processes a specific subdivision, it receives the subdivision-scoped definition; when processing the section as a whole, it receives the broader one.

Sometimes, a term may be defined in two places, and neither indicates what the scope is for the definition.  If the different definitions are equivalent, this is a minor issue, but sometimes the definitions differ.  In those cases, each conflicting definition is moved deeper in the organizational tree, toward the substantive unit where it was found, until it no longer overlaps with the other.  This works as long as the conflicting definitions arise from different substantive units (a conflict arising within a single unit cannot be resolved by scope assignment).  The result is that, for cases of a hard conflict where explicit scopes are given, the version of the definition applied in the context of a particular unit is based on which source is organizationally closest to that unit.

### Summaries

The third stage generates summaries.  There is an initial pass that creates relatively short summaries of each substantive unit of text, which summaries are generally no longer than three sentences each (they can grow longer for text which is so long that it is processed in chunks).

The following is the first-level summary for California Elections Code § 20000, which prohibits distribution of AI-generated deepfakes in election contexts.  It was generated with only the table of contents and preceding section summaries as context, without the definitions:

```
CA ELEC § 20000, summary_1:

  Prohibits distributing materially deceptive election content, including deepfakes,
  during a defined window around elections and imposes explicit labeling and disclosure
  requirements to reveal inauthentic content. It permits exemptions for satire and bona
  fide news, and provides injunctive relief, damages, and attorney's fees for prevailing
  plaintiffs, with a clear-and-convincing standard. Definitions of key terms such as
  deepfake, election communication, and advertisement guide the scope.
```

The model correctly identifies that several defined terms govern the scope of the section, but without those definitions it can only gesture at them.  That gap is addressed at the second summarization level.

For these summaries, there is context that is put together to help the LLM understand the section of text that it is focused on.  The first part of this context is a table of contents, which maps the substantive units of text (e.g. sections, articles, appendices, etc.) to the organizational layout of the document (e.g. parts, chapters, etc.).  This is useful as it gives the LLM a bit of a roadmap to understand where the text it is analyzing fits into the larger picture.  The next piece of context is a compilation of summaries that have preceded the section of text being examined.  This compilation of summaries is chosen to be the most likely to be useful without having the length become so long that the LLM loses focus.  The summaries that are provided for context are those that come earlier in document order within the same organizational level (e.g. the same part or chapter) as the text being analyzed.

A chunking process keeps any individual substantive unit short enough for reliable analysis.  In order to know where a long set of text can be broken without losing coherence (such as would happen if a section were broken partway through a sentence), potential breakpoints are recorded during the parsing process.  The breakpoints are recorded as offsets within the text of the substantive unit, and each is accompanied by a number indicating the level of the breakpoint.

Section 7 of USC Title 1 ("Marriage"), which has three subsections, yields two breakpoints.  Each entry is a `[character_offset, level]` pair marking a point in the text where a split may be made:

```json
{
  "7": {
    "unit_title": "Marriage",
    "text": "    (a) For the purposes of any Federal law, rule, or regulation in which
             marital status is a factor, an individual shall be considered married
             if that individual's marriage is between 2 individuals and is valid in
             the State where the marriage was entered into ...",
    "breakpoints": [
      [467, 1],
      [642, 1]
    ],
    "defined_terms": [
      {
        "term": "State",
        "value": "In this section, the term \"State\" means a State, the District of
                  Columbia, the Commonwealth of Puerto Rico, or any other territory
                  or possession of the United States.",
        "scope": "In this section",
        "def_kind": "direct"
      }
    ]
  }
}
```

*Full file: [`stage1_parsed_s7.json`](../examples/01_usc_title1_ch1/snippets/stage1_parsed_s7.json)*

The `defined_terms` array here also shows that the Stage 1 parser captures definitions found within a section.  In this case, "State" is defined in the section text itself and recorded inline.

If a substantive unit of text is broken into a first order of units such as: (a), (b), (c), etc., then each of these would be followed by a breakpoint at level 1.  If the text within level (a) is further divided into units of: (i), (ii), (iii), etc., then each of these units is followed by a breakpoint at level 2.  In deciding how to split a long section, the preference is to split at the lowest breakpoint level possible.  So, if all level 1 units can be kept intact, that is done.  If a level 1 unit is too long and needs to be split, then we look for level 2 (then 3, 4, etc.) breakpoints that allow us to keep together as much text as we can that naturally goes together.

For this first level of summary, the LLM is also asked to provide a list of definitions needed to interpret the section, as well as a list of references to other provisions (whether in this same document or another).  The references to other provisions will sometimes be presented as a range (e.g. "sections 4 to 29"), which need to be reduced to specific organizational or substantive units.

There is then a second level of summarization, where the summaries are expected to be longer (up to five paragraphs for most sections of text, but longer is permitted in some situations), and are based on more context than is available for the first level of summaries.  This is the benefit of the two-pass design: the first-level pass identifies which definitions and cross-references a section depends on, and the second-level pass is provided with those definitions and the first-level summaries of any referenced provisions, allowing it to produce a substantially better summary.

The second-level summary for CA ELEC § 20000 was generated with the six defined terms the first-level pass identified as necessary, provided as additional context:

```
Definitions provided for second-level summarization:

  "Deepfake": audio or visual media that is digitally created or modified such that
    it would falsely appear to a reasonable person to be an authentic record of the
    actual speech or conduct of the individual depicted.

  "Materially deceptive content": audio or visual media intentionally digitally
    created or modified (including deepfakes) such that it would falsely appear to a
    reasonable person to be an authentic record of the depicted content.

  "Malice": the person distributed the content knowing it was false or with reckless
    disregard for the truth.

  "Election communication": any general or public communication not covered by
    "advertisement" that is broadcast, distributed through the internet, or disseminated
    by print media.

  "Elections official": any person charged with holding or conducting an election,
    a canvass, or a related administrative duty.

  "Committee": a committee as defined in Section 82013 of the Government Code.
```

```
CA ELEC § 20000, summary_2 (5 paragraphs):

  California asserts a need to guard elections from AI-generated disinformation and
  deepfakes by temporarily restricting the distribution of clearly deceptive content
  around elections, and by requiring disclosures to inform viewers about inauthentic
  content. The rule targets both advertisements and other election communications, and
  defines "materially deceptive content" to include deepfakes and other digital
  manipulations that would falsely appear authentic, while excluding minor,
  non-substantive changes.

  During the time window surrounding an election (and a slightly extended period for
  content involving elections officials and voting equipment), it is unlawful to
  knowingly distribute materially deceptive content with malice that portrays a
  candidate, an elections official, an elected official, or voting-related property
  in a way that could harm reputations or undermine confidence in election results.
  The prohibition applies to content that is reasonably likely to influence electoral
  outcomes or trust in the process.

  There are targeted exceptions and labeling requirements. A candidate may portray
  themselves as having done or said something they did not, so long as a disclosure
  states "This [image/audio/video] has been manipulated," with size and placement very
  visible. Satire or parody is exempt if accompanied by a similar disclosure. Content
  removals or republishes must retain the required disclosures; failing to do so is
  itself evidence of intent to distribute deceptive content.

  The act provides remedies in civil actions: injunctive or other equitable relief,
  damages, and attorney's fees. The plaintiff bears the burden of proving violation by
  clear and convincing evidence. There are carve-outs for bona fide news broadcasts
  and traditional publications, along with protections for interactive services under
  federal law.

  Key terms are defined to guide application, including "advertisement," "election
  communication," "deepfake," "materially deceptive content," "elections official,"
  "malice," and "recipient," with a requirement that disclosures appear in the
  advertisement's language.
```

The contrast with the first-level summary is visible.  The second-level summary can state the timing window, the specific prohibition, and the civil action standard, all grounded in the definition text, where the first-level summary could only note that "definitions of key terms guide the scope."

For these, the context is made up of the same table of contents (as a roadmap), all defined terms identified as necessary in the first-level pass, and first level summaries of those other provisions in this document that are referred to in the text.  The goal is to give the LLM what is necessary for interpretation, without providing everything available.

The same process generates summaries at each organizational level (e.g. parts, chapters, etc.) based on the summaries of all the substantive units and sub-levels directly under it.  If any child unit has not yet been summarized, the parent-level summary is not generated.

The following is the summary for Chapter 1 of USC Title 1 ("Rules of Construction"), generated from the eight section-level summaries.  This is the summary that would be reviewed during question-answering to determine whether the chapter as a whole is relevant to a given question:

```
Chapter 1, "RULES OF CONSTRUCTION", summary_1:

  Taken together, these provisions broaden and harmonize federal statutory
  interpretation and scope by expanding who and what is covered, how terms are read,
  and where authority applies. They extend defined terms to include individuals and
  entities such as corporations, broaden instruments like signatures and writings,
  and treat masculine terms as inclusive of women while recognizing present-tense
  terms as covering the future. They expand geographic and jurisdictional concepts
  by including parishes as counties and by establishing federal recognition rules for
  marriages across states and territories, with limits tied to the law in effect at
  the time of the marriage. They widen the definitions of vessels and vehicles to
  encompass all waterborne and land transportation means and ensure contracts and
  duties bind successors and assigns of companies.
```

*Full file: [`stage3_summaries.json`](../examples/01_usc_title1_ch1/snippets/stage3_summaries.json)*

Some loss of detail at higher levels of summarization is expected.  A chapter-level summary is most useful as an abstraction, not as a restatement of everything below it.

### Question answering

After the third stage of processing, the resulting JSON document includes the full text of the document, an organizational map that shows where the substantive text units (e.g. sections or articles) can be found, definitions that are at various organizational levels or attached to substantive units, and summaries of all substantive units and organizational levels of the document.  As such, the document is ready to be used in a process of generating answers to questions that a user may have about the document.

The first step in answering a user question is scoring the relative importance of various organizational levels and substantive units to the question.  Thoroughness is controlled by named configuration options (such as "quick scan", "standard", "thorough", and "maximum confidence") that set the values of the relevant variables and flags.  Users can define additional named options in the configuration file to tune the tradeoffs for a particular use case.  For a high degree of thoroughness, all substantive units are reviewed against the question.  For a lower degree of thoroughness, only substantive units within organizational units (like chapters or parts) that are deemed relevant are scored.  Scoring assigns a relevance number from 0 (not relevant) to 3 (clearly important) for each scored unit (scores of 1 indicate unlikely relevance and scores of 2 indicate likely relevance).  For efficiency, this scoring can be batched, with multiple substantive units reviewed in a single API call; whether scoring is done one unit at a time or in batches is configurable.  By default, scoring is conducted using the first level of summaries, but it can be conducted using the more detailed second level of summaries.  If a high degree of thoroughness is not indicated, then the summaries of the organizational levels of the document are scored, and a process is followed to determine which branches of the organizational tree to continue examining and which to ignore.

The reason for conducting any analysis at less than maximum thoroughness is cost and speed.  By pruning the search tree in cases where an organizational unit in a document appears (based on its summaries) to be irrelevant, the number and size of API calls is reduced, which has a major impact on cost and time to completion.  Similarly, using the shorter first level summaries, rather than the more lengthy second level summaries, also reduces API costs and speeds up the process.

Once the scoring process has completed, we will have a list of substantive sections of the document that appear to be relevant.

For the question "If a company subject to a federal reporting obligation merges into another company, does the surviving company inherit that obligation?", applied to USC Title 1, Chapter 1, only 2 of the 8 sections receive nonzero scores:

```
Scores (section -> relevance score, 0-3):
  Section 1  (Words denoting number, gender...)         -> 2
  Section 2  (County as including parish)               -> 0
  Section 3  (Vessel as including all watercraft)       -> 0
  Section 4  (Vehicle as including land transport)      -> 0
  Section 5  (Company/association includes successors)  -> 3  (analyzed)
  Section 6  (Products of American fisheries)           -> 0
  Section 7  (Marriage)                                 -> 0
  Section 8  (Born-alive infant)                        -> 0
```

*Full file: [`stage4_scores.json`](../examples/01_usc_title1_ch1/snippets/stage4_scores.json)*

The six sections about vessels, vehicles, fisheries, marriage, and similar topics score 0 and are not analyzed further.  The system spends its analysis budget on the two sections where the question has purchase.

At this point it is possible to stop and provide the user with the actual document text that has been deemed to be relevant to the question at hand.  Generally, though, we want the system to generate a proposed answer, so a set of iterative analyses are conducted.

The iterative rounds of analysis are conducted by giving each of a set of LLM "analysts" the full text of a single substantive unit, along with the necessary context (table of contents, necessary definitions, summaries from other substantive units the text refers to) and a "scratch document" that all analysts are contributing to, and having them do a few things: (i) they provide the scratch document with facts from their text that appear to be relevant to the question, (ii) they add to the scratch document requests for more detail about specific elements of the document or questions that may be answered by other analysts, and (iii) they provide answers to questions that other analysts added to the scratch document, if they can answer based on their text.  Everything that an analyst adds to the scratch document is automatically tagged with the source, being the portion of the document that analyst was reviewing.  The iterative rounds continue until either a fixed maximum number of rounds has been reached, or convergence is achieved (meaning a full round completed without any new facts, questions, or requests being added to the scratch document).

For the same USC Title 1 question, after two iterations, the scratch document contains two facts, one from each of the two scored sections:

```json
{
  "fact": {
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
  },
  "question": {},
  "requests": {}
}
```

*Full file: [`stage4_scratch.json`](../examples/01_usc_title1_ch1/snippets/stage4_scratch.json)*

The compactness here is a property of the document and the question, not a limit of the system.  Chapter 1 of Title 1 is eight short sections on rules of construction; the question has a narrow, specific answer.  Larger documents with more complex questions produce longer scratch documents, but the structure is the same.

Analysts may also contribute to the scratch document in ways beyond adding facts.  They may ask questions they believe other analysts can answer, or request that additional sections be retrieved and added to the analysis scope.  The following scratch document excerpt is from a question run against California Government Code § 11120 (the Bagley-Keene Open Meeting Act).  One analyst, working on Section 11131, identified that the section references Section 11135's list of protected characteristics (which is outside the analyzed document) and both asked an open question and filed a section request:

```json
{
  "fact": {
    "fact_001": {
      "content": "The State Bar of California is expressly included as a 'state body'
                  for purposes of this open-meeting article (operative April 1, 2016).",
      "source": ["Section 11121"]
    },
    "fact_002": {
      "content": "All meetings of a state body must be open and public, and all persons
                  must be permitted to attend any meeting except as otherwise provided
                  in the article.",
      "source": ["Section 11123"]
    },
    "fact_003": {
      "content": "A state body may not require any person, as a condition of attending
                  a meeting, to register their name, provide other information, complete
                  a questionnaire, or fulfill any other condition precedent to attendance.
                  However, an attendance list may be posted or circulated only if it
                  clearly states participation is voluntary and that all persons may
                  attend regardless of whether they sign. For teleconferenced meetings,
                  the attendee must be permitted to use a pseudonym.",
      "source": ["Section 11124"]
    }
  },
  "question": {
    "question_002": {
      "content": "What characteristics are 'listed or defined' in Section 11135
                  (as incorporated by Section 11131's nondiscrimination rule on
                  facility admittance)?",
      "importance": 1,
      "source": ["Section 11131"],
      "answers": {
        "answer_001": {
          "content": "Section 11131 references 'any characteristic listed or defined
                      in Section 11135' but does not itself list those characteristics;
                      the specifics depend on Section 11135.",
          "source": ["Section 11131"]
        }
      }
    }
  },
  "requests": {
    "request_002": {
      "action": "request_relevant_section",
      "target_type": "Section",
      "target_number": "11135",
      "source": ["Section 11131"]
    }
  }
}
```

*Full file: [`stage4_scratch.json`](../examples/02_ca_gov_11120/snippets/stage4_scratch.json)*

The question remains open because Section 11135 is not part of the analyzed document.  The system records the limitation rather than fabricating an answer.

Where the text being analyzed references provisions in a separate document, the system resolves those references through a registry of processed documents.  Relevant definitions and summaries from the external document are provided to the analyst alongside the primary context, so that cross-document references are handled in the same way as within-document ones.

After the iterative rounds are complete, an LLM is given the content of the scratch document and asked to deduplicate it and remove information that is not relevant to answering the question.  It is also asked to generate an initial answer to the question, based on the content of the scratch document, which at this point should have all relevant facts about the relevant portions of the document, along with detailed records of where in the document each fact is found.  At this point, the scratch document contains the relevant information needed to answer the question.  The proposed answer goes through further rounds of potential refinement, which ensure that factual assertions are based on citations to the document and that the answer does not make reference to internal parts of the process (such as the analysts and the scratch document).

The final answer for the USC Title 1 merger question, generated from the two facts in the scratch document:

```
Question: If a company subject to a federal reporting obligation merges into another
          company, does the surviving company inherit that obligation?

Answer:
Yes, based on the provided text, the surviving (successor) company would be treated
as inheriting the federal reporting obligation, to the extent that obligation is
imposed on a "company" (or "association") subject to the statute/rule.

- The document provides a rule of construction that when the terms "company" or
  "association" are used in reference to a corporation, those terms are deemed to
  include the corporation's "successors and assigns" as though those words were
  expressly included. This indicates that duties attached to a covered
  "company/association" continue to bind its successor following a merger. (Section 5)

- The document also adopts an interpretive rule that the terms "person" and "whoever"
  include corporations and other entities. This supports that entity-level obligations
  (including reporting duties) can apply to corporate actors generally. (Section 1)

Key limitation: the excerpts do not contain the specific reporting obligation language
(e.g., whether it is imposed on a "company," "association," "person," "whoever," or
some other defined party). Because the successor-inclusion language is explicit for
"company"/"association" (Section 5), the conclusion is strongest when the reporting
duty is framed in those terms. If the reporting obligation is framed differently, that
could change the outcome, but no such limiting text is provided here.
```

*Full file: [`stage4_answer.md`](../examples/01_usc_title1_ch1/snippets/stage4_answer.md)*

The answer cites specific sections, identifies its own limitation (that the framing of the original obligation matters), and appropriately hedges the conclusion.  The USC Title 1 example is intentionally small: the chapter has only eight sections and the question has a narrow answer.  The same process on a more complex document produces a more complex answer.  The following is the bottom-line section of an answer to a question about the EU AI Act, synthesized from 14 facts drawn from 12 articles and 3 annexes:

```
Question: Are the obligations of a provider of a general purpose model limited if
          they only fine-tune it?

Answer (bottom line):

Fine-tuning does not create a reduced obligation track. If, after fine-tuning, the
actor qualifies as a "provider" and places the resulting GPAI model/system on the
market or puts it into service, the standard GPAI provider obligations apply (and
may expand if the model is systemic-risk). Any limitation comes from specific
statutory carve-outs (e.g., qualifying open-source releases) or scope exclusions
(e.g., R&D prior to placing on the market), not from the fact of fine-tuning itself.
(Article 3; Article 53; Article 54; Article 2)
```

*Full file: [`stage4_answer.md`](../examples/03_eu_ai_act/snippets/stage4_answer.md)*

Optionally, there can be further levels of checking performed on the proposed answer.  For example, in order to avoid the possibility that something important was overlooked, the question and proposed answer can be provided to a series of LLM analysts who each compare them to the text of a substantive section of text, looking for any places where some aspect of the proposed answer is actually wrong.  For a minor discrepancy, the concern is appended as a note to the answer.  For a more significant issue, the answer is regenerated with the LLM directed to pay specific attention to getting that particular point correct.  This checking pass can be an expensive process if all substantive sections of the document are included, so a tradeoff may be made to only use those portions of the document that had been identified as relevant in the initial scoring pass.

The number of strategies for using the output of the third stage to answer questions is actually quite large.  There are tradeoffs in terms of cost and quality, but the process can also be optimized for specific types of concerns that a user might have.

### Caching

In order to save my wallet, I implemented different types of caching into this system.

At a first level, there is a local cache file.  If a particular LLM model is sent an API call with a particular set of input (e.g. a section of text as well as instructions on how to respond), the cache file is updated with a fingerprint of that call (derived from the model identifier, the full prompt text, and the maximum output token count) along with the response.  During development, particularly, I may need to run the same set of code over the same set of legal documents many times.  Most of the API calls will not change from one run to the next (except where I am tinkering with the specifics of the call), so all calls are structured to first look in the local cache file for the answer.  If a matching fingerprint is found in the cache, the stored response is used.  There is no use in sending the same LLM model the same query over and over again.

The other type of caching is that offered by the model companies.  Although the mechanics, and pricing advantage, of the caching they offer varies, the basic idea is that early parts of your prompt can be cached if they are long enough and unchanging.  For some types of calls, there is very little that stays the same from one call to the next, so there is no benefit to making use of such caching.  But other times there will be substantial amounts of text that are included in many calls, and by organizing the calls to keep the unchanging portion in the cache, substantial cost savings can be had.  For example, if a longish section of a document includes many defined terms, there are repeated calls to the APIs to find a good definition for each word, and to find the applicable scope of each definition.  Also, for some documents the table of contents that lays out the organizational structure is itself long enough to benefit from caching, so there will be a large number of calls that will benefit from making use of it.

## Handling complex documents

Some documents contain structures that are too large or too data-like to be handled well by the base pipeline.  Two categories have required specific handling.

### Multi-level substantive units

Most of the laws and regulations I am working with have base-level units (such as sections or articles) that are discrete units covering a small number of things.  These are the basic units used for building up summaries of the content, because they are generally short enough to be analyzed and summarized in one piece by the LLMs.  Some units, though, are too long for doing this reliably.  The first thing that I did to address this was to introduce "chunking".  The idea is that a long section can be broken up at meaningful places that allow the individual chunks to be summarized on their own.  Moving through the substantive unit, the first chunk is summarized first, then the second chunk is summarized along with the summary of the previous chunk.  The third chunk is summarized along with the previous summaries, and so on.  In this way, it is intended to mimic the human experience of moving through a long document, retaining a summary recollection of what was read before, instead of retaining the full text and reasoning through it from a perfect memory of it.  In many ways, this chunking of a long substantive unit is similar to the process that this project uses for the overall document, abstracting away details but making available to the summarizer what is needed as context for what they are focusing on.

This mechanism works pretty well most of the time, but it turns out that there are some truly enormous sets of text in some legal texts (particularly regulations).  An example I first encountered is the Export Control Classification Number supplement in 15 CFR Part 774.  It includes a very large number of discrete pieces, and going through it to create a holistic summary in the way described above was not working well.  Arguably, the answer is to treat long compilations of information as being distinctly different from the type of information this project was designed for (legal prose), and exclude it from scope.  That is, in fact, the answer for some cases.  But here, I wanted to see if there was a better mechanism.  The key is that the long supplement, which is a type of appendix to the main document, is effectively its own document that could be broken down and interpreted in the same way that the main document is.  Because the context of the document is that it is organizationally below the level of a supplement, which is generally treated as substantive in nature and handled as a single unit, I decided that what was needed was to introduce the concept of substantive units that can be containers to other substantive units.  The multiple layers of the ECCN supplement could then be described within the supplement.

One way of doing this might have been to break down the distinction between organizational units (e.g. chapters and parts) and substantive units (sections and articles) and let the tree of organization allow substance at any level.  I believe that this may, in fact, be the right way to go, but have not yet undertaken that re-architecture.

The way that this was implemented is to give parsers (which are written to handle a particular type of document, and so are specialized to the needs of a particular type of document) the ability to define substantive unit types that are able to be contained within other substantive unit types.  The parser specifies these types in the document header; downstream stages use that information without any knowledge of the specifics of that document type.  It required giving the later stages some additional capabilities that would be activated by this information the parsers placed in the document, but it allows the downstream processing to remain agnostic about the details of any particular document type.

### Tables

Handling tables has required specific attention.  One issue I had to contend with was simply how to maintain the information in tables in the documents being processed.  I spent considerable time writing code to duplicate, as closely as possible, the layout of a complex HTML table in plain text, so that it would be visually similar to what would be seen if the HTML table were rendered.  It turned out, however, that the LLMs do at least as good a job interpreting a table in HTML form as one that is transformed into plain text.  And the process of making a plain text version of an HTML table is error-prone.  So, although there are functions in the code for turning an HTML table into human-readable text, these aren't currently used and we rely on the fact that LLMs read HTML tables just fine.

However, this was not the only time I had to contend with tables.  Looking at the instances of very long substantive units in regulations such as the US Code of Federal Regulations, it became apparent that many of these were predominantly long HTML tables of data.  An example of this is the Entity List (Supplement No. 4 to 15 CFR Part 744).  Turning this type of table into a long list of substantive sub-units, while possible, would be pointless.  Each row is the name and identifying information for a person or company.  Summarizing each row would take substantial resources and yield nothing useful.  A decision was made that, by default (which can be overridden where helpful), these tables would be retained in their full form (no information is lost), but no effort is spent gathering definitions from them, summarizing them, or analyzing them during question-answering.  Instead, the system generates an AI summary of the table's structure and purpose, which is enough for the question-answering stage to provide meaningful direction (for example, telling the reader what kind of entries the table contains, what to look up, and what to do with the result) without attempting to interpret the table's rows directly.  It may be that a system will be developed at a later time that allows the question-answering analysts to directly look up information in these large tables, but it will be a non-trivial effort and is not an immediate plan.

## Examples

The excerpts in this post are drawn from three curated end-to-end examples, each processed through all four stages.  Full walkthroughs (source file, parsed JSON, extracted definitions, summaries, and Q&A answer) are in the repository:

- [USC Title 1, Chapter 1 (Rules of Construction)](../examples/01_usc_title1_ch1/README.md) (USLM XML): illustrates the core pipeline, direct and scoped definition types, and a narrow Q&A question with a two-fact answer.
- [CA GOV § 11120 (Bagley-Keene Open Meeting Act)](../examples/02_ca_gov_11120/README.md) (California HTML): scoring, an open cross-reference question, and a multi-section answer.
- [EU AI Act](../examples/03_eu_ai_act/README.md) (Formex XML): non-US jurisdiction, complex multi-article answer synthesized from 14 facts across 12 articles and 3 annexes.

## The process of building this project

I have written the code for this project several times.  As is often the case, after getting to a point near the end of a design, I realized that there was a problem best solved by rethinking the architecture.  Each rewrite has advanced the project to a point that would not have been possible within the prior design.  There is still more I would like to do, but the system works well now, and some of that future work may simply not happen.

After the first few rewrites, I started using Cursor and Claude Code to assist with portions of the work.  At this point, directing and reviewing the output of these tools is a larger part of the work than writing code directly.  What I have learned from this is that these tools work well for implementing code you already understand clearly, but they are not ready to take a specification and build an entire system on their own.  In my experience, the plans they produce look reasonable about 60% of the time.  The rest of the time, I find I need to change something, sometimes substantially.  A common failure mode is that a tool becomes overly focused on solving one specific problem and produces a solution that either breaks another part of the project or makes something that should be general into something too specific or fragile.  Staying closely engaged with the process, and maintaining a skeptical view of what the tools propose, makes them genuinely useful for speeding up the development of good code.

The question of why this project should exist as open source, given that similar approaches are almost certainly in use in commercial legal-tech systems, is addressed in the companion post [Getting AI to reliably analyze statutes and regulations](AI_AND_LEGAL_DOCUMENTS.md).

Development on this project is ongoing.  If you work through the documentation and try it out on a specific document, I would be interested to hear about the results, including places where it falls short or fails in ways not covered by the documentation.  Feedback can be submitted as an issue at [https://github.com/steveyoung-random/cassiel-legal-workbench](https://github.com/steveyoung-random/cassiel-legal-workbench).

---

[^1]: Cheng-Ping Hsieh, Simeng Sun, Samuel Kriman, Shantanu Acharya, Dima Rekesh, Fei Jia, Yang Zhang, and Boris Ginsburg. (2024). Ruler: What's the real context size of your long-context language models? https://arxiv.org/abs/2404.06654.

