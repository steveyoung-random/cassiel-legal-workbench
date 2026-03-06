# Cassiel Legal Workbench

> Tools for parsing, annotating, and querying legal documents.

Using an LLM to answer questions about lengthy statutes or regulations can lead to bad results.  Even where an LLM has a context window that is long enough to include the full length of the necessary documents, the LLM is likely to lose focus and give spurious results.  It turns out that even though an LLM can work with large amounts of input, it can only hold focus over a shorter amount of input.  When you feed an LLM lengthy documents and ask detailed questions about them, the LLM may hallucinate citations and point to irrelevant or non-existent provisions.  These issues don't ordinarily occur when the input to the LLM is kept to a much smaller and more focused set.  This project takes inspiration from the fact that, as a human who reads a lot of legal documents, I also am not able to take in a lengthy document in one reading and reliably answer detailed questions about it without going back to look at it further.  The goal of this project is to have the LLM iteratively go through document the way I would: go through the document keeping a general sense for what has been read, note down important definitions, use the high level information about what has gone before to provide context as I read later, and then go back through it against to build up a more detailed understanding while cross-referencing necessary sections.  This system steps an LLM through a legal document in focused chunks, building up definitions and summaries before using them to generate proposed answers to questions.  The result is cited answers that are directly grounded in the actual text of the law or regulation.

This is intended as a tool for legal professionals and is not as a substitute for professional judgment.  Think of it as a person of uneven capabilities who works down the hall.  This person is a very quick reader, remembers a lot, and is generally good at piecing things together, but also sometimes stumbles.  You stand to benefit from having that person of uneven capabilities take a look at your situation and tell you what they think.  If you take your job seriously, you are not going to just take what they give you as a definitive answer.  It will hopefully be a useful analysis to help you figure out what you think the right analysis is, even if that better analysis veers from their answer considerably.

**[Paper / Technical Overview](#paper)** | MIT License

---

## What It Does

Cassiel Legal Workbench is a four-stage pipeline that transforms raw legal documents (such as statutes, regulations, and codes) into a structured, queryable knowledge base.  Given a source document (XML or HTML) of a known type, the pipeline:

1. Parses the document into a standardized JSON format, preserving the meaningful content, organizational hierarchy, cross-references, and footnotes.
2. Extracts definitions, resolving the scope of each defined term to the organizational unit(s) where it applies.
3. Summarizes every section and subsection at two levels of detail, building a hierarchical summary tree.
4. Answers questions by scoring sections for relevance, iteratively analyzing the most relevant content with keyhole context, and producing a cited answer with references to source sections.

In order to maximize the focus of the LLMs, each LLM analyst in the question answering stage is given the full text of just one section (or less, if the section is very long), along with the definitions and context from other sections needed for that section.  Because the amount of input provided to the LLM is limited and focused on a limited set of topics, the dependability of the system is high, and does not exhibit the hallucinations common from working directly with lengthy input text.

There are parsers for  set of document types.  All document-specific details (including the full content of the legal material) are captured by the parsers in the JSON output in the first stage.  Stages 2, 3, and 4 work from that JSON document alone and know nothing about the specifics of any particular document type.  All knowledge about a specific document type is limited to the parsers.

---

## Supported Document Types

| Format     | Source                                                                                                                             | Parser                |
| ---------- | ---------------------------------------------------------------------------------------------------------------------------------- | --------------------- |
| USLM XML   | [United States Code (USC)](https://uscode.house.gov/download/download.shtml)                                                       | `uslm_set_parse.py`   |
| eCFR XML   | [Code of Federal Regulations (CFR)](https://www.ecfr.gov/developers/documentation/api/v1)                                          | `cfr_set_parse.py`    |
| Formex XML | [European Union regulations](https://datadump.publications.europa.eu/) - free registration required, download in Formex XML format | `formex_set_parse.py` |
| HTML       | [California statutes](https://leginfo.legislature.ca.gov/faces/codes.xhtml)                                                        | `CA_parse_set.py`     |

New document types can be added via the parser plugin interface without modifying any of the processing stages.  See [`ADDING_NEW_PARSER.md`](ADDING_NEW_PARSER.md).

---

## Prerequisites and Installation

### Requirements

- Python 3.13 or later
- An OpenAI API key and/or Anthropic API key (OpenAI and Anthropic are both are configurable in `config.json`)
- Dependencies: see `requirements.txt`


### Installation

```bash
git clone https://github.com/steveyoung-random/cassiel-legal-workbench
cd cassiel-legal-workbench
pip install -r requirements.txt
cp config.example.json config.json
# Edit config.json to set your output directory and model preferences
cp api_keys.example.py api_keys.py
# Edit api_keys.py to add your OpenAI and/or Anthropic API keys
```

---

## Quick Start

The UI is still very experimental and janky.  Right now, it is probably more trouble than it is worth.  I use the command line most of the time.

### Run the UI

```bash
# Terminal 1: start the job queue worker (required for async processing)
python worker/run_worker.py

# Terminal 2: start the main UI
streamlit run ui/app.py

# Optional: document viewer (pass a processed JSON file)
streamlit run ui/view_document.py
```

### Run the Pipeline from the Command Line

```bash
# Stage 1: parse a CFR document
python cfr_set_parse.py path/to/ecfr-title15.xml

# Stages 2 and 3: definitions and summaries (batch)
python batch_process.py output/CFR/ --stages 2,3

# Stage 4: ask a question
python question_answering.py output/CFR/part774_processed.json \
  "What semiconductor items require a license for export to France?"
```

Each stage can also be run individually:

```bash
python Process_Stage_2.py <parsed_json>
python Process_Stage_3.py <processed_json>
python question_answering.py <processed_json> "your question" --mode thorough
```

**Q&A modes**: `quick_scan`, `standard` (default), `thorough`,
`maximum_confidence`. See [`USAGE_GUIDE.md`](USAGE_GUIDE.md) for details.

---

## Architecture

Parsers own all knowledge of a particular document type, and the processing stages are provided information entirely through the parser output.  This allows new document types to be added by writing a single parser, without touching any other part of the system (ideally).  Where input has structural problems, the system should fail loudly rather than silently dropping information.

```
  XML / HTML Source Document
             │
             ▼
      ┌─────────────┐
      │   Stage 1   │  · Extract organizational hierarchy
      │    Parse    │  · Convert to standard JSON format
      └─────────────┘  · Record breakpoints and cross-references
             │
             ▼
      ┌─────────────┐
      │   Stage 2   │  · Extract defined terms from each section
      │ Definitions │  · Resolve scope and indirect definitions
      └─────────────┘
             │
             ▼
      ┌─────────────┐
      │   Stage 3   │  · Concise (level 1) and detailed (level 2) summaries
      │  Summaries  │  · Organizational summaries at each level
      └─────────────┘
             │
             │  ◄── repeat for each source document
             ▼
    ┌─────────────────────┐
    │   Document Corpus   │
    │  + Cross-Reference  │
    │      Registry       │
    └─────────────────────┘
             │
             ▼
      ┌─────────────┐
      │   Stage 4   │  · Score sections for relevance
      │     Q&A     │  · Iterative analysis with keyhole context
      └─────────────┘  · Synthesize cited answer
             │
             ▼
    Answer with citations
```

### Key Design Features

- **Parser plugin architecture**: Add support for new document types by implementing a single adapter class.  No changes are needed in the processing stages, UI, or job queue.
- **Keyhole context**: Each AI analyst sees only its assigned section plus the definitions in scope for that section.  This reduces the chance for hallucination and keeps reasoning grounded in the actual text.
- **Nested sub-units**: Very long sections (e.g., the 1.68M-character Commerce Control List in 15 C.F.R. Part 774) can be automatically subdivided into independently processable sub-units at parse time.  For this to work well, it takes some amount of customization in the parser for each document type, based on knowledge of what long sections of operative language are going to be encountered.
- **Large table extraction**: HTML and XML tables with 50 or more rows are extracted as structured sub-units with AI-generated summaries, rather than being flattened to prose.
- **Cross-document references**: A SQLite registry attempts to resolve citations across documents.  During question answering (stage 4) referenced sections from other documents are automatically pulled in as additional context.
- **Flexible AI configuration**: Models are assigned per task in `config.json`, with automatic fallback.  Lightweight models handle scoring and more capable models handle analysis and synthesis.
- **Caching**: All LLM API calls are cached locally, so re-running processing on a document will not incur additional costs where there is no change in what is sent to the LLM.  Additionally, the system organizes API calls in order to take advantage of LLM platform-level caching, to reduce costs.
- **Append-only scratch document**: During question answering (stage 4), analysts accumulate facts and propose questions for the other analysts without overwriting.  A cleanup phase deduplicates before final answer synthesis.

### Configuration

All model assignments, API keys, and directory paths are set in `config.json`.  A template is provided as `config.example.json`.  Copy it to `config.json` and fill in your API keys and output directory.  Models can be mixed across providers (OpenAI and Anthropic) on a per-task basis.

---

## Documentation

| Document                                             | Purpose                                       |
| ---------------------------------------------------- | --------------------------------------------- |
| [`DEVELOPER_GUIDE.md`](DEVELOPER_GUIDE.md)           | Architecture, pipeline stages, file structure |
| [`USAGE_GUIDE.md`](USAGE_GUIDE.md)                   | How to use all tools and scripts              |
| [`JSON_SPECIFICATION.md`](JSON_SPECIFICATION.md)     | Parsed document format (v0.5)                 |
| [`ADDING_NEW_PARSER.md`](ADDING_NEW_PARSER.md)       | How to add support for new document types     |
| [`LARGE_TABLE_HANDLING.md`](LARGE_TABLE_HANDLING.md) | How large tables are extracted as sub-units   |
| [`ui/VIEWER_GUIDE.md`](ui/VIEWER_GUIDE.md)           | Document viewer user guide                    |

---

## Development and Test Corpus

The Cassiel Legal Workbench has been developed and tested against a corpus of approximately 340 documents, including:

- **United States Code**: 198 USC titles (USLM XML)
- **Code of Federal Regulations**: 107 CFR parts (eCFR XML)
- **European Union regulations**: 3 Regulations and Directives (Formex XML)
- **California statutes**: 32 documents (HTML)

I have been working on this project for about a year and a half.  I see a lot more left to be done, but the primary functionality is in place and works.

---

## Roadmap

Although I have been working on this system for a long time, and it is useable as it is, there are a list of future improvements and new capabilities that I intend to add.  Here are some highlights:

- A content refresh mechanism: when a source document is updated, detect what changed and re-process only the affected units, rather than reprocessing the whole document (Workstream 4).
- A redesigned question answering pipeline using role-separated phases (fact extraction question generation, answer collection) with a gatekeeper LLM reviewing each proposed fact before it enters the shared scratch document.  This addresses the current tendency toward fact churn and unfocused question accumulation in long documents (Workstream 8).
- Systematic end-to-end testing across all four document types, and additional edge cases such as very long documents and deep nesting (Workstream 5).

Some future work is described in the [PLANNED_ENHANCEMENTS](PLANNED_ENHANCEMENTS.md) document.

---

## Paper

A paper describing the motivation, design decisions, and lessons learned in building this system is in preparation. It will be linked here when it is completed.

---

## License

MIT — see [`LICENSE.txt`](LICENSE.txt).

---

## Acknowledgment

The name Cassiel was inspired by the Wim Wenders film *Wings of Desire* (1987) and Nick Cave's song *Cassiel's Song*, from the soundtrack to its sequel, *Faraway, So Close!* (1993).