# Document Viewer Guide

The Document Viewer is an interactive Streamlit application for browsing and exploring processed legal documents through summary-driven navigation and progressive disclosure.

## Quick Start

### Launch the Viewer

```bash
# Method 1: Browse library of all processed documents (Recommended)
streamlit run ui/view_document.py

# Method 2: Open specific document directly via command line
streamlit run ui/view_document.py -- path/to/document_processed.json
```

### Example with Direct Document Load

```bash
streamlit run ui/view_document.py -- "~/document_analyzer_output/usc02/usc02_title2_chapter5_processed.json"
```

The viewer will open in your default web browser at `http://localhost:8501`.

### Default Behavior

When you launch the viewer without arguments, it displays a **Document Library** showing all fully processed documents in your output directory. This is the recommended way to use the viewer for browsing multiple documents.

## Features

### 📚 Document Library (NEW!)

**The Document Library is your starting point** for browsing all processed documents:

- **Automatic Discovery**: Scans your output directory for all fully processed documents (Stage 3 complete)
- **Summary Previews**: See truncated summaries for each document in the list
- **Filter & Search**: Filter by document collection and search by title or content
- **One-Click Access**: Click "Open →" to load and explore any document
- **Pagination**: Browse large document collections with easy pagination

The library view makes it easy to:
- Find relevant documents quickly
- Compare summaries across multiple documents
- Navigate your entire document collection

**Document Organization:**
- Documents are **grouped by type** (US Code, EU Regulations, CA Statutes, etc.)
  - Parser type automatically detected from manifest files
  - Supports both per-file manifests (CA, EU) and directory-level manifests (USC)
- Documents with identical titles show **distinguishing information** (e.g., section ranges)
- Example: Multiple "BPC" documents become "BPC (§2050-§2078)", "BPC (§2100-§2150)", etc.

### 📋 Summary-Driven Navigation

The Document Viewer is designed around **progressive disclosure** - start with high-level summaries and drill down into details as needed:

1. **Document Library**: Browse all available documents with summary previews
2. **Document Overview**: See document-level summaries and top-level organizational units
3. **Organizational Units**: Navigate through titles, chapters, parts with their summaries
4. **Substantive Units**: View individual sections with full text, summaries, and definitions

### 🔍 Two Summary Levels

Toggle between two levels of detail:

- **Level 1 (Concise)**: 3-sentence overview for quick understanding
- **Level 2 (Detailed)**: 5+ paragraph in-depth summary with context

Change summary level in the sidebar View Options.

### 📂 Hierarchical Structure

Browse documents by their natural organizational structure:

- **Titles** → **Chapters** → **Sections** (US Code)
- **Parts** → **Articles** → **Sections** (EU Regulations)
- Custom hierarchies based on document type

Each organizational level shows:
- Summary of that unit's content
- Child organizational units
- Substantive units within that range
- Definitions scoped to that level

### 📄 Substantive Unit View

When viewing a section or article, you see:

**Left Panel - Summary:**
- Concise or detailed AI-generated summary
- Toggle between summary levels
- References to other sections

**Right Panel - Original Text:**
- Full original text in scrollable container
- Formatted for easy reading
- Copy to clipboard functionality

**Below:**
- Notes and footnotes (if present)
- Definitions in scope
- References to other sections

### 📖 Definitions Panel

View all definitions applicable to the current unit, organized by scope:

1. **Document-Level Definitions**: Apply throughout entire document
2. **Organizational Definitions**: From parent titles/chapters (inherited)
3. **Unit-Specific Definitions**: Defined in current section
4. **External Definitions**: From other related units

Each definition shows:
- Term and definition text
- Scope (where it applies)
- Type (direct, indirect, elaborational)
- Source reference
- Quality check status

### 🧭 Breadcrumb Navigation

Always know where you are in the document:

```
📄 Title 2 > 📂 Chapter 5 > 📄 Section 131
```

Click any breadcrumb to navigate back to that level.

### 💬 Question-Answering (NEW!)

**Ask questions about documents directly from the viewer:**

- **Ask Questions Tab**: Submit natural language questions about the current document
- **Previous Questions Tab**: Browse all questions and answers for the document
- **Integrated Workflow**: Ask → Submit → Monitor → View Answer
- **Advanced Options**: Control max items, tokens, and reasoning iterations

**How it works:**
1. Load a document that has completed Stage 3 processing
2. Click "Ask Questions" button in the sidebar
3. Type your question in the form (e.g., "What are the requirements for establishing a corporation?")
4. Submit to job queue
5. Switch to "Previous Questions" tab to see your question and (when ready) the answer

**Question Status:**
- **Pending/Processing**: Question submitted, waiting for or generating answer
- **Completed**: Answer is ready and displayed
- **Failed**: Error occurred (check error message for details)

**Tips:**
- Be specific with your questions for better answers
- Questions are processed asynchronously via the job queue
- Answers typically take 1-5 minutes depending on document size
- All questions and answers are saved for future reference

### 🔎 Search

Search across all documents in the library by title or summary content. Individual document search (within sections) coming soon.

## User Interface

### Sidebar

**When in Library View:**
- Output directory configuration
- Refresh library button
- Preview summary level toggle (for library previews)
- Filter by document collection

**When Viewing a Document:**
- "Back to Library" button (return to document list)
- Current document info (title and processing status)
- View options:
  - Summary level toggle (Level 1 / Level 2)
  - Show/hide definitions checkbox
- Q&A section:
  - "Ask Questions" button (opens Q&A panel)

### Main Content Area

The main area displays one of five views:

1. **Document Library** (default on startup)
   - Documents grouped by type (US Code, EU Regulations, CA Statutes)
   - Summary previews with expand option
   - Global search across all documents
   - Duplicate titles automatically disambiguated
   - Click "Open →" to load a document

2. **Document Overview** (after selecting a document)
   - Document title and summary
   - Top-level organizational units
   - Click "Explore →" to drill into any unit

3. **Organizational Unit View**
   - Unit summary
   - Child organizational units
   - Substantive units in this range
   - Definitions scoped to this unit
   - Click "Explore →" or "View →" to navigate deeper

4. **Substantive Unit View**
   - Side-by-side summary and original text (scrollable)
   - Notes/footnotes
   - In-scope definitions (4 categories)
   - References to other sections

5. **Q&A View** (click "Ask Questions" in sidebar)
   - **Ask Question Tab**: Submit questions about the document
   - **Previous Questions Tab**: Browse Q&A history
   - Real-time status updates (pending/processing/completed/failed)
   - Advanced options for fine-tuning answers

## Workflow Examples

### Example 1: Quick Document Understanding

**Goal:** Understand what a document is about in 30 seconds

1. Launch viewer (Document Library appears)
2. Browse document list, reading truncated summaries
3. Click "Open →" on a document of interest
4. Read the document-level summary (Level 1, concise)
5. Scan summaries of top-level organizational units
6. Done! You now understand the document's scope and structure

**Or, if you just want to compare documents:**
1. Launch viewer (Document Library appears)
2. Expand summary previews for documents of interest
3. Compare Level 1 or Level 2 summaries side-by-side
4. Click "Open →" only when you want to explore deeper

### Example 2: Finding Relevant Documents in Your Collection

**Goal:** Find documents related to a specific topic

1. Launch viewer (Document Library appears)
2. Use search box to filter by keywords (e.g., "privacy", "healthcare")
3. Browse filtered results, reading summary previews
4. Use collection filter to narrow to specific document set (e.g., "usc02")
5. Click "Open →" on the most relevant document
6. Use "Back to Library" to return and open another document for comparison

### Example 3: Deep Dive into Specific Section

**Goal:** Thoroughly understand a specific section

1. Open document from library (or via command line)
2. Click through organizational hierarchy to find the section
   - Title 42 → Chapter 6A → Section 201
3. In Substantive Unit View:
   - Read Level 2 detailed summary (left panel)
   - Read original text (right panel, scrollable)
   - Review definitions in scope
   - Check references to related sections
4. Use breadcrumbs to navigate to related sections
5. Click "Back to Library" to explore other documents

### Example 4: Finding Definitions

**Goal:** Understand all definitions applicable to a section

1. Navigate to the section from library or overview
2. Scroll to "Definitions in Scope" section
3. Expand each category:
   - Document-Level: Definitions that apply everywhere
   - Organizational: Inherited from titles/chapters
   - Unit-Specific: Defined in this exact section
   - External: From other referenced sections

### Example 5: Asking Questions About a Document

**Goal:** Get answers to specific questions about document content

1. Browse library and open a document of interest
2. Navigate through the document to understand the content (optional)
3. Click "Ask Questions" button in sidebar
4. Switch to "Ask Question" tab
5. Type your question:
   - "What are the requirements for establishing a corporation?"
   - "What penalties apply for violations?"
   - "How are defined terms used in this statute?"
6. (Optional) Expand "Advanced Options" to adjust parameters
7. Click "Submit Question"
8. Switch to "Previous Questions" tab to see your question
9. Wait for processing to complete (status will update)
10. Read the answer when status shows "Completed"
11. Submit additional follow-up questions as needed

**Tip:** You can also view previous questions asked by others or in earlier sessions!

### Example 6: Exploring Document Structure

**Goal:** Understand how a law is organized

1. Start at Document Overview (or open from library)
2. Note the top-level units (e.g., Titles)
3. Click "Explore →" on first title
4. See child units (e.g., Chapters)
5. Click "Explore →" on first chapter
6. See sub-units and sections
7. Use breadcrumbs to navigate back and explore other branches

## Tips and Best Practices

### Efficient Navigation

- **Start with summaries**: Read Level 1 summaries to quickly assess relevance
- **Use breadcrumbs**: Fastest way to navigate back up the hierarchy
- **Expand details on demand**: Only expand to Level 2 summaries or full text when needed
- **Definition scope awareness**: Check which organizational level definitions are scoped to

### Understanding Summaries

- **Level 1**: Quick orientation - what topics are covered?
- **Level 2**: Deep understanding - detailed explanation with context
- **Summaries + Text**: Use summaries to understand, text to verify specific language

### Working with Definitions

- **Check scope**: A definition in "Title 42" applies throughout that title
- **Inheritance**: Definitions from parent units (chapter) apply to child units (sections)
- **Quality checked**: Look for ✓ mark indicating AI-verified quality
- **Indirect references**: Some definitions point to other sections rather than providing text directly

## Keyboard Shortcuts

*(Coming in future version)*

- `Esc`: Navigate up one level
- `←/→`: Previous/Next sibling unit
- `Ctrl+F`: Search
- `1/2`: Toggle summary level

## Troubleshooting

### Document won't load

- **Check file path**: Ensure path is correct and file exists
- **Verify file format**: Must be a `*_processed.json` file from Stage 3 processing
- **Check permissions**: Ensure you have read access to the file

### Missing summaries

- **Stage 3 required**: Document must have completed Stage 3 processing
- **Check processing status**: Look for "S3" indicator in sidebar
- **Re-run Stage 3**: If summaries are missing, re-process through Stage 3

### Definitions not showing

- **Toggle in sidebar**: Ensure "Show Definitions" is checked
- **Stage 2 required**: Definitions are added in Stage 2 processing
- **Check scope**: Some sections may legitimately have no definitions in scope

### Navigation issues

- **Use breadcrumbs**: Click breadcrumbs to go back
- **Reload document**: Click "Load Document" again to reset to overview

## Technical Details

### Supported Document Types

The viewer works with any processed document that follows the JSON specification v0.3:

- **US Code (USLM)**: Titles → Chapters → Sections
- **EU Regulations (Formex)**: Parts → Articles → Paragraphs
- **California Law (CA HTML)**: Divisions → Chapters → Sections
- **Custom parsers**: Any parser following the specification

### File Requirements

Documents must be:
1. **Parsed** (Stage 1 complete): `*_parsed.json` or later
2. **Stage 2 complete** (for definitions): `*_processed.json`
3. **Stage 3 complete** (for summaries): `*_processed.json` with summaries

### Performance

- **Large documents**: Viewer is optimized for documents with 100s of sections
- **Pagination**: Automatically paginates lists with >20 items
- **Lazy loading**: Only loads visible content, not entire document into memory
- **Caching**: Streamlit caches loaded documents for fast navigation

## Future Enhancements

Planned features for future versions:

- [ ] Full-text search within individual documents (library search already implemented)
- [ ] Export selected sections to PDF/Markdown
- [ ] Bookmarks and favorites
- [ ] Comparison mode (summary_1 vs summary_2 side-by-side)
- [ ] Dark mode
- [ ] Keyboard navigation
- [ ] Collapsible TOC sidebar
- [ ] Multi-document comparison view
- [ ] Question suggestions based on document content
- [ ] Export Q&A history to reports

## Related Documentation

- **USAGE_GUIDE.md**: How to process documents through all stages
- **JSON_SPECIFICATION.md**: Document format specification
- **UI_AND_WORKER_GUIDE.md**: Main UI and job queue system
- **DEVELOPER_GUIDE.md**: Technical architecture and development

## Feedback and Issues

To report issues or request features:

1. Check **PLANNED_ENHANCEMENTS.md** for planned features
2. Open a GitHub issue to report a bug or request a feature

---

*Last Updated: 2026-01-04*
