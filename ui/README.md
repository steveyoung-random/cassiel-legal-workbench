# Cassiel Legal Workbench - Streamlit UI

Unified web-based interface for managing document parsing, processing, and question-answering workflows.

## Installation

1. Install Streamlit (if not already installed):
```bash
pip install streamlit
```

2. Ensure all project dependencies are installed (see main project README).

## Usage

### Starting the UI

From the project root directory:

```bash
streamlit run ui/app.py
```

The UI will open in your default web browser at `http://localhost:8501`.

### Features

#### 📚 Documents Tab
- **Document Overview**: View all parsed documents discovered via manifest files
- **Chapter Status**: See processing status for each chapter (parsed, Stage 2, Stage 3)
- **Progress Indicators**: Real-time progress bars and completion status
- **Chapter Selection**: Select chapters for processing

#### ⚙️ Processing Tab
- **Parse Documents**: Parse source XML/HTML files into structured JSON
  - Supports USLM, Formex, and CA parsers
  - Options for chapter splitting or full document mode
  - Title and chapter filtering for USLM documents
- **Stage 2 Processing**: Run definition extraction and scope resolution
  - Requires parsed JSON files
  - Shows real-time progress
  - Configurable checkpoint threshold
- **Stage 3 Processing**: Generate summaries
  - Requires Stage 2 completion
  - Shows operational and organizational progress
  - Configurable checkpoint threshold

#### ❓ Questions Tab
- **Question Management**: View all question files in the output directory
- **Answer Display**: View questions and their answers
- **Question Deletion**: Remove question files

#### Sidebar
- **Configuration**: View output directory and configuration
- **Cache Statistics**: View API cache size and entry count
- **Refresh Button**: Refresh document list

## Workflow

1. **Parse a Document**:
   - Go to Processing tab
   - Expand "Parse Source Document"
   - Select parser type, enter source file path
   - Choose parse mode and click "Parse Document"

2. **Process Documents**:
   - Go to Documents tab
   - Select a document and chapter
   - Click "Select" on the chapter
   - Go to Processing tab
   - Click "Run Stage 2" or "Run Stage 3"

3. **View Questions**:
   - Go to Questions tab
   - View existing questions and answers
   - Delete questions if needed

## Notes

- The UI automatically scans the output directory for manifest files
- Processing status is read from `processing_status` fields in processed JSON files
- Progress is polled every 2 seconds during processing
- Use the refresh button to update the document list after parsing

## Troubleshooting

- **No documents shown**: Ensure you've parsed at least one document and that manifest files exist in the output directory
- **Processing not starting**: Check that the selected chapter has the required input files
- **Progress not updating**: The UI polls files every 2 seconds; large files may take longer to update

