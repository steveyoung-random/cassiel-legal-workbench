"""
Q&A Panel Component - Question-answering interface for document viewer.

Allows users to ask questions about the current document and view previous
questions and answers.
"""
# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License

import streamlit as st
from pathlib import Path
from typing import Dict, List, Optional
import json
import os


def render_qa_panel():
    """Render the Q&A panel for the current document."""
    doc = st.session_state.viewer_document
    file_path = st.session_state.viewer_file_path

    if not doc or not file_path:
        st.error("❌ No document loaded")
        return

    doc_info = doc.get('document_information', {})
    doc_title = doc_info.get('title', 'Unknown Document')

    st.title("💬 Questions & Answers")
    st.markdown(f"**Document:** {doc_title}")

    st.markdown("---")

    # Check if document is Stage 3 complete
    processing_status = doc_info.get('processing_status', {})
    if not processing_status.get('stage_3_complete'):
        st.warning("⚠️ **Stage 3 Required**")
        st.info("""
        This document has not completed Stage 3 processing (Summary Generation).

        Questions can only be answered for documents with Stage 3 complete.

        **To enable Q&A:**
        1. Go to the main processing UI: `streamlit run ui/app.py`
        2. Complete Stage 3 for this document
        3. Return here to ask questions
        """)
        return

    # Tab view: Ask Question | View Previous Questions
    tab_ask, tab_previous = st.tabs(["❓ Ask Question", "📜 Previous Questions"])

    with tab_ask:
        render_ask_question_tab(file_path, doc_title)

    with tab_previous:
        render_previous_questions_tab(file_path, doc_title)


def render_ask_question_tab(file_path: str, doc_title: str):
    """
    Render the 'Ask Question' tab.

    Args:
        file_path: Path to processed file
        doc_title: Document title for display
    """
    st.subheader("Ask a Question")

    st.info(f"**About:** {doc_title}")

    # Question form
    with st.form("ask_question_form"):
        question_text = st.text_area(
            "Your Question",
            height=150,
            placeholder="e.g., What are the requirements for establishing a corporation?",
            help="Enter your question about this document"
        )

        # Q&A Mode selection
        qa_mode = st.selectbox(
            "Analysis Mode",
            options=["quick_scan", "standard", "thorough", "maximum_confidence"],
            index=1,  # Default to "standard"
            format_func=lambda x: {
                "quick_scan": "Quick Scan (scoring only, fastest)",
                "standard": "Standard (balanced speed/quality)",
                "thorough": "Thorough (higher quality, slower)",
                "maximum_confidence": "Maximum Confidence (highest quality, slowest)"
            }.get(x, x),
            help="Select the Q&A analysis mode. Quick Scan returns scored sections only."
        )

        # Advanced options (collapsed)
        with st.expander("⚙️ Advanced Options"):
            max_items = st.number_input(
                "Max Items",
                min_value=50,
                max_value=500,
                value=st.session_state.get('max_items', 10000),
                help="Maximum number of items to process"
            )

            max_tokens = st.number_input(
                "Max Tokens",
                min_value=500,
                max_value=4000,
                value=1000,
                help="Maximum tokens for answer"
            )

            max_iterations = st.number_input(
                "Max Iterations",
                min_value=1,
                max_value=5,
                value=3,
                help="Maximum reasoning iterations (overrides mode default if set)"
            )

        # Submit button
        submitted = st.form_submit_button("Submit Question", type="primary", use_container_width=True)

        if submitted:
            if not question_text.strip():
                st.error("❌ Please enter a question")
            else:
                _submit_question(
                    file_path,
                    question_text.strip(),
                    max_items,
                    max_tokens,
                    max_iterations,
                    qa_mode
                )


def _submit_question(file_path: str, question_text: str, max_items: int,
                     max_tokens: int, max_iterations: int, qa_mode: str = "standard"):
    """
    Submit question job to queue.

    Args:
        file_path: Path to processed file
        question_text: Question text
        max_items: Max items to process
        max_tokens: Max tokens for answer
        max_iterations: Max reasoning iterations
        qa_mode: Q&A analysis mode (quick_scan, standard, thorough, maximum_confidence)
    """
    try:
        # Import here to avoid circular dependencies
        import sys
        sys.path.insert(0, str(Path(__file__).parent.parent.parent))

        from worker.queue import JobQueue
        from utils.config import get_config, get_job_queue_database

        config = get_config()
        db_path = get_job_queue_database(config)
        queue = JobQueue(db_path)

        params = {
            'question_text': question_text,
            'max_items': max_items,
            'max_tokens': max_tokens,
            'max_iterations': max_iterations,
            'qa_mode': qa_mode,
            'config': 'config.json'
        }

        job_id = queue.enqueue(
            job_type='question',
            file_path=file_path,
            params=params,
            max_retries=0
        )

        st.success(f"✅ Question submitted! Job ID: {job_id}")
        st.info("""
        **Your question has been submitted to the job queue.**

        You can:
        - Switch to the "Previous Questions" tab to see your question
        - Monitor progress in the main UI's Job History view
        - Return here later to see the answer
        """)

        # Clear form
        st.rerun()

    except Exception as e:
        st.error(f"❌ Failed to submit question: {e}")
        st.info("""
        **Troubleshooting:**
        - Ensure the worker is running: `python worker/run_worker.py`
        - Check that `config.json` is properly configured
        """)


def render_previous_questions_tab(file_path: str, doc_title: str):
    """
    Render the 'Previous Questions' tab showing Q&A history.

    Args:
        file_path: Path to processed file
        doc_title: Document title for display
    """
    st.subheader("Previous Questions")

    # Find question files for this document
    question_files = _find_question_files(file_path)

    if not question_files:
        st.info("📭 **No questions found for this document**")
        st.markdown("""
        Questions you submit will appear here after processing completes.

        **Tip:** Switch to the "Ask Question" tab to submit your first question!
        """)
        return

    st.caption(f"Found {len(question_files)} question(s)")

    st.markdown("---")

    # Display questions
    for q_file in question_files:
        try:
            with open(q_file, 'r', encoding='utf-8') as f:
                qa_data = json.load(f)

            question = qa_data.get('question', 'No question text')
            answer = qa_data.get('answer', '')
            status = qa_data.get('status', 'unknown')

            # Question card
            with st.expander(f"**Q:** {question[:100]}..." if len(question) > 100 else f"**Q:** {question}", expanded=False):
                st.markdown(f"**Question:**")
                st.markdown(question)

                st.markdown("")

                if status == 'completed' and answer:
                    st.markdown(f"**Answer:**")
                    st.markdown(answer)
                elif status == 'pending' or status == 'processing':
                    st.info(f"⏳ Status: {status.title()}")
                    st.caption("Check back later for the answer")
                elif status == 'failed':
                    st.error("❌ Failed to generate answer")
                    error_msg = qa_data.get('error', 'Unknown error')
                    st.caption(f"Error: {error_msg}")
                else:
                    st.warning("⚠️ No answer available")

                # Metadata
                st.markdown("---")
                st.caption(f"📄 File: {Path(q_file).name}")
                if 'timestamp' in qa_data:
                    st.caption(f"🕒 Asked: {qa_data['timestamp']}")

        except Exception as e:
            st.error(f"❌ Failed to load question from {Path(q_file).name}: {e}")


def _find_question_files(processed_file_path: str) -> List[str]:
    """
    Find all question files for a processed document.

    Args:
        processed_file_path: Path to processed document

    Returns:
        List of paths to question files (JSON)
    """
    processed_path = Path(processed_file_path)

    if not processed_path.exists():
        return []

    # Get file stem (remove _processed.json suffix)
    file_stem = processed_path.stem.replace('_processed', '')

    # Look for question files in same directory
    # Pattern: {file_stem}_question_*.json
    question_dir = processed_path.parent
    pattern = f"{file_stem}_question_*.json"

    question_files = list(question_dir.glob(pattern))

    # Sort by modification time (newest first)
    question_files.sort(key=lambda x: x.stat().st_mtime, reverse=True)

    return [str(f) for f in question_files]
