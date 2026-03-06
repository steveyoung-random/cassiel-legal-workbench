"""
Global Questions View Component.

PHASE 3 IMPLEMENTATION - Global question management interface.

Displays all questions across documents with submission and viewing capabilities.
"""
# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License

import streamlit as st
import os
import json
import re
from typing import Dict, List, Optional, Any
from pathlib import Path

from worker.queue import JobQueue
from utils.config import get_job_queue_database
from ui.utils import show_error, show_error_with_action


def render(config: Dict, output_dir: str):
    """
    Render global questions view.

    Main entry point called from column_layout.py render_questions_mode().

    Args:
        config: Application configuration
        output_dir: Output directory path
    """
    st.header("❓ Questions")

    # Initialize session state
    # Note: Session state is initialized globally in ui/utils.py

    # Section 1: Submit New Question
    render_submit_question_section(config, output_dir)

    st.divider()

    # Section 2: Questions List
    render_questions_list(config, output_dir)


def render_submit_question_section(config: Dict, output_dir: str):
    """
    Render question submission form at top of view.

    Args:
        config: Application configuration
        output_dir: Output directory path
    """
    st.subheader("Submit New Question")

    # Get Stage 3 complete documents (with loading feedback)
    with st.spinner("Loading available documents..."):
        stage3_docs = _get_stage3_complete_documents(output_dir)

    if not stage3_docs:
        st.info("📚 **No Stage 3 complete documents found**")
        st.markdown("""
            **To ask questions, you need to:**
            1. Switch to **Browse Documents** view
            2. Select a document
            3. Complete Stage 3 (Summary Generation)
            4. Return to this view to ask questions
        """)
        st.caption("💡 Tip: Stage 3 must be complete before questions can be answered")
        return

    # Use form for better submission handling
    with st.form("submit_question_form"):
        # Document selection dropdown
        doc_options = {
            doc['display_name']: doc['processed_file']
            for doc in stage3_docs
        }

        # Pre-populate with selected doc if available
        default_doc = None
        selected_doc = st.session_state.get('selected_doc')
        if selected_doc:
            chapter = st.session_state.get('selected_chapter')
            if chapter and chapter.get('processed_file'):
                # Try to match current selection
                for display_name, proc_file in doc_options.items():
                    if proc_file == chapter['processed_file']:
                        default_doc = display_name
                        break

        if default_doc:
            default_idx = list(doc_options.keys()).index(default_doc)
        else:
            default_idx = 0

        selected_display_name = st.selectbox(
            "Select Document",
            list(doc_options.keys()),
            index=default_idx,
            help="Choose a Stage 3 complete document to ask a question about"
        )

        selected_file = doc_options[selected_display_name]

        # Question text area
        question_text = st.text_area(
            "Question",
            height=100,
            placeholder="e.g., What are the requirements for establishing a corporation?",
            help="Enter your question about the selected document"
        )

        # Submit button
        submitted = st.form_submit_button("Submit Question", type="primary", use_container_width=True)

        if submitted:
            if not question_text.strip():
                st.error("Please enter a question")
            else:
                _submit_question_job(selected_file, question_text.strip(), config)


def _get_stage3_complete_documents(output_dir: str) -> List[Dict[str, str]]:
    """
    Get list of Stage 3 complete documents.

    Args:
        output_dir: Output directory path

    Returns:
        List of dicts with keys: display_name, processed_file, file_stem
    """
    from ui.app import scan_documents, get_document_status

    docs = scan_documents(output_dir)
    stage3_docs = []

    for doc in docs:
        parsed_files = doc.get('parsed_files', [])
        short_title = doc.get('short_title', 'Unknown Document')

        for pf in parsed_files:
            processed_file = pf.get('processed_file')
            if not processed_file or not Path(processed_file).exists():
                continue

            # Check if Stage 3 complete
            status = get_document_status(pf)
            if status.get('stage_3_complete'):
                # Build display name
                org_units = pf.get('organizational_units', {})
                if org_units:
                    from ui.structure_display import format_organizational_units
                    display_name = f"{short_title} - {format_organizational_units(org_units)}"
                else:
                    display_name = short_title

                # Get file stem for question file pattern
                file_stem = Path(processed_file).stem.replace('_processed', '')

                stage3_docs.append({
                    'display_name': display_name,
                    'processed_file': processed_file,
                    'file_stem': file_stem
                })

    return stage3_docs


def _submit_question_job(processed_file: str, question_text: str, config: Dict):
    """
    Submit question job to queue.

    Args:
        processed_file: Path to processed file
        question_text: Question text
        config: Application configuration
    """
    try:
        db_path = get_job_queue_database(config)
        queue = JobQueue(db_path)

        params = {
            'question_text': question_text,
            'max_items': st.session_state.get('max_items', 300),
            'max_tokens': 1000,
            'max_iterations': 3,
            'qa_mode': st.session_state.get('qa_mode', 'standard'),
            'config': 'config.json'
        }

        job_id = queue.enqueue(
            job_type='question',
            file_path=processed_file,
            params=params,
            max_retries=0
        )

        st.success(f"✅ Question job submitted! Job ID: {job_id}")
        st.info("Switch to **Job History** view to monitor progress.")

        # Clear cache to refresh question list
        st.cache_data.clear()

    except Exception as e:
        show_error_with_action(st,
            "Failed to submit question job",
            "Check worker status",
            "Ensure worker is running: python worker/run_worker.py",
            exception=e)


def render_questions_list(config: Dict, output_dir: str):
    """
    Render list of all questions with filtering and management.

    Args:
        config: Application configuration
        output_dir: Output directory path
    """
    st.subheader("All Questions")

    # Get all questions
    questions = _find_all_questions(output_dir)

    if not questions:
        st.info("❓ **No questions found**")
        st.caption("Use the form above to submit your first question about a document")
        return

    # Render filters
    render_question_filters(questions)

    st.divider()

    # Apply filters
    filtered_questions = _apply_question_filters(questions)

    if not filtered_questions:
        st.info("🔍 **No questions match current filters**")
        st.caption("Try adjusting the document or status filters above")
        return

    st.caption(f"Showing {len(filtered_questions)} question(s)")

    # Render each question
    for question in filtered_questions:
        render_question_card(question, output_dir)


@st.cache_data(ttl=10)
def _find_all_questions(output_dir: str) -> List[Dict[str, Any]]:
    """
    Find all question files in output directory.

    Cached with 10-second TTL to avoid constant re-scanning.

    Args:
        output_dir: Output directory path

    Returns:
        List of question dictionaries
    """
    questions = []
    output_path = Path(output_dir)

    if not output_path.exists():
        return questions

    # Scan for question files: *_question_*.json
    for q_file in output_path.rglob("*_question_*.json"):
        try:
            with open(q_file, 'r', encoding='utf-8') as f:
                q_content = json.load(f)

            # Extract question info
            question_obj = q_content.get('question', {})
            question_text = question_obj.get('text', '')

            # Extract answer info
            working_answer = q_content.get('working_answer', {})
            answer_text = working_answer.get('text', '').strip()

            # Status
            complete = q_content.get('complete', False)

            # Document info (derive from file path)
            doc_dir = q_file.parent
            file_stem = q_file.stem.split('_question_')[0]

            # Try to get document title
            doc_title = _get_document_title_for_question(doc_dir, file_stem)

            questions.append({
                'file_path': str(q_file),
                'file_name': q_file.name,
                'question_text': question_text,
                'answer_text': answer_text,
                'complete': complete,
                'has_answer': bool(answer_text),
                'document_title': doc_title,
                'document_dir': str(doc_dir),
                'file_stem': file_stem
            })

        except Exception as e:
            # Skip malformed question files
            continue

    # Sort by file name (implicit chronological order)
    questions.sort(key=lambda q: q['file_name'], reverse=True)

    return questions


def _get_document_title_for_question(doc_dir: Path, file_stem: str) -> str:
    """
    Get document title for a question file.

    Args:
        doc_dir: Directory containing question file
        file_stem: File stem (before _question_)

    Returns:
        Document title or file stem if not found
    """
    # Try to find processed file
    processed_file = doc_dir / f"{file_stem}_processed.json"

    if processed_file.exists():
        try:
            with open(processed_file, 'r', encoding='utf-8') as f:
                content = json.load(f)

            doc_info = content.get('document_information', {})
            title = doc_info.get('title') or doc_info.get('long_title')

            if title:
                return title
        except Exception:
            pass

    # Fallback to file stem
    return file_stem


def render_question_filters(questions: List[Dict[str, Any]]):
    """
    Render filtering controls for questions.

    Args:
        questions: List of all questions
    """
    col1, col2 = st.columns(2)

    with col1:
        # Document filter
        doc_titles = sorted(set(q['document_title'] for q in questions))
        doc_filter_options = ['all'] + doc_titles

        current_doc_filter = st.session_state.question_doc_filter
        if current_doc_filter not in doc_filter_options:
            current_doc_filter = 'all'
            st.session_state.question_doc_filter = 'all'

        doc_filter = st.selectbox(
            "Filter by Document",
            doc_filter_options,
            index=doc_filter_options.index(current_doc_filter),
            key='question_doc_filter_selectbox'
        )

        if doc_filter != st.session_state.question_doc_filter:
            st.session_state.question_doc_filter = doc_filter
            st.rerun()

    with col2:
        # Status filter
        status_filter_options = ['all', 'complete', 'in_progress']
        current_status_filter = st.session_state.question_status_filter

        if current_status_filter not in status_filter_options:
            current_status_filter = 'all'
            st.session_state.question_status_filter = 'all'

        status_filter = st.selectbox(
            "Filter by Status",
            status_filter_options,
            index=status_filter_options.index(current_status_filter),
            key='question_status_filter_selectbox'
        )

        if status_filter != st.session_state.question_status_filter:
            st.session_state.question_status_filter = status_filter
            st.rerun()


def _apply_question_filters(questions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Apply current filters to question list.

    Args:
        questions: List of all questions

    Returns:
        Filtered list of questions
    """
    filtered = questions

    # Document filter
    if st.session_state.question_doc_filter != 'all':
        filtered = [
            q for q in filtered
            if q['document_title'] == st.session_state.question_doc_filter
        ]

    # Status filter
    if st.session_state.question_status_filter == 'complete':
        filtered = [q for q in filtered if q['complete']]
    elif st.session_state.question_status_filter == 'in_progress':
        filtered = [q for q in filtered if not q['complete']]

    return filtered


def render_question_card(question: Dict[str, Any], output_dir: str):
    """
    Render expandable question card.

    Args:
        question: Question dictionary
        output_dir: Output directory path
    """
    file_path = question['file_path']
    question_text = question['question_text']
    answer_text = question['answer_text']
    complete = question['complete']
    doc_title = question['document_title']

    # Status icon
    if complete:
        status_icon = "✅"
        status_text = "Complete"
    else:
        status_icon = "⏳"
        status_text = "In Progress"

    # Truncate question for title
    question_preview = _truncate_text(question_text, max_length=60)

    # Expandable container
    with st.expander(
        f"{status_icon} **{doc_title}** - {question_preview}",
        expanded=(st.session_state.expanded_question_id == file_path)
    ):
        # Full question
        st.caption("**Question:**")
        st.write(question_text)

        st.divider()

        # Answer
        st.caption("**Answer:**")
        if complete and answer_text:
            st.write(answer_text)
        elif answer_text:
            st.info("Answer in progress...")
            st.write(answer_text)
        else:
            st.warning("Answer not yet generated. Check Job History to monitor progress.")

        st.divider()

        # Actions
        col1, col2 = st.columns([1, 3])

        with col1:
            # Delete button
            if st.button("🗑️ Delete", key=f"delete_{file_path}", use_container_width=True):
                st.session_state.delete_confirm_question = file_path
                st.rerun()

        # Delete confirmation dialog
        if st.session_state.delete_confirm_question == file_path:
            _render_delete_confirmation(file_path)


def _render_delete_confirmation(file_path: str):
    """
    Render delete confirmation dialog.

    Args:
        file_path: Path to question file to delete
    """
    st.divider()
    st.warning("⚠️ **Confirm Deletion**")
    st.caption("Are you sure you want to delete this question? This action cannot be undone.")

    col1, col2 = st.columns(2)

    with col1:
        if st.button("✅ Yes, Delete", key=f"confirm_delete_{file_path}", type="primary"):
            _delete_question_file(file_path)

    with col2:
        if st.button("❌ Cancel", key=f"cancel_delete_{file_path}"):
            st.session_state.delete_confirm_question = None
            st.rerun()


def _delete_question_file(file_path: str):
    """
    Delete question file and refresh view.

    Args:
        file_path: Path to question file
    """
    with st.spinner("Deleting question..."):
        try:
            if Path(file_path).exists():
                os.remove(file_path)
                st.success("✅ Question deleted successfully")

                # Clear state
                st.session_state.delete_confirm_question = None
                st.session_state.expanded_question_id = None

                # Clear cache to refresh list
                st.cache_data.clear()

                st.rerun()
            else:
                st.error("❌ Question file not found")

        except Exception as e:
            show_error(st, "Failed to delete question", exception=e)


def _truncate_text(text: str, max_length: int = 100) -> str:
    """
    Truncate text for display.

    Args:
        text: Text to truncate
        max_length: Maximum length

    Returns:
        Truncated text with ellipsis if needed
    """
    if len(text) <= max_length:
        return text
    return text[:max_length] + "..."
