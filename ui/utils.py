"""
UI Utility Functions for Document Analyzer Streamlit Interface.

Helper functions for column layout management, document status checking,
and other UI-related operations.
"""
# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License

import os
from pathlib import Path
from typing import Dict, List, Optional, Tuple


def should_show_column_4(doc: Optional[Dict]) -> bool:
    """
    Determine if Column 4 (sub-documents) should be displayed.

    Column 4 is shown when the selected document has multiple parsed files
    (split document). For single-file documents, Column 4 is hidden and
    Column 5 gets extra horizontal space.

    Args:
        doc: Document dictionary from scan_documents()

    Returns:
        bool: True if Column 4 should be visible, False otherwise
    """
    if not doc:
        return False
    parsed_files = doc.get('parsed_files', [])
    return len(parsed_files) > 1


def get_column_widths(show_col4: bool) -> List[float]:
    """
    Get dynamic column widths based on Column 4 visibility.

    Args:
        show_col4: Whether Column 4 should be displayed

    Returns:
        List of column width ratios for st.columns()

    Examples:
        >>> get_column_widths(True)
        [1, 1.5, 2, 1.5, 2.5]  # All 5 columns
        >>> get_column_widths(False)
        [1, 1.5, 2, 0, 4]  # Column 4 hidden, Column 5 wider
    """
    if show_col4:
        # All columns visible
        return [1, 1.5, 2, 1.5, 2.5]
    else:
        # Column 4 hidden, Column 5 gets extra space
        return [1, 1.5, 2, 0, 4]


def get_overall_document_status(doc: Dict) -> Dict[str, bool]:
    """
    Get aggregated status for document across all parsed files.

    Checks status of all parsed files in document and returns aggregate status.
    Useful for displaying document-level status indicators in Column 3.

    Args:
        doc: Document dictionary from scan_documents()

    Returns:
        Dict with keys:
            - any_parsed: True if any parsed file exists
            - all_stage_2: True if all parsed files have Stage 2 complete
            - all_stage_3: True if all parsed files have Stage 3 complete
    """
    from ui.app import get_document_status  # Import here to avoid circular dependency

    parsed_files = doc.get('parsed_files', [])

    if not parsed_files:
        return {
            'any_parsed': False,
            'all_stage_2': False,
            'all_stage_3': False
        }

    statuses = [get_document_status(pf) for pf in parsed_files]

    return {
        'any_parsed': any(s['parsed'] for s in statuses),
        'all_stage_2': all(s['stage_2_complete'] for s in statuses),
        'all_stage_3': all(s['stage_3_complete'] for s in statuses)
    }


def get_status_icons(status: Dict[str, bool]) -> List[str]:
    """
    Convert status dictionary to list of status icon strings.

    Args:
        status: Status dict from get_overall_document_status() or get_document_status()

    Returns:
        List of status icons (e.g., ['✅', 'S2', 'S3'])
    """
    icons = []

    if status.get('parsed') or status.get('any_parsed'):
        icons.append('✅')

    if status.get('stage_2_complete') or status.get('all_stage_2'):
        icons.append('S2')

    if status.get('stage_3_complete') or status.get('all_stage_3'):
        icons.append('S3')

    return icons


def format_status_badge(status: Dict[str, bool]) -> str:
    """
    Format status dictionary as a badge string for display.

    Args:
        status: Status dict from get_overall_document_status() or get_document_status()

    Returns:
        Formatted status string (e.g., "✅ | S2 | S3" or "Not started")
    """
    icons = get_status_icons(status)
    return " | ".join(icons) if icons else "Not started"


def init_session_state(st) -> None:
    """
    Initialize all session state variables with defaults.

    Centralized initialization to avoid scattered if-checks throughout code.
    Call this once at app startup.

    Args:
        st: Streamlit module instance
    """
    defaults = {
        # View control
        'view_mode': 'Browse Documents',

        # Navigation (hierarchical)
        'selected_category': None,      # Parser type string
        'selected_doc': None,           # Full document dict
        'selected_chapter': None,       # Parsed file dict

        # Processing
        'monitoring_job_id': None,
        'processing_active': False,

        # Config
        'max_items': 300,
        'checkpoint_threshold': 30,

        # Pagination
        'doc_page': 0,  # Current page in Column 3

        # Job History view
        'job_history_page': 0,
        'job_history_filter_status': 'all',
        'selected_job_id': None,
        'show_logs_job_id': None,

        # Questions view
        'question_doc_filter': 'all',
        'question_status_filter': 'all',
        'expanded_question_id': None,
        'delete_confirm_question': None,

        # Legacy fields (for compatibility during transition)
        'processing_stage': None,
        'processing_file': None,
        'processing_complete': False,
        'processing_process': None,
        'process_start_time': None,
        'last_progress_check': None
    }

    for key, default_value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = default_value


def format_file_size(size_bytes: int) -> str:
    """
    Format file size in human-readable format.

    Args:
        size_bytes: File size in bytes

    Returns:
        Formatted string (e.g., "1.5 MB", "234 KB")
    """
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} KB"
    elif size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.1f} MB"
    else:
        return f"{size_bytes / (1024 * 1024 * 1024):.1f} GB"


def get_file_size(file_path: str) -> Optional[int]:
    """
    Get file size in bytes, returning None if file doesn't exist.

    Args:
        file_path: Path to file

    Returns:
        File size in bytes, or None if file doesn't exist
    """
    try:
        return os.path.getsize(file_path)
    except (OSError, FileNotFoundError):
        return None


def truncate_text(text: str, max_length: int = 80, suffix: str = "...") -> str:
    """
    Truncate text to maximum length with suffix.

    Args:
        text: Text to truncate
        max_length: Maximum length (including suffix)
        suffix: Suffix to append when truncated

    Returns:
        Truncated text with suffix, or original if shorter than max_length
    """
    if len(text) <= max_length:
        return text
    return text[:max_length - len(suffix)] + suffix


def show_toast(st, message: str, toast_type: str = 'info'):
    """
    Display a toast notification message.

    Toast notifications provide quick feedback for user actions.
    Uses Streamlit's native messaging components with consistent styling.

    Args:
        st: Streamlit module instance
        message: Message text to display
        toast_type: Type of toast ('success', 'info', 'warning', 'error')

    Examples:
        >>> show_toast(st, "Job submitted successfully", 'success')
        >>> show_toast(st, "Cache cleared", 'info')
        >>> show_toast(st, "Worker not responding", 'warning')
    """
    if toast_type == 'success':
        st.success(message)
    elif toast_type == 'warning':
        st.warning(message)
    elif toast_type == 'error':
        st.error(message)
    else:  # 'info' or default
        st.info(message)


def show_error(st, message: str, exception: Optional[Exception] = None, details: Optional[str] = None):
    """
    Display a user-friendly error message with optional technical details.

    Args:
        st: Streamlit module instance
        message: User-friendly error message
        exception: Exception object (optional)
        details: Additional technical details (optional)

    Examples:
        >>> show_error(st, "Failed to load document")
        >>> show_error(st, "Failed to load document", exception=e)
        >>> show_error(st, "Failed to submit job", details="Connection timeout")
    """
    st.error(f"❌ {message}")

    # Show technical details in expander if provided
    if exception or details:
        with st.expander("Show technical details", expanded=False):
            if exception:
                import traceback
                st.code(traceback.format_exc(), language='text')
            elif details:
                st.code(details, language='text')


def show_error_with_action(st, message: str, action_label: str, action_info: str,
                           exception: Optional[Exception] = None):
    """
    Display an error message with actionable next steps.

    Args:
        st: Streamlit module instance
        message: User-friendly error message
        action_label: Label for the action (e.g., "To fix")
        action_info: Information about how to resolve (e.g., command to run)
        exception: Exception object (optional)

    Examples:
        >>> show_error_with_action(st,
        ...     "Worker not responding",
        ...     "To start the worker",
        ...     "python worker/run_worker.py")
    """
    st.error(f"❌ {message}")

    # Show actionable next steps
    st.info(f"**{action_label}:** {action_info}")

    # Show technical details if exception provided
    if exception:
        with st.expander("Show technical details", expanded=False):
            import traceback
            st.code(traceback.format_exc(), language='text')


def show_error_with_retry(st, message: str, retry_callback: callable, retry_label: str = "Retry",
                          exception: Optional[Exception] = None):
    """
    Display an error message with a retry button.

    Args:
        st: Streamlit module instance
        message: User-friendly error message
        retry_callback: Function to call when retry button is clicked
        retry_label: Label for retry button (default: "Retry")
        exception: Exception object (optional)

    Examples:
        >>> def retry_load():
        ...     st.session_state.reload_trigger = True
        ...     st.rerun()
        >>> show_error_with_retry(st, "Failed to load jobs", retry_load)
    """
    st.error(f"❌ {message}")

    # Show retry button
    if st.button(f"🔄 {retry_label}", key=f"retry_{id(message)}"):
        retry_callback()

    # Show technical details if exception provided
    if exception:
        with st.expander("Show technical details", expanded=False):
            import traceback
            st.code(traceback.format_exc(), language='text')
