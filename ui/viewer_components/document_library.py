"""
Document Library Component - Browse and select processed documents.

Displays a list of all processed documents with summaries, allowing users
to select one for detailed exploration.
"""
# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License

import streamlit as st
from pathlib import Path
from typing import List, Dict, Any
import json


def get_parser_type_from_processed_file(processed_file_path: str) -> str:
    """
    Determine parser type by reading the manifest file.

    Handles three manifest patterns:
    1. Per-file manifests: {filename}_manifest.json (CA, EU)
    2. Directory-level manifests: {dirname}_manifest.json
    3. USC-style manifests: {usc_prefix}_manifest.json (e.g., usc01_manifest.json)

    Args:
        processed_file_path: Path to processed document file

    Returns:
        Parser type string (e.g., 'uslm', 'formex', 'ca_html', or 'unknown')
    """
    try:
        processed_path = Path(processed_file_path)

        # Try pattern 1: Per-file manifest
        # Extract base name by removing _processed.json or _parse_output.json
        file_stem = processed_path.stem
        if file_stem.endswith('_processed'):
            base_name = file_stem[:-len('_processed')]
        elif file_stem.endswith('_parse_output'):
            base_name = file_stem[:-len('_parse_output')]
        else:
            base_name = file_stem

        manifest_path = processed_path.parent / f"{base_name}_manifest.json"

        # Try pattern 2: Directory-level manifest
        if not manifest_path.exists():
            dir_name = processed_path.parent.name
            manifest_path = processed_path.parent / f"{dir_name}_manifest.json"

        # Try pattern 3: USC-style manifest (usc##_manifest.json)
        # For files like usc01_title1_chapter1_processed.json -> usc01_manifest.json
        if not manifest_path.exists():
            if '_title' in base_name and base_name.startswith('usc'):
                usc_prefix = base_name.split('_title')[0]
                manifest_path = processed_path.parent / f"{usc_prefix}_manifest.json"

        if not manifest_path.exists():
            return 'unknown'

        # Read manifest and extract parser type
        with open(manifest_path, 'r', encoding='utf-8') as f:
            manifest = json.load(f)

        # Use same logic as processing UI (manifest_utils.get_parser_from_manifest)
        parser_type = manifest.get('parser_type')
        if parser_type:
            return parser_type

        # Fallback to legacy source_type field
        source_type = manifest.get('source_type')
        if source_type:
            return source_type

        return 'unknown'

    except Exception as e:
        # If we can't read the manifest, return unknown
        return 'unknown'


def get_parser_display_name(parser_type: str) -> str:
    """
    Get user-friendly display name for parser type.

    Args:
        parser_type: Parser type identifier

    Returns:
        User-friendly display name
    """
    display_names = {
        'uslm': 'United States Code',
        'formex': 'EU Regulations',
        'ca_html': 'California Statutes',
        'ecfr': 'CFR',
    }
    return display_names.get(parser_type, parser_type.title())


def scan_processed_documents(output_dir: str) -> Dict[str, List[Dict[str, Any]]]:
    """
    Scan output directory for processed documents, grouped by parser type.

    Args:
        output_dir: Path to output directory

    Returns:
        Dict mapping parser_type to list of document info dicts
    """
    from collections import defaultdict

    docs_by_type = defaultdict(list)
    output_path = Path(output_dir)

    if not output_path.exists():
        return {}

    # Find all processed documents
    for doc_file in output_path.rglob("*_processed.json"):
        try:
            with open(doc_file, 'r', encoding='utf-8') as f:
                content = json.load(f)

            doc_info = content.get('document_information', {})
            processing_status = doc_info.get('processing_status', {})

            # Only include if Stage 3 is complete (has summaries)
            if not processing_status.get('stage_3_complete'):
                continue

            # Get document-level summary (from top-level org unit or first available)
            doc_summary_1 = None
            doc_summary_2 = None

            org_content = doc_info.get('organization', {}).get('content', {})
            for org_type, org_units in org_content.items():
                for unit_id, unit_data in org_units.items():
                    # If this is the only top-level unit, use its summary as document summary
                    if len(org_units) == 1:
                        doc_summary_1 = unit_data.get('summary_1', '')
                        doc_summary_2 = unit_data.get('summary_2', '')
                        break
                if doc_summary_1 or doc_summary_2:
                    break

            # Determine parser type from manifest
            parser_type = get_parser_type_from_processed_file(str(doc_file))
            parser_dir = doc_file.parent.name

            doc_dict = {
                'file_path': str(doc_file),
                'file_name': doc_file.name,
                'title': doc_info.get('title', 'Untitled Document'),
                'long_title': doc_info.get('long_title', ''),
                'topic_statement': doc_info.get('topic_statement', ''),
                'summary_1': doc_summary_1 or '',
                'summary_2': doc_summary_2 or '',
                'status': processing_status,
                'parser_dir': parser_dir,
                'parser_type': parser_type,
                'content': content  # Store for disambiguation
            }

            docs_by_type[parser_type].append(doc_dict)

        except Exception as e:
            # Skip files that can't be loaded
            continue

    # Disambiguate duplicate titles within each parser type
    for parser_type, docs in docs_by_type.items():
        _disambiguate_duplicate_titles(docs)

        # Sort by title
        docs.sort(key=lambda x: x['title'])

    return dict(docs_by_type)


def _disambiguate_duplicate_titles(docs: List[Dict[str, Any]]) -> None:
    """
    Detect documents with duplicate titles and add disambiguating information.

    Uses same logic as processing UI to add section/article ranges.
    Modifies docs list in place.
    """
    from collections import defaultdict

    # Group documents by title
    title_groups = defaultdict(list)
    for doc in docs:
        title = doc.get('title', '')
        if title:
            title_groups[title].append(doc)

    # Process groups with duplicates
    for title, doc_list in title_groups.items():
        if len(doc_list) <= 1:
            continue  # No duplicates

        # Add distinguishing information to each duplicate
        for doc in doc_list:
            content = doc.get('content', {})
            if not content:
                continue

            # Find first and last operational units
            unit_type, first_designation, last_designation = _find_unit_range(content)

            if unit_type and first_designation and last_designation:
                # Add unit range to title
                unit_label = unit_type.capitalize()
                if first_designation == last_designation:
                    doc['title'] = f"{title} ({unit_label} {first_designation})"
                else:
                    doc['title'] = f"{title} ({unit_label} {first_designation}-{last_designation})"
            elif unit_type and first_designation:
                # Only have first designation
                doc['title'] = f"{title} ({unit_type.capitalize()} {first_designation})"


def _find_unit_range(parsed_content: Dict[str, Any]) -> tuple:
    """
    Find the first and last operational substantive units in the document.

    Returns:
        Tuple of (unit_type, first_designation, last_designation)
    """
    doc_info = parsed_content.get('document_information', {})
    parameters = doc_info.get('parameters', {})
    content = parsed_content.get('content', {})

    # Find operational parameters
    operational_params = [
        (param_id, param_data.get('name_plural', 'units'))
        for param_id, param_data in parameters.items()
        if param_data.get('operational') == 1
    ]

    if not operational_params:
        return None, None, None

    # Use first operational parameter type
    _, unit_type_plural = operational_params[0]

    # Get units from content
    units = content.get(unit_type_plural, {})

    if not units:
        return None, None, None

    # Get sorted unit numbers
    unit_numbers = sorted(units.keys(), key=lambda x: (
        int(x.split('-')[0]) if x.split('-')[0].isdigit() else float('inf'),
        x
    ))

    if not unit_numbers:
        return None, None, None

    first_designation = unit_numbers[0]
    last_designation = unit_numbers[-1]

    # Get singular form of unit type
    unit_type_singular = unit_type_plural[:-1] if unit_type_plural.endswith('s') else unit_type_plural

    return unit_type_singular, first_designation, last_designation


def render_document_library():
    """Render the document library view."""
    st.title("📚 Processed Document Library")

    st.markdown("""
    Browse all fully processed documents with AI-generated summaries.
    Select a document to explore its structure, summaries, and full text.
    """)

    st.markdown("---")

    # Configuration in sidebar
    with st.sidebar:
        st.subheader("Library Settings")

        # Output directory selection
        if 'viewer_output_dir' not in st.session_state:
            # Try to get from config
            try:
                import sys
                import os
                sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
                from utils.config import get_config, get_output_directory
                config = get_config()
                st.session_state.viewer_output_dir = get_output_directory(config)
            except:
                st.session_state.viewer_output_dir = os.path.join(os.path.expanduser("~"), "document_analyzer_output")

        output_dir = st.text_input(
            "Output Directory",
            value=st.session_state.viewer_output_dir,
            help="Directory containing processed documents"
        )

        if st.button("🔄 Refresh Library", use_container_width=True):
            st.session_state.viewer_output_dir = output_dir
            st.cache_data.clear()
            st.rerun()

        st.markdown("---")

        # Summary level for library preview
        preview_level = st.radio(
            "Preview Summary Level",
            options=[1, 2],
            format_func=lambda x: f"Level {x} ({'Brief' if x == 1 else 'Detailed'})",
            key='library_preview_level',
            help="Choose summary detail level for document previews"
        )

    # Scan for documents (grouped by parser type)
    with st.spinner("Scanning for processed documents..."):
        docs_by_type = scan_processed_documents(output_dir)

    if not docs_by_type:
        st.warning("⚠️ No fully processed documents found.")
        st.info(f"""
        **Looking for documents in:** `{output_dir}`

        Documents must have completed Stage 3 processing to appear here.

        To process documents:
        1. Use the main UI: `streamlit run ui/app.py`
        2. Or use batch processing: `python batch_process.py <directory> --stages 2,3`
        """)
        return

    # Count total documents
    total_docs = sum(len(docs) for docs in docs_by_type.values())
    st.success(f"✅ Found {total_docs} processed document(s) in {len(docs_by_type)} category/categories")
    st.markdown("")

    # Global search filter
    search_query = st.text_input(
        "🔍 Search all documents",
        placeholder="Search by title or content...",
        key='library_search'
    )

    st.markdown("---")

    # Display documents grouped by parser type
    for parser_type in sorted(docs_by_type.keys()):
        docs = docs_by_type[parser_type]

        # Filter by search
        if search_query:
            search_lower = search_query.lower()
            docs = [
                doc for doc in docs
                if search_lower in doc['title'].lower()
                or search_lower in doc.get('topic_statement', '').lower()
                or search_lower in doc.get('summary_1', '').lower()
                or search_lower in doc.get('summary_2', '').lower()
            ]

        if not docs:
            continue  # Skip empty categories after search

        # Category header
        display_name = get_parser_display_name(parser_type)
        st.subheader(f"📚 {display_name}")
        st.caption(f"{len(docs)} document{'s' if len(docs) != 1 else ''}")

        # Display documents in this category
        for doc in docs:
            with st.container():
                # Document header
                col_title, col_action = st.columns([5, 1])

                with col_title:
                    st.markdown(f"**📄 {doc['title']}**")
                    if doc['long_title'] and doc['long_title'] != doc['title']:
                        st.caption(doc['long_title'])
                    # Display topic statement for quick scanning
                    topic_statement = doc.get('topic_statement', '')
                    if topic_statement:
                        st.caption(f"*{topic_statement}*")

                with col_action:
                    if st.button(
                        "Open →",
                        key=f"open_doc_{doc['file_path']}",
                        use_container_width=True
                    ):
                        # Load this document and switch to overview
                        from ui.view_document import load_document
                        # Remove 'content' key before loading (it's just for disambiguation)
                        loaded_doc = doc.get('content')
                        if loaded_doc:
                            st.session_state.viewer_document = loaded_doc
                            st.session_state.viewer_file_path = doc['file_path']
                            st.session_state.viewer_current_view = 'overview'
                            st.session_state.viewer_nav_stack = []
                            st.rerun()

                # Document summary preview
                summary_to_show = doc.get(f'summary_{preview_level}', '') or doc.get('summary_1', '')

                if summary_to_show:
                    # Truncate for preview (initially)
                    with st.expander("📝 View Summary", expanded=False):
                        st.markdown(summary_to_show)
                else:
                    st.info("No summary available")

                # Metadata
                st.caption(f"📄 File: {doc['file_name']}")

                st.markdown("---")

        # Spacing between categories
        st.markdown("")
        st.markdown("")
