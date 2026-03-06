"""
Document Analyzer - Streamlit UI

Unified interface for managing document parsing, processing, and question-answering workflows.
"""
# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License

import streamlit as st
import json
import os
import subprocess
import time
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime

# Import project modules
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.config import get_config, get_output_directory, get_checkpoint_threshold, get_job_queue_database
from utils.api_cache import APICache
from utils.manifest_utils import find_manifests, ManifestManager, get_parser_from_manifest
from utils.processing_status import (
    get_processing_status,
    is_stage_2_complete,
    is_stage_3_complete
)
from parsers.registry import get_parser, load_parsers_from_config, get_registry
from ui.document_scanner import scan_all_document_roots, get_available_parsers
from ui.structure_display import display_document_structure, format_organizational_units
from ui.column_layout import render_column_layout
from ui.utils import init_session_state
from worker.queue import JobQueue


# Page configuration (moved to main() function below)


# ============================================================================
# Helper Functions
# ============================================================================

def get_parser_display_name(parser_type: str) -> str:
    """
    Get user-friendly display name for parser type.
    
    Args:
        parser_type: Parser type identifier (e.g., 'uslm', 'formex', 'ca_html')
        
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


def extract_short_title(parse_file_path: Optional[str]) -> Optional[str]:
    """
    Extract short title from a parsed JSON file.
    
    Args:
        parse_file_path: Path to parsed JSON file
        
    Returns:
        Short title if found, None otherwise
    """
    if not parse_file_path or not Path(parse_file_path).exists():
        return None
    
    try:
        with open(parse_file_path, 'r', encoding='utf-8') as f:
            content = json.load(f)
        
        # Try to get short title from document_information.title
        doc_info = content.get('document_information', {})
        short_title = doc_info.get('title')
        
        if short_title and short_title.strip():
            return short_title.strip()
        
        # Fallback to long_title if short title not available
        long_title = doc_info.get('long_title')
        if long_title and long_title.strip():
            return long_title.strip()
        
        return None
    except Exception:
        return None


@st.cache_data(ttl=10)  # Reduced TTL to refresh more frequently
def scan_documents(output_dir: str) -> List[Dict[str, Any]]:
    """
    Scan output directory for manifest files and build document tree.
    
    Uses discovery-based organizational units from manifests.
    
    Args:
        output_dir: Path to output directory
        
    Returns:
        List of document dictionaries with manifest info and parsed files
    """
    docs = []
    output_path = Path(output_dir)
    
    if not output_path.exists():
        return docs
    
    # Find all manifest files
    for manifest_path in output_path.rglob("*_manifest.json"):
        try:
            manifest_mgr = ManifestManager(str(manifest_path))
            manifest = manifest_mgr.load()
            
            # Get parser type from manifest (discovery-based)
            parser_type = get_parser_from_manifest(manifest)
            
            doc = {
                'manifest_path': str(manifest_path),
                'manifest_dir': str(manifest_path.parent),
                'source_file': manifest.get('source_file', ''),
                'source_type': manifest.get('source_type', ''),  # Legacy
                'parser': manifest.get('parser', ''),  # Legacy
                'parser_type': parser_type,  # New field
                'parsed_files': [],  # Renamed from 'chapters' to be more generic
                'short_title': manifest.get('short_title')  # Try manifest first, will fallback to parsed files
            }
            
            # Process parsed files from manifest
            for pf in manifest.get('parsed_files', []):
                # Build organizational_units from manifest fields (title, chapter, etc.)
                org_units = {}
                for key in ['title', 'chapter', 'part', 'section']:
                    if key in pf and pf[key]:
                        org_units[key] = pf[key]

                parsed_file = {
                    'type': pf.get('type', 'unknown'),
                    'file': pf.get('file', ''),
                    'organizational_units': org_units,  # Built from manifest fields
                    'metadata': {k: v for k, v in pf.items()
                               if k not in ['type', 'file', 'organizational_units', 'title', 'chapter', 'part', 'section']}  # Legacy fields
                }
                
                # Compute absolute paths
                parse_file = manifest_path.parent / pf['file']
                parsed_file['parse_file'] = str(parse_file) if parse_file.exists() else None
                
                # Derive processed file path
                if parsed_file['parse_file']:
                    processed_file = Path(parsed_file['parse_file']).with_name(
                        Path(parsed_file['parse_file']).name.replace('_parse_output.json', '_processed.json')
                    )
                    parsed_file['processed_file'] = str(processed_file) if processed_file.exists() else None
                else:
                    parsed_file['processed_file'] = None
                
                # Extract short title from parsed file (use first available)
                if not doc['short_title'] and parsed_file['parse_file']:
                    short_title = extract_short_title(parsed_file['parse_file'])
                    if short_title:
                        doc['short_title'] = short_title

                doc['parsed_files'].append(parsed_file)

            # For split documents, use organizational units from first file to build parent-level title
            # Example: If first file is "Title 2, CHAPTER 1", we want document title to be just "Title 2"
            if len(doc['parsed_files']) > 1 and doc['parsed_files']:
                first_file_org_units = doc['parsed_files'][0].get('organizational_units', {})
                # If we have parent organizational unit (like 'title'), use it for the document name
                if 'title' in first_file_org_units:
                    # Build title from parent units only (exclude chapter, part, section, etc.)
                    doc['short_title'] = f"Title {first_file_org_units['title']}"
            
            docs.append(doc)
        except Exception as e:
            # Skip corrupted manifests
            continue

    # Detect and fix duplicate short_titles by adding section range information
    _disambiguate_duplicate_titles(docs)

    return docs


def _disambiguate_duplicate_titles(docs: List[Dict[str, Any]]) -> None:
    """
    Detect documents with duplicate short_titles and add disambiguating information.

    For documents with the same title and no multi-part splits, adds section range
    from the parsed content (e.g., "BPC" -> "BPC (§2050-§2078)").

    Modifies docs list in place.
    """
    # Group documents by short_title
    from collections import defaultdict
    title_groups = defaultdict(list)
    for doc in docs:
        short_title = doc.get('short_title', '')
        if short_title:
            title_groups[short_title].append(doc)

    # Process groups with duplicates
    for title, doc_list in title_groups.items():
        if len(doc_list) <= 1:
            continue  # No duplicates

        # Check if these are single-file documents (not multi-part splits)
        for doc in doc_list:
            parsed_files = doc.get('parsed_files', [])
            if len(parsed_files) != 1:
                continue  # Skip multi-part documents

            # Try to extract section range from parsed content
            parse_file = parsed_files[0].get('parse_file')
            if not parse_file:
                continue

            try:
                import json
                with open(parse_file, 'r', encoding='utf-8') as f:
                    content = json.load(f)

                # Find first and last operational units
                unit_type, first_designation, last_designation = _find_section_range(content)

                if unit_type and first_designation and last_designation:
                    # Add unit range to title
                    unit_label = unit_type.capitalize()
                    if first_designation == last_designation:
                        doc['short_title'] = f"{title} ({unit_label} {first_designation})"
                    else:
                        doc['short_title'] = f"{title} ({unit_label} {first_designation}-{last_designation})"
                elif unit_type and first_designation:
                    # Only have first designation
                    doc['short_title'] = f"{title} ({unit_type.capitalize()} {first_designation})"
            except Exception:
                # If we can't read the file or find section info, leave title as is
                pass


def _find_section_range(parsed_content: Dict[str, Any]) -> tuple:
    """
    Find the first and last operational substantive units in the document.

    Uses the parameters section to identify operational units, then examines
    the content section to find the first and last unit designations.

    Returns:
        tuple: (unit_type_name, first_designation, last_designation) or (None, None, None)
    """
    from utils.document_handling import get_operational_item_name_set

    try:
        # Get operational item names from parameters
        operational_names = get_operational_item_name_set(parsed_content)
        if not operational_names:
            return (None, None, None)

        # Get parameters to find plural form
        params = parsed_content.get('document_information', {}).get('parameters', {})

        # Map operational singular names to their plural forms
        operational_plurals = {}
        for param_key, param_data in params.items():
            if param_data.get('operational') == 1:
                name = param_data.get('name')
                name_plural = param_data.get('name_plural')
                if name and name_plural:
                    operational_plurals[name] = name_plural

        # Check content section for operational units
        content = parsed_content.get('content', {})

        # Find first operational unit type that exists in content
        first_unit_type = None
        first_designation = None
        last_designation = None

        for singular_name, plural_name in operational_plurals.items():
            if plural_name in content and content[plural_name]:
                # Found operational units in content
                unit_dict = content[plural_name]
                if isinstance(unit_dict, dict) and unit_dict:
                    # Get first and last keys (designations)
                    designations = list(unit_dict.keys())
                    if designations:
                        first_unit_type = singular_name
                        first_designation = designations[0]
                        last_designation = designations[-1]
                        break

        return (first_unit_type, first_designation, last_designation)

    except Exception:
        return (None, None, None)


def get_document_status(parsed_file: Dict[str, Any]) -> Dict[str, Any]:
    """
    Get detailed processing status for a parsed file.
    
    Args:
        parsed_file: Parsed file dictionary with file paths (can be chapter or any parsed file)
        
    Returns:
        Status dictionary with completion flags and progress
    """
    status = {
        'parsed': parsed_file.get('parse_file') is not None,
        'stage_2_complete': False,
        'stage_3_complete': False,
        'stage_2_progress': None,
        'stage_3_progress': None,
        'checkpoint_state': None
    }
    
    # Read processed file if it exists
    processed_file = parsed_file.get('processed_file')
    if processed_file and Path(processed_file).exists():
        try:
            with open(processed_file, 'r', encoding='utf-8') as f:
                content = json.load(f)
            
            proc_status = get_processing_status(content)
            
            status['stage_2_complete'] = is_stage_2_complete(content)
            status['stage_3_complete'] = is_stage_3_complete(content)
            status['stage_2_progress'] = proc_status.get('stage_2_progress')
            status['stage_3_progress'] = proc_status.get('stage_3_progress')
            status['checkpoint_state'] = proc_status.get('checkpoint_state')
        except Exception:
            pass
    
    return status


def find_question_files(output_dir: str) -> List[Dict[str, Any]]:
    """
    Find all question files in output directory.
    
    Args:
        output_dir: Path to output directory
        
    Returns:
        List of question file dictionaries
    """
    questions = []
    output_path = Path(output_dir)
    
    if not output_path.exists():
        return questions
    
    # Find all question files
    for q_file in output_path.rglob("*_question_*.json"):
        try:
            with open(q_file, 'r', encoding='utf-8') as f:
                q_content = json.load(f)
            
            question = {
                'file_path': str(q_file),
                'file_name': q_file.name,
                'question_text': q_content.get('question', {}).get('text', ''),
                'complete': q_content.get('complete', False),
                'has_answer': bool(q_content.get('working_answer', {}).get('text', '').strip()),
                'answer_text': q_content.get('working_answer', {}).get('text', ''),
                'document_dir': str(q_file.parent)
            }
            
            questions.append(question)
        except Exception:
            continue
    
    return questions


def get_cache_stats(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Get cache statistics.
    
    Args:
        config: Configuration dictionary
        
    Returns:
        Cache statistics dictionary
    """
    cache_file = config.get('cache', {}).get('file', 'api_cache.json')
    
    if not os.path.exists(cache_file):
        return {
            'exists': False,
            'total_entries': 0,
            'size_mb': 0.0,
            'last_updated': None
        }
    
    try:
        cache = APICache(cache_file)
        stats = cache.get_cache_stats()
        
        # Get file size
        file_size = os.path.getsize(cache_file)
        size_mb = file_size / (1024 * 1024)
        
        # Get modification time
        mtime = os.path.getmtime(cache_file)
        last_updated = datetime.fromtimestamp(mtime).strftime('%Y-%m-%d %H:%M:%S')
        
        return {
            'exists': True,
            'total_entries': stats.get('size', 0),
            'size_mb': size_mb,
            'last_updated': last_updated,
            'cache_file': cache_file
        }
    except Exception as e:
        return {
            'exists': True,
            'error': str(e),
            'total_entries': 0,
            'size_mb': 0.0
        }


def run_processing_command(script: str, args: List[str], output_dir: str) -> subprocess.Popen:
    """
    Run a processing script as a subprocess.
    
    Args:
        script: Script name (e.g., 'Process_Stage_2.py')
        args: List of command-line arguments
        output_dir: Working directory
        
    Returns:
        subprocess.Popen object
    """
    # Get the script path (assume it's in the project root)
    project_root = Path(__file__).parent.parent
    script_path = project_root / script
    
    # Build command
    cmd = ['python', str(script_path)] + args
    
    # Run subprocess with UTF-8 encoding to handle Unicode characters
    env = os.environ.copy()
    env['PYTHONIOENCODING'] = 'utf-8'
    
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding='utf-8',
        errors='replace',  # Replace problematic characters instead of failing
        cwd=str(project_root),
        bufsize=1,
        env=env
    )
    
    return process


# ============================================================================
# Main UI
# ============================================================================

def main():
    """Main Streamlit application."""

    # Page configuration
    st.set_page_config(
        page_title="Document Analyzer",
        page_icon="📄",
        layout="wide",
        initial_sidebar_state="collapsed"  # Minimize sidebar for column layout
    )

    # Load configuration
    config = get_config()
    output_dir = get_output_directory(config)

    # Initialize session state
    init_session_state(st)
    
    # Sidebar: Configuration & Cache Stats
    with st.sidebar:
        st.header("⚙️ Configuration")
        
        st.subheader("Output Directory")
        st.text(output_dir)
        
        if st.button("🔄 Refresh Documents"):
            st.cache_data.clear()
            st.rerun()
        
        st.divider()
        
        st.header("👷 Worker Status")
        # Check if worker is active by looking for running jobs
        db_path = get_job_queue_database(config)
        queue = JobQueue(db_path)
        stats = queue.get_queue_stats()
        
        # Show database path for debugging
        with st.expander("ℹ️ Database Info", expanded=False):
            st.caption(f"Database: `{db_path}`")
            st.caption("Make sure worker uses the same path:")
            st.code(f"python worker/run_worker.py --db \"{db_path}\"", language="bash")
        
        # Check if there are running jobs (indicates worker is active)
        has_running_jobs = stats.get('running', 0) > 0
        
        if has_running_jobs:
            st.success("✅ Worker Active")
            st.caption(f"{stats.get('running', 0)} job(s) running")
        elif stats.get('queued', 0) > 0:
            st.warning("⚠️ Worker Not Detected")
            st.caption(f"{stats.get('queued', 0)} job(s) queued")
            st.caption("Start worker with:")
            st.code(f"python worker/run_worker.py --db \"{db_path}\"", language="bash")
        else:
            st.info("ℹ️ No Active Jobs")
            st.caption("Worker will start automatically when jobs are submitted")
        
        st.divider()
        
        st.header("📊 Cache Statistics")
        cache_stats = get_cache_stats(config)
        
        if cache_stats.get('exists'):
            st.metric("Total Entries", cache_stats.get('total_entries', 0))
            st.metric("Cache Size", f"{cache_stats.get('size_mb', 0.0):.1f} MB")
            if cache_stats.get('last_updated'):
                st.caption(f"Last updated: {cache_stats['last_updated']}")
        else:
            st.info("No cache file found")
    
    # Main area: Multi-Column Progressive Disclosure UI
    st.title("📄 Document Analyzer")

    # Render the new column-based interface
    render_column_layout(config, output_dir)


if __name__ == '__main__':
    main()
