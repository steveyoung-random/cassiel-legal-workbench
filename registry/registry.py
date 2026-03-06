"""
SQLite-based cross-reference registry.

Tracks cross-document references extracted from processed documents and their
resolution status. Mirrors the structure of worker/queue.py.

Database file: cross_reference_registry.db in the output directory.
"""
# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License

import json
import sqlite3
import os
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, List


class Registry:
    """SQLite-based cross-reference registry."""

    def __init__(self, db_path: str):
        """
        Initialize registry with database path.

        Args:
            db_path: Path to SQLite database file (created if it doesn't exist).
        """
        self.db_path = db_path
        self._init_database()

    def _init_database(self):
        """Initialize database schema if it doesn't exist."""
        schema_path = Path(__file__).parent / 'schema.sql'
        with open(schema_path, 'r') as f:
            schema_sql = f.read()
        with sqlite3.connect(self.db_path) as conn:
            conn.executescript(schema_sql)
            # Migration: add metadata column if upgrading from a pre-metadata schema.
            # ALTER TABLE ADD COLUMN is idempotent via the exception catch.
            try:
                conn.execute("ALTER TABLE corpus_documents ADD COLUMN metadata TEXT")
                conn.commit()
            except sqlite3.OperationalError:
                pass  # column already exists

    def _get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    # ------------------------------------------------------------------
    # Corpus Documents
    # ------------------------------------------------------------------

    def add_document(self, file_path: str, doc_id: Optional[str], title: Optional[str],
                     parser_type: str, stage_reached: int = 1,
                     metadata: Optional[Dict] = None) -> int:
        """
        Add a processed document to the corpus index.

        If the document is already registered (by file_path), returns its existing ID
        and updates doc_id, title, parser_type, stage_reached, and metadata.

        Args:
            file_path:     Absolute path to the processed JSON file.
            doc_id:        document_information.document_id from the JSON, or None.
            title:         Human-readable title for display, or None.
            parser_type:   'uslm', 'cfr', 'ca_html', or 'formex'.
            stage_reached: Highest pipeline stage completed (1=parsed, 2=defs, 3=summaries).
            metadata:      Dict with org_keys and content_items; stored as JSON, or None.

        Returns:
            Integer ID of the corpus_documents row.
        """
        norm_path = self._normalize_path(file_path)
        meta_json = json.dumps(metadata) if metadata is not None else None
        with self._get_connection() as conn:
            cursor = conn.execute(
                "SELECT id FROM corpus_documents WHERE file_path = ?",
                (norm_path,)
            )
            row = cursor.fetchone()
            if row:
                # Update fields; keep existing metadata if no new metadata provided.
                if meta_json is not None:
                    conn.execute(
                        """
                        UPDATE corpus_documents
                        SET doc_id = ?, title = ?, parser_type = ?, stage_reached = ?, metadata = ?
                        WHERE id = ?
                        """,
                        (doc_id, title, parser_type, stage_reached, meta_json, row['id'])
                    )
                else:
                    conn.execute(
                        """
                        UPDATE corpus_documents
                        SET doc_id = ?, title = ?, parser_type = ?, stage_reached = ?
                        WHERE id = ?
                        """,
                        (doc_id, title, parser_type, stage_reached, row['id'])
                    )
                conn.commit()
                return row['id']
            else:
                cursor = conn.execute(
                    """
                    INSERT INTO corpus_documents (file_path, doc_id, title, parser_type, stage_reached, metadata)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (norm_path, doc_id, title, parser_type, stage_reached, meta_json)
                )
                conn.commit()
                return cursor.lastrowid

    def get_document_by_path(self, file_path: str) -> Optional[Dict[str, Any]]:
        """Return corpus_documents row for the given file path, or None."""
        norm_path = self._normalize_path(file_path)
        with self._get_connection() as conn:
            cursor = conn.execute(
                "SELECT * FROM corpus_documents WHERE file_path = ?",
                (norm_path,)
            )
            row = cursor.fetchone()
            return dict(row) if row else None

    def get_document_by_id(self, corpus_doc_id: int) -> Optional[Dict[str, Any]]:
        """Return corpus_documents row for the given integer ID, or None."""
        with self._get_connection() as conn:
            cursor = conn.execute(
                "SELECT * FROM corpus_documents WHERE id = ?",
                (corpus_doc_id,)
            )
            row = cursor.fetchone()
            return dict(row) if row else None

    def get_all_documents(self) -> List[Dict[str, Any]]:
        """Return all registered corpus documents."""
        with self._get_connection() as conn:
            cursor = conn.execute(
                "SELECT * FROM corpus_documents ORDER BY added_at"
            )
            return [dict(r) for r in cursor.fetchall()]

    def mark_document_scanned(self, corpus_doc_id: int):
        """Update last_scanned_at to now for the given document."""
        with self._get_connection() as conn:
            conn.execute(
                "UPDATE corpus_documents SET last_scanned_at = CURRENT_TIMESTAMP WHERE id = ?",
                (corpus_doc_id,)
            )
            conn.commit()

    def update_document_metadata(self, corpus_doc_id: int, metadata: Dict):
        """Store or replace the metadata JSON for a corpus document."""
        with self._get_connection() as conn:
            conn.execute(
                "UPDATE corpus_documents SET metadata = ? WHERE id = ?",
                (json.dumps(metadata), corpus_doc_id)
            )
            conn.commit()

    def update_document_stage(self, corpus_doc_id: int, stage_reached: int):
        """Update the highest pipeline stage completed for a document."""
        with self._get_connection() as conn:
            conn.execute(
                "UPDATE corpus_documents SET stage_reached = ? WHERE id = ?",
                (stage_reached, corpus_doc_id)
            )
            conn.commit()

    # ------------------------------------------------------------------
    # Cross-References
    # ------------------------------------------------------------------

    def add_reference(self, source_doc_id: int, source_item_type: str,
                      source_item_number: str, ref_text: str,
                      ref_context: str, term: Optional[str] = None) -> int:
        """
        Record a cross-reference extracted from a processed document.

        If an identical reference already exists (same source + ref_text + ref_context),
        returns the existing ID without modifying resolution fields (idempotent re-scan).

        Args:
            source_doc_id:      ID from corpus_documents.
            source_item_type:   'section', 'appendix', 'supplement', etc.
            source_item_number: Item number within the source document.
            ref_text:           Raw citation string as it appears in the document.
            ref_context:        'need_ref' or 'definition'.
            term:               Defined term (only when ref_context='definition').

        Returns:
            Integer ID of the cross_references row.
        """
        with self._get_connection() as conn:
            # Match the partial unique index for each ref_context.
            # need_ref: unique on (source, ref_text) — term is always NULL, not in key.
            # definition: unique on (source, ref_text, term) — NULL term uses IS NULL.
            if ref_context == 'need_ref':
                cursor = conn.execute(
                    """
                    SELECT id FROM cross_references
                    WHERE source_doc_id = ? AND source_item_type = ? AND source_item_number = ?
                      AND ref_text = ? AND ref_context = 'need_ref'
                    """,
                    (source_doc_id, source_item_type, source_item_number, ref_text)
                )
            elif term is not None:
                cursor = conn.execute(
                    """
                    SELECT id FROM cross_references
                    WHERE source_doc_id = ? AND source_item_type = ? AND source_item_number = ?
                      AND ref_text = ? AND ref_context = 'definition' AND term = ?
                    """,
                    (source_doc_id, source_item_type, source_item_number, ref_text, term)
                )
            else:
                cursor = conn.execute(
                    """
                    SELECT id FROM cross_references
                    WHERE source_doc_id = ? AND source_item_type = ? AND source_item_number = ?
                      AND ref_text = ? AND ref_context = 'definition' AND term IS NULL
                    """,
                    (source_doc_id, source_item_type, source_item_number, ref_text)
                )
            row = cursor.fetchone()
            if row:
                return row['id']
            cursor = conn.execute(
                """
                INSERT INTO cross_references
                    (source_doc_id, source_item_type, source_item_number,
                     ref_text, ref_context, term)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (source_doc_id, source_item_type, source_item_number,
                 ref_text, ref_context, term)
            )
            conn.commit()
            return cursor.lastrowid

    def resolve_reference(self, ref_id: int, target_doc_id: int,
                          target_item_type: Optional[str], target_item_number: Optional[str],
                          resolution_method: str, notes: Optional[str] = None):
        """
        Mark a reference as resolved to a specific corpus document (and optionally item).

        Args:
            ref_id:              ID from cross_references.
            target_doc_id:       ID from corpus_documents for the target document.
            target_item_type:    Item type in the target doc, or None for doc-level match.
            target_item_number:  Item number in the target doc, or None for doc-level match.
            resolution_method:   'automatic' or 'manual'.
            notes:               Optional explanation of how the resolution was determined.
        """
        with self._get_connection() as conn:
            conn.execute(
                """
                UPDATE cross_references
                SET resolution_status = 'resolved',
                    target_doc_id = ?,
                    target_item_type = ?,
                    target_item_number = ?,
                    resolution_method = ?,
                    resolved_at = CURRENT_TIMESTAMP,
                    notes = ?
                WHERE id = ?
                """,
                (target_doc_id, target_item_type, target_item_number,
                 resolution_method, notes, ref_id)
            )
            conn.commit()

    def mark_ambiguous(self, ref_id: int, notes: str, resolution_method: str = 'automatic'):
        """Mark a reference as ambiguous (matched multiple corpus documents)."""
        with self._get_connection() as conn:
            conn.execute(
                """
                UPDATE cross_references
                SET resolution_status = 'ambiguous',
                    resolution_method = ?,
                    resolved_at = CURRENT_TIMESTAMP,
                    notes = ?
                WHERE id = ?
                """,
                (resolution_method, notes, ref_id)
            )
            conn.commit()

    def mark_not_in_corpus(self, ref_id: int, notes: Optional[str] = None):
        """Mark a reference as confirmed not present in the corpus."""
        with self._get_connection() as conn:
            conn.execute(
                """
                UPDATE cross_references
                SET resolution_status = 'not_in_corpus',
                    resolution_method = 'automatic',
                    resolved_at = CURRENT_TIMESTAMP,
                    notes = ?
                WHERE id = ?
                """,
                (notes, ref_id)
            )
            conn.commit()

    def get_references(self, source_doc_id: Optional[int] = None,
                       resolution_status: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Return cross-references, optionally filtered by source document and/or status.

        Args:
            source_doc_id:     Filter to references from this document (None = all).
            resolution_status: Filter to this status (None = all).

        Returns:
            List of cross_references rows as dicts.
        """
        conditions = []
        params = []
        if source_doc_id is not None:
            conditions.append("source_doc_id = ?")
            params.append(source_doc_id)
        if resolution_status is not None:
            conditions.append("resolution_status = ?")
            params.append(resolution_status)

        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
        with self._get_connection() as conn:
            cursor = conn.execute(
                f"SELECT * FROM cross_references {where} ORDER BY source_doc_id, id",
                params
            )
            return [dict(r) for r in cursor.fetchall()]

    def get_reference_by_id(self, ref_id: int) -> Optional[Dict[str, Any]]:
        """Return a cross_references row by ID, or None."""
        with self._get_connection() as conn:
            cursor = conn.execute(
                "SELECT * FROM cross_references WHERE id = ?",
                (ref_id,)
            )
            row = cursor.fetchone()
            return dict(row) if row else None

    def get_references_to_document(self, target_doc_id: int) -> List[Dict[str, Any]]:
        """Return all cross_references resolved to the given target document."""
        with self._get_connection() as conn:
            cursor = conn.execute(
                "SELECT * FROM cross_references WHERE target_doc_id = ? ORDER BY id",
                (target_doc_id,)
            )
            return [dict(r) for r in cursor.fetchall()]

    def reset_reference(self, ref_id: int):
        """Reset a reference to 'unresolved', clearing all resolution fields."""
        with self._get_connection() as conn:
            conn.execute(
                """
                UPDATE cross_references
                SET resolution_status = 'unresolved',
                    target_doc_id = NULL,
                    target_item_type = NULL,
                    target_item_number = NULL,
                    resolution_method = NULL,
                    resolved_at = NULL,
                    notes = NULL
                WHERE id = ?
                """,
                (ref_id,)
            )
            conn.commit()

    def get_registry_stats(self) -> Dict[str, Any]:
        """Return summary counts for display in the CLI tool."""
        with self._get_connection() as conn:
            doc_count = conn.execute("SELECT COUNT(*) FROM corpus_documents").fetchone()[0]
            ref_counts = {}
            for row in conn.execute(
                "SELECT resolution_status, COUNT(*) as n FROM cross_references GROUP BY resolution_status"
            ):
                ref_counts[row['resolution_status']] = row['n']
            split_count = conn.execute("SELECT COUNT(*) FROM document_splits").fetchone()[0]
        return {
            'corpus_documents': doc_count,
            'cross_references': ref_counts,
            'document_splits': split_count,
        }

    # ------------------------------------------------------------------
    # Document Splits
    # ------------------------------------------------------------------

    def add_document_split(self, parent_doc_id: int, child_doc_id: int,
                           split_type: str):
        """
        Record a parent-child split relationship.

        Idempotent: silently ignores duplicate (parent, child) pairs.

        Args:
            parent_doc_id: corpus_documents ID of the parent document.
            child_doc_id:  corpus_documents ID of the child document.
            split_type:    'chapter', 'part', 'title', 'section', etc.
        """
        with self._get_connection() as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO document_splits (parent_doc_id, child_doc_id, split_type)
                VALUES (?, ?, ?)
                """,
                (parent_doc_id, child_doc_id, split_type)
            )
            conn.commit()

    def get_children(self, parent_doc_id: int) -> List[Dict[str, Any]]:
        """Return all corpus_documents rows for children of the given parent."""
        with self._get_connection() as conn:
            cursor = conn.execute(
                """
                SELECT cd.*, ds.split_type
                FROM document_splits ds
                JOIN corpus_documents cd ON cd.id = ds.child_doc_id
                WHERE ds.parent_doc_id = ?
                ORDER BY cd.title
                """,
                (parent_doc_id,)
            )
            return [dict(r) for r in cursor.fetchall()]

    def get_parent(self, child_doc_id: int) -> Optional[Dict[str, Any]]:
        """Return the corpus_documents row for the parent of a split child, or None."""
        with self._get_connection() as conn:
            cursor = conn.execute(
                """
                SELECT cd.*, ds.split_type
                FROM document_splits ds
                JOIN corpus_documents cd ON cd.id = ds.parent_doc_id
                WHERE ds.child_doc_id = ?
                LIMIT 1
                """,
                (child_doc_id,)
            )
            row = cursor.fetchone()
            return dict(row) if row else None

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _normalize_path(file_path: str) -> str:
        """Return a canonical absolute path string for storage and comparison."""
        try:
            p = Path(file_path)
            return str(p.resolve() if p.exists() else p.absolute())
        except (OSError, RuntimeError, ValueError):
            return os.path.normpath(file_path)
