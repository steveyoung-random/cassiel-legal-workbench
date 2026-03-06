-- Cross-Reference Registry Database Schema
-- Tracks cross-document references extracted from processed documents and their
-- resolution status (whether the referenced document is in the corpus).
--
-- Database file lives in the output directory (config: output.directory),
-- alongside jobs.db. Filename: cross_reference_registry.db

-- ============================================================
-- CORPUS DOCUMENTS
-- Index of all processed documents in the corpus.
-- References and resolutions use integer IDs, not file paths,
-- so the registry remains valid if files are moved (update file_path).
-- ============================================================
CREATE TABLE IF NOT EXISTS corpus_documents (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    file_path        TEXT    NOT NULL UNIQUE,  -- absolute path to processed JSON
    doc_id           TEXT,                     -- document_information.document_id from JSON
    title            TEXT,                     -- human-readable title for display
    parser_type      TEXT    NOT NULL,         -- 'uslm', 'cfr', 'ca_html', 'formex'
    stage_reached    INTEGER NOT NULL DEFAULT 1, -- highest stage completed (1=parsed, 2=defs, 3=summaries)
    added_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_scanned_at  TIMESTAMP,                -- when references were last extracted from this doc
    metadata         TEXT                      -- JSON: {"org_top_keys": {...}, "content_items": {...}}
);

-- ============================================================
-- CROSS-REFERENCES
-- One row per extracted reference instance.
-- Source: the item (section, appendix, etc.) that contains the reference.
-- ref_context distinguishes two extraction sources:
--   'need_ref'   - from need_ref[type="External"] at the section level
--   'definition' - from external_reference in a defined_terms entry
-- For definition-level references, 'term' holds the defined term.
-- Resolution fields are populated by the resolution tool (task 3.3).
-- ============================================================
CREATE TABLE IF NOT EXISTS cross_references (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,

    -- Source location
    source_doc_id       INTEGER NOT NULL REFERENCES corpus_documents(id) ON DELETE CASCADE,
    source_item_type    TEXT    NOT NULL,  -- 'section', 'appendix', 'supplement', etc.
    source_item_number  TEXT    NOT NULL,  -- item number within the source document

    -- Reference text
    ref_text            TEXT    NOT NULL,  -- raw citation string as it appears in the document
    ref_context         TEXT    NOT NULL   -- 'need_ref' or 'definition'
                            CHECK(ref_context IN ('need_ref', 'definition')),
    term                TEXT,             -- defined term, when ref_context = 'definition'

    -- Resolution (populated by resolution tool or manual update)
    resolution_status   TEXT    NOT NULL DEFAULT 'unresolved'
                            CHECK(resolution_status IN ('unresolved', 'resolved', 'ambiguous', 'not_in_corpus')),
    target_doc_id       INTEGER REFERENCES corpus_documents(id),  -- null until resolved
    target_item_type    TEXT,   -- item type in target doc (null = doc-level match only)
    target_item_number  TEXT,   -- item number in target doc (null = doc-level match only)
    resolution_method   TEXT    CHECK(resolution_method IN ('automatic', 'manual', 'ai')),
    resolved_at         TIMESTAMP,
    notes               TEXT,   -- explanation of resolution or reason for not_in_corpus

    extracted_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP

    -- No table-level UNIQUE here — deduplication is handled by two partial unique
    -- indexes below, because the uniqueness rules differ by ref_context:
    --   need_ref:   unique on (source, ref_text)         — term is always NULL
    --   definition: unique on (source, ref_text, term)   — term identifies which defined
    --               term points to this citation; same citation text used by two
    --               different terms in the same section produces two rows
);

-- ============================================================
-- DOCUMENT SPLITS
-- Parent-child relationships for documents that were split from a larger source
-- (e.g., a USC Title split into Chapter files).
-- Used by task 3.9: propagate parent-level definitions to child documents at Q&A time.
-- ============================================================
CREATE TABLE IF NOT EXISTS document_splits (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    parent_doc_id  INTEGER NOT NULL REFERENCES corpus_documents(id) ON DELETE CASCADE,
    child_doc_id   INTEGER NOT NULL REFERENCES corpus_documents(id) ON DELETE CASCADE,
    split_type     TEXT    NOT NULL,  -- 'chapter', 'part', 'title', 'section', etc.
    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(parent_doc_id, child_doc_id)
);

-- ============================================================
-- INDEXES
-- ============================================================
-- Deduplication: one row per (section, ref_text) for need_ref references
CREATE UNIQUE INDEX IF NOT EXISTS idx_xref_unique_need_ref ON cross_references
    (source_doc_id, source_item_type, source_item_number, ref_text)
    WHERE ref_context = 'need_ref';

-- Deduplication: one row per (section, ref_text, term) for definition references.
-- Two terms in the same section pointing to the same external citation get two rows.
CREATE UNIQUE INDEX IF NOT EXISTS idx_xref_unique_definition ON cross_references
    (source_doc_id, source_item_type, source_item_number, ref_text, term)
    WHERE ref_context = 'definition';

CREATE INDEX IF NOT EXISTS idx_xref_source_doc    ON cross_references(source_doc_id);
CREATE INDEX IF NOT EXISTS idx_xref_status        ON cross_references(resolution_status);
CREATE INDEX IF NOT EXISTS idx_xref_target_doc    ON cross_references(target_doc_id);
CREATE INDEX IF NOT EXISTS idx_xref_ref_text      ON cross_references(ref_text);
CREATE INDEX IF NOT EXISTS idx_splits_parent      ON document_splits(parent_doc_id);
CREATE INDEX IF NOT EXISTS idx_splits_child       ON document_splits(child_doc_id);
CREATE INDEX IF NOT EXISTS idx_corpus_doc_id      ON corpus_documents(doc_id);
CREATE INDEX IF NOT EXISTS idx_corpus_parser_type ON corpus_documents(parser_type);
