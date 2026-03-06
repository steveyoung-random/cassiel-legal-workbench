"""
Cross-Reference Registry module.

Provides the Registry class for managing the cross-document reference database.
The database (cross_reference_registry.db) lives in the output directory
alongside jobs.db.

Usage:
    from registry.registry import Registry
    reg = Registry(db_path)
    doc_id = reg.add_document(file_path, doc_id, title, parser_type, stage_reached)
"""
# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License
