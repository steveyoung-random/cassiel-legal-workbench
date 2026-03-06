"""
UI Components for Multi-Column Progressive Disclosure Interface.

This package contains modular components for the column-based UI:
- column1_overview: Project overview, statistics, view mode selector
- column2_categories: Document categories grouped by parser type
- column3_documents: Document list with search and filtering
- column4_subdocs: Sub-document selection (conditional, for split documents)
- column5_processing: Processing controls, job submission, monitoring
- job_monitoring: Reusable job monitoring widget
- job_history_view: Full job history interface
- questions_view: Global questions interface
"""
# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License

__all__ = [
    'column1_overview',
    'column2_categories',
    'column3_documents',
    'column4_subdocs',
    'column5_processing',
    'job_monitoring',
    'job_history_view',
    'questions_view'
]
