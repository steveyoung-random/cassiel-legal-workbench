"""
Stage 2 processing modules for definition extraction and scope resolution.

This package contains modules for processing legal document definitions:
- processor: DefinitionsProcessor class
- definition_extraction: Definition extraction from text
- scope_resolution: Scope resolution for definitions
- quality_control: Definition quality evaluation and improvement
- indirect_resolution: Indirect definition resolution
- definition_management: Definition placement and management
"""
# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License

from .processor import DefinitionsProcessor
from .definition_extraction import find_defined_terms
from .scope_resolution import find_defined_terms_scopes
from .quality_control import evaluate_and_improve_definitions, review_high_conflict_terms
from .indirect_resolution import process_indirect_definitions, enhance_resolve_indirect_definitions

__all__ = [
    'DefinitionsProcessor',
    'find_defined_terms',
    'find_defined_terms_scopes',
    'evaluate_and_improve_definitions',
    'review_high_conflict_terms',
    'process_indirect_definitions',
    'enhance_resolve_indirect_definitions',
]

