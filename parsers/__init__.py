"""
Parser Plugin System

This package provides a parser-agnostic architecture for handling different
legal document formats. Key principle: organizational structure is discovered
during parsing, not predefined.

Core components:
- adapter.py: Base classes and interfaces for parser adapters
- registry.py: Parser registration and discovery
- discovery.py: Functions to discover organizational units from parsed output
"""
# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License

from .adapter import (
    ParserAdapter,
    ParserCapabilities,
    ParseResult,
    SplitDetectionResult
)
from .registry import (
    ParserRegistry,
    get_registry,
    get_parser,
    load_parsers_from_config
)
from .discovery import (
    discover_organizational_units,
    discover_filterable_units
)

# Import adapters (optional - may not be available)
try:
    from .uslm_adapter import USLMParserAdapter
except ImportError:
    USLMParserAdapter = None

try:
    from .formex_adapter import FormexParserAdapter
except ImportError:
    FormexParserAdapter = None

try:
    from .ca_html_adapter import CAHtmlParserAdapter
except ImportError:
    CAHtmlParserAdapter = None

try:
    from .cfr_adapter import CFRParserAdapter
except ImportError:
    CFRParserAdapter = None

__all__ = [
    # Adapter classes
    'ParserAdapter',
    'ParserCapabilities',
    'ParseResult',
    'SplitDetectionResult',

    # Registry functions
    'ParserRegistry',
    'get_registry',
    'get_parser',
    'load_parsers_from_config',

    # Discovery functions
    'discover_organizational_units',
    'discover_filterable_units',

    # Concrete adapters (may be None if not available)
    'USLMParserAdapter',
    'FormexParserAdapter',
    'CAHtmlParserAdapter',
    'CFRParserAdapter',
]
