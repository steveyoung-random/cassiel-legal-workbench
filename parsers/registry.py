"""
Parser Registry

Simple dictionary-based registry for parser adapters.
Parsers are manually registered at startup (no complex runtime discovery).
"""
# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License

from typing import Dict, Optional, List, Any
from .adapter import ParserAdapter, ParserCapabilities


class ParserRegistry:
    """Registry for parser adapters (simple dictionary)"""

    def __init__(self):
        self._parsers: Dict[str, ParserAdapter] = {}

    def register(self, parser_type: str, adapter: ParserAdapter):
        """
        Register a parser adapter.

        Args:
            parser_type: Parser type identifier (e.g., 'uslm', 'formex')
            adapter: ParserAdapter instance to register
        """
        self._parsers[parser_type] = adapter

    def get(self, parser_type: str) -> Optional[ParserAdapter]:
        """
        Get parser adapter by type.

        Args:
            parser_type: Parser type identifier

        Returns:
            ParserAdapter instance or None if not found
        """
        return self._parsers.get(parser_type)

    def list_parsers(self) -> List[str]:
        """
        List all registered parser types.

        Returns:
            List of parser type identifiers
        """
        return list(self._parsers.keys())

    def get_all_capabilities(self) -> Dict[str, ParserCapabilities]:
        """
        Get capabilities for all registered parsers.

        Returns:
            Dict mapping parser_type to ParserCapabilities
        """
        return {
            parser_type: adapter.get_capabilities()
            for parser_type, adapter in self._parsers.items()
        }


# Global registry instance
_registry = ParserRegistry()


def load_parsers_from_config(config: Dict[str, Any] = None):
    """
    Load parsers from configuration.

    Manually registers built-in parsers (no complex discovery).
    This function should be called at application startup.

    Args:
        config: Configuration dictionary (optional, currently unused - reserved for future use)
                If None, parsers are still registered using hardcoded adapters.
    """
    # Import adapters here to avoid circular imports
    try:
        from .uslm_adapter import USLMParserAdapter
        _registry.register('uslm', USLMParserAdapter())
    except ImportError as e:
        print(f"Warning: Could not load USLM adapter: {e}")

    try:
        from .formex_adapter import FormexParserAdapter
        _registry.register('formex', FormexParserAdapter())
    except ImportError as e:
        print(f"Warning: Could not load Formex adapter: {e}")

    try:
        from .ca_html_adapter import CAHtmlParserAdapter
        _registry.register('ca_html', CAHtmlParserAdapter())
    except ImportError as e:
        print(f"Warning: Could not load CA HTML adapter: {e}")

    try:
        from .cfr_adapter import CFRParserAdapter
        _registry.register('cfr', CFRParserAdapter())
    except ImportError as e:
        print(f"Warning: Could not load CFR adapter: {e}")

    # Future: Could dynamically load additional parsers from config
    # But manual registration is simpler and less fragile


def get_registry() -> ParserRegistry:
    """
    Get global parser registry.

    Returns:
        Global ParserRegistry instance
    """
    return _registry


def get_parser(parser_type: str) -> Optional[ParserAdapter]:
    """
    Get parser adapter (convenience function).

    Args:
        parser_type: Parser type identifier

    Returns:
        ParserAdapter instance or None if not found
    """
    return _registry.get(parser_type)
