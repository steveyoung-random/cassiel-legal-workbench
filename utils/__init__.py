# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License
from .text_processing import clean_text
from .text_processing import canonical_org_types
from .text_processing import table_to_text
from .text_processing import html_table_to_plaintext
from .text_processing import extract_trailing_paren
from .text_processing import deduplicate_breakpoints
from .text_processing import remove_blank_lines
from .document_handling import get_org_pointer
from .document_handling import get_org_pointer_from_context
from .document_handling import get_org_pointer_from_scope
from .document_handling import create_table_of_contents
from .document_handling import get_operational_item_name_set
from .document_handling import get_full_item_name_set
from .document_handling import get_list_string
from .document_handling import get_organizational_item_name_set
from .document_handling import iter_operational_items
from .document_handling import iter_all_items
from .document_handling import has_sub_units
from .document_handling import lookup_item
from .document_handling import get_item_numbers_for_type
from .document_handling import iter_containers
from .document_handling import write_if_updated
from .document_handling import iter_org_content
from .document_handling import iter_definitions
from .document_handling import iter_indirect_definitions
from .document_handling import TextExtractionTools
from .document_handling import get_text_extraction_tools_schema
from .document_handling import add_substantive_markers_org
from .document_handling import get_org_top_unit
from .document_handling import chunk_text
from .document_handling import build_metadata_suffix
from .document_handling import augment_chunk_with_metadata
from .chunking_helpers import create_chunk_summary_prompt
from .chunking_helpers import synthesize_final_summary
from .chunking_helpers import deduplicate_references
from .xml_processing import get_all_elements
from .xml_processing import get_first_element
from .xml_processing import drop_ns_and_prefix_to_underscore
from .error_handling import InputError
from .error_handling import ConfigError
from .error_handling import ParseError
from .error_handling import ModelError
from .error_handling import ParseWarning
from .error_handling import InputWarning
from .error_handling import CheckVersion
from .ai_client import GetClient
from .ai_client import Query
from .ai_client import GetLogfile
from .ai_client import QueryWithBaseClient
from .ai_client import query_json
from .ai_client import query_text_with_retry
from .ai_client import AIResponse
from .ai_client import AIMessage
from .ai_client import ToolCall
from .ai_client import BaseAIClient
from .ai_client import OpenAIClient
from .ai_client import AnthropicClient
from .ai_client import create_ai_client
from .api_cache import get_cache
from .api_cache import set_cache_file
from .api_cache import get_cached_response
from .api_cache import set_cached_response
from .api_cache import APICache
from .document_issues import get_document_issues_logfile
from .document_issues import log_document_issue
from .definition_helpers import strip_sub_prefix
from .definition_helpers import find_substantive_unit_with_maximum_matching
from .definition_helpers import expand_element_range
from .definition_helpers import resolve_current_from_context
from .definition_helpers import find_organizational_unit_path
from .definition_helpers import resolve_compound_organizational_path
from .definition_prompts import build_scope_resolution_prompt_v2
from .definition_prompts import build_scope_prompt
from .definition_prompts import build_definition_quality_evaluation_prompt
from .definition_prompts import build_definition_retry_prompt
from .definition_prompts import build_definition_construction_prompt
from .definition_prompts import build_definition_prompt
from .definition_prompts import build_defined_terms_prompt
from .definition_prompts import build_external_reference_validation_prompt
from .definition_prompts import build_high_conflict_review_prompt
from .table_handling import extract_large_tables, LARGE_TABLE_ROW_THRESHOLD




__all__ = ["clean_text",
           "canonical_org_types", 
           "table_to_text", 
           "html_table_to_plaintext", 
           "extract_trailing_paren",
           "deduplicate_breakpoints",
           "remove_blank_lines",
           "get_org_pointer",
           "get_org_pointer_from_context",
           "get_org_pointer_from_scope",
           "create_table_of_contents",
           "get_operational_item_name_set",
           "get_full_item_name_set",
           "get_list_string",
           "get_organizational_item_name_set",
           "iter_operational_items",
           "iter_all_items",
           "has_sub_units",
           "lookup_item",
           "get_item_numbers_for_type",
           "iter_containers",
           "write_if_updated",
           "iter_org_content",
           "iter_definitions",
           "iter_indirect_definitions",
           "TextExtractionTools",
           "get_text_extraction_tools_schema",
           "add_substantive_markers_org",
           "get_org_top_unit",
           "chunk_text",
           "build_metadata_suffix",
           "augment_chunk_with_metadata",
           "create_chunk_summary_prompt",
           "synthesize_final_summary", 
           "deduplicate_references",
           "get_all_elements",
           "get_first_element",
           "drop_ns_and_prefix_to_underscore",
           "InputError", 
           "ConfigError", 
           "ParseError",
           "ModelError",
           "ParseWarning",
           "InputWarning",
           "CheckVersion", 
           "GetClient", 
           "Query", 
           "GetLogfile",
           "QueryWithBaseClient",
           "query_json",
           "query_text_with_retry",
           "AIResponse",
           "AIMessage",
           "ToolCall",
           "BaseAIClient",
           "OpenAIClient",
           "AnthropicClient",
           "create_ai_client",
           "get_cache",
           "set_cache_file",
           "get_cached_response",
           "set_cached_response",
           "APICache",
           "get_document_issues_logfile",
           "log_document_issue",
           "strip_sub_prefix",
           "find_substantive_unit_with_maximum_matching",
           "expand_element_range",
           "resolve_current_from_context",
           "find_organizational_unit_path",
           "resolve_compound_organizational_path",
           "build_scope_resolution_prompt_v2",
           "build_scope_prompt",
           "build_definition_quality_evaluation_prompt",
           "build_definition_retry_prompt",
           "build_definition_construction_prompt",
           "build_definition_prompt",
           "build_defined_terms_prompt",
           "build_high_conflict_review_prompt",
           "build_external_reference_validation_prompt",
           "extract_large_tables",
           "LARGE_TABLE_ROW_THRESHOLD"]