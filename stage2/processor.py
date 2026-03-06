"""
DefinitionsProcessor class for managing definition extraction and scope resolution.

This module provides the main processor class that coordinates definition processing
operations, manages AI client interactions, and handles document state updates.
"""
# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License

from utils import (
    get_organizational_item_name_set,
    get_list_string,
    get_operational_item_name_set,
    iter_indirect_definitions,
    query_json,
    write_if_updated,
)


class DefinitionsProcessor:
    """
    Processor class for managing definition extraction and scope resolution.

    This class provides a centralized interface for processing defined terms,
    managing AI client interactions, and handling document state updates.

    Attributes:
        client: AI client for making queries
        logfile: Log file for tracking operations
        parsed_content: The document content being processed
        out_path: Output file path for saving updates
        table_of_contents: Document table of contents for scope resolution
        config: Configuration dictionary for task-specific model routing
        dirty: Flag indicating if content has been modified
        org_item_name_set: Set of organizational item names
        org_item_name_string: Formatted string of organizational item names
        type_name_list: List of operational item type names
        type_list_or_string: Formatted string of operational item types
    """
    def __init__(self, client, logfile, parsed_content, out_path, table_of_contents=None, config=None):
        self.client = client
        self.logfile = logfile
        self.parsed_content = parsed_content
        self.out_path = out_path
        self.table_of_contents = table_of_contents or ''
        self.config = config  # Configuration dict for task-specific model routing
        self.dirty = 0

        # Helpers:
        self.org_item_name_set = get_organizational_item_name_set(parsed_content)
        self.org_item_name_string = get_list_string(sorted(self.org_item_name_set), 'or') # String of org_item_name names separated by commas, ending with 'or' if multiple.
        self.type_name_list = sorted(get_operational_item_name_set(parsed_content)) # Note that this gathers only operational item_name names, not all.
        self.type_list_or_string = get_list_string(self.type_name_list, 'or') # String of operational item_name names separated by commas, ending with 'or' if multiple.

        # Cache of indirect references:
        self.indirect_cache = {} # Dictionary.  Each key is a specific indirect reference, and the value is a dictionary with indirect_loc_type and indirect_loc_number.
        self.fill_indirect_cache() # Go through parsed_content and find existing indirect definitions, fill out indirect_cache.

    def fill_indirect_cache(self):
        for definition_entry, org_context, operational_unit in iter_indirect_definitions(self.parsed_content):
            if definition_entry and 'indirect' in definition_entry.keys() and not '' == definition_entry['indirect']: # The indirect string is present.
                indirect_string = definition_entry['indirect']
                if not indirect_string in self.indirect_cache.keys(): # It is not already in the cache.
                    self.indirect_cache[indirect_string] = {'indirect_loc_type': definition_entry['indirect_loc_type'], 'indirect_loc_number':definition_entry['indirect_loc_number']}

    def _create_indirect_prompt(self, indirect_string: str):
        """
        Create a prompt for the AI model to resolve an indirect definition reference.
        
        Args:
            indirect_string (str): String indicating where an indirect definition can be found
        
        Returns:
            str: Formatted prompt for the AI model
        """
        # Get examples based on actual operational unit types
        example_type = self.type_name_list[0] if self.type_name_list else "unit"
        example_number = "12.5"
        
        # Create sub-unit example if we have a type
        sub_unit_example = ""
        if len(self.type_name_list) > 0:
            sub_unit_example = f' (e.g., if {example_type} {example_number} appears in the Table of Contents but {example_type} {example_number}(b) does not, then "{example_type} {example_number}(b)" refers to {example_type} {example_number})'
        cache_prompt = 'You will use the following table of contents to carry out the task below:\n***BEGIN DOCUMENT TABLE OF CONTENTS***\n'
        cache_prompt += self.table_of_contents + '\n***END DOCUMENT TABLE OF CONTENTS***\n\n'
        cache_prompt += '**CRITICAL: RETURN JSON ONLY**\n'
        cache_prompt += '- Return ONLY a JSON array - no explanation, no preamble, no commentary\n'
        cache_prompt += '- Do not include any text before or after the JSON\n'
        cache_prompt += '- Do not explain your reasoning in the response\n'
        cache_prompt += '- The response must be parseable JSON\n\n'

        prompt = f"""
You are analyzing a legal document to identify the operational unit, if any, in this document that is referenced by an indirect definition.

**TASK:**
The indirect definition string is: "{indirect_string}"

**AVAILABLE OPERATIONAL UNIT TYPES:**
{self.type_list_or_string}

**INSTRUCTIONS:**
Your task is to identify which operational unit, if any, in this document is being referenced. Note that:
1. The reference might point to a different document.  You can see from the table of contents what the top-level name for this document is.  If the indirect definition string appears to point to a different document, then return "{{}}".
2. The reference might point to a sub-unit to a unit shown in the table of contents, in which case you should match the closest entry in the table of contents that you can{sub_unit_example}.
3. You should match to the lowest level operational unit shown in the hierarchy of the provided table of contents.
4. The indirect_loc_number might not be numeric (it can be letters or other identifiers).  But it need to be one that appears in the table of contents.

**REQUIRED RESPONSE FORMAT:**
Return ONLY a JSON object with exactly these keys:
- "indirect_loc_type": The type of operational unit (chosen from the available types)
- "indirect_loc_number": The specific identifier that matches the table of contents

**EXAMPLE RESPONSE:**
{{
    "indirect_loc_type": "{example_type}",
    "indirect_loc_number": "{example_number}"
}}

**IF NO MATCH FOUND:**
If you cannot determine the source, return ONLY:
{{}}

**REMEMBER: JSON ONLY - NO EXPLANATION**
"""
        return cache_prompt, prompt

    def get_indirect(self, indirect_string: str) -> dict:
        """
        Resolve an indirect definition reference to find the source operational unit.
        
        Args:
            indirect_string (str): String indicating where an indirect definition can be found
                                  (e.g., "as defined in Section 23.4" or "as defined in section 12.5(b)")
        
        Returns:
            dict: Dictionary with "indirect_loc_type" and "indirect_loc_number" keys identifying the operational unit
        """
        # Check cache first
        if indirect_string in self.indirect_cache:
            return self.indirect_cache[indirect_string]

        # Create prompts for AI model
        cache_prompt, prompt = self._create_indirect_prompt(indirect_string)
        
        try:
            # Query the AI model
            result = query_json(self.client, [cache_prompt], prompt, self.logfile,
                                config=self.config, task_name='stage2.definitions.resolve_indirect')
            
            # Parse the result
            if isinstance(result, dict) and 'indirect_loc_type' in result and 'indirect_loc_number' in result:
                # Validate that source_type is one of the available types
                if result['indirect_loc_type'] in self.type_name_list:
                    # Cache the result
                    self.indirect_cache[indirect_string] = result
                    return result
                else:
                    # Invalid indirect_loc_type, return empty dictionary
                    return {}
            else:
                # Invalid response format, return empty dictionary
                return {}
                
        except Exception as e:
            # Log the error and return empty dictionary
            print(f"Error resolving indirect reference '{indirect_string}': {e}")
            return {}

    def flush(self):
        """
        Write updated content to file if any changes have been made.
        Forces sync to disk to ensure immediate visibility on Windows.
        """
        self.dirty = write_if_updated(self.parsed_content, self.out_path, self.dirty)
        if self.dirty == 0:  # Write was successful (dirty flag reset)
            # Force sync to disk on Windows to ensure immediate visibility
            try:
                import os
                fd = os.open(self.out_path, os.O_RDONLY)
                os.fsync(fd)
                os.close(fd)
            except Exception:
                pass  # If sync fails, continue anyway (file was still written)

