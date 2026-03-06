"""
Prompt building functions for definition processing.

This module provides functions for building AI prompts used in definition extraction,
scope resolution, quality evaluation, and indirect definition resolution.
"""
# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License


def build_scope_resolution_prompt_v2(scope_phrase: str,
                                     document_title: str,
                                     document_long_title: str,
                                     org_item_name_string: str,
                                     substantive_unit_type_string: str):
    """
    Build a prompt for the AI model to resolve scope references using the new approach.

    This function creates a minimal prompt that asks the AI to extract structured information
    about scope references without providing the full table of contents. The AI can return
    compound organizational paths as lists to represent nested organizational references.

    Args:
        scope_phrase (str): The scope phrase to resolve (e.g., "in this chapter" or "Chapter III, Section 2")
        document_title (str): Document title for identification
        document_long_title (str): Full document title
        org_item_name_string (str): String of valid organizational item names
        substantive_unit_type_string (str): String of valid substantive unit type names

    Returns:
        str: Prompt for cache (minimal context)
        str: Formatted prompt for the AI model
    """
    cache_prompt = '**Document Information:**\n'
    if document_title:
        cache_prompt += f'Document Title: {document_title}\n'
    # if document_long_title and document_long_title != document_title:
    #     cache_prompt += f'Full Document Title: {document_long_title}\n'
    cache_prompt += '\n'

    cache_prompt += f'**Available Organizational Level Types:** {org_item_name_string}\n'
    cache_prompt += f'**Available Substantive Unit Types:** {substantive_unit_type_string}\n'
    cache_prompt += '\n'

    cache_prompt += '**CRITICAL: RETURN JSON ONLY**\n'
    cache_prompt += '- Return ONLY a JSON array - no explanation, no preamble, no commentary\n'
    cache_prompt += '- Do not include any text before or after the JSON\n'
    cache_prompt += '- Do not explain your reasoning in the response\n'
    cache_prompt += '- The response must be parseable JSON\n\n'

    prompt = '**Task:**\n'
    prompt += f'Analyze the scope phrase: "{scope_phrase}"\n\n'
    prompt += 'Determine if this phrase indicates a scope within the document described above. '
    prompt += 'Scope-indicating phrases typically use language like: "in this [unit]", "for purposes of this [unit]]", '
    prompt += '"as used in this [unit]]", etc. (where [unit] is an Organizational Level Type or Substantive Unit Type).\n\n'

    prompt += '**Instructions:**\n'
    prompt += '1. If the phrase refers to a different document (not the one described above), return: [{"in_this_document": false}]\n'
    prompt += '2. If the phrase does not contain scope-indicating language, return: []\n'
    prompt += '3. If the phrase indicates one or more scopes, return a JSON list with scope items for each (see format below).\n\n'

    prompt += '**Scope Items:**\n'
    prompt += 'Each scope item can be either:\n'
    prompt += 'A. **Single Unit** (dictionary): A single organizational or substantive unit\n'
    prompt += 'B. **Compound Organizational Path** (list): Multiple organizational units forming a nested path\n\n'

    prompt += '**When to use Compound Organizational Path (list):**\n'
    prompt += 'Use a list when the scope phrase specifies a NESTED path through organizational units, such as:\n'
    prompt += '- "Chapter III, Section 2" → Path from Chapter III down to Section 2 within that chapter\n'
    prompt += '- "Part A, Subpart 3, Division 2" → Path through multiple organizational levels\n'
    prompt += '- "Title 5, Chapter 10" → Path from Title 5 down to Chapter 10 within that title\n'
    prompt += 'The list should contain organizational units ordered from higher to lower level.\n\n'

    prompt += '**When NOT to use Compound Path:**\n'
    prompt += '- "this chapter AND section 5" → Two separate scopes (return two dictionaries)\n'
    prompt += '- "sections 4 to 29" → Single range (return one dictionary with range format)\n'
    prompt += '- "subsection (a)" → Single unit reference (return one dictionary)\n'
    prompt += '- References involving substantive units should generally be single dictionaries\n\n'

    prompt += '**Single Unit Format (dictionary):**\n'
    prompt += 'Each single unit scope item should include:\n'
    prompt += '- "in_this_document": true (required)\n'
    prompt += '- "element_type": The type of unit (from organizational or substantive unit types above)\n'
    prompt += '- "element_designation": The number, letter, or designation identifying the specific unit\n'
    prompt += '  * Use "current" for references to "this [unit]" or "the current [unit]" or similar language indicating the current [unit]\n'
    prompt += '  * For ranges (e.g., "sections 4 to 29"), use {"first": "4", "last": "29"}\n'
    prompt += '- You may use sub-unit types (e.g., "subsection" when "section" is a Substantive Unit Type)\n\n'

    prompt += '**Compound Path Format (list):**\n'
    prompt += 'A list of unit objects, each of which has the fields:\n'
    prompt += '- "element_type": The type of organizational unit\n'
    prompt += '- "element_designation": The designation of that unit\n'
    prompt += 'Do NOT include "in_this_document" in path elements (it is implied by the outer structure)\n'
    prompt += 'Order elements from higher to lower organizational level\n\n'
    prompt += 'A compound path (list) only contains Organizational Level Types\n'
    prompt += 'Compound paths represent a single scope traversing multiple organizational levels\n'

    prompt += '**Response Format (JSON array):**\n'
    prompt += '[\n'
    prompt += '  // Example 1: External reference\n'
    prompt += '  {"in_this_document": false}\n'
    prompt += ']\n\n'

    prompt += '[\n'
    prompt += '  // Example 2: Single organizational unit\n'
    prompt += '  {\n'
    prompt += '    "in_this_document": true,\n'
    prompt += '    "element_type": "chapter",\n'
    prompt += '    "element_designation": "current"\n'
    prompt += '  },\n\n'

    prompt += '  // Example 3: Single substantive unit\n'
    prompt += '  {\n'
    prompt += '    "in_this_document": true,\n'
    prompt += '    "element_type": "section",\n'
    prompt += '    "element_designation": "215"\n'
    prompt += '  },\n\n'

    prompt += '  // Example 4: Range of units\n'
    prompt += '  {\n'
    prompt += '    "in_this_document": true,\n'
    prompt += '    "element_type": "section",\n'
    prompt += '    "element_designation": {"first": "4", "last": "29"}\n'
    prompt += '  },\n\n'

    prompt += '  // Example 5: Compound Organizational PATH (list)\n'
    prompt += '  // This example for "Chapter V, Section 4b" - nested path within org structure\n'
    prompt += '  [\n'
    prompt += '    {"element_type": "chapter", "element_designation": "V"},\n'
    prompt += '    {"element_type": "section", "element_designation": "4b"}\n'
    prompt += '  ],\n\n'

    prompt += '  // Example 6: Another compound path\n'
    prompt += '  // "Part A, Division 5"\n'
    prompt += '  [\n'
    prompt += '    {"element_type": "part", "element_designation": "A"},\n'
    prompt += '    {"element_type": "division", "element_designation": "5"}\n'
    prompt += '  ]\n'
    prompt += ']\n\n'

    prompt += '**Important Note:**\n'
    prompt += '- If the phrase mentions multiple independent scopes, return a list of separate items (dictionaries or lists)\n\n'

    prompt += '**Scope Phrase:**\n'
    prompt += f'"{scope_phrase}"\n\n'
    prompt += '**REMEMBER: JSON ONLY - NO EXPLANATION**'

    return cache_prompt, prompt


def build_scope_prompt(term: str, definition: str, item_type_name: str, type_list_or_string: str, org_item_name_string: str):
    """
    Build a prompt to extract scope information from a definition.
    
    Args:
        term (str): The term being defined
        definition (str): The definition text
        item_type_name (str): Type of the item containing the definition
        type_list_or_string (str): String of valid operational item type names
        org_item_name_string (str): String of valid organizational item names
        
    Returns:
        str: Prompt for extracting scope
    """
    prompt = 'Your task is to extract the scope of applicability for a definition in the above provided '+ item_type_name + '.  '
    prompt += 'The defined term is: "' + term + '" and the definition from the text is: "' + definition + '".\n\n'
    prompt += 'Please follow these instructions carefully:\n\n'
    prompt += '1. Return as the scope any phrase in the provided ' + item_type_name + ' that indicates the **scope of where the definition holds**, '
    prompt += 'if one is provided (e.g., "as used in this chapter").\n   '
    prompt += '2. The valid types of document units scope may point to are: organizational unit (' + org_item_name_string + ') or substantive provision ('+ type_list_or_string + '). '
    prompt += 'If no scope is specified, return a blank string.\n   '
    prompt += '3. Return the scope (even if an empty string) in a JSON list with just one element.\n'
    prompt += """**Example output format (JSON):**
["in this chapter"]

or

[""]""" + '\n'
    prompt += '**CRITICAL: RETURN JSON ONLY**\n'
    prompt += '- Return ONLY the JSON list - no explanation, no preamble, no commentary\n'
    prompt += '- Do not include any text before or after the JSON\n'
    prompt += '- Do not explain your reasoning in the response\n'
    prompt += '- The response must be parseable JSON\n\n'
    return prompt


def build_definition_quality_evaluation_prompt(term: str, definition: str, item_type_name: str, def_kind: str = 'direct', conflict_count: int = 0):
    """
    Build a prompt to evaluate whether a definition is of acceptable quality.
    
    Args:
        term (str): The term being defined
        definition (str): The definition text to evaluate
        item_type_name (str): Type of the item containing the definition
        def_kind (str): 'direct' or 'elaboration'
        
    Returns:
        str: Prompt for evaluating definition quality
    """
    kind = (def_kind or 'direct').strip().lower()
    skeptical_preamble = ''
    if conflict_count > 0:
        skeptical_preamble = (
            f'Note: {conflict_count} different candidates for this term\'s definition were found '
            f'across different sections of this document. The existence of multiple competing '
            f'candidates is a signal to be skeptical. Apply a heightened standard: accept this '
            f'only if there is clear evidence the text is genuinely defining the term, not merely '
            f'using it or describing what a specific provision covers.\n\n'
        )
    if kind == 'elaboration':
        prompt = skeptical_preamble
        prompt += 'Evaluate whether the following would make sense as a usable elaborative statement about a definition:\n\n'
        prompt += '"' + term + '" is subject to the following elaboration in this document: "' + definition + '".\n\n'
        prompt += 'Your task is to judge whether this phrase clearly communicates how the definition of "' + term + '" is adjusted, interpreted, broadened, or limited in a way that is usable in a legal document. '
        prompt += 'For elaborative definitions, repetition of the root term is acceptable if the statement adds meaningful scope beyond that term. '
        prompt += 'Focus on functional utility, not formal circularity. '
        prompt += 'A valid definition may include other terms that would need to be defined separately. '
        prompt += 'Use only the text shown -- do not consult outside sources or invent facts. '
        prompt += 'If this would be a reasonable, sufficiently clear elaborative statement for legal use, output exactly [1]. '
        prompt += 'If not (including purely descriptive/example-only without a rule or blank), output exactly [0].\n\n'
    else:
        prompt = skeptical_preamble
        prompt += 'Evaluate whether the following would make sense as a usable definition:\n\n'
        prompt += '"' + term + '" is defined by "' + definition + '".\n\n'
        prompt += 'Your task is to judge whether this is a reasonable, sufficiently clear definition of "' + term + '". '
        prompt += 'Focus on functional utility -- whether the definition provides practical guidance for interpreting or applying the term in context, '
        prompt += 'even if such application may require some subjective judgment. Treat minor ambiguity as acceptable if the definition is structurally sound. '
        prompt += 'A valid definition may include other terms that would need to be defined separately. '
        prompt += 'Use only the text shown -- do not consult outside sources or invent facts. '
        prompt += 'If this would be a reasonable usable legal definition, output exactly [1]. '
        prompt += 'If not (including purely descriptive/example text, imported-from-elsewhere, or blank), output exactly [0].\n\n'
    prompt += '**CRITICAL: RETURN JSON ONLY**\n'
    prompt += '- Return ONLY the JSON list - no explanation, no preamble, no commentary\n'
    prompt += '- Do not include any text before or after the JSON\n'
    prompt += '- Do not explain your reasoning in the response\n'
    prompt += '- The response must be parseable JSON\n\n'
    prompt += 'Respond only with one of these two JSON expressions -- exactly [1] or [0] -- with no additional characters, explanation, whitespace, or newlines.'
    return prompt


def build_high_conflict_review_prompt(term: str, candidates: list) -> str:
    """
    Build a prompt to review high-conflict definition candidates for authenticity.

    Used when N >= threshold conflicting definitions exist for the same term across
    different sections. Makes a single AI call to determine whether the candidates
    are genuine definitions or merely uses/descriptions of the term.

    Args:
        term (str): The term being defined
        candidates (list): List of dicts, each with 'value', 'source_type', 'source_number'

    Returns:
        str: Prompt requesting {"verdict": "genuine"|"not_definitions", "reason": "..."}
    """
    n = len(candidates)
    prompt = (
        f'I found {n} different candidate definitions of the term "{term}" in a legal document, '
        f'each from a different section. The existence of multiple different candidates is itself '
        f'a reason for skepticism about whether any of them truly define the term.\n\n'
    )
    for i, c in enumerate(candidates, 1):
        source_label = f'{c.get("source_type", "unknown")} {c.get("source_number", "unknown")}'
        prompt += f'Candidate {i} (from {source_label}):\n"{c.get("value", "").strip()}"\n\n'
    prompt += (
        'Your task: determine whether these candidates are genuinely intended to be definitions '
        'of the term — meaning each clearly explains what the term means in its own right — '
        'or whether they are instances where the term is used, described, or referenced rather '
        'than defined (for example, text that lists what a specific provision covers, or text '
        'that uses the term as part of a longer phrase without defining it).\n\n'
        'Apply a conservative standard. Return "not_definitions" whenever there is reasonable '
        'doubt. Return "genuine" only when it is clear that the candidates are intended as '
        'definitions of the term.\n\n'
        'Return a JSON object with exactly these fields:\n'
        '  - "verdict": exactly "genuine" or "not_definitions"\n'
        '  - "reason": a brief explanation (1-2 sentences)\n\n'
        '**CRITICAL: RETURN JSON ONLY**\n'
        '- Return ONLY the JSON object - no explanation, no preamble, no commentary\n'
        '- Do not include any text before or after the JSON\n'
        '- The response must be parseable JSON\n'
    )
    return prompt


def build_definition_retry_prompt(term: str, existing_definition: str, item_type_name: str, def_kind: str = 'direct'):
    """
    Build a prompt to retry definition extraction from text.

    Args:
        term (str): The term to find a definition for
        existing_definition (str): The existing definition (may be empty or poor)
        item_type_name (str): Type of the item containing the text
        def_kind (str): 'direct' or 'elaboration'

    Returns:
        str: Prompt for retrying definition extraction
    """
    prompt = ''
    if existing_definition and existing_definition.strip():
        prompt += 'In an earlier analysis of the above provided ' + item_type_name + ', the term: "' + term + '" was found to be defined as: "' + existing_definition + '"\n\n'
        prompt += 'However, this has since been deemed to be poor or incomplete.  It may be because the text is lacking, or because the term is not actually defined in the text.\n\n'
    else:
        prompt += 'I am trying to find a definition in the above provided ' + item_type_name + ' for the term: "' + term + '"\n\n'
    kind = (def_kind or 'direct').strip().lower()
    if kind == 'elaboration':
        prompt += 'I want you to take a look at the above provided ' + item_type_name + ' and determine a clear elaborative statement if present — text that clarifies how the definition of "' + term + '" is adjusted, interpreted, broadened, or limited (e.g., includes/excludes/also means). '
    else:
        prompt += 'I want you to take a look at the above provided ' + item_type_name + ' and determine a good definition if there is one present in the text.  '
    prompt += 'Do not consult outside sources or invent facts.  If appropriate text is available from the ' + item_type_name + ', return a JSON object in the form of:\n\n'
    prompt += '{"term": "the term defined", "value": "the definition for the term"}\n\n'
    prompt += 'Otherwise, return the empty JSON object: {}\n\n'
    if kind == 'elaboration':
        prompt += 'Return the elaborative phrase as it appears (it may include connecting words such as "includes" or "does not include").\n\n'
    else:
        prompt += 'The definition you return should not include connecting words or phrases like, "means" or "is defined in this document to mean".\n\n'
    prompt += '**IMPORTANT: Preserve exact formatting.** Return the term and definition text exactly as they appear in the source, '
    prompt += 'including any HTML/XML font presentation markup (such as <sub>, </sub>, <sup>, </sup>, <i>, </i>, <b>, </b>, etc.).\n\n'
    prompt += '**CRITICAL: RETURN JSON ONLY**\n'
    prompt += '- Return ONLY the JSON object - no explanation, no preamble, no commentary\n'
    prompt += '- Do not include any text before or after the JSON\n'
    prompt += '- Do not explain your reasoning in the response\n'
    prompt += '- The response must be parseable JSON\n\n'
    return prompt


def build_definition_construction_prompt(term: str, target_loc_type: str, target_loc_number: str):
    """
    Build a prompt to construct a definition from target location text.

    Args:
        term (str): The term to construct a definition for
        target_loc_type (str): Type of the target location
        target_loc_number (str): Number/identifier of the target location

    Returns:
        str: Prompt for constructing definition from target text
    """
    prompt = 'I am trying to find a definition for the term: "' + term + '" in the above provided ' + target_loc_type + ' ' + target_loc_number + '.\n\n'
    prompt += 'This term is referenced elsewhere in the document, and the reference points to this ' + target_loc_type + ' ' + target_loc_number + ' as the location where the definition should be found.\n\n'
    prompt += 'I want you to take a look at the above provided ' + target_loc_type + ' ' + target_loc_number + ' and determine if a good definition for "' + term + '" can be constructed from the text.  '
    prompt += 'Do not consult outside sources or invent facts.  If a good definition can be constructed from the text, return a JSON object in the form of:\n\n'
    prompt += '{"term": "the term defined", "value": "the definition for the term"}\n\n'
    prompt += 'Otherwise, return the empty JSON object: {}\n\n'
    prompt += 'The definition you return should not include connecting words or phrases like, "means" or "is defined in this document to mean".\n\n'
    prompt += '**IMPORTANT: Preserve exact formatting.** Return the term and definition text exactly as they appear in the source, '
    prompt += 'including any HTML/XML font presentation markup (such as <sub>, </sub>, <sup>, </sup>, <i>, </i>, <b>, </b>, etc.).\n\n'
    prompt += '**CRITICAL: RETURN JSON ONLY**\n'
    prompt += '- Return ONLY the JSON object - no explanation, no preamble, no commentary\n'
    prompt += '- Do not include any text before or after the JSON\n'
    prompt += '- Do not explain your reasoning in the response\n'
    prompt += '- The response must be parseable JSON\n\n'
    return prompt


def build_definition_prompt(term: str, item_type_name: str, type_list_or_string: str):
    """
    Build a prompt to extract a definition for a specific term.

    Args:
        term (str): The term to find a definition for
        item_type_name (str): Type of the item containing the text
        type_list_or_string (str): String of valid operational item type names

    Returns:
        str: Prompt for extracting definition
    """
    prompt = 'Your task is to extract **an explicitly created, usable definition** for the term "' + term + '" from the above provided '+ item_type_name + '\n\n'
    prompt += 'Please follow these instructions carefully:\n\n   '
    prompt += '1. You will return a JSON object with three keys: "definition", "indirect", and "def_kind".\n   '
    prompt += '2. For "definition", if a usable definition is available from the text, return the full phrase that gives the definition, '
    prompt += 'including the term itself and any connecting language (e.g., \'the term "' + term + '" means ...\'). If the text instead **elaborates** '
    prompt += "on a separate definition (e.g., \"The word 'county' includes ...\", \"does not include ...\"), return that elaborative phrase as the definition.\n   "
    prompt += '3. For "def_kind", return "direct" if the text is giving a full/standalone definition of the term; return "elaboration" if the text is augmenting/limiting/clarifying a pre-existing/common definition (e.g., "includes"/"does not include"/"also means").\n   '
    prompt += '4. Do not consult outside sources or invent facts. Any returned values must be extracted directly from the text.\n   '
    prompt += '5. Return an empty value for "definition" if "' + term + '" is only **mentioned, described, or used**, but **not directly defined nor elaborated upon**.\n   '
    prompt += '6. If the term is defined by reference to a particular ' + type_list_or_string + ' (e.g., "as defined in ' + item_type_name + ' 42"), then set "indirect" to that specific reference (e.g., "' + item_type_name + ' 42"); otherwise set "indirect" to an empty string.\n   '
    prompt += '7. **IMPORTANT: Preserve exact formatting.** When returning the definition text, preserve any HTML/XML font presentation markup '
    prompt += '(such as <sub>, </sub>, <sup>, </sup>, <i>, </i>, <b>, </b>, etc.) exactly as it appears in the source text.\n\n'
    prompt += """**Example output format (JSON):**
[
  {
    "definition": "The term ai means artificial intelligence",
    "indirect": "",
    "def_kind": "direct"
  }
]

or

[
  {
    "definition": "The word \\"county\\" includes a parish, or any other equivalent subdivision of a State or Territory of the United States.",
    "indirect": "",
    "def_kind": "elaboration"
  }
]""" + '\n\n'
    prompt += '**CRITICAL: RETURN JSON ONLY**\n'
    prompt += '- Return ONLY the JSON object - no explanation, no preamble, no commentary\n'
    prompt += '- Do not include any text before or after the JSON\n'
    prompt += '- Do not explain your reasoning in the response\n'
    prompt += '- The response must be parseable JSON\n\n'
    return prompt


def build_defined_terms_prompt(item_type_name: str) -> str:
    """
    Build a prompt for the AI model to extract defined terms from legal text.

    This function creates a detailed prompt that instructs the AI model on how to
    identify and extract defined terms from legal documents, including their scope
    and whether they are direct or indirect definitions.

    Args:
        item_type_name (str): Type name of the item being analyzed

    Returns:
        str: Formatted prompt for the AI model to extract defined terms
    """
    prompt = 'Your task is to extract **defined terms** -- terms for which the above provided '+ item_type_name + ' **explicitly creates a usable definition**.\n\n'
    prompt += 'Please follow these instructions carefully:\n\n'
    prompt += '1. **Include a term in the output if:**\n   - It is clearly being **directly defined** in this '+ item_type_name + '.\n   '
    prompt += '- The scope of applicability may be **unspecified**, apply **beyond this '+ item_type_name + '** (e.g., "in this chapter," "in this part"), '
    prompt += 'or be **limited to this '+ item_type_name + ' or a subunit of it** (e.g., "as used in this '+ item_type_name + '," "for purposes of this sub'+ item_type_name + '").\n\n'
    prompt += '2. **Do NOT include a term if any of the following are true:**\n   '
    prompt += '- The term is only **mentioned, described, or used**, but **not directly defined**.\n   '
    prompt += '- The definition appears to be imported from another part of the statute (i.e., it is not newly defined in this '+ item_type_name + ').\n\n   '
    prompt += '3. Return only a JSON list with each defined term in double quotes.  If there are no definitions, then an empty list is to be returned.\n   '
    prompt += '4. **IMPORTANT: Preserve exact term formatting.** Return each term EXACTLY as it appears in the text, including any HTML/XML font '
    prompt += 'presentation markup such as <sub>, </sub>, <sup>, </sup>, <i>, </i>, <b>, </b>, etc. '
    prompt += 'For example, if the text contains "V<sub>A</sub>", return "V<sub>A</sub>" -- do NOT convert it to "V_A" or "VA" or any other form.\n\n'
    prompt += '**Examples output format (JSON):**\n'
    prompt += '["AI","risk management system","urgent or emergency public health care need"]\n\n'
    prompt += 'Example with formatting markup:\n'
    prompt += '["V<sub>A</sub>","H<sub>2</sub>O","CO<sub>2</sub>"]\n\n'
    prompt += 'If no defined terms are found:\n'
    prompt += '[]\n\n'
    prompt += '**Examples of terms that should NOT be included:**\n'
    prompt += '- Terms that are only **mentioned, described, or used** but not directly defined.\n'
    prompt += '- Terms whose definitions are imported from another part of the statute (not newly defined here).\n\n'
    prompt += '**CRITICAL: RETURN JSON ONLY**\n'
    prompt += '- Return ONLY the JSON list - no explanation, no preamble, no commentary\n'
    prompt += '- Do not include any text before or after the JSON\n'
    prompt += '- Do not explain your reasoning in the response\n'
    prompt += '- The response must be parseable as a JSON list\n\n'
    return prompt


def build_external_reference_validation_prompt(term: str, definition: str, external_reference: str):
    """
    Build a prompt to determine if a definition text clearly relies on an external source.

    This is used as a secondary quality check for definitions that have an external_reference
    field but failed the primary quality evaluation. The goal is to identify valid indirect
    definitions that point to external sources for the actual definition content.

    Args:
        term (str): The term being defined
        definition (str): The definition text to evaluate
        external_reference (str): The external reference identifier

    Returns:
        str: Prompt for validating external reference dependency
    """
    prompt = 'Evaluate whether the following definition text clearly indicates that the actual definition '
    prompt += 'is provided in an external reference (i.e., another section, statute, or document not included here).\n\n'
    prompt += 'Term: "' + term + '"\n'
    prompt += 'Definition text: "' + definition + '"\n'
    prompt += 'External reference: "' + external_reference + '"\n\n'
    prompt += 'Your task is to judge whether this definition text is a **valid indirect definition** that explicitly '
    prompt += 'relies on an external source for the actual definition content. Valid patterns include:\n'
    prompt += '- "has the same meaning as [provided/defined] in [external reference]"\n'
    prompt += '- "as defined in [external reference]"\n'
    prompt += '- "means the same as in [external reference]"\n'
    prompt += '- Similar clear references to another location for the definition\n\n'
    prompt += 'The definition text should make it clear that the reader must look to the external reference '
    prompt += 'to understand what the term means.\n\n'
    prompt += 'If this is a valid indirect definition that clearly relies on the external reference, output exactly [1].\n'
    prompt += 'If not (e.g., the text is vague, incomplete, or does not clearly point to an external definition), output exactly [0].\n\n'
    prompt += '**CRITICAL: RETURN JSON ONLY**\n'
    prompt += '- Return ONLY the JSON list - no explanation, no preamble, no commentary\n'
    prompt += '- Do not include any text before or after the JSON\n'
    prompt += '- Do not explain your reasoning in the response\n'
    prompt += '- The response must be parseable JSON\n\n'
    prompt += 'Respond only with one of these two JSON expressions -- exactly [1] or [0] -- with no additional characters, explanation, whitespace, or newlines.'
    return prompt

