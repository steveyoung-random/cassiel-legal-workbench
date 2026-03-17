# Definitions System Documentation

## Summary

The definitions system extracts, classifies, and places definitions throughout legal documents based on their scope. It handles both self-contained definitions and indirect references, distinguishes between standalone and elaborational definitions, and places definitions in organizational or substantive units based on where they apply. The system is designed to handle complex legal document structures while maintaining traceability of where definitions originate and where they apply.

## Overview

This document explains the concepts underlying how definitions work in the legal document analysis system. It covers the different types of definitions, how scopes work, what definition fields mean, and how definitions are placed within document structures.

## Definition Types

### Direct vs Indirect Definitions

**Direct definitions** are definitions where the term is explicitly defined in the text being analyzed. The definition text is present and usable.

**Indirect definitions** are definitions that reference another location where the term is defined. They contain:
- An `indirect` field with a string reference (e.g., "section 42", "as defined in article 5")
- If the reference can be resolved to a location within the document: `indirect_loc_type` and `indirect_loc_number` fields pointing to the target location
- If the reference points outside the document: `external_reference` field instead (see External References below)

### Direct vs Elaborational Definitions

**Note**: "Direct" is used in two different contexts:
- **Direct vs Indirect**: Whether the definition is self-contained or references another location
- **Direct vs Elaborational**: Whether the definition is standalone or augments an existing definition

**Direct definitions** (in the direct/elaborational sense) provide a complete, standalone definition of a term. Example: "The term 'provider' means a health care facility licensed under this chapter." Direct definitions are stored with `def_kind: "direct"`.

**Elaborational definitions** augment, limit, or clarify a pre-existing definition. They typically use language like:
- "includes" (e.g., "The word 'county' includes a parish")
- "does not include" (e.g., "The term 'person' does not include corporations")
- "also means" (e.g., "The term 'officer' also means any person authorized by law")

Elaborational definitions are stored with `def_kind: "elaboration"` and are evaluated differently during quality control, as they may reference the root term being defined (which is acceptable for elaborations but not for direct definitions).

## Definition Scopes

### Concept of Scope

A definition's **scope** determines where in the document the definition applies. Scopes are expressed through phrases like:
- "in this chapter"
- "for purposes of this section"
- "as used in sections 201 through 299"
- "in this part"

### Scope Resolution

Scope phrases are resolved to specific document locations. The system interprets phrases like "in this chapter" or "for purposes of sections 201 through 299" and maps them to organizational units or substantive units where the definition applies.

### Organizational Units

**Organizational units** are structural divisions of the document (e.g., titles, chapters, parts, subchapters). They provide hierarchical organization but don't contain substantive legal provisions themselves.

When a definition's scope resolves to an organizational unit, the definition is placed in that unit's `unit_definitions` list. This makes the definition available to all substantive units within that organizational scope.

**Example**: A definition scoped to "this chapter" is placed in the chapter's `unit_definitions`, making it available to all sections within that chapter.

### Substantive Units

**Substantive units** contain the body language of the document (e.g., sections, articles, recitals). They contain the actual legal text and substantive content. Note that not all substantive units are operational—recitals, for instance, are substantive but not operational.

When a definition's scope resolves to a substantive unit, there are two cases:

1. **Same unit as source**: If the scope points to the same unit where the definition was found, the definition remains in that unit's `defined_terms` list.

2. **Different unit**: If the scope points to a different substantive unit, the definition is placed in that unit's `ext_definitions` list (external definitions - defined elsewhere but applicable here).

### No Scope

When a definition has no scope specified (or scope resolution fails), it is placed in `document_definitions` at the document level, making it available throughout the entire document.

### Range Scopes

Ranges allow definitions to apply to multiple consecutive units. When a scope phrase indicates a range (e.g., "sections 4 through 29"), the definition is applied to all units within that range. The system expands the range to include each unit between the first and last endpoints.

## External References

### Definitions from Separate Documents

When a definition references a location that doesn't exist in the current document, it is marked as an **external reference**. This occurs when:

1. An indirect definition points to a location that doesn't exist in the document (in this case, `indirect_loc_type` and `indirect_loc_number` are not populated)
2. A scope phrase refers to a different document entirely

The definition is marked with `external_reference` containing the original reference string. The definition may still have a `value` field if one was extracted, but the system recognizes it depends on an external source.

**Example**: A definition saying "as defined in section 42 of Title 18" when Title 18 isn't in the current document would be marked with `external_reference: "section 42 of Title 18"`.

## Definition Storage Locations

Definitions are stored in different locations based on their scope:

1. **`document_definitions`**: Document-wide definitions (no scope or scope resolution failed)
   - Location: `parsed_content['document_information']['document_definitions']`

2. **`unit_definitions`**: Definitions scoped to organizational units
   - Location: `parsed_content['document_information']['organization']['content'][...][unit_type][unit_number]['unit_definitions']`

3. **`defined_terms`**: Definitions scoped to the same substantive unit where they were found
   - Location: `parsed_content['content'][unit_type_plural][unit_number]['defined_terms']`

4. **`ext_definitions`**: Definitions scoped to a different substantive unit than where they were found
   - Location: `parsed_content['content'][unit_type_plural][unit_number]['ext_definitions']`

## Definition Fields

Each definition entry contains:

- **`term`**: The term being defined
- **`value`**: The definition text
- **`def_kind`**: "direct" or "elaboration" (see Direct vs Elaborational above)
- **`source_type`**: Type of unit where definition was originally found (e.g., "section")
- **`source_number`**: Number/identifier of source unit
- **`indirect`**: Original indirect reference string (if applicable)
- **`indirect_loc_type`**: Resolved indirect location type (only populated if the indirect reference points to a location within the document)
- **`indirect_loc_number`**: Resolved indirect location number (only populated if the indirect reference points to a location within the document)
- **`external_reference`**: External reference string (populated if the indirect reference points outside the document, in which case `indirect_loc_type` and `indirect_loc_number` are not populated)
- **`scope`**: Original scope phrase (before resolution)
- **`quality_checked`**: Boolean indicating if quality evaluation has been performed
- **`scope_processed`**: Boolean indicating if scope resolution has been completed


