# Scope Resolution Logic Flow

This document explains how the scope resolution system interprets `element_type` and `element_designation` from the AI's scope response to determine where definitions should be placed in the document structure.

## Overview

The scope resolution process takes structured information from the AI (element_type and element_designation) and maps it to actual document locations. The logic follows a decision tree that handles different types of references in priority order.

## Key Concepts

- **Working Unit**: The substantive unit (e.g., section, article) where the scope language was found
- **element_type**: The type of unit referenced (e.g., "section", "subsection", "chapter", "paragraph")
- **element_designation**: The specific identifier (e.g., "454", "current", "(a)", "454(a)")
- **Substantive Unit Types**: Recognized operational unit types in the document (e.g., "section", "article")
- **Organizational Unit Types**: Recognized organizational structure types (e.g., "chapter", "part", "title")

## Decision Flow for Single Element References (Non-Range)

### Step 1: Initial Classification

First, the system classifies the `element_type`:

```python
is_organizational = element_type in org_item_name_set
is_substantive = element_type in substantive_unit_types
```

**Important**: Only checks if `element_type` itself is recognized, NOT if stripping "sub" prefix would match a type. This allows unrecognized types like "subsection" to fall through to special handling.

### Step 2: Handle "current" References

If `element_designation == "current"`:

#### 2a. If `is_substantive == True`:
- **Action**: Resolve to the current working unit
- **Result**: `[{item_type_name: item_number}]`
- **Important Note**: This resolves to the working unit regardless of whether `element_type` matches `item_type_name`. For example, if the working unit is section 1432 and the AI returns `element_type="article"` with `element_designation="current"`, it will still resolve to section 1432 (the working unit), not to a current article.
- **Logging**: If `element_type` doesn't match `item_type_name`, a document issue is logged to track this unusual case.
- **Example**: "current section" → `[{"section": "1432"}]` (if working unit is section 1432)
- **Example**: "current article" (when working unit is section 1432) → `[{"section": "1432"}]` (resolves to working unit, not an article) + logs issue

#### 2b. If `is_organizational == True`:
- **Action**: Resolve "current" from the working unit's organizational context
- **Process**: Look through `current_item_context` to find the specified organizational unit type
- **Result**: Organizational path (e.g., `[{"title": "2"}, {"chapter": "6A"}]`)
- **Example**: "current chapter" → finds chapter in context and returns its path

#### 2c. If neither recognized:
- **Action**: Treat any unrecognized element_type with "current" designation as a reference to the working unit
- **Result**: `[{item_type_name: item_number}]`
- **Note**: This handles any unrecognized type with "current" (e.g., "current paragraph", "current subparagraph", "current subsection" when subsection is not a recognized type)
- **Rationale**: When the AI identifies an unrecognized type with "current" designation, it's most reasonable to interpret this as referring to the current working unit where the scope language was found

### Step 3: Handle Ranges

If `element_designation` is a dictionary with "first" and "last" keys, handle as range (see Range Handling section below).

### Step 4: Handle Single Element (Not "current", Not Range)

#### 4a. If `is_substantive == True`:
- **Action**: Find the substantive unit using maximum matching
- **Process**: Calls `find_substantive_unit_with_maximum_matching(element_type, element_designation)`
  - First tries exact match with `element_type` (which is recognized since `is_substantive == True`)
  - If exact match fails, tries longest prefix match with `element_type`
  - As a fallback, may strip "sub" prefix and try with base type (e.g., if "subsection" is recognized but no "subsection" units exist, tries "section")
  - **Note**: Since `is_substantive == True` means `element_type` itself is recognized, the stripping is just a fallback inside the matching function, not a special case
- **Result**: `[{matched_type: matched_designation}]` if found, otherwise nothing added
- **Example**: `element_type="section"`, `element_designation="454"` → `[{"section": "454"}]`
- **Example**: `element_type="section"`, `element_designation="454(a)"` → `[{"section": "454"}]` (longest prefix match)

#### 4b. If `is_organizational == True`:
- **Action**: Find the organizational unit in the document structure
- **Process**: Search organizational hierarchy and build full path from root
- **Result**: Organizational path (e.g., `[{"title": "2"}, {"chapter": "6A"}]`)
- **Example**: `element_type="chapter"`, `element_designation="6A"` → `[{"title": "2"}, {"chapter": "6A"}]`

#### 4c. If neither recognized (the "else" block):

This is where the special logic applies:

##### 4c.1. Check for "sub" + recognized_unit_type Reference

**Condition**: `element_type` starts with "sub" or "sub-" and, after stripping, matches a recognized substantive unit type

**Note**: Since we're in Step 4c, we already know `element_type` itself is NOT recognized as organizational or substantive.

**Process** (`_resolve_sub_working_unit_type` function):
1. Strip "sub" or "sub-" prefix from `element_type`
2. Check if stripped type is a recognized substantive unit type
   - If not recognized: Return None
   - If recognized: Continue to step 3
3. Find longest unit number that matches the beginning of `element_designation` for the stripped type
   - If no match found and stripped type matches working unit type: Return `item_number` (current working unit)
   - If no match found and stripped type is different: Return None
   - If match found: Continue to step 4
4. Return result:
   - If stripped type matches `item_type_name` (working unit type): Return the matched unit number (string)
   - If stripped type is different: Return `{stripped_type: matched_unit_number}` (dict)

**Examples**:
- `element_type="subsection"`, `element_designation="(a)"`, working unit is section 1432:
  - "subsection" not recognized ✓
  - Strips to "section" ✓
  - "section" is recognized ✓
  - Matches working unit type "section" ✓
  - "(a)" doesn't start with any section number → Returns "1432" (current working unit)
  - **Result**: `[{"section": "1432"}]`

- `element_type="subsection"`, `element_designation="454(a)"`, working unit is section 1432:
  - "subsection" not recognized ✓
  - Strips to "section" ✓
  - "section" is recognized ✓
  - Matches working unit type "section" ✓
  - "454(a)" starts with "454" (and "45" and "4", but "454" is longest) → Returns "454"
  - **Result**: `[{"section": "454"}]`

- `element_type="subparagraph"`, `element_designation="5"`, working unit is section 1432:
  - "subparagraph" not recognized ✓
  - Strips to "paragraph" ✓
  - "paragraph" is recognized (but different from working unit type "section") ✓
  - "5" matches paragraph 5 → Returns `{"paragraph": "5"}`
  - **Result**: `[{"paragraph": "5"}]`

- `element_type="subsection"`, `element_designation="454(a)"`, but "subsection" IS a recognized type:
  - "subsection" IS recognized → Returns None
  - Falls through to normal substantive unit handling (Step 4a)

##### 4c.2. Check for paragraph/subparagraph Reference

**Condition**: `element_type` is "paragraph" or "subparagraph" AND not recognized as any type AND `element_designation` is NOT "current" (not a range)

**Note**: This is the same logic as Step 2c, but handles cases where `element_designation` has a specific value (e.g., "paragraph 5") rather than "current" (e.g., "current paragraph"). Both resolve to the current working unit.

**Process**:
- Check if "paragraph"/"subparagraph" is in organizational types (case-insensitive)
- Check if "paragraph"/"subparagraph" is in substantive types (case-insensitive)
- Check if stripping "sub" prefix gives a recognized substantive type
- If none of the above:
  - **Action**: Resolve to current working unit (ignoring the designation value)
  - **Result**: `[{item_type_name: item_number}]`

**Example**: `element_type="paragraph"`, `element_designation="5"`, working unit is section 1432:
- "paragraph" not recognized ✓
- **Result**: `[{"section": "1432"}]` (designation "5" is ignored, resolves to current working unit)

**When would execution reach 4c.2 but not 2c?**
- When `element_designation` is NOT "current" (e.g., "paragraph 5" instead of "current paragraph")
- Step 2 only executes when `element_designation == "current"`, so Step 2c never runs
- Execution proceeds to Step 4, and Step 4c.2 handles it

##### 4c.3. Unknown Type

If none of the above conditions are met:
- Log as unknown type
- Skip (no scope added)

## Range Handling

For ranges (when `element_designation` is a dict with "first" and "last"):

1. **Resolve endpoints**:
   - If endpoint is "current": Handle as described in Step 2
   - If endpoint is not "current" and type is unrecognized: Try `_resolve_sub_working_unit_type` for that endpoint
   
2. **Expand range**:
   - If `is_substantive`: Expand using `element_type`
   - If "sub" + working_unit_type: Expand using `item_type_name` (the working unit type)
   - If paragraph/subparagraph: Treat as single working unit
   - Otherwise: Log and skip

## Priority Order

The logic follows this priority:

1. **"current" designation** → Handled first (Step 2)
2. **Range** → Handled second (Step 3)
3. **Recognized substantive type** → Normal substantive unit lookup (Step 4a)
4. **Recognized organizational type** → Organizational path lookup (Step 4b)
5. **"sub" + working_unit_type** → Special logic (Step 4c.1) - **HIGHEST PRIORITY for unrecognized types**
6. **paragraph/subparagraph** → Special logic (Step 4c.2)
7. **Unknown** → Logged and skipped

## Key Design Decisions

1. **Only check `element_type` itself for recognition**: This allows "subsection" (not recognized) to be handled specially even though "section" (base type) is recognized.

2. **Longest prefix matching**: When checking if `element_designation` starts with a unit number, we find the longest match. This handles cases like "454(a)" where "4", "45", and "454" all match, but "454" is the correct choice.

3. **Case-insensitive matching**: All type comparisons are case-insensitive to handle variations.

4. **Working unit as fallback**: When a "sub" + working_unit_type reference doesn't match any unit number, it defaults to the current working unit (where the scope language was found).

## Example Scenarios

### Scenario 1: "For purposes of subsection (a)"
- **Working unit**: section 1432
- **AI returns**: `element_type="subsection"`, `element_designation="a"`
- **Flow**:
  1. `is_substantive = False` (subsection not recognized)
  2. `is_organizational = False`
  3. Not "current", not range
  4. Falls to else block
  5. `_resolve_sub_working_unit_type`:
     - "subsection" not recognized ✓
     - Strips to "section" ✓
     - Matches "section" ✓
     - "(a)" doesn't start with any section number
     - Returns "1432"
  6. **Result**: `[{"section": "1432"}]` → Definition stays in section 1432

### Scenario 2: "For purposes of subsection 454(a)"
- **Working unit**: section 1432
- **AI returns**: `element_type="subsection"`, `element_designation="454(a)"`
- **Flow**:
  1. `is_substantive = False`
  2. `is_organizational = False`
  3. Falls to else block
  4. `_resolve_sub_working_unit_type`:
     - "subsection" not recognized ✓
     - Strips to "section" ✓
     - Matches "section" ✓
     - "454(a)" starts with "454" (longest match)
     - Returns "454"
  5. **Result**: `[{"section": "454"}]` → Definition added to section 454

### Scenario 3: "In this chapter"
- **Working unit**: section 1432
- **AI returns**: `element_type="chapter"`, `element_designation="current"`
- **Flow**:
  1. `is_organizational = True` (chapter is recognized)
  2. `element_designation == "current"`
  3. Resolve from context
  4. **Result**: `[{"title": "2"}, {"chapter": "6A"}]` → Definition added to chapter 6A

### Scenario 4: "In paragraph 5"
- **Working unit**: section 1432
- **AI returns**: `element_type="paragraph"`, `element_designation="5"`
- **Flow**:
  1. `is_substantive = False` (paragraph not recognized)
  2. `is_organizational = False`
  3. Falls to else block
  4. `_resolve_sub_working_unit_type` returns None (doesn't match "sub" + "section")
  5. Checks paragraph/subparagraph: "paragraph" not recognized ✓
  6. **Result**: `[{"section": "1432"}]` → Definition stays in section 1432

## Summary

The scope resolution logic prioritizes:
1. Explicit "current" references
2. Recognized unit types (substantive or organizational)
3. Special handling for "sub" + working_unit_type (when not recognized)
4. Special handling for paragraph/subparagraph (when not recognized)
5. Unknown types are logged and skipped

This ensures that references like "subsection (a)" are correctly interpreted as referring to the current working unit, while still allowing recognized types like "section" to be resolved normally.

