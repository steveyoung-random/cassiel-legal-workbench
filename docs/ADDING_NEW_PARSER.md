# Adding a New Parser

This guide shows exactly where you need to make changes to add a new parser to the system.

## Required Changes (2 places)

### 1. Create the Adapter File
**File**: `parsers/your_parser_adapter.py`

Create a new file following the pattern of existing adapters. Example:

```python
"""
Your Parser Adapter

Wraps your_parser_module.py to provide ParserAdapter interface.
"""

from typing import Dict, Any, Optional
from .adapter import ParserAdapter, ParserCapabilities, ParseResult, SplitDetectionResult


class YourParserAdapter(ParserAdapter):
    """Adapter for Your Parser"""

    def get_capabilities(self) -> ParserCapabilities:
        return ParserCapabilities(
            parser_type='your_parser',  # Unique identifier
            display_name='Your Parser Display Name',
            version='1.0',
            file_extensions=['.ext1', '.ext2'],
            supports_splitting=False,  # or True if supported
            split_by_default=False,
            split_unit_name=None,  # or 'unit_name' if splitting supported
            split_parent_name=None,  # or 'parent_name' if splitting supported
            output_schema_version='0.3',
            module_name='your_parser_module',
            adapter_class='YourParserAdapter'
        )

    def detect_split_units(self, file_path: str) -> SplitDetectionResult:
        # Implementation based on whether splitting is supported
        pass

    def parse_file(
        self,
        file_path: str,
        config: Dict[str, Any],
        params: Optional[Dict[str, Any]] = None
    ) -> ParseResult:
        # Implementation that calls your existing parser module
        pass
```

### 2. Register in Registry
**File**: `parsers/registry.py`

Add registration in the `load_parsers_from_config()` function:

```python
def load_parsers_from_config(config: Dict[str, Any] = None):
    # ... existing registrations ...
    
    try:
        from .your_parser_adapter import YourParserAdapter
        _registry.register('your_parser', YourParserAdapter())
    except ImportError as e:
        print(f"Warning: Could not load Your Parser adapter: {e}")
```

**Lines to modify**: Add after line 94 (after CA HTML adapter registration)

---

## Recommended Changes (1 place)

### 3. Export from Package Init
**File**: `parsers/__init__.py`

Add import and export (optional but recommended for clean imports):

```python
# Add to imports section (around line 42):
try:
    from .your_parser_adapter import YourParserAdapter
except ImportError:
    YourParserAdapter = None

# Add to __all__ list (around line 67):
    'YourParserAdapter',
```

---

## Optional Changes (for testing)

### 4. Add Test Cases
**Files**: 
- `test_parser_adapters.py` - Add capability and detection tests
- `test_registry_loading.py` - Add assertion for new parser

Example additions:

**In `test_parser_adapters.py`**:
```python
def test_your_parser_capabilities():
    """Test Your Parser adapter capabilities"""
    print("Testing Your Parser capabilities...")
    adapter = YourParserAdapter()
    caps = adapter.get_capabilities()
    assert caps.parser_type == 'your_parser'
    # ... more assertions ...
    print("[PASS] Your Parser capabilities test passed")

# Add to run_all_tests():
    test_your_parser_capabilities()
```

**In `test_registry_loading.py`**:
```python
    assert 'your_parser' in parsers, "Your Parser not registered"
```

---

## Summary

**Minimum Required**: **2 places**
1. Create `parsers/your_parser_adapter.py`
2. Register in `parsers/registry.py` → `load_parsers_from_config()`

**Recommended**: **3 places** (adds #3)
3. Export from `parsers/__init__.py`

**For Complete Testing**: **5 places** (adds #4)
4. Add tests to `test_parser_adapters.py`
5. Add assertion to `test_registry_loading.py`

---

## Notes

- **No changes needed** to:
  - `parsers/adapter.py` (base classes)
  - `parsers/discovery.py` (works with any parser)
  - `utils/config.py` (uses defaults if config.json missing)
  - Worker code (uses registry dynamically)
  - UI code (uses registry dynamically)

- The system is designed to be **discovery-based** - organizational structure is found from parsed output, not hardcoded.

- Parser type identifiers (like `'your_parser'`) are only used in:
  1. The adapter's `get_capabilities()` method
  2. The registry registration call
  3. Optional: test assertions

- All other code uses the registry dynamically, so no hardcoded parser types elsewhere.

