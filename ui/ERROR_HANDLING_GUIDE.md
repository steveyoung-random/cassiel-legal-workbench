# UI Error Handling Guide

This guide explains the standardized error handling utilities available in the Cassiel Legal Workbench UI.

## Overview

The UI provides three standardized error handling functions in `ui/utils.py`:

1. **`show_error()`** - Basic error with optional technical details
2. **`show_error_with_action()`** - Error with actionable next steps
3. **`show_error_with_retry()`** - Error with retry button

All functions follow these principles:
- User-friendly messages without technical jargon
- Optional technical details hidden in expander
- Consistent styling with ❌ icon
- Actionable guidance when possible

---

## Functions

### 1. `show_error()`

Display a basic error message with optional technical details.

**Signature:**
```python
show_error(st, message: str, exception: Optional[Exception] = None, details: Optional[str] = None)
```

**Usage:**
```python
from ui.utils import show_error

# Simple error
show_error(st, "Failed to load document")

# Error with exception
try:
    result = process_document()
except Exception as e:
    show_error(st, "Failed to process document", exception=e)

# Error with custom details
show_error(st, "Failed to connect to database", details="Connection timeout after 30s")
```

**When to use:**
- Generic errors without specific recovery actions
- Errors where technical details might help debugging
- Validation failures

---

### 2. `show_error_with_action()`

Display an error with actionable next steps to resolve the issue.

**Signature:**
```python
show_error_with_action(st, message: str, action_label: str, action_info: str,
                       exception: Optional[Exception] = None)
```

**Usage:**
```python
from ui.utils import show_error_with_action

# Worker not responding
show_error_with_action(st,
    "Worker not responding",
    "To start the worker",
    "Run: python worker/run_worker.py")

# Missing configuration
show_error_with_action(st,
    "Configuration file not found",
    "To create config",
    "Copy config.example.json to config.json and update settings")

# Missing dependency
show_error_with_action(st,
    "Database not found",
    "To initialize database",
    "Run: python worker/queue.py --init",
    exception=e)
```

**When to use:**
- Errors with clear resolution steps
- Missing dependencies or setup issues
- Configuration problems
- Service unavailability (worker, database, etc.)

---

### 3. `show_error_with_retry()`

Display an error with a retry button for transient failures.

**Signature:**
```python
show_error_with_retry(st, message: str, retry_callback: callable, retry_label: str = "Retry",
                      exception: Optional[Exception] = None)
```

**Usage:**
```python
from ui.utils import show_error_with_retry

# Failed to load jobs - retry by rerunning
def retry_load_jobs():
    st.session_state.reload_jobs = True
    st.rerun()

show_error_with_retry(st,
    "Failed to load jobs from database",
    retry_load_jobs,
    "Retry Loading")

# Failed network request - retry specific operation
def retry_submit():
    submit_job_to_queue(job_params)

try:
    submit_job_to_queue(job_params)
except ConnectionError as e:
    show_error_with_retry(st,
        "Failed to submit job. Network connection lost.",
        retry_submit,
        exception=e)
```

**When to use:**
- Network errors or timeouts
- Transient database connection failures
- Rate limiting or temporary service unavailability
- User-triggered operations that can be safely retried

---

## Best Practices

### Error Message Guidelines

**Good error messages:**
- ✅ "Failed to load document. The manifest file may be corrupted."
- ✅ "Unable to submit job. Worker process is not running."
- ✅ "Document not found. It may have been deleted."

**Poor error messages:**
- ❌ "KeyError: 'manifest_path'"
- ❌ "Exception in thread: Connection refused"
- ❌ "Error: NoneType object has no attribute 'get'"

### When to Show Technical Details

Always provide technical details for:
- Unexpected exceptions
- System errors (file I/O, database, network)
- Integration failures (job queue, worker)

Optional for:
- Validation errors (usually clear from user-friendly message)
- User input errors (explain what's wrong, not stack trace)

### Choosing the Right Function

| Scenario | Function | Example |
|----------|----------|---------|
| Generic failure | `show_error()` | "Failed to load documents" |
| Missing setup/config | `show_error_with_action()` | "Worker not running. To start: python worker/run_worker.py" |
| Transient network issue | `show_error_with_retry()` | "Failed to fetch job status" + Retry button |
| Validation failure | `show_error()` | "Please enter a valid question" |
| Service unavailable | `show_error_with_action()` | "Database not found. To initialize: python queue.py --init" |

---

## Migration from Old Error Handling

### Old Pattern:
```python
try:
    result = load_document()
except Exception as e:
    st.error(f"Failed: {str(e)}")
    with st.expander("Show details"):
        import traceback
        st.code(traceback.format_exc())
```

### New Pattern:
```python
from ui.utils import show_error

try:
    result = load_document()
except Exception as e:
    show_error(st, "Failed to load document", exception=e)
```

**Benefits:**
- Consistent styling (❌ icon)
- Less boilerplate code
- Standardized expander label ("Show technical details")
- User-friendly message required (forces better UX)

---

## Current Status

**Completed:**
- ✅ Created standardized error handling utilities in `ui/utils.py`
- ✅ Defined three error display functions with clear use cases
- ✅ Documented usage patterns and best practices

**Existing Error Handling:**
Many UI components already follow good practices:
- User-friendly messages
- Technical details in expanders
- Emoji icons (❌) for errors

**Recommended Next Steps:**
1. Gradually migrate existing error handling to use new utilities
2. Add retry buttons for transient failures (job loading, network requests)
3. Add actionable messages for setup/configuration errors
4. Review and improve error messages for clarity and actionability

---

## Examples by Component

### Job Submission Errors

```python
# Old
st.error(f"❌ Failed to submit Stage 2 job: {str(e)}")
with st.expander("Show technical details", expanded=False):
    st.code(str(e), language='text')

# New
show_error_with_action(st,
    "Failed to submit Stage 2 job",
    "Check worker status",
    "Ensure worker is running: python worker/run_worker.py",
    exception=e)
```

### Document Loading Errors

```python
# Old
st.error("Failed to load documents. Please try refreshing.")
with st.expander("Show technical details", expanded=False):
    st.code(traceback.format_exc(), language='text')

# New
def retry_load():
    st.rerun()

show_error_with_retry(st,
    "Failed to load documents",
    retry_load,
    "Refresh",
    exception=e)
```

### Configuration Errors

```python
# Old
st.error("No parsers available. Check parser registry.")

# New
show_error_with_action(st,
    "No parsers available",
    "To check configuration",
    "Verify parsers/ directory and config.json settings")
```

---

*Last Updated: 2026-01-02*
