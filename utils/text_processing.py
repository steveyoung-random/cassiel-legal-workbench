# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License
from math import remainder
from bs4 import BeautifulSoup
import re
from typing import List, Dict, Any, Tuple
from .ai_client import GetClient
from .ai_client import Query

def strip_emphasis_marks(term):
    """
    Strip surrounding quotes and markdown emphasis marks from a proposed term.

    Iteratively removes surrounding single quotes, double quotes, asterisks (*),
    and underscores (_) — only when the mark appears at both the start and end.
    Leading/trailing spaces are stripped before and after each iteration.

    Examples:
        '"term"'       -> 'term'
        "'term'"       -> 'term'
        '*term*'       -> 'term'
        '**term**'     -> 'term'
        '_term_'       -> 'term'
        '__term__'     -> 'term'
        '**"term"**'   -> 'term'  (iterative)
        "term's"       -> "term's"  (interior quote not removed)
        'term*'        -> 'term*'  (not surrounding, not removed)

    Args:
        term: The term string to clean

    Returns:
        str: Term with surrounding quotes and emphasis marks removed
    """
    if not isinstance(term, str):
        return term
    while True:
        original = term
        term = term.strip()
        if len(term) >= 2 and term[0] == '"' and term[-1] == '"':
            term = term[1:-1]
            continue
        if len(term) >= 2 and term[0] == "'" and term[-1] == "'":
            term = term[1:-1]
            continue
        if len(term) >= 2 and term[0] == '*' and term[-1] == '*':
            term = term.strip('*')
            continue
        if len(term) >= 2 and term[0] == '_' and term[-1] == '_':
            term = term.strip('_')
            continue
        if term == original:
            break
    return term


def clean_text(text):
    # Replace non-breaking spaces and other special spaces
    text = text.replace('\xa0', ' ')
    text = text.replace('&nbsp;', ' ')
    text = text.replace('\u202f', ' ')

    # Replace special quotes with standard quotes
    text = text.replace('\u2018', "'")  # single quote open
    text = text.replace('\u2019', "'")  # single quote close
    text = text.replace('\u201c', '"')  # double quote open
    text = text.replace('\u201d', '"')  # double quote close

    text = text.replace('\u2011', '-')  # non-breaking hyphen
    text = text.replace('\u2013', '-')  # en dash
    text = text.replace('&ndash;', '-') # en dash
    text = text.replace('\u2014', '--') # em dash
    text = text.replace('&mdash;', '--') # em dash

    text = text.replace('\u00ad', '') # soft hyphen

    # Normalize spaces
    text = re.sub(r'\n\t+ *', ' ', text) # Solve for conflict between CA and EU file formats
    text = re.sub(r'\n+', '\n', text).strip('\n')

    return text

def clean_summary_text(text):
    """
    Clean AI-generated summary text.

    Applies standard text cleaning, then removes leading hyphens and strips
    leading/trailing whitespace. This is specifically for AI model summaries.

    Args:
        text: The summary text to clean

    Returns:
        str: Cleaned summary text
    """
    # First apply standard text cleaning
    text = clean_text(text)
    # Remove leading hyphens
    text = text.lstrip('-')
    # Strip leading and trailing spaces
    text = text.strip()
    return text

def canonical_org_types(type_name):
    # Take an org type name in singular form, and return it in lower case, with the right plural form.
    plurals = {
        'annex': 'annexes',
        'appendix': 'appendices',
        'ccl_category': 'ccl_categories',
        'index': 'indices',
        'datum': 'data',
        'addendum': 'addenda',
        'erratum': 'errata',
        'analysis': 'analyses',
        'memorandum': 'memoranda',
        'basis': 'bases',
        'thesis': 'theses'}
    name = str(type_name).lower()
    name_plural = name + 's'
    if name in plurals.keys():
        name_plural = plurals[name]
    return name, name_plural

def table_to_text(client, table_html, logfile=''):
    prompt = 'Please take this table that is in html form and reproduce it in plain text, as accurately as you can. '
    prompt += 'Respond with no preamble and no response other than the plain text representation of the table. '
    prompt += 'Here is the table:\n'
    prompt += str(table_html)
    response = Query(client, prompt, logfile=logfile, json_output=0)
    return response

def extract_trailing_paren(input):
    # Function to extract the contents of a trailing parenthetical (if any).
    remainder_text = input
    paren_text = ''
    input = str(input).strip()
    length = len(input)
    location = length - 1
    count = 0
    if ')' == input[location]:
        count = 1
        while count > 0 and location > 0:
            location -= 1
            if ')' == input[location]:
                count += 1
            elif '(' == input[location]:
                count -= 1
    if 0 == count and location < length -1: # Found parenthetical.
        remainder_text = input[:location]
        paren_text = input[location+1:length-1]
    return remainder_text, paren_text

# =============================================================================
# HTML Table to Plain Text Conversion
# =============================================================================
# Converts HTML tables to plain text with word-wrapping within fixed-width
# columns. Handles colspan and rowspan. Falls back to original HTML for
# tables that are too complex to convert reliably (e.g., nested tables).


def _extract_table_cell_text(cell) -> str:
    """
    Extract text from a table cell, properly handling <br> and <sup> tags.

    - <br> tags are converted to spaces
    - <sup> tags (footnote markers) are converted to [n] notation
    """
    # Replace <br> with a unique marker we can convert to space later
    for br in cell.find_all('br'):
        br.replace_with('\u2028')  # Line separator as temp marker

    # Replace <sup> with bracketed text (for footnote markers)
    for sup in cell.find_all('sup'):
        sup_text = sup.get_text(strip=True)
        sup.replace_with(f'[{sup_text}]')

    # Get text
    text = cell.get_text()

    # Convert line separator markers to single space
    text = text.replace('\u2028', ' ')

    # Normalize whitespace
    text = re.sub(r'[\s]+', ' ', text)
    text = text.strip()

    return text


def _parse_table_to_grid(html_table: str) -> Tuple[List[List[str]], int, int, List[int], List[int]]:
    """
    Parse HTML table into a 2D grid, handling colspan and rowspan.

    Returns:
        (grid, num_rows, num_cols, header_row_indices, footer_row_indices)
    """
    soup = BeautifulSoup(html_table, 'html.parser')
    table = soup.find('table')

    if not table:
        return [], 0, 0, [], []

    rows = table.find_all('tr')

    # Determine true column count
    num_cols = 0
    for row in rows:
        cells = row.find_all(['td', 'th'])
        row_cols = sum(int(cell.get('colspan', 1)) for cell in cells)
        num_cols = max(num_cols, row_cols)

    # Track header and footer rows by finding their positions in the full row list
    header_row_indices = []
    footer_row_indices = []

    thead = table.find('thead')
    if thead:
        thead_rows = thead.find_all('tr')
        for i in range(len(thead_rows)):
            header_row_indices.append(i)

    tfoot = table.find('tfoot')
    if tfoot:
        tfoot_rows = tfoot.find_all('tr')
        # Footer rows come after thead and tbody rows
        # Find the actual indices of tfoot rows in the full row list
        for i, row in enumerate(rows):
            if row.parent == tfoot:
                footer_row_indices.append(i)

    # Create grid and track occupied cells
    grid = []
    occupied = {}

    for row_idx, row in enumerate(rows):
        cells = row.find_all(['th', 'td'])

        while len(grid) <= row_idx:
            grid.append([''] * num_cols)

        col_idx = 0
        for cell in cells:
            while col_idx < num_cols and (row_idx, col_idx) in occupied:
                col_idx += 1

            if col_idx >= num_cols:
                break

            rowspan = int(cell.get('rowspan', 1))
            colspan = int(cell.get('colspan', 1))

            text = _extract_table_cell_text(cell)
            grid[row_idx][col_idx] = text

            for r in range(rowspan):
                for c in range(colspan):
                    if r > 0 or c > 0:
                        occupied[(row_idx + r, col_idx + c)] = True
                        while len(grid) <= row_idx + r:
                            grid.append([''] * num_cols)

            col_idx += colspan

    return grid, len(grid), num_cols, header_row_indices, footer_row_indices


def _get_table_caption(html_table: str) -> str:
    """Extract table caption if present."""
    soup = BeautifulSoup(html_table, 'html.parser')
    table = soup.find('table')
    if table:
        caption = table.find('caption')
        if caption:
            return caption.get_text(strip=True)
    return ""


def _assess_table_complexity(html_table: str) -> Dict[str, Any]:
    """
    Assess the complexity of a table to determine if it can be reliably converted.

    Returns dict with:
        - convertible: bool - whether the table should be converted
        - reason: str - reason if not convertible
        - metrics: dict - complexity metrics
    """
    soup = BeautifulSoup(html_table, 'html.parser')
    table = soup.find('table')

    if not table:
        return {'convertible': False, 'reason': 'No table element found', 'metrics': {}}

    # Check for nested tables inside cells
    for cell in table.find_all(['td', 'th']):
        if cell.find('table'):
            return {
                'convertible': False,
                'reason': 'Nested table detected inside cell',
                'metrics': {'nested': True}
            }

    rows = table.find_all('tr')
    if not rows:
        return {'convertible': False, 'reason': 'No rows found', 'metrics': {}}

    # Analyze structure
    metrics = {
        'row_count': len(rows),
        'max_colspan': 0,
        'max_rowspan': 0,
    }

    # Find max colspan and rowspan
    for cell in table.find_all(['td', 'th']):
        colspan = int(cell.get('colspan', 1))
        rowspan = int(cell.get('rowspan', 1))
        metrics['max_colspan'] = max(metrics['max_colspan'], colspan)
        metrics['max_rowspan'] = max(metrics['max_rowspan'], rowspan)

    # Check for extremely complex structures
    if metrics['max_colspan'] > 20:
        return {
            'convertible': False,
            'reason': f'Excessive colspan: {metrics["max_colspan"]}',
            'metrics': metrics
        }

    if metrics['max_rowspan'] > 50:
        return {
            'convertible': False,
            'reason': f'Excessive rowspan: {metrics["max_rowspan"]}',
            'metrics': metrics
        }

    return {'convertible': True, 'reason': None, 'metrics': metrics}


def _wrap_text(text: str, width: int) -> List[str]:
    """
    Word-wrap text to fit within specified width.
    Returns list of lines. Avoids breaking words when possible.
    """
    if not text:
        return ['']

    if width < 1:
        width = 1

    words = text.split()
    if not words:
        return ['']

    lines = []
    current_line = []
    current_length = 0

    for word in words:
        word_len = len(word)

        if word_len > width:
            # Word is longer than width - must break it
            if current_line:
                lines.append(' '.join(current_line))
                current_line = []
                current_length = 0

            while len(word) > width:
                lines.append(word[:width])
                word = word[width:]
            if word:
                current_line = [word]
                current_length = len(word)
        elif current_length + (1 if current_line else 0) + word_len <= width:
            # Word fits on current line
            current_line.append(word)
            current_length += (1 if len(current_line) > 1 else 0) + word_len
        else:
            # Start new line
            if current_line:
                lines.append(' '.join(current_line))
            current_line = [word]
            current_length = word_len

    if current_line:
        lines.append(' '.join(current_line))

    return lines if lines else ['']


def _get_longest_word(text: str) -> int:
    """Get the length of the longest word in text."""
    if not text:
        return 0
    words = text.split()
    return max((len(w) for w in words), default=0)


def _calculate_column_widths(grid: List[List[str]],
                              min_width: int = 4,
                              max_width: int = 35,
                              total_max_width: int = 200) -> List[int]:
    """
    Calculate optimal column widths with constraints.
    Ensures columns are wide enough to avoid breaking words when possible.
    """
    if not grid or not grid[0]:
        return []

    num_cols = len(grid[0])

    natural_widths = [min_width] * num_cols
    word_widths = [min_width] * num_cols

    for row in grid:
        for i, cell in enumerate(row):
            if i < num_cols:
                natural_widths[i] = max(natural_widths[i], len(cell))
                word_widths[i] = max(word_widths[i], _get_longest_word(cell))

    min_practical = [min(max(min_width, ww), max_width) for ww in word_widths]
    col_widths = [min(w, max_width) for w in natural_widths]
    col_widths = [max(cw, mp) for cw, mp in zip(col_widths, min_practical)]

    separator_width = 3 * (num_cols - 1)
    total_width = sum(col_widths) + separator_width

    if total_width > total_max_width and num_cols > 1:
        excess = total_width - total_max_width
        shrinkable = [(i, col_widths[i] - min_practical[i])
                      for i in range(num_cols) if col_widths[i] > min_practical[i]]

        if shrinkable:
            total_shrinkable = sum(s[1] for s in shrinkable)
            if total_shrinkable > 0:
                shrink_ratio = min(1.0, excess / total_shrinkable)
                for i, room in shrinkable:
                    reduction = int(room * shrink_ratio)
                    col_widths[i] -= reduction

    return col_widths


def html_table_to_plaintext(html_table: str,
                            min_width: int = 4,
                            max_width: int = 35,
                            total_max_width: int = 200) -> str:
    """
    Convert an HTML table to a plaintext representation with word-wrapping.

    Long cell content wraps to multiple lines rather than being truncated.
    Falls back to returning the original HTML if the table is too complex
    to convert reliably (e.g., contains nested tables).

    Row separators are added between each row:
    - Headers and footers use =+=  (double line style)
    - Regular data rows use -+-  (single line style)

    Args:
        html_table: HTML string containing a table
        min_width: Minimum column width (default: 4)
        max_width: Maximum column width before wrapping (default: 35)
        total_max_width: Target max total table width (default: 200)

    Returns:
        str: Plaintext representation of the table, or original HTML if
             conversion is not possible
    """
    # First, assess complexity
    assessment = _assess_table_complexity(html_table)
    if not assessment['convertible']:
        return html_table

    try:
        grid, num_rows, num_cols, header_rows, footer_rows = _parse_table_to_grid(html_table)
        caption = _get_table_caption(html_table)

        if not grid:
            return html_table

        col_widths = _calculate_column_widths(grid, min_width, max_width, total_max_width)
        if not col_widths:
            return html_table

        # Pre-build separator strings
        header_sep = "=+=".join("=" * w for w in col_widths)  # For headers/footers
        row_sep = "-+-".join("-" * w for w in col_widths)     # For regular rows

        lines = []
        if caption:
            lines.append(caption)
            lines.append("")

        last_header_row = max(header_rows) if header_rows else -1
        first_footer_row = min(footer_rows) if footer_rows else num_rows

        for row_idx, row in enumerate(grid):
            # Add separator before first footer row
            if row_idx == first_footer_row and footer_rows:
                lines.append(header_sep)

            wrapped_cells = []
            for i, cell in enumerate(row):
                width = col_widths[i] if i < len(col_widths) else min_width
                wrapped_cells.append(_wrap_text(cell, width))

            max_lines = max(len(wc) for wc in wrapped_cells) if wrapped_cells else 1

            for line_num in range(max_lines):
                formatted = []
                for i, wrapped in enumerate(wrapped_cells):
                    width = col_widths[i] if i < len(col_widths) else min_width
                    if line_num < len(wrapped):
                        text = wrapped[line_num]
                    else:
                        text = ''
                    formatted.append(text.ljust(width))

                lines.append(" | ".join(formatted))

            # Add separator after each row
            if row_idx == last_header_row:
                # After headers, use header separator
                lines.append(header_sep)
            elif row_idx < num_rows - 1 and row_idx not in footer_rows:
                # Between data rows (not after last row, not between footer rows)
                if row_idx + 1 not in footer_rows:
                    lines.append(row_sep)

        return "\n".join(lines)

    except Exception:
        # If anything goes wrong, fall back to original HTML
        return html_table


def deduplicate_breakpoints(breakpoints):
    """
    Remove duplicate breakpoint locations, keeping only the breakpoint with
    the lowest priority level for each location.
    
    Each breakpoint is a list of [location, priority] where lower priority 
    numbers take precedence. Returns a new list of breakpoints with no 
    duplicate locations, sorted by location.
    
    Args:
        breakpoints: List of [location, priority] pairs
        
    Returns:
        list: Deduplicated list of breakpoints sorted by location
    """
    if not breakpoints:
        return []
    
    # Create a dictionary to track the minimum priority for each location
    location_priority = {}
    for brk_entry in breakpoints:
        location = brk_entry[0]
        priority = brk_entry[1]
        if location not in location_priority or priority < location_priority[location]:
            location_priority[location] = priority
    
    # Build the result list from the dictionary, sorted by location
    result = [[location, priority] for location, priority in sorted(location_priority.items())]
    
    return result


def remove_blank_lines(text, breakpoints):
    """
    Remove blank lines from the given text and adjust breakpoint offsets accordingly.
    
    This function removes blank lines from text while maintaining the integrity
    of breakpoint locations by adjusting their offsets based on the removed content.
    
    Args:
        text: The text string to process
        breakpoints: List of [location, priority] pairs indicating text breakpoints
        
    Returns:
        tuple: (modified_text, modified_breakpoints) with blank lines removed 
               and breakpoints adjusted
    """
    modified_text = ''
    modified_breakpoints = []
    lines = text.splitlines(keepends=True)
    pos_line_start = 0  # Character position as we move forward through the text.
    
    for line_text in lines:
        line_length = len(line_text)
        if line_text.isspace():  # Line is blank, so don't include in output, and adjust later breakpoints.
            for brk_entry in breakpoints:
                if brk_entry[0] > pos_line_start:
                    brk_entry[0] = brk_entry[0] - line_length  # Move down all above pos_line_start. Any at pos_line_start + line_length drop out in next iteration.
        else:
            modified_text += line_text
            for brk_entry in breakpoints:
                if brk_entry[0] >= pos_line_start and brk_entry[0] <= pos_line_start + line_length:
                    modified_breakpoints.append(brk_entry.copy())
                    brk_entry[0] = -1  # Ensure this one won't be added again.
            pos_line_start += line_length

    return modified_text, modified_breakpoints