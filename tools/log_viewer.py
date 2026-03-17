#!/usr/bin/env python3
"""
AI Model Log Viewer - Enhanced Version

A command line tool to view AI model call logs with split-screen display.
Each log entry contains: timestamp, prompt, and response.
Features:
- Split-screen view (request on left, response on right)
- Formatting preservation (spaces, newlines, indentation)
- Independent scrolling for each side
- Easy navigation between entries

Works with log files from Stage 2, Stage 3, and Stage 4 (Q&A) processing.
Log format: concatenated JSON arrays [timestamp, full_cache?, query_prompt, result, ...].

Usage:
  python tools/log_viewer.py <path/to/log0001.json>
  python tools/log_viewer.py <path/to/log0001.json> --entry 5
"""
# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License

import json
import sys
import os
import argparse
import shutil
from typing import List, Tuple, Optional

# Try to import rich for better UI, fallback to standard library if not available
try:
    from rich.console import Console
    from rich.panel import Panel
    from rich.layout import Layout
    from rich.text import Text
    from rich.live import Live
    from rich.columns import Columns
    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False


class LogViewer:
    def __init__(self, log_file: str):
        self.log_file = log_file
        self.entries: List[Tuple[str, str, str]] = []
        self.current_index = 0
        self.load_log_file()
        self.console = Console() if RICH_AVAILABLE else None

    def load_log_file(self):
        """Load and parse the log file."""
        try:
            with open(self.log_file, 'r', encoding='utf-8') as f:
                content = f.read().strip()

            # Split by '][' to separate individual JSON arrays
            parts = content.split('][')

            for i, part in enumerate(parts):
                # Clean up the part to make it a valid JSON array
                if i == 0:
                    part = part + ']'
                elif i == len(parts) - 1:
                    part = '[' + part
                else:
                    part = '[' + part + ']'

                try:
                    entry = json.loads(part)
                    if len(entry) >= 3:
                        timestamp = entry[0]
                        # Handle different log formats:
                        # Format 1 (newer): [timestamp, full_cache, query_prompt, result, cache_created, cache_read]
                        # Format 2 (older): [timestamp, query_prompt, result]
                        if len(entry) >= 4:
                            # Newer format: combine items 1 and 2 as prompt, item 3 is response
                            full_cache = entry[1] if entry[1] else ""
                            query_prompt = entry[2] if entry[2] else ""
                            prompt = full_cache + "\n\n" + query_prompt if full_cache else query_prompt
                            response = entry[3] if entry[3] else ""
                        else:
                            # Older format: item 1 is prompt, item 2 is response
                            prompt = entry[1] if entry[1] else ""
                            response = entry[2] if entry[2] else ""
                        self.entries.append((timestamp, prompt, response))
                except json.JSONDecodeError as e:
                    print(f"Warning: Could not parse entry {i}: {e}")
                    continue

            print(f"Loaded {len(self.entries)} log entries from {self.log_file}")

        except FileNotFoundError:
            print(f"Error: Log file '{self.log_file}' not found.")
            sys.exit(1)
        except Exception as e:
            print(f"Error loading log file: {e}")
            sys.exit(1)

    def preserve_formatting(self, text: str) -> str:
        """Preserve formatting by converting literal \n to actual newlines and preserving spaces."""
        if not text:
            return ""
        # Replace literal \n with actual newlines
        text = text.replace('\\n', '\n')
        # Preserve all other formatting (spaces, tabs, etc.)
        return text

    def get_terminal_size(self):
        """Get terminal size, with fallbacks."""
        try:
            return shutil.get_terminal_size()
        except Exception:
            from types import SimpleNamespace
            return SimpleNamespace(columns=80, lines=24)

    def split_text_to_lines(self, text: str, max_width: int) -> List[str]:
        """Split text into lines respecting max_width, preserving formatting where possible."""
        lines = []
        for line in text.split('\n'):
            # If line fits, use it as-is
            if len(line) <= max_width:
                lines.append(line)
            else:
                # Split long lines, but try to preserve indentation
                indent = len(line) - len(line.lstrip())
                indent_str = line[:indent]
                remaining = line[indent:]

                # Split remaining text into chunks
                while remaining:
                    # Take up to max_width characters (accounting for indent)
                    chunk = remaining[:max_width - indent]
                    lines.append(indent_str + chunk)
                    remaining = remaining[max_width - indent:]
        return lines

    def wrap_text_to_width(self, text: str, max_width: int) -> List[str]:
        """
        Wrap text to fit within max_width, preserving all content.
        Returns a list of wrapped lines.
        """
        if not text:
            return ['']

        lines = []
        for raw_line in text.split('\n'):
            if len(raw_line) <= max_width:
                lines.append(raw_line)
            else:
                # Wrap this line
                words = raw_line.split(' ')
                current_line = []
                current_length = 0

                for word in words:
                    word_len = len(word)
                    # Handle very long words
                    if word_len > max_width:
                        # Flush current line if any
                        if current_line:
                            lines.append(' '.join(current_line))
                            current_line = []
                            current_length = 0
                        # Break long word into chunks
                        for i in range(0, word_len, max_width):
                            chunk = word[i:i+max_width]
                            lines.append(chunk)
                    else:
                        # Normal word - add space if not first word
                        space_needed = 1 if current_line else 0
                        if current_length + space_needed + word_len > max_width:
                            if current_line:
                                lines.append(' '.join(current_line))
                            current_line = [word]
                            current_length = word_len
                        else:
                            current_line.append(word)
                            current_length += space_needed + word_len

                if current_line:
                    lines.append(' '.join(current_line))

        return lines

    def display_with_rich(self, prompt: str, response: str, timestamp: str):
        """
        Display using rich library with direct console rendering (no Panels for content).
        This gives us full control over text display and prevents truncation issues.
        """
        console = self.console

        # Preserve formatting
        prompt_text = self.preserve_formatting(prompt)
        response_text = self.preserve_formatting(response)

        # Get terminal size
        term_size = self.get_terminal_size()
        width = term_size.columns
        # Reserve space for:
        # - Header Panel: 3 lines (top border, content, bottom border)
        # - Column headers: 3 lines (top border, header row, separator line)
        # - Footer border: 1 line
        # - Footer text: 1 line (compact, no panel - saves 2 lines vs Panel)
        # - Command prompt: 2 lines (newline + prompt)
        # Total reserved: 10 lines base + 2 safety margin = 12 lines
        height = term_size.lines - 12  # Reserve 12 lines total

        # Calculate split width - leave space for separator " │ " (3 chars)
        separator = " │ "
        separator_width = len(separator)
        left_width = (width - separator_width) // 2
        right_width = width - left_width - separator_width

        # Wrap text to exact content widths (no need to account for panel borders)
        prompt_lines = self.wrap_text_to_width(prompt_text, left_width)
        response_lines = self.wrap_text_to_width(response_text, right_width)

        # Scroll positions for each side (0-indexed)
        prompt_scroll = 0
        response_scroll = 0

        while True:
            # Clear screen
            console.clear()

            # Print header
            header_text = f"Entry {self.current_index + 1} of {len(self.entries)} | {timestamp}"
            console.print(Panel(header_text, border_style="bright_blue", title="AI Model Log Viewer"))

            # Calculate visible lines
            max_prompt_scroll = max(0, len(prompt_lines) - height) if len(prompt_lines) > height else 0
            max_response_scroll = max(0, len(response_lines) - height) if len(response_lines) > height else 0

            # Clamp scroll positions
            prompt_scroll = max(0, min(prompt_scroll, max_prompt_scroll))
            response_scroll = max(0, min(response_scroll, max_response_scroll))

            # Get visible lines
            end_prompt = min(prompt_scroll + height, len(prompt_lines))
            end_response = min(response_scroll + height, len(response_lines))

            visible_prompt = prompt_lines[prompt_scroll:end_prompt]
            visible_response = response_lines[response_scroll:end_response]

            # Pad to same height for side-by-side display
            max_visible = max(len(visible_prompt), len(visible_response))
            while len(visible_prompt) < max_visible:
                visible_prompt.append(' ' * left_width)
            while len(visible_response) < max_visible:
                visible_response.append(' ' * right_width)

            # Print column headers
            prompt_header = f"📝 PROMPT"
            if len(prompt_lines) > height or prompt_scroll > 0:
                last_visible = min(prompt_scroll + len(visible_prompt), len(prompt_lines))
                prompt_header += f" [Lines {prompt_scroll + 1}-{last_visible} of {len(prompt_lines)}]"

            response_header = f"🤖 RESPONSE"
            if len(response_lines) > height or response_scroll > 0:
                last_visible = min(response_scroll + len(visible_response), len(response_lines))
                response_header += f" [Lines {response_scroll + 1}-{last_visible} of {len(response_lines)}]"

            # Print headers with borders
            header_line = f"┌{'─' * (left_width)}┬{'─' * (right_width)}┐"
            console.print(header_line)
            header_content = f"│{prompt_header:<{left_width}}│{response_header:<{right_width}}│"
            console.print(header_content)
            console.print(f"├{'─' * (left_width)}┼{'─' * (right_width)}┤")

            # Print content lines side-by-side
            for i in range(max_visible):
                prompt_line = visible_prompt[i] if i < len(visible_prompt) else ' ' * left_width
                response_line = visible_response[i] if i < len(visible_response) else ' ' * right_width

                # Ensure exact width
                prompt_line = prompt_line[:left_width].ljust(left_width)
                response_line = response_line[:right_width].ljust(right_width)

                # Print with separator
                console.print(f"│{prompt_line}│{response_line}│")

            # Print footer border
            console.print(f"└{'─' * (left_width)}┴{'─' * (right_width)}┘")

            # Print compact footer with navigation (single line, no panel)
            footer_text = "[dim]Nav:[/dim] [n]ext [p]rev [g]oto [q]uit | [dim]Scroll:[/dim] [w/s] prompt [a/d] response [pu/pd] page [t]op [b]ottom [h]elp"
            console.print(footer_text)

            # Get user input
            try:
                command = input("\nCommand: ").strip().lower()
            except (KeyboardInterrupt, EOFError):
                return 'quit'

            # Handle scrolling commands
            if len(prompt_lines) > height:
                max_prompt_scroll = len(prompt_lines) - height
            else:
                max_prompt_scroll = 0
            if len(response_lines) > height:
                max_response_scroll = len(response_lines) - height
            else:
                max_response_scroll = 0

            if command == 'w' or command == 'up':
                if prompt_scroll > 0:
                    prompt_scroll -= 1
                continue
            elif command == 's' or command == 'down':
                # Allow scrolling until we've scrolled past the point where the last line is visible
                # We can scroll if prompt_scroll + height < len(prompt_lines)
                # This ensures we can always reach the point where the last line is visible
                if prompt_scroll + height < len(prompt_lines):
                    prompt_scroll += 1
                continue
            elif command == 'a' or command == 'left':
                if response_scroll > 0:
                    response_scroll -= 1
                continue
            elif command == 'd' or command == 'right':
                # Allow scrolling until we've scrolled past the point where the last line is visible
                if response_scroll + height < len(response_lines):
                    response_scroll += 1
                continue
            elif command == 'pu' or command == 'pageup':
                # Page up can work on prompt or response - try prompt first
                if prompt_scroll > 0:
                    prompt_scroll = max(0, prompt_scroll - height)
                elif response_scroll > 0:
                    response_scroll = max(0, response_scroll - height)
                continue
            elif command == 'pd' or command == 'pagedown':
                # Page down can work on prompt or response - try prompt first
                max_allowed_prompt = max(0, len(prompt_lines) - height)
                max_allowed_response = max(0, len(response_lines) - height)
                if prompt_scroll < max_allowed_prompt:
                    prompt_scroll = min(max_allowed_prompt, prompt_scroll + height)
                elif response_scroll < max_allowed_response:
                    response_scroll = min(max_allowed_response, response_scroll + height)
                continue
            elif command == 't' or command == 'top':
                prompt_scroll = 0
                response_scroll = 0
                continue
            elif command == 'b' or command == 'bottom':
                # Go to bottom - scroll to show the last line as the last visible line
                # We want to show the last 'height' lines, ending with the last line
                # If we show lines starting at prompt_scroll, we show: [prompt_scroll : prompt_scroll + height]
                # We want: prompt_scroll + height - 1 >= len(prompt_lines) - 1
                # Therefore: prompt_scroll >= len(prompt_lines) - height
                # Set to the minimum value that satisfies this: prompt_scroll = len(prompt_lines) - height
                if len(prompt_lines) > 0:
                    if len(prompt_lines) <= height:
                        # All lines fit, show from beginning
                        prompt_scroll = 0
                    else:
                        # Scroll so last line is the last visible
                        prompt_scroll = len(prompt_lines) - height
                        # Ensure it's valid
                        if prompt_scroll < 0:
                            prompt_scroll = 0
                else:
                    prompt_scroll = 0
                if len(response_lines) > 0:
                    if len(response_lines) <= height:
                        response_scroll = 0
                    else:
                        response_scroll = len(response_lines) - height
                        if response_scroll < 0:
                            response_scroll = 0
                else:
                    response_scroll = 0
                continue
            elif command == 'h' or command == 'help':
                self.show_help_rich()
                input("Press Enter to continue...")
                continue

            # Handle navigation commands
            if command == 'q' or command == 'quit':
                return 'quit'
            elif command == 'n' or command == 'next':
                if self.current_index < len(self.entries) - 1:
                    self.current_index += 1
                    return 'next'
                else:
                    console.print("[yellow]Already at the last entry.[/yellow]")
                    input("Press Enter to continue...")
            elif command == 'p' or command == 'prev':
                if self.current_index > 0:
                    self.current_index -= 1
                    return 'prev'
                else:
                    console.print("[yellow]Already at the first entry.[/yellow]")
                    input("Press Enter to continue...")
            elif command.startswith('g '):
                try:
                    new_index = int(command[2:]) - 1
                    if 0 <= new_index < len(self.entries):
                        self.current_index = new_index
                        return 'goto'
                    else:
                        console.print(f"[red]Invalid entry number. Valid range: 1-{len(self.entries)}[/red]")
                        input("Press Enter to continue...")
                except ValueError:
                    console.print("[red]Invalid number format. Use 'g <number>'[/red]")
                    input("Press Enter to continue...")
            elif command == 'g':
                console.print(f"[cyan]Enter entry number (1-{len(self.entries)}):[/cyan] ", end="")
                try:
                    new_index = int(input()) - 1
                    if 0 <= new_index < len(self.entries):
                        self.current_index = new_index
                        return 'goto'
                    else:
                        console.print(f"[red]Invalid entry number. Valid range: 1-{len(self.entries)}[/red]")
                        input("Press Enter to continue...")
                except ValueError:
                    console.print("[red]Invalid number format.[/red]")
                    input("Press Enter to continue...")

    def display_with_standard(self, prompt: str, response: str, timestamp: str):
        """Display using standard library (fallback)."""
        # Preserve formatting
        prompt_text = self.preserve_formatting(prompt)
        response_text = self.preserve_formatting(response)

        # Get terminal size
        term_size = self.get_terminal_size()
        width = term_size.columns
        height = term_size.lines - 5

        # Calculate split width
        left_width = (width - 3) // 2
        right_width = width - left_width - 3

        # Split by newlines first
        prompt_raw_lines = prompt_text.split('\n')
        response_raw_lines = response_text.split('\n')

        # Wrap long lines to fit the panel width
        prompt_lines = []
        for line in prompt_raw_lines:
            if len(line) > left_width - 1:
                # Wrap this line
                words = line.split(' ')
                current_line = []
                current_length = 0
                for word in words:
                    word_len = len(word) + 1  # +1 for space
                    if current_length + word_len > left_width - 1:
                        if current_line:
                            prompt_lines.append(' '.join(current_line))
                        current_line = [word]
                        current_length = len(word)
                    else:
                        current_line.append(word)
                        current_length += word_len
                if current_line:
                    prompt_lines.append(' '.join(current_line))
            else:
                prompt_lines.append(line)

        response_lines = []
        for line in response_raw_lines:
            if len(line) > right_width - 1:
                # Wrap this line
                words = line.split(' ')
                current_line = []
                current_length = 0
                for word in words:
                    word_len = len(word) + 1  # +1 for space
                    if current_length + word_len > right_width - 1:
                        if current_line:
                            response_lines.append(' '.join(current_line))
                        current_line = [word]
                        current_length = len(word)
                    else:
                        current_line.append(word)
                        current_length += word_len
                if current_line:
                    response_lines.append(' '.join(current_line))
            else:
                response_lines.append(line)

        # Scroll positions
        prompt_scroll = 0
        response_scroll = 0

        while True:
            os.system('cls' if os.name == 'nt' else 'clear')

            # Header
            print("=" * width)
            print(f"AI Model Log Viewer - Entry {self.current_index + 1} of {len(self.entries)}")
            print(f"Timestamp: {timestamp}")
            print("=" * width)

            # Calculate maximum scroll positions (ensure we can see the last line)
            max_prompt_scroll = max(0, len(prompt_lines) - height) if len(prompt_lines) > height else 0
            max_response_scroll = max(0, len(response_lines) - height) if len(response_lines) > height else 0

            # Clamp scroll positions to valid ranges before using them
            if prompt_scroll > max_prompt_scroll:
                prompt_scroll = max_prompt_scroll
            if prompt_scroll < 0:
                prompt_scroll = 0
            if response_scroll > max_response_scroll:
                response_scroll = max_response_scroll
            if response_scroll < 0:
                response_scroll = 0

            # Get visible lines (ensure we don't go past the end)
            # CRITICAL: If we're at the bottom, show ALL remaining lines (don't limit to height)
            # This ensures the last line is always fully visible
            if prompt_scroll + height >= len(prompt_lines):
                # At or near bottom - show all remaining lines to ensure last line is visible
                end_prompt = len(prompt_lines)
            else:
                end_prompt = prompt_scroll + height

            if response_scroll + height >= len(response_lines):
                end_response = len(response_lines)
            else:
                end_response = response_scroll + height

            visible_prompt_lines = prompt_lines[prompt_scroll:end_prompt]
            visible_response_lines = response_lines[response_scroll:end_response]

            # Pad to same height
            max_visible = max(len(visible_prompt_lines), len(visible_response_lines))
            visible_prompt_lines += [''] * (max_visible - len(visible_prompt_lines))
            visible_response_lines += [''] * (max_visible - len(visible_response_lines))

            # Display split screen
            print(f"{'PROMPT':<{left_width}} │ {'RESPONSE':<{right_width}}")
            print("-" * left_width + "-+-" + "-" * right_width)

            for i in range(max_visible):
                prompt_line = visible_prompt_lines[i][:left_width - 1] if visible_prompt_lines[i] else ""
                response_line = visible_response_lines[i][:right_width - 1] if visible_response_lines[i] else ""

                # Pad lines to fixed width
                prompt_padded = prompt_line.ljust(left_width - 1)
                response_padded = response_line.ljust(right_width - 1)

                print(f"{prompt_padded} │ {response_padded}")

            # Footer
            print("-" * width)
            prompt_info = f"Prompt: {prompt_scroll + 1}-{min(prompt_scroll + height, len(prompt_lines))}/{len(prompt_lines)}"
            response_info = f"Response: {response_scroll + 1}-{min(response_scroll + height, len(response_lines))}/{len(response_lines)}"
            print(f"{prompt_info:<{left_width}} │ {response_info:<{right_width}}")
            print("=" * width)
            print("Navigation: [n]ext, [p]rev, [g]oto, [q]uit")
            print("Scrolling: [w/s] prompt, [a/d] response, [pu/pd] page scroll, [t]op, [b]ottom, [h]elp")
            print("=" * width)

            # Get command
            try:
                command = input("\nCommand: ").strip().lower()
            except (KeyboardInterrupt, EOFError):
                return 'quit'

            if command == 'q' or command == 'quit':
                return 'quit'
            elif command == 'n' or command == 'next':
                if self.current_index < len(self.entries) - 1:
                    self.current_index += 1
                    return 'next'
            elif command == 'p' or command == 'prev':
                if self.current_index > 0:
                    self.current_index -= 1
                    return 'prev'
            elif command.startswith('g '):
                try:
                    new_index = int(command[2:]) - 1
                    if 0 <= new_index < len(self.entries):
                        self.current_index = new_index
                        return 'goto'
                except ValueError:
                    pass
            # Calculate max scroll positions for this iteration
            max_prompt_scroll = max(0, len(prompt_lines) - height) if len(prompt_lines) > height else 0
            max_response_scroll = max(0, len(response_lines) - height) if len(response_lines) > height else 0

            if command == 'w':
                if prompt_scroll > 0:
                    prompt_scroll -= 1
            elif command == 's':
                if prompt_scroll < max_prompt_scroll:
                    prompt_scroll += 1
            elif command == 'a':
                if response_scroll > 0:
                    response_scroll -= 1
            elif command == 'd':
                if response_scroll < max_response_scroll:
                    response_scroll += 1
            elif command == 't':
                prompt_scroll = 0
                response_scroll = 0
            elif command == 'b':
                prompt_scroll = max_prompt_scroll
                response_scroll = max_response_scroll
            elif command == 'h' or command == 'help':
                self.show_help_standard()
        input("Press Enter to continue...")

    def show_help_rich(self):
        """Show help using rich."""
        help_text = """
[bold]Navigation Commands:[/bold]
  [green]n[/green], [green]next[/green]     - Go to next entry
  [green]p[/green], [green]prev[/green]     - Go to previous entry
  [green]g[/green]                    - Go to specific entry (1-based)
  [green]q[/green], [green]quit[/green]     - Exit the viewer

[bold]Scrolling Commands:[/bold]
  [yellow]w[/yellow], [yellow]up[/yellow]           - Scroll prompt up
  [yellow]s[/yellow], [yellow]down[/yellow]         - Scroll prompt down
  [yellow]a[/yellow], [yellow]left[/yellow]         - Scroll response up
  [yellow]d[/yellow], [yellow]right[/yellow]        - Scroll response down
  [yellow]pu[/yellow], [yellow]pageup[/yellow]      - Scroll up one page (prompt first, then response)
  [yellow]pd[/yellow], [yellow]pagedown[/yellow]    - Scroll down one page (prompt first, then response)
  [yellow]t[/yellow], [yellow]top[/yellow]          - Go to top (both sides)
  [yellow]b[/yellow], [yellow]bottom[/yellow]       - Go to bottom (both sides)
  [yellow]h[/yellow], [yellow]help[/yellow]         - Show this help
        """
        self.console.print(Panel(help_text, title="Help", border_style="cyan"))

    def show_help_standard(self):
        """Show help using standard library."""
        print("\n" + "=" * 60)
        print("HELP - AI Model Log Viewer")
        print("=" * 60)
        print("Navigation Commands:")
        print("  n, next     - Go to next entry")
        print("  p, prev     - Go to previous entry")
        print("  g <number>  - Go to specific entry (1-based)")
        print("  q, quit     - Exit the viewer")
        print("\nScrolling Commands:")
        print("  w           - Scroll prompt up")
        print("  s           - Scroll prompt down")
        print("  a           - Scroll response up")
        print("  d           - Scroll response down")
        print("  t           - Go to top (both sides)")
        print("  b           - Go to bottom (both sides)")
        print("  h, help     - Show this help")
        print("=" * 60)

    def display_entry(self, index: int):
        """Display a specific log entry."""
        if not self.entries:
            print("No log entries found.")
            return 'quit'

        if index < 0 or index >= len(self.entries):
            print(f"Invalid index: {index}. Valid range: 0-{len(self.entries)-1}")
            return 'next'

        timestamp, prompt, response = self.entries[index]

        if RICH_AVAILABLE:
            return self.display_with_rich(prompt, response, timestamp)
        else:
            return self.display_with_standard(prompt, response, timestamp)

    def run(self):
        """Run the interactive viewer."""
        if not self.entries:
            print("No log entries to display.")
            return

        if RICH_AVAILABLE:
            print(f"Using rich library for enhanced display.")
        else:
            print(f"Using standard library display (install 'rich' for better UI: pip install rich)")

        self.current_index = 0

        while True:
            result = self.display_entry(self.current_index)
            if result == 'quit':
                print("Goodbye!")
                break


def main():
    parser = argparse.ArgumentParser(description="View AI model call logs with split-screen display")
    parser.add_argument("log_file", help="Path to the log file to view")
    parser.add_argument("--entry", "-e", type=int, help="Start at specific entry number (1-based)")

    args = parser.parse_args()

    viewer = LogViewer(args.log_file)

    if args.entry:
        if 1 <= args.entry <= len(viewer.entries):
            viewer.current_index = args.entry - 1
        else:
            print(f"Invalid entry number: {args.entry}. Valid range: 1-{len(viewer.entries)}")
            return

    viewer.run()


if __name__ == "__main__":
    main()
