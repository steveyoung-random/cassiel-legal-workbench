# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License
import argparse
import datetime as dt
import re
import shutil
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path


BASE_URL = "https://www.ecfr.gov/api/versioner/v1/full/{date}/title-{title}.xml"


def parse_title_list(value: str) -> list[int]:
    if not value:
        return []
    titles = []
    for part in value.split(","):
        part = part.strip()
        if not part:
            continue
        if not part.isdigit():
            raise argparse.ArgumentTypeError(
                f"Invalid title '{part}'. Titles must be integers."
            )
        titles.append(int(part))
    return titles


def parse_date(value: str) -> dt.date:
    try:
        return dt.date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "Invalid date. Use YYYY-MM-DD."
        ) from exc


def sanitize_segment(segment: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", segment)


def build_url(date: str, title: int, filters: dict[str, str]) -> str:
    url = BASE_URL.format(date=date, title=title)
    if filters:
        url = f"{url}?{urllib.parse.urlencode(filters)}"
    return url


def build_filename(date: str, title: int, filters: dict[str, str]) -> str:
    parts = [f"title-{title}", f"date-{date}"]
    for key, value in filters.items():
        parts.append(f"{key}-{value}")
    parts = [sanitize_segment(part) for part in parts]
    return "__".join(parts) + ".xml"


def download_file(url: str, dest_path: Path, timeout_seconds: int = 60) -> bool:
    try:
        with urllib.request.urlopen(url, timeout=timeout_seconds) as response:
            dest_path.parent.mkdir(parents=True, exist_ok=True)
            with dest_path.open("wb") as file_obj:
                shutil.copyfileobj(response, file_obj)
        return True
    except urllib.error.HTTPError as exc:
        print(f"HTTP error {exc.code} for {url}", file=sys.stderr)
    except urllib.error.URLError as exc:
        print(f"Network error for {url}: {exc.reason}", file=sys.stderr)
    except OSError as exc:
        print(f"File error for {dest_path}: {exc}", file=sys.stderr)
    return False


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Download CFR XML via the eCFR versioner full API."
    )
    parser.add_argument(
        "--title",
        type=parse_title_list,
        help="CFR title number(s), comma-separated (default: 1-50).",
    )
    parser.add_argument("--subtitle", help="Optional subtitle filter.")
    parser.add_argument("--chapter", help="Optional chapter filter.")
    parser.add_argument("--subchapter", help="Optional subchapter filter.")
    parser.add_argument("--part", help="Optional part filter.")
    parser.add_argument("--subpart", help="Optional subpart filter.")
    parser.add_argument(
        "--date",
        type=parse_date,
        help="Target date in YYYY-MM-DD (default: 7 days ago).",
    )
    parser.add_argument(
        "--output-dir",
        help="Output directory (default: current user's Downloads folder).",
    )

    args = parser.parse_args()

    target_date = args.date or (dt.date.today() - dt.timedelta(days=7))
    date_str = target_date.isoformat()

    titles = args.title or list(range(1, 51))

    filters = {
        key: value
        for key, value in {
            "subtitle": args.subtitle,
            "chapter": args.chapter,
            "subchapter": args.subchapter,
            "part": args.part,
            "subpart": args.subpart,
        }.items()
        if value
    }

    if args.output_dir:
        output_dir = Path(args.output_dir).expanduser()
    else:
        output_dir = Path.home() / "Downloads"

    output_dir.mkdir(parents=True, exist_ok=True)

    failures = 0
    for title in titles:
        url = build_url(date_str, title, filters)
        filename = build_filename(date_str, title, filters)
        dest_path = output_dir / filename
        print(f"Downloading Title {title} -> {dest_path}")
        if not download_file(url, dest_path):
            failures += 1

    if failures:
        print(f"Completed with {failures} failure(s).", file=sys.stderr)
        return 1
    print("All downloads complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
