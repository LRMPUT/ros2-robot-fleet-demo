#!/usr/bin/env python3
"""Render a LaTeX paper table from analyzer CSV output.

Reads the combined CSV produced by `analyze_latency.py --csv ... --append`
(one `level="run"` row per capture) and fills the LaTeX template
`table_templete.txt`, writing `table_results.txt`.

Rows are grouped Kafka-first then MQTT, each sorted by robot count. The
template's caption, column header, and booktabs rules are preserved; only the
tabular body (the `%%ROWS%%` sentinel) is replaced.

Usage:
    python3 tools/make_paper_table.py --csv combined.csv
    python3 tools/make_paper_table.py --csv combined.csv \
        --template table_templete.txt --out table_results.txt
"""
from __future__ import annotations

import argparse
import csv
import re
import sys

ROW_SENTINEL = "%%ROWS%%"
BROKER_ORDER = {"Kafka": 0, "MQTT": 1}


def read_run_rows(csv_path: str) -> list[dict]:
    """Return only the run-level rows from the analyzer CSV."""
    with open(csv_path, newline="") as fh:
        return [r for r in csv.DictReader(fh) if r.get("level") == "run"]


def sort_rows(rows: list[dict]) -> list[dict]:
    """Group by broker (Kafka first, then MQTT), each sorted by robot count."""
    return sorted(
        rows,
        key=lambda r: (BROKER_ORDER.get(r["broker"], 99), int(r["n_robots"])),
    )


def _latex_int(value) -> str:
    """Format an integer with a LaTeX thin-space thousands separator."""
    n = int(round(float(value)))
    s = f"{n:,}"  # e.g. "5,538"
    return s.replace(",", "\\,")


def _fmt_row(r: dict) -> str:
    received = _latex_int(r["received"])
    expected = _latex_int(r["expected"]) if r.get("expected") not in ("", None) else "--"
    delivery = f"{float(r['delivery_pct']):.1f}" if r.get("delivery_pct") not in ("", None) else "--"
    avg = f"{float(r['avg_ms']):.2f}"
    p50 = f"{float(r['p50_ms']):.2f}"
    p95 = f"{float(r['p95_ms']):.2f}"
    p99 = f"{float(r['p99_ms']):.2f}"
    return (f"{r['broker']} & {r['n_robots']} & {received} & {expected} & "
            f"{delivery} & {avg} & {p50} & {p95} & {p99} \\\\")


def render_table(rows: list[dict], template_text: str) -> str:
    """Inject generated rows into the template at the %%ROWS%% sentinel.

    An inner \\midrule separates the Kafka and MQTT groups. Falls back to
    replacing the body between the first \\midrule and \\bottomrule if the
    sentinel is absent.
    """
    rows = sort_rows(rows)

    body_lines: list[str] = []
    prev_broker = None
    for r in rows:
        if prev_broker is not None and r["broker"] != prev_broker:
            body_lines.append("\\midrule")
        body_lines.append(_fmt_row(r))
        prev_broker = r["broker"]
    body = "\n".join(body_lines)

    if ROW_SENTINEL in template_text:
        return template_text.replace(ROW_SENTINEL, body)

    # Fallback: replace everything between the first \midrule and \bottomrule.
    pattern = re.compile(r"(\\midrule\s*\n).*?(\n\s*\\bottomrule)", re.DOTALL)
    if not pattern.search(template_text):
        print("ERROR: template has no %%ROWS%% sentinel and no \\midrule…\\bottomrule "
              "block to replace.", file=sys.stderr)
        sys.exit(1)
    return pattern.sub(lambda m: m.group(1) + body + m.group(2), template_text)


def main() -> None:
    parser = argparse.ArgumentParser(description="Render LaTeX paper table from analyzer CSV.")
    parser.add_argument("--csv", required=True, help="Combined analyzer CSV (run rows)")
    parser.add_argument("--template", default="table_templete.txt",
                        help="LaTeX template (default: table_templete.txt)")
    parser.add_argument("--out", default="table_results.txt",
                        help="Output LaTeX file (default: table_results.txt)")
    args = parser.parse_args()

    rows = read_run_rows(args.csv)
    if not rows:
        print(f"ERROR: no level=run rows found in {args.csv}", file=sys.stderr)
        sys.exit(1)

    with open(args.template) as fh:
        template_text = fh.read()

    result = render_table(rows, template_text)
    with open(args.out, "w") as fh:
        fh.write(result)

    print(f"Wrote {args.out}  ({len(rows)} run rows)")


if __name__ == "__main__":
    main()
