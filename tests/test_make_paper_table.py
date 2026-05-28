"""Tests for the LaTeX paper-table generator."""
import csv
import pathlib
import sys

REPO = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "tools"))

import make_paper_table as mpt  # noqa: E402

TEMPLATE = (
    "\\begin{table*}\n\\caption{My caption}\n\\begin{tabular}{lrrrrrrrr}\n"
    "\\toprule\nBroker & Robots \\\\\n\\midrule\n%%ROWS%%\n\\bottomrule\n"
    "\\end{tabular}\n\\end{table*}\n"
)


def _row(broker, n, received, expected, delivery, avg, p50, p95, p99):
    return {
        "run_name": f"run_{broker}_{n}", "broker": broker, "n_robots": str(n),
        "level": "run", "suffix": "",
        "received": str(received), "expected": str(expected),
        "matched": str(received), "delivery_pct": str(delivery),
        "avg_ms": str(avg), "p50_ms": str(p50),
        "p95_ms": str(p95), "p99_ms": str(p99),
    }


def test_latex_int_thousands_separator():
    assert mpt._latex_int(5538) == "5\\,538"
    assert mpt._latex_int(99) == "99"
    assert mpt._latex_int("138750") == "138\\,750"


def test_read_run_rows_filters_suffix(tmp_path):
    csv_path = tmp_path / "c.csv"
    with open(csv_path, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=["broker", "n_robots", "level"])
        w.writeheader()
        w.writerow({"broker": "Kafka", "n_robots": "1", "level": "run"})
        w.writerow({"broker": "Kafka", "n_robots": "1", "level": "suffix"})
    rows = mpt.read_run_rows(str(csv_path))
    assert len(rows) == 1
    assert rows[0]["level"] == "run"


def test_render_groups_kafka_then_mqtt_sorted():
    rows = [
        _row("MQTT", 1, 5557, 5550, 100.0, 0.96, 0.47, 1.77, 12.57),
        _row("Kafka", 5, 27748, 27750, 100.0, 1.58, 0.33, 0.81, 1.76),
        _row("Kafka", 1, 5538, 5550, 99.8, 0.94, 0.76, 1.80, 2.53),
    ]
    out = mpt.render_table(rows, TEMPLATE)
    # Caption + booktabs preserved
    assert "\\caption{My caption}" in out
    assert "\\toprule" in out and "\\bottomrule" in out
    assert "%%ROWS%%" not in out
    # Order: Kafka 1, Kafka 5, midrule, MQTT 1
    k1 = out.index("Kafka & 1 ")
    k5 = out.index("Kafka & 5 ")
    m1 = out.index("MQTT & 1 ")
    assert k1 < k5 < m1
    # An inner midrule separates the broker groups (in addition to header one)
    assert out.count("\\midrule") == 2
    # Thousands separator applied
    assert "5\\,538" in out and "27\\,748" in out


def test_render_fallback_without_sentinel():
    template_no_sentinel = (
        "\\begin{tabular}{lr}\n\\toprule\nA & B \\\\\n\\midrule\n"
        "Kafka & 1 & 1 & 1 & 1.0 & 1.0 & 1.0 & 1.0 & 1.0 \\\\\n\\bottomrule\n"
        "\\end{tabular}\n"
    )
    rows = [_row("Kafka", 1, 5538, 5550, 99.8, 0.94, 0.76, 1.80, 2.53)]
    out = mpt.render_table(rows, template_no_sentinel)
    assert "5\\,538" in out
    assert "\\bottomrule" in out


def test_end_to_end_csv_to_table(tmp_path):
    csv_path = tmp_path / "combined.csv"
    fieldnames = list(_row("Kafka", 1, 1, 1, 1, 1, 1, 1, 1).keys())
    with open(csv_path, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fieldnames)
        w.writeheader()
        w.writerow(_row("Kafka", 10, 55374, 55500, 99.8, 0.32, 0.23, 0.47, 0.87))
        w.writerow(_row("MQTT", 10, 55562, 55500, 100.0, 0.45, 0.20, 0.63, 2.54))

    rows = mpt.read_run_rows(str(csv_path))
    out = mpt.render_table(rows, TEMPLATE)
    assert "Kafka & 10 & 55\\,374 & 55\\,500 & 99.8 & 0.32 & 0.23 & 0.47 & 0.87 \\\\" in out
    assert "MQTT & 10 & 55\\,562 & 55\\,500 & 100.0 & 0.45 & 0.20 & 0.63 & 2.54 \\\\" in out
