"""Summarize repeated Linux reports; medians of per-round values, not pooled percentiles."""
import argparse
import csv
import json
from pathlib import Path
import statistics

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("directory", type=Path)
args = parser.parse_args()
with (args.directory / "index.json").open(encoding="utf-8") as source:
    index = json.load(source)
groups = {}
environments = {}
for run in index["runs"]:
    with (args.directory / run["file"]).open(encoding="utf-8") as source:
        report = json.load(source)
    environments[report["environment"]["machine_fingerprint"]] = report["environment"]
    for row in report["results"]:
        row["process_peak_rss_bytes"] = report.get("peak_rss_bytes")
        groups.setdefault((run["case"], row["workload"]), []).append(row)
fields = ["case", "workload", "rounds", "operations_total", "elapsed_seconds_total", "median_ops_s", "median_bytes_s", "median_p50_ms", "median_p95_ms", "median_p99_ms", "median_queue_p50_ms", "median_queue_p95_ms", "median_queue_p99_ms", "median_cpu_seconds", "median_cpu_percent", "median_process_peak_rss_bytes", "median_socket_sent", "median_file_write_bytes", "median_file_sync_ns", "median_directory_sync_ns", "median_temporary_peak", "median_additional_disk_peak", "median_startup_entries"]
summary = []
for (case, workload), rows in sorted(groups.items()):
    def median(field, counters=False):
        values = [(row["io_counters"] or {}).get(field) if counters else row.get(field) for row in rows]
        return statistics.median(values) if all(value is not None for value in values) else None
    summary.append(dict(zip(fields, [case, workload, len(rows), sum(row["operations"] for row in rows), sum(row["elapsed_seconds"] for row in rows),
        median("operations_per_second"), median("logical_bytes_per_second"), median("p50_ms"), median("p95_ms"), median("p99_ms"), median("queue_p50_ms"), median("queue_p95_ms"), median("queue_p99_ms"), median("cpu_seconds"), median("process_cpu_percent"), median("process_peak_rss_bytes"),
        median("socket_sent_bytes", True), median("file_write_bytes", True), median("file_sync_ns", True), median("directory_sync_ns", True),
        median("temporary_peak_bytes"), median("additional_disk_peak_bytes"), median("startup_entries", True)])))
with (args.directory / "summary.csv").open("w", newline="", encoding="utf-8") as output:
    writer = csv.DictWriter(output, fields); writer.writeheader(); writer.writerows(summary)
with (args.directory / "summary.json").open("w", encoding="utf-8") as output:
    json.dump({"method": "median of per-round metrics; percentiles are not pooled", "runner_class": index["runner_class"],
               "environments": environments, "results": summary}, output, indent=2)
print(f"{len(index['runs'])} runs, {len(summary)} repeated workload groups")
