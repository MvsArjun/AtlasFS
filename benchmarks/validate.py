"""Validate benchmark result structure and arithmetic; never gate wall-clock speed."""
import argparse
import json
import math

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("files", nargs="+")
args = parser.parse_args()
for path in args.files:
    with open(path, encoding="utf-8") as source:
        data = json.load(source)
    assert data["schema"] == 1 and data["results"], path
    names = set()
    for row in data["results"]:
        name = row["workload"]
        assert name not in names, (path, name, "duplicate")
        names.add(name)
        assert row["operations"] > 0 and row["unexpected_errors"] == 0, (path, name, "failure")
        assert 0 <= row["p50_ms"] <= row["p95_ms"] <= row["p99_ms"], (path, name, "percentiles")
        assert row["elapsed_seconds"] > 0 and row["temporary_residual_bytes"] == 0, (path, name, "residual temporary bytes")
        for field in ("operations_per_second", "logical_bytes_per_second", "p50_ms", "p95_ms", "p99_ms"):
            assert math.isfinite(row[field]), (path, name, field)
        assert math.isclose(row["operations_per_second"], row["operations"] / row["elapsed_seconds"], rel_tol=1e-8), (path, name, "operations rate")
        assert math.isclose(row["logical_bytes_per_second"], row["logical_bytes"] / row["elapsed_seconds"], rel_tol=1e-8, abs_tol=1e-8), (path, name, "bytes rate")
    print(f'{path}: {len(names)} workloads, {sum(r["operations"] for r in data["results"])} operations, valid')
