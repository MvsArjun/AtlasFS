"""Validate benchmark result structure and arithmetic; never gate wall-clock speed."""
import argparse
import json
import math
from pathlib import Path

if not __debug__:
    raise RuntimeError("benchmark validation requires Python assertions enabled (no -O)")


def schema_check(value, rule, location):
    """Evaluate the JSON Schema keywords used by schema-v2.json, without a dependency."""
    kinds = {"null": value is None, "boolean": isinstance(value, bool), "integer": type(value) is int,
             "number": type(value) in (int, float) and math.isfinite(value), "string": isinstance(value, str),
             "object": isinstance(value, dict), "array": isinstance(value, list)}
    if "type" in rule:
        allowed = rule["type"] if isinstance(rule["type"], list) else [rule["type"]]
        assert any(kinds[kind] for kind in allowed), (location, "schema type")
    if "enum" in rule:
        assert value in rule["enum"], (location, "schema enum")
    if type(value) in (int, float) and "minimum" in rule:
        assert value >= rule["minimum"], (location, "schema minimum")
    if isinstance(value, dict):
        assert all(key in value for key in rule.get("required", [])), (location, "schema required")
        properties = rule.get("properties", {})
        for key, child in value.items():
            if key in properties:
                schema_check(child, properties[key], location + "." + key)
            elif isinstance(rule.get("additionalProperties"), dict):
                schema_check(child, rule["additionalProperties"], location + "." + key)
    if isinstance(value, list):
        assert len(value) >= rule.get("minItems", 0), (location, "schema array count")
        for index, child in enumerate(value):
            schema_check(child, rule.get("items", {}), f"{location}[{index}]")


with Path(__file__).with_name("schema-v2.json").open(encoding="utf-8") as schema_source:
    schema = json.load(schema_source)

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("files", nargs="+")
args = parser.parse_args()
for path in args.files:
    with open(path, encoding="utf-8") as source:
        data = json.load(source)
    assert data["schema"] in (1, 2) and data["results"], path
    if data["schema"] == 2:
        schema_check(data, schema, path)
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
        if data["schema"] == 2:
            assert row["successes"] + row["unexpected_errors"] == row["operations"], (path, name, "outcome count")
            assert row["warmup_operations"] == data["warmup_per_workload"], (path, name, "warmup")
            counters = row["io_counters"]
            if data["instrumentation"]:
                assert counters and not counters["saturated"], (path, name, "counters")
                assert counters["accounting_complete"], (path, name, "accounting bound reached")
                assert counters["framed_sent_bytes"] == counters["serialized_payload_sent_bytes"] + 24 * counters["frames_sent"], (path, name, "framing")
                assert row["temporary_peak_bytes"] >= row["temporary_residual_bytes"], (path, name, "temporary peak")
                assert row["additional_disk_peak_bytes"] >= max(0, row["disk_file_bytes"] - row["disk_baseline_bytes"]), (path, name, "disk peak")
                assert all(isinstance(value, (int, bool)) and value >= 0 for value in counters.values()), (path, name, "counter type")
            else:
                assert counters is None and row["temporary_peak_bytes"] is None and row["additional_disk_peak_bytes"] is None, (path, name, "disabled")
            for prefix in ("queue", "recovery"):
                samples = row[prefix + "_samples"]
                values = [row[f"{prefix}_p{p}_ms"] for p in (50, 95, 99)]
                assert (samples == 0 and values == [None] * 3) or (0 < samples <= row["operations"] and 0 <= values[0] <= values[1] <= values[2]), (path, name, prefix)
    print(f'{path}: {len(names)} workloads, {sum(r["operations"] for r in data["results"])} operations, valid')
