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
    scheduling = data.get("scheduling_schema") == 1
    if scheduling:
        required = ("admission_policy", "admission_capacity", "admission_timeout_ms", "dispatch_policy", "dispatch_workers",
                    "dispatch_capacity", "connection_request_limit", "dispatch_probe_server_timeout_ms", "dispatch_probe_client_timeout_ms")
        assert all(field in data for field in required), (path, "scheduling metadata")
        assert data["admission_capacity"] <= 65536 and data["admission_timeout_ms"] <= 60000, (path, "admission bounds")
        assert data["dispatch_workers"] <= 16 and data["dispatch_capacity"] <= 65536 and data["connection_request_limit"] <= 1000000, (path, "dispatch bounds")
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
                assert counters["framed_sent_bytes"] == counters["serialized_payload_sent_bytes"] + 24 * counters["frames_sent"], (path, name, "framing")
                if counters["accounting_complete"]:
                    assert row["temporary_peak_bytes"] >= row["temporary_residual_bytes"], (path, name, "temporary peak")
                    assert row["additional_disk_peak_bytes"] >= max(0, row["disk_file_bytes"] - row["disk_baseline_bytes"]), (path, name, "disk peak")
                else:
                    assert row["temporary_peak_bytes"] is None and row["additional_disk_peak_bytes"] is None, (path, name, "unavailable accounting must have null peaks")
                assert all(isinstance(value, (int, bool)) and value >= 0 for value in counters.values()), (path, name, "counter type")
            else:
                assert counters is None and row["temporary_peak_bytes"] is None and row["additional_disk_peak_bytes"] is None, (path, name, "disabled")
            for prefix in ("queue", "recovery"):
                samples = row[prefix + "_samples"]
                values = [row[f"{prefix}_p{p}_ms"] for p in (50, 95, 99)]
                assert (samples == 0 and values == [None] * 3) or (0 < samples <= row["operations"] and 0 <= values[0] <= values[1] <= values[2]), (path, name, prefix)
        if scheduling:
            required = ("executed_operations", "successful_operations", "successful_operations_per_second", "admission_attempts",
                        "admitted_operations", "admission_overloads", "admission_cancelled", "admission_deadline_expired",
                        "admission_shutdown_rejected", "admission_overload_rate", "row_admission_policy", "admission_queue_capacity",
                        "admission_queue_peak", "independent_connection_samples", "independent_connection_p50_ms",
                        "independent_connection_p95_ms", "independent_connection_p99_ms", "dispatch_snapshot")
            assert all(field in row for field in required), (path, name, "scheduling result fields")
            rejected = sum(row[field] for field in ("admission_overloads", "admission_cancelled", "admission_deadline_expired", "admission_shutdown_rejected"))
            assert row["admission_attempts"] == row["admitted_operations"] + rejected, (path, name, "admission partition")
            assert 0 <= row["admitted_operations"] <= row["executed_operations"] == row["operations"] - rejected, (path, name, "execution count")
            assert 0 <= row["successful_operations"] <= row["executed_operations"], (path, name, "successful count")
            assert row["expected_rejections"] >= rejected and row["timeouts"] >= row["admission_deadline_expired"], (path, name, "rejection counts")
            assert row["queue_samples"] == row["admission_attempts"], (path, name, "admission latency samples")
            assert math.isclose(row["successful_operations_per_second"], row["successful_operations"] / row["elapsed_seconds"], rel_tol=1e-8, abs_tol=1e-8), (path, name, "successful rate")
            assert math.isclose(row["admission_overload_rate"], row["admission_overloads"] / row["operations"], rel_tol=1e-8, abs_tol=1e-8), (path, name, "overload rate")
            if row["row_admission_policy"] == "fifo":
                assert row["admission_attempts"] and 0 <= row["admission_queue_peak"] <= row["admission_queue_capacity"] <= 65536, (path, name, "queue bound")
            else:
                assert row["admission_queue_capacity"] is None and row["admission_queue_peak"] is None, (path, name, "reference queue accounting")
                assert row["row_admission_policy"] != "none" or row["admission_attempts"] == 0, (path, name, "no admission")
            samples = row["independent_connection_samples"]
            values = [row[f"independent_connection_p{p}_ms"] for p in (50, 95, 99)]
            assert (samples == 0 and values == [None] * 3) or (0 < samples <= row["operations"] and 0 <= values[0] <= values[1] <= values[2]), (path, name, "independent connection")
            dispatch = row["dispatch_snapshot"]
            if dispatch is not None:
                limit = 1 if data["dispatch_policy"] == "serial" else data["dispatch_workers"]
                assert dispatch["active"] == 0 and dispatch["peak_active"] <= limit, (path, name, "connection bound")
                assert dispatch["request_waiting"] == 0 and dispatch["peak_request_waiting"] <= data["dispatch_capacity"], (path, name, "request queue bound")
    print(f'{path}: {len(names)} workloads, {sum(r["operations"] for r in data["results"])} operations, valid')
