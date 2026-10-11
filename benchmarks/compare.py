"""Compare compatible benchmark JSON runs; standard library only, no timing gate."""
import argparse
import csv
import json
import sys

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("before")
parser.add_argument("after")
parser.add_argument("--allow-instrumentation-change", action="store_true", help="Explicit overhead comparison; never an implementation speedup claim")
parser.add_argument("--allow-transport-change", action="store_true", help="Explicit TCP_NODELAY A/B comparison")
parser.add_argument("--allow-admission-change", action="store_true", help="Explicit mutex/FIFO A/B comparison; limits and deadlines must still match")
parser.add_argument("--allow-dispatch-change", action="store_true", help="Explicit serial/bounded dispatch A/B comparison; resource limits must still match")
args = parser.parse_args()
with open(args.before, encoding="utf-8") as source:
    before = json.load(source)
with open(args.after, encoding="utf-8") as source:
    after = json.load(source)
for field in ("schema", "seed", "iterations", "objects", "bytes", "chunk", "nodes",
              "replicas", "workers", "suite", "compiler", "openssl", "build_type", "os", "label"):
    if before.get(field) != after.get(field):
        parser.error(f"incompatible configuration: {field}")
if before["schema"] == 2:
    if before.get("tcp_nodelay", False) != after.get("tcp_nodelay", False) and not args.allow_transport_change:
        parser.error("TCP_NODELAY differs: request an explicit transport comparison")
    for field in ("warmup_per_workload", "compile_flags"):
        if before.get(field) != after.get(field):
            parser.error(f"incompatible configuration: {field}")
    if before["instrumentation"] != after["instrumentation"] and not args.allow_instrumentation_change:
        parser.error("instrumentation differs: request an explicit overhead comparison")
    # Published pre-M22 reports used a mutex and serial full-connection server.
    # New limit defaults merely label unused reference-mode settings for legacy
    # reports; they do not claim that the old mutex bounded its waiting callers.
    for field, legacy, allowed in (("admission_policy", "mutex", args.allow_admission_change),
                                   ("dispatch_policy", "serial", args.allow_dispatch_change)):
        if before.get(field, legacy) != after.get(field, legacy) and not allowed:
            parser.error(f"{field} differs: request an explicit scheduling comparison")
    for field, legacy in (("admission_capacity", 16), ("admission_timeout_ms", 5000),
                          ("dispatch_workers", 4), ("dispatch_capacity", 4), ("connection_request_limit", 1000000),
                          ("dispatch_probe_server_timeout_ms", 200), ("dispatch_probe_client_timeout_ms", 1000)):
        if before.get(field, legacy) != after.get(field, legacy):
            parser.error(f"incompatible resource or deadline configuration: {field}")
    if before.get("environment") or after.get("environment"):
        for field in ("machine_fingerprint", "runner_class"):
            if before.get("environment", {}).get(field) != after.get("environment", {}).get(field):
                parser.error(f"incompatible environment: {field}")
old = {row["workload"]: row for row in before["results"]}
new = {row["workload"]: row for row in after["results"]}
if len(old) != len(before["results"]) or len(new) != len(after["results"]):
    parser.error("duplicate workload names")
if old.keys() != new.keys():
    parser.error("workload sets differ")
writer = csv.writer(sys.stdout, lineterminator="\n")
extra_fields = ("p95_ms", "p99_ms", "latency_max_ms", "queue_p50_ms", "queue_p95_ms", "queue_p99_ms", "queue_max_ms",
                "successful_operations_per_second", "admission_overloads", "admission_deadline_expired",
                "admission_cancelled", "admission_shutdown_rejected", "timeouts", "unexpected_errors",
                "independent_connection_p50_ms", "independent_connection_p95_ms", "independent_connection_p99_ms")
writer.writerow(["workload", "before_ops_s", "after_ops_s", "ops_change_pct", "before_p50_ms", "after_p50_ms",
                 *[side + "_" + field for field in extra_fields for side in ("before", "after")]])
for name, row in old.items():
    replacement = new[name]
    if row["unexpected_errors"] or replacement["unexpected_errors"]:
        parser.error(f"cannot compare failed workload: {name}")
    if row["operations"] != replacement["operations"] or row["logical_bytes"] != replacement["logical_bytes"]:
        parser.error(f"different work volume: {name}")
    a, b = row["operations_per_second"], replacement["operations_per_second"]
    writer.writerow([name, f"{a:.4f}", f"{b:.4f}", f"{100 * (b / a - 1):.2f}", f'{row["p50_ms"]:.4f}', f'{replacement["p50_ms"]:.4f}',
                     *[report.get(field) for field in extra_fields for report in (row, replacement)]])
