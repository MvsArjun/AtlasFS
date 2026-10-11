"""Compare compatible benchmark JSON runs; standard library only, no timing gate."""
import argparse
import json

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("before")
parser.add_argument("after")
parser.add_argument("--allow-instrumentation-change", action="store_true", help="Explicit overhead comparison; never an implementation speedup claim")
parser.add_argument("--allow-transport-change", action="store_true", help="Explicit TCP_NODELAY A/B comparison")
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
print("workload,before_ops_s,after_ops_s,ops_change_pct,before_p50_ms,after_p50_ms")
for name, row in old.items():
    replacement = new[name]
    if row["unexpected_errors"] or replacement["unexpected_errors"]:
        parser.error(f"cannot compare failed workload: {name}")
    if row["operations"] != replacement["operations"] or row["logical_bytes"] != replacement["logical_bytes"]:
        parser.error(f"different work volume: {name}")
    a, b = row["operations_per_second"], replacement["operations_per_second"]
    print(f'{name},{a:.4f},{b:.4f},{100 * (b / a - 1):.2f},{row["p50_ms"]:.4f},{replacement["p50_ms"]:.4f}')
