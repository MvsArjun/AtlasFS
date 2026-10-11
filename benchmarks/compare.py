"""Compare compatible benchmark JSON runs; standard library only, no timing gate."""
import argparse
import json

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("before")
parser.add_argument("after")
args = parser.parse_args()
with open(args.before, encoding="utf-8") as source:
    before = json.load(source)
with open(args.after, encoding="utf-8") as source:
    after = json.load(source)
for field in ("schema", "seed", "iterations", "objects", "bytes", "chunk", "nodes",
              "replicas", "workers", "suite", "compiler", "openssl", "build_type", "os", "label"):
    if before.get(field) != after.get(field):
        parser.error(f"incompatible configuration: {field}")
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
