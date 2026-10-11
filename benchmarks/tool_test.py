"""Negative schema/comparison regressions against a real successful smoke report."""
import copy
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

SOURCE = Path(sys.argv.pop(1))
TOOLS = Path(__file__).resolve().parent
with SOURCE.open(encoding="utf-8") as source:
    REPORT = json.load(source)


class BenchmarkTools(unittest.TestCase):
    def check(self, report, tool="validate.py", extra=(), expected=1):
        with tempfile.TemporaryDirectory(prefix="atlasfs-tool-", dir=SOURCE.resolve().parent) as directory:
            path = Path(directory) / "report.json"
            with path.open("w", encoding="utf-8") as output:
                json.dump(report, output)
            result = subprocess.run([sys.executable, str(TOOLS / tool), *extra, str(path)], capture_output=True)
            if expected == 0:
                self.assertEqual(result.returncode, 0, result.stderr.decode())
            else:
                self.assertNotEqual(result.returncode, 0)

    def test_valid_schema_and_arithmetic(self):
        self.check(REPORT, expected=0)

    def test_framing_counter_corruption_is_rejected(self):
        value = copy.deepcopy(REPORT)
        value["results"][0]["io_counters"]["framed_sent_bytes"] += 1
        self.check(value)

    def test_boolean_is_not_a_byte_count(self):
        value = copy.deepcopy(REPORT)
        value["results"][0]["io_counters"]["socket_sent_bytes"] = True
        self.check(value)

    def test_incompatible_workload_comparison_is_rejected(self):
        value = copy.deepcopy(REPORT)
        value["seed"] += 1
        self.check(value, "compare.py", (str(SOURCE.resolve()),))

    def test_unavailable_accounting_requires_null_peaks(self):
        value = copy.deepcopy(REPORT)
        value["results"][0]["io_counters"]["accounting_complete"] = False
        self.check(value)
        value["results"][0]["temporary_peak_bytes"] = None
        value["results"][0]["additional_disk_peak_bytes"] = None
        self.check(value, expected=0)

    def test_transport_change_requires_explicit_comparison(self):
        value = copy.deepcopy(REPORT)
        value["tcp_nodelay"] = not REPORT.get("tcp_nodelay", False)
        self.check(value, "compare.py", (str(SOURCE.resolve()),))
        self.check(value, "compare.py", ("--allow-transport-change", str(SOURCE.resolve())), expected=0)

    def test_admission_change_requires_explicit_comparison(self):
        value = copy.deepcopy(REPORT)
        value["admission_policy"] = "mutex" if REPORT.get("admission_policy", "mutex") == "fifo" else "fifo"
        self.check(value, "compare.py", (str(SOURCE.resolve()),))
        self.check(value, "compare.py", ("--allow-admission-change", str(SOURCE.resolve())), expected=0)

    def test_dispatch_change_requires_explicit_comparison(self):
        value = copy.deepcopy(REPORT)
        value["dispatch_policy"] = "serial" if REPORT.get("dispatch_policy", "serial") == "bounded" else "bounded"
        self.check(value, "compare.py", (str(SOURCE.resolve()),))
        self.check(value, "compare.py", ("--allow-dispatch-change", str(SOURCE.resolve())), expected=0)

    def test_explicit_policy_change_does_not_hide_capacity_change(self):
        value = copy.deepcopy(REPORT)
        value["admission_capacity"] = REPORT.get("admission_capacity", 16) + 1
        self.check(value, "compare.py", ("--allow-admission-change", str(SOURCE.resolve())))

    @unittest.skipUnless(REPORT.get("scheduling_schema") == 1, "pre-M22 report")
    def test_admission_count_corruption_is_rejected(self):
        value = copy.deepcopy(REPORT)
        value["results"][0]["admission_overloads"] += 1
        self.check(value)

    @unittest.skipUnless(REPORT.get("scheduling_schema") == 1, "pre-M22 report")
    def test_queue_capacity_violation_is_rejected(self):
        value = copy.deepcopy(REPORT)
        row = next(row for row in value["results"] if row["row_admission_policy"] == "fifo")
        row["admission_queue_peak"] = row["admission_queue_capacity"] + 1
        self.check(value)

    @unittest.skipUnless(REPORT.get("scheduling_schema") == 1, "pre-M22 report")
    def test_scheduling_metadata_is_required_when_marked(self):
        value = copy.deepcopy(REPORT)
        del value["admission_timeout_ms"]
        self.check(value)

    def test_machine_fingerprint_change_is_rejected(self):
        value = copy.deepcopy(REPORT)
        value.setdefault("environment", {})["machine_fingerprint"] = "different-machine"
        self.check(value, "compare.py", (str(SOURCE.resolve()),))

    @unittest.skipUnless("queue_max_ms" in REPORT["results"][0], "pre-maximum-wait report")
    def test_maximum_wait_below_percentile_is_rejected(self):
        value = copy.deepcopy(REPORT)
        row = next(row for row in value["results"] if row["queue_samples"])
        row["queue_max_ms"] = -1
        self.check(value)
        row["queue_max_ms"] = 0
        self.check(value)


if __name__ == "__main__":
    unittest.main()
