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


if __name__ == "__main__":
    unittest.main()
