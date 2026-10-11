"""Reproducible Linux rounds with explicit runner classification; no timing gates."""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import tempfile
import uuid

ROOT = Path(__file__).resolve().parents[1]


def command(argv, cwd=None):
    try:
        result = subprocess.run(argv, cwd=cwd, text=True, capture_output=True, timeout=15)
        return result.stdout.strip() if result.returncode == 0 else None
    except (OSError, subprocess.TimeoutExpired):
        return None


def read(path):
    try:
        return Path(path).read_text(encoding="utf-8").strip()
    except OSError:
        return None


def environment(runner_class):
    cpuinfo = read("/proc/cpuinfo") or ""
    model = next((line.split(":", 1)[1].strip() for line in cpuinfo.splitlines() if line.startswith("model name")), None)
    meminfo = read("/proc/meminfo") or ""
    memory = next((int(line.split()[1]) * 1024 for line in meminfo.splitlines() if line.startswith("MemTotal:")), None)
    mount = command(["findmnt", "-J", "-T", tempfile.gettempdir(), "-o", "SOURCE,FSTYPE,OPTIONS,TARGET"])
    storage = command(["lsblk", "-J", "-o", "NAME,TYPE,SIZE,ROTA,MODEL,FSTYPE,MOUNTPOINTS"])
    result = {
        "runner_class": runner_class, "distribution": read("/etc/os-release"),
        "kernel": platform.release(), "architecture": platform.machine(), "cpu_model": model,
        "logical_processors": os.cpu_count(), "affinity": sorted(os.sched_getaffinity(0)),
        "memory_bytes": memory, "filesystem": json.loads(mount) if mount else None,
        "storage": json.loads(storage) if storage else None,
        "compiler": command([os.environ.get("CXX", "g++"), "--version"]),
        "cmake": command(["cmake", "--version"]), "ninja": command(["ninja", "--version"]),
        "openssl": command(["openssl", "version", "-a"]),
        "cpu_governor": read("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor"),
        "git_commit": command(["git", "rev-parse", "HEAD"], ROOT),
        "git_dirty": bool(command(["git", "status", "--porcelain", "--untracked-files=no"], ROOT)),
        "cache_policy": "warm OS cache; no cache purge", "network": "in-process nodes over loopback TLS",
        "scratch_parent": tempfile.gettempdir(),
    }
    identity = {key: result[key] for key in ("kernel", "architecture", "cpu_model", "logical_processors", "affinity",
                "memory_bytes", "filesystem", "compiler", "cmake", "ninja", "openssl", "cpu_governor")}
    result["machine_fingerprint"] = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--executable", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--runner-class", choices=("shared", "controlled", "manual-uncontrolled"), default="manual-uncontrolled")
    parser.add_argument("--profile", choices=("smoke", "characterize", "transport", "admission", "dispatch"), default="smoke")
    parser.add_argument("--rounds", type=int, default=3)
    args = parser.parse_args()
    if platform.system() != "Linux":
        parser.error("Linux is required; MSYS output must not be labeled Linux")
    if not 1 <= args.rounds <= 10:
        parser.error("rounds must be 1..10")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    info = environment(args.runner_class)
    common = ["--seed", "42", "--label", args.runner_class + ":" + info["machine_fingerprint"]]
    cases = [("smoke", ["--iterations", "3", "--objects", "3", "--bytes", "4096", "--chunk", "1024", "--workers", "2", "--warmup", "1"])]
    if args.profile == "transport":
        cases = [("nodelay-" + mode, ["--suite", "distributed", "--iterations", "40", "--objects", "16", "--bytes", "65536", "--chunk", "16384", "--nodes", "4", "--replicas", "2", "--workers", "1", "--warmup", "4", "--tcp-nodelay", mode]) for mode in ("off", "on")]
    if args.profile == "admission":
        cases = []
        # Full-connection serial reference mode is identical on both sides:
        # this isolates admission fairness from the independent dispatch change.
        for workers, iterations, prefix in ((1, 80, "workers-1"), (2, 80, "workers-2"),
                                             (4, 80, "workers-4"), (4, 400, "sustained-4")):
            for policy in ("mutex", "fifo"):
                cases.append((f"{prefix}-{policy}", ["--suite", "distributed", "--iterations", str(iterations),
                    "--objects", "16", "--bytes", "65536", "--chunk", "16384", "--nodes", "4", "--replicas", "2",
                    "--workers", str(workers), "--warmup", "4", "--admission", policy, "--dispatch", "serial",
                    "--admission-capacity", "16", "--admission-timeout-ms", "5000", "--tcp-nodelay", "on"]))
        for policy in ("mutex", "fifo"):
            cases.append((f"fault-{policy}", ["--suite", "fault", "--iterations", "20", "--objects", "20",
                "--bytes", "65536", "--chunk", "16384", "--workers", "4", "--warmup", "3",
                "--admission", policy, "--dispatch", "serial", "--tcp-nodelay", "on"]))
    if args.profile == "dispatch":
        cases = [("dispatch-" + policy, ["--suite", "dispatch", "--iterations", "12", "--objects", "1",
            "--bytes", "4096", "--chunk", "1024", "--workers", "1", "--warmup", "2",
            "--admission", "fifo", "--dispatch", policy, "--dispatch-workers", "4", "--dispatch-capacity", "4", "--tcp-nodelay", "on"])
            for policy in ("serial", "bounded")]
        # Hold coordinator admission at FIFO on both sides. Unlike the earlier
        # admission profile, these pairs audit the default bounded server's
        # handler-gate overhead on real distributed and durable repair work.
        for workers, iterations, prefix in ((1, 80, "dispatch-workers-1"), (2, 80, "dispatch-workers-2"),
                                             (4, 80, "dispatch-workers-4"), (4, 400, "dispatch-sustained-4")):
            for policy in ("serial", "bounded"):
                cases.append((f"{prefix}-{policy}", ["--suite", "distributed", "--iterations", str(iterations),
                    "--objects", "16", "--bytes", "65536", "--chunk", "16384", "--nodes", "4", "--replicas", "2",
                    "--workers", str(workers), "--warmup", "4", "--admission", "fifo", "--dispatch", policy,
                    "--dispatch-workers", "4", "--dispatch-capacity", "4", "--admission-capacity", "16",
                    "--admission-timeout-ms", "5000", "--tcp-nodelay", "on"]))
        for policy in ("serial", "bounded"):
            cases.append((f"dispatch-fault-{policy}", ["--suite", "fault", "--iterations", "20", "--objects", "20",
                "--bytes", "65536", "--chunk", "16384", "--workers", "4", "--warmup", "3", "--admission", "fifo",
                "--dispatch", policy, "--dispatch-workers", "4", "--dispatch-capacity", "4", "--tcp-nodelay", "on"]))
    if args.profile == "characterize":
        cases = []
        for mode in ("off", "on"):
            cases.append(("local-" + mode, ["--suite", "local", "--iterations", "60", "--objects", "32", "--bytes", "1048576", "--chunk", "65536", "--warmup", "4", "--metrics", mode]))
        for workers in (1, 2, 4):
            cases.append((f"workers-{workers}", ["--suite", "distributed", "--iterations", "80", "--objects", "16", "--bytes", "65536", "--chunk", "16384", "--nodes", "4", "--replicas", "2", "--workers", str(workers), "--warmup", "4"]))
        for objects in (8, 32, 128):
            cases.append((f"retained-{objects}", ["--suite", "startup", "--iterations", "20", "--objects", str(objects), "--bytes", "16384", "--chunk", "4096", "--warmup", "3"]))
        for chunk in (4096, 65536):
            cases.append((f"chunks-{chunk}", ["--suite", "local", "--iterations", "40", "--objects", "16", "--bytes", "65536", "--chunk", str(chunk), "--warmup", "4"]))
        for replicas in (1, 3):
            cases.append((f"replicas-{replicas}", ["--suite", "distributed", "--iterations", "40", "--objects", "16", "--bytes", "65536", "--chunk", "16384", "--nodes", "4", "--replicas", str(replicas), "--warmup", "4"]))
        cases.append(("fault", ["--suite", "fault", "--iterations", "20", "--objects", "20", "--bytes", "65536", "--chunk", "16384", "--warmup", "3"]))
    index = []
    for round_number in range(1, args.rounds + 1):
        # Alternate paired variant order to expose, not hide, order/cache noise.
        ordered = list(cases)
        if args.profile in ("characterize", "transport") and round_number % 2 == 0:
            ordered[0], ordered[1] = ordered[1], ordered[0]
        if args.profile in ("admission", "dispatch") and round_number % 2 == 0:
            for pair in range(0, len(ordered), 2):
                ordered[pair], ordered[pair + 1] = ordered[pair + 1], ordered[pair]
        for case, parameters in ordered:
            target = args.output_dir / f"{case}-{round_number}.json"
            if target.exists():
                parser.error(f"refusing to overwrite existing round: {target}")
            run_id = str(uuid.uuid4())
            argv = [str(args.executable.resolve()), *common, *parameters, "--output", str(target.resolve())]
            started = datetime.datetime.now(datetime.timezone.utc).isoformat()
            subprocess.run(argv, check=True, timeout=1800)
            with target.open(encoding="utf-8") as source:
                report = json.load(source)
            if report["source_commit"] != info["git_commit"]:
                raise RuntimeError("compiled benchmark revision differs from checkout; reconfigure first")
            report["environment"] = {**info, "run_id": run_id, "started_utc": started, "round": round_number, "command": argv}
            with target.open("w", encoding="utf-8") as output:
                json.dump(report, output, indent=2)
                output.write("\n")
            subprocess.run([os.sys.executable, str(ROOT / "benchmarks/validate.py"), str(target)], check=True)
            index.append({"case": case, "round": round_number, "run_id": run_id, "file": target.name})
    with (args.output_dir / "index.json").open("w", encoding="utf-8") as output:
        json.dump({"profile": args.profile, "runner_class": args.runner_class, "runs": index}, output, indent=2)


if __name__ == "__main__":
    main()
