"""
Summarise the logs written by run_experiments.sh.

Computes, for every version:
  T_p  = mean total execution time over the runs
  S_p  = T_seq / T_p
  E_p  = S_p / p
  LI   = max(t_i) / mean(t_i)            (per run, then averaged)
  RS   = (max(t_i) - min(t_i)) / mean(t_i)

Usage:  python summarize_results.py [experiment_results/logs]
Writes experiment_results/summary_<log folder>.md (Markdown tables used in README.md).
"""

import glob
import os
import re
import statistics
import sys

LOG_DIR = sys.argv[1] if len(sys.argv) > 1 else os.path.join("experiment_results", "logs")
_LOG_DIR = LOG_DIR.rstrip("/\\")
OUT_FILE = os.path.join(os.path.dirname(_LOG_DIR), "summary_{}.md".format(os.path.basename(_LOG_DIR)))

VERSIONS = [
    ("mpi1", "MPI v1 - static blocks (mpi1.py)"),
    ("mpi2", "MPI v2 - LPT size-weighted (mpi2.py)"),
    ("mpi2dyn", "MPI v2 variant - dynamic master-worker (mpi2.py --strategy dynamic)"),
]


def read(path):
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def run_number(path):
    return int(re.search(r"run(\d+)\.log$", path).group(1))


def parse_baseline(path):
    text = read(path)
    t = float(re.search(r"Tiempo de ejecuci.n: ([\d.]+)", text).group(1))
    top = re.findall(r"^  (\S+): (\d+)$", text, re.M)
    return t, top


def parse_mpi(path):
    text = read(path)
    procs = []
    for m in re.finditer(r"^PROC (.*)$", text, re.M):
        fields = dict(kv.split("=") for kv in m.group(1).split())
        procs.append({
            "rank": int(fields["rank"]),
            "role": fields.get("role", "worker"),
            "files": int(fields["files"]),
            "bytes": int(fields["bytes"]),
            "t_local": float(fields["t_local"]),
        })
    return {
        "total": float(re.search(r"^EXECUTION_TIME=([\d.]+)", text, re.M).group(1)),
        "li": float(re.search(r"^LI=([\d.]+)", text, re.M).group(1)),
        "spread": float(re.search(r"^SPREAD=([\d.]+)", text, re.M).group(1)),
        "check": re.search(r"Check against baseline_results.csv: (\S+)", text).group(1),
        "top": re.findall(r"^  (\S+): (\d+)$", text, re.M),
        "scan": [float(x) for x in re.findall(r"^SCAN_TIME=([\d.]+)", text, re.M)],
        "p": int(re.search(r"Processes \(p\): (\d+)", text).group(1)),
        "procs": procs,
    }


def main():
    out = []
    w = out.append

    base_logs = sorted(glob.glob(os.path.join(LOG_DIR, "baseline_run*.log")), key=run_number)
    base = [parse_baseline(p) for p in base_logs]
    t_seq = statistics.mean(t for t, _ in base)
    ref_top = base[0][1]

    w("## Sequential baseline\n")
    w("| Run | Time (s) |")
    w("|---|---|")
    for path, (t, _) in zip(base_logs, base):
        w("| {} | {:.3f} |".format(run_number(path), t))
    w("| **Mean T_seq** | **{:.3f}** |".format(t_seq))
    w("| Std. dev. | {:.3f} |\n".format(statistics.stdev([t for t, _ in base]) if len(base) > 1 else 0))
    w("Top 10 (baseline): " + ", ".join("{} {}".format(a, b) for a, b in ref_top) + "\n")

    comparison = []
    for key, title in VERSIONS:
        logs = sorted(glob.glob(os.path.join(LOG_DIR, key + "_np*_run*.log")), key=run_number)
        if not logs:
            continue
        runs = [parse_mpi(p) for p in logs]
        p = runs[0]["p"]
        t_p = statistics.mean(r["total"] for r in runs)
        s_p = t_seq / t_p
        e_p = s_p / p
        li = statistics.mean(r["li"] for r in runs)
        rs = statistics.mean(r["spread"] for r in runs)
        all_match = all(r["check"] == "MATCH" and r["top"] == ref_top for r in runs)
        comparison.append((title, p, t_p, s_p, e_p, li, rs, all_match))

        w("## {}\n".format(title))
        w("| Run | Total time (s) | max t_i (s) | mean t_i (s) | min t_i (s) | LI | Rel. spread | Top 10 = baseline |")
        w("|---|---|---|---|---|---|---|---|")
        for path, r in zip(logs, runs):
            ts = [q["t_local"] for q in r["procs"] if q["role"] == "worker"]
            w("| {} | {:.3f} | {:.3f} | {:.3f} | {:.3f} | {:.4f} | {:.4f} | {} |".format(
                run_number(path), r["total"], max(ts), statistics.mean(ts), min(ts),
                r["li"], r["spread"], "yes" if (r["check"] == "MATCH" and r["top"] == ref_top) else "NO"))
        w("| **Mean** | **{:.3f}** | | | | **{:.4f}** | **{:.4f}** | |".format(t_p, li, rs))
        w("")
        w("T_p = {:.3f} s, S_p = {:.3f}, E_p = {:.3f} (p = {})\n".format(t_p, s_p, e_p, p))
        scans = [x for r in runs for x in r["scan"]]
        if scans:
            w("Mean size-scan time (stat of all files before balancing): {:.3f} s\n".format(statistics.mean(scans)))
        overhead = statistics.mean(
            r["total"] - max(q["t_local"] for q in r["procs"] if q["role"] == "worker") for r in runs)
        w("Mean overhead outside counting (total - max t_i): {:.3f} s\n".format(overhead))

        w("Per-process local times t_i (s), files and MB per rank:\n")
        header = "| Rank | Role | Files | MB | " + " | ".join("t_i run {}".format(run_number(x)) for x in logs) + " | mean t_i |"
        w(header)
        w("|" + "---|" * (5 + len(logs)))
        for i, q in enumerate(runs[0]["procs"]):
            times = [r["procs"][i]["t_local"] for r in runs]
            files = "/".join(sorted({str(r["procs"][i]["files"]) for r in runs}))
            mb = "/".join(sorted({"{:.1f}".format(r["procs"][i]["bytes"] / 1e6) for r in runs}))
            w("| {} | {} | {} | {} | {} | {:.3f} |".format(
                q["rank"], q["role"], files, mb, " | ".join("{:.3f}".format(t) for t in times),
                statistics.mean(times)))
        w("")

    w("## Comparison\n")
    w("| Version | p | T (s) | Speedup S_p | Efficiency E_p | LI | Rel. spread | Correct |")
    w("|---|---|---|---|---|---|---|---|")
    w("| Sequential baseline | 1 | {:.3f} | 1.000 | 1.000 | - | - | reference |".format(t_seq))
    for title, p, t_p, s_p, e_p, li, rs, ok in comparison:
        w("| {} | {} | {:.3f} | {:.3f} | {:.3f} | {:.4f} | {:.4f} | {} |".format(
            title, p, t_p, s_p, e_p, li, rs, "yes" if ok else "NO"))

    text = "\n".join(out) + "\n"
    with open(OUT_FILE, "w", encoding="utf-8") as f:
        f.write(text)
    print(text)
    print("Written to", OUT_FILE)


if __name__ == "__main__":
    main()
