"""
MPI version 2 - load-balanced distribution of the corpus files.

Why version 1 is imbalanced
  generator.py produces files of very different sizes (60 % small,
  30 % medium, 10 % large: from 2 000 up to 100 000 words). Version 1 gives
  every rank the same NUMBER of files, so a rank that happens to receive more
  large files does more work, and everybody else waits for it at the gather.

What this version does (default strategy: "lpt")
  Work is balanced by file SIZE (bytes), which is proportional to the number
  of tokens to scan. rank 0 sorts the files from largest to smallest and
  assigns each one to the rank with the smallest accumulated load so far
  (Longest Processing Time first, a greedy bin-packing heuristic). The
  assignment is then sent with comm.scatter, so the communication pattern is
  the same as version 1 (bcast + scatter + gather) and all p ranks count.

Optional strategy: "dynamic" (master-worker task pool)
  rank 0 acts only as a master and hands out one file at a time (largest
  first) to whichever worker asks for work, using point-to-point messages
  (comm.send / comm.recv with MPI.ANY_SOURCE and tags). It adapts to run-time
  noise but only p-1 ranks count words. Select it with:
      mpiexec ... python /app/mpi2.py --strategy dynamic

In both strategies rank 0 broadcasts the query words (comm.bcast), gathers
the partial counters (comm.gather) and prints the same Top 10 as the
sequential baseline, plus files, bytes and local time per process.

Run (inside the course container):
  mpiexec --allow-run-as-root --oversubscribe -np 8 python /app/mpi2.py
"""

import argparse
import glob
import heapq
import os
import sys
from collections import Counter

from mpi4py import MPI

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATASET_DIR = os.path.join(SCRIPT_DIR, "dataset")
QUERY_FILE = "consulta.txt"
BASELINE_CSV = "baseline_results.csv"
TOP_N = 10
CASE_SENSITIVE = False  # same setting as baseline_secuencial.py

# Message tags for the dynamic (master-worker) strategy.
TAG_REQUEST = 1   # worker -> master: "give me work"
TAG_TASK = 2      # master -> worker: one file path
TAG_STOP = 3      # master -> worker: no more work


# ---------------------------------------------------------------------------
# Helpers (same tokenisation rules as baseline_secuencial.py)
# ---------------------------------------------------------------------------
def load_query_words(dataset_dir):
    """Read consulta.txt (one word per line) and return the unique words."""
    path = os.path.join(dataset_dir, QUERY_FILE)
    with open(path, "r", encoding="utf-8") as f:
        words = [line.strip() for line in f if line.strip()]
    if not CASE_SENSITIVE:
        words = [w.lower() for w in words]
    return sorted(set(words))


def collect_file_sizes(comm, rank, size, dataset_dir):
    """
    Build [(path, size_in_bytes)] for every file_*.txt (sorted by name).

    Reading file metadata is not free (about 0.75 ms per file on the Docker
    bind mount used here), so instead of rank 0 calling stat() 3000 times the
    work is split: rank 0 lists the files and broadcasts the names, each rank
    stats an interleaved 1/p share, and rank 0 gathers the sizes.
    Only rank 0 receives the result; the other ranks get None.
    """
    paths = sorted(glob.glob(os.path.join(dataset_dir, "file_*.txt"))) if rank == 0 else None
    paths = comm.bcast(paths, root=0)
    my_sizes = [(i, os.path.getsize(paths[i])) for i in range(rank, len(paths), size)]
    parts = comm.gather(my_sizes, root=0)
    if rank != 0:
        return None
    sizes = [0] * len(paths)
    for part in parts:
        for i, nbytes in part:
            sizes[i] = nbytes
    return list(zip(paths, sizes))


def lpt_partition(files_with_size, parts):
    """
    Longest Processing Time first: take files from largest to smallest and
    give each one to the part with the smallest total bytes so far.
    Returns one list of paths per part.
    """
    heap = [(0, r) for r in range(parts)]          # (accumulated bytes, rank)
    heapq.heapify(heap)
    blocks = [[] for _ in range(parts)]
    for path, size in sorted(files_with_size, key=lambda fs: (-fs[1], fs[0])):
        load, r = heapq.heappop(heap)
        blocks[r].append(path)
        heapq.heappush(heap, (load + size, r))
    return blocks


def count_in_files(paths, targets, freq=None):
    """
    Count occurrences of the target words in the given files (same per-line
    split / lower-case / set-membership logic as the sequential baseline).
    """
    if freq is None:
        freq = Counter()
    tokens = nbytes = 0
    for path in paths:
        with open(path, "r", encoding="utf-8") as f:
            # size from the already-open handle: no extra path lookup
            nbytes += os.fstat(f.fileno()).st_size
            for line in f:
                words = line.split()
                if not CASE_SENSITIVE:
                    words = [w.lower() for w in words]
                tokens += len(words)
                for w in words:
                    if w in targets:
                        freq[w] += 1
    return freq, tokens, nbytes


def top_words(freq, n):
    """Top-n words by count (ties broken alphabetically for determinism)."""
    return sorted(freq.items(), key=lambda kv: (-kv[1], kv[0]))[:n]


def check_against_baseline(freq, dataset_dir):
    """Compare the full global count with the CSV written by the baseline."""
    path = os.path.join(dataset_dir, BASELINE_CSV)
    if not os.path.isfile(path):
        return "NOT_AVAILABLE (run baseline_secuencial.py first)"
    expected = {}
    with open(path, "r", encoding="utf-8") as f:
        next(f)  # header
        for line in f:
            word, count = line.strip().split(",")
            expected[word] = int(count)
    return "MATCH" if expected == dict(freq) else "MISMATCH"


def imbalance_metrics(local_times):
    """LI = max/mean and relative spread = (max - min)/mean."""
    mean_t = sum(local_times) / len(local_times)
    if mean_t == 0:
        return 1.0, 0.0
    return max(local_times) / mean_t, (max(local_times) - min(local_times)) / mean_t


# ---------------------------------------------------------------------------
# Strategy 1: LPT static assignment weighted by file size
# ---------------------------------------------------------------------------
def run_lpt(comm, rank, size, targets, files_with_size):
    blocks = lpt_partition(files_with_size, size) if rank == 0 else None
    my_files = comm.scatter(blocks, root=0)

    t0 = MPI.Wtime()
    freq, tokens, nbytes = count_in_files(my_files, targets)
    t_local = MPI.Wtime() - t0

    stats = {
        "rank": rank,
        "role": "worker",
        "files": len(my_files),
        "bytes": nbytes,
        "t_local": t_local,
        "tokens": tokens,
    }
    return freq, stats


# ---------------------------------------------------------------------------
# Strategy 2: dynamic master-worker task pool (point-to-point)
# ---------------------------------------------------------------------------
def run_dynamic(comm, rank, size, targets, files_with_size):
    if size < 2:
        raise SystemExit("The dynamic strategy needs at least 2 processes.")

    if rank == 0:
        # Largest files first, so the last tasks handed out are the small ones.
        queue = [p for p, _ in sorted(files_with_size, key=lambda fs: (-fs[1], fs[0]))]
        next_task = 0
        stopped = 0
        status = MPI.Status()
        t0 = MPI.Wtime()
        while stopped < size - 1:
            comm.recv(source=MPI.ANY_SOURCE, tag=TAG_REQUEST, status=status)
            worker = status.Get_source()
            if next_task < len(queue):
                comm.send(queue[next_task], dest=worker, tag=TAG_TASK)
                next_task += 1
            else:
                comm.send(None, dest=worker, tag=TAG_STOP)
                stopped += 1
        stats = {
            "rank": 0, "role": "master", "files": 0, "bytes": 0,
            "t_local": MPI.Wtime() - t0, "tokens": 0,
        }
        return Counter(), stats

    freq = Counter()
    files = nbytes = tokens = 0
    busy = 0.0
    status = MPI.Status()
    while True:
        comm.send(None, dest=0, tag=TAG_REQUEST)
        path = comm.recv(source=0, tag=MPI.ANY_TAG, status=status)
        if status.Get_tag() == TAG_STOP:
            break
        t0 = MPI.Wtime()
        _, n_tok, n_bytes = count_in_files([path], targets, freq)
        busy += MPI.Wtime() - t0
        files += 1
        nbytes += n_bytes
        tokens += n_tok
    stats = {
        "rank": rank, "role": "worker", "files": files, "bytes": nbytes,
        "t_local": busy, "tokens": tokens,
    }
    return freq, stats


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------
def print_report(title, size, stats, freq, total_tokens, total_time, dataset_dir):
    """Print per-process table, imbalance metrics, Top 10 and total time."""
    workers = [s for s in stats if s["role"] == "worker"]
    li, spread = imbalance_metrics([s["t_local"] for s in workers])

    print("=" * 64)
    print(title)
    print("=" * 64)
    print("Processes (p): {}  (counting processes: {})".format(size, len(workers)))
    print("Files processed: {}".format(sum(s["files"] for s in stats)))
    print("Tokens read: {}".format(total_tokens))
    print("Total occurrences found: {}".format(sum(freq.values())))
    print()
    print("Per-process report:")
    print("  {:>4} {:>7} {:>7} {:>14} {:>12}".format("rank", "role", "files", "bytes", "t_local(s)"))
    for s in stats:
        print("  {:>4} {:>7} {:>7} {:>14} {:>12.6f}".format(
            s["rank"], s["role"], s["files"], s["bytes"], s["t_local"]))
    for s in stats:  # machine-readable lines for the result parser
        print("PROC rank={} role={} files={} bytes={} t_local={:.6f}".format(
            s["rank"], s["role"], s["files"], s["bytes"], s["t_local"]))
    print()
    print("Load imbalance LI = max(t_i)/mean(t_i) over counting processes: {:.4f}".format(li))
    print("Relative spread (max-min)/mean:                                {:.4f}".format(spread))
    print("LI={:.6f}".format(li))
    print("SPREAD={:.6f}".format(spread))
    print()
    print("Top {} query words in the corpus:".format(TOP_N))
    for word, count in top_words(freq, TOP_N):
        print("  {}: {}".format(word, count))
    print()
    print("Check against baseline_results.csv: {}".format(check_against_baseline(freq, dataset_dir)))
    print("Total execution time: {:.6f} s".format(total_time))
    # Line parsed by run_all.sh
    print("EXECUTION_TIME={:.6f}".format(total_time))
    sys.stdout.flush()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Load-balanced MPI word count.")
    parser.add_argument("--strategy", choices=["lpt", "dynamic"], default="lpt",
                        help="lpt: static size-weighted assignment (default); "
                             "dynamic: master-worker task pool")
    args = parser.parse_args()

    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    comm.Barrier()          # every process starts the clock together
    t_start = MPI.Wtime()

    # rank 0 reads the query words.
    if rank == 0:
        if not os.path.isdir(DATASET_DIR):
            print("Error: dataset folder not found at {}".format(DATASET_DIR))
            comm.Abort(1)
        query_words = load_query_words(DATASET_DIR)
    else:
        query_words = None

    # Broadcast the query words (collective communication).
    query_words = comm.bcast(query_words, root=0)
    targets = set(query_words)

    # File list + sizes (needed to balance by bytes); only rank 0 keeps them.
    t_scan = MPI.Wtime()
    files_with_size = collect_file_sizes(comm, rank, size, DATASET_DIR)
    t_scan = MPI.Wtime() - t_scan

    if args.strategy == "lpt":
        local_freq, my_stats = run_lpt(comm, rank, size, targets, files_with_size)
        title = "MPI version 2 - size-weighted static distribution (LPT)"
    else:
        local_freq, my_stats = run_dynamic(comm, rank, size, targets, files_with_size)
        title = "MPI version 2 - dynamic master-worker task pool"

    # Gather partial results in rank 0.
    all_freqs = comm.gather(local_freq, root=0)
    all_stats = comm.gather(my_stats, root=0)

    if rank == 0:
        global_freq = Counter()
        for partial in all_freqs:
            global_freq.update(partial)
        total_time = MPI.Wtime() - t_start
        total_tokens = sum(s["tokens"] for s in all_stats)
        print_report(title, size, all_stats, global_freq, total_tokens, total_time, DATASET_DIR)
        # Cost of obtaining the file sizes (the price paid for balancing by bytes).
        print("Size scan time (stat of all files, split among ranks): {:.6f} s".format(t_scan))
        print("SCAN_TIME={:.6f}".format(t_scan))


if __name__ == "__main__":
    main()
