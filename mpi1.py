"""
MPI version 1 - static block distribution of the corpus files.

Flow (Part 1 of the lab):
  1. rank 0 reads dataset/consulta.txt and builds the set of query words.
  2. rank 0 broadcasts the query words to every process (comm.bcast).
  3. rank 0 lists the dataset/file_*.txt files (sorted by name).
  4. The files are split statically into p contiguous blocks with almost the
     same number of files each, and every rank receives its block (comm.scatter).
  5. Each rank counts the query words in its own files and times that work
     with MPI.Wtime() (local processing time t_i).
  6. Partial counters and per-process statistics are gathered in rank 0
     (comm.gather).
  7. rank 0 merges the counters, prints the Top 10, the per-process report
     (files, bytes, t_i), the load imbalance metrics and the total time.

Run (inside the course container):
  mpiexec --allow-run-as-root --oversubscribe -np 8 python /app/mpi1.py
"""

import glob
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


def list_corpus_files(dataset_dir):
    """Return the corpus files (file_*.txt) sorted by name."""
    return sorted(glob.glob(os.path.join(dataset_dir, "file_*.txt")))


def block_partition(items, parts):
    """
    Split `items` into `parts` contiguous blocks whose lengths differ by at
    most one. This balances the NUMBER of files, not their size.
    """
    base, extra = divmod(len(items), parts)
    blocks, start = [], 0
    for r in range(parts):
        end = start + base + (1 if r < extra else 0)
        blocks.append(items[start:end])
        start = end
    return blocks


def count_in_files(paths, targets):
    """
    Count occurrences of the target words in the given files.
    Uses the same per-line split / lower-case / set-membership logic as the
    sequential baseline, so the work per token is identical.
    """
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


def print_report(title, size, stats, freq, total_tokens, total_time, dataset_dir):
    """Print the per-process table, imbalance metrics, Top 10 and total time."""
    local_times = [s["t_local"] for s in stats]
    li, spread = imbalance_metrics(local_times)

    print("=" * 64)
    print(title)
    print("=" * 64)
    print("Processes (p): {}".format(size))
    print("Files processed: {}".format(sum(s["files"] for s in stats)))
    print("Tokens read: {}".format(total_tokens))
    print("Total occurrences found: {}".format(sum(freq.values())))
    print()
    print("Per-process report:")
    print("  {:>4} {:>7} {:>14} {:>12}".format("rank", "files", "bytes", "t_local(s)"))
    for s in stats:
        print("  {:>4} {:>7} {:>14} {:>12.6f}".format(s["rank"], s["files"], s["bytes"], s["t_local"]))
    for s in stats:  # machine-readable lines for the result parser
        print("PROC rank={} files={} bytes={} t_local={:.6f}".format(s["rank"], s["files"], s["bytes"], s["t_local"]))
    print()
    print("Load imbalance LI = max(t_i)/mean(t_i): {:.4f}".format(li))
    print("Relative spread (max-min)/mean:       {:.4f}".format(spread))
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
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    comm.Barrier()          # every process starts the clock together
    t_start = MPI.Wtime()

    # 1 and 3. rank 0 reads the query and lists the corpus.
    if rank == 0:
        if not os.path.isdir(DATASET_DIR):
            print("Error: dataset folder not found at {}".format(DATASET_DIR))
            comm.Abort(1)
        query_words = load_query_words(DATASET_DIR)
        files = list_corpus_files(DATASET_DIR)
        blocks = block_partition(files, size)
    else:
        query_words = None
        blocks = None

    # 2. Broadcast the query words (collective communication).
    query_words = comm.bcast(query_words, root=0)
    targets = set(query_words)

    # 4. Static distribution: each rank receives one contiguous block of files.
    my_files = comm.scatter(blocks, root=0)

    # 5. Local counting, timed with MPI.Wtime().
    t0 = MPI.Wtime()
    local_freq, local_tokens, local_bytes = count_in_files(my_files, targets)
    t_local = MPI.Wtime() - t0

    my_stats = {
        "rank": rank,
        "files": len(my_files),
        "bytes": local_bytes,
        "t_local": t_local,
        "tokens": local_tokens,
    }

    # 6. Gather partial results in rank 0.
    all_freqs = comm.gather(local_freq, root=0)
    all_stats = comm.gather(my_stats, root=0)

    # 7. rank 0 builds the global result.
    if rank == 0:
        global_freq = Counter()
        for partial in all_freqs:
            global_freq.update(partial)
        total_time = MPI.Wtime() - t_start
        total_tokens = sum(s["tokens"] for s in all_stats)
        print_report(
            "MPI version 1 - static block distribution",
            size, all_stats, global_freq, total_tokens, total_time, DATASET_DIR,
        )


if __name__ == "__main__":
    main()
