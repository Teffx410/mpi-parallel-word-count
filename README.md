# Parallel Word Counting in a Text Corpus with MPI

## 1. Title

**LAB 2 – Parallel Word Counting in a Text Corpus with MPI**

- Student: **Juan José Bolívar Velandia**
- Course: **Estructura del Computador 2** (Computer Structure II)
- Institution: **Universidad del Norte**, División de Ingenierías
- Tools: Python 3.8 + `mpi4py` 4.1.1 + Open MPI 4.1.4, inside the course container `augustosalazar/slim-mpi:2`

## 2. Problem Description

The corpus is produced by `generator.py` (fixed seed 42). It contains:

- 3000 text files `dataset/file_XXXX.txt` with 44,951,458 words (≈ 275 MB) in total;
- a query file `dataset/consulta.txt` with 200 distinct words.

The task is to count how many times each query word appears in the whole corpus and to report the **Top 10** most frequent query words. The result must be exactly the same as the one produced by the sequential reference `baseline_secuencial.py`.

The workload is embarrassingly parallel: every file can be processed independently, and the partial counters can be added in any order. The difficulty is in distributing the work well:

- **Communication**: the query must reach every process, and the partial counters must be collected and merged at the root. Any time spent sending, receiving or serialising (pickling) Python objects is overhead that the sequential code does not have.
- **Synchronisation**: the root can only build the final answer once *all* processes have finished. The slowest process therefore sets the parallel time.
- **Load balance**: `generator.py` deliberately creates heterogeneous file sizes:
  - 60 % small files (2,000–5,000 words);
  - 30 % medium files (10,000–20,000 words);
  - 10 % large files (50,000–100,000 words).

  Giving every process the same *number* of files does not give them the same *amount of work*. The processes that receive more large files become stragglers, and the others sit idle at the final collective.

## 3. Environment and Execution Instructions

### a. Experimental Plan

| Item | Value |
|---|---|
| Host | Intel Core i3-10100 @ 3.60 GHz, **4 physical cores / 8 hardware threads**, 15.9 GB RAM, Windows 10 |
| Container | Docker Desktop 29.0.1 (WSL2 backend, 8 vCPUs and 7.7 GB visible to Docker), image `augustosalazar/slim-mpi:2` (Python 3.8.20, mpi4py 4.1.1, Open MPI 4.1.4) |
| Working directory | Repository folder bind-mounted at `/app`, as the lab instructions require |
| Dataset | `generator.py` unchanged, seed 42: 3000 files, 44,951,458 tokens, 200 query words |
| Process count `p` | **7**, the value recommended by `detectar_procesos_mpi.py` (see below) |
| Runs per version | 1 warm-up run (discarded) + **3 measured runs** each for the baseline, `mpi1.py` and `mpi2.py` |
| Additional variant | `mpi2.py --strategy dynamic` (master–worker), 3 runs, for comparison |
| Supplementary control | The full plan repeated with the corpus copied into the container's own filesystem (not the bind mount), to separate file-system effects from algorithmic effects |

**Choice of `p`.** `detectar_procesos_mpi.py` (from the course repository `mpi_taller_sudoku`) times a prime-counting MPI task for N = 1 … `nproc` = 8 and recommends the N with the lowest median time.

| N | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | Recommended |
|---|---|---|---|---|---|---|---|---|---|
| Execution 1 (median, s) | 5.393 | 3.310 | 3.860 | 2.526 | 2.561 | 2.701 | **2.193** | 2.248 | **7** |
| Execution 2 (median, s) | 5.489 | 3.288 | 3.450 | **2.261** | 2.418 | 2.640 | 2.310 | 2.359 | 4 |

The first execution recommended **p = 7**, and all experiments use that value. A second execution recommended N = 4, but the medians of N = 4, 7 and 8 lie within about 4 % of each other. The detector therefore shows a *plateau* from N ≈ 4 to N = 8 rather than one sharp optimum, which is what one expects on a machine with 4 physical cores and Hyper-Threading. Logs: `experiment_results/detectar_procesos_mpi_run1.txt` and `experiment_results/detectar_procesos_mpi_run2.log`.

**Measured variables.**

- Baseline: the time printed by `baseline_secuencial.py` (counting phase measured with `time.perf_counter`).
- MPI versions:
  - **total execution time**: `MPI.Wtime()` on rank 0, from a `Barrier` right after `MPI_Init` until the global result is merged;
  - **local processing time** `t_i` of every rank: `MPI.Wtime()` around its counting loop;
  - files and bytes processed by every rank;
  - a full comparison of all 200 counters against `dataset/baseline_results.csv`.

**Derived metrics.**

- $T_p = \frac{1}{k}\sum_j T_{p,j}$
- $S_p = T_{seq}/T_p$
- $E_p = S_p/p$
- $LI = \max(t_i)/\text{mean}(t_i)$
- relative spread $= (\max t_i - \min t_i)/\text{mean}(t_i)$

LI and the spread are computed per run and then averaged. For the master–worker variant they are computed over the counting ranks only.

### b. Sequential baseline (what it does and how)

`baseline_secuencial.py` (unchanged):

1. reads `consulta.txt` into a set of lower-case words;
2. walks over every `file_*.txt` in `dataset/`, reading it line by line;
3. splits each line on whitespace, lower-cases the tokens and increments a `Counter` for every token that belongs to the query set;
4. prints the elapsed time, the number of files and tokens, and the Top 10, and writes all 200 counters to `dataset/baseline_results.csv`.

The MPI versions use this CSV as the correctness reference.

```bash
# Linux / macOS
docker run --rm -v "$(pwd)":/app augustosalazar/slim-mpi:2 python /app/baseline_secuencial.py
# Windows (cmd)
docker run --rm -v "%cd%:/app" augustosalazar/slim-mpi:2 python /app/baseline_secuencial.py
```

### c. MPI version 1 (what it does and how)

`mpi1.py` implements the required flow with a **static block distribution**:

1. All ranks synchronise with `comm.Barrier()` and rank 0 starts the total timer.
2. **rank 0 reads `consulta.txt`** and builds the list of unique lower-case query words.
3. **rank 0 broadcasts the query words** with `comm.bcast(query_words, root=0)`; every rank turns them into a `set` for O(1) look-ups.
4. **rank 0 lists `dataset/file_*.txt`** sorted by name and splits the list into `p` **contiguous blocks** whose lengths differ by at most one (428 or 429 files per rank for p = 7). The blocks are delivered with `comm.scatter(blocks, root=0)`.
5. Each rank **counts the query words in its own files**. It uses exactly the same tokenisation as the baseline (per-line `split()`, `lower()`, set membership), so the work per token is identical. It times this loop with `MPI.Wtime()` (`t_i`).
6. **Partial `Counter`s and per-rank statistics** (files, bytes, `t_i`, tokens) are collected with `comm.gather(..., root=0)`.
7. rank 0 merges the counters and prints:
   - the per-process report (rank, files, bytes, `t_i`);
   - LI and the relative spread;
   - the **Top 10** (ties broken alphabetically);
   - the result of the comparison with `baseline_results.csv`;
   - the total time, also as `EXECUTION_TIME=<s>` so that `run_all.sh` can parse it.

```bash
docker run --rm -v "$(pwd)":/app augustosalazar/slim-mpi:2 \
  mpiexec --allow-run-as-root --oversubscribe -np 7 python /app/mpi1.py
```

### d. Test procedure

All commands are run from the repository folder. On Windows `cmd`, replace `"$(pwd)":/app` with `"%cd%:/app"`.

1. **Generate the dataset** (deterministic, seed 42):
   ```bash
   docker run --rm -v "$(pwd)":/app augustosalazar/slim-mpi:2 python /app/generator.py
   ```
2. **Choose `p`**:
   ```bash
   docker run --rm -v "$(pwd)":/app augustosalazar/slim-mpi:2 python /app/detectar_procesos_mpi.py
   ```
3. **Run the experimental plan.** This does 1 warm-up run, 3 baseline runs, 3 × `mpi1.py`, 3 × `mpi2.py` and 3 × `mpi2.py --strategy dynamic`, with `P=7`. Logs go to `experiment_results/logs/`.
   ```bash
   docker run --rm -v "$(pwd)":/app augustosalazar/slim-mpi:2 sh /app/run_experiments.sh
   ```
4. **Optional control run** on the container's native filesystem (logs go to `experiment_results/logs_native/`):
   ```bash
   docker run --rm -v "$(pwd)":/app augustosalazar/slim-mpi:2 sh -c \
     'cp -r /app /work && APP_DIR=/work OUT_DIR=/app/experiment_results/logs_native sh /work/run_experiments.sh'
   ```
5. **Compute the metrics**. This writes `experiment_results/summary_logs.md` and `summary_logs_native.md`; any Python ≥ 3.6 works, inside or outside the container.
   ```bash
   python summarize_results.py experiment_results/logs
   python summarize_results.py experiment_results/logs_native
   ```
6. **Correctness check.** Every MPI log must contain `Check against baseline_results.csv: MATCH`, meaning all 200 counters are equal to the baseline, not only the Top 10. The summary tables report this in the column *Top 10 = baseline*.
7. The instructor's batch script also works unchanged with both versions:
   ```bash
   docker run --rm -v "$(pwd)":/app augustosalazar/slim-mpi:2 sh /app/run_all.sh
   ```

## 4. Experimental Plan Execution

All numbers below come from the logs in `experiment_results/`. The main experiment uses the bind-mounted `/app` folder, as the lab prescribes.

### a. Sequential baseline timing

| Run | Time (s) |
|---|---|
| 1 | 23.821 |
| 2 | 24.006 |
| 3 | 24.107 |
| **Mean $T_{seq}$** | **23.978** |
| Std. dev. | 0.145 |

The baseline processed 3000 files and 44,951,458 tokens, and found 3,631,778 occurrences in total. Reference **Top 10**, identical in every sequential and parallel run:

| # | Word | Count |
|---|---|---|
| 1 | a | 785,774 |
| 2 | para | 392,156 |
| 3 | sus | 228,913 |
| 4 | otros | 105,530 |
| 5 | ante | 99,832 |
| 6 | unos | 88,794 |
| 7 | otra | 83,901 |
| 8 | vosotros | 61,617 |
| 9 | mios | 58,420 |
| 10 | tuya | 56,635 |

### b. MPI version 1 timing results

`mpi1.py`, p = 7, static blocks:

| Run | Total time (s) | Top 10 / all counters = baseline |
|---|---|---|
| 1 | 6.110 | yes |
| 2 | 6.026 | yes |
| 3 | 5.970 | yes |
| **Mean $T_p$** | **6.035** | |

| $T_{seq}$ (s) | $T_p$ (s) | Speedup $S_p$ | Efficiency $E_p$ |
|---|---|---|---|
| 23.978 | 6.035 | **3.973** | **0.568** |

The time outside the counting loop is small: total time − max(`t_i`) = **0.025 s** on average. It covers reading the query, the `bcast`, the `scatter`, the `gather` and the merge.

### c. Load imbalance evidence (per-process local times and LI)

Per-rank data for `mpi1.py`. All ranks get the same *number* of files, but not the same *volume*:

| Rank | Files | MB assigned | $t_i$ run 1 (s) | $t_i$ run 2 (s) | $t_i$ run 3 (s) | Mean $t_i$ (s) |
|---|---|---|---|---|---|---|
| 0 | 429 | 42.0 | 6.083 | 5.943 | 5.897 | 5.974 |
| 1 | 429 | **35.7** | 5.479 | 5.080 | 5.399 | **5.319** |
| 2 | 429 | **42.6** | 6.079 | 5.997 | 5.950 | **6.009** |
| 3 | 429 | 38.0 | 5.690 | 5.527 | 5.515 | 5.578 |
| 4 | 428 | 38.2 | 5.633 | 5.297 | 5.567 | 5.499 |
| 5 | 428 | 39.5 | 5.836 | 5.733 | 5.683 | 5.751 |
| 6 | 428 | 39.1 | 5.861 | 5.788 | 5.721 | 5.790 |

| Run | max $t_i$ | mean $t_i$ | min $t_i$ | **LI** | Relative spread |
|---|---|---|---|---|---|
| 1 | 6.083 | 5.809 | 5.479 | 1.0472 | 0.1040 |
| 2 | 5.997 | 5.623 | 5.080 | 1.0664 | 0.1631 |
| 3 | 5.950 | 5.676 | 5.399 | 1.0483 | 0.0971 |
| **Mean** | | | | **1.0540** | **0.1214** |

The imbalance is systematic, not noise. In all three runs:

- rank 2 (42.6 MB, the most data) is the slowest;
- rank 1 (35.7 MB, the least data) is the fastest.

The byte loads range from 35.7 to 42.6 MB around a mean of 39.3 MB, a byte-level LI of 1.084. The ordering of `t_i` follows the ordering of the assigned bytes. On average the fastest rank sits idle ≈ 0.7 s per run, waiting for the slowest one at the `gather`.

### d. MPI version 2: how it corrects the imbalance, and its results (total execution time, speedup, efficiency, LI)

**Strategy (`mpi2.py`, default `--strategy lpt`): size-weighted static assignment (Longest Processing Time first).**

The cost of counting a file is proportional to its size, so version 2 balances **bytes** instead of file counts:

1. As before, rank 0 reads the query and `bcast`s it.
2. rank 0 `bcast`s the list of file names. Each rank calls `stat()` on an interleaved 1/p share of the files, and rank 0 `gather`s the sizes. This parallel scan exists because on the bind mount one `stat()` costs ≈ 0.75 ms, and 3000 of them on rank 0 alone took 2.3 s.
3. rank 0 sorts the files from **largest to smallest** and gives each one to the rank with the **smallest accumulated load**, using a min-heap. This is the greedy LPT bin-packing heuristic, whose makespan is within 4/3 of the optimum.
4. The assignment is delivered with `comm.scatter`. Counting, timing, `gather`, merge and the Top 10 check work exactly as in version 1, so only the *distribution* differs.

A second strategy is included for comparison: `--strategy dynamic`, a **master–worker task pool**.

- rank 0 only hands out files, largest first, one at a time.
- Workers ask for work with `comm.send(..., tag=TAG_REQUEST)`.
- The master answers whoever asked first, using `comm.recv(source=MPI.ANY_SOURCE)`, with a `TAG_TASK` or `TAG_STOP` message.

**Results (bind-mounted `/app`, p = 7, 3 runs each):**

| Run | v2 LPT total (s) | v2 LPT LI | v2 LPT spread | Dynamic total (s) | Dynamic LI | Dynamic spread |
|---|---|---|---|---|---|---|
| 1 | 6.124 | 1.0147 | 0.0301 | 7.041 | 1.0006 | 0.0018 |
| 2 | 6.074 | 1.0137 | 0.0276 | 7.162 | 1.0012 | 0.0026 |
| 3 | 6.255 | 1.0130 | 0.0282 | 6.919 | 1.0009 | 0.0032 |
| **Mean** | **6.151** | **1.0138** | **0.0286** | **7.041** | **1.0009** | **0.0025** |

With LPT every rank receives **39.3 MB** (427–429 files), and the mean `t_i` per rank lies between 5.53 and 5.67 s. Every run matched the baseline exactly.

**Side-by-side comparison (main experiment, bind-mounted `/app`):**

| Version | p | $T$ (s) | $S_p$ | $E_p$ | LI | Rel. spread | max $t_i$ (s, mean) | Overhead outside counting (s) | Correct |
|---|---|---|---|---|---|---|---|---|---|
| Sequential baseline | 1 | 23.978 | 1.000 | 1.000 | – | – | – | – | reference |
| MPI v1 – static blocks | 7 | **6.035** | **3.973** | **0.568** | 1.0540 | 0.1214 | 6.010 | 0.025 | yes |
| MPI v2 – LPT (size-weighted) | 7 | 6.151 | 3.898 | 0.557 | 1.0138 | 0.0286 | **5.674** | 0.478 (0.460 = size scan) | yes |
| MPI v2 variant – dynamic master–worker | 7 | 7.041 | 3.405 | 0.486 | **1.0009** | **0.0025** | 6.516 | 0.525 | yes |

**Supplementary control: the same plan with the corpus on the container's native filesystem** (`experiment_results/logs_native/`). File metadata is cheap there (the full size scan takes 0.019 s instead of 0.46 s):

| Version | p | $T$ (s) | $S_p$ | $E_p$ | LI | Rel. spread | Correct |
|---|---|---|---|---|---|---|---|
| Sequential baseline | 1 | 10.564 | 1.000 | 1.000 | – | – | reference |
| MPI v1 – static blocks | 7 | 3.182 | 3.320 | 0.474 | 1.0936 | 0.1756 | yes |
| MPI v2 – LPT (size-weighted) | 7 | **2.688** | **3.930** | **0.561** | 1.0171 | 0.0405 | yes |
| MPI v2 variant – dynamic master–worker | 7 | 3.229 | 3.272 | 0.467 | **1.0035** | 0.0067 | yes |

Run-by-run and per-rank tables for both settings: `experiment_results/summary_logs.md` and `experiment_results/summary_logs_native.md`.

## 5. Analysis

### a. Parallel performance: Did MPI version 1 improve on the sequential baseline, and how close was its speedup to the ideal ($S_p = p$)? What explains the gap?

Yes, version 1 clearly improved on the baseline. It dropped from 23.98 s to 6.04 s, which is **$S_7$ = 3.97** and **$E_7$ = 0.57**: 57 % of the ideal $S_p = 7$. The run-to-run variation is small (std. dev. 0.15 s for the baseline, 0.07 s for v1), so the gap is real and not measurement noise. Four factors explain it, in decreasing order of weight:

1. **The hardware does not offer 7 independent cores.** The i3-10100 has **4 physical cores**, each with 2 hardware threads. With 7 processes, at least 3 cores run two processes on the same execution units. Hyper-Threading typically adds only 15–30 % of throughput, not 100 %. With all cores busy, Turbo Boost also lowers the clock frequency. This shows up directly in the data: on the native filesystem, the 7 LPT ranks together spent 7 × 2.62 ≈ 18.3 CPU-seconds on work that one process does in 10.6 s, so each process ran ≈ 1.7× slower than when running alone. The course's own detector shows the same ceiling for a pure-CPU task: prime counting reaches only 5.39 / 2.19 ≈ 2.5× at N = 7. A realistic upper bound here is roughly 4–5×, not 7×, and v1 already gets 3.97×.
2. **Shared I/O path.** The corpus lives on a Windows folder exposed to the Linux container through Docker Desktop's file-sharing layer. All ranks share that single channel. The baseline needs 24.0 s on the bind mount but only 10.6 s on the native filesystem, so more than half of the sequential time is file access. That part can overlap between processes, but it does not scale perfectly.
3. **Load imbalance.** In v1 the slowest rank sets the time. With LI = 1.054, the critical path is 5.4 % longer than with a perfectly even split (max $t_i$ = 6.01 s against mean $t_i$ = 5.70 s).
4. **Communication, serialisation and the serial part (Amdahl).** These are almost negligible:
   - reading the query, the `bcast` of 200 words, the `scatter` of the file lists, and the `gather` and merge of 7 small counters take ≈ 0.025 s per run (0.4 % of $T_p$);
   - with a serial fraction $f$ ≈ 0.004, Amdahl's law would still allow $S_7$ ≈ 6.8;
   - the gap is therefore caused by contention for hardware resources, not by the MPI algorithm.

   `mpiexec` launch time is outside both timers and therefore not included.

### b. Load balance: What evidence of load imbalance did version 1 show (per-process local times and LI), and did version 2 reduce it enough to produce a real performance improvement?

**Evidence in v1.** The block distribution balances the number of files (428–429 per rank) but not their size: the assigned volume ranges from 35.7 MB to 42.6 MB. The per-rank times follow that volume consistently in all three runs:

- rank 2 (42.6 MB) is always the slowest, at 6.01 s on average;
- rank 1 (35.7 MB) is always the fastest, at 5.32 s on average.

This gives **LI = 1.054** and a relative spread of **0.121** on the bind mount. On the native filesystem the effect is larger (**LI = 1.094**, spread 0.176). There the time is dominated by CPU work proportional to bytes, so the byte imbalance (1.084) appears almost one-to-one in time. On the bind mount part of each file's cost is a fixed per-file access latency, and v1 *does* balance that part, so the imbalance is diluted.

**Did v2 reduce it?** Yes, very clearly:

- **LPT** gave every rank 39.3 MB and lowered LI from 1.054 to **1.014** (spread 0.121 → 0.029).
- The **dynamic** pool lowered it further to **1.001**.
- The critical path of the counting phase (max $t_i$) went from 6.01 s to **5.67 s** with LPT, a 5.6 % reduction.

**Did that become a real speedup?** It depends on the cost of the information that the balancing needs:

- **On the prescribed bind-mounted setup, not really.** LPT needs every file size before it can assign work. On this file-sharing layer the parallel size scan costs **0.46 s**, which is more than the 0.34 s it saves in the counting phase. So v2 ends at 6.15 s against 6.04 s for v1 ($S_p$ 3.90 vs 3.97). The difference is 1.9 %, close to the run-to-run variation, so the honest reading is that the two versions *tie* on this setup.
- **On a filesystem where metadata is cheap, LPT wins clearly.** In the control run the scan takes 0.019 s. LPT then reduces the total time from 3.18 s to **2.69 s (−15.5 %)**, raising $S_7$ from 3.32 to **3.93** and $E_7$ from 0.47 to **0.56**.

  This gain is larger than the 5–9 % LI reduction alone would predict. A plausible extra reason is that LPT gives every rank a similar mix of large and small files, which keeps 7 processes competing for 4 cores evenly throughout the run.
- **The dynamic master–worker achieved the best balance and the worst time** in both settings: 7.04 s (bind mount) and 3.23 s (native). Two reasons:
  - rank 0 only coordinates, so 6 of the 7 processes count words. The expected penalty is 7/6 ≈ 1.17, and 2.69 s × 7/6 ≈ 3.14 s, close to the measured 3.23 s.
  - it needs 3000 request/reply round trips, plus a full size scan to hand out the files largest-first.

  With only 7 processes and fine-grained tasks, giving up a whole core to scheduling costs more than the balance it buys.

### c. Limitations: What limitations affected your experiment, and how much do they weaken your conclusions?

1. **Single node with Hyper-Threading.** All "processes" share 4 physical cores, the caches and the memory bus of one machine, and MPI messages travel through shared memory, not a network. Absolute speedups and efficiencies are therefore specific to this machine and would be different on a real cluster, where communication is more expensive and each rank has its own core and disk. The *relative* comparison between v1 and v2 is not affected, because both ran on identical conditions.
2. **Virtualised file access.** The bind mount crosses the Docker Desktop / WSL2 boundary. It is about 2.3× slower than the native filesystem for the baseline and very slow for metadata (≈ 0.75 ms per `stat`). This changes the conclusion about v2, which is why the control run on the native filesystem was added. The two settings agree on *why* things happen, but differ in *which* version is faster. The verdict on LPT is therefore conditional on the cost of file metadata, and is not universal.
3. **Small number of runs and shared host.** Three runs per version on a desktop with background processes give standard deviations of roughly 0.07–0.2 s. That is enough to separate the baseline from MPI, LPT from dynamic, and v1 from LPT on the native filesystem (≈ 0.5 s difference). It is *not* enough to separate v1 from LPT on the bind mount (≈ 0.1 s difference). The choice of p = 7 itself came from a noisy detector: a second execution suggested N = 4, inside the same 4–8 plateau.
4. **Warm file cache.** A warm-up run was performed, so all measured runs read the corpus from the OS page cache. This keeps the runs comparable, but it does not represent a cold first read from disk, where I/O would weigh more and the speedup would be lower.
5. **Single process count and one dataset.** The comparison uses only p = 7 and the seed-42 corpus, where only 10 % of the files are large. With fewer, larger files, or with smaller `p`, block partitioning could become much more imbalanced, and LPT's advantage would grow. These conclusions should not be extrapolated beyond that.
6. **Timer scope.** The baseline times only its counting phase, while the MPI times include the query read, the communications and the merge. The interpreter and `mpiexec` start-up time is excluded from both. The extra steps inside the MPI timer cost ≈ 0.025 s in v1, so this difference in scope does not affect the comparison.

Overall these limitations do not change the qualitative conclusions:

- MPI gives a ≈ 4× speedup on this machine;
- v1 has a real, size-driven imbalance;
- size-aware distribution removes it.

They do weaken any claim about the *magnitude* of the v2 gain, which ranges from none on the bind mount to 15 % on the native filesystem.

## 6. Conclusions

For this problem, a **size-weighted static distribution (LPT, `mpi2.py`)** is the recommended strategy, with one condition: file sizes must be cheap to obtain.

LPT fixes the actual cause of the imbalance, the heterogeneous file sizes. It lowers LI from 1.054–1.094 (v1) to 1.014–1.017 while keeping the low-overhead communication pattern of v1 (one `bcast`, one `scatter`, one `gather`, and all 7 ranks counting). On a filesystem with cheap metadata, that turned into the best result measured: $T_7$ = 2.69 s, **$S_7$ = 3.93, $E_7$ = 0.56**, against 3.18 s, $S_7$ = 3.32 and $E_7$ = 0.47 for the static blocks of v1, a 15.5 % reduction. On the prescribed Docker bind mount, collecting the 3000 sizes cost 0.46 s and cancelled the 0.34 s gained in the counting phase, so LPT tied with v1 ($S_7$ = 3.90 vs 3.97, $E_7$ = 0.56 vs 0.57). In that environment the simpler block distribution is equally good.

The dynamic master–worker pool should not be used at this scale. It produced the best balance (LI = 1.001) but the worst time ($S_7$ = 3.41, $E_7$ = 0.49), because dedicating one of only 7 processes to scheduling, plus 3000 messages, costs more than the remaining imbalance it removes.

In all cases the main limit on efficiency was not the MPI design. It was the hardware: 4 physical cores shared by 7 processes, and a shared I/O path. The communication overhead stayed below 1 % of the run time.

## Appendix – Scaling check with the provided `run_all.sh`

`run_all.sh` was also executed unchanged. It regenerates the same seed-42 dataset, runs the baseline once and runs both versions 3 times each with `np` = 2, 4 and 8, all on the bind mount. All 18 MPI runs reported `MATCH`.

The CSV it produces has no baseline row: `baseline_secuencial.py` prints `Tiempo de ejecución` rather than the `EXECUTION_TIME=` line that the script looks for. Speedups below therefore use $T_{seq}$ = 23.978 s from Section 4.a. Raw data: `experiment_results/run_all/`.

| np | v1 $T$ (s) | v1 $S_p$ | v1 $E_p$ | v1 LI | v2 (LPT) $T$ (s) | v2 $S_p$ | v2 $E_p$ | v2 LI |
|---|---|---|---|---|---|---|---|---|
| 2 | 10.722 | 2.236 | 1.118 | 1.0157 | 11.139 | 2.153 | 1.076 | 1.0019 |
| 4 | 7.032 | 3.410 | 0.852 | 1.0518 | 6.978 | 3.436 | 0.859 | 1.0055 |
| 8 | 6.220 | 3.855 | 0.482 | 1.0715 | **5.807** | **4.129** | **0.516** | 1.0170 |

These runs confirm the trends from the main experiment:

- **Imbalance grows with p.** With smaller blocks, a few large files make a bigger difference; v1's LI goes 1.016 → 1.052 → 1.072.
- **LPT keeps LI ≤ 1.017 at every p.** Its benefit outweighs the size-scan cost once the imbalance is large enough. At np = 8 it is 6.6 % faster than v1, while at np = 2 the scan cost dominates.
- **Scaling flattens after 4 processes,** the number of physical cores. Efficiency drops from ≈ 0.85 at np = 4 to ≈ 0.5 at np = 8.
- **Efficiency slightly above 1 at np = 2** comes from the slow bind mount. While one process waits on file access, the other uses the CPU, so part of the I/O latency hidden in the sequential run overlaps. Measured against the single baseline run of `run_all.sh` (22.58 s), $E_2$ is 1.05 for v1.

## Repository contents

| File | Description |
|---|---|
| `generator.py`, `baseline_secuencial.py`, `run_all.sh` | Provided by the course, **unchanged** |
| `mpi1.py` | MPI version 1: `bcast` + static block `scatter` + `gather` |
| `mpi2.py` | MPI version 2: LPT size-weighted distribution (default) and `--strategy dynamic` master–worker |
| `detectar_procesos_mpi.py` | Course script used to choose `p` (from `augustosalazar/mpi_taller_sudoku`, unchanged) |
| `run_experiments.sh` | Runs the experimental plan with a fixed `p` |
| `summarize_results.py` | Parses the logs and computes $T_p$, $S_p$, $E_p$, LI and spread |
| `experiment_results/` | Raw logs and generated summaries of every execution reported here (`logs/`: main experiment, `logs_native/`: control run, `run_all/`: output of `run_all.sh`) |
