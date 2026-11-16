## Sequential baseline

| Run | Time (s) |
|---|---|
| 1 | 10.450 |
| 2 | 10.411 |
| 3 | 10.829 |
| **Mean T_seq** | **10.564** |
| Std. dev. | 0.231 |

Top 10 (baseline): a 785774, para 392156, sus 228913, otros 105530, ante 99832, unos 88794, otra 83901, vosotros 61617, mios 58420, tuya 56635

## MPI v1 - static blocks (mpi1.py)

| Run | Total time (s) | max t_i (s) | mean t_i (s) | min t_i (s) | LI | Rel. spread | Top 10 = baseline |
|---|---|---|---|---|---|---|---|
| 1 | 3.123 | 3.091 | 2.836 | 2.571 | 1.0898 | 0.1834 | yes |
| 2 | 3.118 | 3.095 | 2.847 | 2.623 | 1.0868 | 0.1657 | yes |
| 3 | 3.305 | 3.291 | 2.981 | 2.761 | 1.1041 | 0.1777 | yes |
| **Mean** | **3.182** | | | | **1.0936** | **0.1756** | |

T_p = 3.182 s, S_p = 3.320, E_p = 0.474 (p = 7)

Mean overhead outside counting (total - max t_i): 0.024 s

Per-process local times t_i (s), files and MB per rank:

| Rank | Role | Files | MB | t_i run 1 | t_i run 2 | t_i run 3 | mean t_i |
|---|---|---|---|---|---|---|---|
| 0 | worker | 429 | 42.0 | 3.045 | 3.003 | 3.248 | 3.099 |
| 1 | worker | 429 | 35.7 | 2.571 | 2.623 | 2.802 | 2.665 |
| 2 | worker | 429 | 42.6 | 3.091 | 3.095 | 3.291 | 3.159 |
| 3 | worker | 429 | 38.0 | 2.763 | 2.768 | 2.795 | 2.775 |
| 4 | worker | 428 | 38.2 | 2.819 | 2.699 | 2.761 | 2.760 |
| 5 | worker | 428 | 39.5 | 2.783 | 2.907 | 2.914 | 2.868 |
| 6 | worker | 428 | 39.1 | 2.783 | 2.837 | 3.052 | 2.891 |

## MPI v2 - LPT size-weighted (mpi2.py)

| Run | Total time (s) | max t_i (s) | mean t_i (s) | min t_i (s) | LI | Rel. spread | Top 10 = baseline |
|---|---|---|---|---|---|---|---|
| 1 | 2.748 | 2.699 | 2.670 | 2.614 | 1.0108 | 0.0317 | yes |
| 2 | 2.583 | 2.559 | 2.522 | 2.467 | 1.0147 | 0.0367 | yes |
| 3 | 2.732 | 2.709 | 2.641 | 2.569 | 1.0259 | 0.0531 | yes |
| **Mean** | **2.688** | | | | **1.0171** | **0.0405** | |

T_p = 2.688 s, S_p = 3.930, E_p = 0.561 (p = 7)

Mean size-scan time (stat of all files before balancing): 0.019 s

Mean overhead outside counting (total - max t_i): 0.032 s

Per-process local times t_i (s), files and MB per rank:

| Rank | Role | Files | MB | t_i run 1 | t_i run 2 | t_i run 3 | mean t_i |
|---|---|---|---|---|---|---|---|
| 0 | worker | 429 | 39.3 | 2.688 | 2.559 | 2.709 | 2.652 |
| 1 | worker | 428 | 39.3 | 2.696 | 2.508 | 2.681 | 2.628 |
| 2 | worker | 429 | 39.3 | 2.682 | 2.467 | 2.666 | 2.605 |
| 3 | worker | 429 | 39.3 | 2.646 | 2.520 | 2.618 | 2.595 |
| 4 | worker | 427 | 39.3 | 2.666 | 2.542 | 2.642 | 2.616 |
| 5 | worker | 429 | 39.3 | 2.614 | 2.511 | 2.569 | 2.565 |
| 6 | worker | 429 | 39.3 | 2.699 | 2.548 | 2.600 | 2.616 |

## MPI v2 variant - dynamic master-worker (mpi2.py --strategy dynamic)

| Run | Total time (s) | max t_i (s) | mean t_i (s) | min t_i (s) | LI | Rel. spread | Top 10 = baseline |
|---|---|---|---|---|---|---|---|
| 1 | 3.142 | 3.049 | 3.040 | 3.031 | 1.0030 | 0.0058 | yes |
| 2 | 3.094 | 3.036 | 3.030 | 3.023 | 1.0021 | 0.0042 | yes |
| 3 | 3.450 | 3.289 | 3.271 | 3.256 | 1.0055 | 0.0102 | yes |
| **Mean** | **3.229** | | | | **1.0035** | **0.0067** | |

T_p = 3.229 s, S_p = 3.272, E_p = 0.467 (p = 7)

Mean size-scan time (stat of all files before balancing): 0.016 s

Mean overhead outside counting (total - max t_i): 0.104 s

Per-process local times t_i (s), files and MB per rank:

| Rank | Role | Files | MB | t_i run 1 | t_i run 2 | t_i run 3 | mean t_i |
|---|---|---|---|---|---|---|---|
| 0 | master | 0 | 0.0 | 3.121 | 3.075 | 3.431 | 3.209 |
| 1 | worker | 496/503 | 45.8/45.9/46.3 | 3.037 | 3.023 | 3.276 | 3.112 |
| 2 | worker | 499/504/512 | 45.9/46.1/46.4 | 3.042 | 3.028 | 3.256 | 3.109 |
| 3 | worker | 493/497/506 | 45.1/45.8/46.0 | 3.046 | 3.035 | 3.284 | 3.122 |
| 4 | worker | 484/497/500 | 44.7/45.6/46.2 | 3.049 | 3.031 | 3.256 | 3.112 |
| 5 | worker | 489/503/511 | 45.3/45.9/47.1 | 3.033 | 3.026 | 3.289 | 3.116 |
| 6 | worker | 499/500/504 | 45.2/45.7/46.0 | 3.031 | 3.036 | 3.265 | 3.111 |

## Comparison

| Version | p | T (s) | Speedup S_p | Efficiency E_p | LI | Rel. spread | Correct |
|---|---|---|---|---|---|---|---|
| Sequential baseline | 1 | 10.564 | 1.000 | 1.000 | - | - | reference |
| MPI v1 - static blocks (mpi1.py) | 7 | 3.182 | 3.320 | 0.474 | 1.0936 | 0.1756 | yes |
| MPI v2 - LPT size-weighted (mpi2.py) | 7 | 2.688 | 3.930 | 0.561 | 1.0171 | 0.0405 | yes |
| MPI v2 variant - dynamic master-worker (mpi2.py --strategy dynamic) | 7 | 3.229 | 3.272 | 0.467 | 1.0035 | 0.0067 | yes |
