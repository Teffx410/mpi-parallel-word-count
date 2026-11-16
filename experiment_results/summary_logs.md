## Sequential baseline

| Run | Time (s) |
|---|---|
| 1 | 23.821 |
| 2 | 24.006 |
| 3 | 24.107 |
| **Mean T_seq** | **23.978** |
| Std. dev. | 0.145 |

Top 10 (baseline): a 785774, para 392156, sus 228913, otros 105530, ante 99832, unos 88794, otra 83901, vosotros 61617, mios 58420, tuya 56635

## MPI v1 - static blocks (mpi1.py)

| Run | Total time (s) | max t_i (s) | mean t_i (s) | min t_i (s) | LI | Rel. spread | Top 10 = baseline |
|---|---|---|---|---|---|---|---|
| 1 | 6.110 | 6.083 | 5.809 | 5.479 | 1.0472 | 0.1040 | yes |
| 2 | 6.026 | 5.997 | 5.623 | 5.080 | 1.0664 | 0.1631 | yes |
| 3 | 5.970 | 5.950 | 5.676 | 5.399 | 1.0483 | 0.0971 | yes |
| **Mean** | **6.035** | | | | **1.0540** | **0.1214** | |

T_p = 6.035 s, S_p = 3.973, E_p = 0.568 (p = 7)

Mean overhead outside counting (total - max t_i): 0.025 s

Per-process local times t_i (s), files and MB per rank:

| Rank | Role | Files | MB | t_i run 1 | t_i run 2 | t_i run 3 | mean t_i |
|---|---|---|---|---|---|---|---|
| 0 | worker | 429 | 42.0 | 6.083 | 5.943 | 5.897 | 5.974 |
| 1 | worker | 429 | 35.7 | 5.479 | 5.080 | 5.399 | 5.319 |
| 2 | worker | 429 | 42.6 | 6.079 | 5.997 | 5.950 | 6.009 |
| 3 | worker | 429 | 38.0 | 5.690 | 5.527 | 5.515 | 5.578 |
| 4 | worker | 428 | 38.2 | 5.633 | 5.297 | 5.567 | 5.499 |
| 5 | worker | 428 | 39.5 | 5.836 | 5.733 | 5.683 | 5.751 |
| 6 | worker | 428 | 39.1 | 5.861 | 5.788 | 5.721 | 5.790 |

## MPI v2 - LPT size-weighted (mpi2.py)

| Run | Total time (s) | max t_i (s) | mean t_i (s) | min t_i (s) | LI | Rel. spread | Top 10 = baseline |
|---|---|---|---|---|---|---|---|
| 1 | 6.124 | 5.647 | 5.565 | 5.479 | 1.0147 | 0.0301 | yes |
| 2 | 6.074 | 5.584 | 5.508 | 5.432 | 1.0137 | 0.0276 | yes |
| 3 | 6.255 | 5.790 | 5.715 | 5.628 | 1.0130 | 0.0282 | yes |
| **Mean** | **6.151** | | | | **1.0138** | **0.0286** | |

T_p = 6.151 s, S_p = 3.898, E_p = 0.557 (p = 7)

Mean size-scan time (stat of all files before balancing): 0.460 s

Mean overhead outside counting (total - max t_i): 0.478 s

Per-process local times t_i (s), files and MB per rank:

| Rank | Role | Files | MB | t_i run 1 | t_i run 2 | t_i run 3 | mean t_i |
|---|---|---|---|---|---|---|---|
| 0 | worker | 429 | 39.3 | 5.647 | 5.584 | 5.790 | 5.673 |
| 1 | worker | 428 | 39.3 | 5.599 | 5.492 | 5.745 | 5.612 |
| 2 | worker | 429 | 39.3 | 5.518 | 5.532 | 5.740 | 5.597 |
| 3 | worker | 429 | 39.3 | 5.536 | 5.432 | 5.628 | 5.532 |
| 4 | worker | 427 | 39.3 | 5.597 | 5.513 | 5.695 | 5.602 |
| 5 | worker | 429 | 39.3 | 5.578 | 5.469 | 5.715 | 5.587 |
| 6 | worker | 429 | 39.3 | 5.479 | 5.537 | 5.692 | 5.569 |

## MPI v2 variant - dynamic master-worker (mpi2.py --strategy dynamic)

| Run | Total time (s) | max t_i (s) | mean t_i (s) | min t_i (s) | LI | Rel. spread | Top 10 = baseline |
|---|---|---|---|---|---|---|---|
| 1 | 7.041 | 6.503 | 6.499 | 6.492 | 1.0006 | 0.0018 | yes |
| 2 | 7.162 | 6.651 | 6.643 | 6.634 | 1.0012 | 0.0026 | yes |
| 3 | 6.919 | 6.393 | 6.387 | 6.373 | 1.0009 | 0.0032 | yes |
| **Mean** | **7.041** | | | | **1.0009** | **0.0025** | |

T_p = 7.041 s, S_p = 3.405, E_p = 0.486 (p = 7)

Mean size-scan time (stat of all files before balancing): 0.464 s

Mean overhead outside counting (total - max t_i): 0.525 s

Per-process local times t_i (s), files and MB per rank:

| Rank | Role | Files | MB | t_i run 1 | t_i run 2 | t_i run 3 | mean t_i |
|---|---|---|---|---|---|---|---|
| 0 | master | 0 | 0.0 | 6.559 | 6.702 | 6.448 | 6.570 |
| 1 | worker | 499/500/503 | 45.7/46.1/46.6 | 6.503 | 6.651 | 6.373 | 6.509 |
| 2 | worker | 500/501 | 45.6/46.4 | 6.500 | 6.644 | 6.389 | 6.511 |
| 3 | worker | 497/498/499 | 45.1/45.5/45.6 | 6.499 | 6.643 | 6.393 | 6.512 |
| 4 | worker | 497/498/500 | 45.5/45.8/46.3 | 6.502 | 6.634 | 6.390 | 6.509 |
| 5 | worker | 498/501/506 | 45.2/45.5/46.8 | 6.501 | 6.645 | 6.388 | 6.512 |
| 6 | worker | 500/501 | 45.8/46.0 | 6.492 | 6.641 | 6.390 | 6.507 |

## Comparison

| Version | p | T (s) | Speedup S_p | Efficiency E_p | LI | Rel. spread | Correct |
|---|---|---|---|---|---|---|---|
| Sequential baseline | 1 | 23.978 | 1.000 | 1.000 | - | - | reference |
| MPI v1 - static blocks (mpi1.py) | 7 | 6.035 | 3.973 | 0.568 | 1.0540 | 0.1214 | yes |
| MPI v2 - LPT size-weighted (mpi2.py) | 7 | 6.151 | 3.898 | 0.557 | 1.0138 | 0.0286 | yes |
| MPI v2 variant - dynamic master-worker (mpi2.py --strategy dynamic) | 7 | 7.041 | 3.405 | 0.486 | 1.0009 | 0.0025 | yes |
