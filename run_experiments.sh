#!/bin/sh
# Experimental plan of this lab (Parts 0, 2 and 4) with a single process
# count P, the value recommended by detectar_procesos_mpi.py.
#
# Run from the repository folder (the dataset must already exist):
#   docker run --rm -v "$(pwd)":/app augustosalazar/slim-mpi:2 sh /app/run_experiments.sh
#   docker run --rm -v "%cd%:/app"  augustosalazar/slim-mpi:2 sh /app/run_experiments.sh
#
# Optional variables: P (default 7), RUNS (default 3), APP_DIR (folder with
# the scripts and dataset/, default /app) and OUT_DIR (log folder).
# Logs are written to experiment_results/logs; summarise them with
#   python summarize_results.py
#
# Supplementary run with the corpus on the container's native filesystem
# instead of the Windows/macOS bind mount (isolates the I/O effect):
#   docker run --rm -v "$(pwd)":/app augustosalazar/slim-mpi:2 sh -c \
#     'cp -r /app /work && APP_DIR=/work OUT_DIR=/app/experiment_results/logs_native sh /work/run_experiments.sh'

set -eu

APP_DIR="${APP_DIR:-/app}"
OUT_DIR="${OUT_DIR:-$APP_DIR/experiment_results/logs}"
P="${P:-7}"
RUNS="${RUNS:-3}"
MPI="mpiexec --allow-run-as-root --oversubscribe -np $P"

if [ ! -d "$APP_DIR/dataset" ]; then
  echo "dataset/ not found: run generator.py first"
  exit 1
fi

rm -rf "$OUT_DIR"
mkdir -p "$OUT_DIR"

echo "P=$P RUNS=$RUNS nproc=$(nproc)" | tee "$OUT_DIR/environment.txt"
python --version >> "$OUT_DIR/environment.txt" 2>&1
python -c "import mpi4py; print('mpi4py', mpi4py.__version__)" >> "$OUT_DIR/environment.txt"
mpiexec --version | head -n 1 >> "$OUT_DIR/environment.txt"

echo "== Warm-up run (not measured): loads the corpus into the OS file cache"
python "$APP_DIR/baseline_secuencial.py" > "$OUT_DIR/warmup_baseline.log"

for RUN in $(seq 1 "$RUNS"); do
  echo "== Sequential baseline, run $RUN"
  python "$APP_DIR/baseline_secuencial.py" > "$OUT_DIR/baseline_run$RUN.log"
done

for RUN in $(seq 1 "$RUNS"); do
  echo "== mpi1.py (static blocks), p=$P, run $RUN"
  $MPI python "$APP_DIR/mpi1.py" > "$OUT_DIR/mpi1_np${P}_run$RUN.log"
done

for RUN in $(seq 1 "$RUNS"); do
  echo "== mpi2.py (LPT, size-weighted), p=$P, run $RUN"
  $MPI python "$APP_DIR/mpi2.py" > "$OUT_DIR/mpi2_np${P}_run$RUN.log"
done

for RUN in $(seq 1 "$RUNS"); do
  echo "== mpi2.py --strategy dynamic (master-worker), p=$P, run $RUN"
  $MPI python "$APP_DIR/mpi2.py" --strategy dynamic > "$OUT_DIR/mpi2dyn_np${P}_run$RUN.log"
done

echo "Done. Logs in $OUT_DIR"
