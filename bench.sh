#!/usr/bin/env bash
set -euo pipefail

# ── Crawler Scale Benchmark ─────────────────────────────────────────
# Usage:  ./bench.sh <N>        (N must evenly divide 256)
# Example: ./bench.sh 32
#
# What it does:
#   1. Validates N
#   2. Patches control.yaml and ingest.yaml with shards_per_* = 256/N
#   3. Starts Postgres ONLY, waits until ready
#   4. Initializes schema + seeds 256k URLs (from host via port 5433)
#   5. Starts scheduler_control, scheduler_ingest, crawler
#   6. Prints monitor command
# ─────────────────────────────────────────────────────────────────────

N="${1:?Usage: $0 <NUM_WORKERS>  (must evenly divide 256)}"

if (( 256 % N != 0 )); then
  echo "ERROR: N=$N does not evenly divide 256."
  echo "Valid values: 1 2 4 8 16 32 64 128 256"
  exit 1
fi

SHARDS_PER_WORKER=$(( 256 / N ))
echo "=== Benchmark: N=$N workers, $SHARDS_PER_WORKER shards/worker ==="

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CONTROL_YAML="$SCRIPT_DIR/containers/scheduler_control/config/control.yaml"
INGEST_YAML="$SCRIPT_DIR/containers/scheduler_ingest/config/ingest.yaml"

# ── 1. Patch control.yaml ────────────────────────────────────────────
echo "Patching $CONTROL_YAML ..."

# shards_per_offerer
sed -i.bak -E "s/^(  shards_per_offerer:).*/\1 $SHARDS_PER_WORKER/" "$CONTROL_YAML"
# id_end (offerer range is 0..N-1)
sed -i.bak -E "s/^(  id_end:).*/\1 $((N - 1))/" "$CONTROL_YAML"

# ── 2. Patch ingest.yaml ────────────────────────────────────────────
echo "Patching $INGEST_YAML ..."

sed -i.bak -E "s/^(  shards_per_ingestor:).*/\1 $SHARDS_PER_WORKER/" "$INGEST_YAML"

# Clean up .bak files from sed -i
rm -f "$CONTROL_YAML.bak" "$INGEST_YAML.bak"

echo "  control.yaml: shards_per_offerer=$SHARDS_PER_WORKER, id_end=$((N - 1))"
echo "  ingest.yaml:  shards_per_ingestor=$SHARDS_PER_WORKER"

# ── 3. Clean old IPC state ──────────────────────────────────────────
echo "Cleaning IPC directories ..."
rm -rf "$SCRIPT_DIR/ipc/url_queue" "$SCRIPT_DIR/ipc/crawl_result" "$SCRIPT_DIR/ipc/progress" "$SCRIPT_DIR/ipc/stats"

# ── 4. Build images & start Postgres only ──────────────────────────
export NUM_WORKERS="$N"
export INGEST_DRY_RUN=1

cd "$SCRIPT_DIR"
echo "Tearing down previous stack ..."
docker compose down --remove-orphans 2>/dev/null || true

echo "Building images ..."
docker compose build

echo "Starting Postgres ..."
docker compose up -d postgres

# ── 5. Wait for Postgres ─────────────────────────────────────────────
echo "Waiting for Postgres to be ready ..."
for i in $(seq 1 60); do
  if docker exec postgres pg_isready -U crawler -d crawlerdb >/dev/null 2>&1; then
    echo "  Postgres ready after ${i}s"
    break
  fi
  if [ "$i" -eq 60 ]; then
    echo "ERROR: Postgres did not become ready within 60s"
    exit 1
  fi
  sleep 1
done

# ── 6. Schema init ────────────────────────────────────────────────────
echo "Initializing schema ..."
INIT_SCHEMA_DSN="postgresql+psycopg2://crawler:crawler@127.0.0.1:5433/crawlerdb" \
  uv run --group seed python init_schema.py 2>&1 | tail -1

# ── 7. Seed or reset should_crawl ────────────────────────────────────
# Check if shard 000 already has rows; if yes, just reset should_crawl.
# If empty, run a full seed.
ROW_COUNT=$(docker exec postgres psql -U crawler -d crawlerdb -tAq \
  -c "SELECT COUNT(*) FROM url_state_current_000;")

if [ "${ROW_COUNT:-0}" -gt 0 ]; then
  echo "DB already has data (${ROW_COUNT} rows in shard 000). Resetting should_crawl=TRUE ..."
  docker exec postgres psql -U crawler -d crawlerdb -q -c \
    "DO \$\$ BEGIN FOR i IN 0..255 LOOP
       EXECUTE format('UPDATE url_state_current_%s SET should_crawl=TRUE', lpad(i::text,3,'0'));
     END LOOP; END \$\$;"
  echo "  Done."
else
  echo "DB is empty. Seeding benchmark data (256 shards × 1000 URLs) ..."
  SEED_POSTGRES_DSN="postgresql+psycopg2://crawler:crawler@127.0.0.1:5433/crawlerdb" \
    uv run --group seed python seed_bench.py 2>&1
fi

# ── 8. Start remaining services ───────────────────────────────────────
echo "Starting scheduler_control, scheduler_ingest, crawler ..."
docker compose up -d scheduler_control scheduler_ingest crawler

# ── 9. Done ──────────────────────────────────────────────────────────
echo ""
echo "========================================="
echo " Benchmark running: $N workers"
echo " Ingestor: DRY-RUN (no DB writes)"
echo "========================================="
echo ""
echo "Monitor IPC throughput (logs to bench_logs/):"
echo "  python monitor_ipc.py --ipc-root ./ipc --interval 2 --log bench_logs/ipc_N${N}.jsonl"
echo ""
echo "Watch crawler logs:"
echo "  docker logs -f crawler 2>&1 | grep -E 'Download (started|ended)'"
echo ""
echo "Stop:"
echo "  docker compose down"
