#!/usr/bin/env bash
# Start the AgentTx Postgres (data dir lives OUTSIDE the repo; never versioned).
set -e
source /home/lzq/miniconda3/etc/profile.d/conda.sh; conda activate agenttx
export PGDATA=/home/lzq/agenttx_pgdata PGPORT=54329
pg_ctl -D "$PGDATA" -o "-p $PGPORT -k /tmp" -l /tmp/agenttx_pg.log start
pg_isready -h /tmp -p $PGPORT
