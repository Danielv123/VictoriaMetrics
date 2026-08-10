---
name: verify
description: Verify vmsingle storage and query changes through its live HTTP API.
---

# Verify vmsingle through the HTTP API

1. Build the application binary. On Windows, build and run it inside WSL so graceful `SIGINT` shutdown works:

   ```bash
   make victoria-metrics-race
   ```

2. Use an isolated storage directory under `$CLAUDE_JOB_DIR/tmp` and an available loopback port. Start `bin/victoria-metrics-race` with:

   ```text
   -storageDataPath=<temp>/storage
   -retentionPeriod=100y
   -storage.finalDedupScheduleCheckInterval=1h
   -httpListenAddr=127.0.0.1:<port>
   -graphiteListenAddr=127.0.0.1:0
   -opentsdbListenAddr=127.0.0.1:0
   ```

3. Drive behavior through the public/internal HTTP endpoints:
   - import Prometheus rows: `POST /api/v1/import/prometheus`
   - make writes searchable: `GET /internal/force_flush`
   - query MetricsQL: `GET /api/v1/query`
   - materialize storage merges: `GET /internal/force_merge`
   - inspect physically stored samples: `GET /api/v1/export?reduce_mem_usage=1`

4. For changes involving startup storage policy, stop the process with `SIGINT`, wait for it to exit, and restart the same storage path with the new flags. Capture the API response before and after `force_merge`, and include at least one adjacent/negative query probe.

## Windows gotcha

Native Windows app tests cannot gracefully stop vmsingle because `os.Interrupt` is unsupported. Running the Linux race binary and verification flow inside WSL avoids this. When invoking WSL from the Windows host, use the worktree path under `/mnt/c/...`; do not run from the original checkout.
