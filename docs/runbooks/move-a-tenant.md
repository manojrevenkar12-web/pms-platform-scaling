# Runbook: move an account to another shard

When: a shard is running hot, an account has grown large enough to deserve its own shard, or a
shard is being drained for maintenance.

## Before

- Target shard is registered (`reshard init` adds shards) and has free headroom.
- Source and target run PostgreSQL 15+ with `wal_level = logical` and spare
  `max_replication_slots`, `max_wal_senders`, `max_logical_replication_workers`.
- The target can reach the source with the shard's replication connection string.
- For accounts with more than ~100k reservations, expect the write pause to grow with the
  checksum (reports/BENCHMARK.md). Rehearse once with the checksum on; then you may move with
  `mover.skip_checksum: true`.

## Move

    reshard move -control "$PMS_CONTROL_DSN" -account ACC -to shard-3 -id ACC-to-3

It logs one JSON line per step and prints the result (write pause, step timings). During the
freeze, the account's writes wait (up to `router.max_wait`) and then continue on the target;
other accounts are not paused.

## Watch

- `pms_router_retries_total{reason="frozen"}` rises during the pause and stops after release.
- `pms_http_requests_total{route="book",status="Service Unavailable"}` should stay flat; a rise
  means the pause exceeded `router.max_wait`.

## If it stops

Re-run the same command with the same `-id`: the mover resumes from the last recorded step.
`reshard status -account ACC` shows the step and the last error. See stuck-move.md.
