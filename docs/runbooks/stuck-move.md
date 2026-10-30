# Runbook: a move failed or hangs

1. `reshard status -account ACC` and read `step` and `error`.
2. **Not yet flipped** (planned ... verified): either resume (`reshard move ... -id ID`) or abort
   (`reshard abort -id ID`). Abort drops the subscription and publication, deletes the copy on
   the target and unfreezes the account on the source. Writes resume at once.
3. **Flipped or later**: the target owns the account. Do not abort; resume to finish the release
   and cleanup.
4. **Verification mismatch** (`tenant data differs`): the mover has already aborted; the account
   is on the source and writable, and the target's partial copy is gone. The error shows the
   per-table hashes of both sides. A mismatch means something wrote the account's rows on the
   target outside replication (a script, a migration run on the wrong shard): find it before
   moving again.
5. **Waiting in copied/caught_up for long**: check `pg_stat_subscription` on the target and
   `pg_replication_slots` on the source. A missing slot or a worker restarting in the target's
   log is the usual cause. Waiting steps time out after `mover.step_timeout`.
6. Leftovers after a crashed abort: `pg_publication` on the source and `pg_subscription` on the
   target named `mv_<move id>`. `reshard abort` is idempotent: run it again.
