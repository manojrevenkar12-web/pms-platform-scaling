# 1. Scale writes by sharding on the account, and move accounts online

Status: accepted

## Context

A hotel PMS writes far more than it can spread over read replicas: every reservation, amendment,
cancellation and inventory change is a write, and the busiest moments (a big group, a flash sale,
a channel manager catching up) are writes to the same few rows. One primary PostgreSQL database
eventually runs out of headroom: CPU, WAL throughput, vacuum, connection count.

Almost every write belongs to one account (the hotel group that owns the properties), and
almost no transaction spans accounts. That makes the account a natural shard key.

## Decision

- **One directory, many shards.** A small control database maps account -> shard (with an epoch
  bumped on every move). Each shard is an ordinary PostgreSQL primary with the same schema. Every
  tenant table has `account_id` first in its primary key.
- **The shard has the last word.** Every write transaction starts by taking the account's fence
  lock (a shared advisory lock) and reading the account's fence row *on the shard*. Frozen means
  "being moved, wait"; `moved_to` means "gone, re-read the directory". A stale directory cache
  can therefore cost a retry, never a write on the wrong shard.
- **Online moves with logical replication.** An account is moved with a publication that has a
  row filter on `account_id` (PostgreSQL 15+), so only that account's rows travel; the rest of the
  source shard is untouched. The mover copies, streams, freezes the account, waits for the freeze
  itself to arrive on the target (commit order means everything before it has), compares
  checksums, flips the directory and releases. Every step is recorded and idempotent, so a crashed
  mover is simply run again; a move that has not flipped can be aborted.
- **Advisory lock, not a row lock, for the freeze.** With a row lock, new shared lockers can keep
  joining while the freeze waits, and a busy account could starve its own move. PostgreSQL queues
  heavyweight lock requests, so writers that arrive after the freeze request wait behind it.

## Measured (reports/BENCHMARK.md)

Two shards on one 2-CPU machine, durable commits, 8 accounts writing continuously, two of them
moved back and forth 10 times: 39,002 acknowledged writes, 0 lost, 0 duplicated, 0 failed;
write pause p50 205 ms, max 546 ms; the other accounts kept writing throughout (their p99 rose
from about 35 ms to 45-130 ms, because copy and checksum compete for the same two CPUs).
Without load, the pause is about 20 ms plus the checksum, which grows with the account's size
(about 165 ms per 20,000 reservations here).

## Options considered

| | Fit for this problem | Why not (here) |
|---|---|---|
| **Citus** (row-based sharding by `account_id`, `isolate_tenant_to_new_shard`, `citus_move_shard_placement` with logical replication) | The closest match; online tenant isolation and shard moves are built in. | An extension on every node, a coordinator in the query path, and its own operational model. Not available on Amazon RDS/Aurora. A strong choice on self-managed or Azure. |
| **Aurora PostgreSQL Limitless Database** (sharded tables with a shard key, collocated and reference tables, distributed transaction routers) | Managed scale-out, with AWS splitting shards. | Limitless-specific table types and restrictions, cost model by capacity units, and shard placement is AWS's decision: we cannot pin one large hotel group or move it on our schedule. |
| **PgDog** (Rust proxy that routes by shard key; resharding by logical replication and COPY) | Same mechanisms as here, plus routing in a proxy instead of the application. | Resharding moves whole publications rather than one account; younger project. Worth tracking: if it adds per-tenant moves, the router here could be replaced by it. |
| **Multigres** (Supabase, Vitess-style) | The long-term "Vitess for Postgres" option. | v0.1 alpha in June 2026 is single-shard; sharding is still on the roadmap. |
| **Bigger primary + read replicas** | Simplest; buys time. | Does not scale writes, and every hot-row problem stays. |

The approach here is small enough to own: about 1,500 lines of Go, plain PostgreSQL features
(advisory locks, logical replication with row filters), no extension, and it runs on RDS or
Aurora PostgreSQL 15+ unchanged.

## Consequences

- Cross-account queries (reporting, search across a chain's partners) go to a separate read model
  fed from the shards, not to the shards themselves.
- Global uniqueness across accounts needs keys that are unique by construction (UUIDs here).
- Sequences and identity columns would not survive a move without care; tenant tables use UUIDs.
- The write pause grows with account size when the checksum is on. For the largest accounts, run
  a checksum-verified rehearsal move first, then move with `skip_checksum`.
- Schema changes must be applied to every shard; the control database records shard state, and
  migrations run shard by shard.
