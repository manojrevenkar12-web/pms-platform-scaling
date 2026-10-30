# 2. Split hot availability counters into slots

Status: accepted

## Context

Every booking for a room type on a night decrements the same counter. On a popular night all of
those bookings queue on one row lock, and with durable commits each holds it until its commit has
been flushed. Throughput on that night is then bounded by one row's commit rate, whatever the
size of the database.

## Decision

Store the counter as N slots whose remainders add up to the rooms left. A booking takes any slot
with rooms that is not locked (`FOR UPDATE SKIP LOCKED`), and only when every slot with rooms is
locked does it wait, walking the slots in a fixed order. After the wait PostgreSQL re-checks
`remaining > 0` on the new row version, so an emptied slot is skipped.

- No oversell: a slot only changes under its row lock and `CHECK (remaining >= 0)` stands guard.
- No false sold-out: "sold out" is returned only when a fresh snapshot shows zero rooms for the
  night, never because the free slots happened to be busy.
- No deadlocks by construction: bookings and cancellations lock (night, slot) in the same order.
  In the rare case a cycle still forms, PostgreSQL detects it and the router retries the
  transaction (a deadlock counter is exported; the benchmark saw none).
- Idempotent: a client retry with the same `Idempotency-Key` returns the same reservation, even
  when the retries race each other.

## Measured (reports/BENCHMARK.md)

One night, 16 writers, durable commits, 2 CPUs: 872 bookings/s with 1 slot (p99 80 ms, 89% of
bookings queued) and 1,717/s with 16 slots (p99 21 ms, none queued). With 64 writers the two
CPUs are the limit (p99 above 600 ms in every configuration), so more slots help less. The
sell-out race (400 rooms, 400 or 1,600 simultaneous requests) sold exactly 400 every time.

## Consequences

- Reading availability is a `sum()` over the slots of a night (cheap: the primary key covers it).
- The slot count is a setting (`booking.default_slots`); inventory can be re-sliced while a night
  is selling, and cancellations of older bookings still find a slot to give the room back to.
