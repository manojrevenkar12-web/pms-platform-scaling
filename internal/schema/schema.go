// Package schema holds the DDL for the control database and for every shard.
//
// Every shard table carries account_id and has it first in its primary key. That is what makes a
// tenant movable: logical replication can publish one tenant's rows with a row filter, and a row
// filter on UPDATE and DELETE is only allowed on columns of the replica identity (the primary key).
package schema

import (
	"context"
	"database/sql"
	"fmt"
)

// Control is the shard directory: which shard each tenant (account) lives on.
const Control = `
CREATE TABLE IF NOT EXISTS shards (
	shard_id   text PRIMARY KEY,
	dsn        text NOT NULL,
	repl_conn  text NOT NULL,          -- how another shard reaches this one for replication
	accepting  boolean NOT NULL DEFAULT true
);
CREATE TABLE IF NOT EXISTS tenants (
	account_id text PRIMARY KEY,
	shard_id   text NOT NULL REFERENCES shards(shard_id),
	epoch      bigint NOT NULL DEFAULT 1,  -- bumped on every move; caches compare it
	updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS moves (
	move_id     text PRIMARY KEY,
	account_id  text NOT NULL,
	source      text NOT NULL REFERENCES shards(shard_id),
	target      text NOT NULL REFERENCES shards(shard_id),
	step        text NOT NULL,
	started_at  timestamptz NOT NULL DEFAULT now(),
	updated_at  timestamptz NOT NULL DEFAULT now(),
	frozen_at   timestamptz,
	flipped_at  timestamptz,
	finished_at timestamptz,
	error       text,
	timings     jsonb NOT NULL DEFAULT '{}'
);
-- one unfinished move per tenant at a time
CREATE UNIQUE INDEX IF NOT EXISTS moves_one_active ON moves (account_id) WHERE finished_at IS NULL;
`

// Shard tables. The same schema on every shard.
const Shard = `
CREATE TABLE IF NOT EXISTS tenant_fence (
	account_id text PRIMARY KEY,
	frozen     boolean NOT NULL DEFAULT false,
	moved_to   text,                         -- set on the old shard once a move is done
	epoch      bigint NOT NULL DEFAULT 1
);
CREATE TABLE IF NOT EXISTS properties (
	account_id  text NOT NULL,
	property_id text NOT NULL,
	name        text NOT NULL,
	PRIMARY KEY (account_id, property_id)
);
CREATE TABLE IF NOT EXISTS availability (
	account_id  text NOT NULL,
	property_id text NOT NULL,
	room_type   text NOT NULL,
	night       date NOT NULL,
	slot        smallint NOT NULL,
	remaining   integer NOT NULL CHECK (remaining >= 0),
	PRIMARY KEY (account_id, property_id, room_type, night, slot)
);
CREATE TABLE IF NOT EXISTS reservations (
	account_id     text NOT NULL,
	reservation_id uuid NOT NULL,
	property_id    text NOT NULL,
	room_type      text NOT NULL,
	arrival        date NOT NULL,
	departure      date NOT NULL CHECK (departure > arrival),
	guest_ref      text NOT NULL,
	status         text NOT NULL DEFAULT 'confirmed',
	request_key    text NOT NULL,               -- client idempotency key
	created_at     timestamptz NOT NULL DEFAULT now(),
	PRIMARY KEY (account_id, reservation_id),
	UNIQUE (account_id, request_key)
);
-- which availability slot each night of a reservation took, so a cancellation returns it
CREATE TABLE IF NOT EXISTS reservation_nights (
	account_id     text NOT NULL,
	reservation_id uuid NOT NULL,
	night          date NOT NULL,
	slot           smallint NOT NULL,
	PRIMARY KEY (account_id, reservation_id, night)
);
`

// FenceLockSpace is the first key of the advisory lock that guards a tenant's fence. Writers hold
// it shared for their whole transaction; a move takes it exclusively to freeze the tenant.
// PostgreSQL queues lock requests, so a new writer waits behind a waiting freeze: the freeze
// cannot be starved by a steady stream of writers (a row lock would let shared lockers jump the queue).
const FenceLockSpace = 7311

// TenantTables are the tables whose rows belong to a tenant, in dependency order.
var TenantTables = []string{"tenant_fence", "properties", "availability", "reservations", "reservation_nights"}

// Apply runs DDL.
func Apply(ctx context.Context, db *sql.DB, ddl string) error {
	if _, err := db.ExecContext(ctx, ddl); err != nil {
		return fmt.Errorf("apply schema: %w", err)
	}
	return nil
}
