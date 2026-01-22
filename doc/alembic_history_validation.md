# Alembic History Validation

This document records what the Alembic history validator checks, how each
phase operates, and how the `_fingerprint` helpers underpin the process.  The
validator lives in `lib/rucio/db/sqla/alembic_validation.py` and is executed in
CI to guarantee that migrations remain deterministic and compatible with the
SQLAlchemy models.

## High level phases

The driver function `validate_history` orchestrates three broad phases:

1. **Repository topology sanity checks** – `_assert_single_head` enforces that
   the Alembic script directory exposes a single head matching
   `rucio.alembicrevision.ALEMBIC_REVISION`.  `_discover_revision_graph` and
   `_build_linear_revision_order` then confirm that the migration graph forms a
   single linear chain with no merges, forks or cycles.  Any deviation raises a
   `multi-*` issue before the database is touched.
2. **Linear walk with round-trips** – after resetting the database to an empty
   base state we iterate through the ordered revisions.  For each parent→child
   edge `_check_linear_revision_edge` performs forward and backward
   upgrade/downgrade round-trips using `_verify_roundtrip`.  The helper compares
   schema fingerprints before and after each trip and validates that
   `models.is_old_db()` reflects the current revision state.  The validator
   halts immediately when drift is detected so that the reported issue can be
   addressed before any further checks execute.
3. **Head verification** – once the walk succeeds we upgrade to the repository
   head, call `verify_revision_markers_at_head` to confirm that the version
   table matches the declared Alembic head, and finally execute
   `_autogen_diff_is_empty` to diff the live schema against the SQLAlchemy
   models.  Any autogenerate output is included in the reported issues.

## What `_fingerprint` provides

`lib/rucio/db/sqla/_fingerprint.py` exposes two key primitives that make the
validation possible:

- `reflect_metadata(engine, include_schemas)` introspects the live database for
  all relevant schemas while handling differences between SQLAlchemy dialects.
- `schema_fingerprint(engine, include_schemas)` turns the reflected metadata
  into a deterministic textual dump and a SHA-256 digest (captured inside a
  `SchemaSnapshot`).  The snapshot also carries tokenised structures that allow
  `_verify_roundtrip` to pinpoint the first mismatch when two snapshots differ.

Round-trip checks rely on these fingerprints to prove that different migration
paths yield the exact same structure.  Because the canonical text strips noisy
attributes such as volatile defaults and dialect-specific owner names the
comparison is both stable across environments and precise enough to detect
missing columns, altered constraints or misconfigured indexes.

## Guarantees and limitations

- A mismatch between the repository head and `ALEMBIC_REVISION`, multiple heads
  or branching history is reported immediately.
- Every migration must be reversible with respect to its neighbours; schemas
  must converge after both A→B→A and B→A→B paths.
- `models.is_old_db()` must agree with the revision reached during validation,
  ensuring the runtime guard can still detect outdated deployments.
- The schema produced by Alembic migrations at head must match the declarative
  SQLAlchemy models or the autogenerate diff is surfaced to developers.

The validator focuses on structural DDL.  Data-only migrations, database
functions or triggers that Alembic/SQLAlchemy cannot introspect will not be
compared unless explicit fingerprinting support is added.
