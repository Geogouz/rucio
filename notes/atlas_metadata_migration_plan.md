# Plan: Move ATLAS-specific DID metadata from table columns to a JSON plugin

## Summary of the target system
- **Storage model**: Migrate all ATLAS-derived DID metadata keys (`project`, `datatype`, `run_number`, `stream_name`, `prod_step`, `version`, `campaign`, `task_id`, `panda_id`, `lumiblocknr`, `provenance`, `phys_group`) out of the `dids` table and store them in an ATLAS-only JSON-backed DID metadata plugin named `DidMeta`, using the existing JSON column that powers `JSONDidMeta` (the `DidMeta.metadata` column). Keep the same key names and reuse that JSON column implementation instead of adding a new table. Copy any existing JSON stored in `DidMeta.meta` into `DidMeta.metadata` so the consolidated payload contains both the legacy column values and any preexisting JSON metadata. Use `JSONB` for PostgreSQL and the dialect-equivalent JSON type for Oracle when defining the column to keep native JSON capabilities. ATLAS will run the plugin first; broader adoption and defaulting for other experiments are explicitly deferred to the Future work section.
- **Plugin routing**: Stop `DidColumnMeta` from claiming the ATLAS keys, while a dedicated JSON plugin (`DidMeta`) claims all metadata and is ordered ahead of the generic catch-all JSON plugin. Route reads and writes for ATLAS keys to the JSON store after migration, with the base column plugin retaining responsibility for the legacy column storage and fallback paths. Activate `DidMeta` for ATLAS and document that other experiments may enable it if desired, but ATLAS must enable it once the ATLAS columns are removed. Defaulting behavior for other experiments is described in Future work.
- **Dual-write migration window (2 years)**: During the window, apply writes for ATLAS keys atomically to both the legacy columns and JSON. Prefer JSON for reads and filters but transparently fall back to the columns until each DID is marked migrated. Trigger logged fallbacks to column-only handling per DID if JSON writes fail.
- **Migration tooling**: Run an idempotent, resumable backfill tool that copies the ATLAS columns into JSON, records checkpoints, verifies progress, and can resume safely. Use migration status per DID to control whether requests are routed to JSON-only or dual-write.
- **Filtering and performance**: Serve `list_dids` filters on ATLAS keys via the JSON plugin. Add DB-level JSON path indexes (where supported) to retain filter performance without adding new columns.
- **Rollout**: Enable the plugin and dual-write window globally via feature flags. After the 2-year period and successful validation, disable legacy column writes and prepare columns for deprecation.

## Step 1: Delegate ATLAS keys away from the base column plugin
- **Code**: Update `lib/rucio/core/did_meta_plugins/did_column_meta.py`, which currently manages all DID columns except a small excluded list, so it explicitly returns `False` for the ATLAS keys.
- **Action**:
  1. Update `DidColumnMeta.manages_key` to exclude `project`, `datatype`, `run_number`, `stream_name`, `prod_step`, `version`, `campaign`, `task_id`, `panda_id`, `lumiblocknr`, `provenance`, `phys_group`.
  2. Add unit tests ensuring `manages_key` returns `False` for each ATLAS key and still returns `True` for other base columns (e.g., `bytes`, `length`).
- **Result**: Allow the routing layer to hand off ATLAS keys to another plugin without changing the base behavior for core columns.

## Step 2: Introduce an ATLAS-targeted JSON plugin
- **Code**: Build a new plugin (`DidMeta`) in `lib/rucio/core/did_meta_plugins/` that wraps the existing JSON storage logic. This is a new ATLAS-specific plugin that reuses the same JSON column and serialization machinery already used by the generic `JSONDidMeta` plugin, not a replacement of `JSONDidMeta` for other experiments. Load the plugin only for the ATLAS experiment and avoid enabling it for other deployments.
- **Action**:
  1. Implement `manages_key` to return `True` for all metadata keys so the plugin captures both the ATLAS list and any additional JSON payload in a single location.
  2. Reuse the `DidMeta.metadata` JSON column for storage (same serialization and dialect handling as `JSONDidMeta`).
  3. Implement `get_metadata`, `set_metadata_bulk`, `delete_metadata`, and `list_dids` by delegating to JSON semantics, with the dual-write logic described in Step 3.
  4. Register the plugin in `lib/rucio/core/did_meta_plugins/__init__.py` ahead of the generic `JSONDidMeta` so routing reaches it first. Since only ATLAS enables this plugin, ordering changes do not affect other experiments; document that custom deployments must leave `DidMeta` disabled.
  5. Add configuration knob(s) (e.g., `metadata.atlas_json_enabled`) to toggle this plugin and default it on for the global rollout.
- **Result**: Write and read ATLAS keys via the JSON store while keeping key names stable for ATLAS; global defaulting behavior is deferred to Future work.

## Step 3: Dual-write and fallback behavior for the 2-year window
- **Action**:
  1. For ATLAS keys, attempt an atomic transaction that writes both to the JSON store and the legacy `DataIdentifier` columns. If JSON storage fails, log the error, roll back JSON changes, and fall back to column-only while keeping the DID marked as “not yet migrated”. When consolidating values between columns and any existing JSON payload, prefer column values for overlapping keys and log every conflict so operators can audit divergences.
  2. Maintain a per-DID migration marker using two new columns on the `did_meta` table: `schema_name` (name of the schema version owner, e.g., `atlas_open_meta`) and `schema_ver` (numeric schema version). Initialise preexisting rows with `NULL` markers; when a DID finishes migration, set `schema_name` to the default open schema and `schema_ver` to `1`. When `schema_name`/`schema_ver` indicate the JSON schema is authoritative, stop writing to columns and route reads/filters to JSON only. Store the JSON metadata payload in a `metadata` column on the same table; populate this column with both values migrated from the legacy `dids` columns and any preexisting JSON stored in `DidMeta.meta`.
  3. Ensure `get_metadata(plugin="all")` and `get_metadata` prefer JSON values when the marker is set but fall back to column values during the window if JSON is missing or the marker is unset.
  4. Keep concurrent writes for a single DID consistent by performing both writes within the same DB transaction or using advisory locks where the dialect supports them.
  5. Emit structured logs for all fallback cases and for successful promotion of a DID to JSON-only status.
- **Result**: Keep data consistent during migration, with clear visibility into any fallback paths.

## Step 4: Filtering and query behavior on JSON-managed keys
- **Action**:
  1. Update the plugin routing so `list_dids` filters containing ATLAS keys select the new JSON plugin. Preserve the existing `FilterEngine` usage with `DidMeta.metadata` as the JSON column.
  2. Add DB-specific index recommendations/migrations (e.g., PostgreSQL GIN indexes on `DidMeta.metadata -> 'project'`, Oracle JSON indexes, MySQL JSON path indexes) to retain performance without introducing new table columns.
  3. Validate that range/eq filters on these keys behave as before; add integration tests for `list_dids` using representative filters (e.g., `project`, `run_number`, `stream_name`).
- **Result**: Allow clients to continue filtering on ATLAS metadata with comparable performance after migration.

## Step 5: Idempotent, resumable backfill tool for administrators
- **Action**:
  1. Add a CLI tool under `tools/` that iterates over `dids` (and `deleted_dids` if required), reads the ATLAS columns, and writes them into the JSON store with dual-write semantics. Copy any JSON already present in `DidMeta.meta` into the `metadata` column so the consolidated payload captures both sources, preferring column values for overlapping keys and logging any conflicts detected during the merge.
  2. Implement checkpointing (e.g., last processed `(scope, name)` or a high-watermark ID) so the tool can resume safely; skip rows already marked as migrated.
  3. Provide dry-run and verify modes: dry-run reports counts and sample diffs; verify mode re-reads migrated rows to confirm JSON matches column values.
  4. Log progress, errors, and summary statistics; optionally expose metrics for external monitoring.
- **Result**: Allow operators to backfill existing data safely and monitor progress without downtime.

## Step 6: Backward compatibility and cutover controls
- **Action**:
  1. Add configuration for dual-write end date (2 years) and a switch to disable column fallbacks once the window closes and validation passes.
  2. Provide admin tooling or migration scripts to flip all DIDs to JSON-only after successful validation, then disable column writes.
  3. Update any client-facing documentation to note that the keys remain available but are served from JSON, with no API changes required.
- **Result**: Deliver a predictable sunset of legacy column usage with a clear operational switch.

## Step 7: Validation, monitoring, and observability
- **Action**:
  1. Emit structured logs for each migration phase: start/end, per-DID promotion, fallback events, and verification summaries.
  2. Expose metrics (e.g., counts of migrated DIDs, fallback write attempts, JSON vs column read rates) for dashboards/alerts.
  3. Add automated tests covering: routing decisions, dual-write success/failure, fallback reads, list filtering, and migration tool idempotency.
- **Result**: Provide developers and operators with clear signals about migration health to detect regressions quickly.

## Step 8: Deployment and rollout
- **Action**:
  1. Ship the new plugin and configuration defaults with the feature enabled. Use the backfill tool to populate JSON before disabling column fallbacks.
  2. Perform a staged data validation (counts and sampled value comparisons) before announcing the global switch.
  3. After the 2-year window and successful validation, remove dual-write logic and plan deprecation of the ATLAS columns in the schema (subject to separate DB migration policy).
- **Result**: Achieve a controlled global rollout with a clear path to remove legacy columns later.

## Reference: ATLAS-specific columns in the base schema
Use the `DataIdentifier` model to confirm the ATLAS-specific columns within the hardcoded metadata block before migrating them out of the base table:
- `project`, `datatype`, `run_number`, `stream_name`, `prod_step`, `version`, `campaign`, `task_id`, `panda_id`, `lumiblocknr`, `provenance`, `phys_group`.

## Schema marker defaults and coordination
- Initialise `did_meta.schema_name` and `did_meta.schema_ver` as `NULL` for preexisting rows so legacy-only DIDs are clearly identified at the start of the migration. After a DID finishes migration to JSON, set `schema_name` to the default open schema (version `1`). This schema is the permissive Rucio default used when an experiment has not provided a more specific schema list.
- Treat the open schema as a placeholder until the future schema engine is available. That engine will store available schemas and versions in a dedicated table, decide which schema applies to each DID, and validate metadata before insertion. Keep this migration permissive now while preserving markers to ease later validation work.

## Open questions to eliminate remaining ambiguity

## Decision checklist to remove ambiguity
Use the following question-and-option list to make the document self-sufficient. For each item, select one option and record the decision near this section.

1. **Scope of backfill for deleted/tombstoned DIDs**: Should `deleted_dids` (or other tombstone tables) be migrated?
   - **Option A: Backfill tombstones** — Migrate JSON metadata and schema markers for `deleted_dids` to preserve historical search/filter parity; expect longer runtime and more IO.
   - **Option B: Skip tombstones** — Restrict backfill to live DIDs; faster runtime but historical filters remain column-backed and may diverge.
   - **Option C: Hybrid** — Backfill tombstones for a bounded retention window (e.g., last N months/years) to balance parity and runtime; document the cutoff.

2. **Feature-flag strategy for ATLAS-only enablement**: How is the plugin gated, and how are non-ATLAS deployments protected?
   - **Option A: Single flag** — One switch (e.g., `metadata.atlas_json_enabled`) controls registration and routing of `DidMeta`; ATLAS must set it to `true` once columns are removed.
   - **Option B: Two-stage flags** — Separate flags for plugin registration and for routing/traffic cutover, allowing code rollout ahead of enablement; include startup validation that non-ATLAS scopes default to disabled.
   - **Option C: Scope-allowlist** — Require an explicit allowlist of scopes/experiments for which the plugin is registered; defaults to empty to protect non-ATLAS deployments.

3. **Routing guarantees for non-JSON/base columns during dual-write**: How to avoid `DidMeta` shadowing `DidColumnMeta` responsibilities?
   - **Option A: Routing short-circuit** — `DidColumnMeta` retains priority for its native columns; routing checks it first and only forwards ATLAS keys to `DidMeta`, even if `DidMeta.manages_key` returns `True`.
   - **Option B: Narrow `DidMeta.manages_key`** — Limit `DidMeta` to the ATLAS key list; generic JSON keys stay with `JSONDidMeta` (or another JSON plugin), preventing accidental capture.
   - **Option C: Layered claims with schema marker** — Allow `DidMeta` to claim all keys but require routing to consult the per-DID schema marker: if unset, direct non-ATLAS keys to `DidColumnMeta`; once set, permit `DidMeta` to respond for all JSON-managed keys while still delegating base columns to `DidColumnMeta`.

4. **Conflict resolution precedence during merge**: When column values and existing JSON payloads differ, which wins?
   - **Option A: Columns win** — Legacy column values override JSON during backfill and dual-write merges; log every divergence.
   - **Option B: JSON wins** — Preserve preexisting JSON values and only fill missing keys from columns; log divergences for audit.
   - **Option C: Schema-driven** — Use a per-key policy (e.g., columns win for canonical ATLAS keys, JSON wins for auxiliary keys); document the map.

5. **Transaction/locking model for dual writes**: How to ensure atomicity between column and JSON updates?
   - **Option A: Single DB transaction** — Enclose JSON and column writes in one transaction; rely on DB rollback on failure.
   - **Option B: Advisory locks** — Acquire per-DID advisory locks (where supported) to serialize writes across processes, combined with a shared transaction.
   - **Option C: Best-effort with retry** — Attempt dual write without locks but include retries and conflict detection; acceptable only if contention is demonstrably low.

6. **Error handling when JSON write fails**: What is the fallback behavior and user-visible signal?
   - **Option A: Column-only fallback** — Roll back JSON, commit column write, leave migration marker unset, and emit structured logs/metrics.
   - **Option B: Fail the request** — Treat JSON failure as fatal and abort the whole operation to keep parity; requires clients to retry.
   - **Option C: Configurable policy** — Default to column-only fallback but allow deployments to choose fail-fast via configuration.

7. **List filtering performance safeguards**: Which database index strategy should be mandated for ATLAS keys?
   - **Option A: Mandatory JSON indexes** — Require a documented set of JSON path/GIN indexes for ATLAS keys in supported DBs; migrations include index creation.
   - **Option B: Optional indexes** — Provide recommended index definitions but allow operators to opt out; note the performance trade-off.
   - **Option C: Hybrid** — Mandate indexes on the highest-volume keys (e.g., `project`, `run_number`) and make the rest optional.

8. **Backfill execution order and batching**: How should the tool iterate and checkpoint?
   - **Option A: Ordered by primary key** — Process by `(scope, name)` ordering with a high-watermark checkpoint.
   - **Option B: Chunked by scope** — Process per-scope batches to limit lock contention and ease partial retries.
   - **Option C: Time-sliced** — Process in time windows (e.g., based on creation date) to prioritize recent data; document scheduling expectations.

9. **Verification depth after backfill**: What validation level is required before marking a DID as migrated?
   - **Option A: Key-for-key equality** — Require exact match between columns and JSON for all ATLAS keys before setting the schema marker.
   - **Option B: Spot sampling** — Promote based on successful write plus sampling audits; faster but lower assurance.
   - **Option C: Tiered** — Use strict equality for new/updated DIDs and sampling for cold historical data; document thresholds.

10. **Operational rollout controls**: How should the cutover and sunset be enforced?
    - **Option A: Calendar-based cutoff** — Enforce a hard end date after which column writes are disabled automatically.
    - **Option B: Manual switch** — Require an explicit admin action to disable column fallbacks after validation is signed off.
    - **Option C: Guardrailed switch** — Manual switch gated by automated health checks/metrics thresholds; block switch if thresholds are unmet.

11. **Observability expectations**: What telemetry is mandatory for detecting issues?
    - **Option A: Logging only** — Structured logs for dual-write attempts, fallbacks, promotions, and backfill progress.
    - **Option B: Logs + metrics** — Add counters/gauges for migration status, fallback events, and filter usage; dashboards required.
    - **Option C: Logs + metrics + tracing** — Include trace spans for metadata operations during the window; needed if tracing infra is standard.

12. **Behavior for non-ATLAS experiments during rollout**: What defaults apply elsewhere?
    - **Option A: Opt-in only** — Other experiments stay on existing plugins unless they explicitly opt into `DidMeta`.
    - **Option B: Shadow mode** — Allow non-ATLAS deployments to run `DidMeta` in read-only shadow mode for observability without traffic.
    - **Option C: Gradual opt-out** — Default-enable `DidMeta` for all experiments after a grace period unless an explicit opt-out is set.

Document the selected options here once chosen to keep the migration plan coherent and implementation-ready.

## Future work
- **Default behavior for all experiments**: After the ATLAS rollout proves stable and the `DidMeta` plugin covers all endpoints and validations, allow other experiments to migrate from their current metadata plugins to `DidMeta` and adopt it as the default metadata backend. When no metadata plugin is specified in configuration, select `DidMeta` by default instead of `JSONDidMeta` once the global migration path is available. Deployments that explicitly configure another plugin (e.g., `JSONDidMeta`) keep their choice until they opt in.
- **Schema selection and validation**: The server will eventually select which schema name/version to apply to each DID before accepting metadata and will validate the payload against that schema during insertion. The policy for choosing schema and version per DID is intentionally deferred and will be designed alongside the future schema-management work.
- **Schema marker evolution**: Coordinate future schema changes by incrementing `schema_ver` under the same `schema_name` when tightening validations, and introduce new `schema_name` values if administrators need divergent schema families. Plan version bumps alongside database migrations and backfill scripts that apply the new validations and update the marker for each affected DID.
- **Open questions**: Resolve outstanding items such as tombstoned DID backfill scope, the precise feature-flag combination for ATLAS vs. non-ATLAS deployments, and the routing rules that prevent `DidMeta` from shadowing non-JSON column handling during dual-write.
- **Additional schema work**: Treat any schema management and validation concepts (such as defining multiple schema names, tightening key whitelists, or running schema-specific migrations) as follow-up tasks after the migration completes, not as part of the current design.
