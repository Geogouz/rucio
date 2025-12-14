# Plan: Move ATLAS-specific DID metadata from table columns to a JSON plugin

## Summary of the target system
- **Storage model**: All ATLAS-derived DID metadata keys (`project`, `datatype`, `run_number`, `stream_name`, `prod_step`, `version`, `campaign`, `task_id`, `panda_id`, `lumiblocknr`, `provenance`, `phys_group`) are migrated out of the `dids` table and stored in the JSON-backed DID metadata plugin (`DidMeta.metadata` column). The plugin keeps the same key names and reuses the existing JSON column implementation, not a new table. Any existing JSON stored in `DidMeta.meta` must also be copied into `DidMeta.metadata` so the consolidated payload contains both the legacy column values and any preexisting JSON metadata.
- **Plugin routing**: `DidColumnMeta` stops claiming the ATLAS keys, while a dedicated JSON plugin (`DidMeta`) claims exactly those keys and is ordered ahead of the generic catch-all JSON plugin. Reads and writes for these keys go to the JSON store after migration; other keys remain owned by the base column plugin. The ATLAS deployment is the only deployment that activates `DidMeta`.
- **Dual-write migration window (2 years)**: During the window, writes for ATLAS keys are applied atomically to both the legacy columns and JSON. Reads and filters prefer JSON but transparently fall back to the columns until each DID is marked migrated. Failures to write JSON trigger logged fallbacks to column-only handling per DID.
- **Migration tooling**: Administrators run an idempotent, resumable backfill tool that copies the ATLAS columns into JSON, records checkpoints, verifies progress, and can resume safely. Migration status per DID controls whether requests are routed to JSON-only or dual-write.
- **Filtering and performance**: `list_dids` filters on ATLAS keys are served by the JSON plugin. DB-level JSON path indexes (where supported) are recommended to retain filter performance without adding new columns.
- **Rollout**: Feature flags enable the plugin and dual-write window globally. After the 2-year period and successful validation, legacy column writes can be disabled and columns prepared for deprecation.

## Step 1: Delegate ATLAS keys away from the base column plugin
- **Code**: `lib/rucio/core/did_meta_plugins/did_column_meta.py` currently manages all DID columns except a small excluded list, so it must explicitly return `False` for the ATLAS keys.
- **Action**:
  1. Update `DidColumnMeta.manages_key` to exclude `project`, `datatype`, `run_number`, `stream_name`, `prod_step`, `version`, `campaign`, `task_id`, `panda_id`, `lumiblocknr`, `provenance`, `phys_group`.
  2. Add unit tests ensuring `manages_key` returns `False` for each ATLAS key and still returns `True` for other base columns (e.g., `bytes`, `length`).
- **Result**: The routing layer can hand off ATLAS keys to another plugin without changing the base behavior for core columns.

## Step 2: Introduce an ATLAS-targeted JSON plugin
- **Code**: Build a new plugin (`DidMeta`) in `lib/rucio/core/did_meta_plugins/` that wraps the existing JSON storage logic. The plugin is only loaded for the ATLAS experiment and is not intended for other deployments.
- **Action**:
  1. Implement `manages_key` to return `True` only for the ATLAS key list.
  2. Reuse the `DidMeta.metadata` JSON column for storage (same serialization and dialect handling as `JSONDidMeta`).
  3. Implement `get_metadata`, `set_metadata_bulk`, `delete_metadata`, and `list_dids` by delegating to JSON semantics, with the dual-write logic described in Step 3.
  4. Register the plugin in `lib/rucio/core/did_meta_plugins/__init__.py` ahead of the generic `JSONDidMeta` so routing reaches it first. Since only ATLAS enables this plugin, ordering changes do not affect other experiments; document that custom deployments should leave `DidMeta` disabled.
  5. Add configuration knob(s) (e.g., `metadata.atlas_json_enabled`) to toggle this plugin and default it on for the global rollout.
- **Result**: Writes and reads for the ATLAS keys target the JSON store while keeping key names stable.

## Step 3: Dual-write and fallback behavior for the 2-year window
- **Action**:
  1. For ATLAS keys, attempt an atomic transaction that writes both to the JSON store and the legacy `DataIdentifier` columns. If JSON storage fails, log the error, roll back JSON changes, and fall back to column-only while keeping the DID marked as “not yet migrated”.
  2. Maintain a per-DID migration marker using two new columns on the `did_meta` table: `schema_name` (name of the schema version owner, e.g., `atlas_did_meta`) and `schema_ver` (numeric schema version). When `schema_name`/`schema_ver` indicate the JSON schema is authoritative, stop writing to columns and route reads/filters to JSON only. Store the JSON metadata payload in a `metadata` column on the same table; populate this column with both values migrated from the legacy `dids` columns and any preexisting JSON stored in `DidMeta.meta`.
  3. Ensure `get_metadata(plugin="all")` and `get_metadata` prefer JSON values when the marker is set, but fall back to column values during the window if JSON is missing or the marker is unset.
  4. Ensure concurrent writes for a single DID remain consistent by performing both writes within the same DB transaction or using advisory locks where the dialect supports them.
  5. Emit structured logs for all fallback cases and for successful promotion of a DID to JSON-only status.
- **Result**: Data stays consistent during migration, with clear visibility into any fallback paths.

## Step 4: Filtering and query behavior on JSON-managed keys
- **Action**:
  1. Update the plugin routing so `list_dids` filters containing ATLAS keys select the new JSON plugin. Preserve the existing `FilterEngine` usage with `DidMeta.metadata` as the JSON column.
  2. Add DB-specific index recommendations/migrations (e.g., PostgreSQL GIN indexes on `DidMeta.metadata -> 'project'`, Oracle JSON indexes, MySQL JSON path indexes) to retain performance without introducing new table columns.
  3. Validate that range/eq filters on these keys behave as before; add integration tests for `list_dids` using representative filters (e.g., `project`, `run_number`, `stream_name`).
- **Result**: Clients can continue filtering on ATLAS metadata with comparable performance after migration.

## Step 5: Idempotent, resumable backfill tool for administrators
- **Action**:
  1. Add a CLI tool under `tools/` that iterates over `dids` (and `deleted_dids` if required), reading the ATLAS columns and writing them into the JSON store with dual-write semantics. The tool must also copy any JSON already present in `DidMeta.meta` into the `metadata` column so the consolidated payload captures both sources.
  2. Implement checkpointing (e.g., last processed `(scope, name)` or a high-watermark ID) so the tool can resume safely; skip rows already marked as migrated.
  3. Provide dry-run and verify modes: dry-run reports counts and sample diffs; verify mode re-reads migrated rows to confirm JSON matches column values.
  4. Log progress, errors, and summary statistics; optionally expose metrics for external monitoring.
- **Result**: Operators can safely backfill existing data and monitor progress without downtime.

## Step 6: Backward compatibility and cutover controls
- **Action**:
  1. Add configuration for dual-write end date (2 years) and a switch to disable column fallbacks once the window closes and validation passes.
  2. Provide admin tooling or migration scripts to flip all DIDs to JSON-only after successful validation, then disable column writes.
  3. Update any client-facing documentation to note that the keys remain available but are served from JSON, with no API changes required.
- **Result**: Predictable sunset of legacy column usage with a clear operational switch.

## Step 7: Validation, monitoring, and observability
- **Action**:
  1. Emit structured logs for each migration phase: start/end, per-DID promotion, fallback events, and verification summaries.
  2. Expose metrics (e.g., counts of migrated DIDs, fallback write attempts, JSON vs column read rates) for dashboards/alerts.
  3. Add automated tests covering: routing decisions, dual-write success/failure, fallback reads, list filtering, and migration tool idempotency.
- **Result**: Developers and operators have clear signals about migration health and can detect regressions quickly.

## Step 8: Deployment and rollout
- **Action**:
  1. Ship the new plugin and configuration defaults with the feature enabled. Use the backfill tool to populate JSON before disabling column fallbacks.
  2. Perform a staged data validation (counts and sampled value comparisons) before announcing the global switch.
  3. After the 2-year window and successful validation, remove dual-write logic and plan deprecation of the ATLAS columns in the schema (subject to separate DB migration policy).
- **Result**: A controlled global rollout with a clear path to remove legacy columns later.

## Reference: ATLAS-specific columns in the base schema
The `DataIdentifier` model defines the ATLAS-specific columns within the hardcoded metadata block, confirming their current placement in the base table (source for migration):
- `project`, `datatype`, `run_number`, `stream_name`, `prod_step`, `version`, `campaign`, `task_id`, `panda_id`, `lumiblocknr`, `provenance`, `phys_group`.

## Open questions to eliminate remaining ambiguity
- What exact values (and initial defaults) should be written into `did_meta.schema_name` and `did_meta.schema_ver`, and how are version increments coordinated during future schema changes?
- What data type, size limits, and validation rules should the new `did_meta.metadata` JSON column enforce across supported database engines?
- How long should `schema_name`/`schema_ver` and `metadata` remain populated after the 2-year window, and what is the precise cleanup process (e.g., nulling markers vs. dropping columns)?
- Should the backfill tool also populate `did_meta.metadata` for DIDs that are already deleted/tombstoned, and how are concurrent deletions handled while setting the migration markers?
- Which configuration flag(s) explicitly gate enabling `DidMeta` for ATLAS, and how should non-ATLAS deployments verify the plugin stays disabled during upgrades?
- If both the legacy columns and `DidMeta.meta` contain the same key with divergent values, which source should take precedence when composing the consolidated `metadata` payload, and should conflicts be surfaced to operators?
