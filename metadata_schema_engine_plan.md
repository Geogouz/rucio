# Metadata Schema Validation Engine Plan

## Capability Overview
- **Schema identifiers & versions stored with metadata** so each DID records the schema used at insertion/migration time, enabling consistent validation, auditing, and future upgrades.
- **Centralized validator shared across metadata backends** (legacy and future plugins) to guarantee uniform behavior on all metadata write paths (single/bulk setters, migrations, repairs).
- **Admin-controlled schema catalog** supporting permissive "allow all" schemas for current open-ended usage as well as strict schemas for future plugins; catalog is versioned, signed/approved, and discoverable by all services.
- **Fail-fast enforcement and observability**: validation cannot silently disable; configuration issues surface as critical errors/alerts, with metrics/logging for all validation outcomes.
- **Migration and tooling** to backfill schema IDs, revalidate existing records, and orchestrate schema upgrades with reporting and rollback controls.

## Workstream Ordering (early to late)

### 1) Data model & schema catalog
- Introduce persisted schema identifiers and versions on DIDs/metadata rows (per-VO where applicable). Define storage fields and indexing strategy.
- Define the schema catalog abstraction (API + storage) that maps `(schema_id, version)` to a JSON Schema payload plus metadata (status, deprecation flag, checksum/signature, description). Support a permissive baseline schema that allows any JSON to preserve current behavior.
- Add configuration for allowed catalogs (local module, DB table, signed bundle) and trust policy (e.g., signature verification or checksum pinning). Ensure defaults keep behavior equivalent to today (permissive schema selected implicitly).
- Specify backward compatibility: when no schema is present on existing records, treat them as bound to the permissive baseline schema.

### 2) Schema loading, caching, and governance
- Implement loader interfaces that fetch schemas from the configured catalog(s), with version resolution and caching (in-process + shared cache if available). Include cache invalidation on catalog updates.
- Add admin workflows to register/approve/deprecate schema versions, including validation of schema syntax and compatibility checks (e.g., backwards/forwards compatibility hints where possible).
- Provide policy hooks to restrict which schemas/versions are selectable per VO or namespace (scope), including “allow-all” for legacy.

### 3) Central validation service (API-facing layer)
- Create a dedicated validation module to replace ad-hoc `validate_schema` usage. Responsibilities: resolve schema by reference on the payload, enforce fail-fast semantics, and emit structured results (pass/fail + error paths).
- Require schema reference on all metadata write APIs (`set_metadata`, `set_metadata_bulk`, migrations, plugin-specific upserts). If omitted, default to the VO’s configured baseline (likely permissive) and persist that reference.
- Harden behavior when configuration is broken: raise critical exceptions and block writes instead of silently bypassing validation; surface clear operator-facing logs and metrics.
- Update client/server request validation to ensure schema references are well-formed and authorized for the target VO/scope.

### 4) Backend integration layer
- Define a backend-agnostic validation hook interface (e.g., `validate_metadata(schema_ref, payload, context)`) that backends must call before persistence; include guidance for future metadata plugins to reuse the centralized validator.
- Refactor existing Postgres JSON metadata plugin to delegate validation to the central service, removing inlined schema handling and eliminating the `AttributeError` path. Ensure multi-VO settings are respected.
- Provide an escape-hatch for permissive schemas (no-op validation) to preserve current behavior where strict schemas are not yet adopted.

### 5) Migration bootstrap from legacy data
- Design a bootstrap routine to assign schema references to existing metadata: default to permissive schema unless operators provide mappings (e.g., by scope or metadata pattern).
- Build offline/online revalidation tools that iterate existing records, validate against chosen schemas, and produce reports (passed, failed with reasons). Support chunked processing and resumability.
- Add controls for remediation: mark invalid records, export diffs, or allow operator-provided fixups before reattempting validation.

### 6) Schema evolution & upgrade workflows
- Define process for introducing new schema versions: compatibility checks, staged rollout (canary VO/scope), and deprecation of old versions with grace periods.
- Implement migration hooks that revalidate and, if necessary, transform metadata when moving from `(schema_id, v1)` to `(schema_id, v2)`. Provide dry-run mode and rollback guidelines.
- Persist audit trails: who initiated upgrades, counts of validated/failed items, timestamps, and schemas used.

### 7) Observability, testing, and operations
- Add metrics for validation attempts, failures (by reason), schema resolution errors, and latency; integrate with existing monitoring.
- Enhance logging with structured entries including schema references, VO/scope, and validation outcomes; ensure PII-safe logging of payload snippets.
- Build test suites covering: configuration failure paths (must fail fast), schema resolution precedence (VO override vs. global), backend integration, permissive schema behavior, and migration tools.
- Provide operator runbooks: how to register schemas, configure defaults, perform revalidation, interpret metrics/logs, and execute migrations.

### 8) Security & resilience
- Enforce signature/checksum verification for schema artifacts and ensure only trusted sources are accepted. Validate that policy package versions match server expectations.
- Rate-limit or bound validation workload to avoid DoS from pathological schemas or payloads; document safe limits and timeouts.
- Ensure the validator uses hardened JSON Schema settings (no remote refs unless explicitly allowed, controlled resolver) to prevent SSRF or untrusted fetches.

### 9) Rollout strategy and backward compatibility
- Start with permissive schema defaults to avoid breaking existing clients; allow operators to opt-in VO/scope-specific stricter schemas.
- Provide feature flags to enable enforcement phases: observe-only (log/metric), warn, enforce (reject on failure). Document expected impacts and rollback steps.
- Maintain compatibility layers so legacy clients without explicit schema references still function, by auto-attaching the configured default schema reference until deprecation timelines expire.

## Developer Guidance & Considerations
- Keep the validator as a shared service/module with a stable interface so new metadata plugins can call it without duplicating logic.
- Avoid coupling to a specific storage backend; schema references and validation results should flow through generic DTOs/ORM models.
- Design schema references as opaque tokens (`schema_id@version`) validated by the catalog; avoid embedding full schemas in requests.
- Ensure all new fields and APIs are multi-VO aware and respect existing authorization checks.
- Provide a minimal “allow all” schema and make it the default assignment for legacy data and new deployments that have not opted into stricter policies.
