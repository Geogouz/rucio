# Rucio Metadata and Filtering Guide

This guide summarizes every capability related to metadata management in Rucio, including the plugin system, configuration, filtering, and available interfaces for users and administrators.

## Architecture Overview

Rucio metadata is provided by a **plugin system**. A registry of enabled plugins is built from the `[metadata]` configuration section. The system always loads the core DID column plugin first, followed by custom plugins listed in `metadata.plugins` (comma-separated). If the configuration is absent, only the built-in JSON plugin is added after the base plugin. Plugin order matters for both key ownership and write precedence; the first plugin that declares support for a key handles it. Metadata keys may not contain the `.` character because it is used in filter syntax.

## Available Plugins

### DID column plugin (`DID_COLUMN`)
- Manages metadata backed by columns of the `dids` table; keys are limited to those columns and extra hardcoded filter helpers.
- Supports single and bulk updates and can optionally propagate updates recursively to child content.
- Special handling exists for lifecycle fields (`eol_at`, `lifetime`), file-specific checksums/bytes/events, and updates cascade to associations, locks, and counters.
- Listing supports semantic `type` values (all, collection, container, dataset, file) and recursive expansion of attached contents.
- Key management covers DID columns except a small exclude list, plus helper filter keys such as `created_before/after` (translated to `created_at` ranges) and numeric comparisons for `length`.
- Deleting metadata is not implemented for this plugin.

### JSON plugin (`JSON`)
- Stores arbitrary key/value metadata per DID in the relational database using the `did_meta` table with a JSON column.
- Automatically creates a `did_meta` row if absent during writes. Supports single and bulk updates, with overwrite semantics per key.
- Deleting removes the key from the JSON blob, raising an error if the key is missing.
- Listing supports wildcard matching, type coercion, and optional recursive traversal of container/dataset contents; long output omits size/type fields because they are not stored in JSON.
- Requires a database backend with JSON support (Oracle and SQLite use string serialization).

### Elasticsearch plugin (`ELASTIC`)
- Stores metadata documents in an Elasticsearch index. Connection parameters (hosts, credentials, SSL, client certificates, indices, timeouts, retries) are read from `[metadata]` configuration entries.
- Automatically seeds immutable keys (`scope`, `name`, `vo`) on new documents; other fields are freely mutable. Archive support allows copying documents to an archive index before deletion.
- Supports get, single/bulk set, and delete operations; recursive writes and recursive searches are currently unsupported.
- Listing uses a point-in-time cursor with sorting and pagination; `long` responses include optional `did_type`, `bytes`, and `length` fields if present in the document.
- Key management is permissive (all keys are accepted by this plugin).

## Plugin Resolution and Key Ownership

- Plugin name matching is case-insensitive. The reserved name `ALL` queries every plugin and merges results when reading metadata; writes must resolve to exactly one plugin that claims ownership of the key(s).
- Bulk writes partition keys per plugin. If any key is unmanaged or multiple plugins would be required, the operation is rejected.
- Filtering with `list_dids` must use keys from a single plugin; otherwise a validation error is raised. If no filter keys are provided, the base plugin is used by default.

## Filtering Capabilities

Filtering is powered by the `FilterEngine` with the following features:

- **Input forms**: filters can be a JSON string, a single dictionary, or a list of dictionaries representing OR groups. Within each dictionary, key-value pairs are ANDed.
- **Operators**: suffix the key with `.gt`, `.lt`, `.gte`, `.lte`, `.ne`; default is equality. Wildcards (`*` or `%`) are allowed only with equality/inequality.
- **Type coercion**: string inputs are auto-cast to booleans, numbers, datetimes (multiple formats), or left as strings. `created_before/after` keys are converted to `created_at` range filters.
- **Validation**: ensures type-only equality, name equality/inequality, numeric `length`, valid date formats, and rejects duplicate criteria per OR group.
- **Backends**: can generate SQLAlchemy queries, PostgreSQL JSONB filters, MongoDB queries, and Elasticsearch queries. JSON-aware filtering (e.g., JSON plugin) skips coercion to model attributes and can target JSON columns.
- **Recursive listing**: base and JSON plugins can walk container/dataset contents when `recursive=true`, inserting derived names back into the filters; Elastic forbids recursion.

## Operations

### Read metadata
- REST: `GET /dids/<scope:name>/meta?plugin=<PLUGIN|ALL>` returns the full metadata from the chosen plugin, or merged when `ALL` is specified.
- Client/CLI: `rucio get-metadata <scope:name> [--plugin PLUGIN]` (or `rucio did get-metadata` in the new CLI) maps to the same API.

### Write metadata
- REST single key: `POST /dids/<scope:name>/meta/<key>` body `{ "value": ... , "recursive": false }`.
- REST bulk: `POST /dids/<scope:name>/meta` body `{ "meta": {"k1": v1, ...}, "recursive": false }`.
- REST delete: `DEL /dids/<scope:name>/meta/<key>` (supported only by plugins that implement deletion).
- Client/CLI: `rucio set-metadata`, `rucio delete-metadata`, and bulk helpers follow the same plugin-selection and recursion semantics.

### Bulk metadata operations
- REST bulk set across DIDs: `POST /dids/meta/bulk` with `{"dids": [{"scope":..., "name":..., "meta": {...}}, ...]}` applies metadata atomically; failure on any DID aborts the batch.
- REST bulk get: `POST /dids/meta/bulk?plugin=<PLUGIN|ALL>&inherit=<bool>` streams newline-delimited JSON responses. When `inherit=true`, parent metadata is concatenated if supported by the plugin.

### Listing DIDs by metadata
- `list_dids` requires all filter keys to belong to a single plugin. Name filters are always allowed. Unsupported keys produce a validation error.
- Results can be returned as names or, with `long=True`, as dictionaries containing scope, name, type, bytes, and length where available.
- Additional controls: `did_type` or semantic `type` filter, `limit`, `offset`, `ignore_dids` for deduplication across OR groups, and `ignore_case` placeholder (not enforced in base plugin).

## Configuration Reference (metadata section)

- `plugins`: comma-separated list of fully qualified plugin classes to load after the base plugin (default: JSON plugin).
- **Elasticsearch options**: `elastic_service_hosts`, `elastic_user`, `elastic_password`, `meta_index` (default `rucio_did_meta`), `archive_index` (default `archive_meta`), `use_ssl`, `ca_certs`, `client_cert`, `client_key`, `elastic retries` (`request_timeout`, `max_retries`, `retry_on_timeout`), and `verify_certs` toggle.
- Additional plugins can be authored by implementing `DidMetaPlugin` and exposing the class via `metadata.plugins`.

## Key Restrictions and Error Handling

- Keys containing restricted characters (currently `.`) are rejected before writes.
- Using a plugin name not enabled on the server raises `UnsupportedMetadataPlugin`.
- Attempting cross-plugin filters raises `InvalidMetadata` errors; unmanaged keys in write requests also raise `InvalidMetadata`.
- Plugin-specific limitations apply (e.g., DID column plugin cannot delete metadata; Elastic forbids recursion; JSON plugin requires JSON support on the DB backend).

## Extending the System

To add a new metadata backend:
1. Implement `DidMetaPlugin`, providing CRUD methods, listing, and key-ownership logic.
2. Add the fully qualified class path to `metadata.plugins` in configuration.
3. Ensure the plugin name (`_plugin_name`) is unique and documents which keys it manages.
4. Respect the filtering contract so that all keys advertised by `manages_key` can be used in `list_dids` for that plugin.
