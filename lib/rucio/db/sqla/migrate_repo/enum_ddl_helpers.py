# Copyright European Organization for Nuclear Research (CERN) since 2012
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""PostgreSQL enum DDL helpers for Alembic migrations.

The functions in this module build raw SQL strings for enum-related DDL that
Alembic can execute with :func:`alembic.op.execute`.  Each helper focuses on a
single operation so that migrations remain explicit and easy to review.

Key features
------------
* Identifiers and literals are quoted through SQLAlchemy’s dialect preparer so
  that schema names and enum labels are emitted safely.
* :func:`render_enum_name` consistently applies the default Alembic schema when
  no schema is provided explicitly.
* Enum labels are validated to catch empty values, duplicates, or items that
  exceed PostgreSQL’s 63-byte limit.
* PostgreSQL-only behaviour is enforced early; attempting to use these helpers
  on another dialect raises a clear error.
* ``create_enum_if_absent_block`` wraps enum creation in a ``DO`` block that can
  be used when migrations must be idempotent.

Operational notes
-----------------
* PostgreSQL does not implement ``CREATE TYPE IF NOT EXISTS``.  Use
  :func:`create_enum_if_absent_block` when you need idempotent creation.
* ``ALTER TYPE ... ADD VALUE`` cannot be used within the same transaction that
  introduced the new value; the transaction must commit before the value can be
  used.
* Some helpers depend on server features that only appear in PostgreSQL 10 or
  later.  When the server version is known, a helpful exception is raised if the
  operation is unsupported.
* The effective schema defaults to Alembic’s ``version_table_schema``.  Supply a
  schema argument directly when enums live elsewhere.

Examples
--------
Create an enum within a migration::

    sql = create_enum_sql("status", ["new", "open", "closed"], schema="public")
    op.execute(sql)

Create an enum only if it does not already exist::

    sql = create_enum_if_absent_block("status", ["new", "open", "closed"], schema="public")
    op.execute(sql)

Add a new value after an existing label::

    sql = alter_enum_add_value_sql(
        "status", "archived", schema="public", after="closed", if_not_exists=True
    )
    op.execute(sql)

Rename a label (PostgreSQL 10+)::

    sql = enum_rename_value_sql("status", "open", "in_progress", schema="public")
    op.execute(sql)

Move a type into a different schema::

    sql = enum_set_schema_sql("status", "app_types", schema="public")
    op.execute(sql)
"""

from collections.abc import Iterable, Sequence
from typing import Optional, Tuple

from alembic import context
from sqlalchemy.engine.default import DefaultDialect

from rucio.db.sqla.migrate_repo.ddl_helpers import get_migration_context


def _safe_get_context():
    """Fetch the Alembic migration context when one has been established.

    The helpers in this module are occasionally imported in tooling that does
    not run inside an Alembic migration.  Returning ``None`` in that situation
    keeps the callers defensive without forcing them to handle import-time
    errors.
    """
    return get_migration_context()


def _get_identifier_preparer():
    """Return a SQLAlchemy identifier preparer for the active Alembic dialect.

    When no migration context is present the preparer from
    :class:`~sqlalchemy.engine.default.DefaultDialect` is used so that quoting
    continues to work in unit tests or REPL sessions.
    """
    ctx = _safe_get_context()
    dialect = getattr(ctx, "dialect", None) if ctx else None
    if dialect is None:
        dialect = DefaultDialect()
    return dialect.identifier_preparer


def _quote_identifier(identifier: str) -> str:
    """Quote a SQL identifier for safe use in raw statements.

    Parameters
    ----------
    identifier:
        The identifier to quote. Empty strings are returned unchanged so that
        callers can forward optional schema names directly.
    """
    preparer = _get_identifier_preparer()
    if not identifier:
        return identifier
    return preparer.quote_identifier(identifier)


def _quote_literal(value: str) -> str:
    """Quote an enum label as a SQL literal using single quotes.

    Embedded single quotes are doubled to preserve the literal value exactly as
    PostgreSQL expects.
    """
    escaped = value.replace("'", "''")
    return f"'{escaped}'"


def _server_version_info():
    """Return the server version tuple reported by the current dialect.

    Returns
    -------
    tuple | None
        ``(major, minor, patch)`` when the dialect exposes its version
        information or ``None`` when the details are unavailable.
    """
    ctx = _safe_get_context()
    return getattr(getattr(ctx, "dialect", None), "server_version_info", None)


def _validate_identifier(name: str, *, allow_qualified: bool = False) -> None:
    """Check that *name* satisfies PostgreSQL's identifier requirements.

    The following constraints are enforced:

    * identifiers must be strings and non-empty;
    * unqualified identifiers must not contain dots when ``allow_qualified`` is
      ``False``;
    * qualified identifiers must not contain empty segments or exceed the
      63-byte limit per segment;
    * the NUL byte (``"\x00"``) is rejected in every case.
    """

    def _check(part: str) -> None:
        if not isinstance(part, str):
            raise TypeError("Identifier must be a string.")
        if part == "":
            raise ValueError("Identifier must be a non-empty string.")
        if "\x00" in part:
            raise ValueError("Identifier must not contain NUL (\\x00) bytes.")
        if len(part.encode("utf-8")) > 63:
            raise ValueError("Identifier must be at most 63 bytes in UTF-8.")

    if not isinstance(name, str):
        raise TypeError("Identifier must be a string.")

    if not allow_qualified and "." in name:
        raise ValueError("Identifier must be unqualified; pass schema separately.")

    if allow_qualified and "." in name:
        for segment in name.split("."):
            _check(segment)
    else:
        _check(name)


def get_default_schema() -> Optional[str]:
    """Return the schema Alembic uses for its version table, when configured.

    The helpers fall back to this value when no explicit schema is supplied.
    Projects with enums stored outside the version schema should pass
    ``schema=`` explicitly.
    """
    ctx = _safe_get_context()
    if ctx is not None:
        schema = getattr(ctx, "version_table_schema", None)
        if schema:
            return schema
        # Alembic stores the parameters in ``opts`` as well.
        opts = getattr(ctx, "opts", {}) or {}
        schema = opts.get("version_table_schema")
        if schema:
            return schema
    config = getattr(context, "config", None)
    if config is not None:
        schema = config.get_main_option("version_table_schema")
        if schema:
            return schema
    return None


def _validate_enum_labels(values: Iterable[str]) -> Tuple[str, ...]:
    """Validate enum labels and return them in a tuple.

    The check rejects ``None`` values, empty strings, duplicate entries, labels
    longer than 63 bytes in UTF-8, and any label containing the NUL byte.  The
    original ordering is preserved so the resulting tuple may be fed straight
    into ``CREATE TYPE ... AS ENUM``.

    Parameters
    ----------
    values:
        Iterable of labels to validate.

    Returns
    -------
    tuple[str, ...]
        Validated labels, in the order supplied.
    """
    labels = tuple(values)
    seen = set()
    for label in labels:
        if label is None:
            raise ValueError("Enum labels must not be None.")
        if not isinstance(label, str):
            raise TypeError("Enum labels must be strings.")
        if label == "":
            raise ValueError("Enum labels must be non-empty strings.")
        if "\x00" in label:
            raise ValueError("Enum labels must not contain NUL (\\x00) bytes.")
        if len(label.encode("utf-8")) > 63:
            raise ValueError("Enum labels must be at most 63 bytes in UTF-8.")
        if label in seen:
            raise ValueError("Enum labels must be unique.")
        seen.add(label)
    return labels


def _assert_postgresql():
    """Verify that the current Alembic dialect is PostgreSQL (or unknown).

    The helpers are implemented exclusively for PostgreSQL.  When the dialect is
    known and differs, a :class:`NotImplementedError` is raised to make the
    mismatch obvious.
    """
    ctx = _safe_get_context()
    name = getattr(getattr(ctx, "dialect", None), "name", None)
    if name is not None and name != "postgresql":
        raise NotImplementedError("These enum DDL helpers are for PostgreSQL only.")


def render_enum_name(
        name: str,
        schema: Optional[str] = None
) -> str:
    """Return a schema-qualified enum type identifier with proper quoting.

    Parameters
    ----------
    name:
        Unqualified enum type name (without schema).
    schema:
        Target schema. If ``None``, uses :func:`get_default_schema` when
        available; otherwise emits an unqualified type name.
    """
    _validate_identifier(name, allow_qualified=False)
    effective_schema = get_default_schema() if schema is None else schema
    if effective_schema:
        _validate_identifier(effective_schema, allow_qualified=False)
        return f"{_quote_identifier(effective_schema)}.{_quote_identifier(name)}"
    return _quote_identifier(name)


def enum_values_clause(
        values: Iterable[str]
) -> str:
    """Return a comma-separated list of quoted enum labels.

    Each label is validated through :func:`_validate_enum_labels` before the SQL
    fragment is assembled.
    """
    validated = _validate_enum_labels(values)
    return ", ".join(_quote_literal(value) for value in validated)


def create_enum_sql(
        name: str,
        values: Iterable[str],
        *,
        schema: Optional[str] = None,
        if_not_exists: bool = False,
) -> str:
    """Construct a ``CREATE TYPE`` statement for a PostgreSQL enum type.

    Parameters
    ----------
    name:
        Unqualified enum type name.
    values:
        Iterable of enum labels; order defines the label sort order.
    schema:
        Optional target schema. If omitted, falls back to
        :func:`get_default_schema` when available.
    if_not_exists:
        Unsupported for PostgreSQL. If ``True``, a ``NotImplementedError`` is
        raised. Use :func:`create_enum_if_absent_block` for idempotency.

    Returns
    -------
    str
        The SQL statement to create the enum type.

    Notes
    -----
    PostgreSQL does **not** support ``CREATE TYPE IF NOT EXISTS``. This helper
    raises if you try to request it. For idempotent creation, prefer
    :func:`create_enum_if_absent_block`.
    """
    _assert_postgresql()
    if if_not_exists:
        raise NotImplementedError(
            "PostgreSQL lacks 'CREATE TYPE IF NOT EXISTS'; use create_enum_if_absent_block()."
        )

    _validate_identifier(name, allow_qualified=False)
    validated_values = _validate_enum_labels(values)

    parts = ["CREATE TYPE", render_enum_name(name, schema), "AS ENUM (", enum_values_clause(validated_values), ")"]
    return " ".join(parts)


def drop_enum_sql(
        name: str,
        *,
        schema: Optional[str] = None,
        if_exists: bool = True,
        cascade: bool = False,
) -> str:
    """Construct a ``DROP TYPE`` statement for a PostgreSQL enum type.

    Parameters
    ----------
    name:
        Unqualified enum type name.
    schema:
        Optional schema; if omitted uses :func:`get_default_schema` when present.
    if_exists:
        If ``True``, include ``IF EXISTS``.
    cascade:
        If ``True``, include ``CASCADE``.
    """
    _assert_postgresql()
    _validate_identifier(name, allow_qualified=False)

    parts = ["DROP TYPE"]
    if if_exists:
        parts.append("IF EXISTS")
    parts.append(render_enum_name(name, schema))
    if cascade:
        parts.append("CASCADE")
    return " ".join(parts)


def alter_enum_add_value_sql(
        name: str,
        value: str,
        *,
        schema: Optional[str] = None,
        before: Optional[str] = None,
        after: Optional[str] = None,
        if_not_exists: bool = False,
) -> str:
    """Construct an ``ALTER TYPE ... ADD VALUE`` statement.

    Parameters
    ----------
    name:
        Unqualified enum type name.
    value:
        The new label to add.
    schema:
        Optional schema; if omitted uses :func:`get_default_schema` when present.
    before, after:
        Mutually exclusive. Place the new label relative to an existing label.
    if_not_exists:
        If ``True``, prefer ``IF NOT EXISTS`` on PostgreSQL 10+; otherwise
        emit an idempotent DO block that ignores duplicate_object on older
        servers or when the version is unknown.

    Returns
    -------
    str
        The SQL statement to add the new enum value. On older servers (or
        when version is unknown) and with ``if_not_exists=True``, this will be
        a ``DO $$ ... $$ LANGUAGE plpgsql;`` block that safely ignores the
        duplicate case.

    Notes
    -----
    * The new label is not usable until the current transaction commits.
    * ``before`` and ``after`` are mutually exclusive.
    """
    _assert_postgresql()
    _validate_identifier(name, allow_qualified=False)

    if before and after:
        raise ValueError("'before' and 'after' are mutually exclusive")

    _validate_enum_labels((value,))

    if before == "" or after == "":
        raise ValueError("Position labels (before/after) must be non-empty if provided.")
    if before is not None and before == value:
        raise ValueError("'before' label cannot equal the value being added.")
    if after is not None and after == value:
        raise ValueError("'after' label cannot equal the value being added.")

    ver = _server_version_info()

    # Build the core ALTER TYPE ... ADD VALUE statement parts
    parts = ["ALTER TYPE", render_enum_name(name, schema), "ADD VALUE"]

    use_do_block = False
    if if_not_exists:
        # "IF NOT EXISTS" for enum ADD VALUE is reliably available on PG 10+.
        # For older or unknown versions, emit a DO block that ignores
        # duplicate_object to keep migrations idempotent.
        if ver and ver >= (10, 0):
            parts.append("IF NOT EXISTS")
        else:
            use_do_block = True

    parts.append(_quote_literal(value))
    if before:
        parts.extend(["BEFORE", _quote_literal(before)])
    elif after:
        parts.extend(["AFTER", _quote_literal(after)])

    stmt = " ".join(parts)

    if use_do_block:
        return f"DO $$ BEGIN {stmt}; EXCEPTION WHEN duplicate_object THEN NULL; END $$ LANGUAGE plpgsql;"

    return stmt


def enum_rename_sql(
        old_name: str,
        new_name: str,
        *, schema:
        Optional[str] = None
) -> str:
    """Construct an ``ALTER TYPE ... RENAME TO`` statement.

    Parameters
    ----------
    old_name:
        The existing, unqualified enum type name.
    new_name:
        The new, unqualified enum type name.
    schema:
        Optional schema for the *old* type.
    """
    _assert_postgresql()
    _validate_identifier(old_name, allow_qualified=False)
    _validate_identifier(new_name, allow_qualified=False)

    parts = ["ALTER TYPE", render_enum_name(old_name, schema), "RENAME TO", _quote_identifier(new_name)]
    return " ".join(parts)


def create_enum_if_absent_block(
        name: str,
        values: Sequence[str],
        *,
        schema: Optional[str] = None,
) -> str:
    """Return a PL/pgSQL ``DO`` block that creates an enum only if missing.

    PostgreSQL lacks ``CREATE TYPE IF NOT EXISTS``.  The block emitted here is
    the standard workaround; it traps ``duplicate_object`` and quietly skips the
    creation when the type already exists.

    Parameters
    ----------
    name:
        Unqualified enum type name.
    values:
        Sequence of labels; same semantics as :func:`create_enum_sql`.
    schema:
        Optional schema where the type should be created.

    Returns
    -------
    str
        A ``DO $$ ... $$ LANGUAGE plpgsql;`` block performing the create if absent.
    """
    create_stmt = create_enum_sql(name, values, schema=schema, if_not_exists=False)
    return f"DO $$ BEGIN {create_stmt}; EXCEPTION WHEN duplicate_object THEN NULL; END $$ LANGUAGE plpgsql;"


def enum_rename_value_sql(
        name: str,
        old: str,
        new: str,
        *,
        schema: Optional[str] = None,
) -> str:
    """Construct an ``ALTER TYPE ... RENAME VALUE`` statement (PostgreSQL 10+).

    Parameters
    ----------
    name:
        Unqualified enum type name.
    old, new:
        Old and new label values.
    schema:
        Optional schema where the type resides.

    Raises
    ------
    NotImplementedError
        If the server version is known and older than PostgreSQL 10.
    """
    _assert_postgresql()
    ver = _server_version_info()
    if ver and ver < (10, 0):
        raise NotImplementedError("ALTER TYPE ... RENAME VALUE requires PostgreSQL 10+.")

    _validate_identifier(name, allow_qualified=False)
    _validate_enum_labels((old, new))

    return " ".join(
        ["ALTER TYPE", render_enum_name(name, schema), "RENAME VALUE", _quote_literal(old), "TO", _quote_literal(new)]
    )


def enum_set_schema_sql(
        name: str,
        new_schema: str,
        *,
        schema: Optional[str] = None,
) -> str:
    """Construct an ``ALTER TYPE ... SET SCHEMA`` statement.

    Parameters
    ----------
    name:
        Unqualified enum type name.
    new_schema:
        The destination schema (unqualified).
    schema:
        Optional schema where the *current* type resides.
    """
    _assert_postgresql()
    _validate_identifier(name, allow_qualified=False)
    _validate_identifier(new_schema, allow_qualified=False)

    return " ".join(["ALTER TYPE", render_enum_name(name, schema), "SET SCHEMA", _quote_identifier(new_schema)])


__all__ = [
    "create_enum_if_absent_block",
    "alter_enum_add_value_sql",
    "create_enum_sql",
    "drop_enum_sql",
    "enum_rename_value_sql",
    "enum_rename_sql",
    "enum_set_schema_sql",
    "enum_values_clause",
    "get_default_schema",
    "render_enum_name",
]
