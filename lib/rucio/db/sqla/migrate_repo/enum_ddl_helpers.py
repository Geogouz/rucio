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

"""
PostgreSQL enum DDL helpers for Alembic migrations.

The functions in this module build raw SQL strings for enum-related DDL that
Alembic can execute with `alembic.op.execute`.  Each helper focuses on a
single operation so that migrations remain explicit and easy to review. The
offered helpers span the meaningful enum operations PostgreSQL allows.

Key features
------------
* Identifiers and literals are quoted through SQLAlchemy’s dialect preparer so
  that schema names and enum labels are emitted safely.
* `render_enum_name` consistently applies the default Alembic schema when
  no schema is provided explicitly.
* Enum labels are validated to catch empty values, duplicates, or items that
  exceed PostgreSQL’s 63-byte limit.
* PostgreSQL-only behaviour is enforced early; attempting to use these helpers
  on another dialect raises a clear error.
* ``create_enum_if_absent_block`` wraps enum creation in a ``DO`` block that can
  be used when migrations must be idempotent.

Operational notes
-----------------
* PostgreSQL does not implement ``CREATE TYPE IF NOT EXISTS``. Use
  `create_enum_if_absent_block` for idempotent creation.
* For ``ALTER TYPE ... ADD VALUE`` transaction semantics:
  - On PG < 12: this cannot run inside a transaction block.
  - On PG ≥ 12: it can run in a transaction, but the new value cannot be
    used until the transaction commits.
* Some helpers depend on server features that only appear in PostgreSQL 10 or
  later. When the server version is known, a helpful exception is raised if the
  operation is unsupported.
* The effective schema defaults to Alembic’s ``version_table_schema``. Supply a
  schema argument directly when enums live elsewhere.
"""

from collections.abc import Iterable, Sequence
from typing import Optional, Tuple
from sqlalchemy.engine.default import DefaultDialect
from rucio.db.sqla.migrate_repo.ddl_helpers import get_migration_context, get_effective_schema


def _get_identifier_preparer():
    """
    Return an SQLAlchemy identifier preparer for the current dialect.

    When no migration context is present, the preparer from
    `sqlalchemy.engine.default.DefaultDialect` is used so quoting continues
    to work in unit tests and REPL sessions.

    Returns
    -------
    sqlalchemy.sql.compiler.IdentifierPreparer
        The identifier preparer appropriate for the active (or default) dialect.

    Examples
    --------
    >>> preparer = _get_identifier_preparer()
    >>> preparer.quote_identifier("request_state")
    '"request_state"'
    """

    ctx = get_migration_context()
    dialect = getattr(ctx, "dialect", None) if ctx else None
    if dialect is None:
        dialect = DefaultDialect()
    return dialect.identifier_preparer


def _quote_identifier(identifier: str) -> str:
    """
    Quote a SQL identifier for safe use in raw statements.

    Parameters
    ----------
    identifier : str
        The identifier to quote. Empty strings are returned unchanged so that
        callers can forward optional schema names directly.

    Returns
    -------
    str
        The quoted identifier (or the empty string if *identifier* was empty).

    Examples
    --------
    >>> _quote_identifier("rucio")
    '"rucio"'
    >>> _quote_identifier("request_state")
    '"request_state"'
    """

    preparer = _get_identifier_preparer()
    if not identifier:
        return identifier
    return preparer.quote_identifier(identifier)


def _quote_literal(value: str) -> str:
    """
    Quote an enum label as a SQL literal using single quotes.

    Embedded single quotes are doubled to preserve the literal value exactly as
    PostgreSQL expects.

    Parameters
    ----------
    value : str
        The enum label to quote.

    Returns
    -------
    str
        The quoted SQL literal.

    Examples
    --------
    >>> _quote_literal("REPLICATING")
    "'REPLICATING'"
    >>> _quote_literal("DON'T_RETRY")
    "'DON''T_RETRY'"
    """

    escaped = value.replace("'", "''")
    return f"'{escaped}'"


def _server_version_info():
    """
    Return the server version tuple reported by the current dialect.

    Returns
    -------
    tuple[int, int, int] | tuple[int, int] | None
        The tuple provided by ``dialect.server_version_info`` (commonly
        ``(major, minor[, patch])``) when the dialect exposes version
        information, or ``None`` when the details are unavailable.

    Examples
    --------
    >>> v = _server_version_info()
    >>> if v and v >= (12, 0):  # enable PG12+ behavior
    ...     pass
    """

    ctx = get_migration_context()
    return getattr(getattr(ctx, "dialect", None), "server_version_info", None)


def _validate_identifier(name: str, *, allow_qualified: bool = False) -> None:
    """
    Validate that *name* satisfies PostgreSQL identifier requirements.

    The following constraints are enforced:

    * identifiers must be strings and non-empty;
    * unqualified identifiers must not contain dots when ``allow_qualified`` is ``False``;
    * qualified identifiers must not contain empty segments or exceed the 63-byte limit per segment;
    * the NUL byte (``"\x00"``) is rejected in every case.

    Parameters
    ----------
    name : str
        Identifier to validate. May be schema-qualified when *allow_qualified* is ``True``.
    allow_qualified : bool, optional
        Whether to allow a ``schema.name`` qualified identifier. Default is ``False``.

    Raises
    ------
    TypeError
        If *name* (or a qualified segment) is not a string.
    ValueError
        If *name* is empty, contains the NUL byte, is qualified when not allowed,
        or any segment exceeds 63 bytes.

    Examples
    --------
    >>> _validate_identifier("request_state")
    >>> _validate_identifier("rucio.request_state", allow_qualified=True)
    """

    def _check(part: str) -> None:
        """
        Validate a single, unqualified identifier segment.

        Parameters
        ----------
        part : str
            The identifier to validate. Must be a non-empty string with no NUL bytes
            and at most 63 bytes when encoded as UTF-8.

        Raises
        ------
        TypeError
            If ``part`` is not a string.
        ValueError
            If ``part`` is empty, contains NUL bytes, or exceeds the 63‑byte
            PostgreSQL name length limit (63 bytes).
        """
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


def _validate_enum_labels(values: Iterable[str]) -> Tuple[str, ...]:
    """
    Validate enum labels and return them in a tuple.

    The check rejects ``None`` values, empty strings, duplicate entries, labels
    longer than 63 bytes in UTF-8, and any label containing the NUL byte. The
    original ordering is preserved so the resulting tuple may be fed straight
    into ``CREATE TYPE ... AS ENUM``.

    Parameters
    ----------
    values : Iterable[str]
        Iterable of labels to validate.

    Returns
    -------
    tuple[str, ...]
        Validated labels, in the order supplied.

    Raises
    ------
    TypeError
        If any label is not a string (excluding ``None`` values, which raise
        ``ValueError``).
    ValueError
        If any label is ``None`` or empty, contains the NUL byte, exceeds 63
        bytes in UTF‑8, or duplicates another label.

    Examples
    --------
    >>> _validate_enum_labels(["FILE", "DATASET", "CONTAINER"])
    ('FILE', 'DATASET', 'CONTAINER')
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
    """
    Ensure the active Alembic dialect is PostgreSQL (or unknown).

    The helpers are implemented exclusively for PostgreSQL. When the dialect is
    known and differs, a clear exception is raised.

    Raises
    ------
    NotImplementedError
        If the current dialect is known and not PostgreSQL.

    Examples
    --------
    >>> _assert_postgresql()  # no-op under PostgreSQL/unknown
    """

    ctx = get_migration_context()
    name = getattr(getattr(ctx, "dialect", None), "name", None)
    if name is not None and name != "postgresql":
        raise NotImplementedError("These enum DDL helpers are for PostgreSQL only.")


def render_enum_name(
        name: str,
        schema: Optional[str] = None
) -> str:
    """
    Render a schema-qualified, safely-quoted enum type name.

    What this does
    --------------
    * Validates that ``name`` is an unqualified PostgreSQL identifier
      (no dots, non-empty, <= 63 bytes, no NULs).
    * Chooses the effective schema:
      - ``schema`` argument if provided and non-empty, otherwise
      - `get_effective_schema` when available, else no schema.
    * Quotes schema and name with SQLAlchemy's identifier preparer so the
      result is safe to interpolate into raw SQL.

    Parameters
    ----------
    name : str
        Unqualified enum type name (e.g. ``"request_state"``).
    schema : str | None, optional
        Target schema. If ``None``, uses `get_effective_schema` when available;
        otherwise emits an unqualified type name.

    Returns
    -------
    str
        The fully rendered enum type identifier, quoted and optionally
        schema-qualified (i.e. either ``"schema"."name"`` or just ``"name"``).

    Raises
    ------
    TypeError
        If ``name`` or a non‑empty ``schema`` fails identifier type validation.
    ValueError
        If ``name`` is an invalid identifier value (empty string, contains NUL, or exceeds
        the 63‑byte limit), or a non‑empty ``schema`` violates the same constraints.

    Examples
    --------
    >>> render_enum_name("request_state", schema="rucio")
    '"rucio"."request_state"'
    >>> # falls back to Alembic's version_table_schema when present
    >>> render_enum_name("request_state")
    '"rucio"."request_state"'
    """

    _validate_identifier(name, allow_qualified=False)
    effective_schema = get_effective_schema() if schema is None else schema
    if effective_schema:
        _validate_identifier(effective_schema, allow_qualified=False)
        return f"{_quote_identifier(effective_schema)}.{_quote_identifier(name)}"
    return _quote_identifier(name)


def enum_values_clause(
        values: Iterable[str]
) -> str:
    """
    Return a comma-separated list of quoted enum labels.

    All labels are validated and then single-quoted for SQL, producing a
    fragment suitable for interpolating into ``CREATE TYPE ... AS ENUM``.

    Parameters
    ----------
    values : Iterable[str]
        Labels to include. Order is preserved and defines the enum's sort
        order within PostgreSQL.

    Returns
    -------
    str
        A fragment of the form ``'value1', 'value2', ...``.

    Raises
    ------
    TypeError
        If any label is not a string.
    ValueError
        If any label is ``None`` or empty, contains the NUL byte, exceeds 63
        bytes in UTF‑8, or duplicates another label.

    See Also
    --------
    create_enum_sql : Builds the full ``CREATE TYPE ... AS ENUM (...)`` SQL.

    Examples
    --------
    >>> enum_values_clause(["QUEUED", "SUBMITTED", "DONE"])
    "'QUEUED', 'SUBMITTED', 'DONE'"
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
    """
    Build a ``CREATE TYPE ... AS ENUM`` statement for PostgreSQL.

    This function only *constructs* SQL — it does not execute anything.
    Identifiers are validated and quoted; labels are validated, quoted and
    kept in the given order (which defines the enum's collation order).

    Parameters
    ----------
    name : str
        Unqualified enum type name.
    values : Iterable[str]
        Iterable of enum labels. Order defines the enum's sort ordering.
    schema : str | None, optional
        Target schema. If omitted, `get_effective_schema` is used when available;
        otherwise the type name is unqualified.
    if_not_exists : bool, optional
        Not implemented. Kept for parity with other helpers.
        Use :func:`create_enum_if_absent_block` for idempotent creation.

    Returns
    -------
    str
        A single SQL statement, for example:
        ``CREATE TYPE "rucio"."request_state" AS ENUM ('QUEUED', 'SUBMITTED', 'DONE')``.

    Raises
    ------
    NotImplementedError
        If ``if_not_exists=True`` (use :func:`create_enum_if_absent_block` instead).
    TypeError
        If ``name`` or any label fails type validation.
    ValueError
        If ``name`` or any label fails value validation (for example empty
        identifiers, ``None`` labels, NUL bytes, excessive length, or duplicates).

    Examples
    --------
    >>> from alembic import op
    >>> sql = create_enum_sql(
    ...     "request_state",
    ...     ["QUEUED", "SUBMITTED", "DONE", "FAILED"],
    ...     schema="rucio",
    ... )
    >>> op.execute(sql)
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
    """
    Build a ``DROP TYPE`` statement for a PostgreSQL enum.

    Parameters
    ----------
    name : str
        Unqualified enum type name.
    schema : str | None, optional
        Schema of the type. If omitted, `get_effective_schema` is used when available;
        otherwise an unqualified name is emitted.
    if_exists : bool, default True
        Include ``IF EXISTS`` so the statement is idempotent.
    cascade : bool, default False
        Append ``CASCADE`` to drop dependent objects (use with care).

    Returns
    -------
    str
        The assembled SQL, e.g. ``DROP TYPE IF EXISTS "dev"."status"``.

    Raises
    ------
    TypeError
        If ``name`` is not a string (including ``None``).
    ValueError
        If ``name`` is an invalid identifier value (empty string, contains NUL,
        or exceeds the 63‑byte limit).

    Examples
    --------
    >>> from alembic import op
    >>> # Safe drop (no error if missing)
    >>> op.execute(drop_enum_sql("request_state", schema="rucio"))
    >>> # Force drop of dependents
    >>> op.execute(drop_enum_sql("request_state", schema="rucio", cascade=True))
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
    """
    Build an ``ALTER TYPE ... ADD VALUE`` statement (PostgreSQL ≥ 10 is preferred).

    The statement can place the new label before or after an existing label.
    On newer PostgreSQL versions, ``IF NOT EXISTS`` may be used to make the
    operation idempotent when the server supports it.

    Parameters
    ----------
    name : str
        Unqualified enum type name.
    value : str
        New label to add.
    before : str | None, optional
        Insert the new label before this existing label.
    after : str | None, optional
        Insert the new label after this existing label.
    schema : str | None, optional
        Optional schema where the type exists.
    if_not_exists : bool, optional
        If ``True``, prefer ``IF NOT EXISTS`` on PostgreSQL 9.3+; otherwise
        emit an idempotent DO block that ignores duplicate_object on older
        servers or when the version is unknown.

    Returns
    -------
    str
        A single SQL statement, e.g.:
        ``ALTER TYPE "rucio"."request_state" ADD VALUE IF NOT EXISTS 'ARCHIVED' AFTER 'DONE'``,
        or a small ``DO $$ ... $$`` block on older servers.

    Raises
    ------
    ValueError
        If both ``before`` and ``after`` are provided; if a position label is empty;
        if a position label equals ``value``; or if ``value`` itself is invalid
        (``None``, empty, contains NUL, exceeds 63 bytes).

    Notes
    -----
    * On PostgreSQL < 12, ``ALTER TYPE ... ADD VALUE`` cannot run inside a transaction block.
      On 12+, it can, but the new label remains unusable until the transaction commits.

    Examples
    --------
    >>> from alembic import op
    >>> # Add a value after an existing label
    >>> op.execute(alter_enum_add_value_sql("request_state", "ARCHIVED", after="DONE", schema="rucio"))
    >>> # Idempotent add (uses native IF NOT EXISTS when available; otherwise a DO block)
    >>> op.execute(alter_enum_add_value_sql("request_state", "RETRYING", before="SUBMITTED", schema="rucio", if_not_exists=True))
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
        # "IF NOT EXISTS" for enum ADD VALUE is available on PG 9.3+.
        # For older or unknown versions, emit a DO block that ignores
        # duplicate_object to keep migrations idempotent.
        if ver and ver >= (9, 3):
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
        *,
        schema: Optional[str] = None
) -> str:
    """
    Build an ``ALTER TYPE ... RENAME TO`` statement to rename the enum type.

    Parameters
    ----------
    old_name : str
        The current (unqualified) type name.
    new_name : str
        The new (unqualified) type name.
    schema : str | None, optional
        Optional schema for the *old* type.

    Returns
    -------
    str
        SQL such as ``ALTER TYPE "rucio"."request_state" RENAME TO "request_status"``.

    Raises
    ------
    TypeError
        If either name fails type validation.
    ValueError
        If identifiers are empty/invalid.

    Examples
    --------
    >>> from alembic import op
    >>> # Rename the enum type itself
    >>> op.execute(enum_rename_sql("request_state", "request_status", schema="rucio"))
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
    """
    Return a PL/pgSQL ``DO`` block that creates an enum only if missing.

    PostgreSQL lacks ``CREATE TYPE IF NOT EXISTS``. The block emitted here is
    the standard workaround; it traps ``duplicate_object`` and quietly skips the
    creation when the type already exists.

    Parameters
    ----------
    name : str
        Unqualified enum type name.
    values : Sequence[str]
        Enum labels. Order defines the enum's sort ordering.
    schema : str | None, optional
        Optional schema where the type should be created.

    Returns
    -------
    str
        A ``DO $$ ... $$`` block that creates the type if it does not exist.

    Caveats
    -------
    This pattern only guards against the presence of a type with the same name.
    It does not verify that an existing type's label set matches ``values``.
    If you need to reconcile label differences, use the other helpers to add/rename labels.

    Examples
    --------
    >>> from alembic import op
    >>> # Create the enum if it's not already present (idempotent)
    >>> op.execute(create_enum_if_absent_block("did_type", ["FILE", "DATASET", "CONTAINER"], schema="rucio"))
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
    """
    Build an ``ALTER TYPE ... RENAME VALUE`` statement (PostgreSQL 10+).

    Parameters
    ----------
    name : str
        Unqualified enum type name.
    old : str
        Existing label to rename.
    new : str
        New label.
    schema : str | None, optional
        Schema containing the type. Falls back to `get_effective_schema`.

    Returns
    -------
    str
        SQL such as
        ``ALTER TYPE "rucio"."request_state" RENAME VALUE 'SUBMITTED' TO 'IN_PROGRESS'``.

    Raises
    ------
    NotImplementedError
        If the active dialect is known and not PostgreSQL, or if the server
        version is known and older than PostgreSQL 10.
    TypeError
        If ``name`` is not a string; or if either label (``old`` or ``new``) is not a string.
    ValueError
        If a label (``old`` or ``new``) is ``None`` or empty, contains NUL, exceeds
        63 bytes, or if both labels are identical (duplicate within the provided labels).

    Examples
    --------
    >>> from alembic import op
    >>> op.execute(enum_rename_value_sql("request_state", "SUBMITTED", "IN_PROGRESS", schema="rucio"))
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
    """
    Build an ``ALTER TYPE ... SET SCHEMA`` statement to move the enum type.

    Parameters
    ----------
    name : str
        Unqualified enum type name.
    new_schema : str
        Destination schema (unqualified).
    schema : str | None, optional
        Current schema of the type. Falls back to `get_effective_schema`.

    Returns
    -------
    str
        SQL such as
        ``ALTER TYPE "public"."request_state" SET SCHEMA "rucio"``.

    Raises
    ------
    TypeError
        If ``name`` or ``new_schema`` fails type validation.
    ValueError
        If ``name`` or ``new_schema`` is an invalid identifier value
        (empty string, contains NUL, or exceeds the 63‑byte limit).

    Examples
    --------
    >>> from alembic import op
    >>> # Move the enum type to a different schema
    >>> op.execute(enum_set_schema_sql("request_state", "rucio", schema="public"))
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
    "render_enum_name",
]