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
Utility helpers that keep Alembic DDL migrations predictable.

The functions collected here smooth over backend differences so that schema changes
can be expressed once and executed safely across Rucio's supported databases.

Key features
------------
* Uses SQLAlchemy’s dialect preparer to quote identifiers correctly.
* Dialect detection works both online and offline, enabling migrations to run
  deterministically even when no live connection is available.
* ``try_drop_constraint`` and ``try_drop_index`` treat missing objects as a no-op,
  so rerunning migrations is safe.
* Quotes literals to keep PL/pgSQL and PL/SQL code robust with unusual names.

Operational notes
--------------------
* Error handling uses known message fragments to spot “already missing” cases.
  Review and extend the ``MYSQL_*_MISSING_TOKENS`` constants and
  ``ORACLE_INDEX_MISSING_TOKENS`` when database versions or locales change,
  otherwise previously ignored errors may surface as `RuntimeError`.
* Unsupported dialects are skipped (logged as no-ops). Backends outside the
  supported set (currently MySQL/MariaDB, PostgreSQL, and Oracle) are logged
  and skipped rather than raising. Validate that your migrations run on an expected
  backend; otherwise schema changes might appear to succeed while having no effect.
* Direct DDL requires privileges. Ensure the DB user has rights for the emitted
  `alembic.op` calls or raw ``ALTER``/``DROP`` statements.
"""

import logging
from collections.abc import Iterable
from typing import Optional, Tuple

from alembic import context, op
from alembic.runtime.migration import MigrationContext
from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.exc import DatabaseError
from sqlalchemy.sql.compiler import IdentifierPreparer

logger = logging.getLogger(__name__)

MYSQL_GENERAL_MISSING_TOKENS: Tuple[str, ...] = (
    "doesn't exist",
    "does not exist",
    "unknown constraint",
    "unknown index",
    "unknown key",
    "check that column/key exists",
    "check that it exists",
)

MYSQL_CONSTRAINT_MISSING_TOKENS: Tuple[str, ...] = (
    *MYSQL_GENERAL_MISSING_TOKENS,
    "can't drop foreign key",
    "can't drop constraint",
)

MYSQL_INDEX_MISSING_TOKENS: Tuple[str, ...] = (
    *MYSQL_GENERAL_MISSING_TOKENS,
    "no such index",
    "can't drop index",
)

MYSQL_PRIMARY_KEY_MISSING_TOKENS: Tuple[str, ...] = (
    *MYSQL_GENERAL_MISSING_TOKENS,
    "can't drop primary key",
)

ORACLE_INDEX_MISSING_TOKENS: Tuple[str, ...] = (
    "specified index does not exist",
    "ora-01418",
)


def _matches_any(
        message: Optional[str],
        tokens: Iterable[str]
) -> bool:
    """
    Return ``True`` when any token is present in *message*.

    Parameters
    ----------
    message : str | None
        The string to search. ``None`` is treated as the empty string so that
        unexpected exceptions do not bubble further up the stack.
    tokens : Iterable[str]
        Search terms to look for. The comparison is case-insensitive so callers
        can provide tokens in any casing. Falsy tokens are normalised to the
        empty string, which matches any message; callers should pre-filter such
        values if that behaviour is undesirable.

    Returns
    -------
    bool
        ``True`` if at least one token is present in ``message``; otherwise ``False``.
    """

    msg = (message or "").lower()
    return any((token or "").lower() in msg for token in tokens)


def _qliteral(
        value: Optional[str]
) -> str:
    """
    Wrap *value* in single quotes for SQL/PL blocks.

    Parameters
    ----------
    value : str | None
        The literal value to quote. Embedded single quotes are doubled to
        preserve the original contents.

    Returns
    -------
    str
        The quoted literal. ``None`` is emitted as the SQL ``NULL`` literal.
    """

    if value is None:
        return "NULL"
    return "'" + value.replace("'", "''") + "'"


def _dialect_name() -> Optional[str]:
    """
    Internal shorthand for `_get_current_dialect`, normalising "mariadb" to "mysql".
    """

    name = _get_current_dialect()
    if name == "mariadb":
        return "mysql"
    return name


def _preparer() -> IdentifierPreparer:
    """
    Return the current dialect's identifier preparer.

    Falls back to SQLAlchemy's `DefaultDialect` preparer when no migration context
    is active, producing the dialect's generic (double-quoted) identifier rules
    rather than any Alembic-configured backend-specific quoting.
    """

    ctx = get_migration_context()
    dialect = getattr(ctx, "dialect", None) or DefaultDialect()
    return dialect.identifier_preparer


def _qident(
        name: Optional[str]
) -> str:
    """
    Quote *name* using the active dialect's identifier rules.

    Parameters
    ----------
    name : str | None
        The identifier to quote. Falsy values (``None`` or the empty string)
        result in ``""`` so the helper can be used directly with optional values.

    Returns
    -------
    str
        The quoted identifier or the empty string when ``name`` is falsy.
    """

    if not name:
        return ""
    return _preparer().quote_identifier(name)


def _quoted_table(
        table_name: str,
        schema: Optional[str]
) -> str:
    """
    Quote *table_name* and *schema* for SQL emission.
    """

    if schema:
        return f"{_qident(schema)}.{_qident(table_name)}"
    return _qident(table_name)


def _quoted_index(
        index_name: str,
        schema: Optional[str]
) -> str:
    """
    Quote *index_name* and *schema* for SQL emission.
    """

    if schema:
        return f"{_qident(schema)}.{_qident(index_name)}"
    return _qident(index_name)


def _get_current_dialect() -> Optional[str]:
    """
    Return the name of the database dialect used for the current run.

    The lookup prefers an active `MigrationContext` but gracefully
    falls back to Alembic configuration when no context is present (e.g.
    offline migrations): it checks ``context.config`` for ``dialect`` and
    finally derives the name from ``sqlalchemy.url`` if needed.

    Returns
    -------
    str | None
        The dialect name recognised by SQLAlchemy (for example ``"mysql"`` or
        ``"postgresql"``) or ``None`` when no hint can be derived.
    """

    ctx = get_migration_context()
    dialect = getattr(ctx, "dialect", None)
    name = getattr(dialect, "name", None)
    if name:
        return name

    cfg = getattr(context, "config", None)
    if cfg is not None:
        name = cfg.get_main_option("dialect")
        if name:
            return name

        url = cfg.get_main_option("sqlalchemy.url")
        if url:
            parsed = url.split(":", 1)[0]
            name = parsed.split("+", 1)[0]
            if name:
                return name
    return None


def get_migration_context() -> Optional[MigrationContext]:
    """
    Return the active Alembic `alembic.runtime.migration.MigrationContext`, if any.

    Tries ``alembic.op.get_context()`` first (typical for online migrations),
    then falls back to ``alembic.context.get_context()`` (useful in some offline
    or test setups). If neither is available, returns ``None``.

    Returns
    -------
    MigrationContext | None
        The context currently bound to Alembic operations, or ``None`` when no
        migration is running.

    Notes
    -----
    * This helper never creates or configures a context; it only *retrieves* an
      already-active one if present.
    * Safe to call outside of Alembic migrations (it simply returns ``None``).
    """

    try:
        return op.get_context()
    except Exception:
        pass

    try:
        return context.get_context()
    except Exception:
        return None


def get_effective_schema() -> Optional[str]:
    """
    Return the schema Alembic treats as the default for migrations, if any.

    When helpers in this module are called without a ``schema=`` argument,
    they fall back to this value so that objects live alongside your Alembic
    version table by default.

    Resolution order (first non‑empty wins)
    --------------------------------------
    1. ``alembic.runtime.migration.MigrationContext.version_table_schema``
    2. ``MigrationContext.opts['version_table_schema']``
    3. ``alembic.context.config.get_main_option('version_table_schema')``

    Returns
    -------
    str | None
        The configured version‑table schema, or ``None`` when no default is set.

    Notes
    -----
    This function inspects only the active Alembic context/configuration; it
    does not query the server.
    """

    ctx = get_migration_context()
    if ctx is not None:
        schema = getattr(ctx, "version_table_schema", None)
        if schema:
            return schema
        opts = getattr(ctx, "opts", {}) or {}
        schema = opts.get("version_table_schema")
        if schema:
            return schema
    cfg = getattr(context, "config", None)
    if cfg is not None:
        schema = cfg.get_main_option("version_table_schema")
        if schema:
            return schema
    return None


def is_current_dialect(
        *dialect_names: str
) -> bool:
    """
    Return ``True`` if the *active* database dialect matches any provided names.

    Parameters
    ----------
    *dialect_names : str
        One or more dialect names to test (e.g. ``"postgresql"``, ``"mysql"``, ``"oracle"``, ``"sqlite"``).
        Matching is case‑insensitive but otherwise literal; backends that report ``"mariadb"``
        do not match ``"mysql"`` unless you include ``"mariadb"`` explicitly.

    Returns
    -------
    bool
        ``True`` when the active dialect name is one of ``dialect_names``, else ``False``.

    How the dialect is resolved
    ---------------------------
    1. If an `alembic.runtime.migration.MigrationContext` is active, use ``context.dialect.name``.
    2. Otherwise, consult ``alembic.context.config``:
       * the ``dialect`` main option, if set
       * otherwise the scheme from ``sqlalchemy.url`` (e.g. ``postgresql+psycopg`` → ``postgresql``)
    3. If no hint can be found, the function returns ``False``.

    Examples
    --------
    >>> if is_current_dialect("postgresql", "sqlite"):
    ...     # Postgres or SQLite specific path
    ...     pass
    """

    name = _get_current_dialect()
    if not name:
        return False
    wanted = {d.lower() for d in dialect_names}
    return name.lower() in wanted


def qualify_table(
        table_name: str,
        schema: Optional[str] = None
) -> str:
    """
    Render a quoted table identifier, schema‑qualified when configured.

    Parameters
    ----------
    table_name : str
        Unqualified table name to quote.
    schema : str | None, optional
        Schema to qualify with. When omitted, uses `get_effective_schema`.
        If that also yields ``None``, the result is unqualified.

    Returns
    -------
    str
        Either ``"schema"."table"`` or just ``"table"`` — quoted using SQLAlchemy's
        `sqlalchemy.sql.compiler.IdentifierPreparer` for the active dialect.

    Notes
    -----
    * This helper performs no server round‑trips.
    * Useful when emitting raw SQL via ``op.execute`` that must be portable
      across PostgreSQL, MySQL/MariaDB, Oracle, and SQLite.

    Examples
    --------
    >>> qualify_table("users", schema="dev")
    '"dev"."users"'
    >>> # falls back to Alembic's version_table_schema when present
    >>> qualify_table("dids")
    '"dev"."dids"'
    """

    if schema is None:
        schema = get_effective_schema()
    return _quoted_table(table_name, schema)


def qualify_index(
        index_name: str,
        schema: Optional[str] = None
) -> str:
    """
    Render a quoted index identifier, schema‑qualified when appropriate.

    Parameters
    ----------
    index_name : str
        Unqualified index name to quote.
    schema : str | None, optional
        Schema to qualify with. When omitted, uses `get_effective_schema`.
        Some dialects (e.g. MySQL/MariaDB) do not schema‑scope indexes in SQL
        statements; this helper still produces a safe quoted name for use in
        statements such as ``DROP INDEX`` (and you may still need to pass the
        table name separately on those dialects).

    Returns
    -------
    str
        Either ``"schema"."index_name"`` or just ``"index_name"`` — quoted using
        SQLAlchemy's `sqlalchemy.sql.compiler.IdentifierPreparer`.

    Example
    -------
    >>> qualify_index("DIDS_PK", schema="dev")
    '"dev"."DIDS_PK"'
    """

    if schema is None:
        schema = get_effective_schema()
    return _quoted_index(index_name, schema)


def try_drop_constraint(
        constraint_name: str,
        table_name: str,
        *,
        type_: Optional[str] = None,
) -> None:
    """
    Drop a named constraint if it exists, without failing on "missing" cases.

    Parameters
    ----------
    constraint_name : str
        Name of the constraint to remove.
    table_name : str
        Table hosting the constraint.
    type_ : str | None, optional
        Explicit constraint type (primarily for MySQL/MariaDB). Providing this lets
        Alembic emit the appropriate ``DROP`` statement without additional probing.
        Canonical Alembic values include ``"foreignkey"``, ``"check"``, ``"unique"``
        and ``"primary"``. On MySQL/MariaDB, when ``type_`` is provided, the call is
        delegated to `alembic.op.drop_constraint` with the type passed through unchanged.
        When ``type_`` is omitted on those backends, the helper probes by trying
        (in order): ``DROP FOREIGN KEY``, then ``DROP INDEX``, then ``DROP CHECK``.
        Legacy "syntax" errors for ``DROP CHECK`` (older MySQL/MariaDB) are treated as
        harmless during probing. On PostgreSQL the helper emits raw SQL and ignores
        ``type_``. Other backends fall back to Alembic with ``type_`` passed through.

    Behavior by dialect
    -------------------
    * PostgreSQL : emits ``ALTER TABLE IF EXISTS <tbl> DROP CONSTRAINT IF EXISTS <name>``.
    * MySQL / MariaDB: – uses the appropriate ``ALTER TABLE ... DROP ...`` form;
      probes the kind when ``type_`` is not supplied and treats standard "missing"
      errors as a no‑op.
    * SQLite, Oracle, others : – delegates to Alembic (``op.drop_constraint``)
      with ``type_`` as given.

    Returns
    -------
    None

    Raises
    ------
    RuntimeError
        If an unexpected database error occurs (i.e., not a recognised "missing"
        constraint condition).

    Notes
    -----
    * Idempotent by construction : repeated calls leave the schema unchanged.
    * Error strings that indicate a harmless "missing" case are normalised across
      backends and tolerated.
    * Callers should drop dependent objects (e.g., FKs referencing a PK) first,
      mirroring common migration ordering.

    Examples
    --------
    >>> # Works whether or not the FK exists (MySQL/MariaDB will probe)
    >>> try_drop_constraint("fk_orders_user_id", "orders")
    >>> # If you know the type, supply it to avoid probing on MySQL/MariaDB
    >>> try_drop_constraint("fk_orders_user_id", "orders", type_="foreignkey")
    """

    dialect = _dialect_name()
    schema = get_effective_schema()
    quoted_table = _quoted_table(table_name, schema)
    plain_table = f"{schema}.{table_name}" if schema else table_name
    quoted_constraint = _qident(constraint_name)
    if dialect == "postgresql":
        op.execute(
            f"ALTER TABLE IF EXISTS {quoted_table} "
            f"DROP CONSTRAINT IF EXISTS {quoted_constraint}"
        )
        return

    if dialect == "mysql":
        if type_:
            try:
                op.drop_constraint(constraint_name, table_name, type_=type_)
            except (DatabaseError, ValueError) as exc:
                message = str(exc).lower()
                if not _matches_any(message, MYSQL_CONSTRAINT_MISSING_TOKENS):
                    raise RuntimeError(exc) from exc
                logger.debug(
                    "Constraint %s on %s already missing (dialect=mysql, type=%s)",
                    constraint_name,
                    plain_table,
                    type_,
                )
            return

        statements = (
            (
                f"ALTER TABLE {quoted_table} DROP FOREIGN KEY {quoted_constraint}",
                False,
            ),
            (
                f"ALTER TABLE {quoted_table} DROP INDEX {quoted_constraint}",
                False,
            ),
            (
                f"ALTER TABLE {quoted_table} DROP CHECK {quoted_constraint}",
                True,
            ),
        )
        executed = False
        for stmt, allow_syntax in statements:
            try:
                op.execute(stmt)
                executed = True
                return
            except DatabaseError as exc:
                message = str(exc).lower()
                tolerated = _matches_any(message, MYSQL_CONSTRAINT_MISSING_TOKENS)
                if allow_syntax:
                    tolerated = tolerated or "syntax" in message
                if not tolerated:
                    raise RuntimeError(exc) from exc
        if not executed:
            logger.debug(
                "Constraint %s on %s not dropped; treated as already missing",
                constraint_name,
                plain_table,
            )
        return

    try:
        op.drop_constraint(constraint_name, table_name, type_=type_)
    except (DatabaseError, ValueError) as exc:
        message = str(exc).lower()
        tolerated = (
                "nonexistent constraint" in message
                or "undefined object" in message
                or "undefinedobject" in message
                or "no such constraint" in message
                or _matches_any(message, MYSQL_CONSTRAINT_MISSING_TOKENS)
        )
        if not tolerated:
            raise RuntimeError(exc) from exc
        logger.debug(
            "Constraint %s on %s already missing; treated as no-op",
            constraint_name,
            plain_table,
        )


def try_drop_index(
        index_name: str,
        table_name: Optional[str] = None
) -> None:
    """
    Drop a named index (tolerating missing objects) across backends.

    Parameters
    ----------
    index_name : str
        Name of the index to remove.
    table_name : str | None, optional
        Table containing the index. MySQL/MariaDB requires this argument
        for ``DROP INDEX`` statements; omitting it raises `ValueError`
        with the message "MySQL requires table_name for DROP INDEX" on
        those dialects (the helper normalizes MariaDB to MySQL).

    Behavior by dialect
    -------------------
    * PostgreSQL : – emits ``DROP INDEX IF EXISTS <schema>.<index>``.
    * MySQL / MariaDB: – emits ``DROP INDEX <index> ON <table>``.
    * Others (including SQLite): – delegates to Alembic where possible and
      tolerates recognised "already missing" errors.

    Returns
    -------
    None

    Raises
    ------
    ValueError
        If ``table_name`` is omitted on MySQL/MariaDB.
    RuntimeError
        If an unexpected database error occurs (i.e., not a recognised "missing"
        index condition).

    Notes
    -----
    Missing indexes are treated as a successful no-op so that repeated
    migrations remain idempotent. Errors outside the known "missing"
    cases propagate (wrapped in `RuntimeError` where the helper intercepts them).

    Examples
    --------
    >>> # PostgreSQL
    >>> try_drop_index("ix_users_email")
    >>> # MySQL/MariaDB
    >>> try_drop_index("ix_users_email", table_name="users")
    """

    dialect = _dialect_name()
    schema = get_effective_schema()
    quoted_index = _quoted_index(index_name, schema)
    quoted_table = _quoted_table(table_name, schema) if table_name else None
    plain_index = f"{schema}.{index_name}" if schema else index_name
    plain_table = f"{schema}.{table_name}" if schema and table_name else table_name

    if dialect == "postgresql":
        op.execute(f"DROP INDEX IF EXISTS {quoted_index}")
        return

    if dialect == "mysql":
        if not table_name:
            raise ValueError("MySQL requires table_name for DROP INDEX")
        try:
            op.execute(f"DROP INDEX {_qident(index_name)} ON {quoted_table}")
        except DatabaseError as exc:
            message = str(exc).lower()
            tolerated = _matches_any(message, MYSQL_INDEX_MISSING_TOKENS)
            if not tolerated:
                raise RuntimeError(exc) from exc
            logger.debug(
                "Index %s on %s already missing (dialect=mysql)",
                plain_index,
                plain_table,
            )
        return

    try:
        op.drop_index(index_name, table_name=table_name)
    except (DatabaseError, ValueError) as exc:
        message = str(exc).lower()
        tolerated = (
                "nonexistent" in message
                or "undefined object" in message
                or "undefinedobject" in message
                or _matches_any(message, MYSQL_INDEX_MISSING_TOKENS)
                or _matches_any(message, ORACLE_INDEX_MISSING_TOKENS)
        )
        if not tolerated:
            raise RuntimeError(exc) from exc
        logger.debug(
            "Index %s on %s already missing; treated as no-op",
            plain_index,
            plain_table,
        )


def drop_current_primary_key(
        table_name: str
) -> None:
    """
    Drop the current primary key on ``table_name`` if one exists.

    Parameters
    ----------
    table_name : str
        The table whose primary key should be removed.

    Behavior by dialect
    -------------------
    * PostgreSQL : – looks up the PK name from catalogs and drops it only if present
      (via a small ``DO $$`` block for safety).
    * Oracle : – queries catalogs to discover and drop the PK if present.
    * MySQL / MariaDB: – executes ``ALTER TABLE <table> DROP PRIMARY KEY`` and
      tolerates the standard "already missing" errors.
    * Others : – logged as a no‑op.

    Returns
    -------
    None

    Raises
    ------
    RuntimeError
        On MySQL/MariaDB, if an unexpected database error occurs (i.e., not a recognised
        "missing primary key" condition).

    Notes
    -----
    * Idempotent : repeating the call leaves the database unchanged.
    * Drop dependent foreign keys first to mirror common migration ordering.

    Examples
    --------
    >>> drop_current_primary_key("orders")
    """

    dialect = _dialect_name()
    schema = get_effective_schema()
    plain_table = f"{schema}.{table_name}" if schema else table_name

    if dialect == "mysql":
        quoted_table = _quoted_table(table_name, schema)
        try:
            op.execute(f"ALTER TABLE {quoted_table} DROP PRIMARY KEY")
        except DatabaseError as exc:
            message = str(exc).lower()
            tolerated = _matches_any(message, MYSQL_PRIMARY_KEY_MISSING_TOKENS)
            if not tolerated:
                raise RuntimeError(exc) from exc
            logger.debug(
                "Primary key on %s already missing (dialect=mysql)",
                plain_table,
            )
        return

    if dialect == "postgresql":
        schema_init = _qliteral(schema) if schema else "NULL"
        op.execute(
            f"""
        DO $$
        DECLARE
            schemaname text := {schema_init};
            pkname     text;
            tblname    text := {_qliteral(table_name)};
        BEGIN
            IF schemaname IS NULL THEN
                schemaname := current_schema();
            END IF;

            SELECT c.conname
              INTO pkname
              FROM pg_constraint c
              JOIN pg_class      r ON r.oid = c.conrelid
              JOIN pg_namespace  n ON n.oid = r.relnamespace
             WHERE c.contype = 'p'
               AND n.nspname = schemaname
               AND r.relname = tblname;

            IF pkname IS NOT NULL THEN
                EXECUTE format('ALTER TABLE %I.%I DROP CONSTRAINT %I',
                               schemaname, tblname, pkname);
            END IF;
        END$$;
        """
        )
        return

    if dialect == "oracle":
        owner_expr = (
            f"UPPER({_qliteral(schema)})" if schema else "SYS_CONTEXT('USERENV','CURRENT_SCHEMA')"
        )
        tab_expr = f"UPPER({_qliteral(table_name)})"
        quoted_table = _quoted_table(table_name, schema)

        op.execute(
            f"""
        DECLARE
            v_cnt NUMBER;
        BEGIN
            SELECT COUNT(*)
              INTO v_cnt
              FROM ALL_CONSTRAINTS
             WHERE OWNER = {owner_expr}
               AND TABLE_NAME = {tab_expr}
               AND CONSTRAINT_TYPE = 'P';

            IF v_cnt > 0 THEN
                EXECUTE IMMEDIATE 'ALTER TABLE {quoted_table} DROP PRIMARY KEY';
            END IF;
        END;
        """
        )
        return

    logger.debug(
        "Primary key drop on %s skipped; unsupported dialect treated as no-op",
        plain_table,
    )


__all__ = [
    "get_migration_context",
    "get_effective_schema",
    "is_current_dialect",
    "qualify_index",
    "qualify_table",
    "try_drop_constraint",
    "try_drop_index",
    "drop_current_primary_key",
]
