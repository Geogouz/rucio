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

"""Utility helpers that keep Alembic DDL migrations predictable.

The functions collected here smooth over backend differences so that schema
changes can be expressed once and executed safely across Rucio's supported
databases.
"""

import logging
from collections.abc import Iterable
from typing import Optional, Tuple

from alembic import context, op
from alembic.runtime.migration import MigrationContext
from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.exc import DatabaseError

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


def _matches_any(message: str, tokens: Iterable[str]) -> bool:
    """Case-insensitive substring match of *tokens* in *message*.

    Normalizes the incoming *message* to lowercase to avoid relying on
    callers to pre-normalize exception messages.
    """

    msg = (message or "").lower()
    return any(token in msg for token in tokens)


def get_migration_context() -> Optional[MigrationContext]:
    """Return the active Alembic :class:`MigrationContext`, if one is available."""

    try:
        return op.get_context()
    except Exception:
        pass

    try:
        return context.get_context()
    except Exception:
        return None


def _safe_get_context():
    """Return the active migration context without raising on failure."""

    return get_migration_context()


def get_current_dialect() -> Optional[str]:
    """Return the name of the database dialect used for the current run.

    The lookup prefers an active :class:`MigrationContext` but gracefully
    falls back to Alembic configuration options when no context is present,
    such as during offline migrations.
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
    return None


def is_current_dialect(*dialect_names: str) -> bool:
    """Return ``True`` if the active dialect matches any provided names."""

    name = get_current_dialect()
    return bool(name and name in dialect_names)


def _dialect_name() -> Optional[str]:
    """Internal shorthand for :func:`get_current_dialect`."""

    return get_current_dialect()


def _preparer():
    """Return the current dialect's identifier preparer."""

    ctx = _safe_get_context()
    dialect = getattr(ctx, "dialect", None) or DefaultDialect()
    return dialect.identifier_preparer


def _qident(name: Optional[str]) -> str:
    """Quote *name* using the active dialect's identifier rules."""

    if not name:
        return ""
    return _preparer().quote_identifier(name)


def _quoted_table(table_name: str, schema: Optional[str]) -> str:
    """Quote *table_name* and *schema* for SQL emission."""

    if schema:
        return f"{_qident(schema)}.{_qident(table_name)}"
    return _qident(table_name)


def _quoted_index(index_name: str, schema: Optional[str]) -> str:
    """Quote *index_name* and *schema* for SQL emission."""

    if schema:
        return f"{_qident(schema)}.{_qident(index_name)}"
    return _qident(index_name)


def qualify_table(table_name: str, schema: Optional[str] = None) -> str:
    """Return a fully qualified, quoted table name for the active schema.

    When *schema* is omitted the function uses :func:`get_effective_schema`
    to honour Alembic's ``version_table_schema`` setting.
    """

    if schema is None:
        schema = get_effective_schema()
    return _quoted_table(table_name, schema)


def qualify_index(index_name: str, schema: Optional[str] = None) -> str:
    """Return a fully qualified, quoted index name for the active schema.

    Behaviour mirrors :func:`qualify_table`, ensuring both helpers can be
    used interchangeably when generating SQL.
    """

    if schema is None:
        schema = get_effective_schema()
    return _quoted_index(index_name, schema)


def get_effective_schema() -> Optional[str]:
    """Return Alembic's ``version_table_schema`` if configured; otherwise ``None``.

    The result reflects both the live migration context and any values
    defined in the Alembic configuration file, keeping behaviour consistent
    between online and offline runs.
    """

    ctx = _safe_get_context()
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


def get_schema_prefix(delimiter: str = ".") -> str:
    """Return the configured schema with ``delimiter`` appended if present.

    This is useful when building raw SQL fragments that need to prefix
    multiple identifiers with the same schema name.
    """

    schema = get_effective_schema()
    return f"{schema}{delimiter}" if schema else ""


def try_drop_constraint(
    constraint_name: str,
    table_name: str,
    *,
    type_: Optional[str] = None,
) -> None:
    """Drop a named constraint if it exists on any supported backend.

    The helper normalises dialect quirks so that migrations remain
    idempotent. When the constraint is already absent the function simply
    logs the condition and returns without raising. For MySQL, providing
    ``type_`` allows Alembic to select the correct ``DROP`` statement and
    avoids additional probing queries.
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


def try_drop_index(index_name: str, table_name: Optional[str] = None) -> None:
    """Drop a named index, tolerating missing objects across backends.

    The ``table_name`` argument is optional everywhere except on MySQL, where
    it is required by the server syntax. Missing indexes are treated as a
    successful no-op so that repeated migrations remain idempotent.
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
        )
        if not tolerated:
            raise RuntimeError(exc) from exc
        logger.debug(
            "Index %s on %s already missing; treated as no-op",
            plain_index,
            plain_table,
        )


def drop_current_primary_key(table_name: str) -> None:
    """Drop the primary key on *table_name* if one is currently defined.

    The helper is explicitly idempotent: repeated calls leave the database
    in the same state without surfacing benign "missing" errors. Callers are
    expected to drop dependent foreign keys beforehand, mirroring the
    ordering already used in schema migrations. Unsupported dialects simply
    log the attempted action so that the behaviour remains predictable on
    development setups such as SQLite.
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
        schema_init = f"'{schema}'" if schema else "NULL"
        op.execute(
            f"""
        DO $$
        DECLARE
            schemaname text := {schema_init};
            pkname     text;
            tblname    text := '{table_name}';
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
            f"UPPER('{schema}')" if schema else "SYS_CONTEXT('USERENV','CURRENT_SCHEMA')"
        )
        tab_expr = f"UPPER('{table_name}')"
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
    "get_effective_schema",
    "get_migration_context",
    "get_current_dialect",
    "is_current_dialect",
    "qualify_index",
    "qualify_table",
    "get_schema_prefix",
    "try_drop_constraint",
    "try_drop_index",
    "drop_current_primary_key",
]
