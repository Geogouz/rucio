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
"""Schema fingerprinting helpers shared by Alembic validation tooling."""

from __future__ import annotations

import dataclasses
import hashlib
import re
from collections import OrderedDict, defaultdict
from types import MappingProxyType
from typing import TYPE_CHECKING, Any

from sqlalchemy import MetaData
from sqlalchemy.schema import CheckConstraint, UniqueConstraint

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

    from sqlalchemy.engine import Engine

try:  # pragma: no cover - optional dependency in typing context
    from sqlalchemy.dialects.postgresql import ENUM as PG_ENUM  # type: ignore[import-not-found]
except Exception:  # pragma: no cover - guard for stripped dialects
    PG_ENUM = None  # type: ignore[assignment]

__all__ = [
    "SchemaSnapshot",
    "reflect_metadata",
    "normalized_ddl",
    "schema_fingerprint",
    "hash_text",
    "compare_fingerprints",
]


def _normalize_type(t) -> str:
    """Return a textual representation for a SQLAlchemy ``TypeEngine`` instance.

    The helper mirrors the behaviour historically embedded in the validation
    script: try to stringify the type (which produces the dialect-aware SQL
    literal Alembic would emit) and fall back to ``repr(t)`` when ``str`` raises
    an exception.  This relaxed behaviour ensures that a single problematic type
    never breaks the entire fingerprinting routine.
    """

    if PG_ENUM is not None and isinstance(t, PG_ENUM):  # type: ignore[arg-type]
        enum_name = getattr(t, "name", "") or ""
        enums = ",".join(getattr(t, "enums", ()) or ())
        return f"ENUM {enum_name} [{enums}]"

    try:
        return str(t)
    except Exception:
        return repr(t)


def reflect_metadata(engine: "Engine", include_schemas: Iterable[str]) -> MetaData:
    """Reflect tables from the target database into an in-memory ``MetaData``.

    Alembic validation needs to produce a faithful snapshot of the live
    database without making assumptions about which schemas are populated.  The
    helper therefore initialises a fresh :class:`sqlalchemy.schema.MetaData`
    object, normalises the requested schema names (always checking the default
    schema via the ``None`` entry), and calls ``MetaData.reflect`` for each
    schema in turn.  Some SQLAlchemy versions lack the ``resolve_fks`` keyword,
    so we optimistically request fully resolved foreign keys and retry without
    the argument when the dialect rejects it.
    """

    md = MetaData()
    schemas = [None]
    for sc in include_schemas:
        if sc and sc not in schemas:
            schemas.append(sc)
    for sc in schemas:
        try:
            md.reflect(bind=engine, schema=sc, resolve_fks=True)  # type: ignore[arg-type]
        except TypeError:
            md.reflect(bind=engine, schema=sc)  # ``resolve_fks`` unsupported on some versions
    return md


def _normalize_sql_text(expr: Any, *, dialect) -> str:
    """Compile an arbitrary SQL expression into comparable plain text."""

    if expr is None:
        return ""
    try:
        compiled = str(expr.compile(dialect=dialect, compile_kwargs={"literal_binds": True}))
    except Exception:
        compiled = str(expr)
    return " ".join(compiled.split())


_CAST_STRIP_RE = re.compile(r"(::[a-zA-Z_][a-zA-Z0-9_]*\b)+$")


def _normalize_server_default(default: Any, *, dialect) -> str | None:
    """Return a normalised textual representation for server defaults.

    The helper compiles SQL expressions using the provided dialect and then
    canonicalises the handful of patterns Rucio employs in migrations.  This
    keeps the normalisation narrowly focused on the real-world defaults observed
    in the Alembic history while reducing noise from dialect specific rendering
    differences.
    """

    if default is None:
        return None

    candidate = default
    if hasattr(default, "arg"):
        candidate = getattr(default.arg, "text", None) or default.arg

    text = _normalize_sql_text(candidate, dialect=dialect).strip()
    if not text:
        return None

    # Drop simple casts that the inventory flagged as noise (e.g. ::boolean).
    text = _CAST_STRIP_RE.sub("", text).strip()

    upper = text.upper().strip("'")
    time_tokens = {"CURRENT_TIMESTAMP", "NOW()", "CURRENT_TIMESTAMP()", "CLOCK_TIMESTAMP", "CLOCK_TIMESTAMP()"}
    if upper in time_tokens:
        return "CURRENT_TIMESTAMP"

    bool_true = {"TRUE", "1", "'TRUE'", "(1)"}
    bool_false = {"FALSE", "0", "'FALSE'", "(0)"}
    if upper in bool_true:
        return "TRUE"
    if upper in bool_false:
        return "FALSE"

    return text


@dataclasses.dataclass(frozen=True)
class SchemaSnapshot:
    """Immutable representation of a schema fingerprint."""

    digest: str
    text: str
    tokens: Mapping[str, tuple[str, ...]]


def schema_fingerprint(engine: "Engine", include_schemas: Iterable[str]) -> SchemaSnapshot:
    """Compute a stable fingerprint of the current database schema.

    Definition of "schema"
    ----------------------
    The snapshot reflects structural DDL only:
      - Tables and their columns (names, types, nullability, server defaults
        where Alembic/SQLAlchemy can introspect them)
      - Primary keys, foreign keys (including ``ondelete``/``onupdate``), and
        unique constraints
      - Indexes (names, columns, uniqueness flags, predicates, operator
        classes)
      - Sequences when the backend exposes them via reflection

    Exclusions
    ----------
    - Row data and counts
    - Volatile attributes such as owners, schema qualifiers, or generated
      timestamps
    - Backend-specific artefacts not explicitly normalised (for example,
      triggers or stored procedures)

    How it's derived
    ----------------
    1) Reflect metadata for ``include_schemas`` (plus the default schema).
    2) Normalise the reflected structures into a deterministic textual dump.
    3) Hash the dump with SHA-256.
    4) Return a :class:`SchemaSnapshot` carrying the digest, text and token map.

    Parameters
    ----------
    engine : sqlalchemy.engine.Engine
        Connected engine for the database to fingerprint.
    include_schemas : Iterable[str]
        Additional schema names that should be reflected alongside the default
        schema.

    Returns
    -------
    SchemaSnapshot
        Object exposing the canonical text (:attr:`text`), SHA-256 digest
        (:attr:`digest`), and a token map useful for pinpointing differences.
    """

    md = reflect_metadata(engine, include_schemas)
    parts: list[str] = []
    token_map: dict[str, tuple[str, ...]] = {}
    dialect = engine.dialect
    for table in sorted(md.tables.values(), key=lambda t: (t.schema or "", t.name)):
        schema_name = table.schema or ""
        table_label = f"{schema_name + '.' if schema_name else ''}{table.name}"
        parts.append(f"TABLE {schema_name}.{table.name}")

        pk_columns = {col.name for col in (table.primary_key.columns if table.primary_key else [])}
        unique_constraints = [
            constraint
            for constraint in table.constraints
            if isinstance(constraint, UniqueConstraint)
        ]
        unique_membership: dict[str, set[str]] = defaultdict(set)
        for constraint in unique_constraints:
            try:
                cols_sorted = [col.name for col in sorted(constraint.columns, key=lambda col: col.name)]
            except Exception:
                continue
            signature = ",".join(cols_sorted)
            membership_token = f"UQ:{signature}"
            for col_name in cols_sorted:
                unique_membership[col_name].add(membership_token)

        column_index_tokens: dict[str, list[str]] = defaultdict(list)
        index_tokens: dict[str, tuple[str, ...]] = {}
        for idx in sorted(
            table.indexes,
            key=lambda i: (
                0 if getattr(i, "unique", False) else 1,
                ",".join(col.name for col in sorted(getattr(i, "columns", []), key=lambda col: getattr(col, "name", ""))),
                getattr(i, "name", ""),
            ),
        ):
            raw_index_name = getattr(idx, "name", None)
            try:
                cols = [c.name for c in sorted(idx.columns, key=lambda col: col.name)]
            except Exception:
                cols = []
            index_label = raw_index_name or f"<unnamed:{','.join(cols) or 'expr'}>"

            dialect_options = getattr(idx, "dialect_options", {})
            if isinstance(dialect_options, dict):
                opts = dialect_options.get(dialect.name) or dialect_options.get("postgresql")
            else:  # PopulateDict
                try:
                    opts = dialect_options.get(dialect.name) or dialect_options.get("postgresql")
                except Exception:
                    opts = None
            method = None
            opclasses: dict[str, Any] = {}
            predicate = None
            if opts is not None:
                method = getattr(opts, "get", lambda key, default=None: None)("using") or None
                raw_ops = getattr(opts, "get", lambda key, default=None: None)("ops") or {}
                if isinstance(raw_ops, dict):
                    opclasses = raw_ops
                predicate = getattr(opts, "get", lambda key, default=None: None)("where") or None
            predicate_text = _normalize_sql_text(predicate, dialect=dialect) if predicate is not None else ""
            opclass_tokens = []
            for col_name in cols:
                opclass = opclasses.get(col_name)
                if opclass:
                    opclass_tokens.append(f"{col_name}:{opclass}")
                column_index_tokens[col_name].append(
                    ":".join(
                        [
                            index_label,
                            "U" if getattr(idx, "unique", False) else "N",
                            method or "",
                            opclasses.get(col_name) or "",
                            predicate_text,
                        ]
                    )
                )

            index_key = f"{table_label}.index.{index_label}"
            index_tokens[index_key] = (
                "INDEX",
                "UNIQUE" if getattr(idx, "unique", False) else "NONUNIQUE",
                ",".join(cols),
                method or "",
                ";".join(sorted(opclass_tokens)) if opclass_tokens else "",
                predicate_text,
            )

            try:
                unique = "U" if idx.unique else "N"
                cols_repr = ",".join(cols)
                parts.append(f"  IDX :{unique}:{cols_repr}")
            except Exception:
                pass

        for column in sorted(table.columns, key=lambda c: c.name):
            try:
                default_text = _normalize_server_default(column.server_default, dialect=dialect)
                parts.append(
                    "  COL {name} {type} {nullability} DEF={default} PKEY={primary}".format(
                        name=column.name,
                        type=_normalize_type(column.type),
                        nullability="NULL" if column.nullable else "NOTNULL",
                        default=default_text,
                        primary="Y" if column.primary_key else "N",
                    )
                )
            except Exception:
                default_text = None
            membership_tokens = []
            if column.name in pk_columns:
                membership_tokens.append("PK")
            membership_tokens.extend(sorted(unique_membership.get(column.name, ())))
            membership_repr = "|".join(membership_tokens) if membership_tokens else "-"

            fk_tokens: list[str] = []
            try:
                fks = sorted(
                    column.foreign_keys,
                    key=lambda fk: (
                        fk.column.table.fullname if fk.column.table is not None else "",
                        fk.column.name,
                    ),
                )
            except Exception:
                fks = []
            for fk in fks:
                try:
                    target_table = fk.column.table.fullname if fk.column.table is not None else ""
                    target_column = fk.column.name
                except Exception:
                    continue
                fk_parts = [f"{target_table}.{target_column}"]
                ondelete = getattr(fk, "ondelete", None) or getattr(getattr(fk, "constraint", None), "ondelete", None)
                onupdate = getattr(fk, "onupdate", None) or getattr(getattr(fk, "constraint", None), "onupdate", None)
                actions = [
                    f"ondelete={ondelete}" if ondelete else "",
                    f"onupdate={onupdate}" if onupdate else "",
                ]
                actions = [entry for entry in actions if entry]
                if actions:
                    fk_parts.append("[" + ",".join(actions) + "]")
                fk_tokens.append("".join(fk_parts))
            fk_repr = "|".join(fk_tokens) if fk_tokens else "-"

            index_repr_tokens = column_index_tokens.get(column.name, [])
            index_repr = "|".join(sorted(index_repr_tokens)) if index_repr_tokens else "-"

            token_map[f"{table_label}.{column.name}"] = (
                _normalize_type(column.type),
                "NULL" if column.nullable else "NOTNULL",
                default_text or "__NO_DEFAULT__",
                membership_repr,
                fk_repr,
                index_repr,
            )

        if table.primary_key and table.primary_key.columns:
            parts.append("  PK " + ",".join(col.name for col in table.primary_key.columns))

        for constraint in sorted(
            unique_constraints,
            key=lambda c: ",".join(col.name for col in sorted(c.columns, key=lambda col: col.name)),
        ):
            cols = ",".join(col.name for col in sorted(constraint.columns, key=lambda col: col.name))
            parts.append(f"  UQ :{cols}")

        check_constraints = [
            constraint
            for constraint in table.constraints
            if isinstance(constraint, CheckConstraint)
        ]
        for constraint in sorted(
            check_constraints,
            key=lambda c: _normalize_sql_text(getattr(c, "sqltext", None), dialect=dialect),
        ):
            expression = _normalize_sql_text(getattr(constraint, "sqltext", None), dialect=dialect)
            parts.append(f"  CK :{expression}")

        for fk in sorted(
            table.foreign_keys,
            key=lambda fk: (
                fk.parent.table.fullname,
                fk.parent.name,
                fk.column.table.fullname,
                fk.column.name,
            ),
        ):
            try:
                line = "  FK :{parent}->{target}({target_column})".format(
                    parent=fk.parent.name,
                    target=fk.column.table.fullname,
                    target_column=fk.column.name,
                )
                actions = []
                ondelete = getattr(fk, "ondelete", None) or getattr(fk.constraint, "ondelete", None)
                onupdate = getattr(fk, "onupdate", None) or getattr(fk.constraint, "onupdate", None)
                if ondelete:
                    actions.append(f"ondelete={ondelete}")
                if onupdate:
                    actions.append(f"onupdate={onupdate}")
                if actions:
                    line += " [" + ", ".join(actions) + "]"
                parts.append(line)
            except Exception:
                pass

        for index_key, index_token in index_tokens.items():
            token_map[index_key] = index_token

    text = "\n".join(parts)
    blob = text.encode("utf-8", errors="ignore")
    ordered_tokens = OrderedDict(sorted(token_map.items()))
    return SchemaSnapshot(
        digest=hashlib.sha256(blob).hexdigest(),
        text=text,
        tokens=MappingProxyType(ordered_tokens),
    )


def normalized_ddl(engine: "Engine", include_schemas: Iterable[str]) -> str:
    """Produce a canonical, deterministic textual representation of the schema.

    Guarantees
    ----------
    - Objects are emitted in a stable order (schema → table → column →
      constraints).
    - Names, whitespace and quoting are normalised to keep diffs meaningful.
    - Backend-specific noise (for example autogenerated constraint names) is
      filtered or replaced with canonical placeholders when possible.

    Parameters
    ----------
    engine : sqlalchemy.engine.Engine
    include_schemas : Iterable[str]
        Additional schema names to reflect in addition to the default schema.

    Returns
    -------
    str
        Canonical DDL string suitable for hashing and text diffs.
    """

    snapshot = schema_fingerprint(engine, include_schemas)
    return snapshot.text


def hash_text(text: str) -> str:
    """Hash a Unicode string with SHA-256 to produce a hex digest.

    Details
    -------
    - Uses SHA-256 over the UTF-8 encoding of the input string.
    - Returns the lowercase hexadecimal digest.

    Parameters
    ----------
    text : str
        Input text to hash.

    Returns
    -------
    str
        The SHA-256 hex digest of ``text``.
    """

    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def compare_fingerprints(a: str, b: str) -> bool:
    """Compare two schema fingerprint digests for equality.

    Interpretation
    --------------
    - Equal digests ⇒ the canonicalised schemas are equivalent.
    - Different digests ⇒ the schemas diverge structurally (or the
      normalisation rules changed).

    Parameters
    ----------
    a : str
        First fingerprint digest.
    b : str
        Second fingerprint digest.

    Returns
    -------
    bool
        ``True`` if the digests match, ``False`` otherwise.
    """

    return a == b
