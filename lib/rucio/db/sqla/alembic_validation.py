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
"""Tools that power the Alembic validation command used in CI.

The helpers in this module allow the validation script to start from an empty
database, replay every Alembic migration, and compare the resulting schema with
the one produced by the SQLAlchemy models.  The high level flow is:

* load the Alembic configuration, metadata models and database engine,
* discover the migration graph and take fingerprints of the database schema
  after every migration is applied,
* perform upgrade/downgrade round-trips for every edge in the revision graph to
  ensure migrations are reversible,
* compare the schema produced by the migrations with the declarative models via
  Alembic's autogeneration diffing utilities, and
* validate helper flags such as :func:`rucio.db.sqla.models.is_old_db` and
  record structured issues that can be surfaced to the caller or test suite.

Most functions operate on :class:`ValidationContext` objects which capture the
inputs needed to repeat the checks.  The routines are intentionally verbose and
log-friendly, making it possible to understand precisely which stage of the
validation pipeline produced a failure when the script is executed in CI.
"""

from __future__ import annotations

import dataclasses
import difflib
from collections import defaultdict
from collections.abc import Callable, Mapping, Sequence
from typing import TYPE_CHECKING

from alembic import command as alembic_cmd
from alembic.autogenerate import api as autogen_api
from alembic.runtime.migration import MigrationContext
from alembic.script import ScriptDirectory
from sqlalchemy import Column, MetaData, inspect
from sqlalchemy.schema import DropTable, Table
from sqlalchemy.types import NullType

from rucio import alembicrevision
from rucio.db.sqla._fingerprint import SchemaSnapshot, schema_fingerprint

if TYPE_CHECKING:
    from alembic.config import Config as AlembicConfig
    from sqlalchemy.engine import Engine

Logger = Callable[[str], None]
IsOldDBCallable = Callable[[], bool]


def _database_revision_heads(engine: "Engine", schema: str | None) -> set[str]:
    """Return the revision head markers recorded in the target database."""

    with engine.connect() as connection:
        opts: dict[str, str] = {}
        if schema is not None:
            opts["version_table_schema"] = schema
        context = MigrationContext.configure(connection=connection, opts=opts)
        return set(map(str, context.get_current_heads()))


@dataclasses.dataclass
class Issue:
    """Structured record describing a validation failure.

    Each issue captures enough information for automated tooling and humans to
    understand what went wrong without re-running the validator.  The
    ``fingerprint_*`` fields are optional because only round-trip drift checks
    produce diffable schema dumps.
    """
    # Known kinds: "roundtrip" | "models-drift" | "is-old-db" | "revision-marker-mismatch"
    # | "multi-head-repository" | "multi-branch-repository" | "alembicrevision-mismatch"
    kind: str
    at_revision: str
    path: tuple[str, ...]
    detail: str
    fingerprint_before: str | None = None
    fingerprint_after: str | None = None
    fingerprint_diff: str | None = None


@dataclasses.dataclass(frozen=True)
class ValidationContext:
    """Bundle of lazily loaded objects required to perform validation.

    The context is assembled once at the beginning of a run and then passed to
    downstream helpers.  Doing so avoids repeatedly touching the filesystem or
    re-creating engines while also providing a convenient, type annotated
    carrier for the configuration knobs that drive the checks.
    """
    cfg: "AlembicConfig"
    script: ScriptDirectory
    engine: "Engine"
    models_metadata: MetaData
    include_schemas: Sequence[str]
    schemas_to_clear: Sequence[str | None]
    version_table_schema: str | None


def build_validation_context(
        *,
        load_config: Callable[[], "AlembicConfig"],
        load_models_metadata: Callable[[], MetaData],
        load_engine: Callable[[], "Engine"],
) -> ValidationContext:
    """Resolve configuration, metadata and database handles for validation.

    The public entry point used by the CLI pulls the Alembic configuration,
    SQLAlchemy models and engine factories from callables.  This keeps
    construction lazy—callers can defer heavy imports until the validator is
    actually invoked.  The helper also mirrors Alembic's own schema resolution
    rules by consulting the INI file and the optional ``database.schema``
    config entry, ultimately returning a :class:`ValidationContext` populated
    with every schema that must be reflected and cleaned between checks.
    """
    cfg = load_config()
    script = ScriptDirectory.from_config(cfg)
    models_metadata = load_models_metadata()
    engine = load_engine()

    configured_schema: str | None = None
    try:
        from rucio.common.config import config_get
    except Exception:
        pass
    else:
        try:
            configured_schema = config_get(
                "database",
                "schema",
                raise_exception=False,
                default=None,
                check_config_table=False,
            )
        except Exception:
            configured_schema = None

    version_table_schema = (cfg.get_main_option("version_table_schema") or None) or configured_schema

    include_schemas, schemas_to_clear = derive_validation_schemas(
        models_metadata=models_metadata,
        configured_schema=configured_schema,
    )

    return ValidationContext(
        cfg=cfg,
        script=script,
        engine=engine,
        models_metadata=models_metadata,
        include_schemas=include_schemas,
        schemas_to_clear=schemas_to_clear,
        version_table_schema=version_table_schema,
    )


def _assert_single_head(script: ScriptDirectory, logger: Logger) -> Issue | None:
    """Ensure the Alembic repository exposes a single head matching the code."""

    heads = tuple(script.get_heads())
    if len(heads) != 1:
        detail = (
            f"Expected a single head but repository has {len(heads)}: {heads}. "
            "Run `alembic merge <rev1> <rev2> ...` to collapse to one head."
        )
        logger(f"Repository multi-head detected: {heads}")
        return Issue(
            kind="multi-head-repository",
            at_revision="repository",
            path=("repository", "heads"),
            detail=detail,
        )

    head = heads[0]
    if head != alembicrevision.ALEMBIC_REVISION:
        detail = (
            f"Repository head {head!r} does not match ALEMBIC_REVISION "
            f"{alembicrevision.ALEMBIC_REVISION!r}. Update alembicrevision.py."
        )
        logger(
            f"Repository head mismatch: expected {head!r}, ALEMBIC_REVISION reports "
            f"{alembicrevision.ALEMBIC_REVISION!r}"
        )
        return Issue(
            kind="alembicrevision-mismatch",
            at_revision=head,
            path=("repository", "ALEMBIC_REVISION"),
            detail=detail,
        )

    logger(f"Repository single-head OK: {head}")
    return None


def _reset_db_to_revision(
        *,
        cfg: "AlembicConfig",
        engine: "Engine",
        schemas_to_clear: Sequence[str | None],
        revision: str,
) -> None:
    """Reset the database to a known clean state before validation steps.

    The helper performs a two phase operation:

    * drop every object (tables, constraints and the version table) from the
      configured schemas via :func:`_drop_everything`, and
    * when ``revision`` is not :data:`_BASE_REVISION`, invoke Alembic's upgrade
      command so that the schema matches the requested revision.

    Performing the cleanup explicitly instead of relying on Alembic ensures
    that leftover user objects never influence the validation results.
    """

    _drop_everything(engine, schemas_to_clear)
    if not _is_base_revision(revision):
        _alembic_upgrade(cfg, revision)


def _drop_everything(engine: Engine, schemas: Sequence[str | None] = ()) -> None:
    """Remove every table, constraint and enum type from the requested schemas.

    The routine connects to the engine, temporarily disables referential
    integrity on MySQL, reflects the database for each schema, and then issues
    ``DROP TABLE`` statements in dependency order.  It also removes the
    ``alembic_version`` table so that Alembic upgrades start from a blank
    slate.  PostgreSQL keeps enum types in the catalog; to avoid leaking types
    between runs we query ``pg_enum`` via SQLAlchemy's inspector and explicitly
    drop every enum encountered.  The combination leaves the database as empty
    as possible while tolerating older SQLAlchemy behaviours and assorted
    dialect quirks.
    """
    from sqlalchemy import text as sql_text

    schemas = list(schemas or [])
    normalized_schemas: list[str | None] = []
    seen: set[str | None] = set()

    def _normalize(sc: str | None) -> str | None:
        if sc is None:
            return None
        stripped = sc.strip()
        return stripped or None

    for candidate in [None, *schemas]:
        norm = _normalize(candidate)
        if norm not in seen:
            seen.add(norm)
            normalized_schemas.append(norm)

    with engine.connect() as conn:
        trans = conn.begin()
        try:
            dialect_name = conn.dialect.name
            if dialect_name == "mysql":
                conn.execute(sql_text("SET FOREIGN_KEY_CHECKS=0"))
            inspector = inspect(conn)
            for schema in normalized_schemas:
                md = MetaData(schema=schema)
                md.reflect(bind=conn, schema=schema)
                for table in reversed(md.sorted_tables):
                    try:
                        conn.execute(DropTable(table, if_exists=True))
                    except Exception:
                        conn.execute(DropTable(table))
                version_table = None
                for tbl in md.tables.values():
                    if tbl.name == "alembic_version" and tbl.schema == schema:
                        version_table = tbl
                        break
                if version_table is None:
                    version_table = Table(
                        "alembic_version",
                        MetaData(),
                        Column("version_num", NullType()),
                        schema=schema,
                    )
                try:
                    conn.execute(DropTable(version_table, if_exists=True))
                except Exception:
                    try:
                        conn.execute(DropTable(version_table))
                    except Exception:
                        pass

            if dialect_name == "postgresql":
                preparer = conn.dialect.identifier_preparer
                enums_seen: set[tuple[str | None, str]] = set()
                enum_schemas: list[str | None] = []
                default_schema = inspector.default_schema_name
                for schema in normalized_schemas:
                    enum_schemas.append(schema if schema is not None else default_schema)
                if not enum_schemas:
                    enum_schemas.append(default_schema)

                for enum_schema in enum_schemas:
                    try:
                        enums = inspector.get_enums(schema=enum_schema)
                    except NotImplementedError:
                        continue
                    for enum in enums:
                        enum_name = enum.get("name")
                        if not enum_name:
                            continue
                        enum_schema_name = enum.get("schema") or enum_schema
                        key = (enum_schema_name, enum_name)
                        if key in enums_seen:
                            continue
                        enums_seen.add(key)
                        schema_prefix = (
                            f"{preparer.quote_identifier(enum_schema_name)}."
                            if enum_schema_name else ""
                        )
                        type_name = f"{schema_prefix}{preparer.quote_identifier(enum_name)}"
                        conn.execute(sql_text(f"DROP TYPE IF EXISTS {type_name} CASCADE"))
            trans.commit()
        except Exception:
            trans.rollback()
            raise
        finally:
            if conn.dialect.name == "mysql":
                conn.execute(sql_text("SET FOREIGN_KEY_CHECKS=1"))


def _unified_diff(before_text: str, after_text: str, *, before_label: str, after_label: str) -> str:
    """Return a unified diff describing schema fingerprint changes.

    ``difflib.unified_diff`` mirrors the output format produced by familiar
    tooling such as ``git diff``.  By labelling the before/after snapshots with
    meaningful revision names we can surface the exact structural differences
    when round-trip validation fails.  An empty diff is converted into a
    placeholder string so that downstream logging always shows some context.
    """

    diff_lines = difflib.unified_diff(
        before_text.splitlines(),
        after_text.splitlines(),
        fromfile=before_label,
        tofile=after_label,
        lineterm="",
    )
    return "\n".join(diff_lines) or "(no textual diff available)"


def _first_token_mismatch(
        before: Mapping[str, tuple[str, ...]],
        after: Mapping[str, tuple[str, ...]],
) -> str | None:
    """Return a one-line description of the first fingerprint token mismatch."""

    keys = sorted(set(before.keys()) | set(after.keys()))
    for key in keys:
        before_token = before.get(key)
        after_token = after.get(key)
        if before_token != after_token:
            before_repr = repr(before_token) if before_token is not None else "None"
            after_repr = repr(after_token) if after_token is not None else "None"
            return f"{key}: before={before_repr} after={after_repr}"
    return None


def _verify_roundtrip(
        *,
        cfg: "AlembicConfig",
        engine: "Engine",
        include_schemas: Sequence[str],
        prepared_snapshot: SchemaSnapshot,
        target_revision: str,
        parent_revision: str,
        downgrade_target: str,
        before_label: str,
        after_label: str,
        issue_kind: str,
        issue_revision: str,
        issue_path: tuple[str, ...],
        issue_detail_prefix: str,
        logger: Logger,
        issue_callback: Callable[[Issue], None] | None,
        upgrade_then_downgrade: bool,
        ok_log_message: str,
        drift_log_message: str,
        state_checker: Callable[[str | None, str], Issue | None] | None = None,
) -> Issue | None:
    """Execute one migration round trip, state check, and fingerprint diff.

    The helper orchestrates both directions of a migration edge.  Depending on
    ``upgrade_then_downgrade`` it either performs an ``upgrade`` followed by a
    ``downgrade`` (A→B→A) or the inverse (B→A→B).  After each Alembic command we
    optionally invoke ``state_checker`` so callers can validate auxiliary
    invariants such as :func:`rucio.db.sqla.models.is_old_db`.  Any issue raised
    by the state checker short-circuits the process and is returned directly.

    When the database survives the round-trip we compute a fresh
    :func:`schema_fingerprint`, compare it with ``prepared_snapshot`` and, if
    differences are detected, build a rich :class:`Issue` including human
    friendly digests and a unified diff.  The issue is optionally streamed to
    ``issue_callback`` before being returned to the caller.  Successful runs
    simply emit ``ok_log_message`` to keep the logs concise.
    """
    state_issue: Issue | None = None
    if upgrade_then_downgrade:
        _alembic_upgrade(cfg, target_revision)
        if state_checker is not None:
            state_issue = state_issue or state_checker(target_revision, "upgrade")
        _alembic_downgrade(cfg, downgrade_target)
        if state_checker is not None:
            state_issue = state_issue or state_checker(parent_revision, "downgrade")
    else:
        _alembic_downgrade(cfg, downgrade_target)
        if state_checker is not None:
            state_issue = state_issue or state_checker(parent_revision, "downgrade")
        _alembic_upgrade(cfg, target_revision)
        if state_checker is not None:
            state_issue = state_issue or state_checker(target_revision, "upgrade")

    if state_issue is not None:
        return state_issue

    snap_back = schema_fingerprint(engine, include_schemas)
    if snap_back.digest != prepared_snapshot.digest:
        diff_text = _unified_diff(
            prepared_snapshot.text,
            snap_back.text,
            before_label=before_label,
            after_label=after_label,
        )
        token_diff = _first_token_mismatch(prepared_snapshot.tokens, snap_back.tokens)
        detail = (
            f"{issue_detail_prefix}"
            f"\n  before: {prepared_snapshot.digest}"
            f"\n  after : {snap_back.digest}"
        )
        if token_diff:
            detail += f"\n  first mismatch: {token_diff}"
        detail += f"\n{diff_text}"
        issue = Issue(
            kind=issue_kind,
            at_revision=issue_revision,
            path=issue_path,
            detail=detail,
            fingerprint_before=prepared_snapshot.text,
            fingerprint_after=snap_back.text,
            fingerprint_diff=diff_text,
        )
        if issue_callback is not None:
            issue_callback(issue)
        logger(drift_log_message)
        return issue

    logger(ok_log_message)
    return None


def log_heading(logger: Logger, text: str) -> None:
    """Log a simple underlined heading to improve readability of output.

    The validator emits a large amount of text.  Wrapping major stages in an
    underlined heading makes it significantly easier to skim logs from CI and
    locate the section that produced a failure.
    """
    bar = "=" * len(text)
    logger(f"\n{bar}\n{text}\n{bar}")


def _autogen_diff_is_empty(
        engine: Engine,
        models_md: MetaData,
        *,
        version_table_schema: str | None,
) -> tuple[bool, str]:
    """Return whether Alembic autogeneration detects pending upgrade ops.

    We spin up an Alembic :class:`~alembic.migration.MigrationContext` against
    the live database, feed it the SQLAlchemy metadata, and let the autogenerate
    subsystem compute the diff.  When ``upgrade_ops`` is empty the schema
    created by migrations matches the declarative models.  Otherwise we format
    the proposed operations into a multiline string that is attached to a
    :class:`Issue` so that developers can replicate and fix the drift.
    """
    with engine.connect() as conn:
        mctx = MigrationContext.configure(
            conn,
            opts={
                "compare_type": True,
                "compare_server_default": True,
                "include_schemas": True,
                "version_table_schema": version_table_schema,
                "target_metadata": models_md,
            },
        )
        diffs = autogen_api.produce_migrations(mctx, models_md)
        has_ops = not diffs.upgrade_ops.is_empty()
        if not has_ops:
            return True, ""
        ops = "\n".join(f"  - {op!r}" for op in diffs.upgrade_ops.ops)
        return False, f"Autogenerate suggests the following upgrade operations:\n{ops}\n"


def _alembic_upgrade(cfg: AlembicConfig, rev: str) -> None:
    """Upgrade the database to ``rev`` using Alembic.

    Thin wrapper kept for symmetry with :func:`_alembic_downgrade`.  Having a
    dedicated function allows us to centralise any logging or error handling in
    the future without touching every call site.
    """
    alembic_cmd.upgrade(cfg, rev)


def _alembic_downgrade(cfg: AlembicConfig, rev: str) -> None:
    """Downgrade the database to ``rev`` using Alembic.

    See :func:`_alembic_upgrade` for context.  The helper mainly exists so that
    higher level routines read naturally when alternating upgrades and
    downgrades during round-trip checks.
    """
    alembic_cmd.downgrade(cfg, rev)


_BASE_REVISION = "__BASE__"


def _is_base_revision(revision: str) -> bool:
    """Return ``True`` if ``revision`` represents the sentinel base revision.

    The validator treats ``__BASE__`` as a synthetic revision representing an
    empty database.  This helper keeps comparisons readable throughout the
    module.
    """
    return revision == _BASE_REVISION


def _display_revision(revision: str) -> str:
    """Return a human friendly label for ``revision`` used in logs.

    ``__BASE__`` is displayed as ``"base"`` while every other revision is shown
    verbatim.  Having a dedicated helper ensures the label is consistent across
    log messages, issue descriptions and diff labels.
    """
    return "base" if _is_base_revision(revision) else revision


def _downgrade_target(revision: str) -> str:
    """Return the Alembic downgrade target for a revision or the base sentinel.

    Alembic expects the literal string ``"base"`` for full downgrades whereas
    our internal representation uses :data:`_BASE_REVISION`.  The helper hides
    this conversion from the callers that construct downgrade plans.
    """
    return "base" if _is_base_revision(revision) else revision


def _normalize_revision_for_flag(revision: str | None) -> str | None:
    """Return the revision identifier used when evaluating ``is_old_db()``."""

    if revision is None:
        return None
    if revision == _BASE_REVISION:
        return None
    if isinstance(revision, str) and revision.lower() == "base":
        return None
    return revision


def _expected_is_old_db(flag_revision: str | None, code_head: str) -> bool:
    """Return the expected ``is_old_db`` value for ``flag_revision``."""

    if flag_revision is None:
        return False
    return flag_revision != code_head


def _check_is_old_db_state(
        *,
        expected_revision: str | None,
        display_revision: str,
        stage_label: str,
        code_head: str,
        is_old_db_checker: IsOldDBCallable,
        logger: Logger,
) -> Issue | None:
    """Validate ``is_old_db()`` for the current revision state."""

    expected_value = _expected_is_old_db(expected_revision, code_head)
    actual_value = bool(is_old_db_checker())
    if actual_value != expected_value:
        detail = (
            f"is_old_db() returned {actual_value} during {stage_label}; expected {expected_value}. "
            "The database revision markers are inconsistent with the Alembic state."
        )
        issue = Issue(
            kind="is-old-db",
            at_revision=display_revision,
            path=(stage_label,),
            detail=detail,
        )
        logger(
            f"    is_old_db() check FAILED during {stage_label}: expected {expected_value}, got {actual_value}"
        )
        return issue

    logger(f"    is_old_db() returned {actual_value} as expected during {stage_label}")
    return None


def _check_linear_revision_edge(
        *,
        cfg: "AlembicConfig",
        engine: "Engine",
        include_schemas: Sequence[str],
        schemas_to_clear: Sequence[str | None],
        parent_revision: str,
        parent_snapshot: SchemaSnapshot,
        child_revision: str,
        code_head: str,
        is_old_db_checker: IsOldDBCallable,
        logger: Logger,
        issue_callback: Callable[[Issue], None] | None,
) -> tuple[list[Issue], SchemaSnapshot, bool]:
    """Exercise one revision edge with forward/backward round-trips.

    The validator keeps a single database instance alive while walking the
    linear history.  For each parent→child edge we:

    #. run an ``upgrade`` followed by a ``downgrade`` (A→B→A) using
       :func:`_verify_roundtrip`,
    #. upgrade to the child revision and capture a fingerprint for later
       comparisons,
    #. perform the inverse ``downgrade``/``upgrade`` sequence (B→A→B) to ensure
       reversibility in both directions, and
    #. evaluate :func:`rucio.db.sqla.models.is_old_db` after every state change
       so flag regressions surface next to the offending revision.

    Should a round-trip report schema drift the validator logs the failure and
    terminates the run so that developers can address the offending revision
    before rerunning the checks.  The function returns any issues, the snapshot
    representing the child revision, and a boolean indicating whether the
    optimistic in-place traversal remained valid.
    """

    issues: list[Issue] = []

    display_parent = _display_revision(parent_revision)
    display_child = _display_revision(child_revision)
    downgrade_target = _downgrade_target(parent_revision)

    logger(
        "  Round-trip A->B->A for edge "
        f"{display_parent} -> {display_child} -> {display_parent}"
    )

    path = (display_parent, display_child, display_parent)
    path_str = "->".join(path)

    def _state_checker(revision: str | None, action: str) -> Issue | None:
        display_revision = _display_revision(revision) if revision is not None else "base"
        label = f"{path_str} {action} at {display_revision}"
        return _check_is_old_db_state(
            expected_revision=_normalize_revision_for_flag(revision),
            display_revision=display_revision,
            stage_label=label,
            code_head=code_head,
            is_old_db_checker=is_old_db_checker,
            logger=logger,
        )

    issue = _verify_roundtrip(
        cfg=cfg,
        engine=engine,
        include_schemas=include_schemas,
        prepared_snapshot=parent_snapshot,
        target_revision=child_revision,
        parent_revision=parent_revision,
        downgrade_target=downgrade_target,
        before_label=f"{path[0]} before",
        after_label=f"{path[0]} after {path_str}",
        issue_kind="roundtrip",
        issue_revision=path[0],
        issue_path=path,
        issue_detail_prefix=f"Schema fingerprint after {path_str} differs from original at {path[0]}",
        logger=logger,
        issue_callback=issue_callback,
        upgrade_then_downgrade=True,
        ok_log_message="OK",
        drift_log_message="Drift detected on r->c->r",
        state_checker=_state_checker,
    )
    if issue is not None:
        if issue_callback is not None and issue.kind != "roundtrip":
            issue_callback(issue)
        issues.append(issue)
        logger("  Halting validation after detecting an issue on round-trip r->c->r")
        return issues, parent_snapshot, False

    logger(f"  Upgrade {display_parent} -> {display_child}")
    _alembic_upgrade(cfg, child_revision)

    issue = _check_is_old_db_state(
        expected_revision=_normalize_revision_for_flag(child_revision),
        display_revision=display_child,
        stage_label=f"prepare {display_child}",
        code_head=code_head,
        is_old_db_checker=is_old_db_checker,
        logger=logger,
    )
    if issue is not None:
        issues.append(issue)
        if issue_callback is not None:
            issue_callback(issue)

    child_snapshot = schema_fingerprint(engine, include_schemas)

    logger(
        "  Round-trip B->A->B for edge "
        f"{display_child} -> {display_parent} -> {display_child}"
    )

    reverse_path = (display_child, display_parent, display_child)
    reverse_path_str = "->".join(reverse_path)

    def _reverse_state_checker(revision: str | None, action: str) -> Issue | None:
        display_revision = _display_revision(revision) if revision is not None else "base"
        label = f"{reverse_path_str} {action} at {display_revision}"
        return _check_is_old_db_state(
            expected_revision=_normalize_revision_for_flag(revision),
            display_revision=display_revision,
            stage_label=label,
            code_head=code_head,
            is_old_db_checker=is_old_db_checker,
            logger=logger,
        )

    issue = _verify_roundtrip(
        cfg=cfg,
        engine=engine,
        include_schemas=include_schemas,
        prepared_snapshot=child_snapshot,
        target_revision=child_revision,
        parent_revision=parent_revision,
        downgrade_target=downgrade_target,
        before_label=f"{reverse_path[0]} before",
        after_label=f"{reverse_path[0]} after {reverse_path_str}",
        issue_kind="roundtrip",
        issue_revision=reverse_path[0],
        issue_path=reverse_path,
        issue_detail_prefix=(
            f"Schema fingerprint after {reverse_path_str} differs from original at {reverse_path[0]}"
        ),
        logger=logger,
        issue_callback=issue_callback,
        upgrade_then_downgrade=False,
        ok_log_message="OK",
        drift_log_message="Drift detected on c->r->c",
        state_checker=_reverse_state_checker,
    )
    if issue is not None:
        if issue_callback is not None and issue.kind != "roundtrip":
            issue_callback(issue)
        issues.append(issue)
        logger("  Halting validation after detecting an issue on round-trip c->r->c")
        return issues, child_snapshot, False

    return issues, child_snapshot, True


def _discover_revision_graph(script: ScriptDirectory) -> tuple[list[str], dict[str, set[str]], dict[str, set[str]]]:
    """Return a topological ordering and adjacency lists for the revision graph.

    Alembic exposes the migration graph via :meth:`ScriptDirectory.walk_revisions`.
    We iterate over every revision reachable from ``base`` to ``heads``, track
    parent/child relationships, and synthesise a ``__BASE__`` node so that the
    rest of the validator can treat the empty database as a normal revision.
    The function returns three artefacts: a topologically sorted list of
    revisions (including the base sentinel), a mapping of revision→parents, and
    a mapping of revision→children.  These structures drive the exhaustive
    round-trip and model comparison checks.
    """
    parents: dict[str, set[str]] = defaultdict(set)
    children: dict[str, set[str]] = defaultdict(set)
    concrete: set[str] = set()

    for s in script.walk_revisions(base="base", head="heads"):
        rid = s.revision
        concrete.add(rid)
        parents.setdefault(rid, set())
        children.setdefault(rid, set())

        dr = s.down_revision
        if dr is None:
            continue

        if isinstance(dr, (list, tuple, set)):
            down_revisions = dr
        else:
            down_revisions = (dr,)

        for d in down_revisions:
            if not d:
                continue
            if isinstance(d, str) and d.lower() == "base":
                continue
            parents[rid].add(d)
            concrete.add(d)

    for child, ps in parents.items():
        for p in ps:
            children[p].add(child)

    roots = {rev for rev in concrete if not parents.get(rev)}
    if roots:
        children[_BASE_REVISION].update(roots)
        for r0 in roots:
            parents.setdefault(r0, set()).add(_BASE_REVISION)

    parents.setdefault(_BASE_REVISION, set())
    children.setdefault(_BASE_REVISION, set())

    topo_from_base: list[str] = list(
        reversed([s.revision for s in script.walk_revisions(base="base", head="heads")])
    )
    topo_with_base = [_BASE_REVISION] + topo_from_base
    return topo_with_base, parents, children


def _build_linear_revision_order(
        topo: Sequence[str],
        parents: dict[str, set[str]],
        children: dict[str, set[str]],
        logger: Logger,
) -> tuple[list[str], Issue | None]:
    """Ensure the migration graph is linear and return its ordered revisions."""

    for rev, cs in children.items():
        if rev == _BASE_REVISION:
            if len(cs) > 1:
                detail = (
                    "Base revision has multiple children; Rucio requires a single linear branch. "
                    f"Children: {sorted(cs)}."
                )
                logger(f"Repository branching detected at base: {sorted(cs)}")
                return [], Issue(
                    kind="multi-branch-repository",
                    at_revision="repository",
                    path=("repository", "children", "base"),
                    detail=detail,
                )
            continue
        if len(cs) > 1:
            detail = (
                f"Revision {rev} has multiple children {sorted(cs)}. "
                "Rucio enforces a single linear Alembic history."
            )
            logger(f"Repository branching detected at {rev}: children={sorted(cs)}")
            return [], Issue(
                kind="multi-branch-repository",
                at_revision=rev,
                path=("repository", "children", rev),
                detail=detail,
            )

    for rev, ps in parents.items():
        if rev == _BASE_REVISION:
            continue
        filtered = [p for p in ps if p != _BASE_REVISION]
        if len(filtered) > 1:
            detail = (
                f"Revision {rev} has multiple parents {sorted(filtered)}. "
                "Rucio enforces a single linear Alembic history."
            )
            logger(f"Repository merge detected at {rev}: parents={sorted(filtered)}")
            return [], Issue(
                kind="multi-branch-repository",
                at_revision=rev,
                path=("repository", "parents", rev),
                detail=detail,
            )

    order: list[str] = [_BASE_REVISION]
    visited: set[str] = {_BASE_REVISION}
    current = _BASE_REVISION

    while True:
        cs = sorted(children.get(current, ()))
        if not cs:
            break
        child = cs[0]
        if child in visited:
            detail = (
                f"Cycle detected while traversing Alembic history at {child}. "
                "Ensure revisions form a simple chain."
            )
            logger(f"Repository cycle detected at {child}")
            return [], Issue(
                kind="multi-branch-repository",
                at_revision=child,
                path=("repository", "cycle", child),
                detail=detail,
            )
        order.append(child)
        visited.add(child)
        current = child

    expected = set(topo)
    if visited != expected:
        missing = sorted(expected - visited)
        detail = (
            "Unable to produce linear ordering covering every revision. "
            f"Unreached revisions: {missing}."
        )
        logger(f"Repository traversal skipped revisions: {missing}")
        return [], Issue(
            kind="multi-branch-repository",
            at_revision="repository",
            path=("repository", "linear-order"),
            detail=detail,
        )

    return order, None


def derive_validation_schemas(
        *,
        models_metadata: MetaData,
        configured_schema: str | None,
) -> tuple[list[str], list[str | None]]:
    """Compute schema lists used to reflect and reset the database.

    ``include_schemas`` controls which schemas are reflected into metadata and
    therefore participate in fingerprint comparisons.  ``schemas_to_clear``
    lists the schemas that must be truncated before each validation step.  The
    helper folds together the configured schema and any schema declared on the
    SQLAlchemy models, deduplicates them while keeping ``None`` (default
    schema) first, and returns both lists for the caller to use.
    """
    include_schemas: list[str] = []
    model_schemas = sorted({t.schema for t in models_metadata.tables.values() if getattr(t, "schema", None)})
    for sc in [configured_schema, *model_schemas]:
        if sc and sc not in include_schemas:
            include_schemas.append(sc)

    seen_clear: set[str | None] = set()
    schemas_to_clear: list[str | None] = []
    for candidate in [None, *include_schemas]:
        if candidate not in seen_clear:
            seen_clear.add(candidate)
            schemas_to_clear.append(candidate)

    return include_schemas, schemas_to_clear


def verify_revision_markers_at_head(
        *,
        script: ScriptDirectory,
        engine: Engine,
        version_table_schema: str | None,
        logger: Logger,
        stage_label: str = "head",
) -> Issue | None:
    """Verify that revision markers match Alembic's declared heads."""

    repo_heads = tuple(script.get_heads())
    if not repo_heads:
        logger("    Skipping revision marker comparison: Alembic returned no heads")
        return None

    expected = alembicrevision.ALEMBIC_REVISION
    if len(repo_heads) != 1:
        detail = (
            "Alembic repository reports multiple heads during marker verification. "
            f"Found {repo_heads!r}."
        )
        logger(f"    Repository multi-head detected during marker verification: {repo_heads!r}")
        return Issue(
            kind="revision-marker-mismatch",
            at_revision=stage_label,
            path=(stage_label, "repository-heads"),
            detail=detail,
        )

    repo_head = repo_heads[0]
    if repo_head != expected:
        detail = (
            f"Repository head {repo_head!r} does not match ALEMBIC_REVISION "
            f"{expected!r}. Update alembicrevision.py."
        )
        logger(
            f"    ALEMBIC_REVISION mismatch: expected repository head {repo_head!r}, "
            f"found {expected!r}"
        )
        return Issue(
            kind="revision-marker-mismatch",
            at_revision=stage_label,
            path=(stage_label, "alembicrevision"),
            detail=detail,
        )

    db_heads = _database_revision_heads(engine, version_table_schema)
    if db_heads != {expected}:
        detail = (
            "alembic_version must contain exactly the code head. "
            f"Expected {{{expected!r}}}, found {sorted(db_heads)!r}."
        )
        logger(
            f"    Version table mismatch: expected {{{expected!r}}}, found {sorted(db_heads)!r}"
        )
        return Issue(
            kind="revision-marker-mismatch",
            at_revision=stage_label,
            path=(stage_label, "version-table"),
            detail=detail,
        )

    logger(f"    Revision markers match expected head {expected!r}")
    return None


def validate_history(
        *,
        cfg: AlembicConfig,
        script: ScriptDirectory,
        engine: Engine,
        models_metadata: MetaData,
        logger: Logger,
        include_schemas: Sequence[str],
        schemas_to_clear: Sequence[str | None],
        version_table_schema: str | None,
        is_old_db_checker: IsOldDBCallable,
        issue_callback: Callable[[Issue], None] | None = None,
) -> list[Issue]:
    """Run the full Alembic validation workflow and return discovered issues.

    The driver performs the following phases:

    1.  Discover the revision graph and assert that it forms a single linear
        branch.
    2.  Walk the branch once, performing incremental upgrade/downgrade
        round-trips for every edge while keeping a single database instance
        alive.
    3.  Upgrade to ``head`` a single time, verify revision markers, and compare
        the resulting schema with the SQLAlchemy models via
        :func:`_autogen_diff_is_empty`.

    Issues are accumulated and optionally streamed to ``issue_callback`` as they
    are found.  The validator now stops after the first failure so that each
    inconsistency can be addressed individually before subsequent checks run.

    """
    issues: list[Issue] = []

    single_head_issue = _assert_single_head(script, logger)
    repository_single_head_ok = single_head_issue is None
    if single_head_issue is not None:
        if issue_callback is not None:
            issue_callback(single_head_issue)
        issues.append(single_head_issue)
        return issues

    topo, parents, children = _discover_revision_graph(script)
    linear_order, branch_issue = _build_linear_revision_order(topo, parents, children, logger)
    if branch_issue is not None:
        if issue_callback is not None:
            issue_callback(branch_issue)
        issues.append(branch_issue)
        return issues

    logger("\nRunning linear Alembic revision checks")
    logger(f"Using DB URL: {engine.url!s}")
    logger(f"Total revisions in scope (including base): {len(linear_order)}")
    logger("Round-trip checks: ENABLED")

    total_revisions = len(linear_order)
    code_head = alembicrevision.ALEMBIC_REVISION

    logger(f"\n[1/{total_revisions}] Prepare at BASE: reset database (canonical base)")
    _reset_db_to_revision(
        cfg=cfg,
        engine=engine,
        schemas_to_clear=schemas_to_clear,
        revision=_BASE_REVISION,
    )
    base_snapshot = schema_fingerprint(engine, include_schemas)

    issue = _check_is_old_db_state(
        expected_revision=_normalize_revision_for_flag(_BASE_REVISION),
        display_revision="base",
        stage_label="prepare base",
        code_head=code_head,
        is_old_db_checker=is_old_db_checker,
        logger=logger,
    )
    if issue is not None:
        if issue_callback is not None:
            issue_callback(issue)
        issues.append(issue)
        return issues

    snapshots: dict[str, SchemaSnapshot] = {_BASE_REVISION: base_snapshot}
    current_revision = _BASE_REVISION

    for index, child_revision in enumerate(linear_order[1:], start=2):
        display_parent = _display_revision(current_revision)
        display_child = _display_revision(child_revision)
        logger(f"\n[{index}/{total_revisions}] Validate edge {display_parent} -> {display_child}")

        parent_snapshot = snapshots[current_revision]
        edge_issues, child_snapshot, _ = _check_linear_revision_edge(
            cfg=cfg,
            engine=engine,
            include_schemas=include_schemas,
            schemas_to_clear=schemas_to_clear,
            parent_revision=current_revision,
            parent_snapshot=parent_snapshot,
            child_revision=child_revision,
            code_head=code_head,
            is_old_db_checker=is_old_db_checker,
            logger=logger,
            issue_callback=issue_callback,
        )
        for edge_issue in edge_issues:
            issues.append(edge_issue)
            return issues

        snapshots[child_revision] = child_snapshot
        current_revision = child_revision

    logger("\nFinal head comparison against SQLAlchemy models")
    _alembic_upgrade(cfg, "head")

    if repository_single_head_ok:
        marker_issue = verify_revision_markers_at_head(
            script=script,
            engine=engine,
            version_table_schema=version_table_schema,
            logger=logger,
            stage_label="head",
        )
        if marker_issue is not None:
            if issue_callback is not None:
                issue_callback(marker_issue)
            issues.append(marker_issue)
            return issues

    ok, diff_text = _autogen_diff_is_empty(
        engine,
        models_metadata,
        version_table_schema=version_table_schema,
    )
    display_head = _display_revision(linear_order[-1]) if linear_order else "base"
    if not ok:
        issue = Issue(
            kind="models-drift",
            at_revision=display_head,
            path=(display_head, "head"),
            detail=diff_text or "Autogenerate reported differences between DB and models",
        )
        if issue_callback is not None:
            issue_callback(issue)
        issues.append(issue)
        logger("Models drift at head vs. SQLAlchemy models")
        return issues
    else:
        logger("Head matches SQLAlchemy models")

    return issues
