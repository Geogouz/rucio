#!/usr/bin/env python3
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
Design goals
------------
1) From *any* Alembic revision state, upgrading to head must produce a schema
   that exactly matches Rucio's SQLAlchemy models.
2) Every adjacent upgrade/downgrade pair must be a lossless round-trip:
   - A -> B -> A yields the same schema as A
   - B -> A -> B yields the same schema as B
3) Stop on first problem by default, but optionally report all issues.
4) Usable locally (manual run) and in CI (non-interactive, JSON artifact).

Usage
-----
# use the repository's alembic.ini and models module
$ python3 tools/check_alembic_history.py \
    --alembic-cfg etc/alembic.ini \
    --models rucio.db.sqla.models:BASE \
    --report-json artifacts/alembic_history_report.json

Optional Flags
--------------
--db-url               Override the database URL (otherwise read from alembic.ini)
--report-all-drift     Continue after a failure to collect all issues
--skip-roundtrips      Only verify 'upgrade to head matches models' (faster)
--schemas SC1,SC2      Extra schemas to include when comparing (comma separated)
--include-views        Include views in the fingerprint (experimental, default off)
--verbose              Chatty logs

Exit code is non‑zero when issues are found.

Implementation notes
--------------------
- We rely on Alembic's ScriptDirectory to discover the revision DAG.
- For round-trip checks we compute a structural "fingerprint" of the current
  database using SQLAlchemy's reflection in a backend‑agnostic way.
- To validate "head matches models" we use Alembic's autogenerate engine to
  produce migrations vs. the models metadata and assert that the upgrade ops
  are empty.
"""
import argparse
import dataclasses
import importlib
import io
import json
import logging
import os
import sys
import time
from collections import defaultdict
from typing import TYPE_CHECKING, Optional

from alembic import command as alembic_cmd
from alembic.autogenerate import api as autogen_api
from alembic.config import Config as AlembicConfig
from alembic.migration import MigrationContext
from alembic.script import ScriptDirectory
from sqlalchemy import MetaData, create_engine

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Sequence

    from sqlalchemy.engine import Engine


# --------------------------- CLI parsing -----------------------------------

def _parse_arguments(argv: "Sequence[str]") -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Exhaustive Alembic history validation")
    p.add_argument("--alembic-cfg", required=True, help="Path to alembic.ini")
    p.add_argument("--models", default="rucio.db.sqla.models:BASE",
                   help="Import path to models metadata. Formats: 'pkg.mod' (has 'Base' or 'BASE'), or 'pkg.mod:BaseName'")
    p.add_argument("--db-url", default=None, help="Override sqlalchemy.url in alembic.ini")
    p.add_argument("--report-json", default=None, help="Write a JSON report here")
    p.add_argument("--report-all-drift", action="store_true", help="Continue after failures to collect all issues")
    p.add_argument("--skip-roundtrips", action="store_true", help="Skip A<->B round-trip checks (faster)")
    p.add_argument("--schemas", default=None, help="Comma separated extra schemas to include in reflection")
    p.add_argument("--include-views", action="store_true", help="Include views in fingerprint")
    p.add_argument("--verbose", action="store_true", help="Verbose logging")
    return p.parse_args(argv)


# --------------------------- Utilities -------------------------------------

def _load_models_metadata(models_expr: str) -> MetaData:
    """
    Accepts either 'package.module' (with attribute 'Base' or 'BASE')
    or 'package.module:Attribute' which resolves to a declarative base
    or a MetaData instance. Returns a MetaData.
    """
    if ":" in models_expr:
        mod_name, attr = models_expr.split(":", 1)
        mod = importlib.import_module(mod_name)
        obj = getattr(mod, attr)
    else:
        mod = importlib.import_module(models_expr)
        if hasattr(mod, "BASE"):
            obj = getattr(mod, "BASE")
        elif hasattr(mod, "Base"):
            obj = getattr(mod, "Base")
        else:
            # fallback to module-level metadata
            obj = getattr(mod, "metadata")
    # resolve to MetaData
    if hasattr(obj, "metadata"):
        md = obj.metadata  # declarative base
    else:
        md = obj  # already MetaData
    if not isinstance(md, MetaData):
        raise TypeError(f"Resolved models object is not a SQLAlchemy MetaData: {type(md)}")
    return md


def _build_alembic_config(alembic_cfg_path: str, db_url_override: Optional[str] = None) -> AlembicConfig:
    cfg = AlembicConfig(alembic_cfg_path)
    if db_url_override:
        cfg.set_main_option("sqlalchemy.url", db_url_override)
    # Alembic might be invoked from any cwd; make script_location absolute
    script_loc = cfg.get_main_option("script_location")
    if script_loc and not os.path.isabs(script_loc):
        cfg.set_main_option("script_location", os.path.abspath(script_loc))
    return cfg


def _get_engine_from_cfg(cfg: AlembicConfig) -> "Engine":
    url = cfg.get_main_option("sqlalchemy.url")
    if not url:
        raise RuntimeError("No sqlalchemy.url found; supply --db-url or set it in alembic.ini")
    return create_engine(url, future=True)


def _reflect_metadata(engine: "Engine", include_schemas: "Iterable[str]", include_views: bool) -> MetaData:
    md = MetaData()
    schemas = [None]
    for sc in include_schemas:
        if sc and sc not in schemas:
            schemas.append(sc)
    for sc in schemas:
        try:
            md.reflect(bind=engine, schema=sc, views=include_views, resolve_fks=True)  # type: ignore[arg-type]
        except TypeError:
            md.reflect(bind=engine, schema=sc)  # views kw not supported in some versions
    return md


def _normalize_type(t) -> str:
    try:
        return str(t)
    except Exception:
        return repr(t)


def _schema_fingerprint(engine: "Engine", include_schemas: "Iterable[str]", include_views: bool) -> str:
    """
    Compute a backend‑agnostic fingerprint of the current database schema.
    This is used for round‑trip checks.
    """
    md = _reflect_metadata(engine, include_schemas, include_views)
    parts = []
    for table in sorted(md.tables.values(), key=lambda t: (t.schema or "", t.name)):
        parts.append(f"TABLE {table.schema or ''}.{table.name}")
        for c in sorted(table.columns, key=lambda c: c.name):
            try:
                default_text = None
                if c.server_default is not None:
                    # SQLAlchemy 2.0 has .arg for server_default, older may vary
                    if hasattr(c.server_default, "arg") and hasattr(c.server_default.arg, "text"):
                        default_text = c.server_default.arg.text  # type: ignore[attr-defined]
                    else:
                        default_text = str(c.server_default)
                parts.append(f"  COL {c.name} {_normalize_type(c.type)} "
                             f"{'NULL' if c.nullable else 'NOTNULL'} "
                             f"DEF={default_text} "
                             f"PKEY={'Y' if c.primary_key else 'N'}")
            except Exception:
                pass
        if table.primary_key and table.primary_key.columns:
            parts.append("  PK " + ",".join(col.name for col in table.primary_key.columns))
        # indexes
        for idx in sorted(table.indexes, key=lambda i: i.name or ""):
            try:
                unique = "U" if idx.unique else "N"
                cols = ",".join(c.name for c in idx.columns)
                parts.append(f"  IDX {idx.name}:{unique}:{cols}")
            except Exception:
                pass
        # foreign keys
        for fk in sorted(table.foreign_keys, key=lambda fk: fk.parent.name + (fk.constraint.name or "")):
            try:
                parts.append(
                    f"  FK {fk.constraint.name}:{fk.parent.name}->{fk.column.table.fullname}({fk.column.name})")
            except Exception:
                pass
    import hashlib
    blob = "\n".join(parts).encode("utf-8", errors="ignore")
    return hashlib.sha256(blob).hexdigest()


def _drop_everything(engine: "Engine") -> None:
    """
    Hard drop of all reflected tables; safer and faster than downgrade base when
    not all down-revisions exist. Attempts a best‑effort for common dialects.
    """
    from sqlalchemy import MetaData
    from sqlalchemy import text as sql_text
    with engine.connect() as conn:
        trans = conn.begin()
        try:
            # disable FK checks if possible (mysql)
            dialect_name = conn.dialect.name
            if dialect_name == "mysql":
                conn.execute(sql_text("SET FOREIGN_KEY_CHECKS=0"))
            md = MetaData()
            md.reflect(bind=conn)
            md.drop_all(bind=conn)
            # try dropping alembic_version table if left
            try:
                conn.execute(sql_text("DROP TABLE IF EXISTS alembic_version"))
            except Exception:
                pass
            trans.commit()
        except Exception:
            trans.rollback()
            raise


@dataclasses.dataclass
class Issue:
    kind: str  # "roundtrip" | "models-drift"
    at_revision: str
    path: tuple[str, ...]  # e.g., ("r", "c", "r") for roundtrip A->B->A
    detail: str


def _log_heading(logger: "Callable[[str], None]", text: str) -> None:
    bar = "=" * len(text)
    logger(f"\n{bar}\n{text}\n{bar}")


def _autogen_diff_is_empty(engine: "Engine", models_md: MetaData) -> tuple[bool, str]:
    """
    Return True if autogenerate produces no upgrade ops.
    """
    with engine.connect() as conn:
        mctx = MigrationContext.configure(conn, opts={
            "compare_type": True,
            "compare_server_default": True,
            "include_schemas": True,
            "version_table_schema": None,
            "target_metadata": models_md,
        })
        diffs = autogen_api.produce_migrations(mctx, models_md)
        has_ops = not diffs.upgrade_ops.is_empty()
        buf = io.StringIO()
        if has_ops:
            buf.write("Autogenerate suggests the following upgrade operations:\n")
            for op in diffs.upgrade_ops.ops:
                buf.write(f"  - {op!r}\n")
        return (not has_ops), buf.getvalue()


def _alembic_upgrade(cfg: AlembicConfig, rev: str) -> None:
    alembic_cmd.upgrade(cfg, rev)


def _alembic_downgrade(cfg: AlembicConfig, rev: str) -> None:
    alembic_cmd.downgrade(cfg, rev)


def _discover_revision_graph(script: ScriptDirectory) -> tuple[list[str], dict[str, set[str]], dict[str, set[str]]]:
    """
    Returns (topo_order, parents, children) where parents[r] is set of down_revisions of r, and
    children[r] are immediate upgrades from r.
    """
    parents: dict[str, set[str]] = defaultdict(set)
    children: dict[str, set[str]] = defaultdict(set)
    for s in script.walk_revisions(base="base", head="heads"):
        rid = s.revision
        dr = s.down_revision
        if dr is None:
            pass
        elif isinstance(dr, (list, tuple, set)):
            parents[rid].update(dr)
        else:
            parents[rid].add(dr)
    for child, ps in parents.items():
        for p in ps:
            children[p].add(child)
    topo_from_base: list[str] = list(reversed([s.revision for s in script.walk_revisions(base="base", head="heads")]))
    return topo_from_base, parents, children


def validate_alembic_history(
        alembic_cfg: str,
        models_expr: str,
        db_url: Optional[str],
        logger: "Callable[[str], None]",
        report_all_drift: bool = False,
        skip_roundtrips: bool = False,
        include_schemas: Optional["Iterable[str]"] = None,
        include_views: bool = False,
) -> list[Issue]:
    cfg = _build_alembic_config(alembic_cfg, db_url_override=db_url)
    script = ScriptDirectory.from_config(cfg)
    engine = _get_engine_from_cfg(cfg)
    models_md = _load_models_metadata(models_expr)
    include_schemas = list(include_schemas or [])

    topo, parents, children = _discover_revision_graph(script)

    issues: list[Issue] = []

    _log_heading(logger, "Alembic history validation")
    logger(f"Using DB URL: {engine.url!s}")
    logger(f"Total revisions in scope: {len(topo)}")
    if skip_roundtrips:
        logger("Round-trip checks: SKIPPED")
    else:
        logger("Round-trip checks: ENABLED")

    for i, r in enumerate(topo, start=1):
        logger(f"\n[{i}/{len(topo)}] Prepare at revision {r}: hard-reset database and upgrade to {r}")

        # fresh DB at r
        _drop_everything(engine)
        _alembic_upgrade(cfg, r)

        # baseline fingerprint for r
        f_r = _schema_fingerprint(engine, include_schemas, include_views)

        if not skip_roundtrips:
            # For each adjacent child c: r -> c
            for c in sorted(children.get(r, ())):
                logger(f"  Round-trip A->B->A for edge {r} -> {c} -> {r}")
                # r -> c -> r
                _alembic_upgrade(cfg, c)
                _alembic_downgrade(cfg, r)
                f_back = _schema_fingerprint(engine, include_schemas, include_views)
                if f_back != f_r:
                    issues.append(Issue(
                        kind="roundtrip",
                        at_revision=r,
                        path=(r, c, r),
                        detail="Schema fingerprint after r->c->r differs from original at r"
                    ))
                    logger("Drift detected on r->c->r")
                    if not report_all_drift:
                        return issues
                else:
                    logger("OK")

                # Now B->A->B starting from B
                logger(f"  Round-trip B->A->B for edge {r} <- {c} <- {r}")
                _drop_everything(engine)
                _alembic_upgrade(cfg, c)
                f_c = _schema_fingerprint(engine, include_schemas, include_views)
                _alembic_downgrade(cfg, r)
                _alembic_upgrade(cfg, c)
                f_c_back = _schema_fingerprint(engine, include_schemas, include_views)
                if f_c_back != f_c:
                    issues.append(Issue(
                        kind="roundtrip",
                        at_revision=c,
                        path=(c, r, c),
                        detail="Schema fingerprint after c->r->c differs from original at c"
                    ))
                    logger("Drift detected on c->r->c")
                    if not report_all_drift:
                        return issues
                else:
                    logger("OK")

        # From r upgrade to head, then compare with models
        logger(f"  Upgrade {r} -> head and compare with models")
        _alembic_upgrade(cfg, "head")
        ok, diff_text = _autogen_diff_is_empty(engine, models_md, include_schemas)
        if not ok:
            issues.append(Issue(
                kind="models-drift",
                at_revision=r,
                path=(r, "head"),
                detail=diff_text or "Autogenerate reported differences between DB and models",
            ))
            logger("Models drift at head vs. SQLAlchemy models")
            if not report_all_drift:
                return issues
        else:
            logger("Head matches SQLAlchemy models")

    return issues


def main(argv: Optional["Sequence[str]"] = None) -> int:
    args = _parse_arguments(sys.argv[1:] if argv is None else argv)
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO, format="%(message)s")

    def _log(msg: str):
        print(msg, flush=True)

    try:
        issues = validate_alembic_history(
            alembic_cfg=args.alembic_cfg,
            models_expr=args.models,
            db_url=args.db_url,
            logger=_log,
            report_all_drift=args.report_all_drift,
            skip_roundtrips=args.skip_roundtrips,
            include_schemas=(args.schemas.split(",") if args.schemas else []),
            include_views=args.include_views,
        )
    except Exception as exc:
        _log(f"FAILED with unexpected exception: {exc!r}")
        return 2

    report = {
        "issues_found": len(issues),
        "issues": [dataclasses.asdict(i) for i in issues],
        "timestamp": int(time.time()),
    }

    if args.report_json:
        path = __import__("pathlib").Path  # lazy import to keep top small
        path(args.report_json).parent.mkdir(parents=True, exist_ok=True)
        path(args.report_json).write_text(json.dumps(report, indent=2), encoding="utf-8")

    if issues:
        _log("\nSummary: issues found. See report above.")
        return 1
    else:
        _log("\nSummary: no issues found.")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
