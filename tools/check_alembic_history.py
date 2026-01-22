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

"""Alembic history validation entry point used in Rucio CI and local dev.

Overview
--------
This script invokes :func:`rucio.db.sqla.util.validate_alembic_history` against
the repository's Alembic configuration. The validator performs four checks:

1. **Single-branch history** – rejects repositories whose revision graph has
   forks, joins, or cycles so that migrations remain a linear chain.
2. **Application flag sanity** – exercises ``is_old_db()`` across known
   upgrade/downgrade transitions and verifies the revision markers recorded in
   both the source tree and the database.
3. **Revision round-trips** – for every adjacent pair of revisions, upgrading
   and then downgrading back must reproduce the original schema fingerprint.
4. **Head marker vs. models** – upgrading to ``head`` must yield exactly the
   schema described by the SQLAlchemy models bundled with the repository.

The validator stops as soon as the first inconsistency is detected so that each
issue can be addressed individually before re-running the checks.

Safety
------
Because the routine performs destructive migrations, it should only be used
against disposable databases. CI and the local developer harness provision a
fresh database inside a container and inject the generated ``alembic.ini`` via
``ALEMBIC_CONFIG`` before invoking the checker. Running the tool against a
shared or production database is strongly discouraged.

Usage
-----
$ python3 tools/check_alembic_history.py [--verbose]

Exit codes
----------
0 – no issues detected
1 – validation completed with at least one reported issue
2 – unexpected error while running the checks
"""
import argparse
import importlib
import logging
import os
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Optional

from alembic.config import Config as AlembicConfig
from alembic.script import ScriptDirectory
from sqlalchemy import MetaData, create_engine

from rucio.db.sqla import alembicrevision
from rucio.db.sqla.alembic_validation import (
    Issue,
    log_heading,
)
from rucio.db.sqla.util import validate_alembic_history as _validate_alembic_history

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from sqlalchemy.engine import Engine

DEFAULT_MODELS_EXPR = "rucio.db.sqla.models:BASE"


# --------------------------- CLI parsing -----------------------------------

def _parse_arguments(argv: "Sequence[str]") -> argparse.Namespace:
    """Parse command line arguments.

    Parameters
    ----------
    argv:
        Raw command line parameters, typically ``sys.argv[1:]``.

    Returns
    -------
    argparse.Namespace
        The parsed arguments including the ``verbose`` flag.
    """

    p = argparse.ArgumentParser(description="Exhaustive Alembic history validation")
    p.add_argument("--verbose", action="store_true", help="Verbose logging")
    return p.parse_args(argv)


# --------------------------- Utilities -------------------------------------

def _load_models_metadata(models_expr: str) -> MetaData:
    """Resolve a dotted expression describing the SQLAlchemy models metadata.

    The utility accepts two forms:

    * ``"package.module"`` – the module is imported and an attribute named
      ``BASE``, ``Base`` or ``metadata`` is looked up automatically.
    * ``"package.module:Attribute"`` – the module is imported and the provided
      attribute is retrieved explicitly.

    In both cases the resolved object can either be a declarative base (with a
    ``metadata`` attribute) or a :class:`sqlalchemy.MetaData` instance.

    Parameters
    ----------
    models_expr:
        Expression describing where to locate the metadata object.

    Returns
    -------
    sqlalchemy.MetaData
        The metadata object that describes the canonical database schema.

    Raises
    ------
    TypeError
        If the resolved object is not a :class:`~sqlalchemy.MetaData` instance.
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


def _resolve_alembic_config_path() -> Path:
    """Return the absolute path to the Alembic configuration file.

    The history checker now mirrors the rest of the tooling: it first respects
    the ``ALEMBIC_CONFIG`` environment variable (which is how the developer and
    CI containers expose their generated configuration) and only falls back to
    the repository default when that override is absent.

    Returns
    -------
    pathlib.Path
        Absolute path to the Alembic configuration file that should be used for
        the validation run.

    Raises
    ------
    FileNotFoundError
        If neither ``ALEMBIC_CONFIG`` nor ``etc/alembic.ini`` resolve to an
        existing file.
    """

    env_cfg = os.environ.get("ALEMBIC_CONFIG")
    if env_cfg:
        cfg_path = Path(env_cfg).expanduser()
        if cfg_path.is_file():
            return cfg_path.resolve()
        raise FileNotFoundError(
            "Alembic configuration specified via ALEMBIC_CONFIG not found: "
            f"{cfg_path}"
        )

    repo_root = Path(__file__).resolve().parent.parent
    default_alembic_cfg = repo_root / "etc" / "alembic.ini"

    if default_alembic_cfg.is_file():
        return default_alembic_cfg.resolve()

    raise FileNotFoundError(
        "Alembic configuration not found. Set ALEMBIC_CONFIG or provide "
        "etc/alembic.ini in the repository checkout."
    )


def _build_alembic_config() -> AlembicConfig:
    """Construct an Alembic configuration object for the validation run.

    The helper resolves the configuration path and ensures that the
    ``script_location`` entry is absolute so that Alembic can find the revision
    scripts regardless of the current working directory. Relative locations are
    resolved against the directory containing the configuration file, mirroring
    Alembic's own behaviour.
    """
    cfg_path = _resolve_alembic_config_path()
    cfg = AlembicConfig(str(cfg_path))
    # Alembic might be invoked from any cwd; make script_location absolute
    script_loc = cfg.get_main_option("script_location")
    if script_loc and not os.path.isabs(script_loc):
        resolved_script_loc = Path(cfg_path).parent.joinpath(script_loc).resolve()
        cfg.set_main_option("script_location", str(resolved_script_loc))
    return cfg


def _assert_single_head_repo(cfg: AlembicConfig) -> None:
    """Validate the Alembic repository head before running history checks.

    This routine loads the Alembic script directory from the given Config and
    enforces two invariants:

    1) The repository exposes exactly one head (no multi-head branches).
    2) That head equals rucio.db.sqla.alembicrevision.ALEMBIC_REVISION (the
       code's notion of the current head).

    If either condition fails, the process terminates (SystemExit) with a
    message that suggests merging heads or updating ALEMBIC_REVISION. This
    early check prevents running the expensive validation against an
    inconsistent revision graph.
    """

    script = ScriptDirectory.from_config(cfg)
    heads = tuple(script.get_heads())
    if len(heads) != 1:
        raise SystemExit(
            f"[alembic-history] Multiple repo heads: {heads}.\n"
            "Fix by merging heads with `alembic merge`."
        )
    if heads[0] != alembicrevision.ALEMBIC_REVISION:
        raise SystemExit(
            f"[alembic-history] Repo head {heads[0]} != "
            f"ALEMBIC_REVISION {alembicrevision.ALEMBIC_REVISION}."
        )


def _get_engine_from_cfg(cfg: AlembicConfig) -> "Engine":
    """Create a SQLAlchemy Engine from alembic.ini.

    Reads the 'sqlalchemy.url' setting from the provided Alembic Config and
    constructs a new Engine (created with 'future=True').

    Returns
    -------
    sqlalchemy.engine.Engine

    Raises
    ------
    RuntimeError
        If 'sqlalchemy.url' is missing or empty in the loaded configuration.
    """
    url = cfg.get_main_option("sqlalchemy.url")
    if not url:
        raise RuntimeError("No sqlalchemy.url found; configure it in alembic.ini")
    return create_engine(url, future=True)


def validate_alembic_history(
        logger: "Callable[[str], None]",
) -> list[Issue]:
    """Run the end-to-end Alembic history validation for the current repository.

    Workflow
    --------
    1) Build the Alembic Config (including converting 'script_location' to an
       absolute path).
    2) Enforce repository invariants via _assert_single_head_repo:
       - exactly one head; and
       - that head equals ALEMBIC_REVISION.
       On failure this function terminates the process (SystemExit).
    3) Log a heading and delegate to the library validator
       (rucio.db.sqla.util.validate_alembic_history) with explicit loaders:
         • load_config           -> returns the already constructed Config
         • load_models_metadata  -> imports and returns the code's MetaData
         • load_engine           -> creates an Engine from alembic.ini

    Parameters
    ----------
    logger : Callable[[str], None]
        Used to report progress and findings (e.g. 'print').
    Returns
    -------
    list[Issue]
        The issues reported by the library validator.

    Raises
    ------
    SystemExit
        If the repository has multiple heads or the head does not match
        ALEMBIC_REVISION.
    FileNotFoundError, RuntimeError
        Propagated from configuration resolution and Engine creation.
    """
    cfg = _build_alembic_config()
    _assert_single_head_repo(cfg)

    log_heading(logger, "Alembic history validation")

    return _validate_alembic_history(
        logger=logger,
        load_config=lambda: cfg,
        load_models_metadata=lambda: _load_models_metadata(DEFAULT_MODELS_EXPR),
        load_engine=lambda: _get_engine_from_cfg(cfg),
    )


def main(argv: Optional["Sequence[str]"] = None) -> int:
    """Command line entry point used by CI and local developers."""
    args = _parse_arguments(sys.argv[1:] if argv is None else argv)
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO, format="%(message)s")

    def _log(msg: str):
        print(msg, flush=True)

    try:
        issues = validate_alembic_history(
            logger=_log,
        )
    except Exception as exc:
        _log(f"FAILED with unexpected exception: {exc!r}")
        return 2

    if issues:
        issue = issues[0]
        _log("\nValidation halted after the first issue:")
        _log(f"  kind : {issue.kind}")
        _log(f"  at   : {issue.at_revision}")
        if issue.path:
            path_str = " -> ".join(issue.path)
            _log(f"  path : {path_str}")
        _log("  detail:")
        for line in issue.detail.splitlines():
            _log(f"    {line}")
        if len(issues) > 1:
            _log(f"  (additional {len(issues) - 1} issues were also returned)")
        _log("\nSummary: issue detected. Fix the reported drift and rerun the validator.")
        return 1
    else:
        _log("\nSummary: no issues found.")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
