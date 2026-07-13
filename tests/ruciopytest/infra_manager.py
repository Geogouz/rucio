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

import argparse
import os
import shutil
import socket
import subprocess  # noqa: S404
import time
from pathlib import Path
from typing import TYPE_CHECKING

from .multi_vo_support import generate_multi_vo_configs
from .votest_support import load_matrix, rewrite_policy_section

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence
    from typing import Optional

    from .profiles import TestCase


class InfraManager:
    def __init__(
        self,
        case: "TestCase",
        *,
        repo_root: Path = Path("/rucio_source"),
        keep_db: bool = False,
        environ: "Optional[Mapping[str, str]]" = None,
    ) -> None:
        self.case = case
        self.repo_root = repo_root
        self.keep_db = keep_db
        self.environment = dict(case.env_vars)
        self.environment.update(os.environ if environ is None else environ)
        self.rucio_home = Path(
            self.environment.get("RUCIO_HOME", "/opt/rucio")
        )

    def setup(self) -> None:
        self._ensure_memcached()
        self._cleanup_temporary_state()

        multi_vo_leg = self.environment.get("RUCIO_MULTI_VO_LEG")
        if self.case.suite == "multi_vo":
            generate_multi_vo_configs(
                self.repo_root,
                Path("/opt/rucio/etc/multi_vo"),
            )

        if self.case.policy:
            self._apply_policy()

        self._wait_for_database()
        database_ready = self.keep_db and self._database_initialized()
        second_vo = self.case.suite == "multi_vo" and multi_vo_leg == "ts2"
        if not database_ready and not second_vo:
            self._reset_database()
        if not database_ready and not second_vo:
            self._run_alembic_migration()

        self._restart_httpd()
        self._run_tool("tools/bootstrap_tests.py")
        self._run_tool("tools/sync_rses.py")
        self._run_tool("tools/sync_meta.py")

        if self.case.suite == "integration":
            self._configure_integration()
            self._run_tool("tools/docker_activate_rses.sh")
        if self.case.suite == "client":
            shutil.copyfile(
                self.repo_root / "etc/docker/test/extra/rucio_client.cfg",
                self.rucio_home / "etc/rucio.cfg",
            )

    def _database_initialized(self) -> bool:
        try:
            from sqlalchemy import inspect

            from rucio.db.sqla.session import get_engine

            return "accounts" in inspect(get_engine()).get_table_names()
        except Exception:
            return False

    @staticmethod
    def _wait_for_database() -> None:
        from rucio.db.sqla.session import wait_for_database

        wait_for_database()

    def _reset_database(self) -> None:
        self._run_tool("tools/reset_database.py")

    def _run_alembic_migration(self) -> None:
        environment = dict(self.environment)
        environment["ALEMBIC_CONFIG"] = str(self.rucio_home / "etc/alembic.ini")
        self._run_tool("tools/alembic_migration.sh", environment=environment)

    def _apply_policy(self) -> None:
        matrix = load_matrix(
            self.repo_root / "etc/docker/test/matrix_policy_package_tests.yml"
        )
        rewrite_policy_section(
            str(self.rucio_home / "etc/rucio.cfg"),
            matrix[self.case.policy]["config_overrides"],
        )

    def _configure_integration(self) -> None:
        config_dir = self.rucio_home / "etc"
        accounts = self.repo_root / "etc/rse-accounts.cfg.template"
        shutil.copyfile(accounts, config_dir / "rse-accounts.cfg")
        shutil.copyfile(accounts, config_dir / "rse-accounts.cfg.template")
        shutil.copyfile(
            self.repo_root / "etc/rclone-init.cfg",
            config_dir / "rclone-init.cfg",
        )
        self._run((
            "ln",
            "-sf",
            "/root/.ssh/ruciouser_sshkey",
            "/root/.ssh/id_rsa",
        ))
        self._run((
            "ln",
            "-sf",
            "/root/.ssh/ruciouser_sshkey.pub",
            "/root/.ssh/id_rsa.pub",
        ))

    def _restart_httpd(self) -> None:
        self._run(("httpd", "-k", "graceful"))

    def _run_tool(
        self,
        *arguments: str,
        environment: "Optional[Mapping[str, str]]" = None,
    ) -> None:
        self._run(
            tuple(str(self.repo_root / argument) if index == 0 else argument
                  for index, argument in enumerate(arguments)),
            environment=environment,
        )

    def _run(
        self,
        arguments: "Sequence[str]",
        *,
        environment: "Optional[Mapping[str, str]]" = None,
    ) -> None:
        subprocess.run(  # noqa: S603
            arguments,
            check=True,
            cwd=self.repo_root,
            env=dict(environment or self.environment),
        )

    @staticmethod
    def _cleanup_temporary_state() -> None:
        for token_dir in Path("/tmp").glob(".rucio_*"):
            shutil.rmtree(token_dir, ignore_errors=True)
        shutil.rmtree("/tmp/rucio_rse", ignore_errors=True)

    @staticmethod
    def _ensure_memcached() -> None:
        try:
            with socket.create_connection(("127.0.0.1", 11211), timeout=1) as client:
                client.sendall(b"flush_all\r\n")
                return
        except OSError:
            memcached = shutil.which("memcached")
            if memcached is None:
                raise RuntimeError("memcached executable not found")
            subprocess.run(  # noqa: S603
                (memcached, "-u", "root", "-d"),
                check=True,
            )

        for _ in range(10):
            try:
                with socket.create_connection(("127.0.0.1", 11211), timeout=1) as client:
                    client.sendall(b"flush_all\r\n")
                    return
            except OSError:
                time.sleep(1)
        raise RuntimeError("memcached did not become ready")


def main(arguments: "Optional[Sequence[str]]" = None) -> int:
    from .profiles import get_case

    parser = argparse.ArgumentParser(
        description="Prepare an existing Rucio development container",
    )
    parser.add_argument("--case", required=True)
    parser.add_argument("--keep-db", action="store_true")
    options = parser.parse_args(arguments)
    try:
        case = get_case(options.case)
    except ValueError as error:
        parser.error(str(error))
    InfraManager(case, keep_db=options.keep_db).setup()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
