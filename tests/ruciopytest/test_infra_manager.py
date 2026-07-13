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

from typing import TYPE_CHECKING
from unittest.mock import Mock, call

from tests.ruciopytest.infra_manager import InfraManager
from tests.ruciopytest.profiles import get_case

if TYPE_CHECKING:
    from pathlib import Path


def _manager(
    tmp_path: "Path",
    case_id: str = "remote-dbs-py39-postgres14",
    *,
    keep_db: bool = False,
    environment: dict[str, str] | None = None,
) -> InfraManager:
    return InfraManager(
        get_case(case_id),
        repo_root=tmp_path,
        keep_db=keep_db,
        environ=environment or {},
    )


def _mock_setup_steps(manager: InfraManager, monkeypatch) -> Mock:
    calls = Mock()
    for method in (
        "_ensure_memcached",
        "_cleanup_temporary_state",
        "_database_initialized",
        "_reset_database",
        "_run_alembic_migration",
        "_restart_httpd",
        "_run_tool",
    ):
        monkeypatch.setattr(manager, method, getattr(calls, method))
    calls._database_initialized.return_value = True
    return calls


def test_standard_setup_preserves_initialization_order(tmp_path: "Path", monkeypatch) -> None:
    manager = _manager(tmp_path)
    calls = _mock_setup_steps(manager, monkeypatch)

    manager.setup()

    assert calls.mock_calls == [
        call._ensure_memcached(),
        call._cleanup_temporary_state(),
        call._reset_database(),
        call._run_alembic_migration(),
        call._restart_httpd(),
        call._run_tool("tools/bootstrap_tests.py"),
        call._run_tool("tools/sync_rses.py"),
        call._run_tool("tools/sync_meta.py"),
    ]


def test_keep_db_skips_only_database_reset(tmp_path: "Path", monkeypatch) -> None:
    manager = _manager(tmp_path, keep_db=True)
    calls = _mock_setup_steps(manager, monkeypatch)

    manager.setup()

    calls._reset_database.assert_not_called()
    calls._run_alembic_migration.assert_called_once()
    assert calls._run_tool.call_count == 3


def test_keep_db_initializes_an_empty_database(tmp_path: "Path", monkeypatch) -> None:
    manager = _manager(tmp_path, keep_db=True)
    calls = _mock_setup_steps(manager, monkeypatch)
    calls._database_initialized.return_value = False

    manager.setup()

    calls._reset_database.assert_called_once()


def test_second_multi_vo_leg_reuses_shared_database(tmp_path: "Path", monkeypatch) -> None:
    manager = _manager(
        tmp_path,
        "multi-vo-py39-postgres14",
        environment={
            "RUCIO_HOME": "/opt/rucio/etc/multi_vo/ts2",
            "RUCIO_MULTI_VO_LEG": "ts2",
        },
    )
    calls = _mock_setup_steps(manager, monkeypatch)
    monkeypatch.setattr(
        "tests.ruciopytest.infra_manager.generate_multi_vo_configs",
        Mock(),
    )

    manager.setup()

    calls._reset_database.assert_not_called()
    calls._run_alembic_migration.assert_not_called()
    assert calls._run_tool.call_count == 3


def test_integration_setup_activates_storage_rses(tmp_path: "Path", monkeypatch) -> None:
    manager = _manager(tmp_path, "integration-py39-postgres14")
    calls = _mock_setup_steps(manager, monkeypatch)

    manager.setup()

    calls._run_tool.assert_any_call("tools/docker_activate_rses.sh")
