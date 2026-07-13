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

from typing import TYPE_CHECKING, Optional
from unittest.mock import MagicMock, Mock, call

import pytest

from tests.ruciopytest import infra_manager
from tests.ruciopytest.infra_manager import InfraManager
from tests.ruciopytest.profiles import get_case

if TYPE_CHECKING:
    from pathlib import Path


def _manager(
    tmp_path: "Path",
    case_id: str = "remote-dbs-py39-postgres14",
    *,
    keep_db: bool = False,
    environment: Optional[dict[str, str]] = None,
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
        "_wait_for_database",
        "_database_initialized",
        "_reset_database",
        "_run_alembic_migration",
        "_upgrade_database",
        "_restart_httpd",
        "_configure_integration",
        "_run_tool",
    ):
        monkeypatch.setattr(manager, method, getattr(calls, method))
    calls._database_initialized.return_value = True
    return calls


def test_canonical_test_setup_preserves_initialization_order(tmp_path: "Path", monkeypatch) -> None:
    manager = _manager(tmp_path, environment={"RUCIO_PYTEST_INNER": "1"})
    calls = _mock_setup_steps(manager, monkeypatch)

    manager.setup()

    assert calls.mock_calls == [
        call._ensure_memcached(),
        call._cleanup_temporary_state(),
        call._wait_for_database(),
        call._reset_database(),
        call._run_alembic_migration(),
        call._restart_httpd(),
        call._run_tool("tools/bootstrap_tests.py"),
        call._run_tool("tools/sync_rses.py"),
        call._run_tool("tools/sync_meta.py"),
    ]


def test_development_setup_reloads_httpd_after_schema_upgrade(tmp_path: "Path", monkeypatch) -> None:
    manager = _manager(tmp_path)
    calls = _mock_setup_steps(manager, monkeypatch)

    manager.setup()

    calls._restart_httpd.assert_called_once()
    calls._run_alembic_migration.assert_not_called()
    calls._upgrade_database.assert_called_once()


def test_keep_db_upgrades_without_destructive_migration(
    tmp_path: "Path",
    monkeypatch,
) -> None:
    manager = _manager(tmp_path, keep_db=True)
    calls = _mock_setup_steps(manager, monkeypatch)

    manager.setup()

    calls._reset_database.assert_not_called()
    calls._run_alembic_migration.assert_not_called()
    calls._upgrade_database.assert_called_once()
    assert calls._run_tool.call_count == 3


def test_canonical_keep_db_upgrades_without_destructive_migration(
    tmp_path: "Path",
    monkeypatch,
) -> None:
    manager = _manager(
        tmp_path,
        keep_db=True,
        environment={"RUCIO_PYTEST_INNER": "1"},
    )
    calls = _mock_setup_steps(manager, monkeypatch)

    manager.setup()

    calls._reset_database.assert_not_called()
    calls._run_alembic_migration.assert_not_called()
    calls._upgrade_database.assert_called_once()
    calls._restart_httpd.assert_called_once()


def test_keep_db_initializes_an_empty_database(tmp_path: "Path", monkeypatch) -> None:
    manager = _manager(tmp_path, keep_db=True)
    calls = _mock_setup_steps(manager, monkeypatch)
    calls._database_initialized.return_value = False

    manager.setup()

    calls._reset_database.assert_called_once()


def test_initialized_database_uses_configured_schema(
    tmp_path: "Path",
    monkeypatch,
) -> None:
    manager = _manager(tmp_path, keep_db=True)
    inspector = Mock()
    inspector.has_table.return_value = True
    monkeypatch.setattr("sqlalchemy.inspect", Mock(return_value=inspector))
    monkeypatch.setattr("rucio.common.config.config_get", Mock(return_value="dev"))
    monkeypatch.setattr("rucio.db.sqla.session.get_engine", Mock())

    assert manager._database_initialized()
    inspector.has_table.assert_called_once_with("accounts", schema="dev")


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
    calls._upgrade_database.assert_not_called()
    assert calls._run_tool.call_count == 3


def test_exec_environment_overrides_case_defaults(tmp_path: "Path") -> None:
    manager = _manager(
        tmp_path,
        "multi-vo-py39-postgres14",
        environment={"RUCIO_HOME": "/opt/rucio/etc/multi_vo/ts2"},
    )

    assert manager.rucio_home.as_posix() == "/opt/rucio/etc/multi_vo/ts2"


def test_development_upgrade_only_moves_schema_to_head(tmp_path: "Path", monkeypatch) -> None:
    manager = _manager(tmp_path, environment={"RUCIO_HOME": "/opt/rucio"})
    run = Mock()
    monkeypatch.setattr(manager, "_run", run)

    manager._upgrade_database()

    run.assert_called_once_with((
        "alembic",
        "-c",
        "/opt/rucio/etc/alembic.ini",
        "upgrade",
        "head",
    ))


def test_setup_rejects_mismatched_database(tmp_path: "Path") -> None:
    manager = _manager(tmp_path, environment={"RDBMS": "oracle"})

    with pytest.raises(RuntimeError, match="requires postgres14"):
        manager.setup()


def test_integration_setup_activates_storage_rses(tmp_path: "Path", monkeypatch) -> None:
    manager = _manager(tmp_path, "integration-py39-postgres14")
    calls = _mock_setup_steps(manager, monkeypatch)

    manager.setup()

    calls._configure_integration.assert_called_once()
    calls._run_tool.assert_any_call("tools/docker_activate_rses.sh")


@pytest.mark.parametrize(
    ("case_id", "configures_integration"),
    (
        ("remote-dbs-py39-postgres14", False),
        ("integration-py39-postgres14", True),
    ),
)
def test_runtime_preparation_only_initializes_container_local_state(
    tmp_path: "Path",
    monkeypatch,
    case_id: str,
    configures_integration: bool,
) -> None:
    manager = _manager(tmp_path, case_id)
    ensure_memcached = Mock()
    cleanup_temporary_state = Mock()
    configure_integration = Mock()
    monkeypatch.setattr(manager, "_ensure_memcached", ensure_memcached)
    monkeypatch.setattr(manager, "_cleanup_temporary_state", cleanup_temporary_state)
    monkeypatch.setattr(manager, "_configure_integration", configure_integration)

    manager.prepare_runtime()

    ensure_memcached.assert_called_once_with(flush=False)
    cleanup_temporary_state.assert_called_once_with()
    assert configure_integration.call_count == int(configures_integration)


@pytest.mark.parametrize("flush", (False, True))
def test_memcached_probe_only_flushes_when_requested(monkeypatch, flush: bool) -> None:
    client = MagicMock()
    connection = MagicMock()
    connection.__enter__.return_value = client
    monkeypatch.setattr(infra_manager.socket, "create_connection", Mock(return_value=connection))

    InfraManager._ensure_memcached(flush=flush)

    assert client.sendall.call_count == int(flush)


def test_integration_configures_protocol_credentials(tmp_path: "Path", monkeypatch) -> None:
    home = tmp_path / "home"
    config_dir = home / "etc"
    config_dir.mkdir(parents=True)
    source_dir = tmp_path / "etc"
    source_dir.mkdir()
    (source_dir / "rse-accounts.cfg.template").write_text("accounts")
    (source_dir / "rclone-init.cfg").write_text("rclone")
    manager = _manager(
        tmp_path,
        "integration-py39-postgres14",
        environment={"RUCIO_HOME": str(home)},
    )
    run = Mock()
    monkeypatch.setattr(manager, "_run", run)

    manager._configure_integration()

    assert (config_dir / "rse-accounts.cfg").read_text() == "accounts"
    assert (config_dir / "rse-accounts.cfg.template").read_text() == "accounts"
    assert (config_dir / "rclone-init.cfg").read_text() == "rclone"
    assert run.mock_calls == [
        call((
            "install",
            "-m",
            "0600",
            "/root/.ssh/ruciouser_sshkey",
            "/root/.ssh/id_rsa",
        )),
        call((
            "ln",
            "-sf",
            "/root/.ssh/ruciouser_sshkey.pub",
            "/root/.ssh/id_rsa.pub",
        )),
    ]


def test_main_prepares_requested_case(monkeypatch) -> None:
    manager = Mock()
    create_manager = Mock(return_value=manager)
    monkeypatch.setattr(infra_manager, "InfraManager", create_manager)

    assert infra_manager.main([
        "--case",
        "integration-py39-postgres14",
        "--keep-db",
    ]) == 0

    case = create_manager.call_args.args[0]
    assert case.id == "integration-py39-postgres14"
    assert create_manager.call_args.kwargs == {"keep_db": True}
    manager.setup.assert_called_once()


def test_main_can_prepare_only_container_local_runtime(monkeypatch) -> None:
    manager = Mock()
    create_manager = Mock(return_value=manager)
    monkeypatch.setattr(infra_manager, "InfraManager", create_manager)

    assert infra_manager.main([
        "--case",
        "integration-py39-postgres14",
        "--runtime-only",
    ]) == 0

    manager.prepare_runtime.assert_called_once()
    manager.setup.assert_not_called()
