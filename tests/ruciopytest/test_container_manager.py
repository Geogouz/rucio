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

import json
import os
import subprocess  # noqa: S404
from typing import TYPE_CHECKING

import pytest

from tests.ruciopytest.container_manager import ContainerManager
from tests.ruciopytest.profiles import get_case

if TYPE_CHECKING:
    from pathlib import Path


def _manager(
    tmp_path: "Path",
    *,
    keep_db: bool = False,
    image: str | None = "runtime:test",
    environ: dict[str, str] | None = None,
) -> ContainerManager:
    return ContainerManager(
        get_case("remote-dbs-py39-postgres14"),
        tmp_path,
        keep_db=keep_db,
        image=image,
        environ=environ or {},
    )


def test_projects_are_unique_by_default(tmp_path: "Path") -> None:
    first = _manager(tmp_path)
    second = _manager(tmp_path)

    assert first.project_name != second.project_name
    assert first.environment["RUCIO_NETWORK_NAME"] != second.environment["RUCIO_NETWORK_NAME"]


def test_reusable_projects_are_stable_per_checkout(tmp_path: "Path") -> None:
    first = _manager(tmp_path, keep_db=True)
    second = _manager(tmp_path, keep_db=True)

    assert first.project_name == second.project_name


def test_manager_does_not_mutate_process_environment(tmp_path: "Path", monkeypatch) -> None:
    monkeypatch.delenv("RUCIO_NETWORK_NAME", raising=False)

    _manager(tmp_path)

    assert "RUCIO_NETWORK_NAME" not in os.environ


def test_manager_ignores_inherited_compose_profiles(tmp_path: "Path") -> None:
    manager = _manager(
        tmp_path,
        environ={"COMPOSE_PROFILES": "storage,donotstart"},
    )

    assert "COMPOSE_PROFILES" not in manager.environment


def test_compose_commands_use_project_and_service_profiles(tmp_path: "Path") -> None:
    manager = _manager(tmp_path)

    command = manager.compose_command("exec", "rucio", "pytest")

    assert command[:4] == ["docker", "compose", "-p", manager.project_name]
    assert command.count("--profile") == 1
    assert "postgres14" in command
    assert command[-3:] == ["exec", "rucio", "pytest"]


def test_prebuilt_image_skips_build(tmp_path: "Path", monkeypatch) -> None:
    manager = _manager(tmp_path, image="runtime:test")
    commands = []

    def run(command, **kwargs):
        commands.append(list(command))
        return subprocess.CompletedProcess(command, 0, stdout="")

    monkeypatch.setattr(manager, "_run", run)
    manager.start()

    assert not any("build" in command for command in commands)


def test_local_image_uses_runtime_dockerfile(tmp_path: "Path", monkeypatch) -> None:
    ContainerManager._locally_built_images.clear()
    manager = _manager(tmp_path, image=None)
    commands = []

    def run(command, **kwargs):
        commands.append(list(command))
        return subprocess.CompletedProcess(command, 0, stdout="")

    monkeypatch.setattr(manager, "_run", run)
    manager.start()

    assert commands[0][:3] == ["docker", "buildx", "build"]
    assert str(tmp_path / manager.RUNTIME_DOCKERFILE) in commands[0]


def test_local_runtime_is_built_once_per_interpreter(tmp_path: "Path", monkeypatch) -> None:
    ContainerManager._locally_built_images.clear()
    managers = [_manager(tmp_path, image=None), _manager(tmp_path, image=None)]
    commands = []

    def run(command, **kwargs):
        commands.append(list(command))
        return subprocess.CompletedProcess(command, 0, stdout="")

    for manager in managers:
        monkeypatch.setattr(manager, "_run", run)
        manager.start()

    assert sum(command[:3] == ["docker", "buildx", "build"] for command in commands) == 1


def test_runtime_image_can_be_selected_per_python(tmp_path: "Path") -> None:
    manager = _manager(
        tmp_path,
        image=None,
        environ={"RUCIO_TEST_IMAGE_PY39": "runtime:py39"},
    )

    assert manager.image == "runtime:py39"
    assert not manager.build_local


def test_stop_removes_only_owned_project(tmp_path: "Path", monkeypatch) -> None:
    manager = _manager(tmp_path)
    commands = []

    def run(command, **kwargs):
        commands.append(list(command))
        return subprocess.CompletedProcess(command, 0, stdout="")

    monkeypatch.setattr(manager, "_run", run)
    manager.stop()

    down = commands[-1]
    assert manager.project_name in down
    assert down[-1] == "--volumes"
    assert all("compose ls" not in " ".join(command) for command in commands)


def test_keep_db_preserves_compose_volumes(tmp_path: "Path", monkeypatch) -> None:
    manager = _manager(tmp_path, keep_db=True)
    commands = []

    def run(command, **kwargs):
        commands.append(list(command))
        stdout = '{"volumes": {}}' if "config" in command else ""
        return subprocess.CompletedProcess(command, 0, stdout=stdout)

    monkeypatch.setattr(manager, "_run", run)
    manager.stop()

    down = next(command for command in commands if "down" in command)
    assert "--volumes" not in down


def test_keep_db_removes_only_non_database_volumes(tmp_path: "Path", monkeypatch) -> None:
    manager = _manager(tmp_path, keep_db=True)
    commands = []
    config = {
        "volumes": {
            "database": {
                "name": "owned_database",
                "labels": {"rucio.test.database": "postgres14"},
            },
            "logs": {"name": "owned_logs"},
        },
    }

    def run(command, **kwargs):
        commands.append(list(command))
        stdout = json.dumps(config) if "config" in command else ""
        return subprocess.CompletedProcess(command, 0, stdout=stdout)

    monkeypatch.setattr(manager, "_run", run)

    manager.stop()

    volume_rm = next(command for command in commands if command[:3] == ["docker", "volume", "rm"])
    assert volume_rm == ["docker", "volume", "rm", "owned_logs"]


def test_reusable_project_rejects_concurrent_run(tmp_path: "Path") -> None:
    first = _manager(tmp_path, keep_db=True)
    second = _manager(tmp_path, keep_db=True)
    first._acquire_project_lock()
    try:
        with pytest.raises(RuntimeError, match="already running"):
            second._acquire_project_lock()
    finally:
        first._release_project_lock()


def test_failed_cleanup_can_be_retried(tmp_path: "Path", monkeypatch) -> None:
    manager = _manager(tmp_path)
    down_results = iter((1, 0))
    commands = []

    def run(command, **kwargs):
        commands.append(list(command))
        returncode = next(down_results) if "down" in command else 0
        return subprocess.CompletedProcess(command, returncode, stdout="")

    monkeypatch.setattr(manager, "_run", run)

    manager.stop()
    manager.stop()

    assert sum("down" in command for command in commands) == 2
    assert manager._stopped


def test_podman_uses_the_same_compose_model(tmp_path: "Path") -> None:
    manager = _manager(
        tmp_path,
        image="runtime:test",
        environ={"USE_PODMAN": "1"},
    )

    assert manager.compose_command("up")[:2] == ["podman", "compose"]
