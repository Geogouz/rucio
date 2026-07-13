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
import stat
import subprocess  # noqa: S404
from pathlib import Path
from typing import Optional

import pytest

from tests.ruciopytest.container_manager import (
    REPORT_IMAGE_MANIFEST_ENV,
    REPORT_IMAGE_MANIFEST_PREFIX,
    REPORT_SOURCE_GID_ENV,
    REPORT_SOURCE_UID_ENV,
    ContainerManager,
    open_test_log_file,
)
from tests.ruciopytest.profiles import get_case
from tools.devenv.runtime import RUNTIME_INPUTS


def _write_runtime_build_inputs(root: Path) -> None:
    for relative in RUNTIME_INPUTS:
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f"fixture for {relative.as_posix()}\n")


def _manager(
    tmp_path: "Path",
    *,
    keep_db: bool = False,
    image: Optional[str] = "runtime:test",
    environ: Optional[dict[str, str]] = None,
    log_output: bool = False,
) -> ContainerManager:
    environment = {"RUCIO_TEST_NATIVE_PLATFORM": "linux/amd64"}
    environment.update(environ or {})
    return ContainerManager(
        get_case("remote-dbs-py39-postgres14"),
        tmp_path,
        keep_db=keep_db,
        image=image,
        environ=environment,
        log_output=log_output,
    )


def test_projects_are_unique_by_default(tmp_path: "Path") -> None:
    first = _manager(tmp_path)
    second = _manager(tmp_path)

    assert first.project_name != second.project_name
    assert first.environment["RUCIO_NETWORK_NAME"] != second.environment["RUCIO_NETWORK_NAME"]


def test_explicit_project_nonce_is_stable(tmp_path: "Path") -> None:
    first = _manager(tmp_path, environ={"RUCIO_TEST_PROJECT_NONCE": "0123abcd"})
    second = _manager(tmp_path, environ={"RUCIO_TEST_PROJECT_NONCE": "0123abcd"})

    assert first.project_name == second.project_name
    assert first.project_name.endswith("-0123abcd")


def test_explicit_project_nonce_is_validated(tmp_path: "Path") -> None:
    with pytest.raises(ValueError, match="eight lowercase hexadecimal"):
        _manager(tmp_path, environ={"RUCIO_TEST_PROJECT_NONCE": "unsafe"})


def test_report_source_owner_must_match_host_user(tmp_path: "Path") -> None:
    with pytest.raises(ValueError, match="must match the invoking host user"):
        _manager(
            tmp_path,
            environ={
                REPORT_SOURCE_UID_ENV: str(os.getuid() + 1),
                REPORT_SOURCE_GID_ENV: str(os.getgid()),
            },
        )


def test_reusable_projects_are_stable_per_checkout(tmp_path: "Path") -> None:
    first = _manager(tmp_path, keep_db=True)
    second = _manager(tmp_path, keep_db=True)

    assert first.project_name == second.project_name


@pytest.mark.parametrize(
    "relative_path",
    (
        "lib/rucio/db/sqla/models.py",
        "lib/rucio/db/sqla/migrate_repo/versions/revision.py",
    ),
)
def test_reusable_project_changes_with_database_schema(
    tmp_path: "Path",
    relative_path: str,
) -> None:
    schema_path = tmp_path / relative_path
    schema_path.parent.mkdir(parents=True)
    schema_path.write_text("before")
    before = _manager(tmp_path, keep_db=True)

    schema_path.write_text("after")
    after = _manager(tmp_path, keep_db=True)

    assert before.project_name != after.project_name


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


def test_manager_ignores_inherited_default_platform(tmp_path: "Path") -> None:
    manager = _manager(
        tmp_path,
        environ={"DOCKER_DEFAULT_PLATFORM": "linux/amd64"},
    )

    assert "DOCKER_DEFAULT_PLATFORM" not in manager.environment


def test_manager_ignores_inherited_rucio_home(tmp_path: "Path") -> None:
    manager = _manager(
        tmp_path,
        environ={"RUCIO_HOME": "/host/config"},
    )

    assert manager.environment["RUCIO_HOME"] == "/opt/rucio"


def test_manager_uses_docker_server_platform(tmp_path: "Path", monkeypatch) -> None:
    manager = ContainerManager(
        get_case("remote-dbs-py39-postgres14"),
        tmp_path,
        image="runtime:test",
        environ={},
    )

    def run(command, **kwargs):
        return subprocess.CompletedProcess(
            command,
            0,
            stdout="linux/arm64\n",
        )

    monkeypatch.setattr(manager, "_run", run)

    manager._set_native_platform()

    assert manager.environment["RUCIO_TEST_NATIVE_PLATFORM"] == "linux/arm64"
    assert manager.environment["RUCIO_TEST_IMAGE_PLATFORM"] == "linux/arm64"


def test_compose_commands_use_project_and_service_profiles(tmp_path: "Path") -> None:
    manager = _manager(tmp_path)

    command = manager.compose_command("exec", "rucio", "pytest")

    assert command[:4] == ["docker", "compose", "-p", manager.project_name]
    assert command.count("--profile") == 2
    assert "postgres14" in command
    assert "test-dependencies" in command
    assert "workspace" not in command
    assert command[-3:] == ["exec", "rucio", "pytest"]


def test_debug_compose_adds_only_debug_port(tmp_path: "Path") -> None:
    manager = _manager(tmp_path, environ={"RUCIO_DEV_DEBUG": "1"})

    command = manager.compose_command("up")

    assert not any(argument.endswith("docker-compose.test-instrumentation.yml") for argument in command)
    assert any(argument.endswith("docker-compose.test-debug.yml") for argument in command)
    assert not any(argument.endswith("docker-compose.ports.yml") for argument in command)


def test_cpu_profile_adds_ptrace_instrumentation(tmp_path: "Path") -> None:
    manager = _manager(tmp_path, environ={"RUCIO_DEV_PROFILE": "cpu"})

    command = manager.compose_command("up")

    assert any(argument.endswith("docker-compose.test-instrumentation.yml") for argument in command)


def test_observed_test_adds_telemetry_overlay(tmp_path: "Path") -> None:
    manager = _manager(tmp_path, environ={"RUCIO_DEV_OBSERVE": "1"})

    command = manager.compose_command("up")

    assert any(argument.endswith("docker-compose.test-observe.yml") for argument in command)


def test_prebuilt_image_skips_build(tmp_path: "Path", monkeypatch) -> None:
    manager = _manager(tmp_path, image="runtime:test")
    commands = []

    def run(command, **kwargs):
        commands.append(list(command))
        return subprocess.CompletedProcess(command, 0, stdout="")

    monkeypatch.setattr(manager, "_run", run)
    manager.start()

    assert not any("build" in command for command in commands)


def test_start_refreshes_dependencies_without_pulling_runtime(
    tmp_path: "Path",
    monkeypatch,
) -> None:
    manager = _manager(tmp_path, image="runtime:test")
    commands = []

    def run(command, **kwargs):
        commands.append((list(command), kwargs))
        stdout = "ruciodb\nrucio\nworkspace\n" if command[-2:] == ["config", "--services"] else ""
        return subprocess.CompletedProcess(command, 0, stdout=stdout)

    monkeypatch.setattr(manager, "_run", run)

    manager.start()

    pull_index = next(index for index, (command, _) in enumerate(commands) if "pull" in command)
    up_index = next(index for index, (command, _) in enumerate(commands) if "up" in command)
    assert commands[pull_index][0][-4:] == [
        "pull",
        "--policy",
        "always",
        "ruciodb",
    ]
    assert commands[pull_index][1]["timeout"] == 900
    assert pull_index < up_index


def test_local_image_uses_runtime_dockerfile(tmp_path: "Path", monkeypatch) -> None:
    ContainerManager._locally_built_images.clear()
    manager = _manager(tmp_path, image=None)
    commands = []
    timeouts = []
    build_context = None
    staged_files = set()
    _write_runtime_build_inputs(tmp_path)
    (tmp_path / "requirements/untracked-sentinel.txt").write_text("must not be sent\n")

    def run(command, **kwargs):
        nonlocal build_context, staged_files
        commands.append(list(command))
        timeouts.append(kwargs.get("timeout"))
        if command[:3] == ["docker", "buildx", "build"]:
            build_context = Path(command[-1])
            staged_files = {
                path.relative_to(build_context)
                for path in build_context.rglob("*")
                if path.is_file()
            }
        return subprocess.CompletedProcess(command, 0, stdout="")

    monkeypatch.setattr(manager, "_run", run)
    manager.start()

    assert commands[0][:3] == ["docker", "buildx", "build"]
    assert staged_files == set(RUNTIME_INPUTS)
    assert build_context is not None
    assert commands[0][commands[0].index("--file") + 1] == str(build_context / manager.RUNTIME_DOCKERFILE)
    assert not build_context.exists()
    assert commands[0][commands[0].index("--platform") + 1] == "linux/amd64"
    assert commands[0][commands[0].index("--target") + 1] == "final"
    assert timeouts[0] == 3600


def test_local_runtime_is_built_once_per_identity(tmp_path: "Path", monkeypatch) -> None:
    ContainerManager._locally_built_images.clear()
    _write_runtime_build_inputs(tmp_path)
    managers = [_manager(tmp_path, image=None), _manager(tmp_path, image=None)]
    commands = []

    def run(command, **kwargs):
        commands.append(list(command))
        return subprocess.CompletedProcess(command, 0, stdout="")

    for manager in managers:
        monkeypatch.setattr(manager, "_run", run)
        manager.start()

    assert sum(command[:3] == ["docker", "buildx", "build"] for command in commands) == 1


def test_oracle_runtime_has_distinct_local_build_identity(tmp_path: "Path", monkeypatch) -> None:
    ContainerManager._locally_built_images.clear()
    _write_runtime_build_inputs(tmp_path)
    managers = [
        ContainerManager(
            get_case(case),
            tmp_path,
            image=None,
            environ={"RUCIO_TEST_NATIVE_PLATFORM": "linux/arm64"},
        )
        for case in (
            "remote-dbs-py39-postgres14",
            "remote-dbs-py39-oracle",
        )
    ]
    commands = []

    def run(command, **kwargs):
        commands.append(list(command))
        return subprocess.CompletedProcess(command, 0, stdout="")

    for manager in managers:
        monkeypatch.setattr(manager, "_run", run)
        manager.start()

    builds = [command for command in commands if command[:3] == ["docker", "buildx", "build"]]
    assert len(builds) == 2
    assert managers[0].image.endswith("-py39")
    assert managers[1].image.endswith("-py39-oracle")
    assert managers[0].environment["RUCIO_TEST_IMAGE_PLATFORM"] == "linux/arm64"
    assert managers[1].environment["RUCIO_TEST_IMAGE_PLATFORM"] == "linux/amd64"
    assert managers[0].environment["RUCIO_TEST_IMAGE_TARGET"] == "final"
    assert managers[1].environment["RUCIO_TEST_IMAGE_TARGET"] == "oracle"
    assert builds[0][builds[0].index("--target") + 1] == "final"
    assert builds[0][builds[0].index("--platform") + 1] == "linux/arm64"
    assert builds[1][builds[1].index("--target") + 1] == "oracle"
    assert builds[1][builds[1].index("--platform") + 1] == "linux/amd64"


def test_runtime_image_can_be_selected_per_python(tmp_path: "Path") -> None:
    manager = _manager(
        tmp_path,
        image=None,
        environ={"RUCIO_TEST_IMAGE_PY39": "runtime:py39"},
    )

    assert manager.image == "runtime:py39"
    assert not manager.build_local


def test_runtime_images_are_selected_per_target(tmp_path: "Path") -> None:
    environment = {
        "RUCIO_TEST_IMAGE_PY39": "runtime:final",
        "RUCIO_TEST_IMAGE_PY39_ORACLE": "runtime:oracle",
    }
    final = ContainerManager(
        get_case("remote-dbs-py39-postgres14"),
        tmp_path,
        environ=environment,
    )
    oracle = ContainerManager(
        get_case("remote-dbs-py39-oracle"),
        tmp_path,
        environ=environment,
    )

    assert final.image == "runtime:final"
    assert oracle.image == "runtime:oracle"
    assert not final.build_local
    assert not oracle.build_local


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
    assert "--remove-orphans" in down
    assert down[-1] == "--volumes"
    assert all("compose ls" not in " ".join(command) for command in commands)


def test_report_cleanup_restores_source_ownership_before_compose_down(tmp_path: "Path", monkeypatch) -> None:
    manager = _manager(
        tmp_path,
        environ={
            REPORT_SOURCE_UID_ENV: str(os.getuid()),
            REPORT_SOURCE_GID_ENV: str(os.getgid()),
        },
    )
    commands = []

    def run(command, **kwargs):
        commands.append(list(command))
        return subprocess.CompletedProcess(command, 0, stdout="")

    monkeypatch.setattr(manager, "_run", run)
    manager.stop()

    chown = next(command for command in commands if "chown" in command)
    down = next(command for command in commands if "down" in command)
    assert commands.index(chown) < commands.index(down)
    assert chown[-6:] == [
        "chown",
        "--recursive",
        "--no-dereference",
        f"{os.getuid()}:{os.getgid()}",
        "--",
        "/rucio_source",
    ]


def test_report_manifest_uses_images_from_running_service_containers(
    tmp_path: "Path",
    monkeypatch,
    capsys,
) -> None:
    manager = _manager(tmp_path, environ={REPORT_IMAGE_MANIFEST_ENV: "1"})
    runtime_id = "sha256:" + "1" * 64
    database_id = "sha256:" + "2" * 64
    compose_config = {
        "services": {
            "rucio": {"build": {"target": "final"}, "image": "runtime:test"},
            "ruciodb": {"image": "docker.io/postgres:14"},
        }
    }

    def run(command, **kwargs):
        if "config" in command:
            stdout = json.dumps(compose_config)
        elif "ps" in command:
            stdout = "runtime-container\n" if command[-1] == "rucio" else "database-container\n"
        elif list(command[:3]) == ["docker", "container", "inspect"]:
            stdout = json.dumps({"Image": runtime_id if command[-1] == "runtime-container" else database_id})
        elif list(command[:3]) == ["docker", "image", "inspect"]:
            image_id = command[-1]
            stdout = json.dumps({"Id": image_id, "RepoDigests": [f"example.test/image@{image_id}"]})
        else:
            raise AssertionError(command)
        return subprocess.CompletedProcess(command, 0, stdout=stdout)

    monkeypatch.setattr(manager, "_run", run)
    monkeypatch.setattr(
        manager,
        "_runtime_environment_record",
        lambda: {
            "system": "Linux",
            "release": "6.8",
            "machine": "aarch64",
            "python": "3.10.4",
            "cpu_count": 8,
            "affinity_count": 6,
            "cpu_quota": 6.0,
            "cpu_limit_source": "cgroup-v2",
            "memory_limit_bytes": 6 * 1024**3,
            "memory_limit_source": "cgroup-v2",
            "xdist_version": "3.5.0",
        },
    )
    manager._emit_image_manifest()

    output = capsys.readouterr().out.strip()
    assert output.startswith(REPORT_IMAGE_MANIFEST_PREFIX)
    manifest = json.loads(output.removeprefix(REPORT_IMAGE_MANIFEST_PREFIX))
    assert manifest["runtime"]["image_id"] == runtime_id
    assert manifest["runtime_environment"]["cpu_quota"] == 6.0
    assert manifest["dependencies"][0]["image_id"] == database_id


def test_report_runtime_probe_executes_in_running_test_container(tmp_path: "Path", monkeypatch) -> None:
    manager = _manager(tmp_path)
    probe = {
        "system": "Linux",
        "release": "6.8",
        "machine": "aarch64",
        "python": "3.10.4",
        "cpu_count": 8,
        "affinity_count": 6,
        "cpu_quota": 6.0,
        "cpu_limit_source": "cgroup-v2",
        "memory_limit_bytes": 6 * 1024**3,
        "memory_limit_source": "cgroup-v2",
        "xdist_version": "3.5.0",
    }
    captured = {}

    def execute(service, *arguments, **kwargs):
        captured.update(service=service, arguments=arguments, kwargs=kwargs)
        return subprocess.CompletedProcess(arguments, 0, stdout=json.dumps(probe))

    monkeypatch.setattr(manager, "exec", execute)

    assert manager._runtime_environment_record() == probe
    assert captured["service"] == "rucio"
    assert captured["arguments"][:2] == ("/opt/venv/bin/python", "-c")
    assert captured["kwargs"] == {"capture_output": True, "timeout": 30}


@pytest.mark.parametrize("linked_component", ("log-root", "project"))
def test_test_log_directories_do_not_follow_symlinks(
    tmp_path: "Path",
    linked_component: str,
) -> None:
    external = tmp_path / "external"
    external.mkdir()
    external.chmod(0o755)
    log_root = tmp_path / ".test-logs"
    if linked_component == "log-root":
        log_root.symlink_to(external, target_is_directory=True)
    else:
        log_root.mkdir()
        (log_root / "project").symlink_to(external, target_is_directory=True)

    with pytest.raises(RuntimeError, match="safe"):
        with open_test_log_file(tmp_path, "project", "case.log", "w"):
            pass

    assert stat.S_IMODE(external.stat().st_mode) == 0o755
    assert not (external / "case.log").exists()


@pytest.mark.parametrize("link_type", ("symbolic", "hard"))
def test_test_log_files_do_not_follow_links(tmp_path: "Path", link_type: str) -> None:
    external = tmp_path / "external.log"
    external.write_text("preserve me")
    external.chmod(0o644)
    log_dir = tmp_path / ".test-logs/project"
    log_dir.mkdir(parents=True)
    log_path = log_dir / "case.log"
    if link_type == "symbolic":
        log_path.symlink_to(external)
    else:
        os.link(external, log_path)

    with pytest.raises(RuntimeError, match="safe"):
        with open_test_log_file(tmp_path, "project", "case.log", "w"):
            pass

    assert external.read_text() == "preserve me"
    assert stat.S_IMODE(external.stat().st_mode) == 0o644


def test_test_log_files_must_be_regular(tmp_path: "Path") -> None:
    log_dir = tmp_path / ".test-logs/project"
    log_dir.mkdir(parents=True)
    os.mkfifo(log_dir / "case.log")

    with pytest.raises(RuntimeError, match="safe"):
        with open_test_log_file(tmp_path, "project", "case.log", "w"):
            pass


@pytest.mark.parametrize(
    ("project_name", "filename"),
    (("../outside", "case.log"), ("project", "../outside.log")),
)
def test_test_log_paths_require_single_components(
    tmp_path: "Path",
    project_name: str,
    filename: str,
) -> None:
    with pytest.raises(ValueError, match="single path components"):
        open_test_log_file(tmp_path, project_name, filename, "w")


def test_test_log_directories_must_be_owned_by_current_user(tmp_path: "Path", monkeypatch) -> None:
    log_dir = tmp_path / ".test-logs/project"
    log_dir.mkdir(parents=True)
    log_root = log_dir.parent
    log_root.chmod(0o755)
    actual_uid = os.getuid()
    monkeypatch.setattr(os, "getuid", lambda: actual_uid + 1)

    with pytest.raises(RuntimeError, match="unsafe"):
        with open_test_log_file(tmp_path, "project", "case.log", "w"):
            pass

    assert stat.S_IMODE(log_root.stat().st_mode) == 0o755
    assert not (log_dir / "case.log").exists()


def test_test_log_writes_keep_the_verified_file_descriptor(tmp_path: "Path") -> None:
    external = tmp_path / "external.log"
    external.write_text("preserve me")

    with open_test_log_file(tmp_path, "project", "case.log", "w") as output:
        log_path = tmp_path / ".test-logs/project/case.log"
        log_path.unlink()
        log_path.symlink_to(external)
        output.write("private output")

    assert external.read_text() == "preserve me"


def test_capture_logs_includes_httpd_errors(tmp_path: "Path", monkeypatch) -> None:
    manager = _manager(tmp_path)
    manager.log_dir.mkdir(parents=True)
    (tmp_path / ".test-logs").chmod(0o755)
    manager.log_dir.chmod(0o755)
    for filename in ("compose.log", "httpd_error.log"):
        path = manager.log_dir / filename
        path.write_text("old log")
        path.chmod(0o644)

    def run(command, **kwargs):
        stdout = "httpd error" if any(argument.endswith("httpd_error_log") for argument in command) else "compose log"
        return subprocess.CompletedProcess(command, 0, stdout=stdout)

    monkeypatch.setattr(manager, "_run", run)

    manager.capture_logs()

    assert (manager.log_dir / "compose.log").read_text() == "compose log"
    assert (manager.log_dir / "httpd_error.log").read_text() == "httpd error"
    assert stat.S_IMODE((tmp_path / ".test-logs").stat().st_mode) == 0o700
    assert stat.S_IMODE(manager.log_dir.stat().st_mode) == 0o700
    assert stat.S_IMODE((manager.log_dir / "compose.log").stat().st_mode) == 0o600
    assert stat.S_IMODE((manager.log_dir / "httpd_error.log").stat().st_mode) == 0o600


def test_case_output_is_written_to_one_log(tmp_path: "Path", monkeypatch) -> None:
    manager = _manager(tmp_path, log_output=True)
    manager.log_dir.mkdir(parents=True)
    (tmp_path / ".test-logs").chmod(0o755)
    manager.log_dir.chmod(0o755)
    manager.output_log.write_text("")
    manager.output_log.chmod(0o644)

    def run(command, **kwargs):
        if kwargs.get("capture_output"):
            return subprocess.CompletedProcess(
                command,
                0,
                stdout="captured output\n",
                stderr="captured error\n",
            )
        kwargs["stdout"].write("streamed output\n")
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(subprocess, "run", run)

    manager._run(("stream",))
    manager._run(("capture",), capture_output=True)

    assert manager.output_log.read_text() == ("streamed output\ncaptured output\ncaptured error\n")
    assert stat.S_IMODE((tmp_path / ".test-logs").stat().st_mode) == 0o700
    assert stat.S_IMODE(manager.log_dir.stat().st_mode) == 0o700
    assert stat.S_IMODE(manager.output_log.stat().st_mode) == 0o600


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
    first = _manager(tmp_path, keep_db=True, log_output=True)
    second = _manager(tmp_path, keep_db=True, log_output=True)
    first.log_dir.mkdir(parents=True)
    first.output_log.write_text("active run\n")
    first._acquire_project_lock()
    try:
        assert stat.S_IMODE((tmp_path / ".test-logs").stat().st_mode) == 0o700
        assert stat.S_IMODE(first.log_dir.stat().st_mode) == 0o700
        assert stat.S_IMODE((first.log_dir / "project.lock").stat().st_mode) == 0o600
        with pytest.raises(RuntimeError, match="already running"):
            second.start()
        assert first.output_log.read_text() == "active run\n"
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

    with pytest.raises(RuntimeError, match="Failed to clean up test project"):
        manager.stop()
    manager.stop()

    assert sum("down" in command for command in commands) == 2
    assert manager._stopped


def test_cleanup_failure_does_not_hide_test_failure(
    tmp_path: "Path",
    monkeypatch,
    capsys,
) -> None:
    manager = _manager(tmp_path)

    def run(command, **kwargs):
        returncode = 1 if "down" in command else 0
        return subprocess.CompletedProcess(command, returncode, stdout="")

    monkeypatch.setattr(manager, "_run", run)

    manager.__exit__(ValueError, ValueError("test failed"), None)

    assert not manager._stopped
    assert "Warning: Failed to clean up test project" in capsys.readouterr().err
