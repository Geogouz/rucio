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

from __future__ import annotations

import json
import os
import stat
import subprocess  # noqa: S404
from contextlib import nullcontext
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pytest

from tests.ruciopytest.container_manager import ContainerManager
from tools.devenv import artifacts, cli
from tools.devenv.compose import DAEMON_PROFILES, ComposeProject, profiles_for_daemon, workspace_lock


def test_parser_preserves_profile_target_arguments() -> None:
    args = cli.parser().parse_args(
        [
            "profile",
            "cpu",
            "test",
            "remote-dbs-py310-postgres14",
            "--",
            "tests/test_ping.py",
        ]
    )

    assert args.command == "profile"
    assert args.kind == "cpu"
    assert args.idle is False
    assert args.target == "test"
    assert args.arguments == [
        "remote-dbs-py310-postgres14",
        "--",
        "tests/test_ping.py",
    ]


def test_parser_accepts_idle_cpu_profile_before_target() -> None:
    args = cli.parser().parse_args([
        "profile",
        "cpu",
        "--idle",
        "test",
        "remote-dbs-py310-postgres14",
        "tests/test_ping.py",
    ])

    assert args.idle is True
    assert args.arguments == [
        "remote-dbs-py310-postgres14",
        "tests/test_ping.py",
    ]


@pytest.mark.parametrize(
    "prefix",
    (
        ("observe", "test"),
        ("debug", "test"),
        ("profile", "cpu", "test"),
    ),
)
def test_parser_preserves_instrumented_test_axes(prefix: tuple[str, ...]) -> None:
    args = cli.parser().parse_args([
        *prefix,
        "--python",
        "3.13",
        "--rdbms",
        "postgres14",
        "remote_dbs",
        "--",
        "tests/test_ping.py",
    ])

    assert args.arguments == [
        "--python",
        "3.13",
        "--rdbms",
        "postgres14",
        "remote_dbs",
        "--",
        "tests/test_ping.py",
    ]


def test_parser_accepts_explicit_suite_axes() -> None:
    args = cli.parser().parse_args([
        "test",
        "--python",
        "3.13",
        "--rdbms",
        "postgres14",
        "remote_dbs",
        "--",
        "tests/test_ping.py",
    ])

    assert args.python_version == "3.13"
    assert args.rdbms == "postgres14"
    assert args.case == "remote_dbs"
    assert args.pytest_args == ["tests/test_ping.py"]


def test_parser_accepts_existing_webui_checkout() -> None:
    args = cli.parser().parse_args(["webui", "../webui-worktree"])

    assert args.command == "webui"
    assert args.checkout == Path("../webui-worktree")


def test_public_cli_lists_canonical_cases(
    tmp_path: "Path",
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.setattr(cli, "repository_root", lambda: tmp_path)

    assert cli.main(["test", "--list"]) == 0

    output = capsys.readouterr()
    assert "Canonical Rucio test cases:" in output.out
    assert "remote-dbs-py310-postgres14" in output.out
    assert "Supported Python versions: 3.9, 3.10, 3.11, 3.12, 3.13" in output.out
    assert "./tools/dev test --python 3.13 --rdbms postgres14 remote_dbs" in output.out
    assert output.err == ""


@pytest.mark.parametrize(
    "axis",
    (
        ("--python", "3.13"),
        ("--rdbms", "postgres14"),
        ("--policy", "atlas"),
    ),
)
def test_explicit_axes_require_a_positional_suite_before_choosing(
    tmp_path: "Path",
    monkeypatch,
    capsys,
    axis: tuple[str, str],
) -> None:
    monkeypatch.setattr(cli, "repository_root", lambda: tmp_path)
    monkeypatch.setattr(cli, "choose_case", lambda: pytest.fail("must reject before choosing"))

    assert cli.main(("test", *axis)) == 1

    output = capsys.readouterr()
    assert "requires a positional test suite" in output.err


@pytest.mark.parametrize(
    "selection",
    (
        ("unit",),
        ("--python", "3.13"),
        ("--rdbms", "postgres14"),
        ("--policy", "atlas"),
        ("--python", "3.13", "unit"),
    ),
)
def test_case_list_rejects_ignored_selection(
    tmp_path: "Path",
    monkeypatch,
    capsys,
    selection: tuple[str, ...],
) -> None:
    monkeypatch.setattr(cli, "repository_root", lambda: tmp_path)
    monkeypatch.setattr(cli, "list_cases", lambda: pytest.fail("must reject before listing"))

    assert cli.main(("test", "--list", *selection)) == 1

    output = capsys.readouterr()
    assert "--list cannot be combined" in output.err


@pytest.mark.parametrize(
    ("selection", "expected"),
    (
        ("all", "--suite=all"),
        ("remote_dbs", "--suite=remote_dbs"),
        ("remote-dbs-py310-postgres14", "--case=remote-dbs-py310-postgres14"),
    ),
)
def test_test_command_maps_case_and_suite_selection(
    tmp_path: "Path",
    monkeypatch,
    selection: str,
    expected: str,
) -> None:
    monkeypatch.setattr(cli, "control_python", lambda root: root / "control-python")

    command = cli.test_command(tmp_path, selection, ())

    assert command == [
        str(tmp_path / "control-python"),
        "-m",
        "pytest",
        expected,
    ]


def test_test_command_removes_blank_selectors_and_separator(
    tmp_path: "Path",
    monkeypatch,
) -> None:
    monkeypatch.setattr(cli, "control_python", lambda root: root / "control-python")

    command = cli.test_command(
        tmp_path,
        "votest",
        ("", "--", "", "-k", "", "ping"),
        keep_db=True,
        case_workers=2,
        xdist_workers=0,
        python_version="3.13",
        rdbms="postgres14",
        policy="atlas",
        container_environment={"RUCIO_DEV_PROFILE": "cpu"},
    )

    assert command == [
        str(tmp_path / "control-python"),
        "-m",
        "pytest",
        "--suite=votest",
        "--keep-db",
        "--case-workers=2",
        "--xdist-workers=0",
        "--python=3.13",
        "--rdbms=postgres14",
        "--policy=atlas",
        "--container-env",
        "RUCIO_DEV_PROFILE=cpu",
        "-k",
        "ping",
    ]


def test_test_project_name_matches_container_manager(tmp_path: "Path") -> None:
    case = "Remote_DBs Py3.10"
    nonce = "0123abcd"

    assert cli.test_project_name(tmp_path, case, nonce) == ContainerManager.make_project_name(
        case,
        tmp_path,
        reusable=False,
        nonce=nonce,
    )
    assert cli.test_project_name(tmp_path, case, nonce) == "rucio-test-remote-dbs-py3-10-0123abcd"


def test_outer_test_interrupt_requests_graceful_project_cleanup(
    tmp_path: "Path",
    monkeypatch,
) -> None:
    waits = []
    signals = []
    options = {}

    class Process:
        pid = 123

        @staticmethod
        def poll():
            return None

        @staticmethod
        def wait(timeout=None):
            waits.append(timeout)
            if timeout is None:
                raise KeyboardInterrupt
            return 130

    def popen(command, **kwargs):
        options.update(kwargs)
        return Process()

    monkeypatch.setattr(cli, "test_command", lambda *args, **kwargs: ["control-python", "-m", "pytest"])
    monkeypatch.setattr(cli.subprocess, "Popen", popen)
    monkeypatch.setattr(cli, "_signal_process", lambda process, value: signals.append(value))

    with pytest.raises(KeyboardInterrupt):
        cli.run_test(tmp_path, "unit-py310", ())

    assert options["start_new_session"] is (os.name == "posix")
    assert waits == [None, cli.INTERRUPT_GRACE_SECONDS]
    assert signals == [cli.signal.SIGINT]


def test_outer_test_cleanup_escalates_after_bounded_waits(monkeypatch) -> None:
    waits = []
    signals = []

    class Process:
        pid = 123

        @staticmethod
        def poll():
            return None

        @staticmethod
        def wait(timeout=None):
            waits.append(timeout)
            if len(waits) < 3:
                raise subprocess.TimeoutExpired("test", timeout)
            return -9

    monkeypatch.setattr(cli, "_signal_process", lambda process, value: signals.append(value))

    cli._stop_process(Process())

    assert waits == [cli.INTERRUPT_GRACE_SECONDS, cli.TERMINATE_GRACE_SECONDS, None]
    assert signals == [cli.signal.SIGINT, cli.signal.SIGTERM, cli.signal.SIGKILL]


@pytest.mark.parametrize(
    "termination_signal",
    tuple(
        value
        for value in (cli.signal.SIGTERM, getattr(cli.signal, "SIGHUP", None))
        if value is not None
    ),
)
def test_termination_signals_follow_keyboard_interrupt_cleanup_path(
    monkeypatch,
    termination_signal,
) -> None:
    originals = {
        cli.signal.SIGTERM: object(),
        **({cli.signal.SIGHUP: object()} if hasattr(cli.signal, "SIGHUP") else {}),
    }
    handlers = dict(originals)

    def install(value, handler):
        previous = handlers[value]
        handlers[value] = handler
        return previous

    monkeypatch.setattr(cli.signal, "signal", install)

    with pytest.raises(KeyboardInterrupt):
        with cli._termination_signals():
            handlers[termination_signal](termination_signal, None)

    assert handlers == originals


def test_workspace_profile_interrupt_waits_for_container_cleanup(
    tmp_path: "Path",
    monkeypatch,
) -> None:
    events = []
    captured = {}

    class Project:
        environment = {"COMPOSE": "environment"}

        @staticmethod
        def command(*arguments):
            captured["arguments"] = arguments
            return ["docker", "compose", *arguments]

    class Process:
        pid = 123

        @staticmethod
        def poll():
            return None

        @staticmethod
        def wait(timeout=None):
            events.append(("wait", timeout))
            if timeout is None:
                raise KeyboardInterrupt
            return 130

    def popen(command, **kwargs):
        captured["command"] = command
        captured["options"] = kwargs
        return Process()

    monkeypatch.setattr(cli.subprocess, "Popen", popen)
    monkeypatch.setattr(
        cli,
        "_signal_process",
        lambda process, value: events.append(("signal", value)),
    )

    result = cli._run_compose_exec(
        Project(),
        tmp_path,
        "workspace",
        ("sh", "-c", "profile-and-chown"),
        {
            "RUCIO_DEV_ARTIFACT_DIR": "/rucio_source/.rucio-dev/artifacts/run",
            "RUCIO_DEV_HOST_UID": "1234",
            "RUCIO_DEV_HOST_GID": "5678",
        },
    )

    assert result == 130
    assert events == [
        ("wait", None),
        ("signal", cli.signal.SIGINT),
        ("wait", cli.INTERRUPT_GRACE_SECONDS),
    ]
    assert captured["arguments"] == (
        "exec",
        "--no-TTY",
        "--env",
        "RUCIO_DEV_ARTIFACT_DIR=/rucio_source/.rucio-dev/artifacts/run",
        "--env",
        "RUCIO_DEV_HOST_UID=1234",
        "--env",
        "RUCIO_DEV_HOST_GID=5678",
        "workspace",
        "sh",
        "-c",
        "profile-and-chown",
    )
    assert captured["options"] == {
        "cwd": tmp_path,
        "env": Project.environment,
        "start_new_session": os.name == "posix",
    }


def test_performance_report_interrupt_waits_for_cleanup(tmp_path: "Path", monkeypatch) -> None:
    events = []
    captured = {}

    class Process:
        pid = 123

        @staticmethod
        def poll():
            return None

        @staticmethod
        def wait(timeout=None):
            events.append(("wait", timeout))
            if timeout is None:
                raise KeyboardInterrupt
            return 130

    def popen(command, **kwargs):
        captured["command"] = command
        captured["options"] = kwargs
        return Process()

    monkeypatch.setattr(cli, "control_python", lambda root: root / ".venv/bin/python")
    monkeypatch.setattr(cli.subprocess, "Popen", popen)
    monkeypatch.setattr(cli, "_signal_process", lambda process, value: events.append(("signal", value)))

    assert cli.run_performance_report(tmp_path, 3, ("--output", "report.html")) == 130
    assert captured["command"] == [
        str(tmp_path / ".venv/bin/python"),
        str(tmp_path / "tools/test/run_test_performance_report.py"),
        "--workers",
        "3",
        "--output",
        "report.html",
    ]
    assert captured["options"] == {
        "cwd": tmp_path,
        "start_new_session": os.name == "posix",
    }
    assert events == [
        ("wait", None),
        ("signal", cli.signal.SIGINT),
        ("wait", cli.INTERRUPT_GRACE_SECONDS),
    ]


@pytest.mark.parametrize(
    ("public_command", "compose_command"),
    (
        (("shell",), ("exec", "workspace", "/bin/bash")),
        (("logs",), ("logs", "--follow", "rucio", "workspace")),
    ),
)
def test_interactive_commands_use_managed_process_groups(
    tmp_path: "Path",
    monkeypatch,
    public_command: tuple[str, ...],
    compose_command: tuple[str, ...],
) -> None:
    captured = {}

    class Project:
        def __init__(self, root, **kwargs):
            return None

        @staticmethod
        def up() -> None:
            return None

    monkeypatch.setattr(cli, "repository_root", lambda: tmp_path)
    monkeypatch.setattr(cli, "ComposeProject", Project)
    monkeypatch.setattr(
        cli,
        "_run_compose_process",
        lambda project, root, arguments: captured.update(root=root, arguments=tuple(arguments)) or 0,
    )

    assert cli.main(public_command) == 0
    assert captured == {"root": tmp_path, "arguments": compose_command}


def test_logs_follow_existing_services_without_mutating_workspace(
    tmp_path: "Path",
    monkeypatch,
) -> None:
    captured = {}

    class Project:
        def __init__(self, root, **kwargs):
            captured.update(project_root=root, project_options=kwargs)

        @staticmethod
        def up() -> None:
            raise AssertionError("logs must not start services")

        @staticmethod
        def recreate_workspace() -> None:
            raise AssertionError("logs must not recreate services")

    monkeypatch.setattr(cli, "repository_root", lambda: tmp_path)
    monkeypatch.setattr(cli, "ComposeProject", Project)
    monkeypatch.setattr(
        cli,
        "workspace_lock",
        lambda root: pytest.fail("logs must not mutate the workspace lock"),
    )
    monkeypatch.setattr(
        cli,
        "_run_compose_process",
        lambda project, root, arguments: captured.update(
            process_root=root,
            arguments=tuple(arguments),
        ) or 17,
    )

    assert cli.main([
        "logs",
        "--profile",
        "storage",
        "--profile",
        "messaging",
        "--ports",
    ]) == 17
    assert captured == {
        "project_root": tmp_path,
        "project_options": {
            "profiles": ("storage", "messaging"),
            "ports": True,
            "build": False,
        },
        "process_root": tmp_path,
        "arguments": ("logs", "--follow", "rucio", "workspace"),
    }


def test_profile_test_disables_xdist_and_enables_host_instrumentation(
    tmp_path: "Path",
    monkeypatch,
) -> None:
    captured = {}

    class Run:
        container_path = "/rucio_source/.rucio-dev/artifacts/run"
        path = tmp_path / "run"

        def __init__(self, *args, **kwargs) -> None:
            self.path.mkdir()
            captured["artifact"] = kwargs

        def finish(self, returncode: int) -> None:
            captured["returncode"] = returncode

    def run_test(*args, **kwargs) -> int:
        captured["run_test_args"] = args
        captured.update(kwargs)
        return 0

    monkeypatch.setattr(cli, "ArtifactRun", Run)
    monkeypatch.setattr(cli, "docker_platform", lambda root, environment: "linux/arm64")
    monkeypatch.setattr(cli, "run_test", run_test)
    monkeypatch.setattr(cli.secrets, "token_hex", lambda length: "0123abcd")
    monkeypatch.setenv("RUCIO_TEST_IMAGE_PY313_ORACLE", "runtime:py313-oracle")

    assert (
        cli.profile_test(
            tmp_path,
            "cpu",
            (
                "--python",
                "3.13",
                "--rdbms",
                "oracle",
                "remote_dbs",
                "--",
                "tests/test_ping.py::test_rucio_ping_rest",
            ),
            idle=True,
        )
        == 0
    )

    assert captured["run_test_args"][1:] == (
        "remote_dbs",
        ["tests/test_ping.py::test_rucio_ping_rest"],
    )
    assert captured["xdist_workers"] == 0
    assert captured.get("case_workers") is None
    assert captured["python_version"] == "3.13"
    assert captured["rdbms"] == "oracle"
    assert captured["policy"] is None
    assert captured["environment"]["RUCIO_DEV_PROFILE"] == "cpu"
    assert captured["environment"]["RUCIO_DEV_PROFILE_IDLE"] == "1"
    assert captured["environment"]["RUCIO_TEST_PROJECT_NONCE"] == "0123abcd"
    assert captured["environment"]["RUCIO_DEV_STATS_PATH"] == str(Run.path / "docker-stats.jsonl")
    assert captured["container_environment"] == {
        "RUCIO_DEV_ARTIFACT_DIR": Run.container_path,
        "RUCIO_DEV_PROFILE": "cpu",
        "RUCIO_DEV_PROFILE_IDLE": "1",
    }
    assert captured["artifact"]["name"] == "cpu-idle-test-remote-dbs-py313-oracle"
    assert captured["artifact"]["command"][:4] == ("profile", "cpu", "--idle", "test")
    assert captured["artifact"]["image"] == "runtime:py313-oracle"
    assert captured["artifact"]["platform"] == "linux/amd64"
    assert captured["artifact"]["project"] == "rucio-test-remote-dbs-py313-oracle-0123abcd"


def test_observe_test_starts_persistent_backend_and_instruments_one_worker(
    tmp_path: "Path",
    monkeypatch,
    capsys,
) -> None:
    captured = {}

    class Backend:
        environment = {"RUCIO_NETWORK_NAME": "rucio-dev-observe-network"}

        def __init__(self, *args, **kwargs) -> None:
            captured["backend_args"] = args
            captured["backend_options"] = kwargs

        def run(self, *args, **kwargs) -> None:
            captured["backend_command"] = args
            captured["backend_run_options"] = kwargs

    def run_test(*args, **kwargs) -> int:
        captured["run_test_args"] = args
        captured.update(kwargs)
        return 0

    monkeypatch.setattr(cli, "ComposeProject", Backend)
    monkeypatch.setattr(cli, "run_test", run_test)
    monkeypatch.setattr(cli, "workspace_lock", lambda root: nullcontext())
    monkeypatch.setattr(cli.secrets, "token_hex", lambda length: "0123abcd")
    monkeypatch.setattr(cli.time, "time", lambda: 1000.0)

    assert cli.observe_test(
        tmp_path,
        (
            "remote-dbs-py310-postgres14",
            "tests/test_download.py::test_overlapping_containers_and_wildcards",
        ),
    ) == 0

    assert captured["backend_args"] == (tmp_path,)
    assert captured["backend_options"] == {
        "profiles": ("observe",),
        "observe": True,
        "build": False,
    }
    assert captured["backend_command"][-1] == "otel-lgtm"
    assert captured["backend_run_options"] == {"timeout": 240}
    assert captured["run_test_args"][1:] == (
        "remote-dbs-py310-postgres14",
        ["tests/test_download.py::test_overlapping_containers_and_wildcards"],
    )
    assert captured["xdist_workers"] == 0
    assert captured["container_environment"]["RUCIO_DEV_OBSERVE"] == "1"
    assert captured["container_environment"]["OTEL_EXPORTER_OTLP_ENDPOINT"] == "http://otel-lgtm:4318"
    assert captured["container_environment"]["OTEL_LOGS_EXPORTER"] == "none"
    assert captured["container_environment"]["RUCIO_DEV_OBSERVE_RUN_ID"] == "0123abcd"
    assert captured["container_environment"]["RUCIO_DEV_OBSERVE_TEST_CASE"] == (
        "remote-dbs-py310-postgres14"
    )
    assert captured["container_environment"]["RUCIO_DEV_OBSERVE_TEST_SELECTOR"] == (
        "tests/test_download.py::test_overlapping_containers_and_wildcards"
    )
    assert captured["environment"]["RUCIO_DEV_OBSERVE"] == "1"
    assert captured["environment"]["RUCIO_OBSERVE_NETWORK_NAME"] == "rucio-dev-observe-network"
    assert captured["environment"]["RUCIO_TEST_PROJECT_NONCE"] == "0123abcd"
    output = capsys.readouterr().out
    assert "Run: 0123abcd" in output
    assert "Tempo trace: http://127.0.0.1:3001/explore?" in output
    assert "rucio-test-remote-dbs-py310-postgres14-0123abcd" in output
    observation = json.loads(
        (
            tmp_path
            / ".test-logs"
            / "rucio-test-remote-dbs-py310-postgres14-0123abcd"
            / "observation.json"
        ).read_text()
    )
    assert observation == {
        "case": "remote-dbs-py310-postgres14",
        "exit_code": 0,
        "run_id": "0123abcd",
        "selector": "tests/test_download.py::test_overlapping_containers_and_wildcards",
        "started_at_unix_ms": 1_000_000,
        "tempo_query": '{ span.rucio.test.run_id = "0123abcd" }',
        "tempo_url": observation["tempo_url"],
    }
    query = parse_qs(urlparse(observation["tempo_url"]).query)
    panes = json.loads(query["panes"][0])
    assert panes["test"]["queries"][0]["query"] == observation["tempo_query"]
    assert panes["test"]["range"] == {"from": "700000", "to": "87400000"}


@pytest.mark.parametrize(
    ("kind", "selectors", "expected_workers"),
    (
        ("cpu", (), None),
        ("resources", (), None),
        ("cpu", ("tests/test_ping.py",), 0),
        ("resources", ("tests/test_ping.py",), 0),
        ("memory", (), 0),
        ("calls", (), 0),
    ),
)
def test_profile_test_selects_safe_xdist_mode(
    tmp_path: "Path",
    monkeypatch,
    kind: str,
    selectors: tuple[str, ...],
    expected_workers: int | None,
) -> None:
    captured = {}

    class Run:
        container_path = "/rucio_source/.rucio-dev/artifacts/run"
        path = tmp_path / "run"

        def __init__(self, *args, **kwargs) -> None:
            self.path.mkdir()

        @staticmethod
        def finish(returncode: int) -> None:
            return None

    monkeypatch.setattr(cli, "ArtifactRun", Run)
    monkeypatch.setattr(cli, "docker_platform", lambda root, environment: "linux/arm64")
    monkeypatch.setattr(cli, "run_test", lambda *args, **kwargs: captured.update(kwargs) or 0)

    assert cli.profile_test(tmp_path, kind, ("remote-dbs-py310-postgres14", *selectors)) == 0
    assert captured["xdist_workers"] == expected_workers


def test_unit_resource_profile_uses_discoverable_unit_container(
    tmp_path: "Path",
    monkeypatch,
) -> None:
    captured = {}

    class Run:
        container_path = "/rucio_source/.rucio-dev/artifacts/run"
        path = tmp_path / "run"

        def __init__(self, *args, **kwargs) -> None:
            self.path.mkdir()
            captured["artifact"] = kwargs

        def finish(self, returncode: int) -> None:
            captured["returncode"] = returncode

    def run_test(*args, **kwargs) -> int:
        captured.update(kwargs)
        return 0

    monkeypatch.setattr(cli, "ArtifactRun", Run)
    monkeypatch.setattr(cli, "docker_platform", lambda root, environment: "linux/arm64")
    monkeypatch.setattr(cli, "run_test", run_test)
    monkeypatch.setattr(cli.secrets, "token_hex", lambda length: "0123abcd")

    assert cli.profile_test(tmp_path, "resources", ("unit-py310", "tests/rucio/test_common.py")) == 0

    assert captured["artifact"]["image"].startswith("rucio-test-unit:")
    assert captured["artifact"]["image"].endswith("-py310")
    assert captured["artifact"]["platform"] == "linux/arm64"
    assert captured["container_environment"] == {}
    assert captured["environment"]["RUCIO_TEST_PROJECT_NONCE"] == "0123abcd"
    assert captured["environment"]["RUCIO_DEV_STATS_PATH"] == str(Run.path / "docker-stats.jsonl")
    assert "RUCIO_DEV_PROFILE" not in captured["environment"]
    assert captured["xdist_workers"] == 0


def test_debug_test_disables_xdist_without_case_workers(
    tmp_path: "Path",
    monkeypatch,
) -> None:
    captured = {}

    def run_test(*args, **kwargs) -> int:
        captured["run_test_args"] = args
        captured.update(kwargs)
        return 0

    monkeypatch.setattr(cli, "run_test", run_test)
    monkeypatch.setattr(cli.secrets, "token_hex", lambda length: "0123abcd")

    assert (
        cli.debug_test(
            tmp_path,
            (
                "--python",
                "3.13",
                "--rdbms",
                "postgres14",
                "remote_dbs",
                "--",
                "tests/test_ping.py::test_rucio_ping_rest",
            ),
        )
        == 0
    )

    assert captured["run_test_args"][1:] == (
        "remote_dbs",
        ["tests/test_ping.py::test_rucio_ping_rest"],
    )
    assert captured["xdist_workers"] == 0
    assert captured.get("case_workers") is None
    assert captured["python_version"] == "3.13"
    assert captured["rdbms"] == "postgres14"
    assert captured["policy"] is None
    assert captured["debug"] is True
    assert captured["debug_project"] == "rucio-test-remote-dbs-py313-postgres14-0123abcd"
    assert captured["environment"]["RUCIO_DEV_DEBUG"] == "1"


def test_debug_test_preserves_the_default_canonical_case(
    tmp_path: "Path",
    monkeypatch,
) -> None:
    captured = {}

    def run_test(*args, **kwargs) -> int:
        captured["args"] = args
        captured.update(kwargs)
        return 0

    monkeypatch.setattr(cli, "run_test", run_test)
    monkeypatch.setattr(cli.secrets, "token_hex", lambda length: "0123abcd")

    assert cli.debug_test(tmp_path, ()) == 0
    assert captured["args"][1:] == (cli.DEFAULT_CASE, [])
    assert captured["python_version"] is None
    assert captured["rdbms"] is None
    assert captured["policy"] is None
    assert captured["debug_project"] == "rucio-test-remote-dbs-py310-postgres14-0123abcd"


@pytest.mark.parametrize(
    ("mode", "arguments", "expected_count"),
    (
        ("debug", ("--python", "3.13", "remote_dbs"), 2),
        (
            "profile",
            ("--python", "3.13", "--rdbms", "postgres14", "votest"),
            2,
        ),
    ),
)
def test_instrumented_test_selection_requires_exactly_one_effective_case(
    tmp_path: "Path",
    mode: str,
    arguments: tuple[str, ...],
    expected_count: int,
) -> None:
    with pytest.raises(ValueError, match=rf"resolved to {expected_count} cases"):
        if mode == "debug":
            cli.debug_test(tmp_path, arguments)
        else:
            cli.profile_test(tmp_path, "cpu", arguments)


@pytest.mark.parametrize("executable", ("python", "python3", "python3.10", "/opt/venv/bin/python3.12"))
def test_debug_and_profile_commands_recognize_python_interpreters(executable: str) -> None:
    debugged = cli.debug_command((executable, "-m", "example.module", "--", "--literal"))
    profiled = cli.profile_command(
        "memory",
        (executable, "example.py", "--", "--literal"),
        "/artifacts",
    )

    assert debugged[-4:] == ["-m", "example.module", "--", "--literal"]
    assert profiled[-3:] == ["example.py", "--", "--literal"]


def test_cpu_profile_command_optionally_includes_idle_threads() -> None:
    command = ("python", "example.py")

    regular = cli.profile_command("cpu", command, "/artifacts")
    idle = cli.profile_command("cpu", command, "/artifacts", idle=True)

    assert "--idle" not in regular
    assert idle[idle.index("--subprocesses") + 1] == "--idle"

    with pytest.raises(ValueError, match="only supported for CPU"):
        cli.profile_command("memory", command, "/artifacts", idle=True)


def test_call_profile_preserves_original_python_command() -> None:
    command = ("python", "-bb", "-m", "example.module", "--argument")

    assert cli.profile_command("calls", command, "/artifacts") == [
        "python",
        "-m",
        "tools.devenv.calls",
        "--output",
        "/artifacts/calls.pstats",
        "--",
        *command,
    ]


def test_memory_profile_reports_name_parent_and_fork_captures(tmp_path: "Path") -> None:
    for name in ("memory.bin", "memory.bin.41", "memory.bin.7", "memory.bin.worker"):
        (tmp_path / name).write_text("capture")
    (tmp_path / "memory.bin.9").mkdir()

    assert cli._memory_profile_reports(tmp_path) == [
        ("memory.bin", "memory.html"),
        ("memory.bin.7", "memory-fork-7.html"),
        ("memory.bin.41", "memory-fork-41.html"),
    ]


def test_workspace_command_removes_only_toolbox_separator(tmp_path: "Path", monkeypatch) -> None:
    captured = {}

    class Project:
        project_name = "rucio-dev"
        image = "runtime:test"
        platform = "linux/arm64"
        environment = {}

        def __init__(self, root, **kwargs):
            captured["project"] = kwargs

        def up(self):
            return None

    monkeypatch.setattr(cli, "ComposeProject", Project)
    monkeypatch.setattr(
        cli,
        "_run_compose_exec",
        lambda project, root, service, command, environment: captured.update(
            service=service,
            command=tuple(command),
            environment=environment,
        ) or 0,
    )

    assert cli.workspace_command(
        tmp_path,
        "command",
        ("--", "python3", "example.py", "--", "--literal"),
    ) == 0

    assert captured["command"] == ("python3", "example.py", "--", "--literal")
    assert captured["service"] == "workspace"
    assert captured["environment"] == {}


def test_webui_command_reuses_checkout_and_supervises_both_processes(tmp_path: "Path", monkeypatch) -> None:
    webui_root = tmp_path / "webui"
    projects = []
    commands = []
    stopped = []
    readiness = []

    class Project:
        environment = {"RUCIO_DEV_IMAGE": "runtime:test"}

        def __init__(self, root, **kwargs):
            self.options = kwargs
            projects.append(self)

        @staticmethod
        def up() -> None:
            return None

        def exec(self, *arguments, **kwargs):
            commands.append(("exec", arguments, kwargs))

        @staticmethod
        def command(*arguments):
            return ["compose", *arguments]

        def recreate_workspace(self) -> None:
            self.restored = True

    class Process:
        def __init__(self, command):
            self.command = tuple(command)

    def popen(command, **kwargs):
        process = Process(command)
        commands.append(("popen", process, kwargs))
        return process

    monkeypatch.setattr(cli, "checkout_root", lambda path: webui_root)
    monkeypatch.setattr(cli.shutil, "which", lambda name: f"/usr/bin/{name}")
    monkeypatch.setattr(cli, "workspace_lock", lambda root: nullcontext())
    monkeypatch.setattr(cli, "_ensure_port_available", lambda port, service: None)
    monkeypatch.setattr(
        cli,
        "ensure_dependencies",
        lambda root, node, npm: commands.append(("dependencies", root, node, npm)),
    )
    monkeypatch.setattr(cli, "ComposeProject", Project)
    monkeypatch.setattr(cli.subprocess, "Popen", popen)
    monkeypatch.setattr(cli, "_wait_for_http", lambda process, url, service: readiness.append((process, url, service)))
    monkeypatch.setattr(cli, "_wait_for_webui_processes", lambda processes: 7)
    monkeypatch.setattr(cli, "_stop_process", lambda process: stopped.append(process))
    monkeypatch.setattr(cli.secrets, "token_hex", lambda size: "development-secret")

    assert cli.webui_command(tmp_path, Path("../existing-webui-worktree")) == 7

    assert [project.options for project in projects] == [
        {"api": True},
        {
            "build": False,
            "environment": Project.environment,
        },
    ]
    assert commands[0] == ("dependencies", webui_root, "/usr/bin/node", "/usr/bin/npm")
    assert commands[1] == (
        "exec",
        ("workspace", "python", "-m", "tools.devenv.webui", "seed"),
        {"timeout": 60},
    )
    backend = commands[2][1]
    frontend = commands[3][1]
    assert backend.command[:4] == ("compose", "exec", "--no-TTY", "workspace")
    assert frontend.command == (
        "/usr/bin/npm",
        "run",
        "dev",
        "--",
        "--hostname",
        "127.0.0.1",
        "--port",
        "3000",
    )
    assert commands[3][2]["cwd"] == webui_root
    assert commands[3][2]["env"]["RUCIO_HOST"] == "http://127.0.0.1:8080"
    assert commands[3][2]["env"]["NEXTAUTH_SECRET"] == "development-secret"
    assert readiness == [
        (backend, "http://127.0.0.1:8080/ping", "Rucio API"),
        (frontend, "http://127.0.0.1:3000/auth/login", "Rucio WebUI"),
    ]
    assert stopped == [frontend, backend]
    assert projects[-1].restored is True


def test_workspace_lock_rejects_concurrent_commands_and_releases(tmp_path: "Path") -> None:
    with workspace_lock(tmp_path):
        assert stat.S_IMODE((tmp_path / ".rucio-dev").stat().st_mode) == 0o700
        assert stat.S_IMODE((tmp_path / ".rucio-dev/workspace.lock").stat().st_mode) == 0o600
        with pytest.raises(RuntimeError, match=r"Rucio workspace is busy \(PID \d+\)"):
            with workspace_lock(tmp_path):
                pass

    with workspace_lock(tmp_path):
        pass


@pytest.mark.parametrize("link_kind", ("symlink", "hardlink"))
def test_workspace_lock_rejects_links_without_truncating_target(tmp_path: "Path", link_kind: str) -> None:
    lock_dir = tmp_path / ".rucio-dev"
    lock_dir.mkdir()
    target = tmp_path / "target"
    target.write_text("preserve me")
    lock_path = lock_dir / "workspace.lock"
    if link_kind == "symlink":
        lock_path.symlink_to(target)
    else:
        os.link(target, lock_path)

    with pytest.raises(RuntimeError, match="Rucio workspace lock"):
        with workspace_lock(tmp_path):
            pass

    assert target.read_text() == "preserve me"


def test_observed_server_has_explicit_root_span(tmp_path: "Path", monkeypatch) -> None:
    captured = {"projects": [], "restored": 0}

    class Project:
        project_name = "rucio-dev"
        image = "runtime:test"
        platform = "linux/arm64"
        environment = {}

        def __init__(self, root, **kwargs):
            captured["projects"].append(kwargs)

        def up(self):
            return None

        def recreate_workspace(self):
            captured["restored"] += 1

    monkeypatch.setattr(cli, "ComposeProject", Project)
    monkeypatch.setattr(
        cli,
        "_run_compose_exec",
        lambda project, root, service, command, environment: captured.update(
            service=service,
            command=tuple(command),
        ) or 0,
    )

    assert cli.workspace_command(tmp_path, "server", (), mode="observe") == 0

    command = captured["command"]
    assert command[:3] == ("opentelemetry-instrument", "--service_name", "rucio-server")
    assert command[3:10] == (
        "python",
        "-m",
        "tools.devenv.observe",
        "--span-name",
        "rucio-server.run",
        "--",
        "python",
    )
    assert captured["projects"] == [
        {
            "profiles": ("observe",),
            "debug": False,
            "api": True,
            "instrumentation": False,
            "observe": True,
        },
        {
            "profiles": ("observe",),
            "build": False,
            "environment": Project.environment,
        },
    ]
    assert captured["restored"] == 1
    assert captured["service"] == "workspace"


@pytest.mark.parametrize(("kind", "instrumentation"), (("cpu", True), ("memory", False), ("calls", False), ("resources", False)))
def test_only_cpu_workspace_profile_requests_ptrace(
    tmp_path: "Path",
    monkeypatch,
    kind: str,
    instrumentation: bool,
) -> None:
    captured = {"projects": [], "restored": []}

    class Project:
        project_name = "rucio-dev"
        image = "runtime:test"
        platform = "linux/arm64"
        environment = {}

        def __init__(self, root, **kwargs):
            captured["projects"].append(kwargs)

        def up(self):
            return None

        def exec(self, service, *command, **kwargs):
            captured["command"] = command
            captured["exec"] = kwargs
            return subprocess.CompletedProcess(command, 0, stdout="")

        def recreate_workspace(self):
            captured["restored"].append(self)

    class Run:
        path = tmp_path / "run"
        container_path = "/rucio_source/.rucio-dev/artifacts/run"
        container_environment = {
            "RUCIO_DEV_ARTIFACT_DIR": container_path,
            "RUCIO_DEV_HOST_UID": "1234",
            "RUCIO_DEV_HOST_GID": "5678",
        }

        def __init__(self, *args, **kwargs):
            self.path.mkdir(exist_ok=True)

        def finish(self, returncode):
            return None

    monkeypatch.setattr(cli, "ComposeProject", Project)
    monkeypatch.setattr(cli, "ArtifactRun", Run)
    monkeypatch.setattr(cli, "DockerStatsSampler", lambda *args: nullcontext())

    def run_compose_exec(project, root, service, command, environment):
        captured["service"] = service
        captured["command"] = tuple(command)
        captured["exec"] = {"environment": environment}
        return 0

    monkeypatch.setattr(cli, "_run_compose_exec", run_compose_exec)

    assert cli.workspace_command(
        tmp_path,
        "command",
        ("python", "example.py"),
        mode="profile",
        profile_kind=kind,
        profile_idle=instrumentation,
    ) == 0

    assert captured["projects"][0]["instrumentation"] is instrumentation
    if instrumentation:
        assert captured["projects"][1] == {
            "profiles": (),
            "build": False,
            "environment": Project.environment,
        }
        assert len(captured["restored"]) == 1
        assert "--idle" in captured["command"]
    else:
        assert len(captured["projects"]) == 1
        assert captured["restored"] == []
        assert "--idle" not in captured["command"]
    assert captured["service"] == "workspace"
    assert captured["command"][:3] == ("sh", "-c", artifacts._ARTIFACT_OWNERSHIP_SCRIPT)
    assert captured["command"][3] == "rucio-artifact-owner"
    assert captured["exec"]["environment"] == Run.container_environment


def test_workspace_memory_profile_renders_and_indexes_every_fork_capture(
    tmp_path: "Path",
    monkeypatch,
) -> None:
    commands = []
    runs = []

    class Project:
        project_name = "rucio-dev"
        image = "runtime:test"
        platform = "linux/arm64"
        environment = {}

        @staticmethod
        def up() -> None:
            return None

    class Run(artifacts.ArtifactRun):
        def __init__(self, *args, **kwargs) -> None:
            super().__init__(*args, **kwargs)
            runs.append(self)
            for name in ("memory.bin", "memory.bin.29", "memory.bin.5"):
                (self.path / name).write_text("capture")

    def run_compose_exec(project, root, service, command, environment):
        actual = tuple(command[4:])
        commands.append(actual)
        if actual[:4] == ("python", "-m", "memray", "flamegraph"):
            output = actual[actual.index("--output") + 1]
            (runs[0].path / Path(output).name).write_text("report")
        return 0

    monkeypatch.setattr(cli, "ArtifactRun", Run)
    monkeypatch.setattr(cli, "DockerStatsSampler", lambda *args: nullcontext())
    monkeypatch.setattr(cli, "_run_compose_exec", run_compose_exec)

    assert cli._run_workspace_command(
        Project(),
        tmp_path,
        "command",
        ("python", "example.py"),
        ["python", "example.py"],
        "profile",
        "memory",
    ) == 0

    run = runs[0]
    assert commands[1:] == [
        (
            "python",
            "-m",
            "memray",
            "flamegraph",
            "--force",
            "--output",
            f"{run.container_path}/memory.html",
            f"{run.container_path}/memory.bin",
        ),
        (
            "python",
            "-m",
            "memray",
            "flamegraph",
            "--force",
            "--output",
            f"{run.container_path}/memory-fork-5.html",
            f"{run.container_path}/memory.bin.5",
        ),
        (
            "python",
            "-m",
            "memray",
            "flamegraph",
            "--force",
            "--output",
            f"{run.container_path}/memory-fork-29.html",
            f"{run.container_path}/memory.bin.29",
        ),
    ]
    manifest = run.manifest
    assert manifest["returncode"] == 0
    assert {"memory.html", "memory-fork-5.html", "memory-fork-29.html"} <= set(manifest["artifacts"])
    index = (run.path / "index.html").read_text()
    assert all(report in index for report in ("memory.html", "memory-fork-5.html", "memory-fork-29.html"))


@pytest.mark.parametrize(
    ("results", "expected"),
    (
        ((6, 7, 8), 6),
        ((0, 7, 8), 7),
        ((0, 0, 8), 8),
    ),
)
def test_workspace_memory_profile_preserves_target_and_render_failures(
    tmp_path: "Path",
    monkeypatch,
    results: tuple[int, ...],
    expected: int,
) -> None:
    finished = []
    commands = []

    class Project:
        project_name = "rucio-dev"
        image = "runtime:test"
        platform = "linux/arm64"
        environment = {}

        @staticmethod
        def up() -> None:
            return None

    class Run:
        path = tmp_path / "run"
        container_path = "/rucio_source/.rucio-dev/artifacts/run"
        container_environment = {"RUCIO_DEV_ARTIFACT_DIR": container_path}

        def __init__(self, *args, **kwargs) -> None:
            self.path.mkdir()
            for name in ("memory.bin", "memory.bin.11"):
                (self.path / name).write_text("capture")

        @staticmethod
        def finish(returncode: int) -> None:
            finished.append(returncode)

    outcomes = iter(results)

    def run_compose_exec(project, root, service, command, environment):
        commands.append(tuple(command))
        return next(outcomes)

    monkeypatch.setattr(cli, "ArtifactRun", Run)
    monkeypatch.setattr(cli, "DockerStatsSampler", lambda *args: nullcontext())
    monkeypatch.setattr(cli, "_run_compose_exec", run_compose_exec)

    assert cli._run_workspace_command(
        Project(),
        tmp_path,
        "command",
        ("python", "example.py"),
        ["python", "example.py"],
        "profile",
        "memory",
    ) == expected

    assert len(commands) == 3
    assert finished == [expected]


@pytest.mark.parametrize(
    ("target", "arguments", "mode", "profile_kind", "profiles"),
    (
        ("command", ("python", "example.py"), "debug", "", ()),
        ("server", (), "run", "", ()),
        ("server", (), "observe", "", ("observe",)),
        ("daemon", ("hermes", "--run-once"), "observe", "", ("messaging", "observe")),
        ("command", ("python", "example.py"), "profile", "cpu", ()),
    ),
)
@pytest.mark.parametrize(
    "outcome",
    (7, RuntimeError("target failed"), KeyboardInterrupt()),
    ids=("nonzero", "exception", "interrupt"),
)
def test_workspace_overlays_restore_base_workspace_after_target_failure(
    tmp_path: "Path",
    monkeypatch,
    target: str,
    arguments: tuple[str, ...],
    mode: str,
    profile_kind: str,
    profiles: tuple[str, ...],
    outcome,
) -> None:
    projects = []

    class Project:
        environment = {"RUCIO_DEV_IMAGE": "runtime:test"}

        def __init__(self, root, **kwargs):
            self.options = kwargs
            projects.append(self)

        def recreate_workspace(self):
            self.restored = True

    def run_workspace_command(*args, **kwargs):
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    monkeypatch.setattr(cli, "ComposeProject", Project)
    monkeypatch.setattr(cli, "_run_workspace_command", run_workspace_command)

    if isinstance(outcome, BaseException):
        with pytest.raises(type(outcome), match="target failed" if isinstance(outcome, RuntimeError) else None):
            cli.workspace_command(
                tmp_path,
                target,
                arguments,
                mode=mode,
                profile_kind=profile_kind,
            )
    else:
        assert cli.workspace_command(
            tmp_path,
            target,
            arguments,
            mode=mode,
            profile_kind=profile_kind,
        ) == outcome

    assert len(projects) == 2
    assert projects[0].options == {
        "profiles": profiles,
        "debug": mode == "debug",
        "api": target == "server",
        "instrumentation": mode == "profile" and profile_kind == "cpu",
        "observe": mode == "observe",
    }
    assert projects[1].options == {
        "profiles": profiles,
        "build": False,
        "environment": Project.environment,
    }
    assert projects[1].restored is True


@pytest.mark.parametrize(
    "outcome",
    (0, ValueError("command failed")),
    ids=("success", "failure"),
)
def test_workspace_restore_failure_preserves_command_failure(
    tmp_path: "Path",
    monkeypatch,
    capsys,
    outcome,
) -> None:
    class Project:
        environment = {}

        def __init__(self, root, **kwargs):
            return None

        def recreate_workspace(self) -> None:
            raise RuntimeError("restore failed")

    def run_workspace_command(*args, **kwargs):
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    monkeypatch.setattr(cli, "ComposeProject", Project)
    monkeypatch.setattr(cli, "_run_workspace_command", run_workspace_command)

    if isinstance(outcome, BaseException):
        with pytest.raises(ValueError, match="command failed"):
            cli.workspace_command(tmp_path, "command", ("python", "example.py"), mode="debug")
        assert "Workspace restoration also failed: restore failed" in capsys.readouterr().err
    else:
        with pytest.raises(RuntimeError, match="restore failed"):
            cli.workspace_command(tmp_path, "command", ("python", "example.py"), mode="debug")


@pytest.mark.parametrize(
    ("daemon", "expected"),
    (
        ("rucio-conveyor-submitter", ("storage",)),
        ("rucio-conveyor-receiver", ("messaging",)),
        ("rucio-conveyor-finisher", ()),
        ("rucio-reaper", ("storage",)),
        ("rucio-hermes", ("messaging",)),
        ("rucio-kronos", ("messaging",)),
        ("rucio-cache-consumer", ("messaging",)),
        ("rucio-minos", ()),
        ("rucio-automatix", ("storage",)),
        ("dark-reaper", ("storage",)),
        ("rucio-auditor", ("storage",)),
        ("rucio-storage-consistency-actions", ("storage",)),
        ("rucio-judge-evaluator", ()),
        ("rucio-meta-conventions", ()),
    ),
)
def test_profiles_for_daemon(daemon: str, expected: tuple[str, ...]) -> None:
    assert profiles_for_daemon(daemon) == expected


def test_daemon_profile_map_covers_every_shipped_daemon() -> None:
    daemon_commands = {path.name for path in (Path(__file__).parents[2] / "bin").glob("rucio-*")} - {"rucio-admin", "rucio-cache-client"}

    assert set(DAEMON_PROFILES) == daemon_commands


def test_compose_project_sanitizes_host_platform_and_uses_narrow_overlays(
    tmp_path: "Path",
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "tools.devenv.compose.ensure_runtime_image",
        lambda root, environment: ("runtime:test", "fingerprint", "linux/arm64"),
    )

    project = ComposeProject(
        tmp_path,
        debug=True,
        api=True,
        instrumentation=True,
        environment={
            "COMPOSE_PROFILES": "unexpected",
            "DOCKER_DEFAULT_PLATFORM": "linux/amd64",
        },
    )

    assert "COMPOSE_PROFILES" not in project.environment
    assert "DOCKER_DEFAULT_PLATFORM" not in project.environment
    assert [path.name for path in project.files] == [
        "docker-compose.yml",
        "docker-compose.instrumentation.yml",
        "docker-compose.debug.yml",
        "docker-compose.api.yml",
    ]
    command = project.command("ps")
    assert command[command.index("--profile") + 1] == "workspace"


def test_compose_project_preserves_initialized_development_database(
    tmp_path: "Path",
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "tools.devenv.compose.ensure_runtime_image",
        lambda root, environment: ("runtime:test", "fingerprint", "linux/arm64"),
    )
    project = ComposeProject(tmp_path, environment={})
    commands = []
    monkeypatch.setattr(project, "pull_dependencies", lambda: None)
    monkeypatch.setattr(project, "run", lambda *args, **kwargs: commands.append(args))
    monkeypatch.setattr(project, "exec", lambda *args, **kwargs: commands.append(args))

    project.up()

    infra_commands = [command for command in commands if "tests.ruciopytest.infra_manager" in command]
    install = next(command for command in commands if "install" in command)
    assert install[0] == "workspace"
    assert infra_commands == [
        (
            "rucio",
            "python",
            "-m",
            "tests.ruciopytest.infra_manager",
            "--case",
            "remote-dbs-py310-postgres14",
            "--keep-db",
        ),
        (
            "workspace",
            "python",
            "-m",
            "tests.ruciopytest.infra_manager",
            "--case",
            "remote-dbs-py310-postgres14",
            "--runtime-only",
        ),
    ]


def test_compose_project_refreshes_dependencies_without_pulling_runtime(
    tmp_path: "Path",
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "tools.devenv.compose.ensure_runtime_image",
        lambda root, environment: ("runtime:test", "fingerprint", "linux/arm64"),
    )
    project = ComposeProject(tmp_path, environment={})
    commands = []
    exec_commands = []

    def run(*args, **kwargs):
        commands.append((args, kwargs))
        stdout = "rucio\nworkspace\nruciodb\n" if args == ("config", "--services") else ""
        return subprocess.CompletedProcess(args, 0, stdout=stdout)

    monkeypatch.setattr(project, "run", run)
    monkeypatch.setattr(project, "exec", lambda *args, **kwargs: exec_commands.append(args))

    project.up(initialize=False)

    assert commands[0] == (
        ("config", "--services"),
        {"capture_output": True, "timeout": 30},
    )
    assert commands[1] == (
        ("pull", "--policy", "always", "ruciodb"),
        {"timeout": 900},
    )
    assert commands[2][0][:2] == ("up", "--detach")
    assert not any(command[0] == "rucio" for command in exec_commands)
    assert exec_commands[-1][-1] == "--runtime-only"


def test_compose_project_recreates_only_base_workspace(tmp_path: "Path", monkeypatch) -> None:
    project = ComposeProject(
        tmp_path,
        profiles=("storage",),
        build=False,
        environment={"RUCIO_DEV_IMAGE": "runtime:test"},
    )
    run_commands = []
    exec_commands = []
    monkeypatch.setattr(project, "run", lambda *args, **kwargs: run_commands.append((args, kwargs)))
    monkeypatch.setattr(project, "exec", lambda *args, **kwargs: exec_commands.append((args, kwargs)))

    project.recreate_workspace()

    assert run_commands == [
        (
            (
                "up",
                "--detach",
                "--wait",
                "--wait-timeout",
                "180",
                "--no-deps",
                "--force-recreate",
                "workspace",
            ),
            {"timeout": 240},
        )
    ]
    assert not any(command[0][0] == "rucio" for command in exec_commands)
    assert exec_commands[-1][0] == (
        "workspace",
        "python",
        "-m",
        "tests.ruciopytest.infra_manager",
        "--case",
        "integration-py39-postgres14",
        "--runtime-only",
    )


def test_reset_includes_every_optional_service_and_volume_model(tmp_path: "Path", monkeypatch) -> None:
    captured = {}

    class Project:
        def __init__(self, root, **kwargs):
            captured["options"] = kwargs

        def run(self, *command, **kwargs):
            captured["command"] = command
            return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(cli, "repository_root", lambda: tmp_path)
    monkeypatch.setattr(cli, "ComposeProject", Project)

    assert cli.main(["reset"]) == 0

    assert captured["options"] == {
        "profiles": ("storage", "messaging", "externalmetadata", "iam", "observe"),
        "observe": True,
        "build": False,
    }
    assert captured["command"] == ("down", "--remove-orphans", "--volumes")


@pytest.mark.parametrize(
    ("child_status", "chown_status", "expected"),
    ((7, 0, 7), (0, 9, 9)),
)
def test_artifact_owner_cleanup_preserves_command_failure_and_reports_chown_failure(
    tmp_path: "Path",
    child_status: int,
    chown_status: int,
    expected: int,
) -> None:
    chown = tmp_path / "chown"
    chown.write_text(
        '#!/bin/sh\nprintf "%s\\n" "$@" > "$CHOWN_LOG"\nexit "$CHOWN_STATUS"\n'
    )
    chown.chmod(0o700)
    log = tmp_path / "chown.log"
    environment = {
        **os.environ,
        "PATH": f"{tmp_path}:{os.environ['PATH']}",
        "CHILD_STATUS": str(child_status),
        "CHOWN_LOG": str(log),
        "CHOWN_STATUS": str(chown_status),
        "RUCIO_DEV_ARTIFACT_DIR": "/rucio_source/.rucio-dev/artifacts/run",
        "RUCIO_DEV_HOST_UID": "1234",
        "RUCIO_DEV_HOST_GID": "5678",
    }
    command = artifacts.artifact_ownership_command(
        ("sh", "-c", 'exit "$CHILD_STATUS"'),
        environment,
    )

    result = subprocess.run(command, check=False, env=environment)  # noqa: S603

    assert result.returncode == expected
    assert log.read_text().splitlines() == [
        "-R",
        "1234:5678",
        "--",
        "/rucio_source/.rucio-dev/artifacts/run",
    ]


@pytest.mark.parametrize(
    "environment",
    (
        {
            "RUCIO_DEV_ARTIFACT_DIR": "/",
            "RUCIO_DEV_HOST_UID": "1234",
            "RUCIO_DEV_HOST_GID": "5678",
        },
        {
            "RUCIO_DEV_ARTIFACT_DIR": "/rucio_source",
            "RUCIO_DEV_HOST_UID": "1234",
            "RUCIO_DEV_HOST_GID": "5678",
        },
        {
            "RUCIO_DEV_ARTIFACT_DIR": "/rucio_source/.rucio-dev/artifacts/run",
            "RUCIO_DEV_HOST_UID": "1234",
        },
    ),
)
def test_artifact_owner_rejects_partial_or_unsafe_handoff(environment) -> None:
    with pytest.raises(ValueError, match="Invalid artifact ownership environment"):
        artifacts.artifact_ownership_command(
            ("python", "example.py"),
            environment,
        )


def test_artifact_run_finalizes_manifest_and_index(
    tmp_path: "Path",
    monkeypatch,
) -> None:
    git_values = {
        ("rev-parse", "HEAD"): "deadbeef",
        ("branch", "--show-current"): "feature/toolbox",
        ("status", "--porcelain"): " M lib/rucio/example.py",
    }
    monkeypatch.setattr(artifacts, "_git", lambda root, *args: git_values[args])
    monkeypatch.setattr(artifacts.os, "getuid", lambda: 1234)
    monkeypatch.setattr(artifacts.os, "getgid", lambda: 5678)
    run = artifacts.ArtifactRun(
        tmp_path,
        name="CPU test",
        command=("profile", "cpu", "test"),
        image="runtime:test",
        platform="linux/amd64",
        project="rucio-test-case-0123abcd",
    )
    (run.path / "cpu.svg").write_text(
        """<svg><style>
#search { text-anchor:end; opacity:0.1; cursor:pointer; }
</style><script>
searchbtn.attributes.x.value = svgWidth - xpad;
</script><text id="search" fill="rgb(0,0,0)" x="1190" y="24.00">Search</text></svg>"""
    )
    (run.path / "docker-stats.jsonl").write_text(
        json.dumps(
            {
                "timestamp": "2026-07-16T12:00:00+00:00",
                "Name": "rucio-server",
                "CPUPerc": "12.5%",
                "MemUsage": "1.5MiB / 2GiB",
                "MemPerc": "0.07%",
                "NetIO": "1.25kB / 2MB",
                "BlockIO": "3KiB / 4.5MB",
                "PIDs": "7",
            }
        )
        + "\n"
    )
    nested = run.path / "logs"
    nested.mkdir()
    (nested / "pytest.log").write_text("passed")

    run.finish(0)

    manifest = json.loads((run.path / "manifest.json").read_text())
    assert manifest["returncode"] == 0
    assert manifest["git"] == {
        "revision": "deadbeef",
        "branch": "feature/toolbox",
        "dirty": True,
    }
    assert manifest["runtime"] == {
        "image": "runtime:test",
        "platform": "linux/amd64",
        "compose_project": "rucio-test-case-0123abcd",
    }
    assert manifest["artifacts"] == [
        "cpu.svg",
        "docker-stats.jsonl",
        "logs/pytest.log",
        "resources.html",
    ]
    assert "finished_at" in manifest
    assert manifest["duration_seconds"] >= 0
    assert not run.incomplete.exists()
    assert (run.path / "index.html").is_file()
    assert "cpu.svg" in (run.path / "index.html").read_text()
    cpu_profile = (run.path / "cpu.svg").read_text()
    assert '<rect id="search-background" x="1094" y="8" width="102" height="21" rx="3"/>' in cpu_profile
    assert "#search { opacity:1; font-weight:bold; }" in cpu_profile
    assert 'document.getElementById("search-background").attributes.x.value = svgWidth - xpad - 96;' in cpu_profile
    resources = (run.path / "resources.html").read_text()
    assert "rucio-server" in resources
    assert "Network I/O (cumulative)" in resources
    assert "<script src" not in resources
    assert "http://" not in resources
    assert "https://" not in resources
    assert run.container_path.startswith("/rucio_source/.rucio-dev/artifacts/")
    assert run.container_environment == {
        "RUCIO_DEV_ARTIFACT_DIR": run.container_path,
        "RUCIO_DEV_HOST_UID": "1234",
        "RUCIO_DEV_HOST_GID": "5678",
    }
    assert stat.S_IMODE(run.path.stat().st_mode) == 0o700
    assert stat.S_IMODE(nested.stat().st_mode) == 0o700
    for relative_path in (
        "cpu.svg",
        "docker-stats.jsonl",
        "index.html",
        "logs/pytest.log",
        "manifest.json",
        "resources.html",
    ):
        assert stat.S_IMODE((run.path / relative_path).stat().st_mode) == 0o600


def test_resource_samples_normalize_docker_stats(tmp_path: "Path") -> None:
    source = tmp_path / "docker-stats.jsonl"
    source.write_text(
        "not JSON\n"
        + json.dumps(
            {
                "timestamp": "2026-07-16T12:00:00Z",
                "Name": "rucio-server",
                "CPUPerc": "12.5%",
                "MemUsage": "1.5MiB / 2GiB",
                "MemPerc": "0.07%",
                "NetIO": "1.25kB / 2MB",
                "BlockIO": "3KiB / 4.5MB",
                "PIDs": "7",
            }
        )
        + "\n"
    )

    assert artifacts._load_resource_samples(source) == [
        {
            "timestamp": "2026-07-16T12:00:00Z",
            "time_ms": 1784203200000,
            "container": "rucio-server",
            "cpu_percent": 12.5,
            "memory_bytes": 1572864,
            "memory_percent": 0.07,
            "network_rx_bytes": 1250,
            "network_tx_bytes": 2000000,
            "block_read_bytes": 3072,
            "block_write_bytes": 4500000,
            "pids": 7,
        }
    ]
    assert artifacts._parse_bytes("1iB") is None


def test_resource_report_escapes_embedded_json() -> None:
    report = artifacts._render_resource_report(
        [
            {
                "container": "</script><script>alert(1)</script>",
            }
        ]
    )

    assert "</script><script>alert(1)</script>" not in report
    assert "\\u003c/script\\u003e" in report


def test_docker_stats_sampler_samples_before_enter_returns(
    tmp_path: "Path",
    monkeypatch,
) -> None:
    sampler = artifacts.DockerStatsSampler(tmp_path, "rucio-dev", tmp_path / "stats.jsonl")
    events = []
    monkeypatch.setattr(sampler, "_sample_once", lambda: events.append("sample"))

    with sampler:
        assert events == ["sample"]

    assert events == ["sample"]


def test_docker_stats_sampler_writes_private_sample(
    tmp_path: "Path",
    monkeypatch,
) -> None:
    calls = []

    def run(command, **kwargs):
        calls.append((tuple(command), kwargs))
        stdout = (
            "container-id\n"
            if command[1] == "ps"
            else json.dumps(
                {
                    "Name": "rucio-server",
                    "CPUPerc": "1.2%",
                }
            )
        )
        return subprocess.CompletedProcess(command, 0, stdout=stdout, stderr="")

    monkeypatch.setattr(artifacts, "_executable", lambda name: name)
    monkeypatch.setattr(artifacts.subprocess, "run", run)
    output = tmp_path / "artifacts/docker-stats.jsonl"
    sampler = artifacts.DockerStatsSampler(tmp_path, "rucio-dev", output)

    sampler._sample_once()

    record = json.loads(output.read_text())
    assert record["Name"] == "rucio-server"
    assert record["timestamp"].endswith("+00:00")
    assert stat.S_IMODE(output.stat().st_mode) == 0o600
    assert [call[0][1] for call in calls] == ["ps", "stats"]
    assert all(call[1]["timeout"] == 5 for call in calls)


def test_latest_report_selects_newest_supported_report(
    tmp_path: "Path",
    capsys,
) -> None:
    development = tmp_path / ".rucio-dev/artifacts/run/index.html"
    performance = tmp_path / ".autotest/performance/run/report.html"
    development.parent.mkdir(parents=True)
    performance.parent.mkdir(parents=True)
    development.write_text("development")
    performance.write_text("performance")
    os.utime(development, (10, 10))
    os.utime(performance, (20, 20))

    assert cli.latest_report(tmp_path) == 0

    assert capsys.readouterr().out.strip() == performance.resolve().as_uri()


def test_latest_report_handles_empty_repository(
    tmp_path: "Path",
    capsys,
) -> None:
    assert cli.latest_report(tmp_path) == 1
    assert "No development reports found." in capsys.readouterr().err


@pytest.mark.parametrize(
    ("returncodes", "compose_result", "expected"),
    (
        ((0, 0, 0), (True, "2.22.0"), 0),
        ((0, 0, 0), (False, "2.21.0; requires 2.22.0 or newer"), 1),
    ),
)
def test_public_doctor_reports_prerequisite_results(
    tmp_path: "Path",
    monkeypatch,
    capsys,
    returncodes: tuple[int, ...],
    compose_result: tuple[bool, str],
    expected: int,
) -> None:
    commands = []
    results = iter(returncodes)

    def run(command, **kwargs):
        commands.append(tuple(command))
        return subprocess.CompletedProcess(command, next(results), stdout="", stderr="")

    monkeypatch.setattr(cli, "repository_root", lambda: tmp_path)
    monkeypatch.setattr(cli.subprocess, "run", run)
    monkeypatch.setattr(cli, "check_docker_compose", lambda root: compose_result)
    monkeypatch.setattr(cli, "control_python", lambda root: root / ".venv/bin/python")
    monkeypatch.setattr(cli, "development_environment_is_unmanaged", lambda root: False)

    assert cli.main(["doctor"]) == expected

    output = capsys.readouterr().out
    assert commands == [
        ("docker", "info"),
        ("docker", "buildx", "version"),
        ("git", "rev-parse", "--show-toplevel"),
    ]
    assert f"Docker Compose     {'ok' if compose_result[0] else 'failed'} ({compose_result[1]})" in output
    assert ("Rucio development prerequisites are ready." in output) is (expected == 0)
    assert ("failed" in output) is (expected == 1)
    assert "IDE environment    ok" in output


def test_doctor_reports_and_preserves_unmanaged_environment(tmp_path: "Path", monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        cli.subprocess,
        "run",
        lambda command, **kwargs: subprocess.CompletedProcess(command, 0, stdout="", stderr=""),
    )
    monkeypatch.setattr(cli, "control_python", lambda root: root / ".rucio-dev/control-venv/bin/python")
    monkeypatch.setattr(cli, "development_environment_is_unmanaged", lambda root: True)
    monkeypatch.setattr(cli, "check_docker_compose", lambda root: (True, "2.22.0"))

    assert cli.doctor(tmp_path) == 0

    output = capsys.readouterr().out
    assert "IDE environment    preserved (unmanaged .venv)" in output
    assert "Control Python     ok" in output
