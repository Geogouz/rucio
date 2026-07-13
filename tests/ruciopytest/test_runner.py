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

import stat
import subprocess  # noqa: S404
from pathlib import Path

import pytest

from tests.ruciopytest import runner
from tests.ruciopytest.profiles import get_case, make_case

_PROFILE_ARTIFACT = "/rucio_source/.rucio-dev/artifacts/run"


def _write_unit_build_inputs(root: "Path") -> None:
    for relative in runner._UNIT_BUILD_INPUTS:
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f"fixture for {relative.as_posix()}\n")


class _Manager:
    results: list[int] = []
    commands: list[tuple[str, ...]] = []
    environments: list[dict[str, str]] = []
    stops: list[bool] = []
    artifact = "/fts/log/*__transfer-id"
    interactives: list[bool] = []

    def __init__(self, case, root_dir, keep_db=False, log_output=False):
        self.case = case
        self.root_dir = root_dir
        self.keep_db = keep_db
        self.log_output = log_output
        self.environment = {}
        self.project_name = "rucio-test-project"

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return None

    def start(self):
        return None

    def stop(self, *, check=True):
        self.stops.append(check)

    def exec(
        self,
        service,
        *arguments,
        environment=None,
        capture_output=False,
        **kwargs,
    ):
        self.commands.append((service, *arguments))
        self.environments.append(dict(environment or {}))
        self.interactives.append(kwargs.get("interactive", False))
        if arguments[:2] == ("cat", "/tmp/test_tpc.artifact"):
            return subprocess.CompletedProcess(
                arguments,
                int(self.artifact is None),
                stdout=f"{self.artifact}\n" if self.artifact else "",
            )
        result = self.results.pop(0) if self.results else 0
        return subprocess.CompletedProcess(arguments, result, stdout="")


def _reset_manager(monkeypatch, results=()):
    _Manager.results = list(results)
    _Manager.commands = []
    _Manager.environments = []
    _Manager.stops = []
    _Manager.artifact = "/fts/log/*__transfer-id"
    _Manager.interactives = []
    monkeypatch.setattr(runner, "ContainerManager", _Manager)


def _unwrapped_exec_command(command: tuple[str, ...]) -> tuple[str, ...]:
    assert command[1:3] == ("sh", "-c")
    assert 'chown -R "${RUCIO_DEV_HOST_UID}:${RUCIO_DEV_HOST_GID}"' in command[3]
    assert command[4] == "rucio-artifact-owner"
    return (command[0], *command[5:])


def _unwrapped_run_command(command: list[str]) -> tuple[str, list[str]]:
    entrypoint = command.index("--entrypoint")
    assert command[entrypoint + 1] == "sh"
    image = command[entrypoint + 2]
    assert command[entrypoint + 3] == "-c"
    assert 'chown -R "${RUCIO_DEV_HOST_UID}:${RUCIO_DEV_HOST_GID}"' in command[entrypoint + 4]
    assert command[entrypoint + 5] == "rucio-artifact-owner"
    return image, command[entrypoint + 6:]


def test_forwarded_args_remove_only_runner_options() -> None:
    assert runner.forwarded_pytest_args([
        "--suite=remote_dbs",
        "--case-workers=2",
        "--python",
        "3.13",
        "--rdbms=postgres14",
        "--keep-db",
        "-k",
        "rule",
        "tests/test_rule.py::test_add_rule",
        "--container-env",
        "DEBUG=1",
    ]) == [
        "-k",
        "rule",
        "tests/test_rule.py::test_add_rule",
        "-o",
        "addopts=",
    ]


@pytest.mark.parametrize(
    "config_args",
    (
        ("-c", "config.ini"),
        ("-cconfig.ini",),
        ("--config-file", "config.ini"),
        ("--config-file=config.ini",),
        ("--rootdir", "tests"),
        ("--rootdir=tests",),
    ),
)
def test_forwarded_args_remove_raw_config_options(config_args) -> None:
    assert runner.forwarded_pytest_args((*config_args, "-q")) == [
        "-q",
        "-o",
        "addopts=",
    ]


def test_forwarded_args_preserve_values_after_separator() -> None:
    assert runner.forwarded_pytest_args(("--", "-c", "literal.ini")) == [
        "-o",
        "addopts=",
        "--",
        "-c",
        "literal.ini",
    ]


def test_pytest_config_paths_are_mapped_into_container(tmp_path: "Path") -> None:
    source_root = tmp_path / "source"
    pytest_root = source_root / "tests"
    config_path = source_root / "config" / "pytest.ini"

    assert runner.container_pytest_config(
        ("-q", "--", "test-name"),
        source_root,
        pytest_root,
        config_path,
    ) == [
        "-q",
        "--rootdir=/rucio_source/tests",
        "-c",
        "/rucio_source/config/pytest.ini",
        "--",
        "test-name",
    ]


@pytest.mark.parametrize("external", ("root", "config"))
def test_pytest_config_paths_must_be_inside_checkout(
    tmp_path: "Path",
    external: str,
) -> None:
    source_root = tmp_path / "source"
    pytest_root = source_root / "tests"
    config_path = source_root / "pyproject.toml"
    if external == "root":
        pytest_root = tmp_path / "outside"
    else:
        config_path = tmp_path / "outside.ini"

    with pytest.raises(pytest.UsageError, match="must be inside"):
        runner.container_pytest_config(
            (),
            source_root,
            pytest_root,
            config_path,
        )


def test_pytest_configuration_file_is_required(tmp_path: "Path") -> None:
    with pytest.raises(pytest.UsageError, match="configuration file is required"):
        runner.container_pytest_config((), tmp_path, tmp_path, None)


def test_internal_symlinked_config_keeps_lexical_path(tmp_path: "Path") -> None:
    source_root = tmp_path / "source"
    config_directory = source_root / "config"
    config_directory.mkdir(parents=True)
    config_path = config_directory / "pytest.ini"
    config_path.write_text("[pytest]\n")
    config_link = source_root / "pytest.ini"
    config_link.symlink_to(config_path.relative_to(source_root))

    arguments = runner.container_pytest_config(
        (),
        source_root,
        source_root,
        config_link,
    )

    assert "/rucio_source/pytest.ini" in arguments


def test_config_symlink_cannot_escape_checkout(tmp_path: "Path") -> None:
    source_root = tmp_path / "source"
    source_root.mkdir()
    external_config = tmp_path / "pytest.ini"
    external_config.write_text("[pytest]\n")
    config_link = source_root / "pytest.ini"
    config_link.symlink_to(external_config)

    with pytest.raises(pytest.UsageError, match="must be inside"):
        runner.container_pytest_config(
            (),
            source_root,
            source_root,
            config_link,
        )


def test_outer_args_include_pytest_addopts(monkeypatch) -> None:
    monkeypatch.setenv("PYTEST_ADDOPTS", "-k 'rule creation' -q")

    assert runner.outer_pytest_args(
        ("--case=unit-py39", "-x"),
        ("--strict-markers",),
    ) == [
        "--strict-markers",
        "-k",
        "rule creation",
        "-q",
        "--case=unit-py39",
        "-x",
    ]


def test_addopts_override_precedes_pytest_separator() -> None:
    assert runner.forwarded_pytest_args(("-q", "--", "-literal")) == [
        "-q",
        "-o",
        "addopts=",
        "--",
        "-literal",
    ]


def test_container_resource_sampling_wraps_only_inner_test_lifetime(
    tmp_path: "Path",
    monkeypatch,
) -> None:
    _reset_manager(monkeypatch)
    output = tmp_path / ".rucio-dev/artifacts/run/docker-stats.jsonl"
    monkeypatch.setenv("RUCIO_DEV_STATS_PATH", str(output))
    events = []

    class Sampler:
        def __init__(self, root, project, path, *, interval_seconds):
            events.append(("create", root, project, path, interval_seconds))

        def __enter__(self):
            events.append(("enter",))
            return self

        def __exit__(self, *args):
            events.append(("exit",))

    monkeypatch.setattr(runner, "DockerStatsSampler", Sampler)

    assert runner.run_container_case(get_case("client-py39-postgres14"), tmp_path, ()) == 0

    assert events == [
        ("create", tmp_path, "rucio-test-project", output, 0.2),
        ("enter",),
        ("exit",),
    ]


def test_resource_sampling_path_must_be_in_artifact_root(
    tmp_path: "Path",
    monkeypatch,
) -> None:
    monkeypatch.setenv("RUCIO_DEV_STATS_PATH", str(tmp_path / "outside.jsonl"))

    with pytest.raises(pytest.UsageError, match="must be below"):
        runner._resource_sampler(tmp_path, "rucio-test-project")


def test_multi_vo_runs_both_legs_in_order(tmp_path: "Path", monkeypatch) -> None:
    _reset_manager(monkeypatch, (0, 0))

    result = runner.run_container_case(
        get_case("multi-vo-py39-postgres14"),
        tmp_path,
        (
            "--junitxml=results.xml",
            "--log-file=pytest.log",
            "--basetemp=tmp",
        ),
    )

    assert result == 0
    assert [environment["RUCIO_MULTI_VO_LEG"] for environment in _Manager.environments] == [
        "tst",
        "ts2",
    ]
    assert "--junitxml=results-tst.xml" in _Manager.commands[0]
    assert "--junitxml=results-ts2.xml" in _Manager.commands[1]
    assert "--log-file=pytest-tst.log" in _Manager.commands[0]
    assert "--log-file=pytest-ts2.log" in _Manager.commands[1]
    assert "--basetemp=tmp/tst" in _Manager.commands[0]
    assert "--basetemp=tmp/ts2" in _Manager.commands[1]
    assert (
        "cache_dir=/rucio_source/.pytest_cache/"
        "rucio-cases/multi-vo-py39-postgres14/tst"
    ) in _Manager.commands[0]
    assert (
        "cache_dir=/rucio_source/.pytest_cache/"
        "rucio-cases/multi-vo-py39-postgres14/ts2"
    ) in _Manager.commands[1]


def test_multi_vo_combines_coverage(tmp_path: "Path", monkeypatch) -> None:
    _reset_manager(monkeypatch)
    coverage_args = (
        "--cov=lib/rucio",
        "--cov-report=xml:test-results/coverage.xml",
        "--cov-fail-under=80",
    )

    result = runner.run_container_case(
        get_case("multi-vo-py39-postgres14"),
        tmp_path,
        coverage_args,
    )

    assert result == 0
    assert "--cov-append" not in _Manager.commands[0]
    assert "--cov-fail-under=0" in _Manager.commands[0]
    assert "--cov-append" in _Manager.commands[1]
    assert "--cov-fail-under=0" not in _Manager.commands[1]
    assert all(
        "--cov-report=xml:test-results/coverage.xml" in command
        for command in _Manager.commands
    )


def test_client_case_runs_only_its_test_paths(tmp_path: "Path", monkeypatch) -> None:
    _reset_manager(monkeypatch)
    case = get_case("client-py39-postgres14")

    result = runner.run_container_case(case, tmp_path, ())

    assert result == 0
    assert _Manager.commands[0][-len(case.test_paths):] == case.test_paths


def test_effective_case_axes_are_protected_inside_the_container(
    tmp_path: "Path",
    monkeypatch,
) -> None:
    _reset_manager(monkeypatch)
    case = make_case("remote_dbs", "3.13", rdbms="postgres14")

    assert runner.run_container_case(
        case,
        tmp_path,
        ("tests/test_ping.py",),
        container_environment={"RUCIO_TEST_PYTHON": "3.9"},
        explicit_selectors=("tests/test_ping.py",),
    ) == 0

    assert _Manager.environments[0].items() >= {
        "RUCIO_TEST_CASE": "remote-dbs-py313-postgres14",
        "RUCIO_TEST_SUITE": "remote_dbs",
        "RUCIO_TEST_PYTHON": "3.13",
        "RUCIO_TEST_RDBMS": "postgres14",
        "RUCIO_TEST_POLICY": "",
    }.items()


def test_explicit_selector_replaces_case_paths(tmp_path: "Path", monkeypatch) -> None:
    _reset_manager(monkeypatch)
    case = get_case("client-py39-postgres14")
    selector = "tests/test_clients.py::test_get_protocols"

    result = runner.run_container_case(
        case,
        tmp_path,
        (selector,),
        explicit_selectors=(selector,),
    )

    assert result == 0
    assert _Manager.commands[0].count(selector) == 1
    assert "tests/test_bin_rucio.py" not in _Manager.commands[0]


def test_container_command_keeps_options_before_separator(
    tmp_path: "Path",
    monkeypatch,
) -> None:
    _reset_manager(monkeypatch)
    selector = "tests/test_clients.py"
    arguments = runner.container_pytest_config(
        runner.forwarded_pytest_args(("--", selector)),
        tmp_path,
        tmp_path,
        tmp_path / "pyproject.toml",
    )

    result = runner.run_container_case(
        get_case("client-py39-postgres14"),
        tmp_path,
        arguments,
        explicit_selectors=(selector,),
    )

    assert result == 0
    command = _Manager.commands[0]
    marker = command.index("--")
    assert command[marker + 1:] == (selector,)
    assert command.index("-c") < marker
    assert next(
        index for index, value in enumerate(command) if value.startswith("cache_dir=")
    ) < marker


def test_multi_vo_stops_after_first_failure(tmp_path: "Path", monkeypatch) -> None:
    _reset_manager(monkeypatch, (1, 0))

    result = runner.run_container_case(
        get_case("multi-vo-py39-postgres14"),
        tmp_path,
        (),
    )

    assert result == 1
    assert len(_Manager.commands) == 1
    assert _Manager.stops == [False]


def test_integration_preserves_tpc_postcheck_order(tmp_path: "Path", monkeypatch) -> None:
    _reset_manager(monkeypatch)

    result = runner.run_container_case(
        get_case("integration-py39-postgres14"),
        tmp_path,
        ("--log-file=pytest.log", "--basetemp=tmp"),
    )

    assert result == 0
    pytest_commands = [command for command in _Manager.commands if "pytest" in command]
    cat_index = next(index for index, command in enumerate(_Manager.commands) if command[1:3] == ("cat", "/tmp/test_tpc.artifact"))
    verify_index = next(index for index, command in enumerate(_Manager.commands) if command[1:3] == ("bash", "-c"))
    assert len(pytest_commands) == 15
    assert "--log-file=pytest-01.log" in pytest_commands[0]
    assert "--log-file=pytest-02.log" in pytest_commands[1]
    assert "--basetemp=tmp/01" in pytest_commands[0]
    assert "--basetemp=tmp/02" in pytest_commands[1]
    assert _Manager.commands.index(pytest_commands[0]) < cat_index < verify_index
    assert _Manager.commands.index(pytest_commands[9]) < cat_index
    assert verify_index < _Manager.commands.index(pytest_commands[10])
    assert "RUCIO_SKIP_TEST_SETUP" not in _Manager.environments[0]
    assert all(
        environment["RUCIO_SKIP_TEST_SETUP"] == "1"
        for environment in _Manager.environments[1:]
        if environment
    )


def test_integration_continues_past_empty_filtered_paths(
    tmp_path: "Path",
    monkeypatch,
) -> None:
    _reset_manager(monkeypatch, (*([5] * 9), 0, 0, *([5] * 5)))

    result = runner.run_container_case(
        get_case("integration-py39-postgres14"),
        tmp_path,
        ("--deselect=tests/test_upload.py",),
    )

    assert result == 0
    assert len([command for command in _Manager.commands if "pytest" in command]) == 15
    assert any(command[1:3] == ("bash", "-c") for command in _Manager.commands)


def test_integration_rejects_an_empty_canonical_selector(
    tmp_path: "Path",
    monkeypatch,
) -> None:
    _reset_manager(monkeypatch, (pytest.ExitCode.NO_TESTS_COLLECTED,))

    result = runner.run_container_case(
        get_case("integration-py39-postgres14"),
        tmp_path,
        (),
    )

    assert result == pytest.ExitCode.NO_TESTS_COLLECTED
    assert len([command for command in _Manager.commands if "pytest" in command]) == 1


def test_integration_last_failed_uses_one_pytest_session(
    tmp_path: "Path",
    monkeypatch,
) -> None:
    _reset_manager(monkeypatch)
    _Manager.artifact = None
    case = get_case("integration-py39-postgres14")

    result = runner.run_container_case(case, tmp_path, ("--lf",))

    assert result == 0
    commands = [command for command in _Manager.commands if "pytest" in command]
    assert len(commands) == 1
    assert all(selector in commands[0] for selector in case.test_paths)


def test_integration_looponfail_uses_one_pytest_session(
    tmp_path: "Path",
    monkeypatch,
) -> None:
    _reset_manager(monkeypatch)
    case = get_case("integration-py39-postgres14")

    result = runner.run_container_case(case, tmp_path, ("--looponfail",))

    assert result == 0
    commands = [command for command in _Manager.commands if "pytest" in command]
    assert len(commands) == 1
    assert all(selector in commands[0] for selector in case.test_paths)


def test_integration_cache_clear_requires_tpc_artifact(
    tmp_path: "Path",
    monkeypatch,
) -> None:
    _reset_manager(monkeypatch)
    _Manager.artifact = None

    with pytest.raises(RuntimeError, match="did not export"):
        runner.run_container_case(
            get_case("integration-py39-postgres14"),
            tmp_path,
            ("--cache-clear",),
        )


def test_integration_cache_show_does_not_consume_test_path(
    tmp_path: "Path",
    monkeypatch,
) -> None:
    _reset_manager(monkeypatch)
    case = get_case("integration-py39-postgres14")

    result = runner.run_container_case(case, tmp_path, ("--cache-show",))

    assert result == 0
    commands = [command for command in _Manager.commands if "pytest" in command]
    assert len(commands) == 1
    assert all(selector not in commands[0] for selector in case.test_paths)


def test_integration_broad_selector_verifies_exported_tpc(
    tmp_path: "Path",
    monkeypatch,
) -> None:
    _reset_manager(monkeypatch)

    result = runner.run_container_case(
        get_case("integration-py39-postgres14"),
        tmp_path,
        ("tests/",),
        explicit_selectors=("tests/",),
    )

    assert result == 0
    assert "--export-artifacts-from=test_tpc" in _Manager.commands[0]
    verify = next(
        command for command in _Manager.commands
        if command[1:3] == ("bash", "-c")
    )
    assert verify[-1] == "/fts/log/*__transfer-id"


def test_integration_artifact_option_precedes_separator(
    tmp_path: "Path",
    monkeypatch,
) -> None:
    _reset_manager(monkeypatch)
    selector = "tests/test_tpc.py"

    result = runner.run_container_case(
        get_case("integration-py39-postgres14"),
        tmp_path,
        ("--", selector),
        explicit_selectors=(selector,),
    )

    assert result == 0
    command = _Manager.commands[0]
    marker = command.index("--")
    assert command.index("--export-artifacts-from=test_tpc") < marker
    assert command[marker + 1:] == (selector,)


def test_integration_collect_only_skips_missing_tpc_artifact(
    tmp_path: "Path",
    monkeypatch,
) -> None:
    _reset_manager(monkeypatch)
    _Manager.artifact = None

    result = runner.run_container_case(
        get_case("integration-py39-postgres14"),
        tmp_path,
        ("--collect-only", "tests/"),
        explicit_selectors=("tests/",),
    )

    assert result == 0
    assert not any(
        command[1:3] == ("bash", "-c")
        for command in _Manager.commands
    )


def test_integration_setup_only_skips_missing_tpc_artifact(
    tmp_path: "Path",
    monkeypatch,
) -> None:
    _reset_manager(monkeypatch)
    _Manager.artifact = None
    selector = "tests/test_tpc.py"

    result = runner.run_container_case(
        get_case("integration-py39-postgres14"),
        tmp_path,
        ("--setup-only", selector),
        explicit_selectors=(selector,),
    )

    assert result == 0
    assert not any(
        command[1:3] == ("bash", "-c")
        for command in _Manager.commands
    )


def test_integration_tpc_requires_exported_artifact(
    tmp_path: "Path",
    monkeypatch,
) -> None:
    _reset_manager(monkeypatch)
    _Manager.artifact = None
    selector = "tests/test_tpc.py"

    with pytest.raises(RuntimeError, match="did not export"):
        runner.run_container_case(
            get_case("integration-py39-postgres14"),
            tmp_path,
            (selector,),
            explicit_selectors=(selector,),
        )


def test_integration_filter_fails_when_nothing_matches(
    tmp_path: "Path",
    monkeypatch,
) -> None:
    _reset_manager(monkeypatch, [5] * 15)

    result = runner.run_container_case(
        get_case("integration-py39-postgres14"),
        tmp_path,
        ("-m", "missing"),
    )

    assert result == 5


def test_integration_combines_coverage(tmp_path: "Path", monkeypatch) -> None:
    _reset_manager(monkeypatch)

    result = runner.run_container_case(
        get_case("integration-py39-postgres14"),
        tmp_path,
        ("--cov=lib/rucio", "--cov-fail-under=80"),
    )

    assert result == 0
    commands = [command for command in _Manager.commands if "pytest" in command]
    assert "--cov-append" not in commands[0]
    assert all("--cov-append" in command for command in commands[1:])
    assert all("--cov-fail-under=0" in command for command in commands[:-1])
    assert "--cov-fail-under=0" not in commands[-1]


def test_postgres_uses_ci_worker_count(tmp_path: "Path", monkeypatch) -> None:
    _reset_manager(monkeypatch)
    manager = _Manager(get_case("remote-dbs-py39-postgres14"), tmp_path)
    manager.environment["GITHUB_ACTIONS"] = "true"

    runner._run_inner_pytest(
        manager,
        manager.case,
        (),
        keep_db=False,
    )

    assert "--numprocesses=3" in _Manager.commands[0]
    assert _Manager.commands[0].count("xdist") == 1


def test_user_xdist_setting_is_preserved(tmp_path: "Path", monkeypatch) -> None:
    _reset_manager(monkeypatch)
    manager = _Manager(get_case("remote-dbs-py39-postgres14"), tmp_path)

    runner._run_inner_pytest(
        manager,
        manager.case,
        ("-n", "1"),
        keep_db=False,
    )

    assert _Manager.commands[0].count("-n") == 1
    assert _Manager.commands[0].count("xdist") == 1
    assert "--numprocesses=auto" not in _Manager.commands[0]


@pytest.mark.parametrize(
    "arguments",
    (
        ("-n0",),
        ("--numprocesses=0",),
        ("--tx", "popen//python=python3"),
        ("--dist=no",),
        ("-f",),
    ),
)
def test_xdist_options_override_default_workers(
    tmp_path: "Path",
    monkeypatch,
    arguments,
) -> None:
    _reset_manager(monkeypatch)
    manager = _Manager(get_case("remote-dbs-py39-postgres14"), tmp_path)

    runner._run_inner_pytest(manager, manager.case, arguments, keep_db=False)

    assert "--numprocesses=auto" not in _Manager.commands[0]


@pytest.mark.parametrize(
    "arguments",
    ((), ("-d",), ("--dist=loadscope",), ("--maxprocesses=2",)),
)
def test_xdist_tuning_preserves_default_workers(
    tmp_path: "Path",
    monkeypatch,
    arguments,
) -> None:
    _reset_manager(monkeypatch)
    manager = _Manager(get_case("remote-dbs-py39-postgres14"), tmp_path)

    runner._run_inner_pytest(manager, manager.case, arguments, keep_db=False)

    assert "--numprocesses=auto" in _Manager.commands[0]


@pytest.mark.parametrize(
    "arguments",
    (("--dist=no", "-d"), ("-d", "--dist=no")),
)
def test_distload_preserves_default_workers(
    tmp_path: "Path",
    monkeypatch,
    arguments,
) -> None:
    _reset_manager(monkeypatch)
    manager = _Manager(get_case("remote-dbs-py39-postgres14"), tmp_path)

    runner._run_inner_pytest(
        manager,
        manager.case,
        arguments,
        keep_db=False,
    )

    assert "--numprocesses=auto" in _Manager.commands[0]


def test_xdist_transaction_is_preserved(tmp_path: "Path", monkeypatch) -> None:
    _reset_manager(monkeypatch)
    manager = _Manager(get_case("remote-dbs-py39-postgres14"), tmp_path)
    transaction = "popen//python=python3"

    runner._run_inner_pytest(
        manager,
        manager.case,
        ("--tx", transaction, "--dist=loadscope"),
        keep_db=False,
    )

    command = _Manager.commands[0]
    assert command[command.index("--tx") + 1] == transaction
    assert command.count("--tx") == 1
    assert "--numprocesses=auto" not in command


def test_serial_case_loads_xdist_parser(tmp_path: "Path", monkeypatch) -> None:
    _reset_manager(monkeypatch)
    manager = _Manager(get_case("remote-dbs-py39-oracle"), tmp_path)

    runner._run_inner_pytest(
        manager,
        manager.case,
        ("--maxprocesses=2",),
        keep_db=False,
    )

    assert _Manager.commands[0].count("xdist") == 1


@pytest.mark.parametrize("argument", ("-f", "--looponfail"))
def test_looponfail_loads_plugin_and_terminal(
    tmp_path: "Path",
    monkeypatch,
    argument: str,
) -> None:
    _reset_manager(monkeypatch)
    manager = _Manager(get_case("remote-dbs-py39-postgres14"), tmp_path)

    runner._run_inner_pytest(
        manager,
        manager.case,
        (argument,),
        keep_db=False,
    )

    assert "xdist.looponfail" in _Manager.commands[0]
    assert _Manager.interactives == [True]


def test_inner_coverage_plugin_is_loaded(tmp_path: "Path", monkeypatch) -> None:
    _reset_manager(monkeypatch)
    manager = _Manager(get_case("remote-dbs-py39-postgres14"), tmp_path)

    runner._run_inner_pytest(
        manager,
        manager.case,
        ("--cov=lib/rucio",),
        keep_db=False,
    )

    plugin_index = _Manager.commands[0].index("pytest_cov")
    assert _Manager.commands[0][plugin_index - 1:plugin_index + 1] == (
        "-p",
        "pytest_cov",
    )
    assert "rerunfailures" in _Manager.commands[0]


def test_cpu_profile_wraps_pytest_and_follows_subprocesses(
    tmp_path: "Path",
    monkeypatch,
) -> None:
    _reset_manager(monkeypatch)
    monkeypatch.setattr(
        runner,
        "artifact_ownership_environment",
        lambda path: {
            "RUCIO_DEV_ARTIFACT_DIR": path,
            "RUCIO_DEV_HOST_UID": "1234",
            "RUCIO_DEV_HOST_GID": "5678",
        },
    )
    manager = _Manager(get_case("remote-dbs-py39-postgres14"), tmp_path)

    result = runner._run_inner_pytest(
        manager,
        manager.case,
        ("tests/test_ping.py",),
        keep_db=False,
        environment={
            "RUCIO_DEV_PROFILE": "cpu",
            "RUCIO_DEV_PROFILE_IDLE": "1",
            "RUCIO_DEV_ARTIFACT_DIR": _PROFILE_ARTIFACT,
        },
    )

    assert result == 0
    assert _Manager.commands[0] == (
        "rucio",
        "mkdir",
        "-p",
        "--",
        _PROFILE_ARTIFACT,
    )
    command = _unwrapped_exec_command(_Manager.commands[1])
    assert command[:10] == (
        "rucio",
        "py-spy",
        "record",
        "--format",
        "flamegraph",
        "--subprocesses",
        "--idle",
        "--output",
        f"{_PROFILE_ARTIFACT}/cpu.svg",
        "--",
    )
    assert command[10:14] == ("python", "-bb", "-m", "pytest")
    assert "--numprocesses=auto" in command
    assert _Manager.environments[1]["RUCIO_DEV_HOST_UID"] == "1234"
    assert _Manager.environments[1]["RUCIO_DEV_HOST_GID"] == "5678"


@pytest.mark.parametrize(
    ("pytest_result", "report_results", "expected"),
    (
        (0, (0, 0, 0), 0),
        (5, (0, 0, 0), 5),
        (0, (7, 8, 9), 7),
        (0, (0, 8, 9), 8),
        (5, (7, 8, 9), 5),
    ),
)
def test_memory_profile_renders_parent_and_forks_and_preserves_test_failure(
    tmp_path: "Path",
    monkeypatch,
    pytest_result: int,
    report_results: tuple[int, ...],
    expected: int,
) -> None:
    artifact_dir = tmp_path / Path(_PROFILE_ARTIFACT).relative_to(runner._CONTAINER_SOURCE_ROOT)
    artifact_dir.mkdir(parents=True)
    for name in ("memory.bin", "memory.bin.29", "memory.bin.5", "memory.bin.worker"):
        (artifact_dir / name).write_text("capture")
    (artifact_dir / "memory.bin.8").mkdir()
    _reset_manager(monkeypatch, results=(0, pytest_result, *report_results))
    manager = _Manager(get_case("remote-dbs-py39-postgres14"), tmp_path)

    result = runner._run_inner_pytest(
        manager,
        manager.case,
        ("tests/test_ping.py",),
        keep_db=False,
        environment={
            "RUCIO_DEV_PROFILE": "memory",
            "RUCIO_DEV_ARTIFACT_DIR": _PROFILE_ARTIFACT,
        },
    )

    assert result == expected
    profile_command = _unwrapped_exec_command(_Manager.commands[1])
    assert profile_command[:11] == (
        "rucio",
        "python",
        "-bb",
        "-m",
        "memray",
        "run",
        "--force",
        "--follow-fork",
        "--output",
        f"{_PROFILE_ARTIFACT}/memory.bin",
        "-m",
    )
    assert profile_command[11] == "pytest"
    assert "--numprocesses=auto" not in profile_command
    assert [_unwrapped_exec_command(command) for command in _Manager.commands[2:]] == [
        (
            "rucio",
            "python",
            "-m",
            "memray",
            "flamegraph",
            "--force",
            "--output",
            f"{_PROFILE_ARTIFACT}/memory.html",
            f"{_PROFILE_ARTIFACT}/memory.bin",
        ),
        (
            "rucio",
            "python",
            "-m",
            "memray",
            "flamegraph",
            "--force",
            "--output",
            f"{_PROFILE_ARTIFACT}/memory-fork-5.html",
            f"{_PROFILE_ARTIFACT}/memory.bin.5",
        ),
        (
            "rucio",
            "python",
            "-m",
            "memray",
            "flamegraph",
            "--force",
            "--output",
            f"{_PROFILE_ARTIFACT}/memory-fork-29.html",
            f"{_PROFILE_ARTIFACT}/memory.bin.29",
        ),
    ]


def test_call_profile_wraps_pytest_in_yappi(
    tmp_path: "Path",
    monkeypatch,
) -> None:
    _reset_manager(monkeypatch)
    manager = _Manager(get_case("remote-dbs-py39-postgres14"), tmp_path)

    result = runner._run_inner_pytest(
        manager,
        manager.case,
        ("tests/test_ping.py",),
        keep_db=False,
        environment={
            "RUCIO_DEV_PROFILE": "calls",
            "RUCIO_DEV_ARTIFACT_DIR": _PROFILE_ARTIFACT,
        },
    )

    assert result == 0
    command = _unwrapped_exec_command(_Manager.commands[1])
    assert command[:11] == (
        "rucio",
        "python",
        "-m",
        "tools.devenv.calls",
        "--output",
        f"{_PROFILE_ARTIFACT}/calls.pstats",
        "--",
        "python",
        "-bb",
        "-m",
        "pytest",
    )
    assert "--numprocesses=auto" not in command
    assert _unwrapped_exec_command(_Manager.commands[2]) == (
        "rucio",
        "python",
        "/rucio_source/tools/devenv/pstats_report.py",
        f"{_PROFILE_ARTIFACT}/calls.pstats",
        f"{_PROFILE_ARTIFACT}/calls.html",
    )


def test_debug_wraps_pytest_and_disables_implicit_xdist(
    tmp_path: "Path",
    monkeypatch,
) -> None:
    _reset_manager(monkeypatch)
    manager = _Manager(get_case("remote-dbs-py39-postgres14"), tmp_path)

    result = runner._run_inner_pytest(
        manager,
        manager.case,
        ("tests/test_ping.py",),
        keep_db=False,
        environment={"RUCIO_DEV_DEBUG": "1", "RUCIO_DEBUG_PORT": "5678"},
    )

    assert result == 0
    command = _Manager.commands[0]
    assert command[:11] == (
        "rucio",
        "python",
        "-bb",
        "-m",
        "debugpy",
        "--listen",
        "0.0.0.0:5678",
        "--wait-for-client",
        "-m",
        "pytest",
        "-r",
    )
    assert "--numprocesses=auto" not in command


def test_observe_wraps_pytest_and_disables_implicit_xdist(
    tmp_path: "Path",
    monkeypatch,
) -> None:
    _reset_manager(monkeypatch)
    manager = _Manager(get_case("remote-dbs-py39-postgres14"), tmp_path)

    result = runner._run_inner_pytest(
        manager,
        manager.case,
        ("tests/test_ping.py",),
        keep_db=False,
        environment={
            "RUCIO_DEV_OBSERVE": "1",
            "RUCIO_DEV_OBSERVE_RUN_ID": "0123abcd",
            "RUCIO_DEV_OBSERVE_TEST_CASE": "remote-dbs-py39-postgres14",
            "RUCIO_DEV_OBSERVE_TEST_SELECTOR": "tests/test_ping.py",
        },
    )

    assert result == 0
    command = _Manager.commands[0]
    assert command[:21] == (
        "rucio",
        "opentelemetry-instrument",
        "--service_name",
        "rucio-test-client",
        "--",
        "python",
        "-m",
        "tools.devenv.observe",
        "--span-name",
        "rucio-test.run",
        "--attribute",
        "rucio.test.run_id=0123abcd",
        "--attribute",
        "rucio.test.case=remote-dbs-py39-postgres14",
        "--attribute",
        "rucio.test.selector=tests/test_ping.py",
        "--",
        "python",
        "-bb",
        "-m",
        "pytest",
    )
    assert "--numprocesses=auto" not in command


@pytest.mark.parametrize(
    ("environment", "message"),
    (
        (
            {
                "RUCIO_DEV_PROFILE": "unknown",
                "RUCIO_DEV_ARTIFACT_DIR": _PROFILE_ARTIFACT,
            },
            "RUCIO_DEV_PROFILE must be one of",
        ),
        (
            {
                "RUCIO_DEV_PROFILE": "memory",
                "RUCIO_DEV_PROFILE_IDLE": "1",
                "RUCIO_DEV_ARTIFACT_DIR": _PROFILE_ARTIFACT,
            },
            "only supported for CPU profiles",
        ),
        (
            {
                "RUCIO_DEV_PROFILE": "cpu",
                "RUCIO_DEV_OBSERVE": "1",
                "RUCIO_DEV_ARTIFACT_DIR": _PROFILE_ARTIFACT,
            },
            "cannot be combined",
        ),
        (
            {"RUCIO_DEV_OBSERVE": "yes"},
            "RUCIO_DEV_OBSERVE must be 0 or 1",
        ),
        (
            {"RUCIO_DEV_PROFILE": "cpu"},
            "RUCIO_DEV_ARTIFACT_DIR is required",
        ),
        (
            {
                "RUCIO_DEV_PROFILE": "cpu",
                "RUCIO_DEV_ARTIFACT_DIR": "artifacts/run",
            },
            "safe absolute container path",
        ),
    ),
)
def test_profile_configuration_is_validated_before_container_execution(
    tmp_path: "Path",
    monkeypatch,
    environment,
    message: str,
) -> None:
    _reset_manager(monkeypatch)
    manager = _Manager(get_case("remote-dbs-py39-postgres14"), tmp_path)

    with pytest.raises(pytest.UsageError, match=message):
        runner._run_inner_pytest(
            manager,
            manager.case,
            (),
            keep_db=False,
            environment=environment,
        )

    assert _Manager.commands == []


def test_profile_returns_artifact_directory_creation_failure(
    tmp_path: "Path",
    monkeypatch,
) -> None:
    _reset_manager(monkeypatch, results=(13,))
    manager = _Manager(get_case("remote-dbs-py39-postgres14"), tmp_path)

    result = runner._run_inner_pytest(
        manager,
        manager.case,
        (),
        keep_db=False,
        environment={
            "RUCIO_DEV_PROFILE": "cpu",
            "RUCIO_DEV_ARTIFACT_DIR": _PROFILE_ARTIFACT,
        },
    )

    assert result == 13
    assert len(_Manager.commands) == 1


def test_no_cov_does_not_enable_coverage_aggregation() -> None:
    assert runner.append_coverage(("--no-cov",)) == ["--no-cov"]
    assert runner.defer_coverage_threshold(("-k", "rule")) == ["-k", "rule"]


@pytest.mark.parametrize(
    ("arguments", "expected"),
    (
        (("-o", "cache_dir=/tmp/cache"), "cache_dir=/tmp/cache/case"),
        (("-ocache_dir=cache",), "-ocache_dir=cache/case"),
        (
            ("--override-ini=cache_dir=cache",),
            "--override-ini=cache_dir=cache/case",
        ),
    ),
)
def test_cache_override_is_qualified(arguments, expected) -> None:
    assert expected in runner.qualify_cache_dir(arguments, "case")


def test_default_cache_is_case_specific() -> None:
    assert (
        "cache_dir=/rucio_source/.pytest_cache/rucio-cases/case"
        in runner.qualify_cache_dir(("-q",), "case")
    )


def test_generated_cache_option_precedes_separator() -> None:
    assert runner.qualify_cache_dir(("--", "test-name"), "case") == [
        "-o",
        "cache_dir=/rucio_source/.pytest_cache/rucio-cases/case",
        "--",
        "test-name",
    ]


def test_shared_pytest_paths_are_qualified() -> None:
    arguments = runner.qualify_paths(
        (
            "--junitxml=results.xml",
            "--log-file",
            "logs/pytest.log",
            "--basetemp=tmp",
            "-o",
            "log_file=ini.log",
        ),
        "case",
    )

    assert "--junitxml=results-case.xml" in arguments
    assert "logs/pytest-case.log" in arguments
    assert "--basetemp=tmp/case" in arguments
    assert "log_file=ini-case.log" in arguments


def test_pytest_literals_after_separator_are_unchanged() -> None:
    arguments = (
        "--",
        "--junitxml=literal.xml",
        "--log-file=literal.log",
        "--basetemp=literal",
        "-o",
        "log_file=literal.log",
        "--cov=literal",
        "--pdb",
    )

    assert runner.qualify_paths(arguments, "case") == list(arguments)
    assert not runner.coverage_enabled(arguments)
    assert not runner.is_interactive(arguments)


def test_generated_coverage_options_precede_separator() -> None:
    arguments = ("--cov=lib/rucio", "--", "test-name")

    assert runner.append_coverage(arguments) == [
        "--cov=lib/rucio",
        "--cov-append",
        "--",
        "test-name",
    ]
    assert runner.defer_coverage_threshold(arguments) == [
        "--cov=lib/rucio",
        "--cov-fail-under=0",
        "--",
        "test-name",
    ]


def test_pytest_device_paths_are_preserved() -> None:
    arguments = runner.qualify_paths(
        (
            "--junitxml=/dev/stdout",
            "--log-file=/dev/stderr",
            "-o",
            "log_file=/dev/null",
        ),
        "case",
    )

    assert "--junitxml=/dev/stdout" in arguments
    assert "--log-file=/dev/stderr" in arguments
    assert "log_file=/dev/null" in arguments


@pytest.mark.parametrize(
    ("arguments", "expected"),
    (
        (("--debug", "-q"), "--debug=pytestdebug-case.log"),
        (("--debug", "logs/debug.log"), "logs/debug-case.log"),
        (("--debug=logs/debug.log",), "--debug=logs/debug-case.log"),
    ),
)
def test_pytest_debug_paths_are_qualified(arguments, expected) -> None:
    assert expected in runner.qualify_paths(arguments, "case")


def test_unit_case_builds_and_runs_requested_python(tmp_path: "Path", monkeypatch) -> None:
    commands = []
    timeouts = []
    build_context = None
    staged_files = set()
    _write_unit_build_inputs(tmp_path)

    def run(command, **kwargs):
        nonlocal build_context, staged_files
        commands.append(list(command))
        timeouts.append(kwargs.get("timeout"))
        if command[:4] == ["docker", "buildx", "build", "--load"]:
            build_context = Path(command[-1])
            staged_files = {
                path.relative_to(build_context)
                for path in build_context.rglob("*")
                if path.is_file()
            }
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(runner.subprocess, "run", run)

    result = runner.run_unit_case(
        get_case("unit-py313"),
        tmp_path,
        ("-k", "config"),
        container_environment={"RUCIO_LOGGING_FORMAT": "json"},
    )

    assert result == 0
    assert commands[0][:4] == ["docker", "buildx", "build", "--load"]
    assert timeouts[0] == 1800
    assert staged_files == set(runner._UNIT_BUILD_INPUTS)
    assert build_context is not None
    assert commands[0][commands[0].index("--file") + 1] == str(build_context / runner._UNIT_DOCKERFILE)
    assert not build_context.exists()
    assert "--target" not in commands[0]
    assert "PYTHON=3.13" in commands[0]
    assert commands[1][:3] == ["docker", "run", "--rm"]
    assert commands[0][commands[0].index("--tag") + 1] in commands[1]
    assert f"{tmp_path.resolve()}:/rucio_source:z" in commands[1]
    assert "PYTHONPATH=/rucio_source/lib" in commands[1]
    assert "PYTEST_DISABLE_PLUGIN_AUTOLOAD=true" in commands[1]
    assert "RUCIO_LOGGING_FORMAT=json" in commands[1]
    assert commands[1][-2:] == ["tests/rucio", "tests/ruciopytest"]
    assert commands[1][commands[1].index("-k"):][:2] == ["-k", "config"]


def test_unit_case_debugs_with_deterministic_discovery_labels(
    tmp_path: "Path",
    monkeypatch,
) -> None:
    commands = []
    _write_unit_build_inputs(tmp_path)
    monkeypatch.setenv("RUCIO_TEST_PROJECT_NONCE", "0123abcd")
    monkeypatch.setattr(
        runner.subprocess,
        "run",
        lambda command, **kwargs: commands.append(list(command)) or subprocess.CompletedProcess(command, 0),
    )

    result = runner.run_unit_case(
        get_case("unit-py310"),
        tmp_path,
        ("tests/rucio/test_common.py",),
        container_environment={"RUCIO_DEV_DEBUG": "1", "RUCIO_DEBUG_PORT": "5678"},
        explicit_selectors=("tests/rucio/test_common.py",),
    )

    assert result == 0
    command = commands[1]
    assert "com.docker.compose.project=rucio-test-unit-py310-0123abcd" in command
    assert "com.docker.compose.service=rucio" in command
    assert command[command.index("--publish") + 1] == "127.0.0.1:5678:5678"
    entrypoint = command.index("--entrypoint")
    image = command.index("rucio-test-unit:%s-py310" % runner.checkout_id(tmp_path))
    assert command[entrypoint:image] == ["--entrypoint", "python"]
    assert command[image + 1:image + 9] == [
        "-bb",
        "-m",
        "debugpy",
        "--listen",
        "0.0.0.0:5678",
        "--wait-for-client",
        "-m",
        "pytest",
    ]


def test_unit_case_cpu_profile_writes_artifact_and_enables_ptrace(
    tmp_path: "Path",
    monkeypatch,
) -> None:
    commands = []
    artifact = "/rucio_source/.rucio-dev/artifacts/cpu-unit"
    _write_unit_build_inputs(tmp_path)
    monkeypatch.setenv("RUCIO_TEST_PROJECT_NONCE", "0123abcd")
    monkeypatch.setattr(
        runner,
        "artifact_ownership_environment",
        lambda path: {
            "RUCIO_DEV_ARTIFACT_DIR": path,
            "RUCIO_DEV_HOST_UID": "1234",
            "RUCIO_DEV_HOST_GID": "5678",
        },
    )
    monkeypatch.setattr(
        runner.subprocess,
        "run",
        lambda command, **kwargs: commands.append(list(command)) or subprocess.CompletedProcess(command, 0),
    )

    result = runner.run_unit_case(
        get_case("unit-py310"),
        tmp_path,
        ("tests/rucio/test_common.py",),
        container_environment={
            "RUCIO_DEV_PROFILE": "cpu",
            "RUCIO_DEV_ARTIFACT_DIR": artifact,
        },
        explicit_selectors=("tests/rucio/test_common.py",),
    )

    assert result == 0
    assert (tmp_path / ".rucio-dev/artifacts/cpu-unit").is_dir()
    command = commands[1]
    assert "com.docker.compose.project=rucio-test-unit-py310-0123abcd" in command
    assert command[command.index("--cap-add"):command.index("--cap-add") + 4] == [
        "--cap-add",
        "SYS_PTRACE",
        "--security-opt",
        "seccomp=unconfined",
    ]
    image, profiled = _unwrapped_run_command(command)
    assert profiled[:8] == [
        "py-spy",
        "record",
        "--format",
        "flamegraph",
        "--subprocesses",
        "--output",
        f"{artifact}/cpu.svg",
        "--",
    ]
    assert image == commands[0][commands[0].index("--tag") + 1]
    assert "RUCIO_DEV_HOST_UID=1234" in command
    assert "RUCIO_DEV_HOST_GID=5678" in command


@pytest.mark.parametrize(
    ("profile", "capture", "report", "runner_prefix", "render_prefix"),
    (
        (
            "memory",
            "memory.bin",
            "memory.html",
            ("python", "-bb", "-m", "memray", "run"),
            ("python", "-m", "memray", "flamegraph"),
        ),
        (
            "calls",
            "calls.pstats",
            "calls.html",
            ("python", "-m", "tools.devenv.calls", "--output"),
            ("python", "/rucio_source/tools/devenv/pstats_report.py"),
        ),
    ),
)
def test_unit_case_renders_memory_and_call_profiles(
    tmp_path: "Path",
    monkeypatch,
    profile: str,
    capture: str,
    report: str,
    runner_prefix: tuple[str, ...],
    render_prefix: tuple[str, ...],
) -> None:
    commands = []
    artifact = f"/rucio_source/.rucio-dev/artifacts/{profile}-unit"
    _write_unit_build_inputs(tmp_path)
    if profile == "memory":
        artifact_dir = tmp_path / Path(artifact).relative_to(runner._CONTAINER_SOURCE_ROOT)
        artifact_dir.mkdir(parents=True)
        (artifact_dir / capture).write_text("capture")
    monkeypatch.setattr(
        runner.subprocess,
        "run",
        lambda command, **kwargs: commands.append(list(command)) or subprocess.CompletedProcess(command, 0),
    )

    result = runner.run_unit_case(
        get_case("unit-py310"),
        tmp_path,
        ("tests/rucio/test_common.py",),
        container_environment={
            "RUCIO_DEV_PROFILE": profile,
            "RUCIO_DEV_ARTIFACT_DIR": artifact,
        },
        explicit_selectors=("tests/rucio/test_common.py",),
    )

    assert result == 0
    assert len(commands) == 3
    image = commands[0][commands[0].index("--tag") + 1]
    profile_image, profiled = _unwrapped_run_command(commands[1])
    assert profile_image == image
    assert tuple(profiled[:len(runner_prefix)]) == runner_prefix
    if profile == "memory":
        assert "--follow-fork" in profiled
    render_image, rendered = _unwrapped_run_command(commands[2])
    assert render_image == image
    assert tuple(rendered[:len(render_prefix)]) == render_prefix
    assert f"{artifact}/{capture}" in commands[1]
    assert f"{artifact}/{capture}" in commands[2]
    assert f"{artifact}/{report}" in commands[2]


@pytest.mark.parametrize(
    ("target_result", "report_results", "expected"),
    (
        (0, (0, 0, 0), 0),
        (5, (0, 0, 0), 5),
        (0, (7, 8, 9), 7),
        (0, (0, 8, 9), 8),
        (5, (7, 8, 9), 5),
    ),
)
def test_unit_memory_profile_renders_parent_and_forks_and_preserves_failures(
    tmp_path: "Path",
    monkeypatch,
    target_result: int,
    report_results: tuple[int, ...],
    expected: int,
) -> None:
    commands = []
    artifact = "/rucio_source/.rucio-dev/artifacts/memory-unit-forks"
    artifact_dir = tmp_path / Path(artifact).relative_to(runner._CONTAINER_SOURCE_ROOT)
    artifact_dir.mkdir(parents=True)
    for name in ("memory.bin", "memory.bin.29", "memory.bin.5", "memory.bin.worker"):
        (artifact_dir / name).write_text("capture")
    (artifact_dir / "memory.bin.8").mkdir()
    _write_unit_build_inputs(tmp_path)
    results = iter((0, target_result, *report_results))

    def run(command, **kwargs):
        commands.append(list(command))
        return subprocess.CompletedProcess(command, next(results))

    monkeypatch.setattr(runner.subprocess, "run", run)

    result = runner.run_unit_case(
        get_case("unit-py310"),
        tmp_path,
        ("tests/rucio/test_common.py",),
        container_environment={
            "RUCIO_DEV_PROFILE": "memory",
            "RUCIO_DEV_ARTIFACT_DIR": artifact,
        },
        explicit_selectors=("tests/rucio/test_common.py",),
    )

    assert result == expected
    assert len(commands) == 5
    _, profiled = _unwrapped_run_command(commands[1])
    assert "--follow-fork" in profiled
    reports = []
    for command in commands[2:]:
        _, rendered = _unwrapped_run_command(command)
        reports.append((rendered[rendered.index("--output") + 1], rendered[-1]))
    assert reports == [
        (f"{artifact}/memory.html", f"{artifact}/memory.bin"),
        (f"{artifact}/memory-fork-5.html", f"{artifact}/memory.bin.5"),
        (f"{artifact}/memory-fork-29.html", f"{artifact}/memory.bin.29"),
    ]


def test_unit_case_ignores_inherited_default_platform(tmp_path: "Path", monkeypatch) -> None:
    environments = []
    _write_unit_build_inputs(tmp_path)

    def run(command, **kwargs):
        environments.append(kwargs["env"])
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setenv("DOCKER_DEFAULT_PLATFORM", "linux/amd64")
    monkeypatch.setattr(runner.subprocess, "run", run)

    runner.run_unit_case(get_case("unit-py312"), tmp_path, ())

    assert all("DOCKER_DEFAULT_PLATFORM" not in env for env in environments)


def test_unit_case_captures_parallel_output(tmp_path: "Path", monkeypatch) -> None:
    _write_unit_build_inputs(tmp_path)

    def run(command, **kwargs):
        kwargs["stdout"].write("command output\n")
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(runner.subprocess, "run", run)
    monkeypatch.setattr(
        runner.ContainerManager,
        "make_project_name",
        lambda *args, **kwargs: "unit-project",
    )
    log_root = tmp_path / ".test-logs"
    log_dir = log_root / "unit-project"
    log_dir.mkdir(parents=True)
    log_root.chmod(0o755)
    log_dir.chmod(0o755)
    output = log_dir / "case.log"
    output.write_text("old output")
    output.chmod(0o644)

    runner.run_unit_case(
        get_case("unit-py312"),
        tmp_path,
        (),
        log_output=True,
    )

    assert output.read_text() == "command output\ncommand output\n"
    assert stat.S_IMODE(log_root.stat().st_mode) == 0o700
    assert stat.S_IMODE(log_dir.stat().st_mode) == 0o700
    assert stat.S_IMODE(output.stat().st_mode) == 0o600


def test_unit_case_preserves_explicit_selector(tmp_path: "Path", monkeypatch) -> None:
    commands = []
    _write_unit_build_inputs(tmp_path)

    def run(command, **kwargs):
        commands.append(list(command))
        return subprocess.CompletedProcess(command, 5 if "run" in command else 0)

    monkeypatch.setattr(runner.subprocess, "run", run)

    result = runner.run_unit_case(
        get_case("unit-py39"),
        tmp_path,
        ("tests/rucio/test_common.py",),
        explicit_selectors=("tests/rucio/test_common.py",),
    )

    assert result == 5
    assert commands[1].count("tests/rucio/test_common.py") == 1
    assert "tests/ruciopytest" not in commands[1]


def test_unit_command_keeps_options_before_separator(
    tmp_path: "Path",
    monkeypatch,
) -> None:
    commands = []
    _write_unit_build_inputs(tmp_path)

    def run(command, **kwargs):
        commands.append(list(command))
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(runner.subprocess, "run", run)
    selector = "tests/rucio/test_common.py"
    arguments = runner.container_pytest_config(
        runner.forwarded_pytest_args(("--", selector)),
        tmp_path,
        tmp_path,
        tmp_path / "pyproject.toml",
    )

    runner.run_unit_case(
        get_case("unit-py39"),
        tmp_path,
        arguments,
        explicit_selectors=(selector,),
    )

    marker = commands[1].index("--")
    assert commands[1][marker + 1:] == [selector]
    assert commands[1].index("-c") < marker
    assert next(
        index
        for index, value in enumerate(commands[1])
        if value.startswith("cache_dir=")
    ) < marker


def test_unit_case_applies_canonical_collection(tmp_path: "Path", monkeypatch) -> None:
    commands = []
    _write_unit_build_inputs(tmp_path)

    def run(command, **kwargs):
        commands.append(list(command))
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(runner.subprocess, "run", run)

    runner.run_unit_case(get_case("unit-py39"), tmp_path, ())

    assert "RUCIO_PYTEST_INNER=1" in commands[1]
    assert "RUCIO_SKIP_TEST_SETUP=1" in commands[1]
    assert "RUCIO_TEST_CASE=unit-py39" in commands[1]
    assert "RUCIO_TEST_SUITE=unit" in commands[1]
    assert "RUCIO_TEST_PYTHON=3.9" in commands[1]


def test_unit_case_loads_requested_pytest_plugins(tmp_path: "Path", monkeypatch) -> None:
    commands = []
    _write_unit_build_inputs(tmp_path)

    def run(command, **kwargs):
        commands.append(list(command))
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(runner.subprocess, "run", run)

    runner.run_unit_case(
        get_case("unit-py312"),
        tmp_path,
        ("-n", "2", "--cov=lib/rucio"),
    )

    assert commands[1].count("xdist") == 1
    assert commands[1].count("pytest_cov") == 1
    assert commands[1].count("rerunfailures") == 1


def test_unit_case_loads_looponfail_plugin(tmp_path: "Path", monkeypatch) -> None:
    commands = []
    _write_unit_build_inputs(tmp_path)

    def run(command, **kwargs):
        commands.append(list(command))
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(runner.subprocess, "run", run)

    runner.run_unit_case(get_case("unit-py312"), tmp_path, ("-f",))

    assert commands[1][:4] == ["docker", "run", "--interactive", "--tty"]
    assert commands[1].count("xdist") == 1
    assert commands[1].count("xdist.looponfail") == 1


def test_unit_case_allocates_terminal_for_pdb(tmp_path: "Path", monkeypatch) -> None:
    commands = []
    _write_unit_build_inputs(tmp_path)

    def run(command, **kwargs):
        commands.append(list(command))
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(runner.subprocess, "run", run)

    runner.run_unit_case(get_case("unit-py312"), tmp_path, ("--pdb",))

    assert commands[1][:4] == ["docker", "run", "--interactive", "--tty"]
