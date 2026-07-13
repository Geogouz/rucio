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

import subprocess  # noqa: S404
from typing import TYPE_CHECKING

from tests.ruciopytest import runner
from tests.ruciopytest.profiles import get_case

if TYPE_CHECKING:
    from pathlib import Path


class _Manager:
    results: list[int] = []
    commands: list[tuple[str, ...]] = []
    environments: list[dict[str, str]] = []
    stops: list[bool] = []

    def __init__(self, case, root_dir, keep_db=False):
        self.case = case
        self.root_dir = root_dir
        self.keep_db = keep_db
        self.environment = {}

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
        if arguments[:2] == ("cat", "/tmp/test_tpc.artifact"):
            return subprocess.CompletedProcess(arguments, 0, stdout="/fts/log\n")
        result = self.results.pop(0) if self.results else 0
        return subprocess.CompletedProcess(arguments, result, stdout="")


def _reset_manager(monkeypatch, results=()):
    _Manager.results = list(results)
    _Manager.commands = []
    _Manager.environments = []
    _Manager.stops = []
    monkeypatch.setattr(runner, "ContainerManager", _Manager)


def test_forwarded_args_remove_only_runner_options() -> None:
    assert runner.forwarded_pytest_args([
        "--suite=remote_dbs",
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
    ]


def test_multi_vo_runs_both_legs_in_order(tmp_path: "Path", monkeypatch) -> None:
    _reset_manager(monkeypatch, (0, 0))

    result = runner.run_container_case(
        get_case("multi-vo-py39-postgres14"),
        tmp_path,
        ("--junitxml=results.xml",),
    )

    assert result == 0
    assert [environment["RUCIO_MULTI_VO_LEG"] for environment in _Manager.environments] == [
        "tst",
        "ts2",
    ]
    assert "--junitxml=results-tst.xml" in _Manager.commands[0]
    assert "--junitxml=results-ts2.xml" in _Manager.commands[1]


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
        (),
    )

    assert result == 0
    pytest_commands = [command for command in _Manager.commands if "pytest" in command]
    cat_index = next(index for index, command in enumerate(_Manager.commands) if command[1:3] == ("cat", "/tmp/test_tpc.artifact"))
    grep_index = next(index for index, command in enumerate(_Manager.commands) if command[1:3] == ("grep", "-Fq"))
    assert len(pytest_commands) == 15
    assert _Manager.commands.index(pytest_commands[0]) < cat_index < grep_index
    assert _Manager.commands.index(pytest_commands[9]) < cat_index
    assert grep_index < _Manager.commands.index(pytest_commands[10])
    assert "RUCIO_SKIP_TEST_SETUP" not in _Manager.environments[0]
    assert all(
        environment["RUCIO_SKIP_TEST_SETUP"] == "1"
        for environment in _Manager.environments[1:]
        if environment
    )


def test_integration_filter_continues_past_unmatched_paths(
    tmp_path: "Path",
    monkeypatch,
) -> None:
    _reset_manager(monkeypatch, (*([5] * 9), 0, 0, *([5] * 5)))

    result = runner.run_container_case(
        get_case("integration-py39-postgres14"),
        tmp_path,
        ("-k", "tpc"),
    )

    assert result == 0
    assert len([command for command in _Manager.commands if "pytest" in command]) == 15
    assert any(command[1:3] == ("grep", "-Fq") for command in _Manager.commands)


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


def test_no_cov_does_not_enable_coverage_aggregation() -> None:
    assert runner.append_coverage(("--no-cov",)) == ["--no-cov"]
    assert runner.defer_coverage_threshold(("-k", "rule")) == ["-k", "rule"]


def test_unit_case_builds_and_runs_requested_python(tmp_path: "Path", monkeypatch) -> None:
    commands = []
    timeouts = []

    def run(command, **kwargs):
        commands.append(list(command))
        timeouts.append(kwargs.get("timeout"))
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(runner.subprocess, "run", run)

    result = runner.run_unit_case(
        get_case("unit-py312"),
        tmp_path,
        ("-k", "config"),
        container_environment={"RUCIO_LOGGING_FORMAT": "json"},
    )

    assert result == 0
    assert commands[0][:4] == ["docker", "buildx", "build", "--load"]
    assert timeouts[0] == 1800
    assert str(tmp_path / "etc/docker/test/unit.Dockerfile") in commands[0]
    assert "--target" not in commands[0]
    assert "PYTHON=3.12" in commands[0]
    assert commands[1][:3] == ["docker", "run", "--rm"]
    assert commands[0][commands[0].index("--tag") + 1] in commands[1]
    assert f"{tmp_path.resolve()}:/rucio_source:z" in commands[1]
    assert "RUCIO_LOGGING_FORMAT=json" in commands[1]
    assert commands[1][-4:] == ["-k", "config", "tests/rucio", "tests/ruciopytest"]


def test_unit_case_ignores_inherited_default_platform(tmp_path: "Path", monkeypatch) -> None:
    environments = []

    def run(command, **kwargs):
        environments.append(kwargs["env"])
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setenv("DOCKER_DEFAULT_PLATFORM", "linux/amd64")
    monkeypatch.setattr(runner.subprocess, "run", run)

    runner.run_unit_case(get_case("unit-py312"), tmp_path, ())

    assert all("DOCKER_DEFAULT_PLATFORM" not in env for env in environments)


def test_unit_case_preserves_explicit_selector(tmp_path: "Path", monkeypatch) -> None:
    commands = []

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


def test_unit_case_applies_canonical_collection(tmp_path: "Path", monkeypatch) -> None:
    commands = []

    def run(command, **kwargs):
        commands.append(list(command))
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(runner.subprocess, "run", run)

    runner.run_unit_case(get_case("unit-py39"), tmp_path, ())

    assert "RUCIO_PYTEST_INNER=1" in commands[1]
    assert "RUCIO_SKIP_TEST_SETUP=1" in commands[1]
    assert "RUCIO_TEST_CASE=unit-py39" in commands[1]


def test_unit_case_loads_requested_pytest_plugins(tmp_path: "Path", monkeypatch) -> None:
    commands = []

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


def test_unit_case_allocates_terminal_for_pdb(tmp_path: "Path", monkeypatch) -> None:
    commands = []

    def run(command, **kwargs):
        commands.append(list(command))
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(runner.subprocess, "run", run)

    runner.run_unit_case(get_case("unit-py312"), tmp_path, ("--pdb",))

    assert commands[1][:4] == ["docker", "run", "--interactive", "--tty"]
