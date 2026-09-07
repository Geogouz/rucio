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
import threading
from pathlib import Path
from types import SimpleNamespace

import pytest

from tests.ruciopytest import plugin
from tests.ruciopytest.collection import case_key
from tests.ruciopytest.profiles import SUPPORTED_PYTHON_VERSIONS

REPO_ROOT = Path(__file__).resolve().parents[2]


class _Config:
    class ArgsSource:
        ARGS = object()
        TESTPATHS = object()

    def __init__(self, **options) -> None:
        defaults = {
            "case": None,
            "suite": None,
            "python_version": None,
            "rdbms": None,
            "policy": None,
            "list_cases": False,
            "dry_run": False,
            "dry_run_json": False,
            "xdist_workers": None,
            "case_workers": None,
            "container_env": [],
            "keep_db": False,
            "collectonly": False,
            "dist": "no",
            "distload": False,
            "tx": [],
            "looponfail": False,
        }
        defaults.update(options)
        self.options = defaults
        self.ini_options = self.options.pop("ini_options", {})
        self.rootpath = REPO_ROOT
        self.inipath = REPO_ROOT / "pyproject.toml"
        self.args = ["tests"]
        self.args_source = self.ArgsSource.TESTPATHS
        self.invocation_params = SimpleNamespace(args=())
        self.stash = pytest.Stash()

    def getoption(self, name, default=None):
        return self.options.get(name.lstrip("-"), default)

    def getini(self, name):
        return self.ini_options.get(name, [])


@pytest.fixture(autouse=True)
def outer_runner_environment(monkeypatch) -> None:
    for variable in ("RUCIO_PYTEST_INNER", "RUCIO_TEST_SUITE", "RUCIO_TEST_PYTHON"):
        monkeypatch.delenv(variable, raising=False)


def test_list_cases_is_machine_readable(capsys) -> None:
    config = _Config(list_cases=True)

    assert plugin.pytest_cmdline_main(config) == 0

    cases = json.loads(capsys.readouterr().out)
    assert {case["id"] for case in cases} >= {
        "unit-py39",
        "unit-py313",
        "remote-dbs-py39-postgres14",
        "remote-dbs-py310-oracle",
        "votest-py39-postgres14-atlas",
        "integration-py39-postgres14",
    }
    atlas = next(case for case in cases if case["policy"] == "atlas")
    assert len(atlas["tests"]) == 36
    runtimes = {
        f"py{python.replace('.', '')}"
        for python in SUPPORTED_PYTHON_VERSIONS
    }
    assert {case["runtime"] for case in cases} == runtimes | {
        f"{runtime}-oracle" for runtime in runtimes
    }
    oracle = next(case for case in cases if case["rdbms"] == "oracle")
    assert oracle["runtime_target"] == "oracle"
    assert oracle["runtime_platform"] == "linux/amd64"
    postgres = next(case for case in cases if case["rdbms"] == "postgres14")
    assert postgres["runtime_target"] == "final"
    assert postgres["runtime_platform"] is None


def test_suite_resolves_every_matrix_case() -> None:
    cases = plugin.resolve_requested_cases(_Config(suite="remote_dbs"), REPO_ROOT)

    assert {case.id for case in cases} == {
        f"remote-dbs-py{python.replace('.', '')}-{rdbms}"
        for python in SUPPORTED_PYTHON_VERSIONS
        for rdbms in ("oracle", "postgres14")
    }


def test_explicit_axes_generate_effective_suite_cases() -> None:
    cases = plugin.resolve_requested_cases(
        _Config(
            suite="remote_dbs",
            python_version="3.13",
            rdbms="postgres14",
        ),
        REPO_ROOT,
    )

    assert len(cases) == 1
    case = cases[0]
    assert case.id == "remote-dbs-py313-postgres14"
    assert case.python == "3.13"
    assert case.compose_profiles == ("postgres14", "test-dependencies")


def test_explicit_python_can_generate_every_suite_topology() -> None:
    cases = plugin.resolve_requested_cases(
        _Config(suite="all", python_version="3.13"),
        REPO_ROOT,
    )

    assert len(cases) == 8
    assert {case.python for case in cases} == {"3.13"}


def test_suite_selection_ignores_legacy_environment(monkeypatch) -> None:
    monkeypatch.setenv("PYTHON", "3.9")
    monkeypatch.setenv("RDBMS", "oracle")
    monkeypatch.setenv("POLICY", "atlas")

    cases = plugin.resolve_requested_cases(_Config(suite="remote_dbs"), REPO_ROOT)

    assert len(cases) == 10


def test_votest_suite_resolves_every_policy() -> None:
    cases = plugin.resolve_requested_cases(_Config(suite="votest"), REPO_ROOT)

    assert len(cases) == 10
    assert {case.policy for case in cases} == {"atlas", "belleii"}


def test_votest_resolves_policy_selectors() -> None:
    cases = plugin.resolve_requested_cases(
        _Config(suite="votest", policy="atlas"),
        REPO_ROOT,
    )

    assert len(cases) == 5
    assert {case.python for case in cases} == set(SUPPORTED_PYTHON_VERSIONS)
    assert {case.policy for case in cases} == {"atlas"}
    assert all(len(case.test_paths) == 36 for case in cases)


def test_dry_run_does_not_start_containers(monkeypatch, capsys) -> None:
    config = _Config(case="remote-dbs-py39-postgres14", dry_run=True)
    monkeypatch.setattr(plugin.runner, "run_container_case", pytest.fail)

    assert plugin.pytest_cmdline_main(config) == 0
    assert "remote-dbs-py39-postgres14" in capsys.readouterr().out


@pytest.mark.parametrize(
    "options",
    (
        {"keep_db": True},
        {"dry_run": True},
        {"dry_run_json": True},
        {"container_env": ["DEBUG=1"]},
        {"xdist_workers": 2},
        {"case_workers": 2},
        {"python_version": "3.13"},
        {"rdbms": "postgres14"},
        {"policy": "atlas"},
    ),
)
def test_runner_options_require_case_or_suite(options: dict) -> None:
    with pytest.raises(pytest.UsageError, match="require --case or --suite"):
        plugin.resolve_requested_cases(_Config(**options), REPO_ROOT)


def test_container_case_forwards_standard_pytest_args(monkeypatch) -> None:
    config = _Config(case="remote-dbs-py39-postgres14")
    config.args = ["tests/test_rule.py::test_rule"]
    config.args_source = config.ArgsSource.ARGS
    config.invocation_params = SimpleNamespace(args=(
        "--case=remote-dbs-py39-postgres14",
        "-k",
        "rule",
        "tests/test_rule.py::test_rule",
    ))
    captured = {}

    def run(case, root_path, pytest_args, **kwargs):
        captured.update(case=case, args=pytest_args, kwargs=kwargs)
        return 0

    monkeypatch.setattr(plugin.runner, "run_container_case", run)

    assert plugin.pytest_cmdline_main(config) == 0
    assert captured["args"] == [
        "-k",
        "rule",
        "tests/test_rule.py::test_rule",
        "-o",
        "addopts=",
        "--rootdir=/rucio_source",
        "-c",
        "/rucio_source/pyproject.toml",
    ]
    assert captured["kwargs"]["explicit_selectors"] == (
        "tests/test_rule.py::test_rule",
    )


def test_implicit_testpaths_are_not_explicit_selectors(monkeypatch) -> None:
    config = _Config(case="client-py39-postgres14")
    config.invocation_params = SimpleNamespace(args=(
        "--case=client-py39-postgres14",
        "-k",
        "tests",
    ))
    captured = {}

    def run(case, root_path, pytest_args, **kwargs):
        captured.update(args=pytest_args, kwargs=kwargs)
        return 0

    monkeypatch.setattr(plugin.runner, "run_container_case", run)

    assert plugin.pytest_cmdline_main(config) == 0
    assert captured["args"] == [
        "-k",
        "tests",
        "-o",
        "addopts=",
        "--rootdir=/rucio_source",
        "-c",
        "/rucio_source/pyproject.toml",
    ]
    assert captured["kwargs"]["explicit_selectors"] == ()


def test_container_case_forwards_pytest_addopts(monkeypatch) -> None:
    config = _Config(case="remote-dbs-py39-postgres14")
    monkeypatch.setenv("PYTEST_ADDOPTS", "-k rule -q")
    captured = {}

    def run(case, root_path, pytest_args, **kwargs):
        captured["args"] = pytest_args
        return 0

    monkeypatch.setattr(plugin.runner, "run_container_case", run)

    assert plugin.pytest_cmdline_main(config) == 0
    assert captured["args"] == [
        "-k",
        "rule",
        "-q",
        "-o",
        "addopts=",
        "--rootdir=/rucio_source",
        "-c",
        "/rucio_source/pyproject.toml",
    ]


def test_container_case_forwards_configured_addopts(monkeypatch) -> None:
    config = _Config(
        case="remote-dbs-py39-postgres14",
        ini_options={"addopts": ["--strict-markers", "-q"]},
    )
    captured = {}

    def run(case, root_path, pytest_args, **kwargs):
        captured["args"] = pytest_args
        return 0

    monkeypatch.setattr(plugin.runner, "run_container_case", run)

    assert plugin.pytest_cmdline_main(config) == 0
    assert captured["args"] == [
        "--strict-markers",
        "-q",
        "-o",
        "addopts=",
        "--rootdir=/rucio_source",
        "-c",
        "/rucio_source/pyproject.toml",
    ]


def test_container_case_maps_custom_pytest_paths(monkeypatch) -> None:
    config = _Config(case="remote-dbs-py39-postgres14")
    config.rootpath = REPO_ROOT / "tests"
    config.inipath = REPO_ROOT / "tests" / "pytest.ini"
    captured = {}

    def run(case, root_path, pytest_args, **kwargs):
        captured.update(root=root_path, args=pytest_args)
        return 0

    monkeypatch.setattr(plugin.runner, "run_container_case", run)

    assert plugin.pytest_cmdline_main(config) == 0
    assert captured["root"] == REPO_ROOT
    assert "--rootdir=/rucio_source/tests" in captured["args"]
    assert "/rucio_source/tests/pytest.ini" in captured["args"]


def test_unit_case_uses_container_without_optional_looponfail_plugin(monkeypatch) -> None:
    config = _Config(case="unit-py39")
    config.options.pop("looponfail")
    captured = {}

    def run(case, root_path, pytest_args, **kwargs):
        captured["case"] = case
        return 0

    monkeypatch.setattr(plugin.runner, "run_unit_case", run)

    assert plugin.pytest_cmdline_main(config) == 0
    assert captured["case"].id == "unit-py39"


def test_serial_case_rejects_xdist(monkeypatch) -> None:
    config = _Config(case="remote-dbs-py39-oracle", xdist_workers=2)
    monkeypatch.setattr(plugin.runner, "run_container_case", pytest.fail)

    with pytest.raises(pytest.UsageError, match="does not support xdist"):
        plugin.pytest_cmdline_main(config)


@pytest.mark.parametrize(
    "options",
    (
        {"dist": "load", "tx": ["popen"]},
        {"looponfail": True},
    ),
)
def test_serial_case_rejects_resolved_xdist(monkeypatch, options) -> None:
    config = _Config(case="remote-dbs-py39-oracle", **options)
    monkeypatch.setattr(plugin.runner, "run_container_case", pytest.fail)

    with pytest.raises(pytest.UsageError, match="does not support xdist"):
        plugin.pytest_cmdline_main(config)


@pytest.mark.parametrize(
    "options",
    (
        {"xdist_workers": 0},
        {"dist": "no", "tx": ["popen"]},
        {"collectonly": True, "dist": "load", "tx": ["popen"]},
    ),
)
def test_serial_case_allows_inactive_xdist(monkeypatch, options) -> None:
    config = _Config(case="remote-dbs-py39-oracle", **options)
    monkeypatch.setattr(plugin.runner, "run_container_case", lambda *args, **kwargs: 0)

    assert plugin.pytest_cmdline_main(config) == 0


def test_collect_only_allows_custom_xdist_workers(monkeypatch) -> None:
    config = _Config(
        case="remote-dbs-py39-oracle",
        collectonly=True,
        xdist_workers=2,
    )
    monkeypatch.setattr(plugin.runner, "run_container_case", lambda *args, **kwargs: 0)

    assert plugin.pytest_cmdline_main(config) == 0


def test_custom_zero_workers_override_direct_workers(monkeypatch) -> None:
    config = _Config(
        case="remote-dbs-py39-oracle",
        xdist_workers=0,
        dist="load",
        tx=["popen"],
    )
    config.invocation_params = SimpleNamespace(args=(
        "--case=remote-dbs-py39-oracle",
        "-n2",
        "--xdist-workers=0",
    ))
    captured = {}

    def run(case, root_path, pytest_args, **kwargs):
        captured["args"] = pytest_args
        return 0

    monkeypatch.setattr(plugin.runner, "run_container_case", run)

    assert plugin.pytest_cmdline_main(config) == 0
    assert captured["args"][-2:] == ["-n", "0"]


def test_custom_workers_precede_separator(monkeypatch) -> None:
    config = _Config(case="unit-py39", xdist_workers=2)
    config.args = ["-literal"]
    config.args_source = config.ArgsSource.ARGS
    config.invocation_params = SimpleNamespace(args=(
        "--case=unit-py39",
        "--xdist-workers=2",
        "--",
        "-literal",
    ))
    captured = {}

    def run(case, root_path, pytest_args, **kwargs):
        captured["args"] = pytest_args
        return 0

    monkeypatch.setattr(plugin.runner, "run_unit_case", run)

    assert plugin.pytest_cmdline_main(config) == 0
    marker = captured["args"].index("--")
    assert captured["args"][marker + 1:] == ["-literal"]
    assert captured["args"].index("-n") < marker


def test_custom_zero_workers_preserve_explicit_transaction(monkeypatch) -> None:
    config = _Config(
        case="remote-dbs-py39-oracle",
        xdist_workers=0,
        dist="load",
        tx=["popen"],
    )
    config.invocation_params = SimpleNamespace(args=(
        "--case=remote-dbs-py39-oracle",
        "--dist=load",
        "--tx=popen",
        "--xdist-workers=0",
    ))
    monkeypatch.setattr(plugin.runner, "run_container_case", pytest.fail)

    with pytest.raises(pytest.UsageError, match="does not support xdist"):
        plugin.pytest_cmdline_main(config)


@pytest.mark.parametrize("argument", ("-df", "-fd"))
def test_clustered_looponfail_is_normalized(monkeypatch, argument) -> None:
    config = _Config(
        case="unit-py39",
        dist="load",
        distload=True,
        looponfail=True,
    )
    config.invocation_params = SimpleNamespace(args=(
        "--case=unit-py39",
        argument,
    ))
    captured = {}

    def run(case, root_path, pytest_args, **kwargs):
        captured["args"] = pytest_args
        return 0

    monkeypatch.setattr(plugin.runner, "run_unit_case", run)

    assert plugin.pytest_cmdline_main(config) == 0
    assert "--looponfail" in captured["args"]
    assert "-d" in captured["args"]


def test_multi_vo_case_rejects_looponfail(monkeypatch) -> None:
    config = _Config(case="multi-vo-py39-postgres14", looponfail=True)
    config.invocation_params = SimpleNamespace(args=(
        "--case=multi-vo-py39-postgres14",
        "-f",
    ))
    monkeypatch.setattr(plugin.runner, "run_container_case", pytest.fail)

    with pytest.raises(pytest.UsageError, match="multiple legs"):
        plugin.pytest_cmdline_main(config)


def test_multi_case_suite_rejects_looponfail(monkeypatch) -> None:
    config = _Config(suite="client", looponfail=True)
    config.invocation_params = SimpleNamespace(args=("--suite=client", "-f"))
    monkeypatch.setattr(plugin.runner, "run_container_case", pytest.fail)

    with pytest.raises(pytest.UsageError, match="single-session test case"):
        plugin.pytest_cmdline_main(config)


def test_all_runs_every_case_and_reports_failures(monkeypatch, capsys) -> None:
    config = _Config(suite="all")
    unit_cases = []
    container_cases = []

    def run_unit(case, *args, **kwargs):
        unit_cases.append(case.id)
        return int(case.id == "unit-py310")

    def run_container(case, *args, **kwargs):
        container_cases.append(case.id)
        return 0

    monkeypatch.setattr(plugin.runner, "run_unit_case", run_unit)
    monkeypatch.setattr(plugin.runner, "run_container_case", run_container)

    assert plugin.pytest_cmdline_main(config) == 1
    assert len(unit_cases) == 5
    assert len(container_cases) == 35
    assert "Failed cases: unit-py310" in capsys.readouterr().out


def test_suite_runs_every_matching_case(monkeypatch) -> None:
    config = _Config(suite="client")
    cases = []

    def run(case, *args, **kwargs):
        cases.append(case.id)
        return 0

    monkeypatch.setattr(plugin.runner, "run_container_case", run)

    assert plugin.pytest_cmdline_main(config) == 0
    assert cases == [
        f"client-py{python.replace('.', '')}-postgres14"
        for python in SUPPORTED_PYTHON_VERSIONS
    ]


def test_suite_runs_cases_concurrently(monkeypatch) -> None:
    config = _Config(suite="client", case_workers=5)
    barrier = threading.Barrier(5)
    cases = []
    log_output = []

    def run(case, *args, **kwargs):
        cases.append(case.id)
        log_output.append(kwargs["log_output"])
        barrier.wait(timeout=5)
        return 0

    monkeypatch.setattr(plugin.runner, "run_container_case", run)

    assert plugin.pytest_cmdline_main(config) == 0
    assert set(cases) == {
        f"client-py{python.replace('.', '')}-postgres14"
        for python in SUPPORTED_PYTHON_VERSIONS
    }
    assert log_output == [True] * 5


def test_parallel_exitfirst_does_not_submit_pending_cases(monkeypatch) -> None:
    config = _Config(suite="remote_dbs", case_workers=2, maxfail=1)
    config.invocation_params = SimpleNamespace(args=("--suite=remote_dbs", "-x"))
    cases = []

    class Future:
        def __init__(self, result):
            self.result_value = result
            self.was_cancelled = False

        def cancel(self):
            self.was_cancelled = True

        def cancelled(self):
            return self.was_cancelled

        def result(self):
            return self.result_value

    class Executor:
        def __init__(self, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def submit(self, function, *args):
            return Future(function(*args))

    def run(case, *args, **kwargs):
        cases.append(case.id)
        return int(len(cases) == 1)

    monkeypatch.setattr(plugin, "ThreadPoolExecutor", Executor)
    monkeypatch.setattr(
        plugin,
        "wait",
        lambda pending, **kwargs: ({next(iter(pending))}, set()),
    )
    monkeypatch.setattr(plugin.runner, "run_container_case", run)

    assert plugin.pytest_cmdline_main(config) == 1
    assert cases == [
        "remote-dbs-py39-oracle",
        "remote-dbs-py39-postgres14",
    ]


def test_suite_continues_after_infrastructure_failure(monkeypatch, capsys) -> None:
    config = _Config(suite="client")
    cases = []

    def run(case, *args, **kwargs):
        cases.append(case.id)
        if len(cases) == 1:
            raise RuntimeError("compose failed")
        return 0

    monkeypatch.setattr(plugin.runner, "run_container_case", run)

    assert plugin.pytest_cmdline_main(config) == 1
    assert cases == [
        f"client-py{python.replace('.', '')}-postgres14"
        for python in SUPPORTED_PYTHON_VERSIONS
    ]
    assert "compose failed" in capsys.readouterr().err


def test_suite_skips_cases_without_matching_tests(monkeypatch) -> None:
    config = _Config(suite="client")
    results = iter((
        pytest.ExitCode.NO_TESTS_COLLECTED,
        pytest.ExitCode.OK,
        pytest.ExitCode.OK,
        pytest.ExitCode.OK,
        pytest.ExitCode.OK,
    ))

    monkeypatch.setattr(
        plugin.runner,
        "run_container_case",
        lambda *args, **kwargs: next(results),
    )

    assert plugin.pytest_cmdline_main(config) == pytest.ExitCode.OK


def test_suite_reports_when_no_case_matches(monkeypatch) -> None:
    config = _Config(suite="client")
    monkeypatch.setattr(
        plugin.runner,
        "run_container_case",
        lambda *args, **kwargs: pytest.ExitCode.NO_TESTS_COLLECTED,
    )

    assert (
        plugin.pytest_cmdline_main(config)
        == pytest.ExitCode.NO_TESTS_COLLECTED
    )


def test_suite_exitfirst_stops_after_failed_case(monkeypatch) -> None:
    config = _Config(suite="client", maxfail=1)
    config.invocation_params = SimpleNamespace(args=("--suite=client", "-x"))
    cases = []

    def run(case, *args, **kwargs):
        cases.append(case.id)
        return 1

    monkeypatch.setattr(plugin.runner, "run_container_case", run)

    assert plugin.pytest_cmdline_main(config) == 1
    assert cases == ["client-py39-postgres14"]


def test_suite_combines_coverage(monkeypatch) -> None:
    config = _Config(suite="client")
    config.invocation_params = SimpleNamespace(args=(
        "--suite=client",
        "--cov=lib/rucio",
        "--cov-report=xml:test-results/coverage.xml",
        "--cov-fail-under=80",
    ))
    arguments = []

    def run(case, root_path, pytest_args, **kwargs):
        arguments.append(pytest_args)
        return 0

    monkeypatch.setattr(plugin.runner, "run_container_case", run)

    assert plugin.pytest_cmdline_main(config) == 0
    assert "--cov-append" not in arguments[0]
    assert "--cov-fail-under=0" in arguments[0]
    assert all("--cov-append" in case_args for case_args in arguments[1:])
    assert all(
        "--cov-fail-under=0" in case_args
        for case_args in arguments[:-1]
    )
    assert "--cov-fail-under=0" not in arguments[-1]
    assert all(
        "--cov-report=xml:test-results/coverage.xml" in case_args
        for case_args in arguments
    )


@pytest.mark.parametrize("argument", ("--cov=lib/rucio", "--pdb", "-f"))
def test_parallel_suite_rejects_shared_output(argument, monkeypatch) -> None:
    config = _Config(suite="client", case_workers=2)
    config.invocation_params = SimpleNamespace(args=(
        "--suite=client",
        "--case-workers=2",
        argument,
    ))
    monkeypatch.setattr(plugin.runner, "run_container_case", pytest.fail)

    with pytest.raises(pytest.UsageError):
        plugin.pytest_cmdline_main(config)


def test_case_workers_require_positive_suite_selection() -> None:
    with pytest.raises(pytest.UsageError, match="at least 1"):
        plugin.resolve_requested_cases(
            _Config(suite="client", case_workers=0),
            REPO_ROOT,
        )
    with pytest.raises(pytest.UsageError, match="requires --suite"):
        plugin.resolve_requested_cases(
            _Config(case="unit-py39", case_workers=2),
            REPO_ROOT,
        )
    with pytest.raises(pytest.UsageError, match="cannot be negative"):
        plugin.resolve_requested_cases(
            _Config(case="unit-py39", xdist_workers=-1),
            REPO_ROOT,
        )


@pytest.mark.parametrize(
    "options",
    (
        {"case": "remote-dbs-py310-postgres14", "python_version": "3.13"},
        {"case": "remote-dbs-py310-postgres14", "rdbms": "postgres14"},
    ),
)
def test_explicit_axes_require_a_suite(options: dict) -> None:
    with pytest.raises(pytest.UsageError, match="require --suite"):
        plugin.resolve_requested_cases(_Config(**options), REPO_ROOT)


@pytest.mark.parametrize("axis", ("rdbms", "policy"))
def test_all_rejects_topology_axes(axis: str) -> None:
    value = "postgres14" if axis == "rdbms" else "atlas"

    with pytest.raises(pytest.UsageError, match="cannot be combined"):
        plugin.resolve_requested_cases(
            _Config(suite="all", **{axis: value}),
            REPO_ROOT,
        )


def test_all_dry_run_json_is_machine_readable(capsys) -> None:
    config = _Config(suite="all", dry_run_json=True)

    assert plugin.pytest_cmdline_main(config) == 0

    cases = json.loads(capsys.readouterr().out)
    assert len(cases) == 40


def test_all_rejects_one_image_for_multiple_runtimes(monkeypatch) -> None:
    config = _Config(suite="all")
    monkeypatch.setenv("RUCIO_TEST_IMAGE", "runtime:one-version")

    with pytest.raises(pytest.UsageError, match="Multi-runtime suites"):
        plugin.pytest_cmdline_main(config)


def test_one_python_rejects_one_image_for_native_and_oracle(monkeypatch) -> None:
    config = _Config(suite="remote_dbs", python_version="3.13")
    monkeypatch.setenv("RUCIO_TEST_IMAGE", "runtime:one-target")

    with pytest.raises(pytest.UsageError, match="Multi-runtime suites"):
        plugin.pytest_cmdline_main(config)


def test_inner_configuration_selects_case(monkeypatch) -> None:
    config = _Config()
    python_version = f"{plugin.sys.version_info.major}.{plugin.sys.version_info.minor}"
    case_id = f"unit-py{python_version.replace('.', '')}"
    monkeypatch.setenv("RUCIO_PYTEST_INNER", "1")
    monkeypatch.setenv("RUCIO_TEST_CASE", case_id)

    plugin.pytest_configure(config)

    assert config.stash[case_key].suite == "unit"


def test_inner_configuration_reconstructs_an_effective_case(monkeypatch) -> None:
    config = _Config()
    python_version = f"{plugin.sys.version_info.major}.{plugin.sys.version_info.minor}"
    case_id = f"remote-dbs-py{python_version.replace('.', '')}-postgres14"
    monkeypatch.setenv("RUCIO_PYTEST_INNER", "1")
    monkeypatch.setenv("RUCIO_TEST_CASE", case_id)
    monkeypatch.setenv("RUCIO_TEST_SUITE", "remote_dbs")
    monkeypatch.setenv("RUCIO_TEST_PYTHON", python_version)
    monkeypatch.setenv("RUCIO_TEST_RDBMS", "postgres14")
    monkeypatch.setenv("RUCIO_TEST_POLICY", "")

    plugin.pytest_configure(config)

    case = config.stash[case_key]
    assert case.id == case_id
    assert case.python == python_version
    assert case.rdbms == "postgres14"


def test_inner_configuration_rejects_interpreter_mismatch(monkeypatch) -> None:
    config = _Config()
    runtime_python = f"{plugin.sys.version_info.major}.{plugin.sys.version_info.minor}"
    requested_python = "3.9" if runtime_python != "3.9" else "3.10"
    monkeypatch.setenv("RUCIO_PYTEST_INNER", "1")
    monkeypatch.setenv(
        "RUCIO_TEST_CASE",
        f"unit-py{requested_python.replace('.', '')}",
    )

    with pytest.raises(pytest.UsageError, match="but the container runs"):
        plugin.pytest_configure(config)


def test_inner_configuration_rejects_case_id_axis_mismatch(monkeypatch) -> None:
    config = _Config()
    python_version = f"{plugin.sys.version_info.major}.{plugin.sys.version_info.minor}"
    case_python = "3.9" if python_version != "3.9" else "3.10"
    monkeypatch.setenv("RUCIO_PYTEST_INNER", "1")
    monkeypatch.setenv(
        "RUCIO_TEST_CASE",
        f"unit-py{case_python.replace('.', '')}",
    )
    monkeypatch.setenv("RUCIO_TEST_SUITE", "unit")
    monkeypatch.setenv("RUCIO_TEST_PYTHON", python_version)

    with pytest.raises(pytest.UsageError, match="does not match configured axes"):
        plugin.pytest_configure(config)


@pytest.mark.parametrize("variable", ("RUCIO_TEST_SUITE", "RUCIO_TEST_PYTHON"))
def test_inner_configuration_requires_both_identity_axes(monkeypatch, variable: str) -> None:
    config = _Config()
    python_version = f"{plugin.sys.version_info.major}.{plugin.sys.version_info.minor}"
    monkeypatch.setenv("RUCIO_PYTEST_INNER", "1")
    monkeypatch.setenv(
        "RUCIO_TEST_CASE",
        f"unit-py{python_version.replace('.', '')}",
    )
    monkeypatch.setenv(
        variable,
        "unit" if variable == "RUCIO_TEST_SUITE" else python_version,
    )

    with pytest.raises(pytest.UsageError, match="must be set together"):
        plugin.pytest_configure(config)
