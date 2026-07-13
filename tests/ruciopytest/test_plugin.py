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
from pathlib import Path
from types import SimpleNamespace

import pytest

from tests.ruciopytest import plugin
from tests.ruciopytest.collection import case_key

REPO_ROOT = Path(__file__).resolve().parents[2]


class _Config:
    def __init__(self, **options) -> None:
        defaults = {
            "case": None,
            "suite": None,
            "policy": None,
            "list_cases": False,
            "dry_run": False,
            "dry_run_json": False,
            "xdist_workers": None,
            "container_env": [],
            "keep_db": False,
        }
        defaults.update(options)
        self.options = defaults
        self.rootpath = REPO_ROOT
        self.args = ["tests"]
        self.invocation_params = SimpleNamespace(args=())
        self.stash = pytest.Stash()

    def getoption(self, name, default=None):
        return self.options.get(name.lstrip("-"), default)


def test_list_cases_is_machine_readable(capsys) -> None:
    config = _Config(list_cases=True)

    assert plugin.pytest_cmdline_main(config) == 0

    cases = json.loads(capsys.readouterr().out)
    assert len(cases) == 15
    assert {case["id"] for case in cases} >= {
        "unit-py39",
        "remote-dbs-py310-oracle",
        "votest-py39-postgres14-atlas",
        "integration-py39-postgres14",
    }
    atlas = next(case for case in cases if case["policy"] == "atlas")
    assert len(atlas["tests"]) == 36
    assert {case["runtime"] for case in cases} == {
        "py39",
        "py310",
        "py311",
        "py312",
    }


def test_suite_resolves_every_matrix_case() -> None:
    cases = plugin.resolve_requested_cases(_Config(suite="remote_dbs"), REPO_ROOT)

    assert {case.id for case in cases} == {
        "remote-dbs-py39-oracle",
        "remote-dbs-py39-postgres14",
        "remote-dbs-py310-oracle",
        "remote-dbs-py310-postgres14",
    }


def test_suite_selection_ignores_legacy_environment(monkeypatch) -> None:
    monkeypatch.setenv("PYTHON", "3.9")
    monkeypatch.setenv("RDBMS", "oracle")
    monkeypatch.setenv("POLICY", "atlas")

    cases = plugin.resolve_requested_cases(_Config(suite="remote_dbs"), REPO_ROOT)

    assert len(cases) == 4


def test_votest_suite_resolves_every_policy() -> None:
    cases = plugin.resolve_requested_cases(_Config(suite="votest"), REPO_ROOT)

    assert {case.policy for case in cases} == {"atlas", "belleii"}


def test_votest_resolves_policy_selectors() -> None:
    cases = plugin.resolve_requested_cases(
        _Config(suite="votest", policy="atlas"),
        REPO_ROOT,
    )

    assert len(cases) == 1
    case = cases[0]
    assert case.policy == "atlas"
    assert len(case.test_paths) == 36


def test_dry_run_does_not_start_containers(monkeypatch, capsys) -> None:
    config = _Config(case="remote-dbs-py39-postgres14", dry_run=True)
    monkeypatch.setattr(plugin.runner, "run_container_case", pytest.fail)

    assert plugin.pytest_cmdline_main(config) == 0
    assert "remote-dbs-py39-postgres14" in capsys.readouterr().out


def test_container_case_forwards_standard_pytest_args(monkeypatch) -> None:
    config = _Config(case="remote-dbs-py39-postgres14")
    config.args = ["tests/test_rule.py::test_rule"]
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
    ]
    assert captured["kwargs"]["explicit_selectors"] == (
        "tests/test_rule.py::test_rule",
    )


def test_unit_case_uses_container(monkeypatch) -> None:
    config = _Config(case="unit-py39")
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
    assert len(unit_cases) == 4
    assert len(container_cases) == 11
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
        "client-py39-postgres14",
        "client-py310-postgres14",
    ]


def test_all_dry_run_json_is_machine_readable(capsys) -> None:
    config = _Config(suite="all", dry_run_json=True)

    assert plugin.pytest_cmdline_main(config) == 0

    cases = json.loads(capsys.readouterr().out)
    assert len(cases) == 15


def test_all_rejects_one_runtime_for_two_python_versions(monkeypatch) -> None:
    config = _Config(suite="all")
    monkeypatch.setenv("RUCIO_TEST_IMAGE", "runtime:one-version")

    with pytest.raises(pytest.UsageError, match="Multi-version suites"):
        plugin.pytest_cmdline_main(config)


def test_inner_configuration_selects_case(monkeypatch) -> None:
    config = _Config()
    monkeypatch.setenv("RUCIO_PYTEST_INNER", "1")
    monkeypatch.setenv("RUCIO_TEST_CASE", "client-py39-postgres14")

    plugin.pytest_configure(config)

    assert config.stash[case_key].suite == "client"
