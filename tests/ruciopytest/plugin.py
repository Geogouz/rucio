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
import sys
from dataclasses import replace
from typing import TYPE_CHECKING

import pytest

from . import runner
from .collection import case_key
from .profiles import SUITE_DEFINITIONS, get_case, iter_cases
from .votest_support import collect_votest_paths, load_matrix

if TYPE_CHECKING:
    from collections.abc import Sequence
    from pathlib import Path
    from typing import Optional

    from .profiles import TestCase


def pytest_addoption(parser: pytest.Parser) -> None:
    group = parser.getgroup("rucio", "Rucio test suites")
    group.addoption(
        "--suite",
        choices=(*SUITE_DEFINITIONS, "all"),
        help="Run a Rucio test suite",
    )
    group.addoption("--case", help="Run one canonical local/CI test case")
    group.addoption(
        "--list-cases",
        action="store_true",
        help="Print the canonical local/CI cases as JSON",
    )
    group.addoption(
        "--keep-db",
        action="store_true",
        help="Reuse this checkout's database volume",
    )
    group.addoption("--policy", help="Policy package for the votest suite")
    group.addoption(
        "--xdist-workers",
        type=int,
        help="Override the container pytest worker count",
    )
    group.addoption(
        "--container-env",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="Set an environment variable for container pytest",
    )
    group.addoption(
        "--dry-run",
        action="store_true",
        help="Print the selected test case without running it",
    )
    group.addoption(
        "--dry-run-json",
        action="store_true",
        help="Print the selected test case as JSON without running it",
    )


def pytest_configure(config: pytest.Config) -> None:
    if os.environ.get("RUCIO_PYTEST_INNER") != "1":
        return
    case_id = os.environ.get("RUCIO_TEST_CASE")
    if not case_id:
        raise pytest.UsageError("RUCIO_TEST_CASE is required inside the test container")
    config.stash[case_key] = _resolve_policy_paths(get_case(case_id), config.rootpath)


def pytest_sessionstart(session: pytest.Session) -> None:
    config = session.config
    if os.environ.get("RUCIO_PYTEST_INNER") != "1":
        return
    if hasattr(config, "workerinput") or os.environ.get("RUCIO_SKIP_TEST_SETUP") == "1":
        return

    from .infra_manager import InfraManager

    case = config.stash[case_key]
    InfraManager(
        case,
        keep_db=os.environ.get("RUCIO_KEEP_TEST_DB") == "1",
    ).setup()


def pytest_cmdline_main(config: pytest.Config) -> "Optional[int]":
    if os.environ.get("RUCIO_PYTEST_INNER") == "1":
        return None

    if config.getoption("list_cases"):
        print(json.dumps([
            _case_data(_resolve_policy_paths(case, config.rootpath))
            for case in iter_cases()
        ]))
        return 0

    cases = resolve_requested_cases(config, config.rootpath)
    if not cases:
        return None
    if config.getoption("suite"):
        return _run_cases(config, cases)
    case = cases[0]

    if config.getoption("dry_run") or config.getoption("dry_run_json"):
        data = _case_data(case)
        if config.getoption("dry_run_json"):
            print(json.dumps(data))
        else:
            print(f"Case: {case.id}")
            print(f"Suite: {case.suite}")
            print(f"Python: {case.python}")
            print(f"RDBMS: {case.rdbms or '-'}")
            print(f"Profiles: {', '.join(case.compose_profiles) or '-'}")
            print(f"Tests: {', '.join(case.test_paths)}")
        return 0

    outer_args = runner.outer_pytest_args(config.invocation_params.args)
    explicit_selectors = tuple(
        selector
        for selector in config.args
        if selector in outer_args
    )
    pytest_args = runner.forwarded_pytest_args(outer_args)
    workers = config.getoption("xdist_workers")
    if workers is not None:
        pytest_args.extend(("-n", str(workers)))
    if runner.has_xdist_option(pytest_args) and not case.xdist_enabled:
        raise pytest.UsageError(f"Case {case.id} does not support xdist")
    container_environment = _parse_environment(config.getoption("container_env"))
    if case.suite == "unit":
        return runner.run_unit_case(
            case,
            config.rootpath,
            pytest_args,
            container_environment=container_environment,
            explicit_selectors=explicit_selectors,
        )

    return runner.run_container_case(
        case,
        config.rootpath,
        pytest_args,
        keep_db=config.getoption("keep_db"),
        container_environment=container_environment,
        explicit_selectors=explicit_selectors,
    )


def _run_cases(
    config: pytest.Config,
    cases: "Sequence[TestCase]",
) -> int:
    if config.getoption("dry_run") or config.getoption("dry_run_json"):
        if config.getoption("dry_run_json"):
            print(json.dumps([_case_data(case) for case in cases]))
        else:
            for case in cases:
                print(case.id)
        return 0

    server_versions = {
        case.python.replace(".", "")
        for case in cases
        if case.suite != "unit"
    }
    if (
        os.environ.get("RUCIO_TEST_IMAGE")
        and len(server_versions) > 1
        and not all(
            os.environ.get(f"RUCIO_TEST_IMAGE_PY{version}")
            for version in server_versions
        )
    ):
        raise pytest.UsageError(
            "Multi-version suites require one RUCIO_TEST_IMAGE_PY<version> "
            "variable per Python version instead of RUCIO_TEST_IMAGE"
        )

    outer_args = runner.outer_pytest_args(config.invocation_params.args)
    explicit_selectors = tuple(
        selector
        for selector in config.args
        if selector in outer_args
    )
    pytest_args = runner.forwarded_pytest_args(outer_args)
    workers = config.getoption("xdist_workers")
    if workers is not None:
        pytest_args.extend(("-n", str(workers)))
    if runner.has_xdist_option(pytest_args):
        unsupported = [case.id for case in cases if not case.xdist_enabled]
        if unsupported:
            raise pytest.UsageError(
                "The selected suite cannot use xdist because some cases are serial"
            )
    container_environment = _parse_environment(config.getoption("container_env"))
    fail_fast = config.getoption("maxfail", 0) == 1
    failures = []
    for index, case in enumerate(cases):
        print(f"\n===== {case.id} =====", flush=True)
        case_args = runner.qualify_junit(pytest_args, case.id)
        if index:
            case_args = runner.append_coverage(case_args)
        if index < len(cases) - 1:
            case_args = runner.defer_coverage_threshold(case_args)
        try:
            if case.suite == "unit":
                result = runner.run_unit_case(
                    case,
                    config.rootpath,
                    case_args,
                    container_environment=container_environment,
                    explicit_selectors=explicit_selectors,
                )
            else:
                result = runner.run_container_case(
                    case,
                    config.rootpath,
                    case_args,
                    keep_db=config.getoption("keep_db"),
                    container_environment=container_environment,
                    explicit_selectors=explicit_selectors,
                )
        except Exception as error:
            print(f"Case {case.id} failed: {error}", file=sys.stderr)
            failures.append(case.id)
            if fail_fast:
                break
            continue
        if result:
            failures.append(case.id)
            if fail_fast:
                break

    if failures:
        print(f"Failed cases: {', '.join(failures)}")
        return 1
    return 0


def resolve_requested_cases(
    config: pytest.Config,
    root_path: "Path",
) -> "tuple[TestCase, ...]":
    case_id = config.getoption("case")
    suite = config.getoption("suite")
    policy = config.getoption("policy")
    if case_id and suite:
        raise pytest.UsageError("--case and --suite are mutually exclusive")
    if not case_id and not suite and any((
        config.getoption("keep_db"),
        config.getoption("dry_run"),
        config.getoption("dry_run_json"),
        config.getoption("container_env"),
        config.getoption("xdist_workers") is not None,
        policy,
    )):
        raise pytest.UsageError(
            "Rucio runner options require --case or --suite"
        )
    if case_id:
        try:
            case = get_case(case_id)
        except ValueError as error:
            raise pytest.UsageError(str(error)) from error
        if policy and case.policy != policy:
            raise pytest.UsageError(
                f"Case {case.id} uses policy {case.policy or 'none'}, not {policy}"
            )
        return (_resolve_policy_paths(case, root_path),)
    if not suite:
        return ()

    if suite == "all":
        if policy:
            raise pytest.UsageError("--policy cannot be combined with --suite=all")
        return tuple(
            _resolve_policy_paths(case, root_path)
            for case in iter_cases()
        )

    candidates = [case for case in iter_cases() if case.suite == suite]
    if policy:
        candidates = [case for case in candidates if case.policy == policy]
    if not candidates:
        raise pytest.UsageError(
            f"No canonical case matches suite={suite!r}, policy={policy!r}"
        )

    return tuple(
        _resolve_policy_paths(case, root_path)
        for case in candidates
    )


def _resolve_policy_paths(case: "TestCase", root_path: "Path") -> "TestCase":
    if not case.policy:
        return case
    matrix = load_matrix(
        root_path / "etc/docker/test/matrix_policy_package_tests.yml"
    )
    return replace(
        case,
        test_paths=tuple(collect_votest_paths(matrix, case.policy, root_path)),
    )


def _parse_environment(values: "Sequence[str]") -> dict[str, str]:
    environment = {}
    for value in values:
        key, separator, item = value.partition("=")
        if not separator or not key:
            raise pytest.UsageError(
                f"--container-env expects KEY=VALUE, received {value!r}"
            )
        environment[key] = item
    return environment


def _case_data(case: "TestCase") -> dict:
    return {
        "id": case.id,
        "group": case.group,
        "suite": case.suite,
        "python": case.python,
        "runtime": f"py{case.python.replace('.', '')}",
        "rdbms": case.rdbms,
        "policy": case.policy,
        "profiles": list(case.compose_profiles),
        "tests": list(case.test_paths),
    }
