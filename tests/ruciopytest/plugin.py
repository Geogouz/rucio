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
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import replace
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from . import runner
from .collection import case_key
from .profiles import SUITE_DEFINITIONS, get_case, iter_cases
from .votest_support import collect_votest_paths, load_matrix

if TYPE_CHECKING:
    from collections.abc import Sequence
    from typing import Optional

    from .profiles import TestCase


SOURCE_ROOT = Path(__file__).resolve().parents[2]


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
        "--case-workers",
        type=int,
        help="Run up to this many suite cases concurrently",
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
    config.stash[case_key] = _resolve_policy_paths(get_case(case_id), SOURCE_ROOT)


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
            _case_data(_resolve_policy_paths(case, SOURCE_ROOT))
            for case in iter_cases()
        ]))
        return 0

    cases = resolve_requested_cases(config, SOURCE_ROOT)
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

    outer_args = runner.outer_pytest_args(
        config.invocation_params.args,
        config.getini("addopts"),
    )
    explicit_selectors = _explicit_selectors(config)
    pytest_args = runner.forwarded_pytest_args(outer_args)
    pytest_args = runner.container_pytest_config(
        pytest_args,
        SOURCE_ROOT,
        config.rootpath,
        config.inipath,
    )
    workers = config.getoption("xdist_workers")
    pytest_args = _normalize_xdist_args(config, pytest_args, workers)
    if config.getoption("looponfail") and case.suite == "multi_vo":
        raise pytest.UsageError(
            "Loop-on-fail cannot run a multi-VO case with multiple legs"
        )
    if _xdist_is_active(config, workers, pytest_args) and not case.xdist_enabled:
        raise pytest.UsageError(f"Case {case.id} does not support xdist")
    container_environment = _parse_environment(config.getoption("container_env"))
    if case.suite == "unit":
        return runner.run_unit_case(
            case,
            SOURCE_ROOT,
            pytest_args,
            container_environment=container_environment,
            explicit_selectors=explicit_selectors,
        )

    return runner.run_container_case(
        case,
        SOURCE_ROOT,
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

    outer_args = runner.outer_pytest_args(
        config.invocation_params.args,
        config.getini("addopts"),
    )
    explicit_selectors = _explicit_selectors(config)
    pytest_args = runner.forwarded_pytest_args(outer_args)
    pytest_args = runner.container_pytest_config(
        pytest_args,
        SOURCE_ROOT,
        config.rootpath,
        config.inipath,
    )
    workers = config.getoption("xdist_workers")
    pytest_args = _normalize_xdist_args(config, pytest_args, workers)
    if config.getoption("looponfail") and (
        len(cases) > 1 or cases[0].suite == "multi_vo"
    ):
        raise pytest.UsageError("Loop-on-fail requires a single-session test case")
    if _xdist_is_active(config, workers, pytest_args):
        unsupported = [case.id for case in cases if not case.xdist_enabled]
        if unsupported:
            raise pytest.UsageError(
                "The selected suite cannot use xdist because some cases are serial"
            )
    container_environment = _parse_environment(config.getoption("container_env"))
    worker_count = min(config.getoption("case_workers") or 1, len(cases))
    if worker_count > 1 and runner.coverage_enabled(pytest_args):
        raise pytest.UsageError("Coverage cannot run with multiple case workers")
    if worker_count > 1 and runner.is_interactive(pytest_args):
        raise pytest.UsageError(
            "Interactive debugging cannot run with multiple case workers"
        )
    fail_fast = config.getoption("maxfail", 0) == 1
    failed_cases = set()
    empty_cases = set()

    def run_case(index: int, case: "TestCase") -> int:
        print(f"\n===== {case.id} =====", flush=True)
        case_args = runner.qualify_paths(pytest_args, case.id)
        if index:
            case_args = runner.append_coverage(case_args)
        if index < len(cases) - 1:
            case_args = runner.defer_coverage_threshold(case_args)
        try:
            if case.suite == "unit":
                result = runner.run_unit_case(
                    case,
                    SOURCE_ROOT,
                    case_args,
                    container_environment=container_environment,
                    explicit_selectors=explicit_selectors,
                    log_output=worker_count > 1,
                )
            else:
                result = runner.run_container_case(
                    case,
                    SOURCE_ROOT,
                    case_args,
                    keep_db=config.getoption("keep_db"),
                    container_environment=container_environment,
                    explicit_selectors=explicit_selectors,
                    log_output=worker_count > 1,
                )
        except Exception as error:
            print(f"Case {case.id} failed: {error}", file=sys.stderr)
            return pytest.ExitCode.TESTS_FAILED
        return result

    if worker_count == 1:
        for index, case in enumerate(cases):
            result = run_case(index, case)
            if result == pytest.ExitCode.NO_TESTS_COLLECTED:
                empty_cases.add(case.id)
            elif result:
                failed_cases.add(case.id)
                if fail_fast:
                    break
    else:
        indexed_cases = iter(enumerate(cases))
        pending = {}
        stopping = False
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            while True:
                while not stopping and len(pending) < worker_count:
                    try:
                        index, case = next(indexed_cases)
                    except StopIteration:
                        break
                    pending[executor.submit(run_case, index, case)] = case
                if not pending:
                    break
                completed, _ = wait(pending, return_when=FIRST_COMPLETED)
                for future in completed:
                    case = pending.pop(future)
                    if future.cancelled():
                        continue
                    result = future.result()
                    if result == pytest.ExitCode.NO_TESTS_COLLECTED:
                        empty_cases.add(case.id)
                    elif result:
                        failed_cases.add(case.id)
                        stopping = stopping or fail_fast
                if stopping:
                    for future in pending:
                        future.cancel()

    failures = [case.id for case in cases if case.id in failed_cases]
    if failures:
        print(f"Failed cases: {', '.join(failures)}")
        return 1
    if len(empty_cases) == len(cases):
        return pytest.ExitCode.NO_TESTS_COLLECTED
    return 0


def resolve_requested_cases(
    config: pytest.Config,
    root_path: "Path",
) -> "tuple[TestCase, ...]":
    case_id = config.getoption("case")
    suite = config.getoption("suite")
    policy = config.getoption("policy")
    case_workers = config.getoption("case_workers")
    xdist_workers = config.getoption("xdist_workers")
    if case_workers is not None and case_workers < 1:
        raise pytest.UsageError("--case-workers must be at least 1")
    if xdist_workers is not None and xdist_workers < 0:
        raise pytest.UsageError("--xdist-workers cannot be negative")
    if case_id and suite:
        raise pytest.UsageError("--case and --suite are mutually exclusive")
    if case_id and case_workers is not None:
        raise pytest.UsageError("--case-workers requires --suite")
    if not case_id and not suite and any((
        config.getoption("keep_db"),
        config.getoption("dry_run"),
        config.getoption("dry_run_json"),
        config.getoption("container_env"),
        config.getoption("xdist_workers") is not None,
        case_workers is not None,
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


def _explicit_selectors(config: pytest.Config) -> tuple[str, ...]:
    if config.args_source is config.ArgsSource.ARGS:
        return tuple(config.args)
    return ()


def _normalize_xdist_args(
    config: pytest.Config,
    pytest_args: "Sequence[str]",
    workers: "Optional[int]",
) -> list[str]:
    options = []
    if config.getoption("distload", False):
        options.append("-d")
    if config.getoption("looponfail", False):
        options.append("--looponfail")
    if workers is not None:
        options.extend(("-n", str(workers)))
    return runner.add_pytest_options(pytest_args, *options)


def _xdist_is_active(
    config: pytest.Config,
    workers: "Optional[int]",
    pytest_args: "Sequence[str]",
) -> bool:
    if config.getoption("looponfail", False):
        return True
    if config.getoption("collectonly", False):
        return False
    if workers is None:
        return (
            config.getoption("dist", "no") != "no"
            and bool(config.getoption("tx", ()))
        )
    if workers > 0:
        return True
    return runner.has_xdist_transaction(pytest_args) and (
        config.getoption("distload", False)
        or runner.explicit_xdist_mode(pytest_args) != "no"
    )


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
