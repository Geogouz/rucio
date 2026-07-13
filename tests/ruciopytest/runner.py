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

import os
import shlex
import subprocess  # noqa: S404
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from .container_manager import ContainerManager, checkout_id

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence
    from typing import Optional

    from .profiles import TestCase


_RUNNER_FLAGS = {
    "--dry-run",
    "--dry-run-json",
    "--keep-db",
    "--list-cases",
}
_RUNNER_OPTIONS = {
    "--case",
    "--container-env",
    "--policy",
    "--suite",
    "--xdist-workers",
    "--rootdir",
    "-c",
}
_UNIT_DOCKERFILE = "etc/docker/test/unit.Dockerfile"


def outer_pytest_args(arguments: "Sequence[str]") -> list[str]:
    return [
        *shlex.split(os.environ.get("PYTEST_ADDOPTS", "")),
        *arguments,
    ]


def forwarded_pytest_args(arguments: "Sequence[str]") -> list[str]:
    forwarded = []
    skip_next = False
    for argument in arguments:
        if skip_next:
            skip_next = False
            continue
        if argument in _RUNNER_FLAGS:
            continue
        if argument in _RUNNER_OPTIONS:
            skip_next = True
            continue
        if any(argument.startswith(f"{option}=") for option in _RUNNER_OPTIONS):
            continue
        forwarded.append(argument)
    return forwarded


def run_container_case(
    case: "TestCase",
    root_dir: Path,
    pytest_args: "Sequence[str]",
    *,
    keep_db: bool = False,
    container_environment: "Optional[Mapping[str, str]]" = None,
    explicit_selectors: "Sequence[str]" = (),
) -> int:
    manager = ContainerManager(case, root_dir, keep_db=keep_db)
    manager.start()
    try:
        if case.suite == "multi_vo":
            result = _run_multi_vo(
                manager,
                case,
                pytest_args,
                keep_db,
                container_environment or {},
            )
        elif case.suite == "integration":
            result = _run_integration(
                manager,
                case,
                pytest_args,
                keep_db,
                container_environment or {},
                explicit_selectors,
            )
        else:
            arguments = list(pytest_args)
            if not explicit_selectors:
                arguments.extend(case.test_paths)
            result = _run_inner_pytest(
                manager,
                case,
                arguments,
                keep_db=keep_db,
                environment=container_environment,
            )
    except BaseException:
        manager.stop(check=False)
        raise
    manager.stop(check=result == 0)
    return result


def run_unit_case(
    case: "TestCase",
    root_dir: Path,
    pytest_args: "Sequence[str]",
    *,
    container_environment: "Optional[Mapping[str, str]]" = None,
    explicit_selectors: "Sequence[str]" = (),
) -> int:
    environment = dict(os.environ)
    environment.pop("DOCKER_DEFAULT_PLATFORM", None)
    image = (
        f"rucio-test-unit:{checkout_id(root_dir)}-py{case.python.replace('.', '')}"
    )
    build = ["docker", "buildx", "build", "--load"]
    build.extend((
        "--file",
        str(root_dir / _UNIT_DOCKERFILE),
        "--build-arg",
        f"PYTHON={case.python}",
        "--tag",
        image,
        str(root_dir),
    ))
    subprocess.run(  # noqa: S603
        build,
        check=True,
        cwd=root_dir,
        env=environment,
        timeout=1800,
    )

    command = ["docker", "run"]
    if _is_interactive(pytest_args):
        command.extend(("--interactive", "--tty"))
    command.extend((
        "--rm",
        "--volume",
        f"{root_dir.resolve()}:/rucio_source:z",
        "--workdir",
        "/rucio_source",
    ))
    inner_environment = dict(case.env_vars)
    inner_environment.update(container_environment or {})
    inner_environment.update({
        "RUCIO_PYTEST_INNER": "1",
        "RUCIO_SKIP_TEST_SETUP": "1",
        "RUCIO_TEST_CASE": case.id,
    })
    for key, value in inner_environment.items():
        command.extend(("--env", f"{key}={value}"))
    command.extend((
        image,
        "--rootdir=/rucio_source",
        "-c",
        "/rucio_source/tools/pytest.ini",
        "-r",
        "fExX",
        "--log-level=DEBUG",
        "-p",
        "rerunfailures",
    ))
    if has_xdist_option(pytest_args):
        command.extend(("-p", "xdist"))
    if _has_coverage_option(pytest_args):
        command.extend(("-p", "pytest_cov"))
    command.extend(pytest_args)
    if not explicit_selectors:
        command.extend(case.test_paths)
    return subprocess.run(  # noqa: S603
        command,
        check=False,
        cwd=root_dir,
        env=environment,
    ).returncode


def _run_multi_vo(
    manager: ContainerManager,
    case: "TestCase",
    pytest_args: "Sequence[str]",
    keep_db: bool,
    container_environment: "Mapping[str, str]",
) -> int:
    legs = ("tst", "ts2")
    for index, leg in enumerate(legs):
        environment = dict(container_environment)
        environment.update({
            "RUCIO_HOME": f"/opt/rucio/etc/multi_vo/{leg}",
            "RUCIO_MULTI_VO_LEG": leg,
        })
        arguments = qualify_junit(pytest_args, leg)
        if index:
            arguments = append_coverage(arguments)
        if index < len(legs) - 1:
            arguments = defer_coverage_threshold(arguments)
        result = _run_inner_pytest(
            manager,
            case,
            arguments,
            keep_db=keep_db or index > 0,
            environment=environment,
        )
        if result:
            return result
    return 0


def _run_integration(
    manager: ContainerManager,
    case: "TestCase",
    pytest_args: "Sequence[str]",
    keep_db: bool,
    container_environment: "Mapping[str, str]",
    explicit_selectors: "Sequence[str]",
) -> int:
    if explicit_selectors:
        selected_tpc = any("test_tpc.py" in path for path in explicit_selectors)
        selected_args = list(pytest_args)
        selected_args.append("--export-artifacts-from=test_tpc")
        result = _run_inner_pytest(
            manager,
            case,
            selected_args,
            keep_db=keep_db,
            environment=container_environment,
        )
        if result == 0:
            _verify_tpc_transfer(
                manager,
                required=selected_tpc and _executes_tests(pytest_args),
            )
        return result

    filtered = _has_collection_filter(pytest_args)
    matched = False
    for index, selector in enumerate(case.test_paths):
        arguments = qualify_junit(pytest_args, f"{index + 1:02d}")
        if index:
            arguments = append_coverage(arguments)
        if index < len(case.test_paths) - 1:
            arguments = defer_coverage_threshold(arguments)
        if "test_tpc.py" in selector:
            arguments.append("--export-artifacts-from=test_tpc")
        arguments.append(selector)
        environment = dict(container_environment)
        if index:
            environment["RUCIO_SKIP_TEST_SETUP"] = "1"
        result = _run_inner_pytest(
            manager,
            case,
            arguments,
            keep_db=keep_db or index > 0,
            environment=environment,
        )
        if result == pytest.ExitCode.NO_TESTS_COLLECTED and filtered:
            continue
        if result:
            return result
        matched = True
        if "test_tpc.py" in selector:
            _verify_tpc_transfer(
                manager,
                required=_executes_tests(pytest_args),
            )
    return 0 if matched else pytest.ExitCode.NO_TESTS_COLLECTED


def _verify_tpc_transfer(
    manager: ContainerManager,
    *,
    required: bool,
) -> None:
    exported = manager.exec(
        "rucio",
        "cat",
        "/tmp/test_tpc.artifact",
        check=False,
        capture_output=True,
    )
    if exported.returncode:
        if required:
            raise RuntimeError("TPC test did not export its FTS log path")
        return
    artifact = exported.stdout.strip()
    if not artifact:
        raise RuntimeError("TPC test did not export its FTS log path")
    manager.exec(
        "fts",
        "bash",
        "-c",
        (
            'mapfile -t files < <(compgen -G "$1"); '
            '((${#files[@]})) && grep -Fq "3rd pull" "${files[@]}"'
        ),
        "verify-tpc",
        artifact,
    )


def _run_inner_pytest(
    manager: ContainerManager,
    case: "TestCase",
    pytest_args: "Sequence[str]",
    *,
    keep_db: bool,
    environment: "Optional[Mapping[str, str]]" = None,
) -> int:
    inner_environment = dict(case.env_vars)
    inner_environment.update(environment or {})
    inner_environment.update({
        "RUCIO_PYTEST_INNER": "1",
        "RUCIO_TEST_CASE": case.id,
        "RUCIO_KEEP_TEST_DB": "1" if keep_db else "0",
    })
    if "GITHUB_ACTIONS" in manager.environment:
        inner_environment["GITHUB_ACTIONS"] = manager.environment["GITHUB_ACTIONS"]
    command = [
        "python",
        "-bb",
        "-m",
        "pytest",
        "--rootdir=/rucio_source",
        "-c",
        "/rucio_source/tools/pytest.ini",
        "-r",
        "fExX",
        "--log-level=DEBUG",
        "--ignore=tests/ruciopytest",
        "-p",
        "rerunfailures",
    ]
    if case.xdist_enabled:
        command.extend(("-p", "xdist"))
        if not has_xdist_option(pytest_args):
            workers = "3" if manager.environment.get("GITHUB_ACTIONS") == "true" else "auto"
            command.append(f"--numprocesses={workers}")
    if _has_coverage_option(pytest_args):
        command.extend(("-p", "pytest_cov"))
    command.extend(pytest_args)
    return manager.exec(
        "rucio",
        *command,
        environment=inner_environment,
        check=False,
        interactive=_is_interactive(pytest_args),
    ).returncode


def qualify_junit(arguments: "Sequence[str]", qualifier: str) -> list[str]:
    qualified = list(arguments)
    for index, argument in enumerate(qualified):
        if argument.startswith("--junitxml=") or argument.startswith("--junit-xml="):
            option, value = argument.split("=", 1)
            path = Path(value)
            qualified[index] = f"{option}={path.with_stem(f'{path.stem}-{qualifier}')}"
            break
        if argument in ("--junitxml", "--junit-xml") and index + 1 < len(qualified):
            path = Path(qualified[index + 1])
            qualified[index + 1] = str(path.with_stem(f"{path.stem}-{qualifier}"))
            break
    return qualified


def append_coverage(arguments: "Sequence[str]") -> list[str]:
    updated = list(arguments)
    if _coverage_enabled(updated) and "--cov-append" not in updated:
        updated.append("--cov-append")
    return updated


def defer_coverage_threshold(arguments: "Sequence[str]") -> list[str]:
    updated = list(arguments)
    zero_threshold = "--cov-fail-under=0" in updated or any(
        argument == "--cov-fail-under"
        and index + 1 < len(updated)
        and updated[index + 1] == "0"
        for index, argument in enumerate(updated)
    )
    if _coverage_enabled(updated) and not zero_threshold:
        updated.append("--cov-fail-under=0")
    return updated


def has_xdist_option(arguments: "Sequence[str]") -> bool:
    return any(
        argument == "-n"
        or argument.startswith("-n")
        or argument == "--numprocesses"
        or argument.startswith("--numprocesses=")
        for argument in arguments
    )


def _has_coverage_option(arguments: "Sequence[str]") -> bool:
    return any(
        argument == "--no-cov" or argument.startswith("--cov")
        for argument in arguments
    )


def _coverage_enabled(arguments: "Sequence[str]") -> bool:
    return "--no-cov" not in arguments and any(
        argument == "--cov" or argument.startswith("--cov=")
        for argument in arguments
    )


def _has_collection_filter(arguments: "Sequence[str]") -> bool:
    return any(
        argument == "-k"
        or argument.startswith("-k")
        or argument == "-m"
        or argument.startswith("-m")
        for argument in arguments
    )


def _executes_tests(arguments: "Sequence[str]") -> bool:
    info_options = {
        "--co",
        "--collect-only",
        "--fixtures",
        "--fixtures-per-test",
        "--funcargs",
        "--setup-plan",
    }
    return info_options.isdisjoint(arguments)


def _is_interactive(arguments: "Sequence[str]") -> bool:
    return any(
        argument in ("--pdb", "--trace") or argument.startswith("--pdbcls")
        for argument in arguments
    )
