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
import subprocess  # noqa: S404
from pathlib import Path
from typing import TYPE_CHECKING

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
    with ContainerManager(case, root_dir, keep_db=keep_db) as manager:
        if case.suite == "multi_vo":
            return _run_multi_vo(
                manager,
                case,
                pytest_args,
                keep_db,
                container_environment or {},
            )
        if case.suite == "integration":
            return _run_integration(
                manager,
                case,
                pytest_args,
                keep_db,
                container_environment or {},
                explicit_selectors,
            )
        arguments = list(pytest_args)
        if not explicit_selectors:
            arguments.extend(case.test_paths)
        return _run_inner_pytest(
            manager,
            case,
            arguments,
            keep_db=keep_db,
            environment=container_environment,
        )


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
    runtime = "podman" if environment.get("USE_PODMAN") == "1" else "docker"
    image = (
        f"rucio-test-unit:{checkout_id(root_dir)}-py{case.python.replace('.', '')}"
    )
    if runtime == "docker":
        build = ["docker", "buildx", "build", "--load"]
    else:
        build = ["podman", "build"]
    build.extend((
        "--file",
        str(root_dir / _UNIT_DOCKERFILE),
        "--build-arg",
        f"PYTHON={case.python}",
        "--tag",
        image,
        str(root_dir),
    ))
    subprocess.run(build, check=True, cwd=root_dir, env=environment)  # noqa: S603

    command = [runtime, "run"]
    if _is_interactive(pytest_args):
        command.extend(("--interactive", "--tty"))
    command.extend((
        "--rm",
        "--volume",
        f"{root_dir.resolve()}:/rucio_source",
        "--workdir",
        "/rucio_source",
    ))
    for key, value in (container_environment or {}).items():
        command.extend(("--env", f"{key}={value}"))
    command.extend((
        image,
        "--rootdir=/rucio_source",
        "-c",
        "/rucio_source/tools/pytest.ini",
        "-r",
        "fExX",
        "--log-level=DEBUG",
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
    for index, leg in enumerate(("tst", "ts2")):
        environment = dict(container_environment)
        environment.update({
            "RUCIO_HOME": f"/opt/rucio/etc/multi_vo/{leg}",
            "RUCIO_MULTI_VO_LEG": leg,
        })
        result = _run_inner_pytest(
            manager,
            case,
            qualify_junit(pytest_args, leg),
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
        if selected_tpc:
            selected_args.append("--export-artifacts-from=test_tpc")
        result = _run_inner_pytest(
            manager,
            case,
            selected_args,
            keep_db=keep_db,
            environment=container_environment,
        )
        if result == 0 and selected_tpc:
            _verify_tpc_transfer(manager)
        return result

    for index, selector in enumerate(case.test_paths):
        arguments = [
            *qualify_junit(pytest_args, f"{index + 1:02d}"),
            selector,
        ]
        if "test_tpc.py" in selector:
            arguments.insert(-1, "--export-artifacts-from=test_tpc")
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
        if result:
            return result
        if "test_tpc.py" in selector:
            _verify_tpc_transfer(manager)
    return 0


def _verify_tpc_transfer(manager: ContainerManager) -> None:
    artifact = manager.exec(
        "rucio",
        "cat",
        "/tmp/test_tpc.artifact",
        capture_output=True,
    ).stdout.strip()
    if not artifact:
        raise RuntimeError("TPC test did not export its FTS log path")
    manager.exec("fts", "grep", "-Fq", "3rd pull", artifact)


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


def _is_interactive(arguments: "Sequence[str]") -> bool:
    return any(
        argument in ("--pdb", "--trace") or argument.startswith("--pdbcls")
        for argument in arguments
    )
