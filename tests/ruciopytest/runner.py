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
    "--case-workers",
    "--container-env",
    "--policy",
    "--suite",
    "--xdist-workers",
    "--rootdir",
    "--config-file",
    "-c",
}
_UNIT_DOCKERFILE = "etc/docker/test/unit.Dockerfile"
_CONTAINER_SOURCE_ROOT = Path("/rucio_source")


def outer_pytest_args(
    arguments: "Sequence[str]",
    configured_addopts: "Sequence[str]" = (),
) -> list[str]:
    return [
        *configured_addopts,
        *shlex.split(os.environ.get("PYTEST_ADDOPTS", "")),
        *arguments,
    ]


def forwarded_pytest_args(arguments: "Sequence[str]") -> list[str]:
    forwarded = []
    skip_next = False
    for index, argument in enumerate(arguments):
        if skip_next:
            skip_next = False
            continue
        if argument == "--":
            forwarded.extend(arguments[index:])
            break
        if argument in _RUNNER_FLAGS:
            continue
        if argument in _RUNNER_OPTIONS:
            skip_next = True
            continue
        if any(argument.startswith(f"{option}=") for option in _RUNNER_OPTIONS):
            continue
        if argument.startswith("-c") and argument != "-c":
            continue
        forwarded.append(argument)
    marker = forwarded.index("--") if "--" in forwarded else len(forwarded)
    forwarded[marker:marker] = ("-o", "addopts=")
    return forwarded


def container_pytest_config(
    arguments: "Sequence[str]",
    source_root: Path,
    pytest_root: Path,
    config_path: "Optional[Path]",
) -> list[str]:
    source_root = source_root.resolve()
    if config_path is None:
        raise pytest.UsageError("pytest configuration file is required")

    def map_path(path: Path, description: str) -> Path:
        try:
            relative = path.resolve().relative_to(source_root)
        except ValueError as error:
            raise pytest.UsageError(
                f"pytest {description} must be inside {source_root}"
            ) from error
        return _CONTAINER_SOURCE_ROOT / relative

    updated = list(arguments)
    marker = updated.index("--") if "--" in updated else len(updated)
    updated[marker:marker] = (
        f"--rootdir={map_path(pytest_root, 'root directory')}",
        "-c",
        str(map_path(config_path, 'configuration file')),
    )
    return updated


def add_pytest_options(
    arguments: "Sequence[str]",
    *options: str,
) -> list[str]:
    updated = list(arguments)
    marker = updated.index("--") if "--" in updated else len(updated)
    updated[marker:marker] = options
    return updated


def run_container_case(
    case: "TestCase",
    root_dir: Path,
    pytest_args: "Sequence[str]",
    *,
    keep_db: bool = False,
    container_environment: "Optional[Mapping[str, str]]" = None,
    explicit_selectors: "Sequence[str]" = (),
    log_output: bool = False,
) -> int:
    pytest_args = qualify_cache_dir(pytest_args, case.id)
    manager = ContainerManager(
        case,
        root_dir,
        keep_db=keep_db,
        log_output=log_output,
    )
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
    log_output: bool = False,
) -> int:
    pytest_args = qualify_cache_dir(pytest_args, case.id)
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
    output_log = None
    if log_output:
        project_name = ContainerManager.make_project_name(
            case.id,
            root_dir,
            reusable=False,
        )
        output_log = root_dir / ".test-logs" / project_name / "case.log"
        output_log.parent.mkdir(parents=True, exist_ok=True)
        output_log.write_text("")
        print(f"Case output: {output_log}", flush=True)
    _run_unit_command(
        build,
        check=True,
        root_dir=root_dir,
        environment=environment,
        output_log=output_log,
        timeout=1800,
    )

    command = ["docker", "run"]
    if is_interactive(pytest_args):
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
        "PYTHONPATH": "/rucio_source/lib",
        "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "true",
        "RUCIO_PYTEST_INNER": "1",
        "RUCIO_SKIP_TEST_SETUP": "1",
        "RUCIO_TEST_CASE": case.id,
    })
    for key, value in inner_environment.items():
        command.extend(("--env", f"{key}={value}"))
    command.extend((
        image,
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
    return _run_unit_command(
        command,
        check=False,
        root_dir=root_dir,
        environment=environment,
        output_log=output_log,
    ).returncode


def _run_unit_command(
    command: "Sequence[str]",
    *,
    check: bool,
    root_dir: Path,
    environment: "Mapping[str, str]",
    output_log: "Optional[Path]",
    timeout: "Optional[int]" = None,
) -> subprocess.CompletedProcess:
    if output_log:
        with output_log.open("a") as output:
            return subprocess.run(  # noqa: S603
                command,
                check=check,
                cwd=root_dir,
                env=environment,
                stdout=output,
                stderr=subprocess.STDOUT,
                timeout=timeout,
            )
    return subprocess.run(  # noqa: S603
        command,
        check=check,
        cwd=root_dir,
        env=environment,
        timeout=timeout,
    )


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
        arguments = qualify_cache_dir(qualify_paths(pytest_args, leg), leg)
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
    if explicit_selectors or _requires_single_session(pytest_args):
        selected_tpc = any("test_tpc.py" in path for path in explicit_selectors)
        selected_args = list(pytest_args)
        if not explicit_selectors and not _has_cache_show(pytest_args):
            selected_args.extend(case.test_paths)
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
                required=(
                    _executes_tests(pytest_args)
                    and (
                        selected_tpc
                        or (not explicit_selectors and not _may_skip_tpc(pytest_args))
                    )
                ),
            )
        return result

    matched = False
    for index, selector in enumerate(case.test_paths):
        arguments = qualify_paths(pytest_args, f"{index + 1:02d}")
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
        if result == pytest.ExitCode.NO_TESTS_COLLECTED:
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
        interactive=is_interactive(pytest_args),
    ).returncode


def qualify_junit(arguments: "Sequence[str]", qualifier: str) -> list[str]:
    return _qualify_path_option(
        arguments,
        ("--junitxml", "--junit-xml"),
        qualifier,
    )


def qualify_paths(arguments: "Sequence[str]", qualifier: str) -> list[str]:
    qualified = qualify_junit(arguments, qualifier)
    qualified = _qualify_debug(qualified, qualifier)
    qualified = _qualify_path_option(
        qualified,
        ("--log-file",),
        qualifier,
    )
    qualified = _qualify_path_option(
        qualified,
        ("--basetemp",),
        qualifier,
        directory=True,
    )
    qualified, _ = _qualify_ini_path(
        qualified,
        "log_file",
        qualifier,
    )
    return qualified


def _qualify_debug(arguments: "Sequence[str]", qualifier: str) -> list[str]:
    qualified = list(arguments)
    for index, argument in enumerate(qualified):
        if argument.startswith("--debug="):
            value = argument.split("=", 1)[1]
            if value:
                qualified[index] = f"--debug={_qualify_path(value, qualifier, directory=False)}"
        elif argument == "--debug":
            if index + 1 < len(qualified) and not qualified[index + 1].startswith("-"):
                qualified[index + 1] = _qualify_path(
                    qualified[index + 1], qualifier, directory=False
                )
            else:
                qualified[index] = f"--debug=pytestdebug-{qualifier}.log"
    return qualified


def qualify_cache_dir(arguments: "Sequence[str]", qualifier: str) -> list[str]:
    qualified, found = _qualify_ini_path(
        arguments,
        "cache_dir",
        qualifier,
        directory=True,
    )
    if not found:
        qualified.extend((
            "-o",
            f"cache_dir=/rucio_source/.pytest_cache/rucio-cases/{qualifier}",
        ))
    return qualified


def _qualify_ini_path(
    arguments: "Sequence[str]",
    key: str,
    qualifier: str,
    *,
    directory: bool = False,
) -> tuple[list[str], bool]:
    qualified = list(arguments)
    found = False
    for index, argument in enumerate(qualified):
        if argument in ("-o", "--override-ini") and index + 1 < len(qualified):
            setting, matches = _qualify_ini_setting(
                qualified[index + 1], key, qualifier, directory=directory
            )
            qualified[index + 1] = setting
            found = found or matches
            continue
        for prefix in ("-o=", "--override-ini=", "-o"):
            if argument.startswith(prefix):
                setting, matches = _qualify_ini_setting(
                    argument[len(prefix):], key, qualifier, directory=directory
                )
                if matches:
                    qualified[index] = f"{prefix}{setting}"
                    found = True
                break
    return qualified, found


def _qualify_ini_setting(
    setting: str,
    key: str,
    qualifier: str,
    *,
    directory: bool,
) -> tuple[str, bool]:
    setting_key, separator, value = setting.partition("=")
    if setting_key != key or not separator:
        return setting, False
    return f"{key}={_qualify_path(value, qualifier, directory=directory)}", True


def _qualify_path_option(
    arguments: "Sequence[str]",
    options: "Sequence[str]",
    qualifier: str,
    *,
    directory: bool = False,
) -> list[str]:
    qualified = list(arguments)
    for index, argument in enumerate(qualified):
        for option in options:
            if argument.startswith(f"{option}="):
                value = argument.split("=", 1)[1]
                qualified[index] = (
                    f"{option}={_qualify_path(value, qualifier, directory=directory)}"
                )
                break
            if argument == option and index + 1 < len(qualified):
                qualified[index + 1] = _qualify_path(
                    qualified[index + 1], qualifier, directory=directory
                )
                break
    return qualified


def _qualify_path(value: str, qualifier: str, *, directory: bool) -> str:
    if value in ("/dev/null", "/dev/stderr", "/dev/stdout"):
        return value
    path = Path(value)
    if directory or not path.name:
        return str(path / qualifier)
    return str(path.with_stem(f"{path.stem}-{qualifier}"))


def append_coverage(arguments: "Sequence[str]") -> list[str]:
    updated = list(arguments)
    if coverage_enabled(updated) and "--cov-append" not in updated:
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
    if coverage_enabled(updated) and not zero_threshold:
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


def has_xdist_transaction(arguments: "Sequence[str]") -> bool:
    for argument in arguments:
        if argument == "--":
            return False
        if argument == "--tx" or argument.startswith("--tx="):
            return True
    return False


def explicit_xdist_mode(arguments: "Sequence[str]") -> str:
    dist = "no"
    for index, argument in enumerate(arguments):
        if argument == "--":
            break
        if argument == "--dist" and index + 1 < len(arguments):
            dist = arguments[index + 1]
        elif argument.startswith("--dist="):
            dist = argument.split("=", 1)[1]
    return dist


def _has_coverage_option(arguments: "Sequence[str]") -> bool:
    return any(
        argument == "--no-cov" or argument.startswith("--cov")
        for argument in arguments
    )


def coverage_enabled(arguments: "Sequence[str]") -> bool:
    return "--no-cov" not in arguments and any(
        argument == "--cov" or argument.startswith("--cov=")
        for argument in arguments
    )


def _executes_tests(arguments: "Sequence[str]") -> bool:
    info_options = {
        "--co",
        "--collect-only",
        "--fixtures",
        "--fixtures-per-test",
        "--funcargs",
        "--setup-only",
        "--setup-plan",
    }
    return info_options.isdisjoint(arguments) and not any(
        argument == "--cache-show" or argument.startswith("--cache-show=")
        for argument in arguments
    )


def _requires_single_session(arguments: "Sequence[str]") -> bool:
    options = {
        "--cache-clear",
        "--cache-show",
        "--failed-first",
        "--ff",
        "--last-failed",
        "--lf",
        "--new-first",
        "--nf",
        "--stepwise",
        "--stepwise-reset",
        "--stepwise-skip",
        "--sw",
        "--sw-reset",
        "--sw-skip",
    }
    return any(argument.split("=", 1)[0] in options for argument in arguments)


def _has_cache_show(arguments: "Sequence[str]") -> bool:
    return any(
        argument == "--cache-show" or argument.startswith("--cache-show=")
        for argument in arguments
    )


def _may_skip_tpc(arguments: "Sequence[str]") -> bool:
    exact_options = {
        "--deselect",
        "--ignore",
        "--ignore-glob",
        "--last-failed",
        "--lf",
        "--stepwise",
        "--stepwise-reset",
        "--stepwise-skip",
        "--sw",
        "--sw-reset",
        "--sw-skip",
        "-k",
        "-m",
    }
    return any(
        argument in exact_options
        or argument.startswith((
            "--deselect=",
            "--ignore=",
            "--ignore-glob=",
            "-k",
            "-m",
        ))
        for argument in arguments
    )


def is_interactive(arguments: "Sequence[str]") -> bool:
    return any(
        argument in ("--pdb", "--trace") or argument.startswith("--pdbcls")
        for argument in arguments
    )
