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
from contextlib import nullcontext
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from tools.devenv.artifacts import (
    CONTAINER_ARTIFACT_ROOT,
    DockerStatsSampler,
    artifact_ownership_command,
    artifact_ownership_environment,
)
from tools.devenv.build_context import temporary_docker_context

from .container_manager import ContainerManager, checkout_id, open_test_log_file

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
    "--python",
    "--rdbms",
    "--suite",
    "--xdist-workers",
    "--rootdir",
    "--config-file",
    "-c",
}
_UNIT_DOCKERFILE = Path("etc/docker/test/unit.Dockerfile")
_UNIT_BUILD_INPUTS = (
    _UNIT_DOCKERFILE,
    Path("requirements/requirements.dev.txt"),
    Path("requirements/requirements.devenv.txt"),
)
_CONTAINER_SOURCE_ROOT = Path("/rucio_source")
_CONTAINER_ARTIFACT_ROOT = Path(CONTAINER_ARTIFACT_ROOT)


def _case_environment(case: "TestCase") -> dict[str, str]:
    return {
        "RUCIO_TEST_CASE": case.id,
        "RUCIO_TEST_SUITE": case.suite,
        "RUCIO_TEST_PYTHON": case.python,
        "RUCIO_TEST_RDBMS": case.rdbms,
        "RUCIO_TEST_POLICY": case.policy,
    }


def outer_pytest_args(
    arguments: "Sequence[str]",
    configured_addopts: "Sequence[str]" = (),
) -> list[str]:
    return [
        *configured_addopts,
        *shlex.split(os.environ.get("PYTEST_ADDOPTS", "")),
        *arguments,
    ]


def _separator_index(arguments: "Sequence[str]") -> int:
    for index, argument in enumerate(arguments):
        if argument == "--":
            return index
    return len(arguments)


def _options_before_separator(arguments: "Sequence[str]") -> "Sequence[str]":
    return arguments[:_separator_index(arguments)]


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
    marker = _separator_index(forwarded)
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
        lexical_path = Path(os.path.abspath(path))
        try:
            relative = lexical_path.relative_to(source_root)
            lexical_path.resolve().relative_to(source_root)
        except ValueError as error:
            raise pytest.UsageError(
                f"pytest {description} must be inside {source_root}"
            ) from error
        return _CONTAINER_SOURCE_ROOT / relative

    updated = list(arguments)
    marker = _separator_index(updated)
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
    marker = _separator_index(updated)
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
        with _resource_sampler(root_dir, manager.project_name):
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


def _resource_sampler(root_dir: Path, project_name: str):
    configured = os.environ.get("RUCIO_DEV_STATS_PATH", "").strip()
    if not configured:
        return nullcontext()
    output = Path(configured)
    artifact_root = (root_dir / ".rucio-dev/artifacts").resolve()
    try:
        output.resolve().relative_to(artifact_root)
    except ValueError as error:
        raise pytest.UsageError(
            f"RUCIO_DEV_STATS_PATH must be below {artifact_root}"
        ) from error
    return DockerStatsSampler(
        root_dir,
        project_name,
        output,
        interval_seconds=0.2,
    )


def _development_mode(
    environment: "Optional[Mapping[str, str]]",
) -> "tuple[str, bool, bool, bool, Path, str]":
    values = environment or {}
    profile = values.get("RUCIO_DEV_PROFILE", "").strip()
    idle_value = values.get("RUCIO_DEV_PROFILE_IDLE", "").strip()
    if idle_value not in {"", "0", "1"}:
        raise pytest.UsageError("RUCIO_DEV_PROFILE_IDLE must be 0 or 1")
    profile_idle = idle_value == "1"
    debug = values.get("RUCIO_DEV_DEBUG") == "1"
    observe_value = values.get("RUCIO_DEV_OBSERVE", "").strip()
    if observe_value not in {"", "0", "1"}:
        raise pytest.UsageError("RUCIO_DEV_OBSERVE must be 0 or 1")
    observe = observe_value == "1"
    if sum((bool(profile), debug, observe)) > 1:
        raise pytest.UsageError("Profiling, debugging, and observation cannot be combined")
    if profile not in {"", "cpu", "memory", "calls"}:
        raise pytest.UsageError("RUCIO_DEV_PROFILE must be one of: cpu, memory, calls")
    if profile_idle and profile != "cpu":
        raise pytest.UsageError("RUCIO_DEV_PROFILE_IDLE is only supported for CPU profiles")

    artifact_dir = values.get("RUCIO_DEV_ARTIFACT_DIR", "").strip()
    artifact_path = Path(artifact_dir)
    if profile:
        if not artifact_dir:
            raise pytest.UsageError(
                "RUCIO_DEV_ARTIFACT_DIR is required when RUCIO_DEV_PROFILE is set"
            )
        if (
            not artifact_path.is_absolute()
            or artifact_path == _CONTAINER_ARTIFACT_ROOT
            or not artifact_path.is_relative_to(_CONTAINER_ARTIFACT_ROOT)
            or ".." in artifact_path.parts
        ):
            raise pytest.UsageError(
                "RUCIO_DEV_ARTIFACT_DIR must be a safe absolute container path"
            )

    port = values.get("RUCIO_DEBUG_PORT", "5678")
    if debug and (not port.isdigit() or not 1 <= int(port) <= 65535):
        raise pytest.UsageError("RUCIO_DEBUG_PORT must be a valid TCP port")
    return profile, profile_idle, debug, observe, artifact_path, port


def _instrument_pytest(
    command: "Sequence[str]",
    *,
    profile: str,
    profile_idle: bool,
    debug: bool,
    observe: bool,
    observe_attributes: "Sequence[tuple[str, str]]",
    artifact_path: Path,
    debug_port: str,
) -> "tuple[list[str], Optional[str], Optional[str], Optional[str], Optional[str]]":
    memory_capture = None
    memory_report = None
    calls_capture = None
    calls_report = None
    if profile == "cpu":
        command = [
            "py-spy",
            "record",
            "--format",
            "flamegraph",
            "--subprocesses",
            *(("--idle",) if profile_idle else ()),
            "--output",
            str(artifact_path / "cpu.svg"),
            "--",
            *command,
        ]
    elif profile == "memory":
        memory_capture = str(artifact_path / "memory.bin")
        memory_report = str(artifact_path / "memory.html")
        command = [
            "python",
            "-bb",
            "-m",
            "memray",
            "run",
            "--force",
            "--follow-fork",
            "--output",
            memory_capture,
            "-m",
            "pytest",
            *command[4:],
        ]
    elif profile == "calls":
        calls_capture = str(artifact_path / "calls.pstats")
        calls_report = str(artifact_path / "calls.html")
        command = [
            "python",
            "-m",
            "tools.devenv.calls",
            "--output",
            calls_capture,
            "--",
            *command,
        ]
    elif debug:
        command = [
            "python",
            "-bb",
            "-m",
            "debugpy",
            "--listen",
            f"0.0.0.0:{debug_port}",
            "--wait-for-client",
            "-m",
            "pytest",
            *command[4:],
        ]
    elif observe:
        observed = [
            "opentelemetry-instrument",
            "--service_name",
            "rucio-test-client",
            "--",
            "python",
            "-m",
            "tools.devenv.observe",
            "--span-name",
            "rucio-test.run",
        ]
        for key, value in observe_attributes:
            observed.extend(("--attribute", f"{key}={value}"))
        command = [*observed, "--", *command]
    return list(command), memory_capture, memory_report, calls_capture, calls_report


def _observation_span_attributes(environment: "Mapping[str, str]") -> list[tuple[str, str]]:
    return [
        (attribute, environment[variable])
        for attribute, variable in (
            ("rucio.test.run_id", "RUCIO_DEV_OBSERVE_RUN_ID"),
            ("rucio.test.case", "RUCIO_DEV_OBSERVE_TEST_CASE"),
            ("rucio.test.selector", "RUCIO_DEV_OBSERVE_TEST_SELECTOR"),
        )
        if environment.get(variable)
    ]


def _memory_profile_reports(root_dir: Path, artifact_path: Path) -> list[tuple[str, str]]:
    host_artifact = root_dir.resolve() / artifact_path.relative_to(_CONTAINER_SOURCE_ROOT)
    reports = []
    parent = host_artifact / "memory.bin"
    if parent.is_file():
        reports.append((parent.name, "memory.html"))

    children = []
    prefix = f"{parent.name}."
    for capture in host_artifact.glob(f"{parent.name}.*"):
        pid = capture.name.removeprefix(prefix)
        if capture.is_file() and pid.isdecimal():
            children.append((int(pid), pid, capture.name))
    for _numeric_pid, pid, capture in sorted(children):
        reports.append((capture, f"memory-fork-{pid}.html"))
    return [
        (str(artifact_path / capture), str(artifact_path / report))
        for capture, report in reports
    ]


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
    project_name = ContainerManager.make_project_name(
        case.id,
        root_dir,
        reusable=False,
        nonce=environment.get("RUCIO_TEST_PROJECT_NONCE"),
    )
    image = (
        f"rucio-test-unit:{checkout_id(root_dir)}-py{case.python.replace('.', '')}"
    )
    output_log = None
    if log_output:
        output_log = root_dir.resolve() / ".test-logs" / project_name / "case.log"
        with open_test_log_file(root_dir, project_name, output_log.name, "w"):
            pass
        print(f"Case output: {output_log}", flush=True)
    with temporary_docker_context(root_dir, _UNIT_BUILD_INPUTS) as context:
        build = [
            "docker",
            "buildx",
            "build",
            "--load",
            "--file",
            str(context / _UNIT_DOCKERFILE),
            "--build-arg",
            f"PYTHON={case.python}",
            "--tag",
            image,
            str(context),
        ]
        _run_unit_command(
            build,
            check=True,
            root_dir=root_dir,
            environment=environment,
            output_log=output_log,
            project_name=project_name,
            timeout=1800,
        )

    inner_environment = dict(case.env_vars)
    inner_environment.update(container_environment or {})
    inner_environment.update(_case_environment(case))
    inner_environment.update({
        "PYTHONPATH": "/rucio_source/lib",
        "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "true",
        "RUCIO_PYTEST_INNER": "1",
        "RUCIO_SKIP_TEST_SETUP": "1",
    })
    profile, profile_idle, debug, observe, artifact_path, debug_port = _development_mode(inner_environment)
    if profile:
        inner_environment.update(artifact_ownership_environment(str(artifact_path)))
        try:
            relative_artifact = artifact_path.relative_to(_CONTAINER_SOURCE_ROOT)
        except ValueError as error:
            raise pytest.UsageError(
                "Unit profile artifacts must be below /rucio_source"
            ) from error
        (root_dir / relative_artifact).mkdir(parents=True, exist_ok=True)

    pytest_command = [
        "python",
        "-bb",
        "-m",
        "pytest",
        "-r",
        "fExX",
        "--log-level=DEBUG",
        "-p",
        "rerunfailures",
        "-p",
        "xdist",
    ]
    if has_looponfail(pytest_args):
        pytest_command.extend(("-p", "xdist.looponfail"))
    if _has_coverage_option(pytest_args):
        pytest_command.extend(("-p", "pytest_cov"))
    pytest_command.extend(pytest_args)
    if not explicit_selectors:
        pytest_command.extend(case.test_paths)
    profiled, memory_capture, memory_report, calls_capture, calls_report = _instrument_pytest(
        pytest_command,
        profile=profile,
        profile_idle=profile_idle,
        debug=debug,
        observe=observe,
        observe_attributes=_observation_span_attributes(inner_environment),
        artifact_path=artifact_path,
        debug_port=debug_port,
    )
    if profile:
        profiled = artifact_ownership_command(profiled, inner_environment)

    command = ["docker", "run"]
    if is_interactive(pytest_args):
        command.extend(("--interactive", "--tty"))
    command.extend((
        "--rm",
        "--label",
        f"com.docker.compose.project={project_name}",
        "--label",
        "com.docker.compose.service=rucio",
        "--volume",
        f"{root_dir.resolve()}:/rucio_source:z",
        "--workdir",
        "/rucio_source",
    ))
    if debug:
        command.extend(("--publish", f"127.0.0.1:{debug_port}:{debug_port}"))
    if profile == "cpu":
        command.extend(("--cap-add", "SYS_PTRACE", "--security-opt", "seccomp=unconfined"))
    if observe:
        network = environment.get("RUCIO_OBSERVE_NETWORK_NAME", "").strip()
        if not network:
            raise pytest.UsageError("RUCIO_OBSERVE_NETWORK_NAME is required for observed unit tests")
        command.extend(("--network", network))
    for key, value in inner_environment.items():
        command.extend(("--env", f"{key}={value}"))
    command.extend(("--entrypoint", profiled[0], image, *profiled[1:]))
    with _resource_sampler(root_dir, project_name):
        result = _run_unit_command(
            command,
            check=False,
            root_dir=root_dir,
            environment=environment,
            output_log=output_log,
            project_name=project_name,
        ).returncode
    report_commands: list[tuple[str, ...]] = []
    if memory_capture and memory_report:
        for capture, report in _memory_profile_reports(root_dir, artifact_path):
            report_commands.append((
                "python",
                "-m",
                "memray",
                "flamegraph",
                "--force",
                "--output",
                report,
                capture,
            ))
    elif calls_capture and calls_report:
        report_commands.append((
            "python",
            "/rucio_source/tools/devenv/pstats_report.py",
            calls_capture,
            calls_report,
        ))
    for report_command in report_commands:
        owned_report_command = artifact_ownership_command(report_command, inner_environment)
        render_command = [
            "docker",
            "run",
            "--rm",
            "--volume",
            f"{root_dir.resolve()}:/rucio_source:z",
            "--workdir",
            "/rucio_source",
        ]
        for key in (
            "RUCIO_DEV_ARTIFACT_DIR",
            "RUCIO_DEV_HOST_UID",
            "RUCIO_DEV_HOST_GID",
        ):
            if key in inner_environment:
                render_command.extend(("--env", f"{key}={inner_environment[key]}"))
        render_command.extend((
            "--entrypoint",
            owned_report_command[0],
            image,
            *owned_report_command[1:],
        ))
        render = _run_unit_command(
            render_command,
            check=False,
            root_dir=root_dir,
            environment=environment,
            output_log=output_log,
            project_name=project_name,
        ).returncode
        if result == 0:
            result = render
    return result


def _run_unit_command(
    command: "Sequence[str]",
    *,
    check: bool,
    root_dir: Path,
    environment: "Mapping[str, str]",
    output_log: "Optional[Path]",
    project_name: str,
    timeout: "Optional[int]" = None,
) -> subprocess.CompletedProcess:
    if output_log:
        with open_test_log_file(root_dir, project_name, output_log.name, "a") as output:
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
        selected_args = add_pytest_options(
            selected_args,
            "--export-artifacts-from=test_tpc",
        )
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
            arguments = add_pytest_options(
                arguments,
                "--export-artifacts-from=test_tpc",
            )
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
            if _may_skip_tpc(pytest_args):
                continue
            return result
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
    inner_environment.update(_case_environment(case))
    inner_environment.update({
        "RUCIO_PYTEST_INNER": "1",
        "RUCIO_KEEP_TEST_DB": "1" if keep_db else "0",
    })
    if "GITHUB_ACTIONS" in manager.environment:
        inner_environment["GITHUB_ACTIONS"] = manager.environment["GITHUB_ACTIONS"]
    profile, profile_idle, debug, observe, artifact_path, debug_port = _development_mode(inner_environment)
    if profile:
        inner_environment.update(artifact_ownership_environment(str(artifact_path)))
        prepare = manager.exec(
            "rucio",
            "mkdir",
            "-p",
            "--",
            str(artifact_path),
            environment=inner_environment,
            check=False,
        )
        if prepare.returncode:
            return prepare.returncode
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
        "-p",
        "xdist",
    ]
    if has_looponfail(pytest_args):
        command.extend(("-p", "xdist.looponfail"))
    if case.xdist_enabled and profile not in {"memory", "calls"} and not debug and not observe:
        if not overrides_default_xdist_workers(pytest_args):
            workers = "3" if manager.environment.get("GITHUB_ACTIONS") == "true" else "auto"
            command.append(f"--numprocesses={workers}")
    if _has_coverage_option(pytest_args):
        command.extend(("-p", "pytest_cov"))
    command.extend(pytest_args)
    command, memory_capture, memory_report, calls_capture, calls_report = _instrument_pytest(
        command,
        profile=profile,
        profile_idle=profile_idle,
        debug=debug,
        observe=observe,
        observe_attributes=_observation_span_attributes(inner_environment),
        artifact_path=artifact_path,
        debug_port=debug_port,
    )
    if profile:
        command = artifact_ownership_command(command, inner_environment)
    result = manager.exec(
        "rucio",
        *command,
        environment=inner_environment,
        check=False,
        interactive=is_interactive(pytest_args),
    ).returncode
    if memory_capture and memory_report:
        for capture, report in _memory_profile_reports(manager.root_dir, artifact_path):
            report_command = artifact_ownership_command(
                (
                    "python",
                    "-m",
                    "memray",
                    "flamegraph",
                    "--force",
                    "--output",
                    report,
                    capture,
                ),
                inner_environment,
            )
            report_result = manager.exec(
                "rucio",
                *report_command,
                environment=inner_environment,
                check=False,
            ).returncode
            if result == 0:
                result = report_result
    if calls_capture and calls_report:
        report_command = artifact_ownership_command(
            (
                "python",
                "/rucio_source/tools/devenv/pstats_report.py",
                calls_capture,
                calls_report,
            ),
            inner_environment,
        )
        report_result = manager.exec(
            "rucio",
            *report_command,
            environment=inner_environment,
            check=False,
        ).returncode
        if result == 0 and report_result:
            return report_result
    return result


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
        if argument == "--":
            break
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
        qualified = add_pytest_options(
            qualified,
            "-o",
            f"cache_dir=/rucio_source/.pytest_cache/rucio-cases/{qualifier}",
        )
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
        if argument == "--":
            break
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
        if argument == "--":
            break
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
    if (
        coverage_enabled(updated)
        and "--cov-append" not in _options_before_separator(updated)
    ):
        updated = add_pytest_options(updated, "--cov-append")
    return updated


def defer_coverage_threshold(arguments: "Sequence[str]") -> list[str]:
    updated = list(arguments)
    options = _options_before_separator(updated)
    zero_threshold = "--cov-fail-under=0" in options or any(
        argument == "--cov-fail-under"
        and index + 1 < len(options)
        and options[index + 1] == "0"
        for index, argument in enumerate(options)
    )
    if coverage_enabled(updated) and not zero_threshold:
        updated = add_pytest_options(updated, "--cov-fail-under=0")
    return updated


def overrides_default_xdist_workers(arguments: "Sequence[str]") -> bool:
    dist = None
    distload = False
    for index, argument in enumerate(arguments):
        if argument == "--":
            break
        if (
            argument in ("-n", "--numprocesses", "--tx", "-f", "--looponfail")
            or (argument.startswith("-n") and argument != "-n")
            or argument.startswith("--numprocesses=")
            or argument.startswith("--tx=")
        ):
            return True
        if argument == "--dist" and index + 1 < len(arguments):
            dist = arguments[index + 1]
        elif argument.startswith("--dist="):
            dist = argument.split("=", 1)[1]
        elif argument == "-d":
            distload = True
    return not distload and dist == "no"


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


def has_looponfail(arguments: "Sequence[str]") -> bool:
    for argument in arguments:
        if argument == "--":
            return False
        if argument in ("-f", "--looponfail"):
            return True
    return False


def _has_coverage_option(arguments: "Sequence[str]") -> bool:
    return any(
        argument == "--no-cov" or argument.startswith("--cov")
        for argument in _options_before_separator(arguments)
    )


def coverage_enabled(arguments: "Sequence[str]") -> bool:
    options = _options_before_separator(arguments)
    return "--no-cov" not in options and any(
        argument == "--cov" or argument.startswith("--cov=")
        for argument in options
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
    options = _options_before_separator(arguments)
    return info_options.isdisjoint(options) and not any(
        argument == "--cache-show" or argument.startswith("--cache-show=")
        for argument in options
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
    return has_looponfail(arguments) or any(
        argument.split("=", 1)[0] in options
        for argument in _options_before_separator(arguments)
    )


def _has_cache_show(arguments: "Sequence[str]") -> bool:
    return any(
        argument == "--cache-show" or argument.startswith("--cache-show=")
        for argument in _options_before_separator(arguments)
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
        for argument in _options_before_separator(arguments)
    )


def is_interactive(arguments: "Sequence[str]") -> bool:
    return has_looponfail(arguments) or any(
        argument in ("--pdb", "--trace") or argument.startswith("--pdbcls")
        for argument in _options_before_separator(arguments)
    )
