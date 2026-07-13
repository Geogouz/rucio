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

from __future__ import annotations

import argparse
import json
import os
import re
import secrets
import shlex
import shutil
import signal
import subprocess  # noqa: S404
import sys
import time
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import urlencode

from .artifacts import ArtifactRun, DockerStatsSampler, artifact_ownership_command
from .compose import ComposeProject, profiles_for_daemon, workspace_lock
from .control import control_python, development_environment_is_unmanaged
from .prerequisites import check_docker_compose
from .runtime import checkout_id, docker_platform, repository_root

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping, Sequence

    from tests.ruciopytest.profiles import TestCase


SOURCE_ROOT = Path(__file__).resolve().parents[2]
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))


DEFAULT_CASE = "remote-dbs-py310-postgres14"
DEBUG_PORT = 5678
API_PORT = 8080
INTERRUPT_GRACE_SECONDS = 60
TERMINATE_GRACE_SECONDS = 15


def parser() -> argparse.ArgumentParser:
    root_parser = argparse.ArgumentParser(
        prog="./tools/dev",
        description="Rucio development, testing, debugging and profiling toolbox.",
    )
    subparsers = root_parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("doctor", help="Verify the local prerequisites")

    up = subparsers.add_parser("up", help="Build and start the development environment")
    _add_environment_options(up)
    up.add_argument("--no-initialize", action="store_true", help="Do not initialize the catalogue")

    for name, help_text in (
        ("status", "Show development containers"),
        ("down", "Stop containers and retain data"),
        ("reset", "Stop containers and remove project data"),
        ("shell", "Open a shell in the workspace"),
        ("logs", "Follow workspace logs"),
    ):
        command = subparsers.add_parser(name, help=help_text)
        if name in {"shell", "logs"}:
            _add_environment_options(command)

    test = subparsers.add_parser("test", help="Run a canonical local/CI test case")
    _add_test_options(test)

    run_target = subparsers.add_parser("run", help="Run a daemon, API server, or command")
    run_target.add_argument("target", choices=("daemon", "server", "command"))
    run_target.add_argument("arguments", nargs=argparse.REMAINDER)

    observe = subparsers.add_parser("observe", help="Run a target with local traces, metrics, and logs")
    observe.add_argument("target", choices=("test", "daemon", "server", "command"))
    observe.add_argument("arguments", nargs=argparse.REMAINDER)

    debug = subparsers.add_parser("debug", help="Run a test, daemon, API server, or command under debugpy")
    debug.add_argument("target", choices=("test", "daemon", "server", "command"))
    debug.add_argument("arguments", nargs=argparse.REMAINDER)

    profile = subparsers.add_parser("profile", help="Profile a target; memory renders one report per process")
    profile.add_argument("kind", choices=("cpu", "memory", "calls", "resources"))
    profile.add_argument(
        "--idle",
        action="store_true",
        help="Include idle threads in a CPU profile; place before the target",
    )
    profile.add_argument("target", choices=("test", "daemon", "server", "command"))
    profile.add_argument("arguments", nargs=argparse.REMAINDER)

    report = subparsers.add_parser("report", help="Generate or locate development reports")
    report.add_argument("report", choices=("tests", "latest"))
    report.add_argument("arguments", nargs=argparse.REMAINDER)
    report.add_argument("--workers", type=_positive_integer, default=4)
    return root_parser


def _add_environment_options(command: argparse.ArgumentParser) -> None:
    command.add_argument(
        "--profile",
        action="append",
        default=[],
        choices=("storage", "messaging", "externalmetadata", "iam", "observe"),
        help="Additional service capability; repeat as needed",
    )
    command.add_argument("--ports", action="store_true", help="Publish local service ports")


def _add_test_axis_options(command: argparse.ArgumentParser) -> None:
    from tests.ruciopytest.profiles import SUITE_DEFINITIONS, SUPPORTED_PYTHON_VERSIONS

    databases = tuple(dict.fromkeys(
        rdbms
        for definition in SUITE_DEFINITIONS.values()
        for rdbms in definition.rdbms
    ))
    policies = tuple(dict.fromkeys(
        policy
        for definition in SUITE_DEFINITIONS.values()
        for policy in definition.policies
    ))
    command.add_argument(
        "--python",
        dest="python_version",
        choices=SUPPORTED_PYTHON_VERSIONS,
        help="Run the selected suite with this Python version",
    )
    command.add_argument("--rdbms", choices=databases, help="Run the selected suite with this database")
    command.add_argument("--policy", choices=policies, help="Run the selected suite with this policy")


def _add_test_options(command: argparse.ArgumentParser) -> None:
    command.add_argument("--list", action="store_true", help="List canonical cases")
    command.add_argument("--keep-db", action="store_true", help="Reuse this checkout's database volume")
    command.add_argument("--case-workers", type=_positive_integer)
    command.add_argument("--xdist-workers", type=int)
    _add_test_axis_options(command)
    command.add_argument("case", nargs="?")
    command.add_argument("pytest_args", nargs=argparse.REMAINDER)


def _parse_test_selection(arguments: "Sequence[str]") -> argparse.Namespace:
    selection_parser = argparse.ArgumentParser(prog="test", add_help=False)
    _add_test_axis_options(selection_parser)
    selection_parser.add_argument("case", nargs="?", default=DEFAULT_CASE)
    selection_parser.add_argument("pytest_args", nargs=argparse.REMAINDER)
    return selection_parser.parse_args(arguments)


def _validate_test_axis_suite(
    case: str | None,
    *,
    python_version: str | None,
    rdbms: str | None,
    policy: str | None,
) -> None:
    axes = tuple(
        option
        for option, value in (
            ("--python", python_version),
            ("--rdbms", rdbms),
            ("--policy", policy),
        )
        if value is not None
    )
    if not axes:
        return

    from tests.ruciopytest.profiles import SUITE_DEFINITIONS

    if case == "all":
        if rdbms is not None or policy is not None:
            raise ValueError("--rdbms and --policy cannot be combined with the all suite")
        return
    if case not in SUITE_DEFINITIONS:
        verb = "requires" if len(axes) == 1 else "require"
        raise ValueError(f"{', '.join(axes)} {verb} a positional test suite")


def _resolve_single_test_selection(
    arguments: "Sequence[str]",
) -> "tuple[argparse.Namespace, TestCase]":
    from tests.ruciopytest.profiles import SUITE_DEFINITIONS, get_case, iter_suite_cases

    selection = _parse_test_selection(arguments)
    _validate_test_axis_suite(
        selection.case,
        python_version=selection.python_version,
        rdbms=selection.rdbms,
        policy=selection.policy,
    )
    if selection.case == "all":
        cases = tuple(
            case
            for suite in SUITE_DEFINITIONS
            for case in iter_suite_cases(suite, python=selection.python_version)
        )
    elif selection.case in SUITE_DEFINITIONS:
        cases = tuple(iter_suite_cases(
            selection.case,
            python=selection.python_version,
            rdbms=selection.rdbms,
            policy=selection.policy,
        ))
    else:
        cases = (get_case(selection.case),)
    if len(cases) != 1:
        raise ValueError(
            f"Test selection resolved to {len(cases)} cases; "
            "use --python, --rdbms, and --policy to select exactly one"
        )
    return selection, cases[0]


def _positive_integer(value: str) -> int:
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("value must be at least 1")
    return parsed


def _profiles(values: "Sequence[str]") -> tuple[str, ...]:
    profiles = []
    for value in values:
        profiles.extend(item for item in value.split(",") if item)
    return tuple(dict.fromkeys(profiles))


def list_cases() -> int:
    from tests.ruciopytest.profiles import SUPPORTED_PYTHON_VERSIONS, iter_cases

    print("Canonical Rucio test cases:")
    for case in iter_cases():
        capabilities = ", ".join(case.compose_profiles) or "none"
        print(f"  {case.id:<42} suite={case.suite:<12} services={capabilities}")
    print("\nSuite runs can select any Python version in the canonical matrix.")
    print(f"Supported Python versions: {', '.join(SUPPORTED_PYTHON_VERSIONS)}")
    print("Example: ./tools/dev test --python 3.13 --rdbms postgres14 remote_dbs -- <pytest selectors>")
    return 0


def choose_case() -> str | None:
    from tests.ruciopytest.profiles import iter_cases

    cases = tuple(iter_cases())
    if not sys.stdin.isatty():
        list_cases()
        return None
    list_cases()
    while True:
        value = input(f"Select a case [1-{len(cases)}] or press Enter to cancel: ").strip()
        if not value:
            return None
        if value.isdigit() and 1 <= int(value) <= len(cases):
            return cases[int(value) - 1].id
        print("Invalid selection.", file=sys.stderr)


def test_command(
    root: Path,
    case: str,
    pytest_args: "Sequence[str]",
    *,
    keep_db: bool = False,
    case_workers: int | None = None,
    xdist_workers: int | None = None,
    python_version: str | None = None,
    rdbms: str | None = None,
    policy: str | None = None,
    container_environment: "Mapping[str, str] | None" = None,
) -> list[str]:
    from tests.ruciopytest.profiles import SUITE_DEFINITIONS, get_case

    python = control_python(root)
    command = [str(python), "-m", "pytest"]
    if case == "all":
        command.append("--suite=all")
    elif case in SUITE_DEFINITIONS:
        command.append(f"--suite={case}")
    else:
        get_case(case)
        command.append(f"--case={case}")
    if keep_db:
        command.append("--keep-db")
    if case_workers is not None:
        command.append(f"--case-workers={case_workers}")
    if xdist_workers is not None:
        command.append(f"--xdist-workers={xdist_workers}")
    if python_version is not None:
        command.append(f"--python={python_version}")
    if rdbms is not None:
        command.append(f"--rdbms={rdbms}")
    if policy is not None:
        command.append(f"--policy={policy}")
    for key, value in (container_environment or {}).items():
        command.extend(("--container-env", f"{key}={value}"))
    selectors = [value for value in pytest_args if value]
    if selectors[:1] == ["--"]:
        selectors.pop(0)
    command.extend(selectors)
    return command


def run_test(
    root: Path,
    case: str,
    pytest_args: "Sequence[str]",
    *,
    keep_db: bool = False,
    case_workers: int | None = None,
    xdist_workers: int | None = None,
    python_version: str | None = None,
    rdbms: str | None = None,
    policy: str | None = None,
    container_environment: "Mapping[str, str] | None" = None,
    environment: "Mapping[str, str] | None" = None,
    debug: bool = False,
    debug_project: str = "",
) -> int:
    command = test_command(
        root,
        case,
        pytest_args,
        keep_db=keep_db,
        case_workers=case_workers,
        xdist_workers=xdist_workers,
        python_version=python_version,
        rdbms=rdbms,
        policy=policy,
        container_environment=container_environment,
    )
    process_environment = dict(os.environ if environment is None else environment)
    print("+", shlex.join(command), flush=True)
    if debug:
        print("Rucio debugger starting", flush=True)
    process = subprocess.Popen(  # noqa: S603
        command,
        cwd=root,
        env=process_environment,
        start_new_session=os.name == "posix",
    )
    try:
        if debug:
            _wait_for_debugger(process, root, debug_project, DEBUG_PORT)
        return process.wait()
    except BaseException:
        _stop_process(process)
        raise


def _wait_for_debugger(
    process: subprocess.Popen[bytes],
    root: Path,
    project: str,
    port: int,
    timeout: int = 3600,
    service: str = "rucio",
) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"Debug target exited with code {process.returncode} before opening port {port}")
        if _debugger_is_listening(root, project, port, service):
            print(f"Rucio debugger ready on 127.0.0.1:{port}", flush=True)
            return
        time.sleep(0.25)
    raise TimeoutError(f"Debug target did not open port {port}")


def _debugger_is_listening(root: Path, project: str, port: int, service: str = "rucio") -> bool:
    if not project:
        return False
    docker = shutil.which("docker")
    if docker is None:
        raise RuntimeError("Required executable not found: docker")
    containers = subprocess.run(  # noqa: S603
        (
            docker,
            "ps",
            "--filter",
            f"label=com.docker.compose.project={project}",
            "--filter",
            f"label=com.docker.compose.service={service}",
            "--format",
            "{{.ID}}",
        ),
        cwd=root,
        check=False,
        capture_output=True,
        text=True,
    ).stdout.split()
    check = (
        "import socket, sys\n"
        "sock = socket.socket()\n"
        f"address = ('0.0.0.0', {port})\n"
        "try:\n"
        "    sock.bind(address)\n"
        "except OSError:\n"
        "    sys.exit(0)\n"
        "sys.exit(1)\n"
    )
    return any(
        subprocess.run(  # noqa: S603
            (docker, "exec", container, "python", "-c", check),
            cwd=root,
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        ).returncode
        == 0
        for container in containers
    )


def _stop_process(process: subprocess.Popen[bytes]) -> None:
    if process.poll() is not None:
        return
    _signal_process(process, signal.SIGINT)
    try:
        process.wait(timeout=INTERRUPT_GRACE_SECONDS)
        return
    except (KeyboardInterrupt, subprocess.TimeoutExpired):
        _signal_process(process, signal.SIGTERM)
    try:
        process.wait(timeout=TERMINATE_GRACE_SECONDS)
        return
    except (KeyboardInterrupt, subprocess.TimeoutExpired):
        if os.name == "posix":
            _signal_process(process, signal.SIGKILL)
        else:
            process.kill()
    process.wait()


def _signal_process(process: subprocess.Popen[bytes], value: int) -> None:
    try:
        if os.name == "posix":
            os.killpg(process.pid, value)
        else:
            process.send_signal(value)
    except ProcessLookupError:
        pass


def _run_compose_exec(
    project: ComposeProject,
    root: Path,
    service: str,
    command: "Sequence[str]",
    environment: "Mapping[str, str]",
) -> int:
    arguments = ["exec", "--no-TTY"]
    for key, value in environment.items():
        arguments.extend(("--env", f"{key}={value}"))
    arguments.extend((service, *command))
    return _run_compose_process(project, root, arguments)


def _run_compose_process(
    project: ComposeProject,
    root: Path,
    arguments: "Sequence[str]",
) -> int:
    process = subprocess.Popen(  # noqa: S603
        project.command(*arguments),
        cwd=root,
        env=project.environment,
        start_new_session=os.name == "posix",
    )
    try:
        return process.wait()
    except KeyboardInterrupt:
        _stop_process(process)
        return 130
    except BaseException:
        _stop_process(process)
        raise


def test_project_name(root: Path, case: str, nonce: str) -> str:
    from tests.ruciopytest.container_manager import ContainerManager

    return ContainerManager.make_project_name(case, root, reusable=False, nonce=nonce)


def _tempo_query_url(query: str, started_at_ms: int) -> str:
    panes = {
        "test": {
            "datasource": "tempo",
            "queries": [
                {
                    "datasource": {"type": "tempo", "uid": "tempo"},
                    "limit": 20,
                    "query": query,
                    "queryType": "traceql",
                    "refId": "A",
                    "tableType": "traces",
                },
            ],
            "range": {
                "from": str(started_at_ms - 300_000),
                "to": str(started_at_ms + 86_400_000),
            },
        },
    }
    parameters = urlencode({
        "schemaVersion": "1",
        "panes": json.dumps(panes, separators=(",", ":")),
        "orgId": "1",
    })
    return f"http://127.0.0.1:3001/explore?{parameters}"


def _write_test_observation(
    root: Path,
    project: str,
    observation: "Mapping[str, object]",
) -> None:
    from tests.ruciopytest.container_manager import open_test_log_file

    with open_test_log_file(root, project, "observation.json", "w") as output:
        json.dump(observation, output, indent=2, sort_keys=True)
        output.write("\n")


def debug_command(command: "Sequence[str]", port: int = DEBUG_PORT) -> list[str]:
    prefix = [
        "python",
        "-m",
        "debugpy",
        "--listen",
        f"0.0.0.0:{port}",
        "--wait-for-client",
    ]
    if len(command) >= 3 and _is_python_interpreter(command[0]) and command[1] == "-m":
        return [*prefix, "-m", command[2], *command[3:]]
    if command and _is_python_interpreter(command[0]):
        if len(command) < 2 or command[1].startswith("-"):
            raise ValueError("Debugged Python commands must use a script path or -m module")
        return [*prefix, *command[1:]]
    return [*prefix, *command]


def profile_command(
    kind: str,
    command: "Sequence[str]",
    artifact_dir: str,
    *,
    idle: bool = False,
) -> list[str]:
    if idle and kind != "cpu":
        raise ValueError("--idle is only supported for CPU profiles")
    if kind == "cpu":
        return [
            "py-spy",
            "record",
            "--format",
            "flamegraph",
            "--subprocesses",
            *(("--idle",) if idle else ()),
            "--output",
            f"{artifact_dir}/cpu.svg",
            "--",
            *command,
        ]
    if kind == "calls":
        return [
            "python",
            "-m",
            "tools.devenv.calls",
            "--output",
            f"{artifact_dir}/calls.pstats",
            "--",
            *command,
        ]
    module = len(command) >= 3 and _is_python_interpreter(command[0]) and command[1] == "-m"
    if command and _is_python_interpreter(command[0]) and not module:
        if len(command) < 2 or command[1].startswith("-"):
            raise ValueError("Profiled Python commands must use a script path or -m module")
        target = list(command[1:])
    elif module:
        target = ["-m", command[2], *command[3:]]
    else:
        target = list(command)
    if kind == "memory":
        return [
            "python",
            "-m",
            "memray",
            "run",
            "--force",
            "--follow-fork",
            "--output",
            f"{artifact_dir}/memory.bin",
            *target,
        ]
    return list(command)


def _memory_profile_reports(artifact_path: Path) -> list[tuple[str, str]]:
    reports = []
    parent = artifact_path / "memory.bin"
    if parent.is_file():
        reports.append((parent.name, "memory.html"))

    children = []
    prefix = f"{parent.name}."
    for capture in artifact_path.glob(f"{parent.name}.*"):
        pid = capture.name.removeprefix(prefix)
        if capture.is_file() and pid.isdecimal():
            children.append((int(pid), pid, capture.name))
    for _numeric_pid, pid, capture in sorted(children):
        reports.append((capture, f"memory-fork-{pid}.html"))
    return reports


def _is_python_interpreter(executable: str) -> bool:
    return re.fullmatch(r"python(?:\d+(?:\.\d+)*)?", Path(executable).name) is not None


def profile_test(
    root: Path,
    kind: str,
    arguments: "Sequence[str]",
    *,
    idle: bool = False,
) -> int:
    if idle and kind != "cpu":
        raise ValueError("--idle is only supported for CPU profiles")
    selection, case_definition = _resolve_single_test_selection(arguments)
    selectors = [value for value in selection.pytest_args if value]
    nonce = secrets.token_hex(4)
    project = test_project_name(root, case_definition.id, nonce)
    environment = dict(os.environ)
    if case_definition.suite == "unit":
        image = f"rucio-test-unit:{checkout_id(root)}-py{case_definition.python.replace('.', '')}"
    else:
        image = (
            environment.get(case_definition.runtime_image_variable)
            or environment.get("RUCIO_TEST_IMAGE")
            or f"rucio-test-runtime:{checkout_id(root)}-{case_definition.runtime}"
        )
    platform = case_definition.runtime_platform or (
        environment.get("RUCIO_TEST_NATIVE_PLATFORM") or docker_platform(root, environment)
    )
    run = ArtifactRun(
        root,
        name=f"{kind}{'-idle' if idle else ''}-test-{case_definition.id}",
        command=("profile", kind, *(("--idle",) if idle else ()), "test", *arguments),
        image=image,
        platform=platform,
        project=project,
    )
    container_environment = {}
    if kind != "resources":
        container_environment.update({
            "RUCIO_DEV_ARTIFACT_DIR": run.container_path,
            "RUCIO_DEV_PROFILE": kind,
        })
        if idle:
            container_environment["RUCIO_DEV_PROFILE_IDLE"] = "1"
    environment["RUCIO_TEST_PROJECT_NONCE"] = nonce
    environment["RUCIO_DEV_STATS_PATH"] = str(run.path / "docker-stats.jsonl")
    if kind != "resources":
        environment["RUCIO_DEV_PROFILE"] = kind
        if idle:
            environment["RUCIO_DEV_PROFILE_IDLE"] = "1"
    returncode = 1
    try:
        returncode = run_test(
            root,
            selection.case,
            selectors,
            xdist_workers=None if not selectors and kind in {"cpu", "resources"} else 0,
            python_version=selection.python_version,
            rdbms=selection.rdbms,
            policy=selection.policy,
            container_environment=container_environment,
            environment=environment,
        )
    except KeyboardInterrupt:
        returncode = 130
    except BaseException:
        run.finish(returncode)
        raise
    run.finish(returncode)
    print(f"Profile report: {(run.path / 'index.html').as_uri()}")
    return returncode


def debug_test(root: Path, arguments: "Sequence[str]") -> int:
    selection, case_definition = _resolve_single_test_selection(arguments)
    selectors = [value for value in selection.pytest_args if value]
    nonce = secrets.token_hex(4)
    project = test_project_name(root, case_definition.id, nonce)
    environment = dict(os.environ)
    environment.update({
        "RUCIO_DEV_DEBUG": "1",
        "RUCIO_DEBUG_PORT": str(DEBUG_PORT),
        "RUCIO_TEST_PROJECT_NONCE": nonce,
    })
    return run_test(
        root,
        selection.case,
        selectors,
        xdist_workers=0,
        python_version=selection.python_version,
        rdbms=selection.rdbms,
        policy=selection.policy,
        container_environment={
            "RUCIO_DEV_DEBUG": "1",
            "RUCIO_DEBUG_PORT": str(DEBUG_PORT),
        },
        environment=environment,
        debug=True,
        debug_project=project,
    )


def observe_test(root: Path, arguments: "Sequence[str]") -> int:
    selection, case_definition = _resolve_single_test_selection(arguments)
    selectors = [value for value in selection.pytest_args if value]
    nonce = secrets.token_hex(4)
    project = test_project_name(root, case_definition.id, nonce)
    selector = shlex.join(selectors) or "<case defaults>"
    started_at_ms = round(time.time() * 1000)
    tempo_query = f"{{ span.rucio.test.run_id = {json.dumps(nonce)} }}"
    tempo_url = _tempo_query_url(tempo_query, started_at_ms)
    observation: dict[str, object] = {
        "case": case_definition.id,
        "run_id": nonce,
        "selector": selector,
        "started_at_unix_ms": started_at_ms,
        "tempo_query": tempo_query,
        "tempo_url": tempo_url,
    }
    _write_test_observation(root, project, observation)
    environment = dict(os.environ)
    container_environment = {
        "OTEL_EXPORTER_OTLP_ENDPOINT": "http://otel-lgtm:4318",
        "OTEL_EXPORTER_OTLP_PROTOCOL": "http/protobuf",
        "OTEL_LOGS_EXPORTER": "none",
        "OTEL_METRICS_EXPORTER": "none",
        "OTEL_PYTHON_LOG_AUTO_INSTRUMENTATION": "false",
        "OTEL_PYTHON_LOG_CORRELATION": "false",
        "OTEL_TRACES_EXPORTER": "otlp",
        "RUCIO_DEV_OBSERVE": "1",
        "RUCIO_DEV_OBSERVE_RUN_ID": nonce,
        "RUCIO_DEV_OBSERVE_TEST_CASE": case_definition.id,
        "RUCIO_DEV_OBSERVE_TEST_SELECTOR": selector,
    }
    with workspace_lock(root):
        backend = ComposeProject(root, profiles=("observe",), observe=True, build=False)
        backend.run(
            "up",
            "--detach",
            "--wait",
            "--wait-timeout",
            "180",
            "otel-lgtm",
            timeout=240,
        )
        environment.update({
            "RUCIO_DEV_OBSERVE": "1",
            "RUCIO_OBSERVE_NETWORK_NAME": backend.environment["RUCIO_NETWORK_NAME"],
            "RUCIO_TEST_PROJECT_NONCE": nonce,
        })
        print("Observation", flush=True)
        print(f"  Test: {selector}", flush=True)
        print(f"  Case: {case_definition.id}", flush=True)
        print(f"  Run: {nonce}", flush=True)
        print(f"  Test logs: {root / '.test-logs' / project}", flush=True)
        returncode = run_test(
            root,
            selection.case,
            selectors,
            xdist_workers=0,
            python_version=selection.python_version,
            rdbms=selection.rdbms,
            policy=selection.policy,
            container_environment=container_environment,
            environment=environment,
        )
        observation["exit_code"] = returncode
        _write_test_observation(root, project, observation)
        print(f"Tempo trace: {tempo_url}", flush=True)
        return returncode


def workspace_command(
    root: Path,
    target: str,
    arguments: "Sequence[str]",
    *,
    mode: str = "run",
    profile_kind: str = "",
    profile_idle: bool = False,
) -> int:
    with workspace_lock(root):
        return _workspace_command(
            root,
            target,
            arguments,
            mode=mode,
            profile_kind=profile_kind,
            profile_idle=profile_idle,
        )


def _workspace_command(
    root: Path,
    target: str,
    arguments: "Sequence[str]",
    *,
    mode: str = "run",
    profile_kind: str = "",
    profile_idle: bool = False,
) -> int:
    if profile_idle and profile_kind != "cpu":
        raise ValueError("--idle is only supported for CPU profiles")
    values = list(arguments)
    if values[:1] == ["--"]:
        values.pop(0)
    if target == "daemon":
        if not values:
            raise ValueError("A daemon name is required")
        executable = values.pop(0)
        if not executable.startswith("rucio-"):
            executable = f"rucio-{executable}"
        if not re.fullmatch(r"rucio-[a-z0-9-]+", executable):
            raise ValueError(f"Invalid daemon name: {executable}")
        profiles = profiles_for_daemon(executable)
        command = [executable, *values]
    elif target == "server":
        profiles = ()
        command = [
            "python",
            "-m",
            "flask",
            "--app",
            "rucio.web.rest.flaskapi.v1.main",
            "run",
            "--host=0.0.0.0",
            f"--port={API_PORT}",
            *values,
        ]
    else:
        if not values:
            raise ValueError("A command is required after --")
        profiles = ()
        command = values

    instrumentation = mode == "profile" and profile_kind == "cpu"
    if mode == "observe":
        profiles = (*profiles, "observe")
    project = ComposeProject(
        root,
        profiles=profiles,
        debug=mode == "debug",
        api=target == "server",
        instrumentation=instrumentation,
        observe=mode == "observe",
    )
    restore_workspace = mode in {"debug", "observe"} or target == "server" or instrumentation
    try:
        return _run_workspace_command(
            project,
            root,
            target,
            arguments,
            command,
            mode,
            profile_kind,
            profile_idle,
        )
    finally:
        if restore_workspace:
            command_error = sys.exc_info()[1]
            try:
                ComposeProject(
                    root,
                    profiles=profiles,
                    build=False,
                    environment=project.environment,
                ).recreate_workspace()
            except Exception as restore_error:
                if command_error is None:
                    raise
                print(f"Workspace restoration also failed: {restore_error}", file=sys.stderr)


def _run_workspace_command(
    project: ComposeProject,
    root: Path,
    target: str,
    arguments: "Sequence[str]",
    command: list[str],
    mode: str,
    profile_kind: str,
    profile_idle: bool = False,
) -> int:
    project.up()
    if target == "daemon":
        found = project.exec("workspace", "which", command[0], capture_output=True, check=False)
        if found.returncode:
            raise RuntimeError(f"Daemon executable not found: {command[0]}")
        command[0] = found.stdout.strip()

    if mode == "debug":
        command = debug_command(command)
        print("Rucio debugger starting", flush=True)
        process = subprocess.Popen(  # noqa: S603
            project.command("exec", "--no-TTY", "workspace", *command),
            cwd=root,
            env=project.environment,
            start_new_session=os.name == "posix",
        )
        try:
            _wait_for_debugger(process, root, project.project_name, DEBUG_PORT, service="workspace")
            return process.wait()
        except BaseException:
            _stop_process(process)
            raise

    if mode != "profile":
        if mode == "observe":
            service_name = f"rucio-{target}"
            observed = list(command)
            if target in {"daemon", "server"}:
                observed = [
                    "python",
                    "-m",
                    "tools.devenv.observe",
                    "--span-name",
                    f"{service_name}.run",
                    "--",
                    *observed,
                ]
            command = ["opentelemetry-instrument", "--service_name", service_name, *observed]
            print("Grafana: http://127.0.0.1:3001", flush=True)
        return _run_compose_exec(project, root, "workspace", command, {})

    run = ArtifactRun(
        root,
        name=f"{profile_kind}{'-idle' if profile_idle else ''}-{target}-{command[0]}",
        command=("profile", profile_kind, *(("--idle",) if profile_idle else ()), target, *arguments),
        image=project.image,
        platform=project.platform,
        project=project.project_name,
    )
    container_environment = run.container_environment
    profiled = artifact_ownership_command(
        profile_command(profile_kind, command, run.container_path, idle=profile_idle),
        container_environment,
    )
    returncode = 1
    try:
        with DockerStatsSampler(root, project.project_name, run.path / "docker-stats.jsonl"):
            returncode = _run_compose_exec(
                project,
                root,
                "workspace",
                profiled,
                container_environment,
            )
    except KeyboardInterrupt:
        returncode = 130
    except BaseException:
        run.finish(returncode)
        raise
    if profile_kind == "memory":
        for capture, report in _memory_profile_reports(run.path):
            render_command = artifact_ownership_command(
                (
                    "python",
                    "-m",
                    "memray",
                    "flamegraph",
                    "--force",
                    "--output",
                    f"{run.container_path}/{report}",
                    f"{run.container_path}/{capture}",
                ),
                container_environment,
            )
            rendered = _run_compose_exec(
                project,
                root,
                "workspace",
                render_command,
                container_environment,
            )
            if returncode == 0:
                returncode = rendered
    if profile_kind == "calls" and (run.path / "calls.pstats").is_file():
        render_command = artifact_ownership_command(
            (
                "python",
                "/rucio_source/tools/devenv/pstats_report.py",
                f"{run.container_path}/calls.pstats",
                f"{run.container_path}/calls.html",
            ),
            container_environment,
        )
        rendered = _run_compose_exec(
            project,
            root,
            "workspace",
            render_command,
            container_environment,
        )
        if returncode == 0:
            returncode = rendered
    run.finish(returncode)
    print(f"Profile report: {(run.path / 'index.html').as_uri()}")
    return returncode


def doctor(root: Path) -> int:
    checks = (
        ("Docker daemon", ("docker", "info")),
        ("Docker Buildx", ("docker", "buildx", "version")),
        ("Git checkout", ("git", "rev-parse", "--show-toplevel")),
    )
    failed = False
    for name, command in checks:
        result = subprocess.run(command, cwd=root, check=False, capture_output=True, text=True)  # noqa: S603
        status = "ok" if result.returncode == 0 else "failed"
        print(f"{name:<18} {status}")
        failed |= result.returncode != 0
    compose_ready, compose_detail = check_docker_compose(root)
    compose_status = "ok" if compose_ready else "failed"
    print(f"{'Docker Compose':<18} {compose_status} ({compose_detail})")
    failed |= not compose_ready
    unmanaged = development_environment_is_unmanaged(root)
    try:
        python = control_python(root)
    except (OSError, RuntimeError, subprocess.SubprocessError) as error:
        print(f"{'Python environment':<18} failed ({error})")
        failed = True
    else:
        if unmanaged:
            print(f"{'IDE environment':<18} preserved (unmanaged .venv)")
            print(f"{'Control Python':<18} ok ({python})")
        else:
            print(f"{'IDE environment':<18} ok ({python})")
    if not failed:
        print("Rucio development prerequisites are ready.")
    return int(failed)


def run_performance_report(root: Path, workers: int, arguments: "Sequence[str]") -> int:
    command = [
        str(control_python(root)),
        str(root / "tools/test/run_test_performance_report.py"),
        "--workers",
        str(workers),
        *arguments,
    ]
    process = subprocess.Popen(  # noqa: S603
        command,
        cwd=root,
        start_new_session=os.name == "posix",
    )
    try:
        return process.wait()
    except KeyboardInterrupt:
        _stop_process(process)
        return 130
    except BaseException:
        _stop_process(process)
        raise


def latest_report(root: Path) -> int:
    candidates = sorted((root / ".rucio-dev/artifacts").glob("*/index.html"))
    candidates.extend(sorted((root / ".autotest/performance").glob("*/report.html")))
    if not candidates:
        print("No development reports found.", file=sys.stderr)
        return 1
    latest = max(candidates, key=lambda path: path.stat().st_mtime)
    print(latest.resolve().as_uri())
    return 0


@contextmanager
def _termination_signals() -> "Iterator[None]":
    caught = [signal.SIGTERM]
    if hasattr(signal, "SIGHUP"):
        caught.append(signal.SIGHUP)
    previous = {}

    def interrupt(_signum, _frame) -> None:
        raise KeyboardInterrupt

    try:
        for value in caught:
            previous[value] = signal.signal(value, interrupt)
        yield
    finally:
        for value, handler in previous.items():
            signal.signal(value, handler)


def _dispatch(args: argparse.Namespace, root: Path) -> int:
    if args.command == "doctor":
        return doctor(root)
    if args.command == "up":
        profiles = _profiles(args.profile)
        with workspace_lock(root):
            project = ComposeProject(root, profiles=profiles, ports=args.ports, observe="observe" in profiles)
            project.up(initialize=not args.no_initialize)
        print(f"Rucio workspace ready: {project.project_name}")
        return 0
    if args.command in {"status", "down", "reset"}:
        project = ComposeProject(
            root,
            profiles=("storage", "messaging", "externalmetadata", "iam", "observe"),
            observe=True,
            build=False,
        )
        if args.command == "status":
            return project.run("ps", check=False).returncode
        command = ["down", "--remove-orphans"]
        if args.command == "reset":
            command.append("--volumes")
        with workspace_lock(root):
            return project.run(*command, check=False).returncode
    if args.command == "shell":
        profiles = _profiles(args.profile)
        with workspace_lock(root):
            project = ComposeProject(root, profiles=profiles, ports=args.ports)
            project.up()
            return _run_compose_process(project, root, ("exec", "workspace", "/bin/bash"))
    if args.command == "logs":
        profiles = _profiles(args.profile)
        project = ComposeProject(root, profiles=profiles, ports=args.ports, build=False)
        return _run_compose_process(project, root, ("logs", "--follow", "rucio", "workspace"))
    if args.command == "test":
        if args.list:
            if args.case is not None or any((args.python_version, args.rdbms, args.policy)):
                raise ValueError("--list cannot be combined with a case, suite, or explicit axes")
            return list_cases()
        _validate_test_axis_suite(
            args.case,
            python_version=args.python_version,
            rdbms=args.rdbms,
            policy=args.policy,
        )
        case = args.case or choose_case()
        if case is None:
            return 0
        return run_test(
            root,
            case,
            args.pytest_args,
            keep_db=args.keep_db,
            case_workers=args.case_workers,
            xdist_workers=args.xdist_workers,
            python_version=args.python_version,
            rdbms=args.rdbms,
            policy=args.policy,
        )
    if args.command == "run":
        return workspace_command(root, args.target, args.arguments)
    if args.command == "observe":
        if args.target == "test":
            return observe_test(root, args.arguments)
        return workspace_command(root, args.target, args.arguments, mode="observe")
    if args.command == "debug":
        if args.target == "test":
            return debug_test(root, args.arguments)
        return workspace_command(root, args.target, args.arguments, mode="debug")
    if args.command == "profile":
        if args.target == "test":
            return profile_test(root, args.kind, args.arguments, idle=args.idle)
        return workspace_command(
            root,
            args.target,
            args.arguments,
            mode="profile",
            profile_kind=args.kind,
            profile_idle=args.idle,
        )
    if args.command == "report":
        if args.report == "latest":
            return latest_report(root)
        return run_performance_report(root, args.workers, args.arguments)
    return 2


def main(arguments: "Sequence[str] | None" = None) -> int:
    args = parser().parse_args(arguments)
    root = repository_root()
    try:
        with _termination_signals():
            return _dispatch(args, root)
    except KeyboardInterrupt:
        print("Interrupted.", file=sys.stderr)
        return 130
    except (OSError, RuntimeError, ValueError, subprocess.SubprocessError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
